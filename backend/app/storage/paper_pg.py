"""模拟盘账户 PostgreSQL 事务存储层（DES-005）。

设计要点：
- 6 张表直接建库（无历史数据 / 无迁移负担），遵循 tsp-rule/pgsql.mdc 库表约束。
- 一次成交 = 一个事务：account 行锁 + 同快照内重放 + 多表原子写入。
- 本模块只负责存储 IO，不含撮合/费率等业务口径；`data_dir` 仅为兼容既有调用签名，
  PG 模式下账户按 `account_id` 隔离，不再落文件。
- 连接走 `settings.paper_pg_url`；默认隔离级别 READ COMMITTED + FOR UPDATE 行锁。
"""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings

# 审计字段取值：单密码模式无用户身份，记操作来源。
_SOURCE = "system"

# 跨进程 DDL 同步锁键（advisory lock 为 bigint，固定常量即可）。
_SCHEMA_LOCK_KEY = 0x5453505041524552  # "TSPPAPER" ASCII

# 6 表 DDL（幂等 CREATE TABLE IF NOT EXISTS）。业务唯一键用 UNIQUE，主键统一 id。
_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS paper_account (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      initial_cash NUMERIC(18,2) NOT NULL,
      cash NUMERIC(18,2) NOT NULL,
      commission_pct NUMERIC(10,6) NOT NULL,
      stamp_tax_pct NUMERIC(10,6) NOT NULL,
      slippage_bps INTEGER NOT NULL,
      queue_limit_orders BOOLEAN NOT NULL,
      status TEXT NOT NULL,
      version BIGINT NOT NULL DEFAULT 0,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_fill (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      seq BIGINT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      date DATE NOT NULL,
      order_id TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_paper_fill_account_seq UNIQUE (account_id, seq)
    )
    """,
    "CREATE INDEX IF NOT EXISTS paper_fill_account_ts ON paper_fill (account_id, ts)",
    """
    CREATE TABLE IF NOT EXISTS paper_order (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      order_id TEXT NOT NULL,
      status TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_paper_order_account_order UNIQUE (account_id, order_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_position (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      symbol TEXT NOT NULL,
      asset_type TEXT NOT NULL,
      qty BIGINT NOT NULL,
      avg_cost NUMERIC(18,6) NOT NULL,
      available BIGINT NOT NULL,
      lots JSONB NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_paper_position_account_symbol UNIQUE (account_id, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_auto_rule (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      rule_id TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_paper_auto_rule_account_rule UNIQUE (account_id, rule_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_nav (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      day DATE NOT NULL,
      payload JSONB NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_paper_nav_account_day UNIQUE (account_id, day)
    )
    """,
)

# 表/字段注释（DES-005 §3；随 DDL 一并建，幂等）。
_COMMENTS: tuple[str, ...] = (
    "COMMENT ON TABLE paper_account IS '模拟盘账户静态配置与当前现金；事务写入组在事务内定版 cash'",
    "COMMENT ON COLUMN paper_account.id IS '主键，UUID4 hex'",
    "COMMENT ON COLUMN paper_account.account_id IS '业务账户 ID，对外契约不变'",
    "COMMENT ON TABLE paper_fill IS '模拟盘成交台账，append-only 事件溯源；事务写入组内唯一新增行'",
    "COMMENT ON COLUMN paper_fill.payload IS '完整 fill 字段 JSON，重放唯一数据源'",
    "COMMENT ON COLUMN paper_fill.seq IS '账户内递增序号，行锁内 MAX(seq)+1 分配'",
    "COMMENT ON TABLE paper_order IS '模拟盘订单状态；事务写入组内更新 status/payload'",
    "COMMENT ON TABLE paper_position IS '模拟盘物化持仓，可由 paper_fill 重放；事务写入组内 UPSERT'",
    "COMMENT ON TABLE paper_auto_rule IS '模拟盘自动交易规则；直接建库，无历史数据'",
    "COMMENT ON TABLE paper_nav IS '模拟盘盘后净值历史；直接建库，无历史数据'",
)

_init_lock = threading.Lock()
_initialized = False


def _connect() -> psycopg.Connection[dict[str, Any]]:
    """按配置建立连接；row_factory=dict 让读路径直接拿 dict。"""
    return cast(
        "psycopg.Connection[dict[str, Any]]",
        psycopg.connect(settings.paper_pg_url, row_factory=dict_row),
    )


@contextmanager
def transaction() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    """单事务上下文：异常自动回滚，成功提交。默认 READ COMMITTED。"""
    with _connect() as conn:
        yield conn


def init_schema() -> None:
    """幂等建 6 表 + 索引 + 关键注释；进程内只执行一次。"""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        # 跨进程同步 DDL：多 worker 同时首次访问时避免 CREATE TABLE/COMMENT 互相
        # 等 ShareUpdateExclusiveLock 形成死锁；advisory lock 保证一次只有一个
        # 进程执行 DDL，其余进程等待后直接受益于幂等语句。
        with _connect() as conn:
            conn.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_LOCK_KEY,))
            try:
                for stmt in _DDL:
                    conn.execute(stmt)
                for stmt in _COMMENTS:
                    conn.execute(stmt)
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_LOCK_KEY,))
        _initialized = True


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _new_id() -> str:
    return uuid.uuid4().hex


def _row_to_account(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """DB 行 → 对外账户 dict（字段与文件版一致，不暴露 id/审计字段）。"""
    if row is None:
        return None
    return {
        "id": row["account_id"],
        "name": row["name"],
        "initial_cash": float(row["initial_cash"]),
        "cash": float(row["cash"]),
        "commission_pct": float(row["commission_pct"]),
        "stamp_tax_pct": float(row["stamp_tax_pct"]),
        "slippage_bps": int(row["slippage_bps"]),
        "queue_limit_orders": bool(row["queue_limit_orders"]),
        "status": row["status"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else "",
    }


def get_account(account_id: str) -> dict[str, Any] | None:
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM paper_account WHERE account_id = %s", (account_id,)
        ).fetchone()
    return _row_to_account(row)


def create_account(account_id: str, acc: dict[str, Any]) -> dict[str, Any]:
    """幂等创建：同 account_id 已存在则原样返回。acc 为对外 dict。"""
    init_schema()
    now = _now()
    with transaction() as conn:
        conn.execute("SELECT 1 FROM paper_account WHERE account_id = %s FOR UPDATE", (account_id,))
        row = conn.execute(
            "SELECT * FROM paper_account WHERE account_id = %s", (account_id,)
        ).fetchone()
        if row is not None:
            return _row_to_account(row)  # type: ignore[return-value]
        conn.execute(
            """INSERT INTO paper_account
               (id, account_id, name, initial_cash, cash, commission_pct, stamp_tax_pct,
                slippage_bps, queue_limit_orders, status, version,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                acc["name"],
                acc["initial_cash"],
                acc["cash"],
                acc["commission_pct"],
                acc["stamp_tax_pct"],
                acc["slippage_bps"],
                acc["queue_limit_orders"],
                acc["status"],
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )
    return acc


def save_account(account_id: str, acc: dict[str, Any]) -> None:
    init_schema()
    with _connect() as conn:
        conn.execute(
            """UPDATE paper_account SET name=%s, cash=%s, commission_pct=%s, stamp_tax_pct=%s,
               slippage_bps=%s, queue_limit_orders=%s, status=%s, version=version+1,
               updated_by=%s, updated_at=%s WHERE account_id=%s""",
            (
                acc["name"],
                acc["cash"],
                acc["commission_pct"],
                acc["stamp_tax_pct"],
                acc["slippage_bps"],
                acc["queue_limit_orders"],
                acc["status"],
                _SOURCE,
                _now(),
                account_id,
            ),
        )


def list_account_ids() -> list[str]:
    init_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT account_id FROM paper_account ORDER BY created_at, account_id"
        ).fetchall()
    return [row["account_id"] for row in rows]


def delete_account(account_id: str) -> bool:
    init_schema()
    with transaction() as conn:
        cur = conn.execute("DELETE FROM paper_account WHERE account_id = %s", (account_id,))
        if cur.rowcount == 0:
            return False
        for table in (
            "paper_fill",
            "paper_order",
            "paper_position",
            "paper_auto_rule",
            "paper_nav",
        ):
            conn.execute(f"DELETE FROM {table} WHERE account_id = %s", (account_id,))
    return True


def load_fills(account_id: str) -> list[dict[str, Any]]:
    """读全部 fill 的 payload（重放唯一数据源），按 seq 排序。"""
    init_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT payload FROM paper_fill WHERE account_id = %s ORDER BY seq", (account_id,)
        ).fetchall()
    return [
        json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        for row in rows
    ]


def load_positions(account_id: str) -> dict[str, dict[str, Any]]:
    init_schema()
    with _connect() as conn:
        rows = conn.execute(
            """SELECT symbol, asset_type, qty, avg_cost, available, lots
               FROM paper_position WHERE account_id = %s""",
            (account_id,),
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        lots = row["lots"]
        if isinstance(lots, str):
            lots = json.loads(lots)
        out[row["symbol"]] = {
            "asset_type": row["asset_type"],
            "qty": int(row["qty"]),
            "avg_cost": float(row["avg_cost"]),
            "available_qty": int(row["available"]),
            "lots": lots,
        }
    return out


def save_positions(account_id: str, positions: dict[str, dict[str, Any]]) -> None:
    init_schema()
    now = _now()
    with transaction() as conn:
        for symbol, pos in positions.items():
            conn.execute(
                """INSERT INTO paper_position
                   (id, account_id, symbol, asset_type, qty, avg_cost, available, lots,
                    created_by, created_at, updated_by, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (account_id, symbol) DO UPDATE SET
                     asset_type=EXCLUDED.asset_type, qty=EXCLUDED.qty,
                     avg_cost=EXCLUDED.avg_cost, available=EXCLUDED.available,
                     lots=EXCLUDED.lots, updated_by=EXCLUDED.updated_by,
                     updated_at=EXCLUDED.updated_at""",
                (
                    _new_id(),
                    account_id,
                    symbol,
                    pos.get("asset_type", "stock"),
                    int(pos.get("qty", 0)),
                    pos.get("avg_cost", 0),
                    int(pos.get("available", pos.get("available_qty", 0))),
                    json.dumps(pos.get("lots", []), ensure_ascii=False),
                    _SOURCE,
                    now,
                    _SOURCE,
                    now,
                ),
            )


def append_fill(account_id: str, fill: dict[str, Any]) -> None:
    """单笔 fill 追加（corp_action 等非成交写入）；seq 在行锁内分配。"""
    init_schema()
    now = _now()
    with transaction() as conn:
        conn.execute("SELECT 1 FROM paper_account WHERE account_id = %s FOR UPDATE", (account_id,))
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM paper_fill WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        seq = int(cast("dict[str, Any]", row)["next_seq"])
        payload = {**fill, "seq": seq}
        conn.execute(
            """INSERT INTO paper_fill
               (id, account_id, seq, ts, date, order_id, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                seq,
                fill.get("ts") or now,
                fill.get("date") or now.date(),
                fill.get("order_id") or "",
                json.dumps(payload, ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def load_orders(account_id: str) -> dict[str, dict[str, Any]]:
    init_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT order_id, payload FROM paper_order WHERE account_id = %s", (account_id,)
        ).fetchall()
    return {
        row["order_id"]: (
            json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        )
        for row in rows
    }


def get_order(account_id: str, order_id: str) -> dict[str, Any] | None:
    init_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT payload FROM paper_order WHERE account_id = %s AND order_id = %s",
            (account_id, order_id),
        ).fetchone()
    if row is None:
        return None
    return json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]


def save_order(account_id: str, order: dict[str, Any]) -> None:
    init_schema()
    now = _now()
    with _connect() as conn:
        conn.execute(
            """INSERT INTO paper_order
               (id, account_id, order_id, status, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (account_id, order_id) DO UPDATE SET
                 status=EXCLUDED.status, payload=EXCLUDED.payload,
                 updated_by=EXCLUDED.updated_by, updated_at=EXCLUDED.updated_at""",
            (
                _new_id(),
                account_id,
                order["id"],
                order.get("status", ""),
                json.dumps(order, ensure_ascii=False),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def load_nav(account_id: str) -> list[dict[str, Any]]:
    init_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT payload FROM paper_nav WHERE account_id = %s ORDER BY day", (account_id,)
        ).fetchall()
    return [
        json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        for row in rows
    ]


def append_nav(account_id: str, day: str, payload: dict[str, Any]) -> None:
    init_schema()
    now = _now()
    with _connect() as conn:
        conn.execute(
            """INSERT INTO paper_nav
               (id, account_id, day, payload, created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (account_id, day) DO UPDATE SET
                 payload=EXCLUDED.payload, updated_by=EXCLUDED.updated_by,
                 updated_at=EXCLUDED.updated_at""",
            (
                _new_id(),
                account_id,
                day,
                json.dumps(payload, ensure_ascii=False),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def load_auto_rules(account_id: str, enabled_only: bool = False) -> dict[str, dict[str, Any]]:
    init_schema()
    sql = "SELECT rule_id, payload FROM paper_auto_rule WHERE account_id = %s"
    params: tuple[Any, ...] = (account_id,)
    if enabled_only:
        sql += " AND payload->>'enabled' = 'true'"
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {
        row["rule_id"]: (
            json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        )
        for row in rows
    }


def save_auto_rule(account_id: str, rule: dict[str, Any]) -> None:
    init_schema()
    now = _now()
    with _connect() as conn:
        conn.execute(
            """INSERT INTO paper_auto_rule
               (id, account_id, rule_id, payload, created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (account_id, rule_id) DO UPDATE SET
                 payload=EXCLUDED.payload, updated_by=EXCLUDED.updated_by,
                 updated_at=EXCLUDED.updated_at""",
            (
                _new_id(),
                account_id,
                rule["id"],
                json.dumps(rule, ensure_ascii=False),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def delete_auto_rule(account_id: str, rule_id: str) -> bool:
    init_schema()
    with _connect() as conn:
        cur = conn.execute(
            "DELETE FROM paper_auto_rule WHERE account_id = %s AND rule_id = %s",
            (account_id, rule_id),
        )
    return cur.rowcount > 0


def record_fill_transaction(
    account_id: str,
    order: dict[str, Any],
    fill: dict[str, Any],
    positions: dict[str, dict[str, Any]] | None = None,
    cash: float | None = None,
    *,
    replay_fn: Callable[[list[dict[str, Any]]], tuple[dict[str, dict[str, Any]], float]]
    | None = None,
    source: str = _SOURCE,
) -> float:
    """一次成交 = 一个事务（DES-005 §4）。

    account 行锁 → 锁内 MAX(seq)+1 → 锁内重放 → 记 fill/order/cash/positions → 提交。
    `replay_fn` 在行锁内以「已提交台账 + 本次 fill」重算 cash/positions，这是真实
    多 worker  correctness 的前提：若在事务外用旧台账各自重算，后提交者会用旧
    快照覆盖先提交者的现金，造成账目分叉。不传时沿用调用方算好的 positions/cash
    （兼容直接调用与简单测试）。返回本次定版 cash。
    """
    if replay_fn is None and (positions is None or cash is None):
        raise ValueError("replay_fn 与 positions/cash 必须提供其一")
    init_schema()
    now = _now()
    with transaction() as conn:
        # 1. account 粒度行锁，串行化同账户写。
        conn.execute("SELECT 1 FROM paper_account WHERE account_id = %s FOR UPDATE", (account_id,))
        # 2. 锁内分配账户内递增 seq。
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM paper_fill WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        seq = int(cast("dict[str, Any]", row)["next_seq"])
        fill_payload = {**fill, "seq": seq}
        if replay_fn is not None:
            rows = conn.execute(
                "SELECT payload FROM paper_fill WHERE account_id = %s ORDER BY seq", (account_id,)
            ).fetchall()
            committed = [_jsonb(r["payload"]) for r in rows]
            positions, cash = replay_fn([*committed, fill_payload])
        assert positions is not None and cash is not None
        _write_fill_transaction_rows(
            conn, account_id, order, fill_payload, positions, cash, seq, now, source
        )
    return cash


def _upsert_paper_positions(
    conn: psycopg.Connection[dict[str, Any]],
    account_id: str,
    positions: dict[str, dict[str, Any]],
    source: str,
    now: datetime,
) -> None:
    """物化持仓 UPSERT（步骤 6）。"""
    for symbol, pos in positions.items():
        conn.execute(
            """INSERT INTO paper_position
               (id, account_id, symbol, asset_type, qty, avg_cost, available, lots,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (account_id, symbol) DO UPDATE SET
                 asset_type=EXCLUDED.asset_type, qty=EXCLUDED.qty,
                 avg_cost=EXCLUDED.avg_cost, available=EXCLUDED.available,
                 lots=EXCLUDED.lots, updated_by=EXCLUDED.updated_by,
                 updated_at=EXCLUDED.updated_at""",
            (
                _new_id(),
                account_id,
                symbol,
                pos.get("asset_type", "stock"),
                int(pos.get("qty", 0)),
                pos.get("avg_cost", 0),
                int(pos.get("available", pos.get("available_qty", 0))),
                json.dumps(pos.get("lots", []), ensure_ascii=False, default=str),
                source,
                now,
                source,
                now,
            ),
        )


def _write_fill_transaction_rows(
    conn: psycopg.Connection[dict[str, Any]],
    account_id: str,
    order: dict[str, Any],
    fill_payload: dict[str, Any],
    positions: dict[str, dict[str, Any]],
    cash: float,
    seq: int,
    now: datetime,
    source: str,
) -> None:
    """事务内写 6 步：fill / order / 现金 / 持仓物化（调用方已持 account 行锁）。"""
    # 3. 记成交（append-only）。
    conn.execute(
        """INSERT INTO paper_fill
           (id, account_id, seq, ts, date, order_id, payload,
            created_by, created_at, updated_by, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            _new_id(),
            account_id,
            seq,
            fill_payload.get("ts") or now,
            fill_payload.get("date") or now.date(),
            fill_payload.get("order_id") or order.get("id") or "",
            json.dumps(fill_payload, ensure_ascii=False, default=str),
            source,
            now,
            source,
            now,
        ),
    )
    # 4. 改订单状态。
    conn.execute(
        """INSERT INTO paper_order
           (id, account_id, order_id, status, payload,
            created_by, created_at, updated_by, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (account_id, order_id) DO UPDATE SET
             status=EXCLUDED.status, payload=EXCLUDED.payload,
             updated_by=EXCLUDED.updated_by, updated_at=EXCLUDED.updated_at""",
        (
            _new_id(),
            account_id,
            order["id"],
            order.get("status", "filled"),
            json.dumps(order, ensure_ascii=False, default=str),
            source,
            now,
            source,
            now,
        ),
    )
    # 5. 定版现金 + 版本号递增。
    conn.execute(
        """UPDATE paper_account SET cash=%s, version=version+1,
           updated_by=%s, updated_at=%s WHERE account_id=%s""",
        (cash, source, now, account_id),
    )
    # 6. 物化持仓 UPSERT。
    _upsert_paper_positions(conn, account_id, positions, source, now)


# ── 配置备份用：账户快照导出 / 幂等恢复 ──────────────────────────
_SNAPSHOT_FORMAT = "paper-account-snapshot"
_SNAPSHOT_VERSION = 1


def _jsonb(value: Any) -> Any:
    """psycopg 未注册 JSONB loader 时按 str 返回，兼容两种形态。"""
    return json.loads(value) if isinstance(value, str) else value


def dump_account(account_id: str) -> dict[str, Any] | None:
    """导出账户 6 表完整快照（配置备份用）；账户不存在返回 None。

    fill payload 含原始 seq，恢复时按原 seq 写回，台账顺序与重放结果不变。
    """
    init_schema()
    with _connect() as conn:
        account = conn.execute(
            "SELECT * FROM paper_account WHERE account_id = %s", (account_id,)
        ).fetchone()
        if account is None:
            return None
        fills = conn.execute(
            "SELECT payload FROM paper_fill WHERE account_id = %s ORDER BY seq", (account_id,)
        ).fetchall()
        orders = conn.execute(
            "SELECT payload FROM paper_order WHERE account_id = %s ORDER BY order_id", (account_id,)
        ).fetchall()
        nav = conn.execute(
            "SELECT payload FROM paper_nav WHERE account_id = %s ORDER BY day", (account_id,)
        ).fetchall()
        rules = conn.execute(
            "SELECT payload FROM paper_auto_rule WHERE account_id = %s ORDER BY rule_id",
            (account_id,),
        ).fetchall()
        positions = conn.execute(
            """SELECT symbol, asset_type, qty, avg_cost, available, lots
               FROM paper_position WHERE account_id = %s ORDER BY symbol""",
            (account_id,),
        ).fetchall()
    return {
        "format": _SNAPSHOT_FORMAT,
        "version": _SNAPSHOT_VERSION,
        "account_id": account_id,
        "account": _row_to_account(account),
        "fills": [_jsonb(r["payload"]) for r in fills],
        "orders": [_jsonb(r["payload"]) for r in orders],
        "nav": [_jsonb(r["payload"]) for r in nav],
        "rules": [_jsonb(r["payload"]) for r in rules],
        "positions": [
            {
                "symbol": r["symbol"],
                "asset_type": r["asset_type"],
                "qty": int(r["qty"]),
                "avg_cost": float(r["avg_cost"]),
                "available_qty": int(r["available"]),
                "lots": _jsonb(r["lots"]),
            }
            for r in positions
        ],
    }


def _restore_fill_rows(conn, account_id: str, now: datetime, rows: list[dict]) -> None:
    for fill in rows:
        conn.execute(
            """INSERT INTO paper_fill
               (id, account_id, seq, ts, date, order_id, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                int(fill["seq"]),
                fill.get("ts") or now,
                fill.get("date") or now.date(),
                fill.get("order_id") or "",
                json.dumps(fill, ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def _restore_order_rows(conn, account_id: str, now: datetime, rows: list[dict]) -> None:
    for order in rows:
        conn.execute(
            """INSERT INTO paper_order
               (id, account_id, order_id, status, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                order["id"],
                order.get("status", ""),
                json.dumps(order, ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def _restore_position_rows(conn, account_id: str, now: datetime, rows: list[dict]) -> None:
    for pos in rows:
        conn.execute(
            """INSERT INTO paper_position
               (id, account_id, symbol, asset_type, qty, avg_cost, available, lots,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                pos["symbol"],
                pos.get("asset_type", "stock"),
                int(pos.get("qty", 0)),
                pos.get("avg_cost", 0),
                int(pos.get("available", pos.get("available_qty", 0))),
                json.dumps(pos.get("lots", []), ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def _restore_rule_rows(conn, account_id: str, now: datetime, rows: list[dict]) -> None:
    for rule in rows:
        conn.execute(
            """INSERT INTO paper_auto_rule
               (id, account_id, rule_id, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                rule["id"],
                json.dumps(rule, ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def _restore_nav_rows(conn, account_id: str, now: datetime, rows: list[dict]) -> None:
    for item in rows:
        conn.execute(
            """INSERT INTO paper_nav
               (id, account_id, day, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                item["date"],
                json.dumps(item, ensure_ascii=False, default=str),
                _SOURCE,
                now,
                _SOURCE,
                now,
            ),
        )


def _restore_snapshot_rows(
    conn: psycopg.Connection[dict[str, Any]],
    account_id: str,
    now: datetime,
    snapshot: dict[str, Any],
) -> None:
    """事务内恢复 5 张子表；调用方已删旧行并持有 account 行锁。"""
    _restore_fill_rows(conn, account_id, now, snapshot.get("fills", []))
    _restore_order_rows(conn, account_id, now, snapshot.get("orders", []))
    _restore_position_rows(conn, account_id, now, snapshot.get("positions", []))
    _restore_rule_rows(conn, account_id, now, snapshot.get("rules", []))
    _restore_nav_rows(conn, account_id, now, snapshot.get("nav", []))


def restore_account(account_id: str, snapshot: dict[str, Any]) -> None:
    """从快照整体恢复账户：事务内先删后插，重复执行结果一致。

    账户及 5 张子表是一个一致性整体，不允许逐文件半恢复；调用方按账户判冲突。
    """
    if snapshot.get("format") != _SNAPSHOT_FORMAT:
        raise ValueError("不支持的模拟盘账户快照格式")
    if snapshot.get("version") != _SNAPSHOT_VERSION:
        raise ValueError(f"不支持的模拟盘账户快照版本 {snapshot.get('version')!r}")
    if snapshot.get("account_id") != account_id:
        raise ValueError("快照账户 ID 与导入路径不一致")
    account = snapshot["account"]
    for key in (
        "name",
        "initial_cash",
        "cash",
        "commission_pct",
        "stamp_tax_pct",
        "slippage_bps",
        "queue_limit_orders",
        "status",
    ):
        if key not in account:
            raise ValueError(f"快照账户缺少字段 {key}")
    init_schema()
    now = _now()
    # 快照恢复保留账户原始创建时间（审计链连续）；损坏的时间戳降级为当前时间。
    created_at = account.get("created_at")
    try:
        created = datetime.fromisoformat(created_at) if created_at else now
    except (TypeError, ValueError):
        created = now
    with transaction() as conn:
        conn.execute("SELECT 1 FROM paper_account WHERE account_id = %s FOR UPDATE", (account_id,))
        for table in (
            "paper_fill",
            "paper_order",
            "paper_position",
            "paper_auto_rule",
            "paper_nav",
            "paper_account",
        ):
            conn.execute(f"DELETE FROM {table} WHERE account_id = %s", (account_id,))
        conn.execute(
            """INSERT INTO paper_account
               (id, account_id, name, initial_cash, cash, commission_pct, stamp_tax_pct,
                slippage_bps, queue_limit_orders, status, version,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s)""",
            (
                _new_id(),
                account_id,
                account["name"],
                account["initial_cash"],
                account["cash"],
                account["commission_pct"],
                account["stamp_tax_pct"],
                account["slippage_bps"],
                account["queue_limit_orders"],
                account["status"],
                _SOURCE,
                created,
                _SOURCE,
                now,
            ),
        )
        _restore_snapshot_rows(conn, account_id, now, snapshot)
