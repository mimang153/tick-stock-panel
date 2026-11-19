"""DES-005: 模拟盘账户 PostgreSQL 事务存储层测试（连真实 PG 容器）。

用唯一 account_id 隔离，每个用例结束清理；不依赖文件系统布局。
PG 不可用时整文件 skip（CI/无容器环境不误判失败）。
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import psycopg
import pytest

from app.config import settings
from app.storage import paper_pg as pg


@pytest.fixture(autouse=True, scope="module")
def _require_pg() -> None:
    try:
        with psycopg.connect(settings.paper_pg_url, connect_timeout=2):
            pass
    except Exception as exc:  # noqa: BLE001 - 连通性探测需捕获所有异常以 skip
        pytest.skip(f"PostgreSQL 不可用: {exc}", allow_module_level=True)
    pg.init_schema()


@pytest.fixture()
def account_id() -> Iterator[str]:
    aid = f"t_{uuid.uuid4().hex[:12]}"
    yield aid
    pg.delete_account(aid)


def _acc(aid: str) -> dict:
    return {
        "id": aid,
        "name": aid,
        "initial_cash": 100000.0,
        "cash": 100000.0,
        "commission_pct": 0.00025,
        "stamp_tax_pct": 0.001,
        "slippage_bps": 5.0,
        "queue_limit_orders": False,
        "status": "active",
        "created_at": "2026-11-19T00:00:00+00:00",
    }


def _fill() -> dict:
    return {
        "ts": "2026-11-19T10:00:00+00:00",
        "date": "2026-11-19",
        "order_id": "ord1",
        "symbol": "000001",
        "asset_type": "stock",
        "side": "buy",
        "qty": 1000,
        "price": 10.0,
        "fee": 5.0,
        "kind": "fill",
    }


def _positions() -> dict:
    return {
        "000001": {
            "asset_type": "stock",
            "qty": 1000,
            "avg_cost": 10.0,
            "available": 0,
            "lots": [{"date": "2026-11-19", "qty": 1000}],
        }
    }


def test_create_get_and_idempotency(account_id: str) -> None:
    """create 幂等，get 返回对外字段（不暴露内部 id/审计字段）。"""
    acc = pg.create_account(account_id, _acc(account_id))
    assert acc["cash"] == 100000.0
    got = pg.get_account(account_id)
    assert got is not None
    assert got["id"] == account_id
    assert "version" not in got and "created_by" not in got
    assert pg.create_account(account_id, _acc(account_id))["cash"] == 100000.0


def test_save_account_and_list(account_id: str) -> None:
    """save 更新现金与版本，list 返回账户 id。"""
    pg.create_account(account_id, _acc(account_id))
    acc = pg.get_account(account_id)
    assert acc is not None
    acc["cash"] = 90000.0
    pg.save_account(account_id, acc)
    account = pg.get_account(account_id)
    assert account is not None
    assert account["cash"] == 90000.0
    assert account_id in pg.list_account_ids()


def test_order_crud(account_id: str) -> None:
    pg.create_account(account_id, _acc(account_id))
    pg.save_order(account_id, {"id": "ord1", "status": "pending", "symbol": "000001"})
    order = pg.get_order(account_id, "ord1")
    assert order is not None
    assert order["status"] == "pending"
    assert "ord1" in pg.load_orders(account_id)


def test_record_fill_transaction(account_id: str) -> None:
    """一次成交事务：现金定版 + seq 分配 + 持仓/订单落地。"""
    pg.create_account(account_id, _acc(account_id))
    pg.save_order(account_id, {"id": "ord1", "status": "pending", "symbol": "000001"})
    pg.record_fill_transaction(
        account_id, {"id": "ord1", "status": "filled"}, _fill(), _positions(), 90000.0
    )
    filled_order = pg.get_order(account_id, "ord1")
    assert filled_order is not None
    assert filled_order["status"] == "filled"
    account = pg.get_account(account_id)
    assert account is not None
    assert account["cash"] == 90000.0
    fills = pg.load_fills(account_id)
    assert len(fills) == 1 and fills[0]["seq"] == 1
    assert pg.load_positions(account_id)["000001"]["qty"] == 1000


def test_seq_increments_per_account(account_id: str) -> None:
    """同账户多笔成交 seq 递增，payload 保留 fill 全字段。"""
    pg.create_account(account_id, _acc(account_id))
    for i in range(3):
        fill = {**_fill(), "order_id": f"ord{i}"}
        pg.record_fill_transaction(
            account_id, {"id": f"ord{i}", "status": "filled"}, fill, _positions(), 90000.0
        )
    seqs = [f["seq"] for f in pg.load_fills(account_id)]
    assert seqs == [1, 2, 3]
    assert all(f["symbol"] == "000001" and "price" in f for f in pg.load_fills(account_id))


def test_nav_and_auto_rules(account_id: str) -> None:
    """NAV 按日 UPSERT，auto_rules 支持 enabled 过滤。"""
    pg.create_account(account_id, _acc(account_id))
    pg.append_nav(account_id, "2026-11-19", {"nav": 100000.0})
    pg.append_nav(account_id, "2026-11-19", {"nav": 101000.0})
    assert len(pg.load_nav(account_id)) == 1
    assert pg.load_nav(account_id)[0]["nav"] == 101000.0

    pg.save_auto_rule(account_id, {"id": "r1", "enabled": True})
    pg.save_auto_rule(account_id, {"id": "r2", "enabled": False})
    assert len(pg.load_auto_rules(account_id)) == 2
    assert len(pg.load_auto_rules(account_id, enabled_only=True)) == 1
    assert pg.delete_auto_rule(account_id, "r1") is True


def test_delete_account_cascades(account_id: str) -> None:
    """删账户级联清理 5 张子表；重复删返回 False。"""
    pg.create_account(account_id, _acc(account_id))
    pg.record_fill_transaction(
        account_id, {"id": "ord1", "status": "filled"}, _fill(), _positions(), 90000.0
    )
    pg.append_nav(account_id, "2026-11-19", {"nav": 1.0})
    assert pg.delete_account(account_id) is True
    assert pg.get_account(account_id) is None
    assert pg.load_fills(account_id) == []
    assert pg.load_nav(account_id) == []
    assert pg.delete_account(account_id) is False


def test_transaction_rollback(account_id: str) -> None:
    """事务上下文异常自动回滚，不留半写入。"""
    pg.create_account(account_id, _acc(account_id))
    with pytest.raises(RuntimeError, match="boom"), pg.transaction() as conn:
        conn.execute(
            """INSERT INTO paper_order
               (id, account_id, order_id, status, payload,
                created_by, created_at, updated_by, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                "x",
                account_id,
                "ord_rollback",
                "pending",
                "{}",
                "system",
                pg._now(),
                "system",
                pg._now(),
            ),
        )
        raise RuntimeError("boom")
    assert pg.get_order(account_id, "ord_rollback") is None


def _append_fills_worker(account_id: str, count: int) -> None:
    """子进程入口：每个进程独立连接 PG 追加台账，seq 由 account 行锁内分配。"""
    from app.storage import paper_pg as store

    for i in range(count):
        store.append_fill(
            account_id,
            {
                **_fill(),
                "order_id": f"ord_{i}",
            },
        )


def test_concurrent_append_seq_unique_and_contiguous(account_id: str) -> None:
    """多进程并发写：同账户 seq 无重复、无缺口（account 行锁串行化）。"""
    from concurrent.futures import ProcessPoolExecutor

    pg.create_account(account_id, _acc(account_id))
    workers, per_worker = 4, 5
    with ProcessPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_append_fills_worker, [account_id] * workers, [per_worker] * workers))
    fills = pg.load_fills(account_id)
    assert len(fills) == workers * per_worker
    assert sorted(f["seq"] for f in fills) == list(range(1, workers * per_worker + 1))


def _concurrent_fill_worker(account_id: str, data_dir_str: str, qty: int, price: float) -> float:
    """子进程入口：在事务内重放后成交（模拟两个 worker 并发撮合同一账户）。"""
    from pathlib import Path

    from app.storage import paper_pg as store
    from app.strategy import paper

    data_dir = Path(data_dir_str)

    def replay(fills):
        return paper.replay_positions(data_dir, fills=fills, account_id=account_id)

    return store.record_fill_transaction(
        account_id,
        {"id": f"ord_{qty}", "status": "filled"},
        {**_fill(), "qty": qty, "price": price, "order_id": f"ord_{qty}"},
        replay_fn=replay,
    )


def test_concurrent_fills_cash_not_overwritten(account_id: str, tmp_path) -> None:
    """多 worker 并发成交：后提交者基于已提交台账重算，现金=初始-两笔成本。

    修复前 cash/positions 在事务外算，两个 worker 各自基于空台账重算，后提交
    会覆盖先提交的现金 → 台账两笔但现金只扣一笔（账目分叉）。
    """
    from concurrent.futures import ProcessPoolExecutor

    from app.strategy import paper

    pg.create_account(account_id, _acc(account_id))
    with ProcessPoolExecutor(max_workers=2) as pool:
        list(
            pool.map(
                _concurrent_fill_worker,
                [account_id, account_id],
                [str(tmp_path), str(tmp_path)],
                [100, 200],
                [10.0, 12.0],
            )
        )
    fills = pg.load_fills(account_id)
    assert len(fills) == 2
    expected_cash = 100_000 - (100 * 10.0 + 5.0) - (200 * 12.0 + 5.0)
    account = pg.get_account(account_id)
    assert account is not None
    assert account["cash"] == pytest.approx(expected_cash)
    replayed, replay_cash = paper.replay_positions(tmp_path, account_id=account_id)
    assert replay_cash == pytest.approx(expected_cash)
    assert replayed["000001"]["qty"] == 300


def test_replay_matches_materialized_positions(account_id: str, tmp_path) -> None:
    """payload 重放结果与 PG 物化持仓/现金一致（唯一重放源）。"""
    from app.strategy import paper

    pg.create_account(account_id, _acc(account_id))
    buy = _fill()
    sell = {
        **_fill(),
        "side": "sell",
        "qty": 400,
        "price": 12.0,
        "fee": 6.0,
        "order_id": "ord_sell",
    }
    after_buy = {
        "000001": {
            "asset_type": "stock",
            "qty": 1000,
            "avg_cost": 10.005,
            "available": 0,
            "lots": [{"date": "2026-11-19", "qty": 1000}],
        }
    }
    after_sell = {
        "000001": {
            "asset_type": "stock",
            "qty": 600,
            "avg_cost": 10.005,
            "available": 0,
            "lots": [{"date": "2026-11-19", "qty": 600}],
        }
    }
    pg.record_fill_transaction(
        account_id, {"id": "ord1", "status": "filled"}, buy, after_buy, 89995.0
    )
    pg.record_fill_transaction(
        account_id, {"id": "ord_sell", "status": "filled"}, sell, after_sell, 94789.0
    )

    replayed, cash = paper.replay_positions(tmp_path, account_id=account_id)
    assert cash == 94789.0
    assert replayed["000001"]["qty"] == 600
    materialized = pg.load_positions(account_id)
    assert materialized["000001"]["qty"] == 600
    assert materialized["000001"]["avg_cost"] == pytest.approx(replayed["000001"]["avg_cost"])


def test_dump_restore_account_roundtrip(account_id: str, tmp_path) -> None:
    """配置备份快照：6 表整体导出/恢复，重复恢复幂等。"""
    from app.strategy import paper, paper_auto

    paper.create_account(tmp_path, 100_000, account_id=account_id)
    pg.append_fill(
        account_id,
        {
            "ts": "2026-11-19T10:00:00+08:00",
            "date": "2026-11-19",
            "order_id": "ord1",
            "symbol": "000001.SZ",
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 10.0,
            "fee": 5.0,
            "kind": "fill",
        },
    )
    pg.append_nav(account_id, "2026-11-19", {"date": "2026-11-19", "nav": 100_000.0})
    paper_auto.create_auto_rule(
        tmp_path,
        {
            "name": "规则",
            "match_kind": "strategy",
            "match_id": "s1",
            "side": "buy",
            "size_mode": "fixed_amount",
            "size_value": 1000,
        },
        account_id=account_id,
    )

    original = pg.get_account(account_id)
    snapshot = pg.dump_account(account_id)
    assert snapshot is not None
    assert snapshot["format"] == "paper-account-snapshot"
    assert len(snapshot["fills"]) == 1 and len(snapshot["rules"]) == 1

    assert pg.delete_account(account_id)
    assert pg.dump_account(account_id) is None
    pg.restore_account(account_id, snapshot)
    assert pg.get_account(account_id) == original
    assert pg.load_fills(account_id) == snapshot["fills"]
    assert len(pg.load_nav(account_id)) == 1
    assert len(pg.load_auto_rules(account_id)) == 1
    # 重复恢复不重复追加台账
    pg.restore_account(account_id, snapshot)
    assert len(pg.load_fills(account_id)) == 1


def test_restore_account_rejects_tampered_snapshot(account_id: str, tmp_path) -> None:
    """快照账户 ID / 格式被篡改时拒绝恢复，防止跨账户覆盖。"""
    from app.strategy import paper

    paper.create_account(tmp_path, 100_000, account_id=account_id)
    snapshot = pg.dump_account(account_id)
    assert snapshot is not None
    with pytest.raises(ValueError, match="不一致"):
        pg.restore_account("another_account", snapshot)
    with pytest.raises(ValueError, match="格式"):
        pg.restore_account(account_id, {**snapshot, "format": "bogus"})
