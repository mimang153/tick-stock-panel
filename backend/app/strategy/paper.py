"""虚拟账户(模拟盘)域模块 — 虚拟资金 + 真实行情价格的模拟撮合, 不接券商不做实盘。

设计文档: docs/paper-trading-plan.md。与回测互补: 回测重放历史, 模拟盘实时向前。

账务正确性原则: fills.jsonl 成交台账是唯一事实源 (append-only), 持仓/现金/净值
全部可由台账重放推导; 修复一律以追加记录表达 (除权 corp_action / 冲正), 不改历史行。

价格口径: 全程不复权 raw 价 (与真实交易一致)。费用参数与回测引擎
(app/backtest/engine.py) 同名同默认值: commission_pct 双边佣金 / stamp_tax_pct
卖出印花税 / slippage_bps 滑点。

多账户 (V2): 数据目录 data/paper/accounts/{account_id}/, 账户间完全隔离
(订单/台账/持仓/净值/自动规则均按账户存放)。所有域函数带 account_id 参数
(默认 "default", 即迁移前的单账户)。旧版 data/paper/ 单账户布局首次访问时
自动迁移到 accounts/default/, 一次性且幂等。

撮合规则:
  - 即时单: 盘中由行情轮询钩子按最新快照价成交 (evaluate_intraday);
  - next_open / close 单: 盘后管道 settle_day 按当日 raw_open / raw_close 成交;
  - T+1: 当日买入次一交易日方可卖 (lots 按 buy_date 记账, available = date < today);
  - 涨跌停: 触及涨停的买单 / 跌停的卖单默认拒单 (expired 留痕); 账户开启
    queue_limit_orders 后转「排队次日重试」(转 next_open, 计顺延, 超限过期);
  - 停牌/缺行情: 顺延, 连续顺延超过 max_postpone 日自动过期;
  - 除权: 按 ex_factor 调整持仓数量(乘 factor)与成本(除以 factor), 台账记 corp_action;
    现金分红金额不在因子数据中, 不处理。
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from datetime import date as _date
from datetime import datetime
from datetime import time as _time
from pathlib import Path

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.parquet import scan_daily_parquet, scan_parquet_compat
from app.polars_guard import guarded_collect
from app.storage import paper_pg as store

# --- re-export：保持 import 路径与 monkeypatch 可观察性 ---
from .paper_fill import (
    _append_fill_alert as _append_fill_alert,
)
from .paper_fill import (
    _expire as _expire,
)
from .paper_fill import (
    _fill_check_cash as _fill_check_cash,
)
from .paper_fill import (
    _fill_check_sellable as _fill_check_sellable,
)
from .paper_fill import (
    _fill_event as _fill_event,
)
from .paper_fill import (
    _fill_limit_check as _fill_limit_check,
)
from .paper_fill import (
    _fill_order as _fill_order,
)
from .paper_fill import (
    _queue_or_expire as _queue_or_expire,
)
from .paper_fill import (
    _record_fill as _record_fill,
)
from .paper_fill import (
    day_fill_events as day_fill_events,
)
from .paper_fill import (
    evaluate_intraday as evaluate_intraday,
)
from .paper_orders import (
    _resolve_qty_and_validate as _resolve_qty_and_validate,
)
from .paper_orders import (
    apply_slippage as apply_slippage,
)
from .paper_orders import (
    buy_fee as buy_fee,
)
from .paper_orders import (
    cancel_order as cancel_order,
)
from .paper_orders import (
    create_order as create_order,
)
from .paper_orders import (
    get_order as get_order,
)
from .paper_orders import (
    limit_pct as limit_pct,
)
from .paper_orders import (
    limit_prices as limit_prices,
)
from .paper_orders import (
    load_orders as load_orders,
)
from .paper_orders import (
    normalize_qty as normalize_qty,
)
from .paper_orders import (
    qty_from_amount as qty_from_amount,
)
from .paper_orders import (
    save_order as save_order,
)
from .paper_orders import (
    sell_fee as sell_fee,
)
from .paper_positions import (
    _append_fill as _append_fill,
)
from .paper_positions import (
    _available_of as _available_of,
)
from .paper_positions import (
    _fills_path as _fills_path,
)
from .paper_positions import (
    _materialize as _materialize,
)
from .paper_positions import (
    load_fills as load_fills,
)
from .paper_positions import (
    load_positions as load_positions,
)
from .paper_positions import (
    rebuild_positions as rebuild_positions,
)
from .paper_positions import (
    replay_positions as replay_positions,
)
from .paper_positions import (
    sell_capacity as sell_capacity,
)

logger = logging.getLogger(__name__)

# 单进程内全部写操作互斥 (API 下单 / 行情钩子撮合 / 盘后结算共用一把锁)
PAPER_LOCK = threading.RLock()

# 费用默认值 — 与回测 engine.py 一致
DEFAULT_COMMISSION_PCT = 0.00025  # 双边佣金 万2.5
MIN_COMMISSION = 5.0  # 佣金最低 5 元
DEFAULT_STAMP_TAX_PCT = 0.001  # 印花税, 仅卖出
DEFAULT_SLIPPAGE_BPS = 5.0  # 滑点 (bps, 万分之5)

MAX_POSTPONE_DAYS = 3  # 顺延上限: 连续 N 个交易日无法成交自动过期
# 结算价格的打印时刻 (北京墙钟): 开盘价 09:30 / 收盘价 15:00。下单晚于该时刻的单
# 不能按当日这个价成交 (下单时价格已知), 留到下一交易日结算
_SESSION_OPEN = _time(9, 30)
_SESSION_CLOSE = _time(15, 0)
LOT_SIZE = 100  # 一手 100 股 (股票/ETF 同)
MAX_POSITION_SYMBOLS = 50  # 持仓标的数上限 (防误操作)
DEFAULT_ACCOUNT_ID = "default"
_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


# ── 账户 ID 校验 ────────────────────────────────────────
def validate_account_id(account_id: str) -> str:
    """账户 ID 只允许字母/数字/下划线/短横线 (防路径穿越), 非法抛 ValueError。"""
    if not isinstance(account_id, str) or not _ACCOUNT_ID_RE.match(account_id or ""):
        raise ValueError(f"账户 ID 非法 (仅限字母数字下划线短横线, ≤32 字符): {account_id!r}")
    return account_id


def _now_iso() -> str:
    return cn_now().isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _order_sort_key(order: dict) -> tuple:
    """撮合顺序: created_at 先到先得; 同刻则按 id (内含毫秒戳+随机后缀, 稳定)。"""
    return (order.get("created_at", ""), order.get("id", ""))


# ── 账户 ────────────────────────────────────────────────
def get_account(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict | None:
    return store.get_account(validate_account_id(account_id))


def create_account(
    data_dir: Path,
    initial_cash: float,
    *,
    account_id: str = DEFAULT_ACCOUNT_ID,
    name: str | None = None,
    commission_pct: float = DEFAULT_COMMISSION_PCT,
    stamp_tax_pct: float = DEFAULT_STAMP_TAX_PCT,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    queue_limit_orders: bool = False,
) -> dict:
    """创建账户 (同 id 已存在则原样返回, 不覆盖 — 幂等)。"""
    validate_account_id(account_id)
    with PAPER_LOCK:
        existing = get_account(data_dir, account_id)
        if existing is not None:
            return existing
        if (
            isinstance(initial_cash, bool)
            or not isinstance(initial_cash, int | float)
            or initial_cash <= 0
        ):
            raise ValueError("初始资金必须是正数")
        acc = {
            "id": account_id,
            "name": (name or "").strip() or account_id,
            "initial_cash": float(initial_cash),
            "cash": float(initial_cash),
            "commission_pct": float(commission_pct),
            "stamp_tax_pct": float(stamp_tax_pct),
            "slippage_bps": float(slippage_bps),
            "queue_limit_orders": bool(queue_limit_orders),
            "status": "active",  # active / frozen
            "created_at": _now_iso(),
        }
        return store.create_account(account_id, acc)


def save_account(data_dir: Path, acc: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    store.save_account(account_id, acc)


def list_account_ids(data_dir: Path) -> list[str]:
    """全部账户 id (按创建时间; 供钩子/结算遍历)。"""
    return store.list_account_ids()


def list_accounts(data_dir: Path) -> list[dict]:
    """账户列表 (带最新净值摘要, 供切换器展示)。"""
    out: list[dict] = []
    for aid in list_account_ids(data_dir):
        acc = get_account(data_dir, aid)
        if acc is None:
            continue
        nav = load_nav(data_dir, aid)
        out.append(
            {
                "id": acc["id"],
                "name": acc.get("name", acc["id"]),
                "status": acc.get("status", "active"),
                "initial_cash": acc.get("initial_cash"),
                "cash": acc.get("cash"),
                "latest_nav": nav[-1]["nav"] if nav else acc.get("cash"),
                "created_at": acc.get("created_at"),
            }
        )
    return out


def update_settings(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID, **fields) -> dict:
    """更新账户设置 (涨跌停排队 / 费用三参数); 未知字段忽略。返回更新后账户。

    费用只影响之后的新成交 (与回测费用模型同口径), 已有台账不重算。
    """
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            raise ValueError("尚未创建模拟账户")
        if "queue_limit_orders" in fields and fields["queue_limit_orders"] is not None:
            acc["queue_limit_orders"] = bool(fields["queue_limit_orders"])
        for key, lo, hi in (
            ("commission_pct", 0.0, 0.01),  # 佣金率 ≤1% (100‱)
            ("stamp_tax_pct", 0.0, 0.05),  # 印花税 ≤5% (仅卖出)
            ("slippage_bps", 0.0, 200.0),  # 滑点 ≤200bps
        ):
            v = fields.get(key)
            if v is None:
                continue
            v = float(v)
            if not (lo <= v <= hi):
                raise ValueError(f"{key} 超出合理范围 ({lo}~{hi})")
            acc[key] = v
        save_account(data_dir, acc, account_id)
        return acc


def delete_account(
    data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID, *, force: bool = False
) -> tuple[dict | None, str | None]:
    """删除账户及其全部数据 (台账/持仓/净值/规则, 不可恢复)。

    fail-closed: 账户仍有持仓标的或待成交订单时拒绝删除, 调用方显式 force=True
    才强删。目录删除限定在 accounts/{account_id} (id 校验只允许字母数字下划线
    短横线, 无路径穿越面)。返回 (统计, None) 或 (None, 拒绝原因)。
    """
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            return None, "账户不存在"
        positions = load_positions(data_dir, account_id)
        holdings = sum(1 for p in positions.values() if p["qty"] > 0)
        pending = sum(1 for o in load_orders(data_dir, account_id) if o["status"] == "pending")
        if (holdings or pending) and not force:
            detail = " 与 ".join(
                s
                for s in (
                    f"{holdings} 个持仓标的" if holdings else "",
                    f"{pending} 张待成交订单" if pending else "",
                )
                if s
            )
            return None, f"账户仍有 {detail}, 确认删除需 force=true"
        store.delete_account(account_id)
        return {"holdings": holdings, "pending_orders": pending}, None


# ── 费用 (纯函数) ───────────────────────────────────────


# ── 涨跌停 (纯函数, 金融口径须测试) ─────────────────────


# ── 数量 (纯函数) ───────────────────────────────────────


# ── 订单 ────────────────────────────────────────────────


# ── 成交台账与持仓 ──────────────────────────────────────


# ── 撮合 ────────────────────────────────────────────────
# ---- _fill_order 分段 helper (语义等价) ----


# ── 行情读取 (结算用, 与账户无关) ───────────────────────
def _daily_path(data_dir: Path, asset_type: str) -> Path:
    sub = "kline_etf_daily" if asset_type == "etf" else "kline_daily"
    return data_dir / sub


def read_daily_bar(data_dir: Path, symbol: str, asset_type: str, day: str) -> dict | None:
    """读某标的某日 raw OHLC (不复权); 无数据返回 None。"""
    base = _daily_path(data_dir, asset_type)
    if not base.exists():
        return None
    try:
        df = (
            scan_daily_parquet((base / "**" / "*.parquet").as_posix())
            .filter((pl.col("symbol") == symbol) & (pl.col("date") == _date.fromisoformat(day)))
            .select(["open", "close"])
            .pipe(guarded_collect, priority="interactive")
        )
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("paper read_daily_bar failed %s %s: %s", symbol, day, e)
        return None
    if df.is_empty():
        return None
    return {"open": float(df["open"][0]), "close": float(df["close"][0])}


def _prev_close(data_dir: Path, symbol: str, asset_type: str, day: str) -> float | None:
    """day 之前最近一个有数据的交易日 raw close (严格 < day, 按日期排序取最后)。"""
    base = _daily_path(data_dir, asset_type)
    if not base.exists():
        return None
    try:
        df = (
            scan_daily_parquet((base / "**" / "*.parquet").as_posix())
            .filter((pl.col("symbol") == symbol) & (pl.col("date") < _date.fromisoformat(day)))
            .sort("date")
            .select(pl.col("close").last())
            .pipe(guarded_collect, priority="interactive")
        )
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("paper prev_close failed %s: %s", symbol, e)
        return None
    if df.is_empty() or df["close"][0] is None:
        return None
    return float(df["close"][0])


def _index_close(data_dir: Path, day: str) -> float | None:
    """沪深300 当日 raw close (基准对比); 指数日K缺失返回 None。"""
    base = data_dir / "kline_index_daily"
    if not base.exists():
        return None
    try:
        df = (
            scan_daily_parquet((base / "**" / "*.parquet").as_posix())
            .filter(
                (pl.col("symbol") == "000300.SH") & (pl.col("date") == _date.fromisoformat(day))
            )
            .select("close")
            .pipe(guarded_collect, priority="interactive")
        )
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("paper index close read failed: %s", e)
        return None
    if df.is_empty() or df["close"][0] is None:
        return None
    return float(df["close"][0])


def _factor_on(data_dir: Path, symbol: str, asset_type: str, day: str) -> float | None:
    """symbol 在 day 的除权因子 (无当日事件返回 None)。CK 为唯一读源。"""
    from app.storage.runtime import get_storage

    etf = asset_type == "etf"
    target = _date.fromisoformat(day)
    runtime = get_storage()
    if runtime is not None:
        try:
            df = runtime.store.read_adj_factor(symbols=[symbol], start=target, end=target, etf=etf)
        except Exception as e:  # noqa: BLE001 - 参考价读取失败按缺价处理
            logger.warning("paper factor read failed %s: %s", symbol, e)
            return None
    else:
        # 离线脚本/单测（存储未初始化）回退本地因子文件
        sub = "adj_factor_etf" if etf else "adj_factor"
        p = data_dir / sub / "all.parquet"
        if not p.exists():
            return None
        try:
            df = (
                scan_parquet_compat(p.as_posix())
                .filter((pl.col("symbol") == symbol) & (pl.col("trade_date") == target))
                .select("ex_factor")
                .pipe(guarded_collect, priority="interactive")
            )
        except Exception as e:  # noqa: BLE001 - 参考价读取失败按缺价处理
            logger.warning("paper factor read failed %s: %s", symbol, e)
            return None
    if df.is_empty() or df["ex_factor"][0] is None:
        return None
    f = float(df["ex_factor"][0])
    return f if f > 0 else None


# ── 盘后结算 ────────────────────────────────────────────
def _placed_after_price_time(order: dict, day: str) -> bool:
    """订单 (盘中排队的按排队时刻) 是否晚于 day 结算价格的打印时刻。

    next_open 用当日 09:30 开盘价, close / market 兜底用当日 15:00 收盘价。下单或
    排队发生在同一交易日该时刻之后, 说明这个价格在下单时已经打印过, 按它成交等于
    拿已知价格回填 (次日开盘单的口径是次一交易日 raw_open), 应留到下一交易日结算。
    更早日期的订单不受影响; 时间戳缺失或无法解析时不拦截 (沿用原行为)。
    """
    stamp = order.get("queued_at") or order.get("created_at")
    try:
        ts = datetime.fromisoformat(str(stamp))
    except (TypeError, ValueError):
        return False
    if ts.tzinfo is not None:
        ts = ts.astimezone(CN_TZ)
    if ts.date().isoformat() != day:
        return False
    price_time = _SESSION_OPEN if order.get("order_type") == "next_open" else _SESSION_CLOSE
    return ts.time() >= price_time


def _apply_corp_actions(data_dir, positions, day, account_id) -> int:
    """按持仓应用当日除权因子，返回应用条数。"""
    applied = {
        (f["symbol"], f["date"])
        for f in load_fills(data_dir, account_id)
        if f.get("kind") == "corp_action"
    }
    applied_count = 0
    for symbol, pos in positions.items():
        if pos["qty"] <= 0:
            continue
        factor = _factor_on(data_dir, symbol, pos.get("asset_type", "stock"), day)
        if factor is None or factor == 1.0 or (symbol, day) in applied:
            continue
        before_qty, before_cost = pos["qty"], pos["avg_cost"]
        _append_fill(
            data_dir,
            {
                "seq": int(datetime.now().timestamp() * 1000),
                "ts": _now_iso(),
                "date": day,
                "order_id": None,
                "symbol": symbol,
                "asset_type": pos.get("asset_type", "stock"),
                "side": "corp_action",
                "kind": "corp_action",
                "factor": factor,
                "qty_before": before_qty,
                "cost_before": before_cost,
            },
            account_id,
        )
        applied_count += 1
    return applied_count


def settle_day(data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    """盘后管道阶段: 撮合顺延单 → 除权调整 → 定版净值。幂等 (重跑同日安全)。

    撮合顺序: close 单用 raw_close; next_open 单用 raw_open; 仍 pending 的
    market 单 (当日无快照) 也按 raw_close 兜底成交 — 避免停牌外无限顺延。
    当日开盘后才下 / 排队的 next_open 单, 以及收盘后才下的 close / market 单,
    当日不成交也不计顺延, 留到下一交易日 (见 _placed_after_price_time)。
    """
    summary = {"filled": 0, "expired": 0, "corp_actions": 0, "nav": None, "account": account_id}
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            return summary
        pending_orders = sorted(
            (o for o in load_orders(data_dir, account_id) if o["status"] == "pending"),
            key=_order_sort_key,
        )
        for order in pending_orders:
            bar = read_daily_bar(data_dir, order["symbol"], order.get("asset_type", "stock"), day)
            if bar is None:
                order["postponed"] = int(order.get("postponed", 0)) + 1
                if order["postponed"] > MAX_POSTPONE_DAYS:
                    order["status"] = "expired"
                    order["reason"] = f"连续 {MAX_POSTPONE_DAYS} 个交易日无行情, 自动过期"
                    save_order(data_dir, order, account_id)
                    summary["expired"] += 1  # type: ignore[operator]
                else:
                    save_order(data_dir, order, account_id)
                continue
            if _placed_after_price_time(order, day):
                continue  # 下单/排队时该价已打印, 留到下一交易日 (不计顺延)
            # next_open 用开盘价; close 与 market 兜底用收盘价
            raw = bar["open"] if order["order_type"] == "next_open" else bar["close"]
            before = order["status"]
            if _fill_order(data_dir, order, raw, day, account_id) is not None:
                summary["filled"] += 1  # type: ignore[operator]
            elif before == "pending" and order["status"] == "expired":
                summary["expired"] += 1  # type: ignore[operator]

        # 除权调整 (仅持仓标的; 幂等: 同一 symbol 同日只应用一次 — 重跑结算不二次乘因子)
        positions = load_positions(data_dir, account_id)
        summary["corp_actions"] = _apply_corp_actions(data_dir, positions, day, account_id)  # type: ignore[assignment]
        if summary["corp_actions"]:
            _materialize(data_dir, account_id)

        # 定版净值 (幂等: 重写当日行); 顺带记录沪深300 收盘作基准对比
        nav = daily_nav(data_dir, day, account_id=account_id)
        if nav is not None:
            benchmark = _index_close(data_dir, day)
            if benchmark is not None:
                nav["benchmark_close"] = benchmark
            _write_nav_line(data_dir, day, nav, account_id)
            summary["nav"] = nav  # type: ignore[assignment]
    return summary


# ── 净值与总览 ──────────────────────────────────────────
def latest_prices_from_daily(
    data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> dict[str, float]:
    """day 收盘后各持仓标的 raw_close (定版用)。"""
    out: dict[str, float] = {}
    for symbol, pos in load_positions(data_dir, account_id).items():
        if pos["qty"] <= 0:
            continue
        bar = read_daily_bar(data_dir, symbol, pos.get("asset_type", "stock"), day)
        if bar is not None:
            out[symbol] = bar["close"]
    return out


def daily_nav(
    data_dir: Path,
    day: str,
    price_map: dict[str, float] | None = None,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> dict | None:
    """某日定版净值 {date, cash, mv, nav}; 缺行情的持仓按成本计 (保守, 留痕调用方)。"""
    acc = get_account(data_dir, account_id)
    if acc is None:
        return None
    positions = load_positions(data_dir, account_id)
    prices = (
        price_map if price_map is not None else latest_prices_from_daily(data_dir, day, account_id)
    )
    missing = [s for s, p in positions.items() if p["qty"] > 0 and s not in prices]
    if missing:
        logger.warning("paper nav %s: 缺行情按成本计: %s", day, missing)
    mv = 0.0
    for symbol, pos in positions.items():
        if pos["qty"] <= 0:
            continue
        price = prices.get(symbol, pos["avg_cost"])
        mv += pos["qty"] * price
    return {
        "date": day,
        "cash": round(float(acc["cash"]), 2),
        "mv": round(mv, 2),
        "nav": round(float(acc["cash"]) + mv, 2),
    }


def _write_nav_line(
    data_dir: Path, day: str, nav: dict, account_id: str = DEFAULT_ACCOUNT_ID
) -> None:
    store.append_nav(account_id, day, nav)


def load_nav(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    return store.load_nav(account_id)


def overview(
    data_dir: Path, price_map: dict[str, float] | None = None, account_id: str = DEFAULT_ACCOUNT_ID
) -> dict:
    """总览 (盘中实时估算): 现金 + 持仓市值 + 估算净值。"""
    acc = get_account(data_dir, account_id)
    if acc is None:
        return {"initialized": False}
    positions = load_positions(data_dir, account_id)
    today = cn_today().isoformat()
    holdings = []
    mv = 0.0
    for symbol, pos in sorted(positions.items()):
        if pos["qty"] <= 0:
            continue
        price = (price_map or {}).get(symbol)
        last = price if price is not None else pos["avg_cost"]
        mv += pos["qty"] * last
        holdings.append(
            {
                "symbol": symbol,
                "asset_type": pos.get("asset_type", "stock"),
                "qty": pos["qty"],
                "avg_cost": pos["avg_cost"],
                "last_price": round(last, 4),
                "market_value": round(pos["qty"] * last, 2),
                "pnl": round(pos["qty"] * (last - pos["avg_cost"]), 2),
                "pnl_pct": round((last / pos["avg_cost"] - 1) * 100, 2) if pos["avg_cost"] else 0.0,
                "available_qty": _available_of(pos, today),
            }
        )
    return {
        "initialized": True,
        "account_id": account_id,
        "account_name": acc.get("name", account_id),
        "status": acc["status"],
        "queue_limit_orders": bool(acc.get("queue_limit_orders")),
        "cash": round(float(acc["cash"]), 2),
        "market_value": round(mv, 2),
        "total": round(float(acc["cash"]) + mv, 2),
        "total_pnl": round(float(acc["cash"]) + mv - float(acc["initial_cash"]), 2),
        "initial_cash": float(acc["initial_cash"]),
        "estimating": price_map is not None,
        "holdings": holdings,
    }


# ── 回合统计 (FIFO) ─────────────────────────────────────
def round_trips(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    """FIFO 配对的开→平回合: 入场价含买入费, 出场净额扣卖出费。"""
    fills = [f for f in load_fills(data_dir, account_id) if f.get("kind", "fill") == "fill"]
    lots: dict[str, list[dict]] = {}
    rounds: list[dict] = []
    for f in fills:
        symbol = f["symbol"]
        if f["side"] == "buy":
            lots.setdefault(symbol, []).append(
                {
                    "date": f["date"],
                    "qty": int(f["qty"]),
                    "cost": float(f["price"]) * int(f["qty"]) + float(f.get("fee", 0)),
                }
            )
            continue
        remain = int(f["qty"])
        proceeds_unit = float(f["price"]) - float(f.get("fee", 0)) / max(int(f["qty"]), 1)
        pool = lots.get(symbol, [])
        while remain > 0 and pool:
            lot = pool[0]
            take = min(lot["qty"], remain)
            cost_part = lot["cost"] * take / lot["qty"]
            pnl = proceeds_unit * take - cost_part
            rounds.append(
                {
                    "symbol": symbol,
                    "open_date": lot["date"],
                    "close_date": f["date"],
                    "qty": take,
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl / cost_part * 100, 2) if cost_part else 0.0,
                    "holding_days": _days_between(lot["date"], f["date"]),
                }
            )
            lot["qty"] -= take
            lot["cost"] -= cost_part
            if lot["qty"] <= 0:
                pool.pop(0)
            remain -= take
    return rounds


def max_drawdown(nav_values: list[float]) -> float | None:
    """定版净值序列的最大回撤 (0~1 小数; 空序列返回 None, 单点为 0)。"""
    peak: float | None = None
    mdd: float | None = None
    for v in nav_values:
        if v <= 0:
            continue
        peak = v if peak is None else max(peak, v)
        dd = (peak - v) / peak
        mdd = dd if mdd is None else max(mdd, dd)
    return mdd


def stats(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    """回合汇总: 胜率/盈亏比/平均持有天数/已实现盈亏/最大回撤(定版净值)。"""
    rounds = round_trips(data_dir, account_id)
    wins = [r for r in rounds if r["pnl"] > 0]
    losses = [r for r in rounds if r["pnl"] <= 0]
    avg_win = sum(r["pnl"] for r in wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(r["pnl"] for r in losses) / len(losses)) if losses else 0.0
    mdd = max_drawdown([n["nav"] for n in load_nav(data_dir, account_id)])
    return {
        "rounds": len(rounds),
        "win_rate": round(len(wins) / len(rounds) * 100, 2) if rounds else 0.0,
        "profit_loss_ratio": round(avg_win / avg_loss, 2) if avg_loss else None,
        "avg_holding_days": round(sum(r["holding_days"] for r in rounds) / len(rounds), 1)
        if rounds
        else 0.0,
        "realized_pnl": round(sum(r["pnl"] for r in rounds), 2),
        "max_drawdown": round(mdd * 100, 2) if mdd is not None else None,
    }


def _days_between(d1: str, d2: str) -> int:
    try:
        return (_date.fromisoformat(d2) - _date.fromisoformat(d1)).days
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return 0
