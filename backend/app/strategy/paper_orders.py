"""从 strategy/paper.py 抽取的 paper_orders 子模块（保持原公开 API）。

DES-005：订单读改写走 PostgreSQL（`app.storage.paper_pg`），`data_dir` 保留兼容签名。
"""

from __future__ import annotations

from pathlib import Path

from app.storage import paper_pg as store

DEFAULT_ACCOUNT_ID = "default"
DEFAULT_COMMISSION_PCT = 0.00025  # 双边佣金 万2.5
DEFAULT_SLIPPAGE_BPS = 5.0  # 滑点 (bps, 万分之5)
DEFAULT_STAMP_TAX_PCT = 0.001  # 印花税, 仅卖出
MIN_COMMISSION = 5.0  # 佣金最低 5 元


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.strategy.paper"), name)


_rootmod = _RootProxy()


def apply_slippage(price: float, side: str, slippage_bps: float) -> float:
    """买入向上滑、卖出向下滑 (对成交价不利方向)。"""
    adj = (
        price * (1 + slippage_bps / 10000.0)
        if side == "buy"
        else price * (1 - slippage_bps / 10000.0)
    )
    return round(max(adj, 0.01), 4)


def buy_fee(qty: int, price: float, commission_pct: float) -> float:
    return round(max(qty * price * commission_pct, MIN_COMMISSION), 2)


def sell_fee(qty: int, price: float, commission_pct: float, stamp_tax_pct: float) -> float:
    return round(max(qty * price * commission_pct, MIN_COMMISSION) + qty * price * stamp_tax_pct, 2)


def limit_pct(symbol: str, asset_type: str) -> float:
    """涨跌停幅度。V1 简化: ST 未识别 (无名称数据), 基金统一 10%。

    股票: 创业板 300/301/302 与科创板 688/689 → 20%; 北交所 43/83/87/92 开头 → 30%; 其余 10%。
    """
    if asset_type == "etf":
        return 0.10
    code = symbol.split(".")[0]
    if code.startswith(("300", "301", "302", "688", "689")):
        return 0.20
    if code.startswith(("43", "83", "87", "92")):
        return 0.30
    return 0.10


def limit_prices(prev_close: float, symbol: str, asset_type: str) -> tuple[float, float]:
    """(涨停价, 跌停价)。股票 2 位小数, 基金 3 位小数。"""
    pct = limit_pct(symbol, asset_type)
    digits = 3 if asset_type == "etf" else 2
    return round(prev_close * (1 + pct), digits), round(prev_close * (1 - pct), digits)


def normalize_qty(qty: int) -> int:
    """下取整到百股整数倍。"""
    return int(qty // _rootmod.LOT_SIZE) * _rootmod.LOT_SIZE


def qty_from_amount(amount: float, ref_price: float) -> int:
    """按金额估算数量: 金额 / 参考价 向下取整到百股 (未计费用, 手续费从现金扣)。"""
    if ref_price <= 0:
        return 0
    return normalize_qty(int(amount / ref_price))


def load_orders(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    return list(store.load_orders(account_id).values())


def save_order(data_dir: Path, order: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    store.save_order(account_id, order)


def get_order(data_dir: Path, order_id: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict | None:
    return store.get_order(account_id, order_id)


def _resolve_qty_and_validate(
    data_dir, symbol, side, acc, qty, amount, ref_price, account_id
) -> tuple[int | None, str | None]:
    """折算数量并校验资金/T+1可卖，返回 (qty, error)。"""
    if qty is None and amount is None:
        return None, "qty 与 amount 必须提供一个"
    if qty is None:
        if ref_price is None or ref_price <= 0:
            return None, "amount 模式需要参考价 ref_price"
        qty = qty_from_amount(float(amount), ref_price)  # type: ignore[arg-type]
        if qty <= 0:
            return None, f"金额不足以买一手 (参考价 {ref_price})"
    qty = int(qty)
    if qty <= 0 or qty % _rootmod.LOT_SIZE != 0:
        return None, f"数量必须是 {_rootmod.LOT_SIZE} 的整数倍"

    acc_cash = float(acc["cash"])
    if side == "buy":
        est_price = float(ref_price) if ref_price else 0.0
        if (
            est_price > 0
            and qty * est_price * (1 + acc["slippage_bps"] / 10000)
            + buy_fee(qty, est_price, acc["commission_pct"])
            > acc_cash
        ):
            return None, f"可用资金不足 (需约 {qty * est_price:.0f}, 可用 {acc_cash:.0f})"
    else:
        held = _rootmod.sell_capacity(data_dir, symbol, account_id)
        if held is None:
            return None, f"无 {symbol} 持仓, 不能卖出"
        available, pending_sell = held
        if qty > available:
            return None, f"可卖数量不足 (T+1): 可卖 {available}, 请求数量 {qty}"
        if pending_sell + qty > available:
            return None, (
                f"可卖数量不足 (T+1): 可卖 {available}, "
                f"已有待成交卖出 {pending_sell}, 请求数量 {qty}"
            )
    return qty, None


def create_order(
    data_dir: Path,
    symbol: str,
    side: str,
    *,
    account_id: str = DEFAULT_ACCOUNT_ID,
    qty: int | None = None,
    amount: float | None = None,
    order_type: str = "market",
    asset_type: str | None = None,
    ref_price: float | None = None,
    source: str = "manual",
) -> tuple[dict | None, str | None]:
    """创建订单并校验。qty 与 amount 二选一 (amount 按参考价折算百股)。

    order_type: market(即时, 盘中钩子撮合) / next_open / close。
    ETF 即时单自动转 next_open (盘中钩子只喂股票快照)。
    返回 (order, None) 或 (None, 错误信息)。
    """
    with _rootmod.PAPER_LOCK:
        acc = _rootmod.get_account(data_dir, account_id)
        if acc is None:
            raise ValueError("尚未创建模拟账户")
        if acc.get("status") != "active":
            return None, "账户已冻结, 拒绝新订单"
        if side not in ("buy", "sell"):
            return None, f"side 非法: {side!r}"
        if order_type not in ("market", "next_open", "close"):
            return None, f"order_type 非法: {order_type!r}"
        symbol = (symbol or "").strip()
        if not symbol:
            return None, "symbol 不能为空"
        if asset_type is None:
            asset_type = (
                "etf"
                if symbol.endswith((".SH", ".SZ"))
                and symbol.split(".")[0].startswith(("51", "56", "58", "15"))
                else "stock"
            )

        # ETF 即时单 → 次日开盘 (盘中钩子只喂股票快照)
        if order_type == "market" and asset_type == "etf":
            order_type = "next_open"

        qty, error = _resolve_qty_and_validate(
            data_dir, symbol, side, acc, qty, amount, ref_price, account_id
        )
        if error is not None:
            return None, error

        # 持仓标的数上限 (仅新开仓的买入; 加仓已有持仓不受限)
        positions = _rootmod.load_positions(data_dir, account_id)
        if (
            side == "buy"
            and symbol not in positions
            and len([p for p in positions.values() if p["qty"] > 0])
            >= _rootmod.MAX_POSITION_SYMBOLS
        ):
            return None, f"持仓标的数已达上限 {_rootmod.MAX_POSITION_SYMBOLS}, 不能再开新仓"

        order = {
            "id": _rootmod._new_id("order"),
            "symbol": symbol,
            "asset_type": asset_type,
            "side": side,
            "qty": qty,
            "order_type": order_type,
            "status": "pending",  # pending / filled / cancelled / expired
            "ref_price": float(ref_price) if ref_price else None,
            "postponed": 0,  # 顺延交易日计数 (停牌顺延 / 涨跌停排队共用)
            "source": source,  # manual / auto:{rule_id}
            "created_at": _rootmod._now_iso(),
            "filled_at": None,
            "fill_price": None,
            "fees": None,
            "reason": None,
        }
        save_order(data_dir, order, account_id)
        return order, None


def cancel_order(
    data_dir: Path, order_id: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> tuple[dict | None, str | None]:
    """撤销 pending 订单; 已成交/已过期不可撤。"""
    with _rootmod.PAPER_LOCK:
        order = get_order(data_dir, order_id, account_id)
        if order is None:
            return None, f"订单不存在: {order_id}"
        if order["status"] != "pending":
            return None, f"订单状态为 {order['status']}, 不可撤销"
        order["status"] = "cancelled"
        order["reason"] = "manual cancel"
        save_order(data_dir, order, account_id)
        return order, None
