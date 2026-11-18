"""从 strategy/paper.py 抽取的 paper_fill 子模块（保持原公开 API）。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

DEFAULT_ACCOUNT_ID = "default"


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.strategy.paper"), name)


_rootmod = _RootProxy()


def _fill_limit_check(data_dir, symbol, asset_type, day, side, price, order, acc, account_id):
    # 涨跌停检查 (基于上一交易日 raw close; 无上日收盘则跳过检查 — 新股/数据缺失)
    prev = _rootmod._prev_close(data_dir, symbol, asset_type, day)
    if prev is not None:
        up, down = _rootmod.limit_prices(prev, symbol, asset_type)
        if side == "buy" and price >= up:
            _queue_or_expire(data_dir, order, acc, f"触及涨停 {up} 买不进 (模拟)", account_id)  # type: ignore[arg-type]
            return False
        if side == "sell" and price <= down:
            _queue_or_expire(data_dir, order, acc, f"触及跌停 {down} 卖不出 (模拟)", account_id)  # type: ignore[arg-type]
            return False
    return True


def _fill_check_cash(data_dir, order, acc, account_id, gross, fee):
    if gross + fee > float(acc["cash"]) + 1e-6:  # type: ignore[index]
        _expire(
            data_dir,
            order,
            f"资金不足 (需 {gross + fee:.2f}, 可用 {acc['cash']:.2f})",  # type: ignore[index]
            account_id,
        )
        return False
    # 资金占用兜底: 只计入 created_at 早于本单的其他 pending 买入 (先到先得,
    # 早单优先成交, 晚单不占早单的额度) — 防止多张 pending 买入各自通过预检、
    # 成交时逐张扣现金而穿透。
    earlier_cash = sum(
        int(o["qty"]) * float(o["ref_price"] or 0)
        for o in _rootmod.load_orders(data_dir, account_id)
        if o["status"] == "pending"
        and o["side"] == "buy"
        and o["id"] != order["id"]
        and _rootmod._order_sort_key(o) < _rootmod._order_sort_key(order)
    )
    if earlier_cash and gross + fee + earlier_cash > float(acc["cash"]) + 1e-6:  # type: ignore[index]
        _expire(
            data_dir,
            order,
            f"资金被更早的待成交买入单占用 (约 {earlier_cash:.0f}, 可用 {acc['cash']:.0f})",  # type: ignore[index]
            account_id,
        )
        return False
    return True


def _fill_check_sellable(data_dir, order, account_id, symbol, qty, day):
    pos = _rootmod.load_positions(data_dir, account_id).get(symbol)
    avail = _rootmod._available_of(pos, day) if pos else 0
    if pos is None or pos["qty"] < qty or avail < qty:
        _expire(data_dir, order, f"可卖数量不足 (T+1): 可卖 {avail}", account_id)
        return False
    # 超卖防护 (撮合侧兜底): 只计入 created_at 早于本单的其他 pending 卖出 —
    # 先到先得, 早单优先成交; 剩余额度不足则本单拒 (而非成交出负持仓)。
    pending_sell = sum(
        int(o["qty"])
        for o in _rootmod.load_orders(data_dir, account_id)
        if o["status"] == "pending"
        and o["side"] == "sell"
        and o["symbol"] == symbol
        and o["id"] != order["id"]
        and _rootmod._order_sort_key(o) < _rootmod._order_sort_key(order)
    )
    if pending_sell + qty > avail:
        _expire(
            data_dir,
            order,
            f"可卖数量被更早的待成交卖出单占用: 可卖 {avail}, 先到 {pending_sell}",
            account_id,
        )
        return False
    return True


def _record_fill(data_dir, order, fill, acc, account_id):
    _rootmod._append_fill(data_dir, fill, account_id)

    order["status"] = "filled"
    order["filled_at"] = fill["ts"]
    order["fill_price"] = fill["price"]
    order["fees"] = fill["fee"]
    _rootmod.save_order(data_dir, order, account_id)

    # 现金按台账重放结果定版 (重放是权威, 避免双写不一致)
    _cash = _rootmod.replay_positions(data_dir, account_id=account_id)[1]
    acc["cash"] = _cash  # type: ignore[index]
    _rootmod.save_account(data_dir, acc, account_id)  # type: ignore[arg-type]
    _rootmod._materialize(data_dir, account_id)


def _append_fill_alert(data_dir, symbol, side, qty, price, fee, account_id, acc):
    # 成交落告警记录 (监控中心触发历史可见; source=paper 走前端通用渲染)
    try:
        label = (
            "模拟盘"
            if account_id == DEFAULT_ACCOUNT_ID
            else f"模拟盘[{acc.get('name') or account_id}]"  # type: ignore[union-attr]
        )
        from app.services import alert_store

        alert_store.append_many(
            data_dir,
            [
                {
                    "ts": int(datetime.now().timestamp() * 1000),
                    "source": "paper",
                    "type": "paper_fill",
                    "rule_id": None,
                    "rule_name": "",
                    "strategy_id": None,
                    "symbol": symbol,
                    "name": "",
                    "message": f"{label}{'买入' if side == 'buy' else '卖出'} {symbol} {qty}股 @ {price:.3f} (费 {fee:.2f})",
                    "price": price,
                    "change_pct": 0,
                    "signals": [],
                    "severity": "info",
                    "conditions": [],
                    "logic": "and",
                }
            ],
        )
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("paper 成交告警落盘失败 (不影响账务): %s", e)


def _fill_order(
    data_dir: Path, order: dict, raw_price: float, day: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> dict | None:
    """按指定 raw 价成交一笔订单 (费用/滑点/资金与持仓校验), 返回 fill 或 None。

    调用方持锁。涨跌停按账户 queue_limit_orders 开关: 关 → expired 留痕;
    开 → 转 next_open 排队次日重试 (计顺延, 超限过期)。资金/可卖不足直接过期。
    """
    acc = _rootmod.get_account(data_dir, account_id)
    symbol, side, qty = order["symbol"], order["side"], int(order["qty"])
    asset_type = order.get("asset_type", "stock")
    price = _rootmod.apply_slippage(raw_price, side, float(acc["slippage_bps"]))  # type: ignore[index]

    if not _fill_limit_check(
        data_dir, symbol, asset_type, day, side, price, order, acc, account_id
    ):
        return None

    fee = (
        _rootmod.buy_fee(qty, price, acc["commission_pct"])  # type: ignore[index]
        if side == "buy"
        else _rootmod.sell_fee(qty, price, acc["commission_pct"], acc["stamp_tax_pct"])  # type: ignore[index]
    )
    gross = qty * price
    if side == "buy" and not _fill_check_cash(data_dir, order, acc, account_id, gross, fee):
        return None
    if side == "sell" and not _fill_check_sellable(data_dir, order, account_id, symbol, qty, day):
        return None

    fill = {
        "seq": int(datetime.now().timestamp() * 1000),
        "ts": _rootmod._now_iso(),
        "date": day,
        "order_id": order["id"],
        "symbol": symbol,
        "asset_type": asset_type,
        "side": side,
        "qty": qty,
        "price": price,
        "fee": fee,
        "kind": "fill",
    }
    _record_fill(data_dir, order, fill, acc, account_id)
    _append_fill_alert(data_dir, symbol, side, qty, price, fee, account_id, acc)

    _rootmod.logger.info("paper filled: %s %s %d x %.3f fee %.2f", side, symbol, qty, price, fee)
    return fill


def _expire(data_dir: Path, order: dict, reason: str, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    """拒单置 expired 并立即落盘 (撮合拒绝必须留痕, 不允许静默丢弃)。"""
    order["status"] = "expired"
    order["reason"] = reason
    _rootmod.save_order(data_dir, order, account_id)


def _queue_or_expire(
    data_dir: Path, order: dict, acc: dict, reason: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> None:
    """涨跌停拒单处理: 排队开启 → 转 next_open 次日重试 (计顺延, 超限过期); 关 → 过期。"""
    postponed = int(order.get("postponed", 0))
    if acc.get("queue_limit_orders"):
        if postponed < _rootmod.MAX_POSTPONE_DAYS:
            order["order_type"] = "next_open"
            order["postponed"] = postponed + 1
            order["reason"] = (
                f"{reason}; 排队次日重试 ({postponed + 1}/{_rootmod.MAX_POSTPONE_DAYS})"
            )
            # 排队时刻: 盘中排队的单不能按当日 (排队之前已打印的) 开盘价成交
            order["queued_at"] = _rootmod._now_iso()
            _rootmod.save_order(data_dir, order, account_id)
            return
        reason = f"{reason}; 排队 {_rootmod.MAX_POSTPONE_DAYS} 日未成交, 过期"
    _expire(data_dir, order, reason, account_id)


def day_fill_events(data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    """指定交易日的全部成交 → 推送事件。

    盘后结算路径 (daily_pipeline paper_settle) 用它把结算成交留痕到告警中心;
    盘中即时成交走 evaluate_intraday 返回值, 不经过本函数。
    """
    return [
        _fill_event(f, account_id)
        for f in _rootmod.load_fills(data_dir, account_id)
        if f.get("date") == day and f.get("kind") == "fill"
    ]


def _fill_event(fill: dict, account_id: str) -> dict:
    """成交台账行 → 推送事件。字段对齐监控告警 (AlertEvent), 供 SSE toast /
    语音播报 / 系统通知 / alert_store 留痕 / Webhook 复用, 前端无需新管道。
    ts 用 fill.seq (毫秒), 与 alert_store 的 ts 口径一致; 文案用中文可读措辞。
    """
    side_label = "买入" if fill["side"] == "buy" else "卖出"
    return {
        "source": "paper",
        "type": "fill",
        "severity": "info",
        "ts": int(fill["seq"]),
        "symbol": fill["symbol"],
        "side": fill["side"],
        "qty": fill["qty"],
        "price": fill["price"],
        "account_id": account_id,
        "message": f"模拟盘{side_label}成交 {fill['qty']}股 @ {fill['price']:.2f}",
    }


def evaluate_intraday(
    data_dir: Path, snapshot: dict[str, float], account_id: str = DEFAULT_ACCOUNT_ID
) -> list[dict]:
    """盘中钩子: 用最新快照价撮合 pending 即时单。返回本次成交事件列表。

    snapshot: {symbol: raw 最新价} (来自 enriched raw_close)。仅处理 market 单;
    调用方 (quote_service) 已保证交易时段。ETF 不在快照内自然顺延。
    事件由调用方广播 (SSE/语音/系统通知/留痕/Webhook), 域模块只产出不投递。
    """
    events: list[dict] = []
    with _rootmod.PAPER_LOCK:
        today = _rootmod.cn_today().isoformat()
        pending_orders = sorted(
            (
                o
                for o in _rootmod.load_orders(data_dir, account_id)
                if o["status"] == "pending" and o["order_type"] == "market"
            ),
            key=_rootmod._order_sort_key,
        )
        for order in pending_orders:
            price = snapshot.get(order["symbol"])
            if price is None or price <= 0:
                continue  # 无快照顺延
            fill = _fill_order(data_dir, order, float(price), today, account_id)
            if fill is not None:
                events.append(_fill_event(fill, account_id))
    return events
