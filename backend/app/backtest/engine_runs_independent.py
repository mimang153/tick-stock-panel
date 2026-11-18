from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    MarketMatrix,
    MatcherConfig,
    SimResult,
    SimulationOptions,
    TradeRecord,
    _PanelSignalArrays,
    contextlib,
    np,
)


class _IndependentMatrixRun(_root._IndependentMatrixRunRiskMixin):
    """矩阵独立候选撮合状态机: 每个买入信号为一个独立样本。"""

    def __init__(
        self,
        engine,
        matrix: MarketMatrix,
        raw_candidates: int,
        config: MatcherConfig,
        options: SimulationOptions,
        progress_cb,
        cancel_event,
    ) -> None:
        self.engine = engine
        self.matrix = matrix
        self.raw_candidates = raw_candidates
        self.config = config
        self.options = options
        self.progress_cb = progress_cb
        self.cancel_event = cancel_event
        self.entry_prices = engine._resolve_entry_prices(matrix, config)
        self.exit_prices = matrix.open if config.exit_fill == "open_t+1" else matrix.close
        self.buy_cost_pct = config.buy_cost_pct()
        self.sell_cost_pct = config.sell_cost_pct()
        self.trades: list[TradeRecord] = []
        self.execution_stats = {
            "buy_invalid_price": 0,
            "buy_suspended": 0,
            "buy_limit_up": 0,
            "buy_score_filter": 0,
            "buy_no_next_bar": max(raw_candidates - int(matrix.entry.sum()), 0),
            "sell_invalid_price": 0,
            "sell_suspended": 0,
            "sell_limit_down": 0,
            "sell_no_future": 0,
            "pending_exit": 0,
        }
        self.minute_cache: dict = {}
        if config.minute_fill:
            trigger_times, trigger_assets = np.nonzero(matrix.entry | matrix.exit)
            dates = {matrix.timestamp_labels[int(t)][:10] for t in trigger_times}
            symbols = {matrix.symbols[int(a)] for a in trigger_assets}
            if dates and symbols:
                loaded = engine._load_minute_for_fills(
                    engine.repo,
                    list(symbols),
                    dates,
                    config.asset_type,
                    evidence=engine._evidence,
                )
                self.minute_cache = {
                    key: value
                    for key, value in loaded.items()
                    if value is not None and len(value) > 0
                }

    def _count(self, key: str) -> None:
        self.execution_stats[key] = self.execution_stats.get(key, 0) + 1

    @staticmethod
    def _valid_price(value) -> bool:
        return bool(np.isfinite(value) and value > 0)

    def _present(self, time_id: int, asset_id: int) -> bool:
        m = self.matrix
        return bool(
            np.isfinite(
                [
                    m.open[time_id, asset_id],
                    m.high[time_id, asset_id],
                    m.low[time_id, asset_id],
                    m.close[time_id, asset_id],
                ]
            ).any()
        )

    @staticmethod
    def _signal_id(code: int, ids: tuple[str, ...]) -> str | None:
        return ids[code] if 0 <= code < len(ids) else None

    def _signal_date(self, signal_time: int, fallback: str) -> str:
        labels = self.matrix.timestamp_labels
        return labels[signal_time][:10] if signal_time >= 0 else fallback

    def _minute_rows(self, time_id: int, asset_id: int):
        if not self.config.minute_fill or not self.minute_cache:
            return None
        key = (self.matrix.symbols[asset_id], self.matrix.timestamp_labels[time_id][:10])
        return self.minute_cache.get(key)

    def _refill(self, time_id: int, asset_id: int, side: str, daily_price: float) -> float:
        rows = self._minute_rows(time_id, asset_id)
        if rows is None:
            return daily_price
        reference = float(self.matrix.reference_price[time_id, asset_id])
        precise = self.engine._resolve_minute_fill(
            rows,
            reference if self._valid_price(reference) else None,
            side,
        )
        return precise if precise is not None else daily_price

    def _minute_trigger_price(self, time_id: int, asset_id: int) -> float | None:
        rows = self._minute_rows(time_id, asset_id)
        if rows is None:
            return None
        reference = float(self.matrix.reference_price[time_id, asset_id])
        return self.engine._resolve_minute_exit_trigger(
            rows,
            reference if self._valid_price(reference) else None,
        )

    def _one_price_limit(self, time_id: int, asset_id: int, direction: str) -> bool:
        m = self.matrix
        if not m.tradable[time_id, asset_id]:
            return False
        prices = [
            float(m.open[time_id, asset_id]),
            float(m.high[time_id, asset_id]),
            float(m.low[time_id, asset_id]),
            float(m.close[time_id, asset_id]),
        ]
        if not all(self._valid_price(value) for value in prices):
            return False
        same = max(prices) - min(prices) <= max(abs(prices[3]) * 1e-4, 0.01)
        flags = m.limit_up_locked if direction == "up" else m.limit_down_locked
        return bool(flags[time_id, asset_id]) and same

    def _intraday_entry(self, time_id: int, asset_id: int) -> bool:
        """该格建仓是否发生在盘中: 分钟逐格覆盖或 minute_fill 可用。粗粒度保守。"""
        m = self.matrix
        if m.entry_price is not None and bool(np.isfinite(m.entry_price[time_id, asset_id])):
            return True
        return self.config.minute_fill and bool(self.minute_cache)

    def _can_buy(self, time_id: int, asset_id: int) -> tuple[bool, str]:
        m = self.matrix
        if not m.tradable[time_id, asset_id]:
            return False, "buy_suspended"
        if not self._valid_price(self.entry_prices[time_id, asset_id]):
            return False, "buy_invalid_price"
        if self.config.entry_fill == "close_t" and bool(m.limit_up_locked[time_id, asset_id]):
            # close_t 按信号日收盘价成交: 收盘封板在收盘价的买队排队排不进, 一律拦截。
            return False, "buy_limit_up"
        if self._one_price_limit(time_id, asset_id, "up"):
            return False, "buy_limit_up"
        return True, ""

    def _can_sell(self, time_id: int, asset_id: int, override: float | None) -> tuple[bool, str]:
        m = self.matrix
        if not m.tradable[time_id, asset_id]:
            return False, "sell_suspended"
        price = override if override is not None else self.exit_prices[time_id, asset_id]
        if not self._valid_price(price):
            return False, "sell_invalid_price"
        if (
            override is None
            and self.config.exit_fill == "close_t"
            and bool(m.limit_down_locked[time_id, asset_id])
        ):
            # close_t 按当日收盘价卖出: 收盘封死跌停的卖队排队排不出去, 递延到下一可卖日。
            return False, "sell_limit_down"
        if self._one_price_limit(time_id, asset_id, "down"):
            return False, "sell_limit_down"
        return True, ""

    def _risk_exit(self, pos: dict, time_id: int, asset_id: int) -> tuple[str | None, float | None]:
        if pos.get("pending_exit_reason") or pos["entry_time"] == time_id:
            return None, None
        m = self.matrix
        cfg = self.config
        entry_price = float(pos["entry_price"])
        open_price = float(m.open[time_id, asset_id])
        low_price = float(m.low[time_id, asset_id])
        high_price = float(m.high[time_id, asset_id])
        peak_price = float(pos["max_high"])
        lines: list[tuple[float, str]] = []
        if cfg.stop_loss_pct is not None:
            lines.append((entry_price * (1 - abs(cfg.stop_loss_pct)), "stop_loss"))
        if cfg.trailing_stop_pct is not None:
            lines.append((peak_price * (1 - abs(cfg.trailing_stop_pct)), "trailing_stop"))
        activate = cfg.trailing_take_profit_activate_pct
        drawdown = cfg.trailing_take_profit_drawdown_pct
        if activate is not None and drawdown is not None and peak_price > entry_price:  # noqa: SIM102 - 独立前置条件
            if peak_price / entry_price - 1 >= abs(float(activate)):
                lines.append((peak_price * (1 - abs(float(drawdown))), "trailing_take_profit"))
        valid_lines = [(line, reason) for line, reason in lines if self._valid_price(line)]
        if valid_lines:
            stop_price, reason = max(valid_lines, key=lambda item: item[0])
            if self._valid_price(open_price) and open_price <= stop_price:
                return reason, open_price
            if self._valid_price(low_price) and low_price <= stop_price:
                return reason, stop_price
        if cfg.take_profit_pct is not None:
            take_profit = entry_price * (1 + abs(float(cfg.take_profit_pct)))
            if self._valid_price(open_price) and open_price >= take_profit:
                return "take_profit", open_price
            if self._valid_price(high_price) and high_price >= take_profit:
                return "take_profit", take_profit
        return None, None

    def _prepare_minute_exit_override(
        self,
        pos: dict,
        time_id: int,
        asset_id: int,
        reason: str,
        signal_date: str,
        signal_id: str | None,
    ) -> tuple[float | None, bool]:
        """分钟卖出触发价；返回 (override, allowed)，不可成交时已记计数。"""
        if pos.get("pending_exit_next_open"):
            open_price = float(self.matrix.open[time_id, asset_id])
            return (open_price if self._valid_price(open_price) else None), True
        override = self._minute_trigger_price(time_id, asset_id)
        if override is not None:
            return override, True
        if not pos.get("pending_exit_reason"):
            pos["pending_exit_reason"] = reason
            pos["pending_exit_signal_date"] = signal_date
            pos["pending_exit_signal_id"] = signal_id
            pos["pending_exit_next_open"] = True
            self._count("pending_exit")
        pos["blocked_exit_days"] += 1
        self._count("sell_minute_trigger_fallback")
        return None, False

    def _try_close(
        self,
        pos: dict,
        time_id: int,
        asset_id: int,
        reason: str,
        signal_date: str,
        override: float | None = None,
        force: bool = False,
    ) -> bool:
        m = self.matrix
        signal_id = (
            self._signal_id(int(m.exit_signal_code[time_id, asset_id]), m.exit_signal_ids)
            if reason == "signal"
            else None
        )
        minute_trigger = self.config.exit_fill == "signal_next_minute" and reason == "signal"
        if not force and minute_trigger and override is None:
            override, allowed = self._prepare_minute_exit_override(
                pos, time_id, asset_id, reason, signal_date, signal_id
            )
            if not allowed:
                return False
        ok, blocked = (True, "") if force else self._can_sell(time_id, asset_id, override)
        if not ok:
            if not pos.get("pending_exit_reason"):
                pos["pending_exit_reason"] = reason
                pos["pending_exit_signal_date"] = signal_date
                pos["pending_exit_signal_id"] = signal_id
                self._count("pending_exit")
            if minute_trigger:
                pos["pending_exit_next_open"] = True
            pos["blocked_exit_days"] += 1
            self._count(blocked)
            return False
        exit_price = (
            float(override)
            if override is not None
            else self._refill(time_id, asset_id, "sell", float(self.exit_prices[time_id, asset_id]))
        )
        shares = 100.0
        entry_value = shares * pos["entry_price"] * (1 + self.buy_cost_pct)
        exit_value = shares * exit_price * (1 - self.sell_cost_pct)
        pnl_amount = exit_value - entry_value
        self.trades.append(
            TradeRecord(
                symbol=m.symbols[asset_id],
                name=m.names[asset_id],
                entry_date=pos["entry_date"],
                exit_date=m.timestamp_labels[time_id][:10],  # type: ignore[arg-type]
                entry_price=round(float(pos["entry_price"]), 4),
                exit_price=round(exit_price, 4),
                pnl_pct=round(float(pnl_amount / entry_value), 6) if entry_value > 0 else 0.0,
                duration=int(pos["hold_days"]),
                exit_reason=reason,
                shares=shares,
                lots=1.0,
                entry_value=round(float(entry_value), 2),
                exit_value=round(float(exit_value), 2),
                pnl_amount=round(float(pnl_amount), 2),
                entry_score=round(float(pos["entry_score"]), 2),
                entry_signal_date=pos["entry_signal_date"],
                exit_signal_date=signal_date,
                blocked_exit_days=int(pos["blocked_exit_days"]),
                entry_signal_id=pos["entry_signal_id"],
                exit_signal_id=(pos.get("pending_exit_signal_id") or signal_id)
                if reason == "signal"
                else None,
            )
        )
        return True

    def _make_position(
        self, time_id: int, asset_id: int, entry_price: float, score: float, entry_date: str
    ) -> dict:
        m = self.matrix
        # close_t 且收盘价成交: 当日 high 发生在建仓之前, 不得进入移动止损峰值。
        max_high = (
            entry_price
            if self.config.entry_fill == "close_t" and not self._intraday_entry(time_id, asset_id)
            else max(entry_price, float(m.high[time_id, asset_id]))
        )
        return {
            "entry_time": time_id,
            "entry_date": entry_date,
            "entry_signal_date": self._signal_date(
                int(m.entry_signal_time[time_id, asset_id]), entry_date
            ),
            "entry_signal_id": self._signal_id(
                int(m.entry_signal_code[time_id, asset_id]), m.entry_signal_ids
            ),
            "entry_price": entry_price,
            "entry_score": score,
            "hold_days": 0,
            "max_high": max_high,
            "pending_exit_reason": None,
            "pending_exit_signal_date": None,
            "pending_exit_signal_id": None,
            "pending_exit_next_open": False,
            "blocked_exit_days": 0,
        }

    def run(self) -> SimResult:
        m = self.matrix
        entry_times, entry_assets = np.nonzero(m.entry)
        order = np.lexsort((entry_times, entry_assets))
        for seq, order_pos in enumerate(order, start=1):
            time_id = int(entry_times[order_pos])
            asset_id = int(entry_assets[order_pos])
            if self.cancel_event is not None and self.cancel_event.is_set():
                break
            if self.progress_cb is not None and (seq == 1 or seq % 500 == 0):
                with contextlib.suppress(Exception):
                    self.progress_cb(
                        {
                            "day": seq,
                            "total": len(order),
                            "date": m.timestamp_labels[time_id][:10],
                            "equity": 0,
                        }
                    )
            self._run_entry(time_id, asset_id)
        result = self.engine._calc_independent_candidate_result(
            self.trades, self.raw_candidates, self.execution_stats, options=self.options
        )
        result.stats["market_matrix_shape"] = list(m.shape)
        result.stats["market_matrix_bytes"] = m.nbytes
        return result


def _panel_signal_arrays(panel, entries, exits, config) -> _PanelSignalArrays | None:
    """构建成交掩码与价格数组, 按成交口径右移信号并阻断跨标的 shift。"""
    n = len(panel)
    panel_dates = panel["date"].to_numpy()
    panel_symbols = panel["symbol"].to_numpy()
    ent = np.zeros(n, dtype=bool)
    ext = np.zeros(n, dtype=bool)
    if entries is not None and len(entries) == n:
        ent = entries.to_numpy().astype(bool)
    if exits is not None and len(exits) == n:
        ext = exits.to_numpy().astype(bool)
    if not ent.any():
        return None

    open_prices = panel["open"].to_numpy()
    close_prices = panel["close"].to_numpy()
    same_prev_symbol = np.zeros(n, dtype=bool)
    same_prev_symbol[1:] = panel_symbols[1:] == panel_symbols[:-1]
    entry_prices = open_prices if config.entry_fill == "open_t+1" else close_prices
    exit_prices = open_prices if config.exit_fill == "open_t+1" else close_prices
    if config.entry_fill == "open_t+1":
        ent_s = np.zeros(n, dtype=bool)
        ent_s[1:] = ent[:-1] & same_prev_symbol
        ent = ent_s
    if config.exit_fill == "open_t+1":
        ext_s = np.zeros(n, dtype=bool)
        ext_s[1:] = ext[:-1] & same_prev_symbol
        ext = ext_s
    return _PanelSignalArrays(
        panel_dates, panel_symbols, ent, ext, entry_prices, exit_prices, close_prices
    )


def _simulate_panel_symbol_trades(
    sym,
    sym_ent: np.ndarray,
    sym_ext: np.ndarray,
    sym_entry_prices: np.ndarray,
    sym_exit_prices: np.ndarray,
    sym_close: np.ndarray,
    sym_dates: np.ndarray,
    config: MatcherConfig,
) -> list[TradeRecord]:
    """单个标的的逐行撮合状态机 (止损 → 信号 → 最大持仓天数)。"""
    trades: list[TradeRecord] = []
    holding = False
    entry_idx = -1
    entry_price = 0.0
    hold_days = 0
    for i in range(len(sym_ent)):
        if not holding:
            if sym_ent[i]:
                holding = True
                entry_idx = i
                entry_price = float(sym_entry_prices[i])
                hold_days = 0
            continue
        hold_days += 1
        exit_triggered = False
        exit_reason = ""
        if config.stop_loss_pct is not None:
            pnl = (float(sym_close[i]) - entry_price) / entry_price
            if pnl <= -abs(config.stop_loss_pct):
                exit_triggered = True
                exit_reason = "stop_loss"
        if not exit_triggered and sym_ext[i]:
            exit_triggered = True
            exit_reason = "signal"
        if not exit_triggered and config.max_hold_days is not None:  # noqa: SIM102 - 独立前置条件
            if hold_days >= config.max_hold_days:
                exit_triggered = True
                exit_reason = "max_hold"
        if not exit_triggered:
            continue
        exit_price = float(sym_exit_prices[i])
        pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
        pnl_pct -= config.buy_cost_pct() + config.sell_cost_pct()
        e_date = sym_dates[entry_idx]
        x_date = sym_dates[i]
        trades.append(
            TradeRecord(
                symbol=str(sym),
                entry_date=e_date.item() if hasattr(e_date, "item") else e_date,
                exit_date=x_date.item() if hasattr(x_date, "item") else x_date,
                entry_price=round(entry_price, 4),
                exit_price=round(exit_price, 4),
                pnl_pct=round(pnl_pct, 6),
                duration=int(hold_days),
                exit_reason=exit_reason,
            )
        )
        holding = False
    return trades
