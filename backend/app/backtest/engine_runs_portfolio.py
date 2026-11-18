from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    MarketMatrix,
    MatcherConfig,
    SimResult,
    SimulationOptions,
    TradeRecord,
    _apply_position_policy,
    _matrix_entry_score,
    contextlib,
    logger,
    np,
    time,
)


class _MatrixPortfolioRun(_root._MatrixPortfolioRunRiskMixin):
    """矩阵账户级组合撮合状态机 (时间 x 标的不可变数组)。"""

    def __init__(
        self,
        engine,
        matrix: MarketMatrix,
        config: MatcherConfig,
        options: SimulationOptions,
        progress_cb,
        cancel_event,
    ) -> None:
        self.engine = engine
        self.matrix = matrix
        self.config = config
        self.options = options
        self.progress_cb = progress_cb
        self.cancel_event = cancel_event
        self.time_count, self.asset_count = matrix.shape
        self.entry_prices = engine._resolve_entry_prices(matrix, config)
        self.exit_prices = matrix.open if config.exit_fill == "open_t+1" else matrix.close
        self.buy_cost_pct = config.buy_cost_pct()
        self.sell_cost_pct = config.sell_cost_pct()
        self.cash = float(config.initial_capital)
        self.peak = self.cash
        self.max_positions = max(int(config.max_positions), 0)
        self.max_exposure_pct = min(max(float(config.max_exposure_pct), 0.0), 1.0)
        self.positions: dict[int, dict] = {}
        self.last_close = np.full(self.asset_count, np.nan, dtype=np.float64)
        self.trades: list[TradeRecord] = []
        self.equity_curve: list[dict] = []
        self.drawdown_curve: list[dict] = []
        self.equity_values: list[float] = []
        self.exposure_values: list[float] = []
        self.execution_stats = {
            "buy_invalid_price": 0,
            "buy_suspended": 0,
            "buy_limit_up": 0,
            "buy_no_slot": 0,
            "buy_cash": 0,
            "buy_lot_size": 0,
            "buy_same_day_reentry": 0,
            "buy_exposure": 0,
            "buy_score_filter": 0,
            "sell_invalid_price": 0,
            "sell_suspended": 0,
            "sell_limit_down": 0,
            "pending_exit": 0,
        }
        self.minute_cache: dict = {}
        if config.minute_fill:
            trigger_times, trigger_assets = np.nonzero(matrix.entry | matrix.exit)
            trigger_dates = {matrix.timestamp_labels[int(t)][:10] for t in trigger_times}
            trigger_symbols = {matrix.symbols[int(a)] for a in trigger_assets}
            if trigger_dates and trigger_symbols:
                loaded = engine._load_minute_for_fills(
                    engine.repo,
                    list(trigger_symbols),
                    trigger_dates,
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

    @staticmethod
    def _signal_id(code: int, signal_ids: tuple[str, ...]) -> str | None:
        return signal_ids[code] if 0 <= code < len(signal_ids) else None

    def _signal_date(self, signal_time: int, fallback: str) -> str:
        labels = self.matrix.timestamp_labels
        return labels[signal_time][:10] if signal_time >= 0 else fallback

    def _market_value(self) -> float:
        total = 0.0
        for asset, pos in self.positions.items():
            mark = self.last_close[asset]
            if not self._valid_price(mark):
                mark = pos["entry_price"]
            total += pos["shares"] * mark
        return total

    def _minute_rows(self, time_id: int, asset_id: int):
        if not self.config.minute_fill or not self.minute_cache:
            return None
        key = (self.matrix.symbols[asset_id], self.matrix.timestamp_labels[time_id][:10])
        return self.minute_cache.get(key)

    def _refill_price(self, time_id: int, asset_id: int, side: str, daily_price: float) -> float:
        rows = self._minute_rows(time_id, asset_id)
        if rows is None:
            return daily_price
        reference = float(self.matrix.reference_price[time_id, asset_id])
        precise = self.engine._resolve_minute_fill(
            rows, reference if self._valid_price(reference) else None, side
        )
        return precise if precise is not None else daily_price

    def _minute_trigger_price(self, time_id: int, asset_id: int) -> float | None:
        rows = self._minute_rows(time_id, asset_id)
        if rows is None:
            return None
        reference = float(self.matrix.reference_price[time_id, asset_id])
        return self.engine._resolve_minute_exit_trigger(
            rows, reference if self._valid_price(reference) else None
        )

    def _one_price_limit(self, time_id: int, asset_id: int, direction: str) -> bool:
        m = self.matrix
        if not m.tradable[time_id, asset_id]:
            return False
        prices = (
            float(m.open[time_id, asset_id]),
            float(m.high[time_id, asset_id]),
            float(m.low[time_id, asset_id]),
            float(m.close[time_id, asset_id]),
        )
        if not all(self._valid_price(value) for value in prices):
            return False
        same_price = max(prices) - min(prices) <= max(abs(prices[3]) * 1e-4, 0.01)
        flag = m.limit_up_locked if direction == "up" else m.limit_down_locked
        return bool(flag[time_id, asset_id]) and same_price

    def _intraday_entry(self, time_id: int, asset_id: int) -> bool:
        """建仓是否发生在盘中: 分钟逐格覆盖或 minute_fill 可用。"""
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
            return False, "buy_limit_up"
        if self._one_price_limit(time_id, asset_id, "up"):
            return False, "buy_limit_up"
        return True, ""

    def _can_sell(self, time_id: int, asset_id: int, override: float | None = None):
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
            return False, "sell_limit_down"
        if self._one_price_limit(time_id, asset_id, "down"):
            return False, "sell_limit_down"
        return True, ""

    def _mark_pending(
        self,
        asset_id: int,
        reason: str,
        signal_date: str,
        signal_id: str | None = None,
        next_open: bool = False,
    ) -> None:
        pos = self.positions[asset_id]
        if not pos.get("pending_exit_reason"):
            pos["pending_exit_reason"] = reason
            pos["pending_exit_signal_date"] = signal_date
            pos["pending_exit_signal_id"] = signal_id
            self._count("pending_exit")
        if next_open:
            pos["pending_exit_next_open"] = True
        pos["blocked_exit_days"] += 1

    def _sell(
        self,
        time_id: int,
        asset_id: int,
        reason: str,
        signal_date: str,
        sold_today: set[int],
        override: float | None = None,
    ) -> None:
        m = self.matrix
        pos = self.positions.pop(asset_id)
        exit_price = (
            float(override)
            if override is not None
            else self._refill_price(
                time_id, asset_id, "sell", float(self.exit_prices[time_id, asset_id])
            )
        )
        exit_value = pos["shares"] * exit_price * (1 - self.sell_cost_pct)
        self.cash += exit_value
        pnl_amount = exit_value - pos["entry_value"]
        pnl_pct = pnl_amount / pos["entry_value"] if pos["entry_value"] > 0 else 0.0
        sold_today.add(asset_id)
        self.trades.append(
            TradeRecord(
                symbol=m.symbols[asset_id],
                name=m.names[asset_id],
                entry_date=pos["entry_date"],
                exit_date=m.timestamp_labels[time_id][:10],  # type: ignore[arg-type]
                entry_price=round(float(pos["entry_price"]), 4),
                exit_price=round(exit_price, 4),
                pnl_pct=round(float(pnl_pct), 6),
                duration=int(pos["hold_days"]),
                exit_reason=reason,
                shares=round(float(pos["shares"]), 4),
                lots=round(float(pos["lots"]), 2),
                position_pct=round(float(pos["position_pct"]), 6),
                entry_value=round(float(pos["entry_value"]), 2),
                exit_value=round(float(exit_value), 2),
                pnl_amount=round(float(pnl_amount), 2),
                entry_score=round(float(pos["entry_score"]), 2),
                entry_signal_date=pos["entry_signal_date"],
                exit_signal_date=signal_date,
                blocked_exit_days=int(pos["blocked_exit_days"]),
                entry_signal_id=pos["entry_signal_id"],
                exit_signal_id=(
                    pos.get("pending_exit_signal_id")
                    or self._signal_id(
                        int(m.exit_signal_code[time_id, asset_id]), m.exit_signal_ids
                    )
                )
                if reason == "signal"
                else None,
            )
        )

    def _try_sell(
        self,
        time_id: int,
        asset_id: int,
        reason: str,
        signal_date: str,
        sold_today: set[int],
        override: float | None = None,
    ) -> bool:
        signal_id = (
            self._signal_id(
                int(self.matrix.exit_signal_code[time_id, asset_id]), self.matrix.exit_signal_ids
            )
            if reason == "signal"
            else None
        )
        minute_trigger = self.config.exit_fill == "signal_next_minute" and reason == "signal"
        if minute_trigger and override is None:
            pos = self.positions[asset_id]
            if pos.get("pending_exit_next_open"):
                override = float(self.matrix.open[time_id, asset_id])
            else:
                override = self._minute_trigger_price(time_id, asset_id)
                if override is None:
                    self._mark_pending(asset_id, reason, signal_date, signal_id, next_open=True)
                    self._count("sell_minute_trigger_fallback")
                    return False
        ok, blocked = self._can_sell(time_id, asset_id, override)
        if not ok:
            self._mark_pending(asset_id, reason, signal_date, signal_id, next_open=minute_trigger)
            self._count(blocked)
            return False
        self._sell(time_id, asset_id, reason, signal_date, sold_today, override)
        return True

    def _scheduled_exit(self, time_id: int, date_text: str, sold_today: set[int]) -> None:
        m = self.matrix
        for asset_id in list(self.positions):
            pos = self.positions.get(asset_id)
            if pos is None:
                continue
            reason = ""
            signal_date = date_text
            if pos.get("pending_exit_reason"):
                reason = str(pos["pending_exit_reason"])
                signal_date = str(pos.get("pending_exit_signal_date") or date_text)
            elif m.exit[time_id, asset_id]:
                reason = "signal"
                signal_date = self._signal_date(
                    int(m.exit_signal_time[time_id, asset_id]), date_text
                )
            elif (
                self.config.max_hold_days is not None
                and pos["hold_days"] >= self.config.max_hold_days
            ):
                reason = "max_hold"
            elif time_id == self.time_count - 1:
                reason = "end"
            if reason:
                self._try_sell(time_id, asset_id, reason, signal_date, sold_today)

    def _entry_candidates(self, time_id: int, sold_today: set[int]) -> list[tuple[int, float]]:
        m = self.matrix
        candidates: list[tuple[int, float]] = []
        for raw_asset in np.flatnonzero(m.entry[time_id]):
            asset = int(raw_asset)
            if asset in self.positions or asset in sold_today:
                if asset in sold_today:
                    self._count("buy_same_day_reentry")
                continue
            ok, blocked = self._can_buy(time_id, asset)
            if not ok:
                self._count(blocked)
                continue
            score = _matrix_entry_score(m, time_id, asset)
            if self.config.score_min is not None and score < self.config.score_min:
                self._count("buy_score_filter")
                continue
            if self.config.score_max is not None and score > self.config.score_max:
                self._count("buy_score_filter")
                continue
            candidates.append((asset, score))
        candidates.sort(key=lambda item: item[1], reverse=True)
        return candidates

    def _entry_budget(self, time_id: int, selected: list[tuple[int, float]]) -> tuple | None:
        """返回 (weights, policy_scale, total_budget, equity_before, target_value)。"""
        cfg = self.config
        market_value_before = self._market_value()
        equity_before = self.cash + market_value_before
        target_value = equity_before * self.max_exposure_pct / self.max_positions
        exposure_capacity = equity_before * self.max_exposure_pct - market_value_before
        if equity_before <= 0 or exposure_capacity <= 0 or self.max_exposure_pct <= 0:
            self.execution_stats["buy_exposure"] += len(selected)
            return None
        weights = np.repeat(1 / len(selected), len(selected))
        if cfg.position_sizing == "score_weight":
            raw_weights = np.array([max(item[1], 0.0) for item in selected])
            if raw_weights.sum() > 0:
                weights = raw_weights / raw_weights.sum()
        policy_scale = 1.0
        if cfg.position_sizing == "policy" and cfg.position_policy_compute is not None:
            weights, policy_scale = self._policy_weights(time_id, selected, equity_before)
        total_budget = min(self.cash, exposure_capacity, target_value * len(selected))
        if policy_scale < 1.0:
            total_budget = max(total_budget * policy_scale, 0.0)
        return weights, total_budget, equity_before, target_value

    def _policy_weights(
        self, time_id: int, selected: list[tuple[int, float]], equity_before: float
    ) -> tuple:
        m = self.matrix
        policy_returns: dict[str, np.ndarray] = {}
        for asset_id, _score in selected:
            # 窗口截止到入场信号日 — open_t+1 成交日的 K 线对决策是未来数据。
            source_time = int(m.entry_signal_time[time_id, int(asset_id)])
            if source_time < 0:
                source_time = time_id
            col = m.close[max(0, source_time - 59) : source_time + 1, int(asset_id)].astype(
                np.float64
            )
            finite = col[np.isfinite(col)]
            if finite.size >= 2:
                policy_returns[str(m.symbols[int(asset_id)])] = np.diff(finite) / finite[:-1]
        return _apply_position_policy(
            self.config,
            [(str(m.symbols[int(a)]), float(s)) for a, s in selected],
            policy_returns,
            self.trades,
            self.cash,
            equity_before,
            self.peak,
            len(self.positions),
            self.execution_stats,
        )

    def _open_positions(
        self,
        time_id: int,
        date_text: str,
        selected: list[tuple[int, float]],
        weights,
        total_budget: float,
        equity_before: float,
        target_value: float,
    ) -> None:
        for (asset_id, entry_score), weight in zip(selected, weights, strict=False):
            if len(self.positions) >= self.max_positions:
                self._count("buy_no_slot")
                break
            market_value = self._market_value()
            equity = self.cash + market_value
            capacity = equity * self.max_exposure_pct - market_value
            allocation = min(total_budget * float(weight), target_value, self.cash, capacity)
            if allocation <= 0:
                self._count("buy_exposure")
                continue
            entry_price = self._refill_price(
                time_id, asset_id, "buy", float(self.entry_prices[time_id, asset_id])
            )
            shares = np.floor(allocation / (entry_price * (1 + self.buy_cost_pct)) / 100) * 100
            entry_value = shares * entry_price * (1 + self.buy_cost_pct)
            if shares <= 0:
                self._count("buy_lot_size")
                continue
            if entry_value > self.cash + 1e-6:
                self._count("buy_cash")
                continue
            if entry_value > capacity + 1e-6:
                self._count("buy_exposure")
                continue
            self.cash -= entry_value
            m = self.matrix
            self.positions[asset_id] = {
                "entry_date": date_text,
                "entry_signal_date": self._signal_date(
                    int(m.entry_signal_time[time_id, asset_id]), date_text
                ),
                "entry_signal_id": self._signal_id(
                    int(m.entry_signal_code[time_id, asset_id]), m.entry_signal_ids
                ),
                "entry_price": entry_price,
                "entry_value": entry_value,
                "shares": shares,
                "lots": shares / 100,
                "position_pct": entry_value / equity_before if equity_before > 0 else 0.0,
                "entry_score": entry_score,
                "max_high": entry_price,
                "hold_days": 0,
                "pending_exit_reason": None,
                "pending_exit_signal_date": None,
                "pending_exit_signal_id": None,
                "pending_exit_next_open": False,
                "blocked_exit_days": 0,
            }

    def _process_entries(self, time_id: int, date_text: str, sold_today: set[int]) -> None:
        candidates = self._entry_candidates(time_id, sold_today)
        slots = self.max_positions - len(self.positions)
        if slots <= 0:
            self.execution_stats["buy_no_slot"] += len(candidates)
            return
        if not candidates:
            return
        selected = candidates[:slots]
        budget = self._entry_budget(time_id, selected)
        if budget is None:
            return
        weights, total_budget, equity_before, target_value = budget
        self._open_positions(
            time_id, date_text, selected, weights, total_budget, equity_before, target_value
        )

    def _update_curves(self, time_id: int, date_text: str) -> None:
        m = self.matrix
        for asset_id, pos in self.positions.items():
            high_price = float(m.high[time_id, asset_id])
            # close_t 且收盘价成交的当日建仓: 当日 high 在建仓之前, 不进移动止损峰值。
            if self._valid_price(high_price) and not (
                self.config.entry_fill == "close_t"
                and not self._intraday_entry(time_id, asset_id)
                and pos["entry_date"] == date_text
            ):
                pos["max_high"] = max(float(pos["max_high"]), high_price)
        valid_closes = np.isfinite(m.close[time_id]) & (m.close[time_id] > 0)
        self.last_close[valid_closes] = m.close[time_id, valid_closes]
        market_value = self._market_value()
        equity = self.cash + market_value
        self.peak = max(self.peak, equity)
        drawdown = (equity - self.peak) / self.peak if self.peak > 0 else 0.0
        exposure = market_value / equity if equity > 0 else 0.0
        equity_value = round(float(equity), 2)
        exposure_value = round(float(exposure), 4)
        self.equity_values.append(equity_value)
        self.exposure_values.append(exposure_value)
        if self.options.include_curves:
            self.equity_curve.append(
                {
                    "date": date_text,
                    "value": equity_value,
                    "cash": round(float(self.cash), 2),
                    "positions": len(self.positions),
                    "exposure": exposure_value,
                }
            )
            self.drawdown_curve.append({"date": date_text, "value": round(float(drawdown), 4)})

    def _day_step(self, time_id: int, date_text: str) -> bool:
        if time_id % 20 == 0:
            if self.cancel_event is not None and self.cancel_event.is_set():
                logger.info("回测被用户取消 (第 %d/%d 天)", time_id, self.time_count)
                return False
            if self.progress_cb is not None:
                with contextlib.suppress(Exception):
                    self.progress_cb(
                        {
                            "day": time_id + 1,
                            "total": self.time_count,
                            "date": date_text,
                            "equity": round(self.cash + self._market_value(), 2),
                        }
                    )
        sold_today: set[int] = set()
        for pos in self.positions.values():
            pos["hold_days"] += 1
        self._risk_exit(time_id, date_text, sold_today)
        self._scheduled_exit(time_id, date_text, sold_today)
        if time_id < self.time_count - 1 and self.max_positions > 0:
            self._process_entries(time_id, date_text, sold_today)
        self._update_curves(time_id, date_text)
        return True

    def _force_close(self) -> int:
        pending_exit_count = sum(
            1 for pos in self.positions.values() if pos.get("pending_exit_reason")
        )
        for asset_id in list(self.positions):
            last_time = None
            for t in range(self.time_count - 1, -1, -1):
                if self._valid_price(self.exit_prices[t, asset_id]):
                    last_time = t
                    break
            if last_time is None:
                continue
            self._sell(
                last_time,
                asset_id,
                str(self.positions[asset_id].get("pending_exit_reason") or "end"),
                self.matrix.timestamp_labels[last_time][:10],
                set(),
            )
        return pending_exit_count

    def run(self) -> SimResult:
        for time_id, date_label in enumerate(self.matrix.timestamp_labels):
            if not self._day_step(time_id, date_label[:10]):
                break
        pending_exit_count = self._force_close()
        statistics_started = time.perf_counter()
        stats = self.engine._calc_portfolio_stats_from_values(
            self.equity_values,
            self.exposure_values,
            self.trades,
            self.config.initial_capital,
            include_monte_carlo=self.options.include_monte_carlo,
        )
        stats["statistics_ms"] = round((time.perf_counter() - statistics_started) * 1000, 1)
        stats["execution"] = self.execution_stats
        stats["pending_exit_positions"] = pending_exit_count
        stats["market_matrix_shape"] = [self.time_count, self.asset_count]
        stats["market_matrix_bytes"] = self.matrix.nbytes
        return SimResult(
            equity_curve=self.equity_curve if self.options.include_curves else [],
            drawdown_curve=self.drawdown_curve if self.options.include_curves else [],
            trades=self.trades if self.options.include_trades else [],
            per_symbol_stats=(
                self.engine._calc_per_symbol(self.trades)
                if self.options.include_per_symbol_stats
                else []
            ),
            stats=stats,
        )
