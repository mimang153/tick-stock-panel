from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    MatcherConfig,
    SimResult,
    TradeRecord,
    _apply_position_policy,
    _resolve_signal_id,
    contextlib,
    logger,
    np,
    pl,
)


class _LegacyPortfolioRun(_root._LegacyPortfolioRunRiskMixin):
    """面板账户级组合撮合状态机 (信号 → 成交约束 → 仓位/现金)。"""

    def __init__(
        self,
        engine,
        panel: pl.DataFrame,
        entries,
        exits,
        config: MatcherConfig,
        progress_cb,
        cancel_event,
        entry_signal_ids,
        exit_signal_ids,
    ) -> None:
        self.engine = engine
        self.panel = panel
        self.config = config
        self.progress_cb = progress_cb
        self.cancel_event = cancel_event
        self.entry_signal_ids = entry_signal_ids
        self.exit_signal_ids = exit_signal_ids
        self.n = len(panel)
        self.panel_dates = panel["date"].to_numpy()
        self.panel_symbols = panel["symbol"].to_numpy()
        self.trades: list[TradeRecord] = []
        self.minute_cache: dict = {}
        self._prepare_signals(entries, exits)
        self._prepare_columns()
        self._prepare_dates()
        self._prepare_minute_cache()
        self._prepare_state()

    def _prepare_signals(self, entries, exits) -> None:
        n = self.n
        cfg = self.config
        ent_raw = np.zeros(n, dtype=bool)
        ext_raw = np.zeros(n, dtype=bool)
        if entries is not None and len(entries) == n:
            ent_raw = entries.to_numpy().astype(bool)
        if exits is not None and len(exits) == n:
            ext_raw = exits.to_numpy().astype(bool)
        self.ent_raw = ent_raw
        self.ext_raw = ext_raw
        self.entry_signal_dates = np.array([None] * n, dtype=object)
        self.exit_signal_dates = np.array([None] * n, dtype=object)
        same_prev_symbol = self.panel_symbols[1:] == self.panel_symbols[:-1]
        ent = np.zeros(n, dtype=bool)
        if cfg.entry_fill == "open_t+1":
            ent[1:] = ent_raw[:-1] & same_prev_symbol
            for idx in np.flatnonzero(ent):
                self.entry_signal_dates[idx] = self.engine._date_str(self.panel_dates[idx - 1])
        else:
            ent = ent_raw
            for idx in np.flatnonzero(ent):
                self.entry_signal_dates[idx] = self.engine._date_str(self.panel_dates[idx])
        ext = np.zeros(n, dtype=bool)
        if cfg.exit_fill == "open_t+1":
            ext[1:] = ext_raw[:-1] & same_prev_symbol
            for idx in np.flatnonzero(ext):
                self.exit_signal_dates[idx] = self.engine._date_str(self.panel_dates[idx - 1])
        else:
            ext = ext_raw
            for idx in np.flatnonzero(ext):
                self.exit_signal_dates[idx] = self.engine._date_str(self.panel_dates[idx])
        self.ent = ent
        self.ext = ext

    def _prepare_columns(self) -> None:
        panel = self.panel
        n = self.n
        self.open_prices = panel["open"].to_numpy()
        self.high_prices = panel["high"].to_numpy() if "high" in panel.columns else self.open_prices
        self.low_prices = panel["low"].to_numpy()
        self.close_prices = panel["close"].to_numpy()
        self.entry_prices = (
            self.open_prices if self.config.entry_fill == "open_t+1" else self.close_prices
        )
        self.exit_prices = (
            self.open_prices if self.config.exit_fill == "open_t+1" else self.close_prices
        )
        self.has_volume = "volume" in panel.columns
        self.volumes = (
            panel["volume"].fill_null(0).to_numpy() if self.has_volume else np.ones(n, dtype=float)
        )
        self.names = (
            panel["name"].fill_null("").to_numpy()
            if "name" in panel.columns
            else np.array([""] * n)
        )
        scores = (
            panel["score"].fill_null(0).to_numpy()
            if "score" in panel.columns
            else np.zeros(n, dtype=float)
        )
        self.trade_scores = scores.copy()
        if self.config.entry_fill == "open_t+1":
            self.trade_scores[1:] = np.where(
                self.panel_symbols[1:] == self.panel_symbols[:-1],
                scores[:-1],
                self.trade_scores[1:],
            )
        self.limit_up_flags = (
            panel["signal_limit_up"].fill_null(False).to_numpy().astype(bool)
            if "signal_limit_up" in panel.columns
            else np.zeros(n, dtype=bool)
        )
        self.limit_down_flags = (
            panel["signal_limit_down"].fill_null(False).to_numpy().astype(bool)
            if "signal_limit_down" in panel.columns
            else np.zeros(n, dtype=bool)
        )

    def _prepare_dates(self) -> None:
        self.date_to_indices: dict[str, list[int]] = {}
        for i, d in enumerate(self.panel_dates):
            self.date_to_indices.setdefault(self.engine._date_str(d), []).append(i)
        self.all_dates = sorted(self.date_to_indices.keys())

    def _prepare_minute_cache(self) -> None:
        if not self.config.minute_fill:
            return
        trigger_dates: set[str] = set()
        trigger_symbols: set[str] = set()
        for idx in range(self.n):
            if self.ent[idx] or self.ext[idx]:
                trigger_dates.add(self.engine._date_str(self.panel_dates[idx]))
                trigger_symbols.add(str(self.panel_symbols[idx]))
        if not trigger_dates or not trigger_symbols:
            return
        asset_type = (
            "etf"
            if all(
                str(s).endswith(".SH") and str(s).startswith("5") for s in list(trigger_symbols)[:5]
            )
            else "stock"
        )
        loaded = self.engine._load_minute_for_fills(
            self.engine.repo,
            list(trigger_symbols),
            trigger_dates,
            asset_type,
            evidence=self.engine._evidence,
        )
        for key, marr in loaded.items():
            if marr is not None and len(marr) > 0:
                self.minute_cache[key] = marr

    def _prepare_state(self) -> None:
        cfg = self.config
        self.buy_cost_pct = cfg.buy_cost_pct()
        self.sell_cost_pct = cfg.sell_cost_pct()
        self.cash = float(cfg.initial_capital)
        self.peak = self.cash
        self.max_positions = max(int(cfg.max_positions), 0)
        self.max_exposure_pct = min(max(float(getattr(cfg, "max_exposure_pct", 1.0)), 0.0), 1.0)
        self.score_min = getattr(cfg, "score_min", None)
        self.score_max = getattr(cfg, "score_max", None)
        self.positions: dict[str, dict] = {}
        self.last_close: dict[str, float] = {}
        self.equity_curve: list[dict] = []
        self.drawdown_curve: list[dict] = []
        self.symbol_first_row: dict[str, int] = {}
        if cfg.position_sizing == "policy" and cfg.position_policy_compute is not None:
            uniq_syms, first_idx = np.unique(self.panel_symbols, return_index=True)
            self.symbol_first_row = {
                str(s): int(i) for s, i in zip(uniq_syms, first_idx, strict=True)
            }
        self.execution_stats: dict[str, int] = {
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

    def _minute_reference(self, idx: int) -> float | None:
        """均线参考线代数剔除当根收盘 (盘中不可能知道当日收盘)。"""
        ref = None
        for col, win in (("ma5", 5), ("ma10", 10), ("ma20", 20)):
            if col not in self.panel.columns:
                continue
            try:
                fv = float(self.panel[col][idx])
                if not (fv > 0 and np.isfinite(fv)):
                    continue
                try:
                    cv = float(self.panel["close"][idx])
                except (TypeError, ValueError):
                    cv = float("nan")
                ref = (
                    (win * fv - cv) / (win - 1) if (win > 1 and cv > 0 and np.isfinite(cv)) else fv
                )
                break
            except (TypeError, ValueError):
                pass
        return ref

    def _refill_price(self, idx: int, side: str, daily_price: float) -> float:
        """分钟K精确成交价; 无数据则降级为 daily_price。"""
        if not self.config.minute_fill or not self.minute_cache:
            return daily_price
        sym = str(self.panel_symbols[idx])
        d_str = self.engine._date_str(self.panel_dates[idx])
        marr = self.minute_cache.get((sym, d_str))
        if marr is None:
            return daily_price
        precise = self.engine._resolve_minute_fill(marr, self._minute_reference(idx), side)
        return precise if precise is not None else daily_price

    def _count(self, key: str) -> None:
        self.execution_stats[key] = self.execution_stats.get(key, 0) + 1

    @staticmethod
    def _valid_price(value) -> bool:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        return v > 0 and np.isfinite(v)

    def _market_value(self) -> float:
        value = 0.0
        for pos in self.positions.values():
            mark = self.last_close.get(pos["symbol"], pos["entry_price"])
            value += pos["shares"] * mark
        return value

    def _is_suspended(self, idx: int) -> bool:
        o = float(self.open_prices[idx])
        h = float(self.high_prices[idx])
        lo = float(self.low_prices[idx])
        c = float(self.close_prices[idx])
        valid_bar = any(self._valid_price(x) for x in (o, h, lo, c))
        if not valid_bar:
            return True
        if self.has_volume and float(self.volumes[idx] or 0) <= 0:
            same_price = max(o, h, lo, c) - min(o, h, lo, c) <= max(abs(c) * 1e-4, 0.01)
            if same_price:
                return True
        return False

    def _is_one_price_limit(self, idx: int, direction: str) -> bool:
        if self._is_suspended(idx):
            return False
        o = float(self.open_prices[idx])
        h = float(self.high_prices[idx])
        lo = float(self.low_prices[idx])
        c = float(self.close_prices[idx])
        if not all(self._valid_price(x) for x in (o, h, lo, c)):
            return False
        same_price = max(o, h, lo, c) - min(o, h, lo, c) <= max(abs(c) * 1e-4, 0.01)
        flag = self.limit_up_flags if direction == "up" else self.limit_down_flags
        return bool(flag[idx]) and same_price

    def _can_buy(self, idx: int) -> tuple[bool, str]:
        if self._is_suspended(idx):
            return False, "buy_suspended"
        if not self._valid_price(self.entry_prices[idx]):
            return False, "buy_invalid_price"
        if self.config.entry_fill == "close_t" and bool(self.limit_up_flags[idx]):
            return False, "buy_limit_up"
        if self._is_one_price_limit(idx, "up"):
            return False, "buy_limit_up"
        return True, ""

    def _can_sell(self, idx: int, override: float | None = None) -> tuple[bool, str]:
        if self._is_suspended(idx):
            return False, "sell_suspended"
        price = override if override is not None else self.exit_prices[idx]
        if not self._valid_price(price):
            return False, "sell_invalid_price"
        if (
            override is None
            and self.config.exit_fill == "close_t"
            and bool(self.limit_down_flags[idx])
        ):
            return False, "sell_limit_down"
        if self._is_one_price_limit(idx, "down"):
            return False, "sell_limit_down"
        return True, ""

    def _mark_pending(self, sym: str, reason: str, signal_date: str) -> None:
        pos = self.positions[sym]
        if not pos.get("pending_exit_reason"):
            pos["pending_exit_reason"] = reason
            pos["pending_exit_signal_date"] = signal_date
            self._count("pending_exit")
        pos["blocked_exit_days"] = int(pos.get("blocked_exit_days", 0)) + 1

    def _sell(
        self,
        sym: str,
        idx: int,
        reason: str,
        signal_date: str,
        sold_today: set[str],
        override: float | None = None,
    ) -> None:
        pos = self.positions.pop(sym)
        exit_price = (
            float(override)
            if override is not None
            else self._refill_price(idx, "sell", float(self.exit_prices[idx]))
        )
        exit_value = pos["shares"] * exit_price * (1 - self.sell_cost_pct)
        self.cash += exit_value
        pnl_amount = exit_value - pos["entry_value"]
        pnl_pct = (
            (exit_value - pos["entry_value"]) / pos["entry_value"]
            if pos["entry_value"] > 0
            else 0.0
        )
        sold_today.add(sym)
        self.trades.append(
            TradeRecord(
                symbol=sym,
                name=pos.get("name", ""),
                entry_date=pos["entry_date"],
                exit_date=self.engine._date_str(self.panel_dates[idx]),  # type: ignore[arg-type]
                entry_price=round(float(pos["entry_price"]), 4),
                exit_price=round(exit_price, 4),
                pnl_pct=round(float(pnl_pct), 6),
                duration=int(pos["hold_days"]),
                exit_reason=reason,
                shares=round(float(pos["shares"]), 4),
                lots=round(float(pos["lots"]), 2),
                position_pct=round(float(pos.get("position_pct", 0.0)), 6),
                entry_value=round(float(pos["entry_value"]), 2),
                exit_value=round(float(exit_value), 2),
                pnl_amount=round(float(pnl_amount), 2),
                entry_score=round(float(pos["entry_score"]), 2)
                if pos.get("entry_score") is not None
                else None,
                entry_signal_date=pos.get("entry_signal_date"),
                exit_signal_date=signal_date,
                blocked_exit_days=int(pos.get("blocked_exit_days", 0)),
                entry_signal_id=pos.get("entry_signal_id"),
                exit_signal_id=_resolve_signal_id(self.panel, idx, self.exit_signal_ids)
                if reason == "signal"
                else None,
            )
        )

    def _try_sell(
        self,
        sym: str,
        idx: int | None,
        reason: str,
        signal_date: str,
        sold_today: set[str],
        override: float | None = None,
    ) -> bool:
        if idx is None:
            self._mark_pending(sym, reason, signal_date)
            self._count("sell_suspended")
            return False
        ok, block_reason = self._can_sell(idx, override)
        if not ok:
            self._mark_pending(sym, reason, signal_date)
            self._count(block_reason)
            return False
        self._sell(sym, idx, reason, signal_date, sold_today, override)
        return True

    def _scheduled_exits(
        self, d_idx: int, d_str: str, row_by_symbol: dict[str, int], sold_today: set[str]
    ) -> None:
        for sym in list(self.positions.keys()):
            pos = self.positions.get(sym)
            if pos is None:
                continue
            idx = row_by_symbol.get(sym)
            reason = ""
            signal_date = d_str
            if pos.get("pending_exit_reason"):
                reason = str(pos["pending_exit_reason"])
                signal_date = str(pos.get("pending_exit_signal_date") or d_str)
            elif idx is not None and self.ext[idx]:
                reason = "signal"
                signal_date = str(self.exit_signal_dates[idx] or d_str)
            elif (
                self.config.max_hold_days is not None
                and pos["hold_days"] >= self.config.max_hold_days
            ):
                reason = "max_hold"
            elif d_idx == len(self.all_dates) - 1:
                reason = "end"
            if reason:
                self._try_sell(sym, idx, reason, signal_date, sold_today)

    def _entry_candidates(
        self, idxs: list[int], sold_today: set[str]
    ) -> list[tuple[int, str, float]]:
        candidates: list[tuple[int, str, float]] = []
        for idx in idxs:
            if not self.ent[idx]:
                continue
            sym = str(self.panel_symbols[idx])
            if sym in self.positions:
                continue
            if sym in sold_today:
                self._count("buy_same_day_reentry")
                continue
            ok, block_reason = self._can_buy(idx)
            if not ok:
                self._count(block_reason)
                continue
            score = float(self.trade_scores[idx] or 0.0)
            if self.score_min is not None and score < self.score_min:
                self._count("buy_score_filter")
                continue
            if self.score_max is not None and score > self.score_max:
                self._count("buy_score_filter")
                continue
            candidates.append((idx, sym, score))
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates

    def _entry_budget(self, selected: list[tuple[int, str, float]]) -> tuple | None:
        """返回 (weights, policy_scale, total_budget, account_equity_before_buy)。"""
        market_value_before = self._market_value()
        equity = self.cash + market_value_before
        if equity <= 0 or self.max_exposure_pct <= 0:
            self.execution_stats["buy_exposure"] += len(selected)
            return None
        target = equity * self.max_exposure_pct / self.max_positions
        exposure_capacity = equity * self.max_exposure_pct - market_value_before
        if exposure_capacity <= 0:
            self.execution_stats["buy_exposure"] += len(selected)
            return None
        weights = np.repeat(1 / len(selected), len(selected))
        if self.config.position_sizing == "score_weight":
            raw = np.array([max(x[2], 0.0) for x in selected], dtype=float)
            if raw.sum() > 0:
                weights = raw / raw.sum()
        policy_scale = 1.0
        if (
            self.config.position_sizing == "policy"
            and self.config.position_policy_compute is not None
        ):
            weights, policy_scale = self._policy_weights(selected, equity)
        total_budget = min(self.cash, exposure_capacity, target * len(selected))
        if policy_scale < 1.0:
            total_budget = max(total_budget * policy_scale, 0.0)
        return weights, policy_scale, total_budget, equity, target

    def _policy_weights(self, selected: list[tuple[int, str, float]], equity: float) -> tuple:
        policy_returns: dict[str, np.ndarray] = {}
        for idx, sym, _score in selected:
            # 窗口截止到入场信号日; open_t+1 时成交行的前一行才是信号行。
            sig_idx = int(idx) - 1 if self.config.entry_fill == "open_t+1" else int(idx)
            lo = max(self.symbol_first_row.get(str(sym), 0), sig_idx - 59)
            closes = self.close_prices[lo : sig_idx + 1].astype(np.float64)
            finite = closes[np.isfinite(closes)]
            if finite.size >= 2:
                policy_returns[str(sym)] = np.diff(finite) / finite[:-1]
        return _apply_position_policy(
            self.config,
            [(str(sym), float(score)) for _idx, sym, score in selected],
            policy_returns,
            self.trades,
            self.cash,
            equity,
            self.peak,
            len(self.positions),
            self.execution_stats,
        )

    def _open_positions(
        self,
        selected: list[tuple[int, str, float]],
        weights,
        policy_scale: float,
        total_budget: float,
        equity_before: float,
        target_value: float,
    ) -> None:
        for (idx, sym, _score), weight in zip(selected, weights, strict=False):
            if len(self.positions) >= self.max_positions:
                self._count("buy_no_slot")
                break
            current_market_value = self._market_value()
            current_equity = self.cash + current_market_value
            capacity = current_equity * self.max_exposure_pct - current_market_value
            allocation = min(
                total_budget * float(weight),
                target_value,
                self.cash,
                capacity,
            )
            if allocation <= 0:
                self._count("buy_exposure")
                continue
            entry_price = self._refill_price(idx, "buy", float(self.entry_prices[idx]))
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
            self.positions[sym] = {
                "symbol": sym,
                "name": str(self.names[idx] or ""),
                "entry_date": self.engine._date_str(self.panel_dates[idx]),
                "entry_signal_date": self.entry_signal_dates[idx]
                or self.engine._date_str(self.panel_dates[idx]),
                "entry_signal_id": _resolve_signal_id(self.panel, idx, self.entry_signal_ids),
                "entry_price": entry_price,
                "entry_value": entry_value,
                "shares": shares,
                "lots": shares / 100,
                "position_pct": entry_value / equity_before if equity_before > 0 else 0.0,
                "entry_score": _score,
                "max_high": entry_price,
                "hold_days": 0,
                "pending_exit_reason": None,
                "pending_exit_signal_date": None,
                "blocked_exit_days": 0,
            }

    def _process_entries(self, idxs: list[int], sold_today: set[str]) -> None:
        if self.max_positions <= 0:
            return
        candidates = self._entry_candidates(idxs, sold_today)
        if not candidates:
            return
        slots = self.max_positions - len(self.positions)
        if slots <= 0:
            self.execution_stats["buy_no_slot"] += len(candidates)
            return
        selected = candidates[:slots]
        budget = self._entry_budget(selected)
        if budget is None:
            return
        weights, policy_scale, total_budget, equity, target = budget
        self._open_positions(selected, weights, policy_scale, total_budget, equity, target)

    def _day_step(self, d_idx: int, d_str: str) -> bool:
        if d_idx % 20 == 0:
            if self.cancel_event is not None and self.cancel_event.is_set():
                logger.info("回测被用户取消 (第 %d/%d 天)", d_idx, len(self.all_dates))
                return False
            if self.progress_cb is not None:
                with contextlib.suppress(Exception):
                    self.progress_cb(
                        {
                            "day": d_idx + 1,
                            "total": len(self.all_dates),
                            "date": str(d_str)[:10],
                            "equity": round(self.cash + self._market_value(), 2),
                        }
                    )
        idxs = self.date_to_indices[d_str]
        row_by_symbol = {str(self.panel_symbols[i]): i for i in idxs}
        sold_today: set[str] = set()
        for pos in self.positions.values():
            pos["hold_days"] += 1
        # 执行顺序: 风控 → 计划出场 → 建仓; 当天新建仓不会被风控误杀。
        self._risk_exits(d_str, row_by_symbol, sold_today)
        self._scheduled_exits(d_idx, d_str, row_by_symbol, sold_today)
        if d_idx < len(self.all_dates) - 1:
            self._process_entries(idxs, sold_today)
        for sym, pos in self.positions.items():
            idx = row_by_symbol.get(sym)
            if idx is not None:
                hi = float(self.high_prices[idx])
                legacy_intraday_entry = self.config.minute_fill and bool(self.minute_cache)
                if self._valid_price(hi) and not (
                    self.config.entry_fill == "close_t"
                    and not legacy_intraday_entry
                    and pos["entry_date"] == str(d_str)[:10]
                ):
                    pos["max_high"] = max(float(pos.get("max_high", pos["entry_price"])), hi)
        for i in idxs:
            c = float(self.close_prices[i])
            if c > 0 and np.isfinite(c):
                self.last_close[str(self.panel_symbols[i])] = c
        market_value = self._market_value()
        equity = self.cash + market_value
        self.peak = max(self.peak, equity)
        dd = (equity - self.peak) / self.peak if self.peak > 0 else 0.0
        exposure = market_value / equity if equity > 0 else 0.0
        self.equity_curve.append(
            {
                "date": d_str[:10],
                "value": round(float(equity), 2),
                "cash": round(float(self.cash), 2),
                "positions": len(self.positions),
                "exposure": round(float(exposure), 4),
            }
        )
        self.drawdown_curve.append({"date": d_str[:10], "value": round(float(dd), 4)})
        return True

    def _force_close(self) -> int:
        pending_exit_count = sum(1 for p in self.positions.values() if p.get("pending_exit_reason"))
        for sym in list(self.positions):
            last_idx = None
            for i in range(self.n - 1, -1, -1):
                if str(self.panel_symbols[i]) == sym and self._valid_price(self.exit_prices[i]):
                    last_idx = i
                    break
            if last_idx is None:
                continue
            self._sell(
                sym,
                last_idx,
                str(self.positions[sym].get("pending_exit_reason") or "end"),
                self.engine._date_str(self.panel_dates[last_idx]),
                set(),
            )
        return pending_exit_count

    def run(self) -> SimResult:
        for d_idx, d_str in enumerate(self.all_dates):
            if not self._day_step(d_idx, d_str):
                break
        pending_exit_count = self._force_close()
        stats = self.engine._calc_portfolio_stats(
            self.equity_curve, self.trades, self.config.initial_capital
        )
        stats["execution"] = self.execution_stats
        stats["pending_exit_positions"] = pending_exit_count
        per_symbol = self.engine._calc_per_symbol(self.trades)
        return SimResult(
            equity_curve=self.equity_curve,
            drawdown_curve=self.drawdown_curve,
            trades=self.trades,
            per_symbol_stats=per_symbol,
            stats=stats,
        )
