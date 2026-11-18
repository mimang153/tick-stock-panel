from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    MatcherConfig,
    SimResult,
    TradeRecord,
    _resolve_signal_id,
    contextlib,
    logger,
    np,
    pl,
)


class _LegacyIndependentRun(_root._LegacyIndependentRunRiskMixin):
    """面板独立候选撮合状态机: 每个买入信号为一个独立样本。"""

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
        self._prepare_minute_cache()
        self._prepare_columns()
        self._prepare_rows()
        self.execution_stats: dict[str, int] = {
            "buy_invalid_price": 0,
            "buy_suspended": 0,
            "buy_limit_up": 0,
            "buy_score_filter": 0,
            "buy_no_next_bar": max(self.n_candidates - int(self.ent.sum()), 0),
            "sell_invalid_price": 0,
            "sell_suspended": 0,
            "sell_limit_down": 0,
            "sell_no_future": 0,
            "pending_exit": 0,
        }

    def _prepare_signals(self, entries, exits) -> None:
        """建仓/清仓口径独立 shift, 信号日期跟随成交日回溯。"""
        n = self.n
        cfg = self.config
        ent_raw = np.zeros(n, dtype=bool)
        ext_raw = np.zeros(n, dtype=bool)
        if entries is not None and len(entries) == n:
            ent_raw = entries.to_numpy().astype(bool)
        if exits is not None and len(exits) == n:
            ext_raw = exits.to_numpy().astype(bool)
        self.n_candidates = int(ent_raw.sum())
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
        open_prices = self.panel["open"].to_numpy()
        self.open_prices = open_prices
        self.high_prices = (
            self.panel["high"].to_numpy() if "high" in self.panel.columns else open_prices
        )
        self.low_prices = self.panel["low"].to_numpy()
        close_prices = self.panel["close"].to_numpy()
        self.close_prices = close_prices
        self.entry_prices = open_prices if cfg.entry_fill == "open_t+1" else close_prices
        self.exit_prices = open_prices if cfg.exit_fill == "open_t+1" else close_prices

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
        loaded = self.engine._load_minute_for_fills(
            self.engine.repo,
            list(trigger_symbols),
            trigger_dates,
            "stock",
            evidence=self.engine._evidence,
        )
        for key, marr in loaded.items():
            if marr is not None and len(marr) > 0:
                self.minute_cache[key] = marr

    def _prepare_columns(self) -> None:
        panel = self.panel
        n = self.n
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
        self.buy_cost_pct = self.config.buy_cost_pct()
        self.sell_cost_pct = self.config.sell_cost_pct()
        self.score_min = getattr(self.config, "score_min", None)
        self.score_max = getattr(self.config, "score_max", None)

    def _prepare_rows(self) -> None:
        self.symbol_rows: dict[str, list[int]] = {}
        self.row_pos_in_symbol = np.zeros(self.n, dtype=int)
        for i, sym_value in enumerate(self.panel_symbols):
            sym = str(sym_value)
            rows = self.symbol_rows.setdefault(sym, [])
            self.row_pos_in_symbol[i] = len(rows)
            rows.append(i)

    def _refill_price(self, idx: int, side: str, daily_price: float) -> float:
        if not self.config.minute_fill or not self.minute_cache:
            return daily_price
        sym = str(self.panel_symbols[idx])
        d_str = self.engine._date_str(self.panel_dates[idx])
        marr = self.minute_cache.get((sym, d_str))
        if marr is None:
            return daily_price
        ref = self._minute_reference(idx)
        precise = self.engine._resolve_minute_fill(marr, ref, side)
        return precise if precise is not None else daily_price

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

    def _count(self, key: str) -> None:
        self.execution_stats[key] = self.execution_stats.get(key, 0) + 1

    @staticmethod
    def _valid_price(value) -> bool:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        return v > 0 and np.isfinite(v)

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

    def _try_close(
        self,
        pos: dict,
        idx: int,
        reason: str,
        signal_date: str,
        override: float | None = None,
        force: bool = False,
    ) -> bool:
        if force:
            ok, block_reason = True, ""
        else:
            ok, block_reason = self._can_sell(idx, override)
        if not ok:
            if not pos.get("pending_exit_reason"):
                pos["pending_exit_reason"] = reason
                pos["pending_exit_signal_date"] = signal_date
                self._count("pending_exit")
            pos["blocked_exit_days"] = int(pos.get("blocked_exit_days", 0)) + 1
            self._count(block_reason)
            return False
        exit_price = (
            float(override)
            if override is not None
            else self._refill_price(idx, "sell", float(self.exit_prices[idx]))
        )
        shares = 100.0
        entry_value = shares * float(pos["entry_price"]) * (1 + self.buy_cost_pct)
        exit_value = shares * exit_price * (1 - self.sell_cost_pct)
        pnl_amount = exit_value - entry_value
        pnl_pct = pnl_amount / entry_value if entry_value > 0 else 0.0
        self.trades.append(
            TradeRecord(
                symbol=str(pos["symbol"]),
                name=str(pos.get("name", "")),
                entry_date=pos["entry_date"],
                exit_date=self.engine._date_str(self.panel_dates[idx]),  # type: ignore[arg-type]
                entry_price=round(float(pos["entry_price"]), 4),
                exit_price=round(exit_price, 4),
                pnl_pct=round(float(pnl_pct), 6),
                duration=int(pos["hold_days"]),
                exit_reason=reason,
                shares=shares,
                lots=1.0,
                position_pct=0.0,
                entry_value=round(float(entry_value), 2),
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
        return True

    def _scheduled_reason(self, pos: dict, idx: int, d_str: str, rows: list[int]):
        if pos.get("pending_exit_reason"):
            return str(pos["pending_exit_reason"]), str(
                pos.get("pending_exit_signal_date") or d_str
            )
        if self.ext[idx]:
            return "signal", str(self.exit_signal_dates[idx] or d_str)
        if self.config.max_hold_days is not None and pos["hold_days"] >= self.config.max_hold_days:
            return "max_hold", d_str
        if idx == rows[-1]:
            return "end", d_str
        return None, d_str

    def _make_position(self, entry_idx: int, sym: str, score: float) -> dict:
        entry_price = self._refill_price(entry_idx, "buy", float(self.entry_prices[entry_idx]))
        pos = {
            "symbol": sym,
            "name": str(self.names[entry_idx] or ""),
            "entry_idx": entry_idx,
            "entry_date": self.engine._date_str(self.panel_dates[entry_idx]),
            "entry_signal_date": self.entry_signal_dates[entry_idx]
            or self.engine._date_str(self.panel_dates[entry_idx]),
            "entry_signal_id": _resolve_signal_id(self.panel, entry_idx, self.entry_signal_ids),
            "entry_price": entry_price,
            "entry_score": score,
            "hold_days": 0,
            "max_high": entry_price,
            "pending_exit_reason": None,
            "pending_exit_signal_date": None,
            "blocked_exit_days": 0,
        }
        hi = float(self.high_prices[entry_idx])
        legacy_intraday_entry = self.config.minute_fill and bool(self.minute_cache)
        if (self.config.entry_fill != "close_t" or legacy_intraday_entry) and self._valid_price(hi):
            pos["max_high"] = max(float(pos["max_high"]), hi)
        return pos

    def _run_exit_loop(
        self, pos: dict, entry_idx: int, rows: list[int], entry_price: float
    ) -> None:
        start_pos = int(self.row_pos_in_symbol[entry_idx])
        closed = False
        last_idx = entry_idx
        for idx in rows[start_pos + 1 :]:
            last_idx = idx
            pos["hold_days"] = int(pos["hold_days"]) + 1
            d_str = self.engine._date_str(self.panel_dates[idx])
            reason, override_price = self._risk_exit(pos, idx)
            if reason and self._try_close(pos, idx, reason, d_str, override_price):
                closed = True
                break
            reason, signal_date = self._scheduled_reason(pos, idx, d_str, rows)
            if reason and self._try_close(pos, idx, reason, signal_date):
                closed = True
                break
            hi = float(self.high_prices[idx])
            if self._valid_price(hi):
                pos["max_high"] = max(float(pos.get("max_high", entry_price)), hi)
        if not closed:
            if last_idx == entry_idx:
                self._count("sell_no_future")
            else:
                self._try_close(
                    pos,
                    last_idx,
                    str(pos.get("pending_exit_reason") or "end"),
                    self.engine._date_str(self.panel_dates[last_idx]),
                    force=True,
                )

    def _run_candidate(self, entry_idx: int, seq: int, total: int) -> None:
        if self.cancel_event is not None and self.cancel_event.is_set():
            logger.info("全量模拟被用户取消 (第 %d/%d 个候选)", seq, total)
            return
        if self.progress_cb is not None and (seq == 1 or seq % 500 == 0):
            with contextlib.suppress(Exception):
                self.progress_cb(
                    {
                        "day": seq,
                        "total": total,
                        "date": self.engine._date_str(self.panel_dates[entry_idx]),
                        "equity": 0,
                    }
                )
        ok, block_reason = self._can_buy(entry_idx)
        if not ok:
            self._count(block_reason)
            return
        score = float(self.trade_scores[entry_idx] or 0.0)
        if self.score_min is not None and score < self.score_min:
            self._count("buy_score_filter")
            return
        if self.score_max is not None and score > self.score_max:
            self._count("buy_score_filter")
            return
        sym = str(self.panel_symbols[entry_idx])
        rows = self.symbol_rows.get(sym, [])
        if int(self.row_pos_in_symbol[entry_idx]) >= len(rows):
            self._count("sell_no_future")
            return
        pos = self._make_position(entry_idx, sym, score)
        self._run_exit_loop(pos, entry_idx, rows, float(pos["entry_price"]))

    def run(self) -> SimResult:
        candidate_indices = np.flatnonzero(self.ent)
        for seq, entry_idx in enumerate(candidate_indices, start=1):
            self._run_candidate(int(entry_idx), seq, len(candidate_indices))
            if self.cancel_event is not None and self.cancel_event.is_set():
                break
        return self.engine._calc_independent_candidate_result(
            self.trades, self.n_candidates, self.execution_stats
        )
