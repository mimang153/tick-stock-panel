from __future__ import annotations

from typing import TYPE_CHECKING, Any

import app.backtest.engine as _root
from app.backtest.engine import (
    Callable,
    MarketMatrix,
    MatcherConfig,
    SimResult,
    SimulationOptions,
    TradeRecord,
    build_market_matrix,
    logger,
    np,
    pl,
    threading,
)


class BacktestSimMixin:
    """BacktestEngine 混入方法组 — 由 engine.py 组合。"""

    if TYPE_CHECKING:

        @staticmethod
        def _empty_result(*args: Any, **kwargs: Any) -> Any: ...
        @staticmethod
        def _calc_stats(*args: Any, **kwargs: Any) -> Any: ...
        @staticmethod
        def _calc_per_symbol(*args: Any, **kwargs: Any) -> Any: ...
        @staticmethod
        def _build_curves(*args: Any, **kwargs: Any) -> Any: ...

    def simulate(
        self,
        panel: pl.DataFrame,
        entries: pl.Series | None,
        exits: pl.Series | None,
        config: MatcherConfig,
        entry_signal_ids: list[str] | None = None,
        exit_signal_ids: list[str] | None = None,
    ) -> SimResult:
        """纯 NumPy 撮合模拟 — 逐 symbol 状态机。"""
        if panel.is_empty():
            return self._empty_result()

        arrays = _root._panel_signal_arrays(panel, entries, exits, config)
        if arrays is None:
            return self._empty_result()
        panel_dates = arrays.dates

        # 逐 symbol 撮合
        trades: list[TradeRecord] = []
        for sym in np.unique(arrays.symbols):
            mask = arrays.symbols == sym
            sym_ent = arrays.ent[mask]
            sym_ext = arrays.ext[mask]
            sym_entry_prices = arrays.entry_prices[mask]
            sym_exit_prices = arrays.exit_prices[mask]
            sym_close = arrays.close_prices[mask]
            sym_dates = panel_dates[mask]

            trades.extend(
                _root._simulate_panel_symbol_trades(
                    sym,
                    sym_ent,
                    sym_ext,
                    sym_entry_prices,
                    sym_exit_prices,
                    sym_close,
                    sym_dates,
                    config,
                )
            )

        # 净值曲线: 按出场日期归集收益
        all_dates_sorted = np.sort(np.unique(panel_dates))
        equity_curve, drawdown_curve = self._build_curves(
            trades, all_dates_sorted, config.initial_capital
        )

        # 统计
        date_min = panel_dates.min()
        date_max = panel_dates.max()
        d_min = date_min.item() if hasattr(date_min, "item") else date_min
        d_max = date_max.item() if hasattr(date_max, "item") else date_max
        stats = self._calc_stats(trades, config.initial_capital, d_min, d_max)
        per_symbol = self._calc_per_symbol(trades)

        return SimResult(
            equity_curve=equity_curve,
            drawdown_curve=drawdown_curve,
            trades=trades,
            per_symbol_stats=per_symbol,
            stats=stats,
        )

    def simulate_independent_candidates(
        self,
        panel: pl.DataFrame,
        entries: pl.Series | None,
        exits: pl.Series | None,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        entry_signal_ids: list[str] | None = None,
        exit_signal_ids: list[str] | None = None,
    ) -> SimResult:
        """Execute every candidate independently on MarketMatrix asset columns."""
        if panel.is_empty():
            return self._empty_result()
        raw_candidates = (
            int(entries.fill_null(False).sum())
            if entries is not None and len(entries) == len(panel)
            else 0
        )
        if raw_candidates <= 0:
            return self._empty_result()
        matrix = build_market_matrix(
            panel,
            entries,
            exits,
            entry_delay_bars=1 if config.entry_fill == "open_t+1" else 0,
            exit_delay_bars=1 if config.exit_fill == "open_t+1" else 0,
            entry_signal_ids=entry_signal_ids,
            exit_signal_ids=exit_signal_ids,
            minute_exit_trigger=config.exit_fill == "signal_next_minute",
        )
        return self._simulate_independent_matrix(
            matrix,
            raw_candidates,
            config,
            progress_cb,
            cancel_event,
        )

    @staticmethod
    def _resolve_entry_prices(matrix: MarketMatrix, config: MatcherConfig) -> np.ndarray:
        """入场价矩阵: 分钟策略的逐格覆盖有限值处优先, 否则按 open/close 惯例。"""
        base = matrix.open if config.entry_fill == "open_t+1" else matrix.close
        if matrix.entry_price is None:
            return base
        return np.where(np.isfinite(matrix.entry_price), matrix.entry_price, base)

    def _simulate_independent_matrix(
        self,
        matrix: MarketMatrix,
        raw_candidates: int,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None,
        cancel_event: threading.Event | None,
        options: SimulationOptions | None = None,
    ) -> SimResult:
        run = _root._IndependentMatrixRun(
            self,
            matrix,
            raw_candidates,
            config,
            options or SimulationOptions(),
            progress_cb,
            cancel_event,
        )
        return run.run()

    def simulate_independent_market_matrix(
        self,
        matrix: MarketMatrix,
        raw_candidates: int,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        options: SimulationOptions | None = None,
    ) -> SimResult:
        """Run independent-candidate simulation on a prebuilt MarketMatrix."""
        return self._simulate_independent_matrix(
            matrix,
            raw_candidates,
            config,
            progress_cb,
            cancel_event,
            options,
        )

    def simulate_independent_candidates_legacy(
        self,
        panel: pl.DataFrame,
        entries: pl.Series | None,
        exits: pl.Series | None,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        entry_signal_ids: list[str] | None = None,
        exit_signal_ids: list[str] | None = None,
    ) -> SimResult:
        """全量候选独立执行：每个买入信号都是独立样本, 不受资金/仓位限制。"""
        if panel.is_empty():
            return self._empty_result()
        run = _root._LegacyIndependentRun(
            self,
            panel,
            entries,
            exits,
            config,
            progress_cb,
            cancel_event,
            entry_signal_ids,
            exit_signal_ids,
        )
        if run.n_candidates <= 0:
            return self._empty_result()
        return run.run()

    # ── 分钟K精确成交 ──────────────────────────────────

    @staticmethod
    def _resolve_minute_fill(
        minute_arr: np.ndarray,
        ref_price: float | None,
        side: str,
    ) -> float | None:
        """用当日分钟K确定精确成交价。

        Args:
            minute_arr: float64 2D 数组, 列顺序 = _MINUTE_NUMERIC_COLS
                        [open(0), high(1), low(2), close(3), volume(4), amount(5)]
                        (缺失的尾部列直接不存在, 用 shape 判断)
            ref_price: 信号参考线价格 (如 MA5 值); None 表示无参考线
            side: "buy" 或 "sell", 决定穿越方向

        Returns:
            精确成交价, 或 None (降级到日K口径)
        """
        if minute_arr is None or len(minute_arr) == 0:
            return None

        ncols = minute_arr.shape[1] if minute_arr.ndim == 2 else 1
        opens = minute_arr[:, 0]
        highs = minute_arr[:, 1] if ncols > 1 else opens
        lows = minute_arr[:, 2] if ncols > 2 else opens
        closes = minute_arr[:, 3] if ncols > 3 else opens
        volumes = minute_arr[:, 4] if ncols > 4 else None
        amounts = minute_arr[:, 5] if ncols > 5 else None

        # 有参考线 → 穿越价成交 (逻辑同止损: 找价格穿越参考线的时刻)
        if ref_price is not None and ref_price > 0 and np.isfinite(ref_price):
            if side == "sell":
                # 卖出: 价格跌破参考线 → 开盘已低于则按开盘; 否则按参考线 (低点触及)
                if np.isfinite(opens[0]) and opens[0] <= ref_price:
                    return float(opens[0])
                if np.any(np.isfinite(lows) & (lows <= ref_price)):
                    return float(ref_price)
            else:
                # 买入: 价格涨破参考线 → 开盘已高于则按开盘; 否则按参考线 (高点触及)
                if np.isfinite(opens[0]) and opens[0] >= ref_price:
                    return float(opens[0])
                if np.any(np.isfinite(highs) & (highs >= ref_price)):
                    return float(ref_price)
            # 参考线存在但当日分钟K未穿越 → 用收盘 (信号确认)
            return float(closes[-1]) if np.isfinite(closes[-1]) else None

        # 无参考线 → VWAP (成交额/成交量), 退化到收盘价
        if volumes is not None and amounts is not None:
            total_vol = float(np.nansum(volumes))
            total_amt = float(np.nansum(amounts))
            if total_vol > 0 and total_amt > 0:
                # volume 单位是手 (1 手 = 100 股), amount 是元 — 与 scoring/
                # intraday_features/matrix 的 VWAP 同口径; 缺 x100 会放大 100 倍 (#387)
                return total_amt / (total_vol * 100.0)

        return float(closes[-1]) if np.isfinite(closes[-1]) else None

    @staticmethod
    def _resolve_minute_exit_trigger(
        minute_arr: np.ndarray,
        ref_price: float | None,
    ) -> float | None:
        """分钟收盘确认向下穿越后，返回下一分钟开盘价。"""
        if minute_arr is None or len(minute_arr) < 2:
            return None
        if ref_price is None or not np.isfinite(ref_price) or ref_price <= 0:
            return None

        ncols = minute_arr.shape[1] if minute_arr.ndim == 2 else 1
        if ncols < 4:
            return None
        opens = minute_arr[:, 0]
        closes = minute_arr[:, 3]
        below = np.isfinite(closes) & (closes < ref_price)
        previous_above = np.empty(len(closes), dtype=bool)
        previous_above[0] = True
        previous_above[1:] = np.isfinite(closes[:-1]) & (closes[:-1] >= ref_price)
        crossings = np.flatnonzero(below & previous_above)
        if crossings.size == 0:
            return None
        next_idx = int(crossings[0]) + 1
        if next_idx >= len(opens) or not np.isfinite(opens[next_idx]) or opens[next_idx] <= 0:
            return None
        return float(opens[next_idx])

    # 分钟K cache 存储的数值列及固定顺序 (_resolve_minute_fill 按此顺序整数索引)。
    _MINUTE_NUMERIC_COLS = ["open", "high", "low", "close", "volume", "amount"]  # noqa: RUF012 - RUF012: 类属性为可变配置, 实例隔离

    @staticmethod
    def _load_minute_for_fills(
        repo,
        symbols: list[str],
        dates_needed: set,
        asset_type: str,
        *,
        evidence=None,
    ) -> dict:
        """按触发日加载分钟K, 返回 {(symbol, date_str): float64 2D ndarray}。

        dates_needed: 需要分钟数据的日期集合 (set of date strings "YYYY-MM-DD")

        按触发日分批读取对应分区文件 (get_minute_by_dates), 而非扫描整个区间
        (get_minute_range)。内存与回测区间长度解耦, 只随触发日数量增长 ——
        触发日稀疏时避免读取区间内大量无关日期导致爆内存。

        cache 值为 float64 紧凑 2D 数组 (列顺序见 _MINUTE_NUMERIC_COLS), 而非
        完整 DataFrame, 避免每条记录携带 polars 元数据开销导致内存膨胀。
        """
        if not symbols or not dates_needed:
            return {}
        from datetime import date as _date

        sorted_date_strs = sorted(dates_needed)
        date_objs = [_date.fromisoformat(s) for s in sorted_date_strs]

        cache: dict = {}
        # 分批读取: 每批 50 个交易日, 处理完拼进 cache, 避免单批过大。
        BATCH = 50  # noqa: N806 - N806: 大写名为领域约定常量语义
        numeric_cols = _root.BacktestEngine._MINUTE_NUMERIC_COLS
        for i in range(0, len(date_objs), BATCH):
            batch = date_objs[i : i + BATCH]
            try:
                df = repo.get_minute_by_dates(symbols, batch, asset_type=asset_type)
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning(
                    "minute fill data load failed (batch %d-%d): %s", i, i + len(batch), e
                )
                continue
            if df.is_empty():
                continue
            if evidence is not None:
                evidence.record_frame(f"minute_fill/{asset_type}/{batch[0]}/{batch[-1]}", df)
            # 按 (symbol, 日期) 分组, 每组转紧凑 float64 数组存入 cache
            df = df.sort(["symbol", "datetime"]).with_columns(
                pl.col("datetime").dt.strftime("%Y-%m-%d").alias("_d_str")
            )
            for sub in df.partition_by(["symbol", "_d_str"], as_dict=False):
                if sub.is_empty():
                    continue
                sym = sub["symbol"][0]
                d_str = sub["_d_str"][0]
                cols = [c for c in numeric_cols if c in sub.columns]
                cache[(sym, d_str)] = sub.select(
                    [pl.col(c).cast(pl.Float64) for c in cols]
                ).to_numpy()
        return cache

    def simulate_portfolio(
        self,
        panel: pl.DataFrame,
        entries: pl.Series | None,
        exits: pl.Series | None,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        entry_signal_ids: list[str] | None = None,
        exit_signal_ids: list[str] | None = None,
    ) -> SimResult:
        """Account-level matcher backed by immutable ``time x asset`` arrays."""
        if panel.is_empty():
            return self._empty_result()

        matrix = build_market_matrix(
            panel,
            entries,
            exits,
            entry_delay_bars=1 if config.entry_fill == "open_t+1" else 0,
            exit_delay_bars=1 if config.exit_fill == "open_t+1" else 0,
            entry_signal_ids=entry_signal_ids,
            exit_signal_ids=exit_signal_ids,
            minute_exit_trigger=config.exit_fill == "signal_next_minute",
        )
        if not matrix.entry.any():
            return self._empty_result()
        return self._simulate_portfolio_matrix(matrix, config, progress_cb, cancel_event)

    def simulate_market_matrix(
        self,
        matrix: MarketMatrix,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        options: SimulationOptions | None = None,
    ) -> SimResult:
        """Run the production Python matcher on a prebuilt MarketMatrix."""
        if not matrix.entry.any():
            return self._empty_result()
        return self._simulate_portfolio_matrix(matrix, config, progress_cb, cancel_event, options)

    def _simulate_portfolio_matrix(
        self,
        matrix: MarketMatrix,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None,
        cancel_event: threading.Event | None,
        options: SimulationOptions | None = None,
    ) -> SimResult:
        run = _root._MatrixPortfolioRun(
            self, matrix, config, options or SimulationOptions(), progress_cb, cancel_event
        )
        return run.run()

    def simulate_portfolio_legacy(
        self,
        panel: pl.DataFrame,
        entries: pl.Series | None,
        exits: pl.Series | None,
        config: MatcherConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        entry_signal_ids: list[str] | None = None,
        exit_signal_ids: list[str] | None = None,
    ) -> SimResult:
        """账户级组合回测：日线信号 → 成交约束 → 仓位/现金撮合。"""
        if panel.is_empty():
            return self._empty_result()
        run = _root._LegacyPortfolioRun(
            self,
            panel,
            entries,
            exits,
            config,
            progress_cb,
            cancel_event,
            entry_signal_ids,
            exit_signal_ids,
        )
        if not run.ent_raw.any() or not run.all_dates:
            return self._empty_result()
        return run.run()
