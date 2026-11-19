from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import numpy as np
import polars as pl

from app.backtest.engine import MatcherConfig
from app.backtest.matrix import (
    SignalMatrix,
    build_market_data_matrix,
    build_market_matrix,
    build_market_matrix_from_encoding,
    build_market_matrix_from_signals,
)
from app.backtest.minute_replay import (
    MinuteSignalReplayer,
    minute_panel_start,
    minute_replay_feature_plan,
)
from app.strategy.engine import StrategyDef

from ._common import (
    BacktestResultPolicy,
    PreparedMatrixBacktest,
    PreparedPanelBacktest,
    StrategyBacktestConfig,
    StrategyBacktestResult,
    _factor_attribution_summary,
    _panel_backend,
    _resolve_position_policy,
    _RunState,
    logger,
)


class StrategyBacktestRunTailMixin:
    """主流程后半段：panel 掩码、成交、结算与分钟回测。"""

    # 由组装类/其他 mixin 提供, 拆分后在此显式声明以对 mypy 可见。
    strategy_engine: Any
    engine: Any
    _evaluate_panel_decisions: Any
    _decision_frame: Any
    _build_regime_mask: Any
    _date_range_mask: Any
    _config_to_dict: Any
    _build_benchmark_curve: Any
    _trade_to_dict: Any
    _run_resolve_strategy_and_signals: Any
    _run_resolve_stops: Any
    _run_resolve_feature_plan: Any
    _run_prepare_window: Any
    _run_load_matrix_prepared: Any
    _run_load_matrix_fresh: Any
    _run_load_panel: Any
    _run_build_matcher: Any
    _run_composite_time_masks: Any
    _run_composite_market_matrix: Any
    _run_matrix_time_masks: Any
    _run_matrix_compute_signals: Any
    _run_matrix_apply_masks: Any

    def _run_panel_decisions(self, state: _RunState):
        """面板分支: 评估策略候选/评分/进出场掩码。"""
        if not _panel_backend(state.s.execution_backend):
            return None
        s = state.s
        params = state.params
        overrides = state.overrides
        basic_filter = state.basic_filter
        panel = state.panel
        formal_range = state.formal_range
        if panel is None or formal_range is None:
            return state.err("非矩阵策略不能使用共享基础矩阵")
        try:
            panel, candidate_mask, entry_mask, raw_exit_mask = self._evaluate_panel_decisions(
                panel,
                s,
                params,
                overrides,
                basic_filter,
                state.entry_signals,
                state.exit_signals,
                factor_snapshot=state.factor_snapshot,
            )
        except ValueError as exc:
            return state.err(str(exc))
        if state.evidence is not None:
            state.evidence.check_panel(
                self._decision_frame(panel, candidate_mask, entry_mask, raw_exit_mask),
                lambda data: self._decision_frame(
                    *self._evaluate_panel_decisions(
                        data,
                        s,
                        params,
                        overrides,
                        basic_filter,
                        state.entry_signals,
                        state.exit_signals,
                    )
                ),
            )
        state.panel = panel
        state.candidate_mask = candidate_mask
        state.entry_mask = entry_mask
        state.raw_exit_mask = raw_exit_mask
        return None

    def _run_panel_masks(self, state: _RunState):
        """面板分支: 叠加环境过滤并推导模拟窗口与选择统计。"""
        if not _panel_backend(state.s.execution_backend):
            return None
        config = state.config
        panel = state.panel
        formal_range = state.formal_range
        candidate_mask = state.candidate_mask
        entry_mask = state.entry_mask
        raw_exit_mask = state.raw_exit_mask
        formal_candidate_mask = candidate_mask & formal_range
        entry_mask = entry_mask & formal_range
        # 叠加条件: 在裁剪前的完整 panel (ext_*/因子列已在, polars 路径经
        # compute_signals 附带 ext 列) 上评估, 只 AND 进 entry — exit 掩码
        # 独立构建, 已持仓卖出不受影响 (与 basic_filter 同口径)。
        if config.regime_filter:
            date_values = panel.get_column("date").unique().sort().to_list()
            date_labels = tuple(str(value)[:10] for value in date_values)
            try:
                regime_time_mask = self._build_regime_mask(
                    date_labels,
                    config.regime_filter,
                    getattr(getattr(self.engine.repo, "store", None), "data_dir", None),
                    required_start=config.start,
                    required_end=config.end,
                    evidence=state.evidence,
                )
            except ValueError as e:
                return state.err(str(e))
            if regime_time_mask is not None:
                allowed_dates = [
                    value
                    for value, allowed in zip(date_values, regime_time_mask, strict=True)
                    if allowed
                ]
                regime_row_mask = panel.get_column("date").is_in(allowed_dates).fill_null(False)
                formal_candidate_mask = formal_candidate_mask & regime_row_mask
                entry_mask = entry_mask & regime_row_mask
        exit_range = (
            self._date_range_mask(panel, config.start, state.load_end)
            if config.mode == "full"
            else formal_range
        )
        exit_mask = raw_exit_mask & exit_range
        state.timing_ms["signals_score"] = round((time.perf_counter() - state.t_signal) * 1000, 1)
        if not entry_mask.any():
            return state.err("在指定区间内未产生买入信号")
        sim_range = self._date_range_mask(panel, config.start, state.sim_end)
        sim_columns = [
            column for column in state.feature_plan.matrix_columns if column in panel.columns
        ]
        sim_panel = panel.filter(sim_range).select(sorted(sim_columns))
        sim_entry_mask = entry_mask.filter(sim_range)
        sim_exit_mask = exit_mask.filter(sim_range)
        if sim_panel.is_empty():
            return state.err("正式回测区间内无数据")
        state.panel_rows = int(sim_panel.height)
        state.panel_columns = int(sim_panel.width)
        state.raw_candidates = int(sim_entry_mask.sum())
        strategy_matches = int(formal_candidate_mask.sum())
        state.selection_stats = {
            "strategy_matches": strategy_matches,
            "entry_candidates": state.raw_candidates,
            "entry_trigger_filtered": max(strategy_matches - state.raw_candidates, 0),
            "entry_trigger_enabled": bool(state.entry_signals),
        }
        state.sim_columns = sim_columns
        state.sim_panel = sim_panel
        state.sim_entry_mask = sim_entry_mask
        state.sim_exit_mask = sim_exit_mask
        return None

    def _run_panel_market_matrix(self, state: _RunState):
        """面板分支: 构建市场矩阵 (优先复用共享编码)。"""
        if not _panel_backend(state.s.execution_backend):
            return None
        t_matrix = time.perf_counter()
        shared_encoding = (
            state.prepared_panel.sim_encoding if state.prepared_panel is not None else None
        )
        # score 列只在 run 侧存在 (_apply_score 产物), 编码面板没有 —
        # 重构后两路径的市场矩阵都不含 score 场, 比对忽略它。
        _enc_cols = (
            tuple(c for c in shared_encoding.sim_columns if c != "score")
            if shared_encoding is not None
            else None
        )
        sim_columns = state.sim_columns
        sim_panel = state.sim_panel
        sim_entry_mask = state.sim_entry_mask
        sim_exit_mask = state.sim_exit_mask
        if (
            shared_encoding is not None
            and _enc_cols == tuple(c for c in sorted(sim_columns) if c != "score")
            and len(sim_entry_mask) == shared_encoding.panel_rows
        ):
            # P2a-2: 共享矩阵编码 — 只散布随参数变化的 score/entry/exit。
            # 列序或行数不一致 (计划漂移) 时回退整建, 回退是优化放弃而非
            # 正确性闸门, 整建结果总是正确。
            score_series = (
                sim_panel.get_column("score")
                if "score" in sim_panel.columns and sim_panel["score"].dtype.is_numeric()
                else None
            )
            state.market_matrix = build_market_matrix_from_encoding(
                shared_encoding,
                score=score_series,
                entries=sim_entry_mask,
                exits=sim_exit_mask,
                entry_delay_bars=1 if state.matcher_config.entry_fill == "open_t+1" else 0,
                exit_delay_bars=1 if state.matcher_config.exit_fill == "open_t+1" else 0,
                minute_exit_trigger=state.matcher_config.exit_fill == "signal_next_minute",
            )
        else:
            state.market_matrix = build_market_matrix(
                sim_panel,
                sim_entry_mask,
                sim_exit_mask,
                entry_delay_bars=1 if state.matcher_config.entry_fill == "open_t+1" else 0,
                exit_delay_bars=1 if state.matcher_config.exit_fill == "open_t+1" else 0,
                entry_signal_ids=state.entry_signals,
                exit_signal_ids=state.exit_signals,
                minute_exit_trigger=state.matcher_config.exit_fill == "signal_next_minute",
            )
        state.timing_ms["matrix_build"] = round((time.perf_counter() - t_matrix) * 1000, 1)
        del sim_panel, sim_entry_mask, sim_exit_mask
        return None

    def _run_simulate(self, state: _RunState):
        """撮合市场矩阵；取消或引擎报错时返回错误结果。"""
        t_sim = time.perf_counter()
        if state.config.mode == "full":
            result = self.engine.simulate_independent_market_matrix(
                state.market_matrix,
                state.raw_candidates,
                state.matcher_config,
                state.progress_cb,
                state.cancel_event,
                state.result_policy.simulation_options(),
            )
        else:
            result = self.engine.simulate_market_matrix(
                state.market_matrix,
                state.matcher_config,
                state.progress_cb,
                state.cancel_event,
                state.result_policy.simulation_options(),
            )
        state.timing_ms["simulate"] = round((time.perf_counter() - t_sim) * 1000, 1)
        state.timing_ms["statistics"] = float(result.stats.pop("statistics_ms", 0.0))
        if state.cancel_event is not None and state.cancel_event.is_set():
            return StrategyBacktestResult(
                run_id=state.run_id,
                config=self._config_to_dict(state.config),
                error="cancelled",
                elapsed_ms=round((time.perf_counter() - state.t0) * 1000, 1),
            )
        if result.stats.get("error"):
            return state.err(result.stats["error"])
        state.result = result
        return None

    def _run_populate_stats(self, state: _RunState) -> None:
        """把计时与上下文指标写入引擎结果 stats。"""
        result = state.result
        state.timing_ms["total"] = round((time.perf_counter() - state.t0) * 1000, 1)
        result.stats["timing_ms"] = state.timing_ms
        result.stats["panel_rows"] = state.panel_rows
        result.stats["panel_columns"] = state.panel_columns
        result.stats["feature_columns"] = state.feature_width
        result.stats["full_feature_fallback"] = state.feature_plan.full_feature_fallback
        result.stats["execution_backend"] = state.s.execution_backend
        result.stats["selection"] = state.selection_stats
        result.stats["shared_market_data"] = state.prepared is not None
        result.stats["matrix_data_cache_hit"] = state.matrix_data_cache_hit
        result.stats["matrix_data_cache_status"] = state.matrix_data_cache_status
        result.stats["matrix_data_cache_timing_ms"] = dict(state.matrix_data_cache_timing_ms)
        if state.prepared is not None:
            result.stats["shared_market_data_bytes"] = state.prepared.market_data.nbytes
            result.stats["shared_prepare_timing_ms"] = state.prepared.prepare_timing_ms
            result.stats["matrix_compute_cache"] = state.prepared.compute_cache.snapshot()

    def _run_build_strategy_info(self, state: _RunState) -> None:
        """构建基准曲线与策略信息（受结果策略裁剪）。"""
        config = state.config
        s = state.s
        state.benchmark_curve = (
            self._build_benchmark_curve(config.start, config.end)
            if state.result_policy.include_benchmark
            else []
        )
        state.strategy_info = (
            {
                "id": s.meta.get("id", config.strategy_id),
                "name": s.meta.get("name", config.strategy_id),
                "description": s.meta.get("description", ""),
                "entry_signals": state.entry_signals,
                "exit_signals": state.exit_signals,
                "stop_loss": state.stop_loss,
                "take_profit": state.take_profit,
                "trailing_stop": state.trailing_stop,
                "trailing_take_profit_activate": state.trailing_take_profit_activate,
                "trailing_take_profit_drawdown": state.trailing_take_profit_drawdown,
                "max_hold_days": state.max_hold_days,
                "full_horizon_days": state.full_horizon_days,
                "score_min": state.score_min,
                "score_max": state.score_max,
                "source": s.source,
                "execution_backend": s.execution_backend,
                **(
                    {
                        "composite_children": [
                            {
                                "id": cid,
                                "weight": cw,
                            }
                            for cid, cw in getattr(self, "_composite_children_weights", [])
                        ]
                    }
                    if s.execution_backend == "composite"
                    else {}
                ),
            }
            if state.result_policy.include_strategy_info
            else {}
        )

    def _run_finalize(self, state: _RunState) -> StrategyBacktestResult:
        """裁剪统计、计算因子归因并组装最终返回结果。"""
        result = state.result
        selected_stats = state.result_policy.select_stats(result.stats)
        # 因子归因 (fail-open): 快照与成交按信号日关联, 失败只记日志不影响结果
        factor_attribution = None
        if state.factor_snapshot and result.trades and state.result_policy.include_trades:
            try:
                factor_attribution = _factor_attribution_summary(
                    state.factor_snapshot["frame"], result.trades
                )
            except Exception as exc:  # noqa: BLE001 - 归因失败不影响回测主结果
                logger.warning("factor attribution failed: %s", exc)
        elapsed = (time.perf_counter() - state.t0) * 1000
        return StrategyBacktestResult(
            run_id=state.run_id,
            config=self._config_to_dict(state.config),
            stats=selected_stats,
            equity_curve=result.equity_curve if state.result_policy.include_curves else [],
            drawdown_curve=result.drawdown_curve if state.result_policy.include_curves else [],
            benchmark_curve=state.benchmark_curve,
            trades=(
                [self._trade_to_dict(t) for t in result.trades]
                if state.result_policy.include_trades
                else []
            ),
            per_symbol_stats=(
                result.per_symbol_stats if state.result_policy.include_per_symbol_stats else []
            ),
            strategy_info=state.strategy_info,
            factor_attribution=factor_attribution,
            elapsed_ms=round(elapsed, 1),
        )

    def _run_call_minute_backtest(self, state: _RunState) -> StrategyBacktestResult:
        """把已解析的入场参数交给分钟回测子流程。"""
        return self._run_minute_backtest(
            state.config,
            state.s,
            state.params,
            state.overrides,
            stop_loss=state.stop_loss,
            take_profit=state.take_profit,
            trailing_stop=state.trailing_stop,
            trailing_take_profit_activate=state.trailing_take_profit_activate,
            trailing_take_profit_drawdown=state.trailing_take_profit_drawdown,
            max_hold_days=state.max_hold_days,
            score_min=state.score_min,
            score_max=state.score_max,
            progress_cb=state.progress_cb,
            cancel_event=state.cancel_event,
            result_policy=state.result_policy,
            run_id=state.run_id,
            t0=state.t0,
            evidence=state.evidence,
        )

    def run(
        self,
        config: StrategyBacktestConfig,
        progress_cb: Callable[[dict], None] | None = None,
        cancel_event: threading.Event | None = None,
        prepared: PreparedMatrixBacktest | None = None,
        result_policy: BacktestResultPolicy | None = None,
        prepared_panel: PreparedPanelBacktest | None = None,
        evidence=None,
    ) -> StrategyBacktestResult:
        t0 = time.perf_counter()
        run_id = uuid.uuid4().hex[:10]
        result_policy = result_policy or BacktestResultPolicy()
        # 因子归因快照容器: 日线路径在 _apply_score 里填充, 其余路径保持空
        factor_snapshot: dict = {}

        def _err(msg: str) -> StrategyBacktestResult:
            return StrategyBacktestResult(
                run_id=run_id,
                config=self._config_to_dict(state.config),
                error=msg,
                elapsed_ms=(time.perf_counter() - t0) * 1000,
            )

        state = _RunState(
            config=config,
            t0=t0,
            run_id=run_id,
            result_policy=result_policy,
            progress_cb=progress_cb,
            cancel_event=cancel_event,
            prepared=prepared,
            prepared_panel=prepared_panel,
            evidence=evidence,
            factor_snapshot=factor_snapshot,
            err=_err,
        )

        for stage in (
            self._run_resolve_strategy_and_signals,
            self._run_resolve_stops,
        ):
            if (result := stage(state)) is not None:
                return result
        if state.s.execution_backend == "minute_filter":
            return self._run_call_minute_backtest(state)
        if (result := self._run_resolve_feature_plan(state)) is not None:
            return result
        self._run_prepare_window(state)
        for stage in (
            self._run_load_matrix_prepared,
            self._run_load_matrix_fresh,
            self._run_load_panel,
        ):
            if (result := stage(state)) is not None:
                return result
        self._run_build_matcher(state)
        state.t_signal = time.perf_counter()
        for stage in (
            self._run_composite_time_masks,
            self._run_composite_market_matrix,
            self._run_matrix_time_masks,
            self._run_matrix_compute_signals,
            self._run_matrix_apply_masks,
            self._run_panel_decisions,
            self._run_panel_masks,
            self._run_panel_market_matrix,
            self._run_simulate,
        ):
            if (result := stage(state)) is not None:
                return result
        self._run_populate_stats(state)
        self._run_build_strategy_info(state)
        return self._run_finalize(state)

    # ── 分钟策略回测: 逐日回放入场 + 日K矩阵离场 ──

    def _run_minute_load_and_replay(self, state: _RunState):
        """检查分钟K覆盖、加载日线面板并回放分钟信号。"""
        config = state.config
        s = state.s
        minute_days = self.engine.repo.list_minute_dates(config.start, config.end, "stock")
        if not minute_days:
            earliest = self.engine.repo.earliest_minute_date()
            hint = f"本地分钟K最早到 {earliest}, " if earliest else "本地无分钟K数据, "
            return state.err(
                f"回测区间内无分钟K数据: {hint}请先用「扩展分钟K历史」拉取, 或开启盘中分钟增量"
            )
        # 日线面板一次加载: 覆盖首个回测日的日线窗口 + 模拟区间 (含 full 模式尾部)。
        daily_bars = int(s.minute_daily_bars or 0)
        feature_plan = minute_replay_feature_plan(daily_bars)
        load_start = minute_panel_start(config.start, daily_bars)
        full_horizon_days = int(state.max_hold_days or config.holding_days or 5)
        state.full_horizon_days = full_horizon_days
        load_end = config.end
        if config.mode == "full":
            load_end = config.end + timedelta(days=(full_horizon_days + 5) * 2)
        sim_end = load_end if config.mode == "full" else config.end
        state.sim_end = sim_end
        timing_ms: dict[str, float] = {}
        state.timing_ms = timing_ms
        t_load = time.perf_counter()
        try:
            panel = self.engine.load_panel_for_backtest(
                config.symbols,
                load_start,
                load_end,
                feature_plan,
                asset_type="stock",
                **({"evidence": state.evidence} if state.evidence is not None else {}),
            )
        except (ValueError, OSError, pl.exceptions.PolarsError) as e:
            return state.err(f"回测特征准备失败: {e}")
        timing_ms["load_panel"] = round((time.perf_counter() - t_load) * 1000, 1)
        if panel.is_empty():
            return state.err("无日线数据, 请检查日期范围或先运行盘后管道")
        if state.evidence is not None:
            state.evidence.record_frame("minute_daily_features", panel)
        replayer = MinuteSignalReplayer(self.engine, self.strategy_engine)
        replay = replayer.replay(
            s,
            panel=panel,
            start=config.start,
            end=config.end,
            params=state.params,
            overrides=state.overrides,
            symbols=config.symbols,
            progress_cb=state.progress_cb,
            cancel_event=state.cancel_event,
            **({"evidence": state.evidence} if state.evidence is not None else {}),
        )
        timing_ms["minute_replay"] = replay.elapsed_ms
        if state.cancel_event is not None and state.cancel_event.is_set():
            return StrategyBacktestResult(
                run_id=state.run_id,
                config=self._config_to_dict(config),
                error="cancelled",
                elapsed_ms=round((time.perf_counter() - state.t0) * 1000, 1),
            )
        if not replay.hits:
            skipped_hint = (
                f" (区间内 {len(replay.skipped_days)} 个交易日缺分钟K分区被跳过)"
                if replay.skipped_days
                else ""
            )
            return state.err("在指定区间内未产生买入信号" + skipped_hint)
        state.panel = panel
        state.replay = replay
        return None

    def _run_minute_build_signals(self, state: _RunState):
        """把分钟命中写入日频信号网格并构造入场价覆盖。"""
        config = state.config
        sim_panel = state.panel.filter(
            (pl.col("date") >= config.start) & (pl.col("date") <= state.sim_end)
        )
        if sim_panel.is_empty():
            return state.err("正式回测区间内无数据")
        axis_dates = sim_panel.get_column("date").unique().sort().to_list()
        # 轴顺序必须与 build_market_data_matrix 的 _encode_axes 一致 (unique().sort()),
        # 否则 (time, asset) 下标指向错误的标的。
        axis_symbols = sim_panel.get_column("symbol").cast(pl.Utf8).unique().sort().to_list()
        time_index = {day: i for i, day in enumerate(axis_dates)}
        asset_index = {sym: i for i, sym in enumerate(axis_symbols)}
        shape = (len(axis_dates), len(axis_symbols))
        entry = np.zeros(shape, dtype=np.uint8)
        score = np.zeros(shape, dtype=np.float32)
        entry_price_override = np.full(shape, np.nan, dtype=np.float32)
        trigger_times: dict[tuple[str, date], str] = {}
        dropped_axis_hits = 0
        for hit in state.replay.hits:
            time_id = time_index.get(hit.trade_date)
            asset_id = asset_index.get(hit.symbol)
            if time_id is None or asset_id is None:
                dropped_axis_hits += 1
                continue
            entry[time_id, asset_id] = 1
            score[time_id, asset_id] = hit.score
            entry_price_override[time_id, asset_id] = hit.entry_price
            trigger_times[(hit.symbol, hit.trade_date)] = hit.trigger_time
        raw_candidates = int(entry.sum())
        entry.setflags(write=False)
        score.setflags(write=False)
        entry_price_override.setflags(write=False)
        exit_mask = np.zeros(shape, dtype=np.uint8)
        exit_mask.setflags(write=False)
        codes = np.zeros(shape, dtype=np.int16)
        codes.setflags(write=False)
        state.signals = SignalMatrix(
            entry=entry,
            exit=exit_mask,
            score=score,
            entry_signal_code=codes,
            exit_signal_code=codes,
            entry_signal_ids=(),
            exit_signal_ids=(),
        )
        state.minute_sim_panel = sim_panel
        state.entry_price_override = entry_price_override
        state.axis_dates = axis_dates
        state.axis_symbols = axis_symbols
        state.trigger_times = trigger_times
        state.dropped_axis_hits = dropped_axis_hits
        state.raw_candidates = raw_candidates
        return None

    def _run_minute_market_matrix(self, state: _RunState) -> None:
        """构造分钟路径撮合配置与市场矩阵。"""
        config = state.config
        position_policy_compute, position_policy_params = _resolve_position_policy(config)
        state.matcher_config = MatcherConfig(
            matching=config.matching,
            entry_fill="close_t",
            exit_fill=config.exit_fill,
            fees_pct=config.fees_pct,
            commission_pct=config.commission_pct,
            stamp_tax_pct=config.stamp_tax_pct,
            slippage_bps=config.slippage_bps,
            stop_loss_pct=state.stop_loss,
            take_profit_pct=state.take_profit,
            trailing_stop_pct=state.trailing_stop,
            trailing_take_profit_activate_pct=state.trailing_take_profit_activate,
            trailing_take_profit_drawdown_pct=state.trailing_take_profit_drawdown,
            max_hold_days=state.max_hold_days,
            max_positions=config.max_positions,
            max_exposure_pct=config.max_exposure_pct,
            score_min=state.score_min,
            score_max=state.score_max,
            initial_capital=config.initial_capital,
            position_sizing=config.position_sizing,
            position_policy_compute=position_policy_compute,
            position_policy_params=position_policy_params,
            # 分钟策略的成交价由 entry_price_override 提供 (触发分钟收盘),
            # 不再叠加日线口径的分钟成交细化。
            minute_fill=False,
        )
        t_matrix = time.perf_counter()
        market_data = build_market_data_matrix(state.minute_sim_panel)
        state.market_matrix = build_market_matrix_from_signals(
            market_data,
            state.signals,
            # 入场即信号日盘中 (分钟价覆盖), 离场沿用日K口径。
            entry_delay_bars=0,
            exit_delay_bars=1 if state.matcher_config.exit_fill == "open_t+1" else 0,
            entry_price_override=state.entry_price_override,
        )
        state.timing_ms["matrix_build"] = round((time.perf_counter() - t_matrix) * 1000, 1)

    def _run_minute_simulate(self, state: _RunState):
        """撮合市场矩阵；取消或引擎报错时返回错误结果。"""
        t_sim = time.perf_counter()
        if state.config.mode == "full":
            result = self.engine.simulate_independent_market_matrix(
                state.market_matrix,
                state.raw_candidates,
                state.matcher_config,
                state.progress_cb,
                state.cancel_event,
                state.result_policy.simulation_options(),
            )
        else:
            result = self.engine.simulate_market_matrix(
                state.market_matrix,
                state.matcher_config,
                state.progress_cb,
                state.cancel_event,
                state.result_policy.simulation_options(),
            )
        state.timing_ms["simulate"] = round((time.perf_counter() - t_sim) * 1000, 1)
        state.timing_ms["statistics"] = float(result.stats.pop("statistics_ms", 0.0))
        if state.cancel_event is not None and state.cancel_event.is_set():
            return StrategyBacktestResult(
                run_id=state.run_id,
                config=self._config_to_dict(state.config),
                error="cancelled",
                elapsed_ms=round((time.perf_counter() - state.t0) * 1000, 1),
            )
        if result.stats.get("error"):
            return state.err(result.stats["error"])
        state.result = result
        return None

    def _run_minute_populate_stats(self, state: _RunState) -> None:
        """写入成交限制计数与回测统计元数据。"""
        result = state.result
        execution = result.stats.get("execution") or {}
        execution["buy_limit_up"] = (
            int(execution.get("buy_limit_up", 0)) + state.replay.buy_limit_up
        )
        result.stats["execution"] = execution
        state.timing_ms["total"] = round((time.perf_counter() - state.t0) * 1000, 1)
        result.stats["timing_ms"] = state.timing_ms
        result.stats["panel_rows"] = int(len(state.axis_dates) * len(state.axis_symbols))
        result.stats["panel_columns"] = 0
        result.stats["feature_columns"] = 0
        result.stats["execution_backend"] = state.s.execution_backend
        result.stats["selection"] = {
            "strategy_matches": state.replay.strategy_matches,
            "entry_candidates": state.raw_candidates,
            "entry_trigger_filtered": max(state.replay.strategy_matches - state.raw_candidates, 0),
            "entry_trigger_enabled": False,
        }
        result.stats["minute_replay"] = {
            "replayed_days": state.replay.replayed_days,
            "skipped_days": [str(day) for day in state.replay.skipped_days[:50]],
            "skipped_day_count": len(state.replay.skipped_days),
            "dropped_axis_hits": state.dropped_axis_hits,
        }

    def _run_minute_finalize(self, state: _RunState) -> StrategyBacktestResult:
        """构建策略信息、补分钟入场时间并组装最终结果。"""
        config = state.config
        s = state.s
        result = state.result
        benchmark_curve = (
            self._build_benchmark_curve(config.start, config.end)
            if state.result_policy.include_benchmark
            else []
        )
        strategy_info = (
            {
                "id": s.meta.get("id", config.strategy_id),
                "name": s.meta.get("name", config.strategy_id),
                "description": s.meta.get("description", ""),
                "entry_signals": [],
                "exit_signals": [],
                "stop_loss": state.stop_loss,
                "take_profit": state.take_profit,
                "trailing_stop": state.trailing_stop,
                "trailing_take_profit_activate": state.trailing_take_profit_activate,
                "trailing_take_profit_drawdown": state.trailing_take_profit_drawdown,
                "max_hold_days": state.max_hold_days,
                "full_horizon_days": state.full_horizon_days,
                "score_min": state.score_min,
                "score_max": state.score_max,
                "source": s.source,
                "execution_backend": s.execution_backend,
            }
            if state.result_policy.include_strategy_info
            else {}
        )
        trades = (
            [self._trade_to_dict(t) for t in result.trades]
            if state.result_policy.include_trades
            else []
        )
        # 入场时间戳补分钟: 交易记录携带触发分钟 (HH:MM), 与日线回测的纯日期区分。
        for trade in trades:
            entry_text = str(trade.get("entry_date") or "")
            try:
                key = (str(trade.get("symbol")), date.fromisoformat(entry_text[:10]))
            except ValueError:
                continue
            trigger = state.trigger_times.get(key)
            if trigger:
                trade["entry_date"] = f"{entry_text[:10]} {trigger}"
        selected_stats = state.result_policy.select_stats(result.stats)
        elapsed = (time.perf_counter() - state.t0) * 1000
        return StrategyBacktestResult(
            run_id=state.run_id,
            config=self._config_to_dict(config),
            stats=selected_stats,
            equity_curve=result.equity_curve if state.result_policy.include_curves else [],
            drawdown_curve=result.drawdown_curve if state.result_policy.include_curves else [],
            benchmark_curve=benchmark_curve,
            trades=trades,
            per_symbol_stats=(
                result.per_symbol_stats if state.result_policy.include_per_symbol_stats else []
            ),
            strategy_info=strategy_info,
            elapsed_ms=round(elapsed, 1),
        )

    def _run_minute_backtest(
        self,
        config: StrategyBacktestConfig,
        s: StrategyDef,
        params: dict,
        overrides: dict,
        *,
        stop_loss,
        take_profit,
        trailing_stop,
        trailing_take_profit_activate,
        trailing_take_profit_drawdown,
        max_hold_days,
        score_min,
        score_max,
        progress_cb,
        cancel_event,
        result_policy: BacktestResultPolicy,
        run_id: str,
        t0: float,
        evidence=None,
    ) -> StrategyBacktestResult:
        def _err(msg: str) -> StrategyBacktestResult:
            return StrategyBacktestResult(
                run_id=run_id,
                config=self._config_to_dict(config),
                error=msg,
                elapsed_ms=(time.perf_counter() - t0) * 1000,
            )

        state = _RunState(
            config=config,
            t0=t0,
            run_id=run_id,
            result_policy=result_policy,
            progress_cb=progress_cb,
            cancel_event=cancel_event,
            prepared=None,
            prepared_panel=None,
            evidence=evidence,
            factor_snapshot={},
            err=_err,
            s=s,
            params=params,
            overrides=overrides,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop=trailing_stop,
            trailing_take_profit_activate=trailing_take_profit_activate,
            trailing_take_profit_drawdown=trailing_take_profit_drawdown,
            max_hold_days=max_hold_days,
            score_min=score_min,
            score_max=score_max,
        )
        if config.asset_type != "stock":
            return _err("分钟策略回测当前仅支持 A 股 (stock)")
        if config.exit_fill == "signal_next_minute":
            return _err("分钟策略回测暂不支持「信号触发卖出」离场口径")
        if (result := self._run_minute_load_and_replay(state)) is not None:
            return result
        if (result := self._run_minute_build_signals(state)) is not None:
            return result
        self._run_minute_market_matrix(state)
        if (result := self._run_minute_simulate(state)) is not None:
            return result
        self._run_minute_populate_stats(state)
        return self._run_minute_finalize(state)
