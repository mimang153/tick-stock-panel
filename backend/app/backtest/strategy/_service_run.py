from __future__ import annotations

import time
from dataclasses import replace
from datetime import timedelta
from typing import Any

import numpy as np
import polars as pl

from app.backtest.engine import MatcherConfig, history_load_start
from app.backtest.matrix import (
    MatrixPipelineConfig,
    MatrixStrategyPipeline,
    apply_time_masks,
    build_market_matrix_from_signals,
    slice_market_data_matrix,
    slice_signal_matrix,
)
from app.backtest.minute_trigger import (
    build_minute_entry_reference,
    unsupported_minute_exit_signals,
)
from app.config import settings
from app.strategy.engine import StrategyDataContext, StrategyEngine
from app.strategy.scoring import effective_scoring, effective_scoring_directions

from ._common import (
    StrategyBacktestResult,
    _basic_filter_for_asset,
    _panel_backend,
    _resolve_position_policy,
    _RunState,
)
from ._resolver import StrategyDependencyResolver, build_matrix_cache_profile


class StrategyBacktestRunMixin:
    """主流程前半段：解析、加载、矩阵与 panel 决策。"""

    # 由组装类/其他 mixin 提供, 拆分后在此显式声明以对 mypy 可见。
    strategy_engine: Any
    engine: Any
    run: Any
    _normalize_params: Any
    _effective_basic_filter: Any
    _effective_signals: Any
    _override_value: Any
    _normalize_pct: Any
    _normalize_score_range: Any
    _resolve_composite_feature_plan: Any
    _matrix_prepare_signature: Any
    _clamp_regime_formal_start: Any
    _matrix_date_range_mask: Any
    _date_range_mask: Any
    _build_regime_mask: Any
    _has_matrix_signal_override: Any
    _generate_composite_signal_matrix: Any
    _overlay_matrix_mask: Any

    def run_with_evidence(
        self, config, progress_cb=None, cancel_event=None
    ) -> StrategyBacktestResult:
        """正式交互回测启用检查和留档；优化器继续复用轻量 run。"""
        from app.backtest.evidence import EvidenceRecorder, save_evidence

        started = time.perf_counter()
        try:
            strategy = self.strategy_engine.get(config.strategy_id)
        except ValueError:
            return self.run(config, progress_cb, cancel_event)
        data_dir = self.engine.repo.store.data_dir
        recorder = EvidenceRecorder(data_dir, config, strategy, self.strategy_engine, cancel_event)
        # 每个正式回测 worker 拥有独立引擎；仅在本次运行内观察成交用分钟数据。
        previous = getattr(self.engine, "_evidence", None)
        self.engine._evidence = recorder
        try:
            result = self.run(config, progress_cb, cancel_event, evidence=recorder)
        finally:
            self.engine._evidence = previous
        if result.error == "cancelled":
            return result
        effective = {
            **result.config,
            "params": self._normalize_params(config.params or {}, strategy),
        }
        result.evidence = recorder.finish(result, effective)
        try:
            # 保存状态是结果的一部分；写入失败时不会宣称记录已经落盘。
            result.evidence["persistence"] = {"status": "saved"}
            save_evidence(data_dir, result.evidence)
        except (OSError, ValueError):
            result.evidence["persistence"] = {
                "status": "error",
                "error": "版本记录未能保存，请导出本次报告",
            }
        result.elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return result

    def _run_resolve_strategy_and_signals(self, state: _RunState):
        """解析策略定义、参数与信号配置；失败返回错误结果。"""
        config = state.config
        try:
            s = self.strategy_engine.get(config.strategy_id)
            StrategyEngine.validate_context(
                s,
                StrategyDataContext(
                    asset_type=config.asset_type,
                    timeframe="1m" if s.execution_backend == "minute_filter" else "1d",
                    as_of=config.end,
                ),
            )
        except ValueError as e:
            return state.err(str(e))
        params = self._normalize_params(config.params or {}, s)
        overrides = config.overrides or {}
        # 同回测 run 路径: 挖掘运行期也要按资产类型中和股票专属过滤键 (#215)
        basic_filter = _basic_filter_for_asset(
            self._effective_basic_filter(s, overrides), config.asset_type
        )
        entry_signals = self._effective_signals(overrides, "entry_signals", s.entry_signals)
        exit_signals = self._effective_signals(overrides, "exit_signals", s.exit_signals)
        # 叠加条件 (每策略 overlay 硬过滤): 只 AND 进入场掩码, 不影响已持仓卖出
        overlay_conditions = StrategyEngine._effective_overlay(overrides)
        if overlay_conditions and s.execution_backend in ("composite", "minute_filter"):
            return state.err(f"叠加条件不支持 {s.execution_backend} 策略")
        if config.exit_fill == "signal_next_minute":
            if not config.minute_fill:
                return state.err("触发后下一分钟成交需要先开启分钟成交")
            if not exit_signals:
                return state.err("当前策略没有卖出信号，无法使用触发后下一分钟成交")
            unsupported = unsupported_minute_exit_signals(exit_signals)
            if unsupported:
                return state.err(f"以下卖出信号暂不支持分钟触发回放: {', '.join(unsupported)}")
        state.s = s
        state.params = params
        state.overrides = overrides
        state.basic_filter = basic_filter
        state.entry_signals = entry_signals
        state.exit_signals = exit_signals
        state.overlay_conditions = overlay_conditions
        return None

    def _run_resolve_stops(self, state: _RunState):
        """解析止损止盈与打分范围；组合无意义时返回错误结果。"""
        s = state.s
        overrides = state.overrides
        stop_loss = self._override_value(overrides, "stop_loss", s.stop_loss)
        take_profit = self._normalize_pct(
            self._override_value(overrides, "take_profit", getattr(s, "take_profit", None)),
            0.01,
            5.0,
        )
        trailing_stop = self._normalize_pct(
            self._override_value(overrides, "trailing_stop", getattr(s, "trailing_stop", None)),
            0.005,
            0.5,
        )
        trailing_take_profit_activate = self._normalize_pct(
            self._override_value(
                overrides,
                "trailing_take_profit_activate",
                getattr(s, "trailing_take_profit_activate", None),
            ),
            0.01,
            2.0,
        )
        trailing_take_profit_drawdown = self._normalize_pct(
            self._override_value(
                overrides,
                "trailing_take_profit_drawdown",
                getattr(s, "trailing_take_profit_drawdown", None),
            ),
            0.005,
            0.5,
        )
        if (
            trailing_take_profit_activate is not None
            and trailing_take_profit_drawdown is not None
            and trailing_take_profit_drawdown > trailing_take_profit_activate
        ):
            # 不再静默钳制: 旧 min() 会把用户填的回撤改写成激活值, 配置"不生效"
            # 却无任何提示。组合无意义时显式报错, 让用户调整参数后重跑。
            return state.err(
                f"移动止盈回撤({trailing_take_profit_drawdown:.0%})不能大于"
                f"激活涨幅({trailing_take_profit_activate:.0%}), 请调整后再试"
            )
        max_hold_days = self._override_value(overrides, "max_hold_days", s.max_hold_days)
        score_min, score_max = self._normalize_score_range(
            overrides.get("score_min"),
            overrides.get("score_max"),
        )
        state.stop_loss = stop_loss
        state.take_profit = take_profit
        state.trailing_stop = trailing_stop
        state.trailing_take_profit_activate = trailing_take_profit_activate
        state.trailing_take_profit_drawdown = trailing_take_profit_drawdown
        state.max_hold_days = max_hold_days
        state.score_min = score_min
        state.score_max = score_max
        return None

    def _run_resolve_feature_plan(self, state: _RunState):
        """解析 composite / 普通策略的特征计划；失败返回错误结果。"""
        config = state.config
        s = state.s
        params = state.params
        basic_filter = state.basic_filter
        overrides = state.overrides
        try:
            if s.execution_backend == "composite":
                # composite 回测: 子策略必须全为 matrix_native(否则 fail-closed),
                # feature_plan 取所有子策略计划的并集(_merge_resolved_feature_plans)。
                feature_plan, composite_children_resolved = self._resolve_composite_feature_plan(
                    s,
                    params=params,
                    basic_filter=basic_filter,
                    overrides=overrides,
                    asset_type=config.asset_type,
                )
            else:
                composite_children_resolved = None
                feature_plan = StrategyDependencyResolver().resolve(
                    s,
                    params=params,
                    basic_filter=basic_filter,
                    entry_signals=state.entry_signals,
                    exit_signals=state.exit_signals,
                    overrides=overrides,
                    minute_fill=config.minute_fill,
                    asset_type=config.asset_type,
                )
        except ValueError as e:
            return state.err(str(e))
        state.feature_plan = feature_plan
        state.composite_children_resolved = composite_children_resolved
        return None

    def _run_prepare_window(self, state: _RunState) -> None:
        """初始化计时/缓存状态并推导加载与模拟窗口。"""
        config = state.config
        feature_plan = state.feature_plan
        timing_ms: dict[str, float] = {}
        state.timing_ms = timing_ms
        state.matrix_data_cache_hit = False
        state.matrix_data_cache_status = "none"
        state.matrix_data_cache_timing_ms = {}
        shared = state.prepared or state.prepared_panel
        load_start = (
            shared.load_start
            if shared is not None
            else history_load_start(
                self.engine,
                config.start,
                config.asset_type,
                feature_plan.warmup_bars,
            )
        )
        # 全量模式: entries 只在正式区间触发, exits 需要 end 之后的尾部数据继续执行策略卖点。
        # 若策略有 max_hold_days, 用它决定尾部窗口；否则 holding_days 只作为兜底观察上限。
        full_horizon_days = int(state.max_hold_days or config.holding_days or 5)
        full_horizon_days = max(full_horizon_days, 1)
        load_end = config.end
        if config.mode == "full":
            fwd_buffer = full_horizon_days + 5  # 多取几天, 容错停牌缺口/open_t+1
            load_end = config.end + timedelta(
                days=fwd_buffer * 2
            )  # 日历日放宽, 确保覆盖 N 个交易日
        sim_end = load_end if config.mode == "full" else config.end
        state.load_start = load_start
        state.load_end = load_end
        state.sim_end = sim_end
        state.full_horizon_days = full_horizon_days
        state.panel = None
        state.formal_range = None
        state.market_data = None

    def _run_load_matrix_prepared(self, state: _RunState):
        """复用已准备好的共享矩阵；不适用或配置不匹配时返回错误结果。"""
        if state.prepared is None:
            return None
        config = state.config
        s = state.s
        prepared = state.prepared
        if s.execution_backend != "matrix_native":
            return state.err("共享基础矩阵只能用于 matrix_native 策略")
        if prepared.signature != self._matrix_prepare_signature(config):
            return state.err("共享基础矩阵与当前回测配置不匹配")
        state.load_start = prepared.load_start
        state.load_end = prepared.load_end
        state.sim_end = prepared.sim_end
        state.feature_width = prepared.feature_width
        state.timing_ms["load_panel"] = 0.0
        state.timing_ms["market_data_matrix_build"] = 0.0
        state.matrix_data_cache_status = prepared.market_data.cache_status
        state.matrix_data_cache_hit = state.matrix_data_cache_status in {"exact", "covering"}
        state.matrix_data_cache_timing_ms = prepared.market_data.cache_timing_ms
        # 环境过滤下正式起点=矩阵首日时顺延 (首日让渡为预热)
        if config.regime_filter:
            state.config = self._clamp_regime_formal_start(
                config, prepared.market_data.timestamp_labels
            )
        return None

    def _run_load_matrix_fresh(self, state: _RunState):
        """为 matrix_native/composite 策略加载基础行情矩阵；失败返回错误结果。"""
        if state.prepared is not None or state.s.execution_backend not in (
            "matrix_native",
            "composite",
        ):
            return None
        config = state.config
        s = state.s
        overrides = state.overrides
        feature_plan = state.feature_plan
        t_load = time.perf_counter()
        max_hold_for_profile = self._override_value(
            overrides,
            "max_hold_days",
            s.max_hold_days,
        )
        profile_forward = max(int(max_hold_for_profile or config.holding_days or 5), 1)
        cache_profile = build_matrix_cache_profile(
            self.strategy_engine,
            config.asset_type,
            requested_plan=feature_plan,
            requested_forward_bars=profile_forward,
            max_disk_bytes=settings.backtest_matrix_cache_max_mb * 1024 * 1024,
        )
        coverage_start = history_load_start(
            self.engine, config.start, config.asset_type, cache_profile.warmup_bars
        )
        coverage_end = config.end
        if config.mode == "full":
            coverage_end = config.end + timedelta(days=(cache_profile.forward_bars + 5) * 2)
        try:
            market_data = self.engine.load_market_data_matrix_for_backtest(
                config.symbols,
                state.load_start,
                state.load_end,
                feature_plan,
                asset_type=config.asset_type,
                cache_profile=cache_profile,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
            )
        except (ValueError, OSError) as e:
            return state.err(f"回测矩阵准备失败: {e}")
        direct_load_ms = round((time.perf_counter() - t_load) * 1000, 1)
        state.timing_ms["load_panel"] = direct_load_ms
        state.timing_ms["market_data_matrix_build"] = 0.0
        state.timing_ms["market_data_direct_load"] = direct_load_ms
        state.matrix_data_cache_status = market_data.cache_status
        state.matrix_data_cache_hit = state.matrix_data_cache_status in {"exact", "covering"}
        state.matrix_data_cache_timing_ms = market_data.cache_timing_ms
        # 环境过滤下正式起点=矩阵首日时顺延 (首日让渡为预热)
        if config.regime_filter:
            config = self._clamp_regime_formal_start(config, market_data.timestamp_labels)
            state.config = config
        formal_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            config.start,
            config.end,
        )
        if not formal_time_mask.any():
            return state.err("正式回测区间内无数据")
        state.market_data = market_data
        state.feature_width = len(feature_plan.matrix_columns)
        return None

    def _run_load_panel(self, state: _RunState):
        """为面板路径加载/复用特征面板；失败返回错误结果。"""
        if state.prepared is not None or state.s.execution_backend in (
            "matrix_native",
            "composite",
        ):
            return None
        config = state.config
        s = state.s
        feature_plan = state.feature_plan
        panel = None
        if state.prepared_panel is not None:
            # P2a 共享面板: 优化器准备的超集面板直接复用, 跳过载盘/指标/信号注入。
            if not _panel_backend(s.execution_backend):
                return state.err("共享特征面板只能用于面板路径策略")
            if state.prepared_panel.signature != self._matrix_prepare_signature(config):
                return state.err("共享特征面板与当前回测配置不匹配")
            required_columns = (
                set(feature_plan.base_columns)
                | set(feature_plan.instrument_columns)
                | set(feature_plan.signal_columns)
                | set(feature_plan.indicator_columns)
                | set(feature_plan.fundamental_columns)
            )
            missing_columns = required_columns - state.prepared_panel.columns
            if missing_columns:
                return state.err(f"共享特征面板缺少本参数组所需列: {sorted(missing_columns)}")
            panel = state.prepared_panel.panel
            state.load_start = state.prepared_panel.load_start
            state.load_end = state.prepared_panel.load_end
            state.sim_end = state.prepared_panel.sim_end
            state.timing_ms["load_panel"] = 0.0
        else:
            t_load = time.perf_counter()
            try:
                panel = self.engine.load_panel_for_backtest(
                    config.symbols,
                    state.load_start,
                    state.load_end,
                    feature_plan,
                    asset_type=config.asset_type,
                    **({"evidence": state.evidence} if state.evidence is not None else {}),
                )
            except (ValueError, pl.exceptions.PolarsError) as e:
                return state.err(f"回测特征准备失败: {e}")
            state.timing_ms["load_panel"] = round((time.perf_counter() - t_load) * 1000, 1)
        if panel.is_empty():
            return state.err("无数据，请检查日期范围或先运行盘后管道")
        # 环境过滤下正式起点=面板首日时顺延 (首日让渡为预热)
        if config.regime_filter:
            date_labels = tuple(
                str(value)[:10] for value in panel.get_column("date").unique().sort().to_list()
            )
            config = self._clamp_regime_formal_start(config, date_labels)
            state.config = config
        formal_range = self._date_range_mask(panel, config.start, config.end)
        if not formal_range.any():
            return state.err("正式回测区间内无数据")
        state.panel = panel
        state.formal_range = formal_range
        state.feature_width = int(panel.width)
        return None

    def _run_build_matcher(self, state: _RunState) -> None:
        """按当前 config 与解析后的止损参数构造撮合配置。"""
        config = state.config
        position_policy_compute, position_policy_params = _resolve_position_policy(config)
        state.matcher_config = MatcherConfig(
            matching=config.matching,
            entry_fill=config.entry_fill,
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
            minute_fill=config.minute_fill,
            asset_type=config.asset_type,
        )

    def _run_composite_time_masks(self, state: _RunState):
        """composite 分支: 构造时间掩码并推导模拟下标边界。"""
        if state.s.execution_backend != "composite":
            return None
        config = state.config
        market_data = state.market_data
        if state.composite_children_resolved is None:
            return state.err("叠加策略子策略解析失败")
        if market_data is None:
            return state.err("矩阵回测缺少基础行情矩阵")
        entry_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            config.start,
            config.end,
        )
        # 市场环境过滤(强制 T-1): 只叠加 entry, 不影响 exit
        try:
            _rm = self._build_regime_mask(
                market_data.timestamp_labels,
                config.regime_filter,
                getattr(getattr(self.engine.repo, "store", None), "data_dir", None),
                required_start=config.start,
                required_end=config.end,
                evidence=state.evidence,
            )
        except ValueError as e:
            return state.err(str(e))
        if _rm is not None:
            entry_time_mask = entry_time_mask & _rm
        exit_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            config.start,
            state.load_end if config.mode == "full" else config.end,
        )
        sim_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            config.start,
            state.sim_end,
        )
        time_ids = np.flatnonzero(sim_time_mask)
        if time_ids.size == 0:
            return state.err("正式回测区间内无数据")
        start_id = int(time_ids[0])
        stop_id = int(time_ids[-1]) + 1
        state.entry_time_mask = entry_time_mask
        state.exit_time_mask = exit_time_mask
        state.start_id = start_id
        state.stop_id = stop_id
        state.panel_rows = int(np.isfinite(market_data.close[start_id:stop_id]).sum())
        state.panel_columns = len(state.feature_plan.matrix_columns)
        state.reference_price = (
            build_minute_entry_reference(market_data.close)[start_id:stop_id]
            if state.matcher_config.minute_fill
            else None
        )
        return None

    def _run_composite_market_matrix(self, state: _RunState):
        """composite 分支: 生成合并信号并组装市场矩阵。"""
        if state.s.execution_backend != "composite":
            return None
        params = state.params
        market_data = state.market_data
        start_id = state.start_id
        stop_id = state.stop_id
        merge_mode = str(params.get("merge_mode") or "union")
        min_confirm = int(params.get("min_confirm") or 0)
        # max_hold 用于退出投影窗口封顶; 无值时给一个足够大的兜底(仅靠信号退出)。
        composite_max_hold = max(int(state.max_hold_days or 0), 1) if state.max_hold_days else 250
        try:
            signal_matrix = self._generate_composite_signal_matrix(
                state.composite_children_resolved,
                market_data,
                merge_mode,
                min_confirm,
                composite_max_hold,
                state.timing_ms,
            )
        except ValueError as e:
            return state.err(str(e))
        if state.evidence is not None:
            state.evidence.check_matrix(
                market_data,
                signal_matrix,
                lambda data: self._generate_composite_signal_matrix(
                    state.composite_children_resolved,
                    data,
                    merge_mode,
                    min_confirm,
                    composite_max_hold,
                    {},
                ),
            )
        sim_market_data = slice_market_data_matrix(market_data, start_id, stop_id)
        sim_signal_matrix = slice_signal_matrix(signal_matrix, start_id, stop_id)
        sim_signal_matrix = apply_time_masks(
            sim_signal_matrix,
            state.entry_time_mask[start_id:stop_id],
            state.exit_time_mask[start_id:stop_id],
        )
        state.timing_ms["signals_score"] = round((time.perf_counter() - state.t_signal) * 1000, 1)
        if not sim_signal_matrix.entry.any():
            return state.err("在指定区间内未产生买入信号")
        raw_candidates = int(sim_signal_matrix.entry.sum())
        state.raw_candidates = raw_candidates
        state.selection_stats = {
            "strategy_matches": raw_candidates,
            "entry_candidates": raw_candidates,
            "entry_trigger_filtered": 0,
            "entry_trigger_enabled": False,
        }
        t_matrix = time.perf_counter()
        state.market_matrix = build_market_matrix_from_signals(
            sim_market_data,
            sim_signal_matrix,
            entry_delay_bars=1 if state.matcher_config.entry_fill == "open_t+1" else 0,
            exit_delay_bars=1 if state.matcher_config.exit_fill == "open_t+1" else 0,
            reference_price=state.reference_price,
            minute_exit_trigger=state.matcher_config.exit_fill == "signal_next_minute",
        )
        state.timing_ms["matrix_build"] = round((time.perf_counter() - t_matrix) * 1000, 1)
        return None

    def _run_matrix_time_masks(self, state: _RunState):
        """matrix_native 分支: 构造时间掩码并推导模拟下标边界。"""
        if state.s.execution_backend != "matrix_native":
            return None
        config = state.config
        s = state.s
        overrides = state.overrides
        feature_plan = state.feature_plan
        prepared = state.prepared
        if s.matrix_strategy is None:
            return state.err("矩阵策略未注册")
        if self._has_matrix_signal_override(s, overrides):
            return state.err("matrix_native 策略的进出场信号由策略协议生成，不支持列信号覆盖")
        market_data = state.market_data
        if prepared is not None:
            market_data = prepared.market_data
            entry_time_mask = prepared.entry_time_mask
            exit_time_mask = prepared.exit_time_mask
            start_id = prepared.start_id
            stop_id = prepared.stop_id
            reference_price = prepared.reference_price
            panel_rows = int(np.isfinite(market_data.close[start_id:stop_id]).sum())
            panel_columns = len(feature_plan.matrix_columns)
        else:
            if market_data is None:
                return state.err("矩阵回测缺少基础行情矩阵")
            entry_time_mask = self._matrix_date_range_mask(
                market_data.timestamp_labels,
                config.start,
                config.end,
            )
            try:
                _rm = self._build_regime_mask(
                    market_data.timestamp_labels,
                    config.regime_filter,
                    getattr(getattr(self.engine.repo, "store", None), "data_dir", None),
                    required_start=config.start,
                    required_end=config.end,
                    evidence=state.evidence,
                )
            except ValueError as e:
                return state.err(str(e))
            if _rm is not None:
                entry_time_mask = entry_time_mask & _rm
            exit_time_mask = self._matrix_date_range_mask(
                market_data.timestamp_labels,
                config.start,
                state.load_end if config.mode == "full" else config.end,
            )
            sim_time_mask = self._matrix_date_range_mask(
                market_data.timestamp_labels,
                config.start,
                state.sim_end,
            )
            time_ids = np.flatnonzero(sim_time_mask)
            if time_ids.size == 0:
                return state.err("正式回测区间内无数据")
            start_id = int(time_ids[0])
            stop_id = int(time_ids[-1]) + 1
            panel_rows = int(np.isfinite(market_data.close[start_id:stop_id]).sum())
            panel_columns = len(feature_plan.matrix_columns)
            reference_price = (
                build_minute_entry_reference(market_data.close)[start_id:stop_id]
                if state.matcher_config.minute_fill
                else None
            )
        state.market_data = market_data
        state.entry_time_mask = entry_time_mask
        state.exit_time_mask = exit_time_mask
        state.start_id = start_id
        state.stop_id = stop_id
        state.reference_price = reference_price
        state.panel_rows = panel_rows
        state.panel_columns = panel_columns
        return None

    def _run_matrix_compute_signals(self, state: _RunState):
        """matrix_native 分支: 运行策略协议生成信号矩阵。"""
        if state.s.execution_backend != "matrix_native":
            return None
        s = state.s
        params = state.params
        overrides = state.overrides
        scoring = effective_scoring(s.meta.get("scoring"), overrides)
        try:
            pipeline_config = MatrixPipelineConfig(
                basic_filter=state.basic_filter,
                scoring=scoring,
                scoring_directions=effective_scoring_directions(overrides),
                order_by=s.meta.get("order_by"),
                descending=bool(s.meta.get("descending", True)),
                protect_strategy_cache=state.prepared is not None,
            )
            if state.prepared is None:
                signal_matrix = MatrixStrategyPipeline().run(
                    s.matrix_strategy,
                    state.market_data,
                    params,
                    pipeline_config,
                    state.timing_ms,
                )
            else:
                with state.prepared.compute_cache.activate(state.market_data):
                    signal_matrix = MatrixStrategyPipeline().run(
                        s.matrix_strategy,
                        state.market_data,
                        params,
                        pipeline_config,
                        state.timing_ms,
                    )
        except (TypeError, ValueError) as e:
            return state.err(f"矩阵策略信号计算失败: {e}")
        if state.evidence is not None:
            state.evidence.check_matrix(
                state.market_data,
                signal_matrix,
                lambda data: MatrixStrategyPipeline().run(
                    s.matrix_strategy,
                    data,
                    params,
                    pipeline_config,
                ),
            )
        state.signal_matrix = signal_matrix
        return None

    def _run_matrix_apply_masks(self, state: _RunState):
        """matrix_native 分支: 应用时间/叠加掩码并组装市场矩阵。"""
        if state.s.execution_backend != "matrix_native":
            return None
        market_data = state.market_data
        signal_matrix = state.signal_matrix
        start_id = state.start_id
        stop_id = state.stop_id
        sim_market_data = slice_market_data_matrix(market_data, start_id, stop_id)
        sim_signal_matrix = slice_signal_matrix(signal_matrix, start_id, stop_id)
        sim_signal_matrix = apply_time_masks(
            sim_signal_matrix,
            state.entry_time_mask[start_id:stop_id],
            state.exit_time_mask[start_id:stop_id],
        )
        # 叠加条件: 在矩阵 fields (依赖已并入 feature_plan) 上按 (时间, 标的)
        # 评估, AND 进 entry; exit 不动 — 已持仓卖出不受影响。
        if state.overlay_conditions:
            overlay_mask, overlay_err = self._overlay_matrix_mask(
                market_data, state.overlay_conditions
            )
            if overlay_err is not None:
                return state.err(overlay_err)
            # np.where 产物可写, 须恢复只读不变量 (SignalMatrix 校验会拒)。
            # 该路径此前被"扩展字段不可用"挡在更早阶段, 普通列叠加同样会触发。
            overlay_entry = np.where(
                overlay_mask[start_id:stop_id],
                sim_signal_matrix.entry,
                0,
            ).astype(sim_signal_matrix.entry.dtype)
            overlay_entry.flags.writeable = False
            sim_signal_matrix = replace(
                sim_signal_matrix,
                entry=overlay_entry,
            )
        state.timing_ms["signals_score"] = round((time.perf_counter() - state.t_signal) * 1000, 1)
        if not sim_signal_matrix.entry.any():
            return state.err("在指定区间内未产生买入信号")
        raw_candidates = int(sim_signal_matrix.entry.sum())
        state.raw_candidates = raw_candidates
        state.selection_stats = {
            "strategy_matches": raw_candidates,
            "entry_candidates": raw_candidates,
            "entry_trigger_filtered": 0,
            "entry_trigger_enabled": False,
        }
        t_matrix = time.perf_counter()
        state.market_matrix = build_market_matrix_from_signals(
            sim_market_data,
            sim_signal_matrix,
            entry_delay_bars=1 if state.matcher_config.entry_fill == "open_t+1" else 0,
            exit_delay_bars=1 if state.matcher_config.exit_fill == "open_t+1" else 0,
            reference_price=state.reference_price,
            minute_exit_trigger=state.matcher_config.exit_fill == "signal_next_minute",
        )
        state.timing_ms["matrix_build"] = round((time.perf_counter() - t_matrix) * 1000, 1)
        return None
