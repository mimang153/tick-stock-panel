from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any, cast

import numpy as np
import polars as pl

from app.backtest.engine import history_load_start
from app.backtest.matrix import (
    MarketDataMatrix,
    MatrixComputeCache,
    MatrixPipelineConfig,
    MatrixStrategyPipeline,
    PanelMatrixEncoding,
    encode_panel_matrix,
    slice_market_data_matrix,
)
from app.backtest.minute_trigger import build_minute_entry_reference
from app.config import settings
from app.strategy.engine import StrategyDataContext, StrategyDef, StrategyEngine
from app.strategy.scoring import effective_scoring, effective_scoring_directions

from ._common import (
    PreparedMatrixBacktest,
    PreparedPanelBacktest,
    ResolvedFeaturePlan,
    StrategyBacktestConfig,
    _basic_filter_for_asset,
    _merge_resolved_feature_plans,
    _panel_backend,
    logger,
)
from ._resolver import StrategyDependencyResolver, build_matrix_cache_profile
from ._service_support import StrategyBacktestSupportMixin


class StrategyBacktestPrepareMixin:
    """矩阵/panel 优化准备阶段方法。"""

    # 由组装类/其他 mixin 提供, 拆分后在此显式声明以对 mypy 可见。
    strategy_engine: Any
    engine: Any
    _effective_basic_filter: Any
    _effective_signals: Any
    _normalize_params: Any
    _clamp_regime_formal_start: Any
    _matrix_date_range_mask: Any
    _build_regime_mask: Any
    _override_value: Any

    @staticmethod
    def _matrix_prepare_signature(config: StrategyBacktestConfig) -> tuple:
        return (
            config.strategy_id,
            None if config.symbols is None else tuple(config.symbols),
            config.start,
            config.end,
            config.mode,
            config.asset_type,
            config.holding_days,
            config.minute_fill,
            json.dumps(config.overrides or {}, sort_keys=True, ensure_ascii=False, default=str),
            json.dumps(config.regime_filter or {}, sort_keys=True, ensure_ascii=False, default=str),
        )

    def _resolve_composite_feature_plan(
        self,
        strategy: StrategyDef,
        *,
        params: dict,
        basic_filter: dict,
        overrides: dict,
        asset_type: str = "stock",
    ) -> tuple[ResolvedFeaturePlan, list[tuple[StrategyDef, dict, MatrixPipelineConfig]]]:
        """解析 composite 回测的特征计划: 所有子策略 feature_plan 的并集。

        返回 (合并 feature_plan, [(子策略定义, 子params, 子pipeline_config_dict), ...])。
        子策略必须全为 matrix_native, 否则 fail-closed(首版硬约束)。
        """
        from app.strategy import composite as composite_mod
        from app.strategy.engine import _parse_composite_children

        assert strategy.composite is not None
        # 权重: override.children 优先, 否则 META 声明。
        override_children = overrides.get("children")
        if isinstance(override_children, list) and override_children:
            spec = _parse_composite_children(override_children)
            children = spec.children
        else:
            children = strategy.composite.children

        resolver = StrategyDependencyResolver()
        plans: list[ResolvedFeaturePlan] = []
        resolved_children: list[tuple[StrategyDef, dict, MatrixPipelineConfig]] = []
        for child in children:
            child_def = self.strategy_engine.get(child.strategy_id)
            if child_def.execution_backend != "matrix_native":
                raise ValueError(
                    f"叠加回测暂仅支持矩阵子策略; {child.strategy_id!r} "
                    f"是 {child_def.execution_backend}"
                )
            if child_def.matrix_strategy is None:
                raise ValueError(f"子策略 {child.strategy_id!r} 未注册矩阵策略")
            # 加载子策略的用户 override(参数/评分等), 保证回测与单独跑子策略同口径。
            child_override: dict = {}
            loader = getattr(self.strategy_engine, "_override_loader", None)
            if loader is not None:
                try:
                    loaded = loader(child.strategy_id)
                    if isinstance(loaded, dict):
                        child_override = dict(loaded)
                except Exception:  # noqa: BLE001
                    pass
            child_params = self.strategy_engine.resolve_params(child_def, overrides=child_override)
            child_plan = resolver.resolve(
                child_def,
                params=child_params,
                basic_filter=basic_filter,  # 统一 basic_filter(计划 §3.3)
                entry_signals=[],
                exit_signals=[],
                overrides=child_override,
                asset_type=asset_type,
            )
            plans.append(child_plan)
            # pipeline 用 composite 统一的 basic_filter; scoring 用子策略自己的
            # (默认 + 用户 override), 因为子策略内部排序影响合并器的排名融合。
            child_scoring = effective_scoring(child_def.meta.get("scoring"), child_override)
            child_pipeline_cfg = MatrixPipelineConfig(
                basic_filter=basic_filter,
                scoring=child_scoring,
                scoring_directions=effective_scoring_directions(child_override),
                order_by=child_def.meta.get("order_by"),
                descending=bool(child_def.meta.get("descending", True)),
                protect_strategy_cache=False,
            )
            resolved_children.append((child_def, child_params, child_pipeline_cfg))

        merged_plan = _merge_resolved_feature_plans(plans)
        # composite 模块用于合并时读取权重列表(顺序对齐 resolved_children)。
        self._composite_children_weights = [(c.strategy_id, c.weight) for c in children]
        _ = composite_mod  # 确保模块可导入(回测时由调用方使用)
        return merged_plan, resolved_children

    def _generate_composite_signal_matrix(
        self,
        resolved_children: list[tuple[StrategyDef, dict, MatrixPipelineConfig]],
        market_data: MarketDataMatrix,
        merge_mode: str,
        min_confirm: int,
        max_hold: int,
        timing_ms: dict[str, float],
    ):
        """逐子策略计算 SignalMatrix, 再合并为单个 SignalMatrix(回测合并)。

        合并语义见 app.strategy.composite.merge_signal_matrices:
        - entry: union/intersect
        - exit: 来源投影(每个子的 exit 仅在自己持仓窗口生效, 不串平)
        - score: 标准化排名加权
        """
        from app.strategy import composite as composite_mod

        t_signals = time.perf_counter()
        sigs = []
        for child_def, child_params, child_pipeline_cfg in resolved_children:
            try:
                child_sig = MatrixStrategyPipeline().run(
                    cast(Any, child_def.matrix_strategy),
                    market_data,
                    child_params,
                    child_pipeline_cfg,
                )
            except (TypeError, ValueError) as e:
                raise ValueError(f"子策略 {child_def.meta.get('id')} 信号计算失败: {e}") from e
            sigs.append(child_sig)
        timing_ms["strategy_signals"] = round((time.perf_counter() - t_signals) * 1000, 1)

        children_weights = getattr(self, "_composite_children_weights", None) or [
            (cd.meta.get("id", ""), 1.0) for cd, _, _ in resolved_children
        ]
        return composite_mod.merge_signal_matrices(
            market_data.shape,
            sigs,
            children_weights,
            merge_mode,
            min_confirm,
            max_hold,
        )

    def _prepare_validation(
        self,
        configs: list[StrategyBacktestConfig],
        *,
        panel_path: bool,
        error_msg: str,
    ) -> tuple:
        """校验优化器试算组同构并执行策略上下文校验, 返回 (签名, 首个 config, 策略)。"""
        if not configs:
            raise ValueError("optimizer preparation requires at least one backtest config")
        signature = self._matrix_prepare_signature(configs[0])
        if any(self._matrix_prepare_signature(config) != signature for config in configs[1:]):
            raise ValueError("optimizer trials must share strategy, universe, range and overrides")
        first = configs[0]
        strategy = self.strategy_engine.get(first.strategy_id)
        backend_ok = (
            _panel_backend(strategy.execution_backend)
            if panel_path
            else strategy.execution_backend == "matrix_native"
        )
        if not backend_ok:
            raise ValueError(error_msg)
        StrategyEngine.validate_context(
            strategy,
            StrategyDataContext(
                asset_type=first.asset_type,
                timeframe="1d",
                as_of=first.end,
            ),
        )
        return signature, first, strategy

    def _prepare_feature_plan(
        self,
        first: StrategyBacktestConfig,
        strategy: StrategyDef,
        configs: list[StrategyBacktestConfig],
    ) -> tuple[dict, dict, list[str], list[str], ResolvedFeaturePlan]:
        """逐组解析依赖计划取并集, 返回 (overrides, basic_filter, 入场, 出场, 合并计划)。"""
        overrides = first.overrides or {}
        # 运行期过滤用的也是同一份 basic_filter: 在入口处按资产类型中和,
        # 否则 boards/price_min 会在掩码阶段静默清零 ETF 候选 (#215)
        basic_filter = _basic_filter_for_asset(
            self._effective_basic_filter(strategy, overrides), first.asset_type
        )
        entry_signals = self._effective_signals(overrides, "entry_signals", strategy.entry_signals)
        exit_signals = self._effective_signals(overrides, "exit_signals", strategy.exit_signals)
        resolver = StrategyDependencyResolver()
        plans: list[ResolvedFeaturePlan] = []
        for config in configs:
            params = self._normalize_params(config.params or {}, strategy)
            plans.append(
                resolver.resolve(
                    strategy,
                    params=params,
                    basic_filter=basic_filter,
                    entry_signals=entry_signals,
                    exit_signals=exit_signals,
                    overrides=overrides,
                    minute_fill=config.minute_fill,
                    asset_type=config.asset_type,
                )
            )
        feature_plan = _merge_resolved_feature_plans(plans)
        return overrides, basic_filter, entry_signals, exit_signals, feature_plan

    def _prepare_matrix_window(
        self,
        first: StrategyBacktestConfig,
        feature_plan: ResolvedFeaturePlan,
        full_horizon_days: int,
    ) -> tuple:
        """推导缓存档与加载/覆盖/模拟窗口。"""
        cache_profile = build_matrix_cache_profile(
            self.strategy_engine,
            first.asset_type,
            requested_plan=feature_plan,
            requested_forward_bars=full_horizon_days,
            max_disk_bytes=settings.backtest_matrix_cache_max_mb * 1024 * 1024,
        )
        load_start = history_load_start(
            self.engine, first.start, first.asset_type, feature_plan.warmup_bars
        )
        coverage_start = history_load_start(
            self.engine, first.start, first.asset_type, cache_profile.warmup_bars
        )
        load_end = first.end
        coverage_end = first.end
        if first.mode == "full":
            load_end = first.end + timedelta(days=(full_horizon_days + 5) * 2)
            coverage_end = first.end + timedelta(days=(cache_profile.forward_bars + 5) * 2)
        sim_end = load_end if first.mode == "full" else first.end
        return cache_profile, load_start, coverage_start, load_end, coverage_end, sim_end

    def _load_matrix_for_prepare(
        self,
        first: StrategyBacktestConfig,
        feature_plan: ResolvedFeaturePlan,
        cache_profile,
        load_start: date,
        load_end: date,
        coverage_start: date,
        coverage_end: date,
        market_data_override: MarketDataMatrix | None,
    ) -> tuple[MarketDataMatrix, float]:
        """加载基础行情矩阵, 或从共享 WF 矩阵切只读时间视图。"""
        if market_data_override is None:
            started = time.perf_counter()
            market_data = self.engine.load_market_data_matrix_for_backtest(
                first.symbols,
                load_start,
                load_end,
                feature_plan,
                asset_type=first.asset_type,
                cache_profile=cache_profile,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
            )
            direct_load_ms = round((time.perf_counter() - started) * 1000, 1)
        else:
            labels = market_data_override.timestamp_labels
            visible_ids = np.flatnonzero(
                np.fromiter(
                    (str(load_start) <= label[:10] <= str(load_end) for label in labels),
                    dtype=bool,
                    count=len(labels),
                )
            )
            if visible_ids.size == 0:
                raise ValueError("shared WF matrix does not cover the fold window")
            market_data = slice_market_data_matrix(
                market_data_override,
                int(visible_ids[0]),
                int(visible_ids[-1]) + 1,
            )
            direct_load_ms = 0.0
        return market_data, direct_load_ms

    def _prepare_matrix_masks(
        self,
        first: StrategyBacktestConfig,
        market_data: MarketDataMatrix,
        load_end: date,
        sim_end: date,
        feature_plan: ResolvedFeaturePlan,
    ) -> tuple:
        """构造入场/出场/模拟时间掩码与下标边界。"""
        # 环境过滤下正式起点=矩阵首日时顺延 (首日让渡为预热), 见 _clamp_regime_formal_start
        first = self._clamp_regime_formal_start(first, market_data.timestamp_labels)
        formal_range = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            first.start,
            first.end,
        )
        if not formal_range.any():
            raise ValueError("正式回测区间内无数据")
        feature_width = len(feature_plan.matrix_columns)
        entry_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            first.start,
            first.end,
        )
        # 市场环境过滤(优化器共享, 用首个 config 的 regime_filter)
        _rm = self._build_regime_mask(
            market_data.timestamp_labels,
            first.regime_filter,
            getattr(getattr(self.engine.repo, "store", None), "data_dir", None),
            required_start=first.start,
            required_end=first.end,
        )
        if _rm is not None:
            entry_time_mask = entry_time_mask & _rm
        exit_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            first.start,
            load_end if first.mode == "full" else first.end,
        )
        sim_time_mask = self._matrix_date_range_mask(
            market_data.timestamp_labels,
            first.start,
            sim_end,
        )
        time_ids = np.flatnonzero(sim_time_mask)
        if time_ids.size == 0:
            raise ValueError("正式回测区间内无数据")
        start_id = int(time_ids[0])
        stop_id = int(time_ids[-1]) + 1
        reference_price = (
            build_minute_entry_reference(market_data.close)[start_id:stop_id]
            if first.minute_fill
            else None
        )
        return (
            first,
            feature_width,
            entry_time_mask,
            exit_time_mask,
            start_id,
            stop_id,
            reference_price,
        )

    def prepare_matrix_optimization(
        self,
        configs: list[StrategyBacktestConfig],
        *,
        matrix_cache_max_bytes: int = 512 * 1024 * 1024,
        market_data_override: MarketDataMatrix | None = None,
    ) -> PreparedMatrixBacktest:
        """Load and encode one immutable base matrix for all matrix-native trials.

        ``market_data_override`` is an optional shared WF matrix.  Fold-local
        prepared objects take a read-only time view of it, so the base mmap and
        its arrays are not copied while each fold still receives a bounded
        history window for strict out-of-sample evaluation.
        """
        signature, first, strategy = self._prepare_validation(
            configs,
            panel_path=False,
            error_msg="shared MarketDataMatrix preparation requires matrix_native strategy",
        )
        overrides, _basic_filter, _entry_signals, _exit_signals, feature_plan = (
            self._prepare_feature_plan(first, strategy, configs)
        )
        max_hold_days = self._override_value(overrides, "max_hold_days", strategy.max_hold_days)
        full_horizon_days = max(int(max_hold_days or first.holding_days or 5), 1)
        (
            cache_profile,
            load_start,
            coverage_start,
            load_end,
            coverage_end,
            sim_end,
        ) = self._prepare_matrix_window(first, feature_plan, full_horizon_days)

        timing_ms: dict[str, float] = {}
        prepare_started = time.perf_counter()
        market_data, direct_load_ms = self._load_matrix_for_prepare(
            first,
            feature_plan,
            cache_profile,
            load_start,
            load_end,
            coverage_start,
            coverage_end,
            market_data_override,
        )
        timing_ms["load_panel"] = direct_load_ms
        timing_ms["market_data_matrix_build"] = 0.0
        timing_ms["market_data_direct_load"] = direct_load_ms
        (
            first,
            feature_width,
            entry_time_mask,
            exit_time_mask,
            start_id,
            stop_id,
            reference_price,
        ) = self._prepare_matrix_masks(first, market_data, load_end, sim_end, feature_plan)
        timing_ms["total"] = round((time.perf_counter() - prepare_started) * 1000, 1)
        compute_cache = MatrixComputeCache(max_bytes=matrix_cache_max_bytes)
        return PreparedMatrixBacktest(
            signature=signature,
            market_data=market_data,
            feature_width=feature_width,
            load_start=load_start,
            load_end=load_end,
            sim_end=sim_end,
            entry_time_mask=entry_time_mask,
            exit_time_mask=exit_time_mask,
            start_id=start_id,
            stop_id=stop_id,
            reference_price=reference_price,
            prepare_timing_ms=timing_ms,
            compute_cache=compute_cache,
        )

    @staticmethod
    def _prepare_panel_encoding(
        panel: pl.DataFrame,
        feature_plan: ResolvedFeaturePlan,
        first: StrategyBacktestConfig,
        entry_signals: list[str],
        exit_signals: list[str],
        sim_end: date,
    ) -> PanelMatrixEncoding | None:
        """构建 sim 半边参数无关编码, 失败返回 None (回退逐组建矩阵)。"""
        sim_encoding = None
        try:
            sim_range = StrategyBacktestSupportMixin._date_range_mask(panel, first.start, sim_end)
            sim_columns = [
                column for column in feature_plan.matrix_columns if column in panel.columns
            ]
            sim_panel = panel.filter(sim_range).select(sorted(sim_columns))
            sim_encoding = encode_panel_matrix(
                sim_panel,
                entry_signal_ids=list(entry_signals),
                exit_signal_ids=list(exit_signals),
            )
        except Exception as e:  # noqa: BLE001 — 编码是纯优化, 失败回退整建
            logger.warning("共享面板矩阵编码失败, 试算回退逐组建矩阵: %s", e)
        return sim_encoding

    def prepare_panel_optimization(
        self,
        configs: list[StrategyBacktestConfig],
    ) -> PreparedPanelBacktest:
        """polars_expr 参数网格的共享特征面板 (P2a): 逐组解析依赖计划取并集,
        一次加载全组复用 — 消灭每组重复的 载盘/指标计算/信号注入。

        与 prepare_matrix_optimization 同一套签名约束 (params 不入签名 — 参数
        驱动的列差异由并集计划覆盖); load 窗口推导与 run() 完全同口径。
        """
        signature, first, strategy = self._prepare_validation(
            configs,
            panel_path=True,
            error_msg="shared feature panel preparation requires a panel-path strategy",
        )
        overrides, _basic_filter, entry_signals, exit_signals, feature_plan = (
            self._prepare_feature_plan(first, strategy, configs)
        )
        max_hold_days = self._override_value(overrides, "max_hold_days", strategy.max_hold_days)
        full_horizon_days = max(int(max_hold_days or first.holding_days or 5), 1)
        load_start = history_load_start(
            self.engine, first.start, first.asset_type, feature_plan.warmup_bars
        )
        load_end = first.end
        if first.mode == "full":
            load_end = first.end + timedelta(days=(full_horizon_days + 5) * 2)
        sim_end = load_end if first.mode == "full" else first.end

        panel = self.engine.load_panel_for_backtest(
            first.symbols,
            load_start,
            load_end,
            feature_plan,
            asset_type=first.asset_type,
        )
        if panel.is_empty():
            raise ValueError("共享特征面板无数据，请检查日期范围或先运行盘后管道")

        sim_encoding = self._prepare_panel_encoding(
            panel, feature_plan, first, entry_signals, exit_signals, sim_end
        )
        return PreparedPanelBacktest(
            signature=signature,
            panel=panel,
            columns=frozenset(panel.columns),
            load_start=load_start,
            load_end=load_end,
            sim_end=sim_end,
            sim_encoding=sim_encoding,
        )
