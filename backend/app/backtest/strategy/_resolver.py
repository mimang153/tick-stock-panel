from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import date

from app.backtest.engine import BacktestEngine, history_load_start
from app.backtest.fundamentals import FUNDAMENTAL_FACTOR_NAMES
from app.backtest.matrix import MatrixCacheProfile, MatrixPrewarmCancelledError
from app.config import settings
from app.indicators.pipeline import INDICATOR_COLUMNS, LIMIT_SIGNAL_OUTPUTS, get_signal_dependencies
from app.strategy.engine import StrategyDef, StrategyEngine
from app.strategy.scoring import effective_scoring, scoring_dependencies, scoring_warmup_bars

from ._common import (
    _EXECUTION_COLUMNS,
    _INSTRUMENT_COLUMNS,
    _LIMIT_BASE_COLUMNS,
    FeaturePlan,
    ResolvedFeaturePlan,
    _basic_filter_dependencies,
    _basic_filter_for_asset,
    _filter_dependencies,
    _merge_resolved_feature_plans,
    _normalize_signal_name,
    _overlay_numeric_fields,
    _resolve_base_columns,
    logger,
)


class StrategyDependencyResolver:
    """Resolve all backtest field dependencies once before loading market data."""

    def resolve(
        self,
        strategy: StrategyDef,
        *,
        params: dict,
        basic_filter: dict,
        entry_signals: list[str],
        exit_signals: list[str],
        overrides: dict | None = None,
        minute_fill: bool = False,
        asset_type: str = "stock",
    ) -> ResolvedFeaturePlan:
        overrides = overrides or {}
        basic_filter = _basic_filter_for_asset(basic_filter, asset_type)
        if strategy.execution_backend == "matrix_native":
            return self._resolve_matrix_native(
                strategy,
                params=params,
                basic_filter=basic_filter,
                overrides=overrides,
            )

        (
            required_features,
            required_signals,
            full_fallback,
            scoring,
            signal_dependencies,
        ) = self._resolve_required_sets(
            strategy,
            params=params,
            basic_filter=basic_filter,
            entry_signals=entry_signals,
            exit_signals=exit_signals,
            overrides=overrides,
        )

        indicator_columns = frozenset(required_features & set(INDICATOR_COLUMNS))
        base_columns = _resolve_base_columns(required_features | set(_EXECUTION_COLUMNS))
        # 涨跌停族: 显式请求连板/判定价列, 或引用了判定价的自定义信号
        # (signal_limit_* 等迁移定义) → 统一加载不复权三价。
        if (required_signals & set(LIMIT_SIGNAL_OUTPUTS)) or any(
            signal_dependencies.get(sig, frozenset()) & {"limit_up_price", "limit_down_price"}
            for sig in required_signals
        ):
            base_columns = frozenset(set(base_columns) | set(_LIMIT_BASE_COLUMNS))

        instrument_columns = frozenset(required_features & set(_INSTRUMENT_COLUMNS))
        instrument_columns = frozenset(set(instrument_columns) | {"name"})
        matrix_columns = set(_EXECUTION_COLUMNS) | required_signals
        if minute_fill:
            indicator_columns = frozenset(set(indicator_columns) | {"ma5", "ma10", "ma20"})
            matrix_columns.update({"ma5", "ma10", "ma20"})
            base_columns = frozenset(set(base_columns) | {"close"})

        plan = FeaturePlan(
            required_features=frozenset(required_features),
            required_signals=frozenset(required_signals),
            warmup_bars=max(60, int(strategy.lookback_days or 1), scoring_warmup_bars(scoring)),
        )
        return ResolvedFeaturePlan(
            base_columns=base_columns,
            intermediate_columns=frozenset(),
            indicator_columns=indicator_columns,
            signal_columns=plan.required_signals,
            matrix_columns=frozenset(matrix_columns),
            instrument_columns=instrument_columns,
            warmup_bars=plan.warmup_bars,
            full_feature_fallback=full_fallback,
            execution_backend=strategy.execution_backend,
            fundamental_columns=frozenset(required_features & FUNDAMENTAL_FACTOR_NAMES),
        )

    @staticmethod
    def _resolve_required_sets(
        strategy: StrategyDef,
        *,
        params: dict,
        basic_filter: dict,
        entry_signals: list[str],
        exit_signals: list[str],
        overrides: dict,
    ) -> tuple[set[str], set[str], bool, dict, dict]:
        """收集非矩阵策略的必需特征/信号集合, 并返回评分配置与信号依赖表。"""
        required_features = set(strategy.required_features)
        required_signals = {
            _normalize_signal_name(signal) for signal in [*entry_signals, *exit_signals] if signal
        }
        required_signals.update({"signal_limit_up", "signal_limit_down"})

        scoring = effective_scoring(strategy.meta.get("scoring"), overrides)
        required_features.update(scoring_dependencies(scoring))
        order_by = strategy.meta.get("order_by")
        if order_by and order_by != "score":
            required_features.add(str(order_by))

        required_features.update(_basic_filter_dependencies(basic_filter))
        filter_features, filter_resolved = _filter_dependencies(strategy, params)
        required_features.update(filter_features)
        embedded_signals = {
            feature for feature in required_features if feature.startswith(("signal_", "csg_"))
        }
        required_signals.update(embedded_signals)
        required_features.difference_update(embedded_signals)

        full_fallback = bool(strategy.filter_history_fn and not strategy.required_features)
        full_fallback = full_fallback or not filter_resolved
        signal_dependencies = get_signal_dependencies()
        if full_fallback:
            logger.warning(
                "strategy %s has dynamic Python dependencies without REQUIRED_FEATURES; "
                "backtest falls back to full feature computation",
                strategy.meta.get("id", "<unknown>"),
            )
            required_features.update(INDICATOR_COLUMNS)
            # 动态依赖策略同样可能引用 instruments 股本列 (如 float_shares)
            # — 全量兜底语义下一并加载, 代价是多两列 join。
            required_features.update(_INSTRUMENT_COLUMNS - {"name"})
            required_signals.update(signal_dependencies)
            required_signals.update(LIMIT_SIGNAL_OUTPUTS)

        unknown_signals = required_signals - set(signal_dependencies) - set(LIMIT_SIGNAL_OUTPUTS)
        if unknown_signals:
            raise ValueError(f"策略引用了不存在的信号: {sorted(unknown_signals)}")
        for signal in required_signals:
            required_features.update(signal_dependencies.get(signal, ()))
        return required_features, required_signals, full_fallback, scoring, signal_dependencies

    @staticmethod
    def _resolve_matrix_native(
        strategy: StrategyDef,
        *,
        params: dict,
        basic_filter: dict,
        overrides: dict,
    ) -> ResolvedFeaturePlan:
        if strategy.matrix_strategy is None:
            raise ValueError(
                f"matrix_native strategy {strategy.meta.get('id', '<unknown>')} "
                "must declare MATRIX_STRATEGY"
            )

        required_features = set(strategy.required_features)
        required_features.update(strategy.matrix_strategy.required_fields())
        parameter_fields = getattr(
            strategy.matrix_strategy,
            "required_fields_for_params",
            None,
        )
        parameter_scoring: dict[str, float] = {}
        if callable(parameter_fields):
            parameter_scoring = {str(name): 1.0 for name in parameter_fields(params)}
            required_features.update(scoring_dependencies(parameter_scoring))
        required_features.update(_basic_filter_dependencies(basic_filter))
        scoring = effective_scoring(strategy.meta.get("scoring"), overrides)
        required_features.update(scoring_dependencies(scoring))
        order_by = strategy.meta.get("order_by")
        if order_by and order_by != "score":
            required_features.add(str(order_by))
        # 叠加条件字段并入矩阵列: 基础指标列来自 enriched 分区; ext_* 时序数值列
        # 由引擎在矩阵加载后按 (日期, 标的) 对齐注入, 供入场掩码按 (时间, 标的) 评估。
        overlay_fields = _overlay_numeric_fields(overrides)

        base_columns = _resolve_base_columns(required_features | set(_EXECUTION_COLUMNS))
        base_columns = frozenset(set(base_columns) | set(_LIMIT_BASE_COLUMNS))
        instrument_columns = frozenset(required_features & set(_INSTRUMENT_COLUMNS))
        instrument_columns = frozenset(set(instrument_columns) | {"name"})
        warmup_bars = max(
            60,
            int(strategy.matrix_strategy.required_warmup_bars(params)),
            scoring_warmup_bars(scoring),
            scoring_warmup_bars(parameter_scoring),
        )
        ext_columns = {field for field in required_features if field.startswith("ext_")}
        matrix_columns = (
            set(base_columns)
            | set(instrument_columns)
            | overlay_fields
            | ext_columns
            | {
                "signal_limit_up",
                "signal_limit_down",
            }
        )
        return ResolvedFeaturePlan(
            base_columns=base_columns,
            intermediate_columns=frozenset(),
            indicator_columns=frozenset(),
            signal_columns=frozenset({"signal_limit_up", "signal_limit_down"}),
            matrix_columns=frozenset(matrix_columns),
            instrument_columns=instrument_columns,
            warmup_bars=warmup_bars,
            full_feature_fallback=False,
            execution_backend="matrix_native",
            fundamental_columns=frozenset(required_features & FUNDAMENTAL_FACTOR_NAMES),
        )


def _collect_matrix_profile_plans(
    strategy_engine: StrategyEngine,
    asset_type: str,
    *,
    requested_plan: ResolvedFeaturePlan | None,
    requested_forward_bars: int,
) -> tuple[list[ResolvedFeaturePlan], int]:
    """收集已注册矩阵策略的依赖计划, 并推导合并后的 forward_bars。"""
    resolver = StrategyDependencyResolver()
    plans: list[ResolvedFeaturePlan] = []
    if requested_plan is not None:
        plans.append(requested_plan)
    forward_bars = max(0, int(requested_forward_bars))
    common_filter = {
        "enabled": True,
        "amount_min": 0.0,
        "turnover_min": 0.0,
        "market_cap_min": 0.0,
        "float_cap_min": 0.0,
        "exclude_st": True,
    }
    definitions = (
        strategy_engine.strategy_definitions()
        if hasattr(strategy_engine, "strategy_definitions")
        else ()
    )
    for strategy in definitions:
        if strategy.execution_backend != "matrix_native":
            continue
        if asset_type not in strategy.meta.get("asset_types", ["stock"]):
            continue
        if "1d" not in strategy.meta.get("timeframes", ["1d"]):
            continue
        params = StrategyEngine.resolve_params(strategy)
        for item in strategy.meta.get("params", []):
            if not isinstance(item, dict) or not item.get("id"):
                continue
            if item.get("type") in {"int", "float"} and item.get("max") is not None:
                params[str(item["id"])] = item["max"]
        plans.append(
            resolver.resolve(
                strategy,
                params=params,
                basic_filter={**dict(strategy.basic_filter or {}), **common_filter},
                entry_signals=strategy.entry_signals,
                exit_signals=strategy.exit_signals,
                overrides={},
                minute_fill=False,
                asset_type=asset_type,
            )
        )
        forward_bars = max(forward_bars, int(strategy.max_hold_days or 0))
    return plans, forward_bars


def build_matrix_cache_profile(
    strategy_engine: StrategyEngine,
    asset_type: str,
    *,
    requested_plan: ResolvedFeaturePlan | None = None,
    requested_forward_bars: int = 0,
    max_disk_bytes: int = 512 * 1024 * 1024,
) -> MatrixCacheProfile:
    """Merge registered matrix dependencies into one strategy-agnostic cache profile."""
    plans, forward_bars = _collect_matrix_profile_plans(
        strategy_engine,
        asset_type,
        requested_plan=requested_plan,
        requested_forward_bars=requested_forward_bars,
    )
    if not plans:
        raise ValueError(f"no matrix-native cache profile available for asset_type={asset_type!r}")
    merged = _merge_resolved_feature_plans(plans)
    # 扩展字段按当前策略及其请求日期现场注入, 不属于共享 parquet/mmap 缓存。
    fields = frozenset(
        name
        for name in (
            set(merged.base_columns) | set(merged.instrument_columns) | set(merged.matrix_columns)
        )
        if not name.startswith("ext_")
    )
    generation_payload = json.dumps(
        {
            "asset_type": asset_type,
            "fields": sorted(fields),
            "warmup_bars": merged.warmup_bars,
            "forward_bars": forward_bars,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    generation = hashlib.blake2b(
        generation_payload.encode("utf-8"),
        digest_size=12,
    ).hexdigest()
    return MatrixCacheProfile(
        field_columns=fields,
        warmup_bars=merged.warmup_bars,
        forward_bars=forward_bars,
        max_disk_bytes=int(max_disk_bytes),
        generation=generation,
    )


def prewarm_matrix_cache(
    engine: BacktestEngine,
    strategy_engine: StrategyEngine,
    *,
    asset_type: str,
    latest_date: date,
    years: int = 5,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    """Build the shared full-universe mmap outside a user backtest request."""
    if years <= 0:
        raise ValueError("matrix cache prewarm years must be positive")
    if cancel_event is not None and cancel_event.is_set():
        raise MatrixPrewarmCancelledError("matrix cache prewarm cancelled")
    profile = build_matrix_cache_profile(
        strategy_engine,
        asset_type,
        max_disk_bytes=settings.backtest_matrix_cache_max_mb * 1024 * 1024,
    )
    formal_start = date(max(1, latest_date.year - years + 1), 1, 1)
    coverage_start = history_load_start(engine, formal_start, asset_type, profile.warmup_bars)
    prewarm_columns = frozenset(
        {
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "raw_close",
            "raw_high",
        }
    )
    plan = ResolvedFeaturePlan(
        base_columns=prewarm_columns,
        intermediate_columns=frozenset(),
        indicator_columns=frozenset(),
        signal_columns=frozenset(),
        matrix_columns=prewarm_columns,
        instrument_columns=frozenset({"name"}),
        warmup_bars=profile.warmup_bars,
        full_feature_fallback=False,
        execution_backend="matrix_native",
    )
    started = time.perf_counter()
    market = engine.load_market_data_matrix_for_backtest(
        None,
        coverage_start,
        latest_date,
        plan,
        asset_type=asset_type,
        cache_profile=profile,
        coverage_start=coverage_start,
        coverage_end=latest_date,
        cancel_event=cancel_event,
    )
    result: dict[str, object] = {
        "asset_type": asset_type,
        "start": coverage_start.isoformat(),
        "end": latest_date.isoformat(),
        "cache_status": market.cache_status,
        "cache_path": market.cache_path,
        "bytes": market.nbytes,
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
    }
    del market
    return result
