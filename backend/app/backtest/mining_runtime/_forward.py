"""Internal split module for app.backtest.mining_runtime: _forward."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, cast

import polars as pl

import app.backtest.mining_runtime as _self
from app.backtest.factor import FactorBacktestService
from app.backtest.mining import (
    MAX_FINALISTS,
    MiningBudget,
    MiningRequest,
    generate_nested_folds,
    required_outer_folds,
    required_trading_bars,
    validation_config_for_profile,
)
from app.backtest.strategy import (
    ResolvedFeaturePlan,
    StrategyBacktestService,
    StrategyDependencyResolver,
    _merge_resolved_feature_plans,
)
from app.factors.registry import missing_factor_ids
from app.strategy import config as strategy_config
from app.strategy.engine import StrategyEngine

from ._common import _PROFILE_NAMES, CancelCheck, RuntimeRequest
from ._refine import _bounded_float, _optional_date


def _prepare_single_forward_panel(
    panel: pl.DataFrame,
    *,
    start: date,
    end: date,
    horizon: int,
    trading_dates: Sequence[date],
    factor_names: Sequence[str],
    assume_unique_symbol_date: bool,
) -> tuple[tuple[str, ...], tuple[date, ...], pl.DataFrame]:
    """Validate inputs and build the scoped panel for forward-return attachment."""
    if horizon <= 0:
        raise ValueError("forward horizon must be positive")
    names = tuple(str(name) for name in factor_names)
    required = {"symbol", "date", "close", *names}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"factor panel is missing required columns: {missing}")
    dates = tuple(sorted(dict.fromkeys(value for value in trading_dates if start <= value <= end)))
    if not dates:
        raise ValueError("mining date range has no trading dates")

    valid_rows = (
        (pl.col("date") >= start)
        & (pl.col("date") <= end)
        & pl.col("close").is_not_null()
        & (pl.col("close") > 0)
    )
    if assume_unique_symbol_date:
        invalid_count = panel.select((~valid_rows).sum()).item()
        if invalid_count:
            raise ValueError("prevalidated mining panel contains invalid price rows")
        scoped = panel.select(
            [
                "symbol",
                "date",
                "close",
                *(pl.col(name).cast(pl.Float32, strict=False).alias(name) for name in names),
            ]
        )
    else:
        scoped = (
            panel.filter(valid_rows)
            .select(
                [
                    "symbol",
                    "date",
                    "close",
                    *(pl.col(name).cast(pl.Float32, strict=False).alias(name) for name in names),
                ]
            )
            .unique(subset=["symbol", "date"], keep="last")
        )
    return names, dates, scoped


def attach_single_forward_return(
    panel: pl.DataFrame,
    *,
    start: date,
    end: date,
    horizon: int,
    trading_dates: Sequence[date],
    factor_names: Sequence[str],
    target_column: str = "_next_return",
    assume_unique_symbol_date: bool = False,
) -> pl.DataFrame:
    """Materialize exactly one global-axis forward label and its endpoint date."""
    names, dates, scoped = _prepare_single_forward_panel(
        panel,
        start=start,
        end=end,
        horizon=horizon,
        trading_dates=trading_dates,
        factor_names=factor_names,
        assume_unique_symbol_date=assume_unique_symbol_date,
    )
    date_dtype = scoped.schema["date"]
    target_date_column = "_target_date"
    if len(dates) > horizon:
        date_map = pl.DataFrame(
            {
                "date": dates[:-horizon],
                target_date_column: dates[horizon:],
            }
        ).with_columns(
            pl.col("date").cast(date_dtype),
            pl.col(target_date_column).cast(date_dtype),
        )
    else:
        date_map = pl.DataFrame(schema={"date": date_dtype, target_date_column: date_dtype})
    prices = scoped.select("symbol", "date", "close")
    lookup = prices.select(
        "symbol",
        pl.col("date").alias(target_date_column),
        pl.col("close").alias("_target_close"),
    )
    labels = (
        prices.join(date_map, on="date", how="left")
        .join(lookup, on=["symbol", target_date_column], how="left")
        .select(
            "symbol",
            "date",
            pl.when(pl.col("_target_close").is_not_null())
            .then(pl.col("_target_close") / pl.col("close") - 1.0)
            .otherwise(None)
            .cast(pl.Float32)
            .alias(target_column),
            target_date_column,
        )
    )
    if assume_unique_symbol_date:
        labels = labels.sort(["date", "symbol"])
        if not labels.get_column("symbol").equals(
            scoped.get_column("symbol")
        ) or not labels.get_column("date").equals(scoped.get_column("date")):
            raise ValueError("prevalidated mining panel is not sorted by date and symbol")
        return scoped.select(["symbol", "date", *names]).hstack(
            [
                labels.get_column(target_column),
                labels.get_column(target_date_column),
            ]
        )
    return (
        scoped.select(["symbol", "date", *names])
        .join(labels, on=["symbol", "date"], how="left")
        .sort(["date", "symbol"])
    )


def _decode_runtime_selection(
    payload: Mapping[str, Any],
    strategy_engine: StrategyEngine,
) -> tuple[str, Mapping[str, Any], tuple[str, ...], tuple[str, ...], str]:
    run_id = str(payload.get("run_id") or "")
    request = payload.get("request")
    if not run_id or not isinstance(request, Mapping):
        raise ValueError("mining worker payload is missing run_id or request")

    factor_names = tuple(str(value) for value in request.get("factor_names") or ())
    if not factor_names or len(set(factor_names)) != len(factor_names):
        raise ValueError("factor_names must be non-empty and unique")
    # 注册表动态读取: worker 子进程已在入口加载自定义/复合因子
    unknown_factors = missing_factor_ids(factor_names)
    if unknown_factors:
        raise ValueError(f"unknown mining factors: {unknown_factors}")
    if len(factor_names) > 48:
        raise ValueError("mining supports at most 48 factors")

    strategy_ids = tuple(str(value) for value in request.get("strategy_ids") or ())
    if len(set(strategy_ids)) != len(strategy_ids) or len(strategy_ids) > 8:
        raise ValueError("strategy_ids must be unique and contain at most 8 strategies")
    asset_type = str(request.get("asset_type") or "stock")
    if asset_type not in {"stock", "etf"}:
        raise ValueError("mining asset_type must be stock or etf")
    for strategy_id in strategy_ids:
        strategy = strategy_engine.get(strategy_id)
        if strategy.meta.get("research_only"):
            raise ValueError(
                f"research template cannot be selected as existing strategy: {strategy_id}"
            )
        if strategy.execution_backend != "matrix_native":
            raise ValueError(f"mining strategy is not matrix-native: {strategy_id}")
        if asset_type not in strategy.meta.get("asset_types", ["stock"]):
            raise ValueError(f"mining strategy does not support {asset_type}: {strategy_id}")
    return run_id, request, factor_names, strategy_ids, asset_type


def _resolve_runtime_window(
    data_dir: Path,
    request: Mapping[str, Any],
    asset_type: str,
) -> tuple[list[date], date, date]:
    all_dates = _self.enriched_partition_dates(data_dir, asset_type)
    if not all_dates:
        raise ValueError(f"no enriched {asset_type} trading dates are available")
    requested_start = _optional_date(request.get("start"))
    requested_end = _optional_date(request.get("end"))
    if (
        requested_start is not None
        and requested_end is not None
        and requested_start > requested_end
    ):
        raise ValueError("mining start must not be after end")
    start = max(requested_start or all_dates[0], all_dates[0])
    end = min(requested_end or all_dates[-1], all_dates[-1])
    if start > end:
        raise ValueError("mining date range contains no enriched data")
    return all_dates, start, end


def _resolve_runtime_budget(
    request: Mapping[str, Any],
    all_dates: list[date],
    start: date,
    end: date,
) -> tuple[str, Any, int, MiningBudget, int, float]:
    profile = str(request.get("budget_profile") or "balanced")
    if profile not in _PROFILE_NAMES:
        raise ValueError(f"unsupported mining profile: {profile}")
    validation = validation_config_for_profile(profile)
    forward_horizon = int(request.get("forward_horizon") or 5)
    if forward_horizon not in {1, 3, 5}:
        raise ValueError("forward_horizon must be 1, 3, or 5 trading days")
    if validation.purge_bars < forward_horizon:
        raise ValueError("validation purge must cover the forward horizon")

    budget = getattr(MiningBudget, profile)()
    max_combination = int(request.get("max_combination_factors") or budget.max_combination_size)
    beam_width = int(request.get("beam_width") or budget.beam_width)
    max_finalists = int(request.get("max_finalists") or MAX_FINALISTS)
    if not 1 <= max_finalists <= MAX_FINALISTS:
        raise ValueError(f"max_finalists must be between 1 and {MAX_FINALISTS}")
    scoped_dates = [value for value in all_dates if start <= value <= end]
    required_folds = required_outer_folds(profile)
    required_bars = required_trading_bars(validation, required_folds)
    if len(scoped_dates) < required_bars:
        fold_label = "outer fold" if required_folds == 1 else "outer folds"
        raise ValueError(
            f"{profile} mining requires at least {required_bars} enriched trading "
            f"bars for {required_folds} {fold_label}; effective range "
            f"{start.isoformat()} to {end.isoformat()} has {len(scoped_dates)}"
        )
    nested = generate_nested_folds(
        [value.isoformat() for value in scoped_dates],
        validation,
    )
    reserved_regime_trials = 3 * len(nested) if request.get("require_regime", True) else 0
    real_trials = max(len(nested) + 1, budget.max_trials - reserved_regime_trials)
    real_trials = min(real_trials, budget.max_trials)
    budget = replace(
        budget,
        max_combination_size=max_combination,
        beam_width=beam_width,
        max_trials=real_trials,
    )
    correlation_threshold = _bounded_float(
        request.get("correlation_threshold", 0.75),
        "correlation_threshold",
        0.0,
        1.0,
        exclusive_min=True,
    )
    return (
        profile,
        validation,
        forward_horizon,
        budget,
        max_finalists,
        correlation_threshold,
    )


def _decode_runtime_costs_and_symbols(
    request: Mapping[str, Any],
) -> tuple[float, float, float, list[str] | None]:
    commission_pct = _bounded_float(
        request.get("commission_pct", 0.0002), "commission_pct", 0.0, 0.05
    )
    stamp_tax_pct = _bounded_float(request.get("stamp_tax_pct", 0.0005), "stamp_tax_pct", 0.0, 0.05)
    slippage_bps = _bounded_float(request.get("slippage_bps", 5.0), "slippage_bps", 0.0, 1000.0)
    symbols_value = request.get("symbols")
    symbols = None
    if symbols_value is not None:
        if not isinstance(symbols_value, list):
            raise ValueError("symbols must be a list or null")
        symbols = list(dict.fromkeys(str(value) for value in symbols_value if value))
        if not symbols:
            symbols = None
    return commission_pct, stamp_tax_pct, slippage_bps, symbols


def _decode_runtime_request(
    payload: Mapping[str, Any],
    data_dir: Path,
    strategy_engine: StrategyEngine,
) -> RuntimeRequest:
    run_id, request, factor_names, strategy_ids, asset_type = _decode_runtime_selection(
        payload, strategy_engine
    )
    all_dates, start, end = _resolve_runtime_window(data_dir, request, asset_type)
    (
        profile,
        validation,
        forward_horizon,
        budget,
        max_finalists,
        correlation_threshold,
    ) = _resolve_runtime_budget(request, all_dates, start, end)
    commission_pct, stamp_tax_pct, slippage_bps, symbols = _decode_runtime_costs_and_symbols(
        request
    )

    mining_request = MiningRequest(
        factor_names=factor_names,
        existing_strategy_ids=strategy_ids,
        correlation_threshold=correlation_threshold,
        target_column="_next_return",
        budget=budget,
        validation=validation,
        profile=profile,  # type: ignore[arg-type]
    )
    return RuntimeRequest(
        run_id=run_id,
        factor_names=factor_names,
        strategy_ids=strategy_ids,
        symbols=symbols,
        asset_type=asset_type,  # type: ignore[arg-type]
        start=start,
        end=end,
        profile=profile,  # type: ignore[arg-type]
        forward_horizon=forward_horizon,
        commission_pct=commission_pct,
        stamp_tax_pct=stamp_tax_pct,
        slippage_bps=slippage_bps,
        correlation_threshold=correlation_threshold,
        max_finalists=max_finalists,
        require_regime=bool(request.get("require_regime", True)),
        mining_request=mining_request,
    )


def _prepare_base_market(
    service: StrategyBacktestService,
    strategy_engine: StrategyEngine,
    data_dir: Path,
    request: RuntimeRequest,
    *,
    expected_generation: str | None = None,
    cancel_check: CancelCheck | None = None,
):
    resolver = StrategyDependencyResolver()
    plans: list[ResolvedFeaturePlan] = []
    research = strategy_engine.get("factor_rank_research")
    for offset in range(0, len(request.factor_names), 4):
        factor_chunk = request.factor_names[offset : offset + 4]
        research_params = {
            "scoring": {factor_name: 1.0 for factor_name in factor_chunk},
            "directions": {factor_name: "high" for factor_name in factor_chunk},
        }
        plans.append(
            resolver.resolve(
                research,
                params=research_params,
                basic_filter=service._effective_basic_filter(research, {}),
                entry_signals=research.entry_signals,
                exit_signals=research.exit_signals,
                overrides={},
                asset_type=request.asset_type,
            )
        )
    for strategy_id in request.strategy_ids:
        strategy = strategy_engine.get(strategy_id)
        overrides = strategy_config.load_override(data_dir, strategy_id)
        params = strategy_engine.resolve_params(strategy, overrides=overrides)
        plans.append(
            resolver.resolve(
                strategy,
                params=params,
                basic_filter=service._effective_basic_filter(strategy, overrides),
                entry_signals=service._effective_signals(
                    overrides, "entry_signals", strategy.entry_signals
                ),
                exit_signals=service._effective_signals(
                    overrides, "exit_signals", strategy.exit_signals
                ),
                overrides=overrides,
                asset_type=request.asset_type,
            )
        )
    merged = _merge_resolved_feature_plans(plans)
    profile = _self.build_matrix_cache_profile(
        strategy_engine,
        request.asset_type,
        requested_plan=merged,
        requested_forward_bars=request.forward_horizon,
    )
    load_start = FactorBacktestService(service.engine)._factor_load_start(
        request,
        request.factor_names,
        warmup_bars=merged.warmup_bars,
    )
    return service.engine.load_market_data_matrix_for_backtest(
        request.symbols,
        load_start,
        request.end,
        merged,
        asset_type=request.asset_type,
        cache_profile=profile,
        coverage_start=load_start,
        coverage_end=request.end,
        expected_generation=expected_generation,
        cancel_event=cast(Any, cancel_check),
    )
