"""Internal split module for app.backtest.mining: _search."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import polars as pl

from ._candidates import (
    _cancelled,
    _factor_candidate,
    _finite_sort_value,
    _make_validation_fold,
    _proxy_rank_ic,
    _rank_candidates,
    _validate_factor_names,
)
from ._common import (
    MAX_BEAM_WIDTH,
    MAX_COMBINATION_SIZE,
    BeamSearchResult,
    CancelCheck,
    CorrelationResult,
    FactorExclusion,
    FactorMetric,
    MiningCandidate,
    NestedFold,
    NestedValidationConfig,
    PruneResult,
    ValidationFold,
    logger,
)


def _prepare_correlation_scope(
    panel: pl.DataFrame,
    factor_names: Sequence[str],
    start: Any | None,
    end: Any | None,
    date_column: str,
) -> tuple[tuple[str, ...], pl.DataFrame, float]:
    names = _validate_factor_names(panel, factor_names)
    if date_column not in panel.columns:
        raise ValueError(f"panel is missing date column {date_column!r}")

    filter_started = time.perf_counter()
    date_expr = pl.col(date_column).cast(pl.Utf8).str.slice(0, 10)
    scoped = panel.select(
        [
            pl.col(date_column),
            *(
                pl.when(pl.col(name).is_finite()).then(pl.col(name)).otherwise(None).alias(name)
                for name in names
            ),
        ]
    )
    if start is not None:
        scoped = scoped.filter(date_expr >= str(start)[:10])
    if end is not None:
        scoped = scoped.filter(date_expr <= str(end)[:10])
    if scoped.is_empty():
        raise ValueError("rank correlation date range contains no panel rows")
    return names, scoped, (time.perf_counter() - filter_started) * 1000.0


def _correlation_pair_expressions(
    names: Sequence[str],
) -> tuple[dict[tuple[int, int], str], list[pl.Expr], list[str]]:
    width = len(names)
    pair_columns: dict[tuple[int, int], str] = {}
    pair_expressions: list[pl.Expr] = []
    for left in range(width):
        for right in range(left + 1, width):
            column = f"_rho_{left}_{right}"
            pair_columns[(left, right)] = column
            pair_expressions.append(
                pl.corr(names[left], names[right], method="spearman").alias(column)
            )
    observation_columns = [f"_n_{index}" for index in range(width)]
    return pair_columns, pair_expressions, observation_columns


def _aggregate_correlation_matrices(
    daily: pl.DataFrame,
    names: Sequence[str],
    pair_columns: dict[tuple[int, int], str],
    observation_columns: list[str],
) -> tuple[np.ndarray, np.ndarray]:
    width = len(names)
    correlation = np.full((width, width), np.nan, dtype=np.float64)
    counts = np.zeros((width, width), dtype=np.int32)
    for (left, right), column in pair_columns.items():
        values = daily.get_column(column).to_numpy()
        finite = np.isfinite(values)
        count = int(np.count_nonzero(finite))
        if count:
            value = float(np.mean(values[finite]))
            correlation[left, right] = correlation[right, left] = value
            counts[left, right] = counts[right, left] = count
    for index, column in enumerate(observation_columns):
        count = int(np.count_nonzero(daily.get_column(column).to_numpy() >= 2))
        if count:
            correlation[index, index] = 1.0
            counts[index, index] = count
    return correlation, counts


def compute_rank_correlation(
    panel: pl.DataFrame,
    factor_names: Sequence[str],
    start: Any | None = None,
    end: Any | None = None,
    *,
    date_column: str = "date",
) -> CorrelationResult:
    """Average pairwise daily cross-sectional rank correlations.

    One date partition is ranked and materialized at a time. Only the factor-by-factor
    correlation sums and valid-day counts remain resident across dates.
    """
    started = time.perf_counter()
    names, scoped, filter_ms = _prepare_correlation_scope(
        panel, factor_names, start, end, date_column
    )
    pair_columns, pair_expressions, observation_columns = _correlation_pair_expressions(names)

    rank_started = time.perf_counter()
    daily = scoped.group_by(date_column).agg(
        [
            *pair_expressions,
            *(
                pl.col(name).count().alias(column)
                for name, column in zip(names, observation_columns, strict=True)
            ),
        ]
    )
    rank_ms = (time.perf_counter() - rank_started) * 1000.0

    finish_started = time.perf_counter()
    correlation, counts = _aggregate_correlation_matrices(
        daily, names, pair_columns, observation_columns
    )
    finish_ms = (time.perf_counter() - finish_started) * 1000.0
    n_dates = daily.height
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    timing = {
        "filter": round(filter_ms, 3),
        "rank_accumulate": round(rank_ms, 3),
        "finalize": round(finish_ms, 3),
        "total": round(elapsed_ms, 3),
    }
    logger.info(
        "mining rank correlation factors=%d rows=%d dates=%d elapsed_ms=%.1f",
        len(names),
        scoped.height,
        n_dates,
        elapsed_ms,
    )
    return CorrelationResult(
        factor_names=names,
        matrix=tuple(tuple(float(value) for value in row) for row in correlation),
        pair_counts=tuple(tuple(int(value) for value in row) for row in counts),
        elapsed_ms=round(elapsed_ms, 3),
        n_dates=n_dates,
        n_rows=scoped.height,
        timing_ms=timing,
    )


def prune_correlated_factors(
    metrics: Sequence[FactorMetric],
    correlation: CorrelationResult,
    threshold: float,
) -> PruneResult:
    """Keep the strongest deterministic representative of correlated factors."""
    if not 0.0 < threshold <= 1.0:
        raise ValueError("correlation threshold must be in (0, 1]")
    by_name = {name: index for index, name in enumerate(correlation.factor_names)}
    if len({metric.factor_id for metric in metrics}) != len(metrics):
        raise ValueError("factor metrics must not contain duplicate factor_id values")
    missing = sorted(metric.factor_id for metric in metrics if metric.factor_id not in by_name)
    if missing:
        raise ValueError(f"factor metrics missing from correlation result: {missing}")

    ordered = sorted(
        metrics,
        key=lambda metric: (
            -_finite_sort_value(metric.composite_score),
            -_finite_sort_value(metric.ir),
            -_finite_sort_value(metric.coverage),
            _finite_sort_value(metric.turnover, worst=float("inf")),
            metric.factor_id,
        ),
    )
    selected: list[str] = []
    excluded: list[FactorExclusion] = []
    for metric in ordered:
        factor_index = by_name[metric.factor_id]
        correlated = [
            (representative, correlation.matrix[factor_index][by_name[representative]])
            for representative in selected
            if (
                correlation.pair_counts[factor_index][by_name[representative]] > 0
                and math.isfinite(correlation.matrix[factor_index][by_name[representative]])
                and abs(correlation.matrix[factor_index][by_name[representative]]) >= threshold
            )
        ]
        if not correlated:
            selected.append(metric.factor_id)
            continue
        representative, rho = max(
            correlated,
            key=lambda item: (abs(item[1]), -selected.index(item[0])),
        )
        excluded.append(
            FactorExclusion(
                factor_id=metric.factor_id,
                reason="correlation_threshold",
                representative=representative,
                rho=round(float(rho), 8),
            )
        )
    return PruneResult(selected=tuple(selected), excluded=tuple(excluded))


def _validate_beam_search_params(
    panel: pl.DataFrame,
    factor_names: Sequence[str],
    target_column: str,
    date_column: str,
    max_combination_size: int,
    beam_width: int,
    max_trials: int,
) -> tuple[str, ...]:
    names = _validate_factor_names(panel, factor_names)
    if target_column not in panel.columns:
        raise ValueError(f"panel is missing target column {target_column!r}")
    if date_column not in panel.columns:
        raise ValueError(f"panel is missing date column {date_column!r}")
    if not 1 <= max_combination_size <= MAX_COMBINATION_SIZE:
        raise ValueError(f"max_combination_size must be between 1 and {MAX_COMBINATION_SIZE}")
    if not 1 <= beam_width <= MAX_BEAM_WIDTH:
        raise ValueError(f"beam_width must be between 1 and {MAX_BEAM_WIDTH}")
    if max_trials <= 0:
        raise ValueError("max_trials must be positive")
    return names


def _prepare_beam_blocks(
    panel: pl.DataFrame,
    names: Sequence[str],
    target_column: str,
    date_column: str,
    train_start: Any | None,
    train_end: Any | None,
) -> tuple[tuple[str, ...], dict[str, int], tuple]:
    date_expr = pl.col(date_column).cast(pl.Utf8).str.slice(0, 10)
    scoped = panel.select([date_column, *names, target_column])
    if train_start is not None:
        scoped = scoped.filter(date_expr >= str(train_start)[:10])
    if train_end is not None:
        scoped = scoped.filter(date_expr <= str(train_end)[:10])
    if scoped.is_empty():
        raise ValueError("beam search training range contains no panel rows")

    ordered_names = tuple(sorted(names))
    name_to_column = {name: index for index, name in enumerate(ordered_names)}
    if not scoped.get_column(date_column).is_sorted():
        scoped = scoped.sort(date_column)
    blocks = tuple(
        daily.select([*ordered_names, target_column]).to_numpy()
        for daily in scoped.partition_by(date_column, maintain_order=True)
    )
    return ordered_names, name_to_column, blocks


def _evaluate_beam_single_factors(
    ordered_names: tuple[str, ...],
    evaluate: Callable[[tuple[str, ...], tuple[float, ...], tuple[int, ...]], Any],
) -> tuple[dict[str, int], list[MiningCandidate]]:
    directions_by_factor: dict[str, int] = {}
    single_candidates: list[MiningCandidate] = []
    for factor_name in ordered_names:
        proxy = evaluate((factor_name,), (1.0,), (1,))
        if proxy is None:
            break
        rank_ic, proxy_ir, observations, n_dates = proxy
        if n_dates <= 0 or observations < 3:
            continue
        direction = 1 if rank_ic >= 0.0 else -1
        directions_by_factor[factor_name] = direction
        single_candidates.append(
            _factor_candidate(
                (factor_name,),
                (1.0,),
                (direction,),
                abs(rank_ic),
                abs(proxy_ir),
                observations,
                n_dates,
            )
        )
    return directions_by_factor, single_candidates


def _collect_beam_factor_sets(
    current_beam: Sequence[MiningCandidate],
    searchable_names: tuple[str, ...],
    size: int,
) -> set[tuple[str, ...]]:
    factor_sets: set[tuple[str, ...]] = set()
    for candidate in current_beam:
        for factor_name in searchable_names:
            combined = tuple(sorted({*candidate.factor_names, factor_name}))
            if len(combined) == size:
                factor_sets.add(combined)
    return factor_sets


def _expand_beam_candidates(
    factor_sets: set[tuple[str, ...]],
    directions_by_factor: dict[str, int],
    size: int,
    evaluate: Callable[[tuple[str, ...], tuple[float, ...], tuple[int, ...]], Any],
) -> tuple[list[MiningCandidate], bool]:
    expanded: list[MiningCandidate] = []
    for factors in sorted(factor_sets):
        directions = tuple(directions_by_factor[name] for name in factors)
        patterns = [(1.0,) * size]
        patterns.extend(
            tuple(2.0 if index == doubled else 1.0 for index in range(size))
            for doubled in range(size)
        )
        for weights in patterns:
            proxy = evaluate(factors, weights, directions)
            if proxy is None:
                return expanded, True
            rank_ic, proxy_ir, observations, n_dates = proxy
            if n_dates <= 0 or observations < 3:
                continue
            expanded.append(
                _factor_candidate(
                    factors,
                    weights,
                    directions,
                    rank_ic,
                    proxy_ir,
                    observations,
                    n_dates,
                )
            )
    return expanded, False


def beam_search_factor_combinations(
    panel: pl.DataFrame,
    factor_names: Sequence[str],
    *,
    target_column: str = "_next_return",
    date_column: str = "date",
    train_start: Any | None = None,
    train_end: Any | None = None,
    max_combination_size: int = MAX_COMBINATION_SIZE,
    beam_width: int = 16,
    max_trials: int = 256,
    cancel_check: CancelCheck | None = None,
) -> BeamSearchResult:
    """Search equal and one-factor-double rank combinations on training data only."""
    started = time.perf_counter()
    names = _validate_beam_search_params(
        panel,
        factor_names,
        target_column,
        date_column,
        max_combination_size,
        beam_width,
        max_trials,
    )
    ordered_names, name_to_column, blocks = _prepare_beam_blocks(
        panel, names, target_column, date_column, train_start, train_end
    )
    state: dict[str, Any] = {"trials": 0, "cancelled": False}

    def evaluate(
        factors: tuple[str, ...],
        weights: tuple[float, ...],
        directions: tuple[int, ...],
    ) -> tuple[float, float, int, int] | None:
        if state["trials"] >= max_trials:
            return None
        if _cancelled(cancel_check):
            state["cancelled"] = True
            return None
        state["trials"] += 1
        columns = tuple(name_to_column[name] for name in factors)
        return _proxy_rank_ic(blocks, columns, weights, directions)

    directions_by_factor, single_candidates = _evaluate_beam_single_factors(ordered_names, evaluate)
    current_beam = _rank_candidates(single_candidates)[:beam_width]
    retained = list(current_beam)
    searchable_names = tuple(sorted(directions_by_factor))
    max_size = min(max_combination_size, len(searchable_names))
    for size in range(2, max_size + 1):
        if state["cancelled"] or state["trials"] >= max_trials or not current_beam:
            break
        factor_sets = _collect_beam_factor_sets(current_beam, searchable_names, size)
        expanded, stop = _expand_beam_candidates(factor_sets, directions_by_factor, size, evaluate)
        current_beam = _rank_candidates(expanded)[:beam_width]
        retained.extend(current_beam)
        if stop:
            break

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    trials_used = int(state["trials"])
    return BeamSearchResult(
        candidates=tuple(_rank_candidates(retained)),
        trials_used=trials_used,
        cancelled=bool(state["cancelled"]),
        budget_exhausted=trials_used >= max_trials,
        elapsed_ms=round(elapsed_ms, 3),
    )


def generate_nested_folds(
    trading_labels: Sequence[Any],
    config: NestedValidationConfig,
) -> tuple[NestedFold, ...]:
    """Generate rolling nested folds from ordered trading labels, not calendar days."""
    labels = tuple(dict.fromkeys(str(label) for label in trading_labels))
    if not labels:
        raise ValueError("trading labels are empty")
    if tuple(sorted(labels)) != labels:
        raise ValueError("trading labels must be sorted in ascending order")
    outer_required = config.outer_train_bars + config.purge_bars + config.outer_test_bars
    if len(labels) < outer_required:
        raise ValueError(
            "insufficient trading bars for outer validation: "
            f"need at least {outer_required}, got {len(labels)}"
        )
    inner_required = config.inner_train_bars + config.purge_bars + config.inner_test_bars
    if config.outer_train_bars < inner_required:
        raise ValueError(
            "outer training window is too short for one inner fold: "
            f"need at least {inner_required}, got {config.outer_train_bars}"
        )

    nested: list[NestedFold] = []
    outer_start = 0
    outer_index = 0
    while outer_start + outer_required <= len(labels):
        outer = _make_validation_fold(
            labels,
            level="outer",
            outer_index=outer_index,
            inner_index=None,
            train_start=outer_start,
            train_bars=config.outer_train_bars,
            test_bars=config.outer_test_bars,
            purge_bars=config.purge_bars,
            embargo_bars=config.embargo_bars,
        )
        inner_folds: list[ValidationFold] = []
        inner_start = outer_start
        inner_index = 0
        outer_train_stop = outer_start + config.outer_train_bars
        while inner_start + inner_required <= outer_train_stop:
            inner_folds.append(
                _make_validation_fold(
                    labels,
                    level="inner",
                    outer_index=outer_index,
                    inner_index=inner_index,
                    train_start=inner_start,
                    train_bars=config.inner_train_bars,
                    test_bars=config.inner_test_bars,
                    purge_bars=config.purge_bars,
                    embargo_bars=config.embargo_bars,
                    hard_stop=outer_train_stop,
                )
            )
            inner_index += 1
            inner_start += config.inner_step_bars
        if not inner_folds:
            raise ValueError(f"outer fold {outer_index} contains no valid inner fold")
        nested.append(NestedFold(outer=outer, inner=tuple(inner_folds)))
        outer_index += 1
        outer_start += config.outer_step_bars
    return tuple(nested)
