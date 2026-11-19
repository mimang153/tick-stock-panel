"""Internal split module for app.backtest.mining: _candidates."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from typing import Any, Literal

import numpy as np
import polars as pl

from ._common import (
    MAX_MINING_FACTORS,
    CancelCheck,
    CandidateEvaluation,
    CandidateEvaluator,
    MiningBudget,
    MiningCandidate,
    ValidationFold,
)


def _validate_factor_names(
    panel: pl.DataFrame,
    factor_names: Sequence[str],
) -> tuple[str, ...]:
    names = tuple(str(name) for name in factor_names)
    if not names:
        raise ValueError("factor_names must not be empty")
    if len(names) > MAX_MINING_FACTORS:
        raise ValueError(f"factor count exceeds hard limit {MAX_MINING_FACTORS}")
    if len(set(names)) != len(names):
        raise ValueError("factor_names must not contain duplicates")
    missing = sorted(set(names) - set(panel.columns))
    if missing:
        raise ValueError(f"panel is missing factor columns: {missing}")
    return names


def _finite_sort_value(value: float, *, worst: float = float("-inf")) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return worst
    return number if math.isfinite(number) else worst


def _average_rank(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="stable")
    ordered = values[order]
    boundaries = np.concatenate(
        (
            np.array([0]),
            np.flatnonzero(ordered[1:] != ordered[:-1]) + 1,
            np.array([len(ordered)]),
        )
    )
    counts = np.diff(boundaries)
    average = (boundaries[:-1] + 1 + boundaries[1:]) / 2.0
    ranked = np.empty(len(values), dtype=np.float64)
    ranked[order] = np.repeat(average, counts)
    return ranked


def _proxy_rank_ic(
    blocks: Sequence[np.ndarray],
    columns: tuple[int, ...],
    weights: tuple[float, ...],
    directions: tuple[int, ...],
) -> tuple[float, float, int, int]:
    daily_ics: list[float] = []
    observations = 0
    weight_values = np.asarray(weights, dtype=np.float64)
    direction_values = np.asarray(directions, dtype=np.float64)
    for block in blocks:
        factors = block[:, columns].astype(np.float64, copy=False)
        target = block[:, -1].astype(np.float64, copy=False)
        valid = np.isfinite(target) & np.isfinite(factors).all(axis=1)
        count = int(np.count_nonzero(valid))
        if count < 3:
            continue
        factor_values = factors[valid]
        factor_ranks = np.column_stack(
            [_average_rank(factor_values[:, index]) for index in range(factor_values.shape[1])]
        )
        composite = (factor_ranks * direction_values) @ weight_values
        composite_rank = _average_rank(composite)
        target_rank = _average_rank(target[valid])
        if np.std(composite_rank) <= 0.0 or np.std(target_rank) <= 0.0:
            continue
        rho = float(np.corrcoef(composite_rank, target_rank)[0, 1])
        if math.isfinite(rho):
            daily_ics.append(rho)
            observations += count
    if not daily_ics:
        return 0.0, 0.0, observations, 0
    values = np.asarray(daily_ics, dtype=np.float64)
    mean = float(np.mean(values))
    std = float(np.std(values))
    ir = mean / std if std > 1e-12 else 0.0
    return mean, ir, observations, len(daily_ics)


def _validate_signature_factor(
    factor_name: object, scoring: Mapping[str, Any], directions: Mapping[str, Any]
) -> tuple[str, str]:
    """校验单因子的权重与方向，返回 (权重签名, 方向签名)。"""
    if not isinstance(factor_name, str) or not factor_name:
        raise ValueError("factor names must be non-empty strings")
    value = scoring[factor_name]
    if isinstance(value, bool):
        raise ValueError("factor weights must be numeric")
    try:
        weight = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("factor weights must be numeric") from exc
    if not math.isfinite(weight) or weight <= 0.0:
        raise ValueError("factor weights must be finite and positive")
    direction = directions[factor_name]
    if direction not in {"high", "low"}:
        raise ValueError("factor directions must be high or low")
    return f"{weight:g}", "1" if direction == "high" else "-1"


def compute_candidate_signature(definition: Mapping[str, Any]) -> str:
    """Return the canonical artifact signature for a mining candidate definition."""
    kind = definition.get("kind")
    if kind == "existing_strategy":
        strategy_id = definition.get("strategy_id")
        if not isinstance(strategy_id, str) or not strategy_id:
            raise ValueError("existing strategy candidate requires strategy_id")
        return f"strategy:{strategy_id}"
    if kind != "factor_rank":
        raise ValueError(f"unsupported mining candidate kind: {kind!r}")

    factor_names = definition.get("factor_names")
    scoring = definition.get("scoring")
    directions = definition.get("directions")
    if not isinstance(factor_names, list) or not factor_names:
        raise ValueError("factor candidate requires factor_names")
    if not isinstance(scoring, Mapping) or set(scoring) != set(factor_names):
        raise ValueError("factor candidate scoring keys must match factor_names")
    if not isinstance(directions, Mapping) or set(directions) != set(factor_names):
        raise ValueError("factor candidate direction keys must match factor_names")

    weights: list[str] = []
    direction_values: list[str] = []
    for factor_name in factor_names:
        weight_repr, direction_repr = _validate_signature_factor(factor_name, scoring, directions)
        weights.append(weight_repr)
        direction_values.append(direction_repr)
    return f"factor:{','.join(factor_names)}|w:{','.join(weights)}|d:{','.join(direction_values)}"


def _factor_candidate(
    factors: tuple[str, ...],
    weights: tuple[float, ...],
    directions: tuple[int, ...],
    rank_ic: float,
    proxy_ir: float,
    observations: int,
    dates: int,
) -> MiningCandidate:
    candidate_id = compute_candidate_signature(
        {
            "kind": "factor_rank",
            "factor_names": list(factors),
            "scoring": dict(zip(factors, weights, strict=True)),
            "directions": {
                factor_id: "high" if direction > 0 else "low"
                for factor_id, direction in zip(factors, directions, strict=True)
            },
        }
    )
    return MiningCandidate(
        candidate_id=candidate_id,
        kind="factor_rank",
        factor_names=factors,
        weights=weights,
        directions=directions,
        proxy_rank_ic=round(float(rank_ic), 8),
        proxy_ir=round(float(proxy_ir), 8),
        observations=observations,
        dates=dates,
    )


def benchmark_candidate(strategy_id: str) -> MiningCandidate:
    """Build the fixed benchmark candidate for one user-selected strategy."""
    return MiningCandidate(
        candidate_id=_benchmark_signature(strategy_id),
        kind="existing_strategy",
        strategy_id=strategy_id,
    )


def _benchmark_signature(strategy_id: str) -> str:
    return compute_candidate_signature(
        {
            "kind": "existing_strategy",
            "strategy_id": strategy_id,
        }
    )


def _searchable_factors(
    selected: Sequence[str],
    budget: MiningBudget,
) -> tuple[str, ...]:
    """Cap beam-search inputs so singletons plus combinations fit the phase budget.

    ``selected`` arrives in training-fold metric order, so the cap keeps the
    strongest factors and keeps the lexicographic singleton pass from consuming
    the whole proxy allowance before any combination is scored.
    """
    return tuple(selected[: max(1, budget.beam_width)])


def _candidate_refit_key(candidate: MiningCandidate) -> tuple[Any, ...]:
    if candidate.kind == "existing_strategy":
        return (candidate.kind, candidate.strategy_id)
    return (candidate.kind, candidate.factor_names, candidate.weights)


def _rank_candidates(candidates: Sequence[MiningCandidate]) -> list[MiningCandidate]:
    return sorted(
        candidates,
        key=lambda candidate: (
            -candidate.proxy_rank_ic,
            -candidate.proxy_ir,
            candidate.candidate_id,
        ),
    )


def _make_validation_fold(
    labels: tuple[str, ...],
    *,
    level: Literal["outer", "inner"],
    outer_index: int,
    inner_index: int | None,
    train_start: int,
    train_bars: int,
    test_bars: int,
    purge_bars: int,
    embargo_bars: int,
    hard_stop: int | None = None,
) -> ValidationFold:
    train_stop = train_start + train_bars
    test_start = train_stop + purge_bars
    test_stop = test_start + test_bars
    embargo_stop = test_stop + embargo_bars
    if hard_stop is not None:
        embargo_stop = min(embargo_stop, hard_stop)
    return ValidationFold(
        level=level,
        outer_index=outer_index,
        inner_index=inner_index,
        train_labels=labels[train_start:train_stop],
        purge_labels=labels[train_stop:test_start],
        test_labels=labels[test_start:test_stop],
        embargo_labels=labels[test_stop:embargo_stop],
    )


def _train_panel_for_fold(
    panel: pl.DataFrame,
    date_column: str,
    fold: ValidationFold,
) -> pl.DataFrame:
    train = _panel_for_labels(panel, date_column, fold.train_labels)
    if "_target_date" not in train.columns:
        return train
    target_date = pl.col("_target_date").cast(pl.Utf8).str.slice(0, 10)
    daily = train.select(date_column, "_target_date").unique(
        subset=[date_column],
        maintain_order=True,
    )
    allowed = (
        daily.select((target_date.is_null() | (target_date <= fold.train_end)).alias("_allowed"))
        .get_column("_allowed")
        .to_list()
    )
    first_excluded = next(
        (index for index, value in enumerate(allowed) if not value),
        len(allowed),
    )
    if not any(allowed[first_excluded:]):
        return _panel_for_labels(
            train,
            date_column,
            fold.train_labels[:first_excluded],
        )
    return train.filter(target_date.is_null() | (target_date <= fold.train_end))


def _panel_for_labels(
    panel: pl.DataFrame,
    date_column: str,
    labels: Sequence[str],
) -> pl.DataFrame:
    normalized = tuple(str(label)[:10] for label in labels)
    if not normalized:
        return panel.slice(0, 0)
    values = panel.get_column(date_column)
    if values.is_sorted():
        bounds: tuple[Any, Any] | None = None
        if values.dtype == pl.Date:
            bounds = (
                date.fromisoformat(normalized[0]),
                date.fromisoformat(normalized[-1]),
            )
        elif values.dtype == pl.Utf8:
            bounds = (normalized[0], normalized[-1])
        if bounds is not None:
            start = int(values.search_sorted(bounds[0], side="left"))
            stop = int(values.search_sorted(bounds[1], side="right"))
            sliced = panel.slice(start, stop - start)
            actual = tuple(
                str(value)[:10]
                for value in sliced.get_column(date_column).unique(maintain_order=True)
            )
            if actual == normalized:
                return sliced
    date_expr = pl.col(date_column).cast(pl.Utf8).str.slice(0, 10)
    return panel.filter(date_expr.is_in(normalized))


def _cancelled(cancel_check: CancelCheck | None) -> bool:
    if cancel_check is None:
        return False
    if callable(cancel_check):
        return bool(cancel_check())
    is_set = getattr(cancel_check, "is_set", None)
    return bool(is_set()) if callable(is_set) else False


def _evaluate_labels(
    evaluator: Callable[
        [Sequence[str], Sequence[str], Mapping[str, Any]],
        CandidateEvaluation | Mapping[str, Any] | float,
    ],
    train_labels: Sequence[str],
    test_labels: Sequence[str],
    candidate: MiningCandidate,
) -> CandidateEvaluation:
    try:
        raw = evaluator(train_labels, test_labels, candidate.definition())
    except Exception as exc:  # noqa: BLE001 - 外部策略评分不可控, 捕获后记录为候选评估失败
        return CandidateEvaluation(score=None, error=str(exc))
    return _normalize_evaluation(raw)


def _evaluate(
    evaluator: CandidateEvaluator,
    train: pl.DataFrame,
    test: pl.DataFrame,
    candidate: MiningCandidate,
) -> CandidateEvaluation:
    try:
        raw = evaluator.evaluate_candidate(train, test, candidate.definition())
    except Exception as exc:  # noqa: BLE001 - 外部策略评分不可控, 捕获后记录为候选评估失败
        return CandidateEvaluation(score=None, error=str(exc))
    return _normalize_evaluation(raw)


def _normalize_evaluation(
    raw: CandidateEvaluation | Mapping[str, Any] | float,
) -> CandidateEvaluation:
    if isinstance(raw, CandidateEvaluation):
        return CandidateEvaluation(
            score=_finite_score(raw.score),
            metrics=raw.metrics,
            error=raw.error,
        )
    if isinstance(raw, Mapping):
        score = raw.get("score")
        error = raw.get("error")
        metrics = raw.get("metrics", {})
        return CandidateEvaluation(
            score=_finite_score(score),
            metrics=dict(metrics) if isinstance(metrics, Mapping) else {},
            error=str(error) if error is not None else None,
        )
    return CandidateEvaluation(score=_finite_score(raw))


def _finite_score(value: Any) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return score if math.isfinite(score) else None
