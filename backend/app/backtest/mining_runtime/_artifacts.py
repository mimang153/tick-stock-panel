"""Internal split module for app.backtest.mining_runtime: _artifacts."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from app.backtest.mining import (
    CandidateEvaluation,
    FactorMetric,
    MiningCandidate,
    MiningResult,
    benchmark_candidate,
    compute_rank_correlation,
    generate_nested_folds,
)
from app.backtest.strategy import StrategyBacktestService
from app.factors.registry import factor_columns_view

from ._common import _REGIME_FILTERS, CancelCheck, RuntimeRequest, TrainingMetricProvider

if TYPE_CHECKING:
    from ._panel import MatcherCandidateEvaluator

from ._refine import (
    _candidate_name,
    _confidence,
    _date_labels,
    _empty_candidates_frame,
    _finite_or_none,
    _int_or_none,
    _mean_or_none,
    _panel_for_dates,
    _raise_if_cancelled,
    _rank_artifact_candidates,
    _refine_champion_weights,
    _robust_or_none,
)


def _build_factor_artifacts(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    nested: Sequence[Any],
    metric_provider: TrainingMetricProvider,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    last_train = _panel_for_dates(panel, nested[-1].outer.train_labels)
    if "_target_date" in last_train.columns:
        last_train = last_train.filter(
            pl.col("_target_date").is_not_null()
            & (pl.col("_target_date") <= date.fromisoformat(nested[-1].outer.train_end))
        )
    latest_metrics = {
        metric.factor_id: metric for metric in metric_provider(last_train, request.factor_names)
    }
    selected_factors = {
        factor_name for fold in result.folds for factor_name in fold.selected_factors
    }
    direction_by_factor: dict[str, int] = {}
    for fold in result.folds:
        for candidate in fold.candidates:
            for factor_name, direction in zip(
                candidate.factor_names, candidate.directions, strict=True
            ):
                direction_by_factor.setdefault(factor_name, int(direction))
    metadata = {str(item["id"]): item for item in factor_columns_view()}
    factor_rows = []
    for factor_name in request.factor_names:
        metric = latest_metrics[factor_name]
        factor_rows.append(
            {
                "factor_name": factor_name,
                "label": str(metadata.get(factor_name, {}).get("label", factor_name)),
                "direction": direction_by_factor.get(factor_name, 1 if metric.rank_ic >= 0 else -1),
                "score": _finite_or_none(metric.composite_score),
                "ic_mean": _finite_or_none(metric.rank_ic),
                "ir": _finite_or_none(metric.ir),
                "coverage": _finite_or_none(metric.coverage),
                "turnover": _finite_or_none(metric.turnover),
                "spread_return": None,
                "spread_sharpe": None,
                "selected": factor_name in selected_factors,
                "excluded_reason": None if factor_name in selected_factors else "not_selected",
            }
        )
    factors = pl.DataFrame(factor_rows)

    correlation = compute_rank_correlation(last_train, request.factor_names)
    correlation_rows = []
    for row_id, left in enumerate(correlation.factor_names):
        for column_id, right in enumerate(correlation.factor_names):
            count = int(correlation.pair_counts[row_id][column_id])
            correlation_rows.append(
                {
                    "factor_x": left,
                    "factor_y": right,
                    "rho": (float(correlation.matrix[row_id][column_id]) if count > 0 else None),
                    "pair_count": count,
                }
            )
    correlation_frame = pl.DataFrame(correlation_rows)
    return factors, correlation_frame


def _collect_artifact_candidates(
    request: RuntimeRequest,
    result: MiningResult,
) -> tuple[dict[str, MiningCandidate], dict[str, MiningCandidate]]:
    benchmark_by_id = {
        benchmark_candidate(strategy_id).candidate_id: benchmark_candidate(strategy_id)
        for strategy_id in request.strategy_ids
    }
    candidate_by_id: dict[str, MiningCandidate] = {}
    for fold in result.folds:
        for candidate in fold.candidates:
            candidate_by_id.setdefault(candidate.candidate_id, candidate)
    return benchmark_by_id, candidate_by_id


def _build_regime_fold_rows(
    panel: pl.DataFrame,
    outer_index: int,
    nested_fold: Any,
    selected: MiningCandidate,
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    test = _panel_for_dates(panel, nested_fold.outer.test_labels)
    for state in ("strong", "range", "weak"):
        _raise_if_cancelled(cancel_check)
        regime_evaluation = evaluator.evaluate_test(
            test,
            selected.definition(),
            regime_state=state,
        )
        n_dates = _regime_date_count(
            panel,
            nested_fold.outer,
            state,
            evaluator.data_dir,
        )
        rows.append(
            _fold_row(
                outer_index,
                selected,
                nested_fold.outer,
                regime_evaluation,
                regime_state=state,
                n_dates=n_dates,
                reason=None,
            )
        )
    return rows


def _selected_artifact_candidate(
    fold: Any,
    candidate_by_id: dict[str, MiningCandidate],
) -> MiningCandidate | None:
    if fold.selected_candidate_id is None:
        return None
    return candidate_by_id.get(fold.selected_candidate_id)


def _build_fold_artifacts(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    nested: Sequence[Any],
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
) -> tuple[
    pl.DataFrame,
    list[dict[str, Any]],
    dict[str, MiningCandidate],
    dict[str, MiningCandidate],
]:
    benchmark_by_id, candidate_by_id = _collect_artifact_candidates(request, result)
    fold_rows: list[dict[str, Any]] = []
    for fold, nested_fold in zip(result.folds, nested, strict=False):
        selected = _selected_artifact_candidate(fold, candidate_by_id)
        evaluation = fold.outer_evaluation
        fold_rows.append(
            _fold_row(
                fold.outer_index,
                selected,
                nested_fold.outer,
                evaluation,
                regime_state="overall",
                n_dates=len(nested_fold.outer.test_labels),
                reason=fold.error,
            )
        )
        for candidate_id, cross_evaluation in fold.cross_evaluations:
            fold_rows.append(
                _fold_row(
                    fold.outer_index,
                    candidate_by_id.get(candidate_id),
                    nested_fold.outer,
                    cross_evaluation,
                    regime_state="overall",
                    n_dates=len(nested_fold.outer.test_labels),
                    reason=None,
                    evaluation_kind="cross",
                )
            )
        for candidate_id, benchmark_evaluation in fold.benchmark_evaluations:
            fold_rows.append(
                _fold_row(
                    fold.outer_index,
                    benchmark_by_id.get(candidate_id),
                    nested_fold.outer,
                    benchmark_evaluation,
                    regime_state="overall",
                    n_dates=len(nested_fold.outer.test_labels),
                    reason=None,
                    evaluation_kind="benchmark",
                )
            )
        if (
            selected is None
            or evaluation is None
            or evaluation.error is not None
            or not request.require_regime
        ):
            continue
        fold_rows.extend(
            _build_regime_fold_rows(
                panel, fold.outer_index, nested_fold, selected, evaluator, cancel_check
            )
        )
    folds = pl.DataFrame(
        fold_rows,
        schema_overrides={
            "total_return": pl.Float64,
            "sharpe": pl.Float64,
            "max_drawdown": pl.Float64,
            "n_trades": pl.Int64,
        },
    )
    return folds, fold_rows, candidate_by_id, benchmark_by_id


def _ranked_build_candidates(
    result: MiningResult,
    candidate_by_id: dict[str, MiningCandidate],
    benchmark_by_id: dict[str, MiningCandidate],
    overall_rows: list[dict[str, Any]],
    request: RuntimeRequest,
) -> list[MiningCandidate]:
    winner_by_id = {
        fold.selected_candidate_id: candidate_by_id[fold.selected_candidate_id]
        for fold in result.folds
        if fold.selected_candidate_id is not None and fold.selected_candidate_id in candidate_by_id
    }
    return _rank_artifact_candidates(
        list(winner_by_id.values()),
        overall_rows,
        limit=request.max_finalists,
    ) + [benchmark_by_id[cid] for cid in sorted(benchmark_by_id)]


def _build_candidate_row(
    candidate: MiningCandidate,
    overall_rows: list[dict[str, Any]],
    request: RuntimeRequest,
) -> dict[str, Any]:
    rows = [row for row in overall_rows if row["candidate_signature"] == candidate.candidate_id]
    successful = [row for row in rows if not row["skipped"]]
    returns = [row["total_return"] for row in successful if row["total_return"] is not None]
    sharpes = [row["sharpe"] for row in successful if row["sharpe"] is not None]
    drawdowns = [row["max_drawdown"] for row in successful if row["max_drawdown"] is not None]
    trades = [row["n_trades"] for row in successful if row["n_trades"] is not None]
    definition = candidate.definition()
    return {
        "signature": candidate.candidate_id,
        "name": _candidate_name(candidate),
        "kind": (
            "existing_strategy" if candidate.kind == "existing_strategy" else "factor_combination"
        ),
        "factor_names_json": json.dumps(
            list(candidate.factor_names), ensure_ascii=False, separators=(",", ":")
        ),
        "strategy_id": candidate.strategy_id,
        "definition_json": json.dumps(
            definition,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "regime_state": "overall",
        "score": _robust_or_none(sharpes),
        "oos_return": _mean_or_none(returns),
        "oos_sharpe": _mean_or_none(sharpes),
        "oos_worst_sharpe": min(sharpes) if sharpes else None,
        "oos_max_drawdown": min(drawdowns) if drawdowns else None,
        "oos_positive_fold_ratio": (
            sum(value > 0 for value in returns) / len(returns) if returns else None
        ),
        "oos_n_trades": sum(int(value) for value in trades) if trades else None,
        "confidence": _confidence(request.profile),
        "valid_folds": len(successful),
        "skipped_folds": len(rows) - len(successful),
        "promoted_candidate_id": None,
        "published_strategy_id": None,
    }


def _build_candidate_rows(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    fold_rows: list[dict[str, Any]],
    candidate_by_id: dict[str, MiningCandidate],
    benchmark_by_id: dict[str, MiningCandidate],
    nested: Sequence[Any],
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    candidate_rows: list[dict[str, Any]] = []
    overall_rows = [row for row in fold_rows if row["regime_state"] == "overall"]
    ranked_candidates = _ranked_build_candidates(
        result, candidate_by_id, benchmark_by_id, overall_rows, request
    )
    refined_rows, refinement_summary = _refine_champion_weights(
        panel,
        request,
        result,
        ranked_candidates,
        evaluator,
        nested,
        cancel_check,
    )
    refined_after: dict[str, list[dict[str, Any]]] = {}
    if refined_rows and refinement_summary is not None:
        refined_after[refinement_summary["candidate_signature"]] = refined_rows
    for candidate in ranked_candidates:
        candidate_rows.append(_build_candidate_row(candidate, overall_rows, request))
        # P2b: 原冠军行落盘后紧随其精调变体 (若产出); 冠军取自 ranked[:1], 必在循环内
        candidate_rows.extend(refined_after.pop(candidate.candidate_id, []))
    return candidate_rows, refinement_summary


def _build_artifacts(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    metric_provider: TrainingMetricProvider,
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
) -> tuple[dict[str, pl.DataFrame], dict[str, Any] | None]:
    nested = generate_nested_folds(_date_labels(panel), request.mining_request.validation)
    factors, correlation_frame = _build_factor_artifacts(
        panel, request, result, nested, metric_provider
    )
    folds, fold_rows, candidate_by_id, benchmark_by_id = _build_fold_artifacts(
        panel, request, result, nested, evaluator, cancel_check
    )
    candidate_rows, refinement_summary = _build_candidate_rows(
        panel,
        request,
        result,
        fold_rows,
        candidate_by_id,
        benchmark_by_id,
        nested,
        evaluator,
        cancel_check,
    )
    candidates = (
        pl.DataFrame(
            candidate_rows,
            schema_overrides={
                "score": pl.Float64,
                "oos_return": pl.Float64,
                "oos_sharpe": pl.Float64,
                "oos_worst_sharpe": pl.Float64,
                "oos_max_drawdown": pl.Float64,
                "oos_positive_fold_ratio": pl.Float64,
                "oos_n_trades": pl.Int64,
                "strategy_id": pl.Utf8,
                "promoted_candidate_id": pl.Utf8,
                "published_strategy_id": pl.Utf8,
            },
        )
        if candidate_rows
        else _empty_candidates_frame()
    )
    return {
        "factors": factors,
        "correlation": correlation_frame,
        "candidates": candidates,
        "folds": folds,
    }, refinement_summary


def _factor_metric(
    train: pl.DataFrame,
    factor_name: str,
    target_column: str,
) -> FactorMetric:
    scoped = train.select("date", "symbol", factor_name, target_column)
    finite_factor = pl.col(factor_name).is_not_null() & pl.col(factor_name).is_finite()
    eligible = scoped.filter(
        finite_factor & pl.col(target_column).is_not_null() & pl.col(target_column).is_finite()
    )
    coverage = scoped.select(finite_factor.mean()).item() if scoped.height else 0.0
    daily = (
        eligible.group_by("date")
        .agg(
            pl.corr(
                pl.col(factor_name).rank(method="average"),
                pl.col(target_column).rank(method="average"),
            ).alias("ic")
        )
        .filter(pl.col("ic").is_not_null() & pl.col("ic").is_finite())
        .sort("date")
    )
    values = daily["ic"].to_numpy() if not daily.is_empty() else np.array([])
    mean = float(np.mean(values)) if values.size else 0.0
    std = float(np.std(values)) if values.size else 0.0
    ir = mean / std if std > 1e-12 else 0.0
    turnover = _top_quintile_turnover(eligible, factor_name, direction=1 if mean >= 0 else -1)
    score = abs(ir) * float(coverage or 0.0) / (1.0 + turnover)
    return FactorMetric(
        factor_id=factor_name,
        composite_score=round(score, 8),
        ir=round(abs(ir), 8),
        coverage=round(float(coverage or 0.0), 8),
        turnover=round(turnover, 8),
        rank_ic=round(mean, 8),
    )


def _top_quintile_turnover(
    panel: pl.DataFrame,
    factor_name: str,
    *,
    direction: int,
) -> float:
    if panel.is_empty():
        return 1.0
    ranked = (
        panel.select("date", "symbol", factor_name)
        .sort(["date", factor_name, "symbol"], descending=[False, direction < 0, False])
        .with_columns(
            pl.col(factor_name).rank(method="average").over("date").alias("_rank"),
            pl.len().over("date").alias("_count"),
        )
        .filter(pl.col("_rank") > pl.col("_count") * (0.8 if direction > 0 else 0.0))
    )
    if direction < 0:
        ranked = ranked.filter(pl.col("_rank") <= pl.col("_count") * 0.2)
    holdings = [
        set(str(value) for value in daily["symbol"].to_list())
        for daily in ranked.partition_by("date", maintain_order=True)
    ]
    if len(holdings) < 2:
        return 0.0
    values = []
    for previous, current in pairwise(holdings):
        denominator = max(len(previous), len(current), 1)
        values.append(1.0 - len(previous & current) / denominator)
    return float(np.mean(values)) if values else 0.0


def _fold_row(
    fold_index: int,
    candidate: MiningCandidate | None,
    validation_fold,
    evaluation: CandidateEvaluation | None,
    *,
    regime_state: str,
    n_dates: int,
    reason: str | None,
    evaluation_kind: str = "selected",
) -> dict[str, Any]:
    error = reason or (evaluation.error if evaluation is not None else None)
    metrics = evaluation.metrics if evaluation is not None else {}
    return {
        "candidate_signature": candidate.candidate_id if candidate is not None else None,
        "evaluation_kind": evaluation_kind,
        "fold": fold_index,
        "label": f"OOS {fold_index + 1}",
        "regime_state": regime_state,
        "n_dates": n_dates,
        "train_start": validation_fold.train_start,
        "train_end": validation_fold.train_end,
        "test_start": validation_fold.test_start,
        "test_end": validation_fold.test_end,
        "selected_factors_json": json.dumps(
            list(candidate.factor_names) if candidate is not None else [],
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "total_return": _finite_or_none(metrics.get("total_return")),
        "sharpe": _finite_or_none(metrics.get("sharpe")),
        "max_drawdown": _finite_or_none(metrics.get("max_drawdown")),
        "n_trades": _int_or_none(metrics.get("n_trades")),
        "skipped": evaluation is None or error is not None,
        "reason": error,
    }


def _validate_regime_availability(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    data_dir: Path,
) -> None:
    """搜索开始前 fail-fast 校验市场环境数据。

    环境分组评估 (strong/range/weak) 依赖 T-1 市场环境序列; 数据为空或覆盖不足时,
    若拖到 artifacts 阶段才在 _regime_date_count 中抛错, 整轮嵌套搜索的计算全部浪费。
    这里用与 artifacts 相同的 fold 口径 (panel 标签) 预先构建一次 mask:
    required 区间取所有外层 fold 测试窗的并集, 覆盖后续每个 fold 单独调用的要求,
    任何 ValueError (数据为空 / 覆盖不完整) 立即带指引消息终止运行。
    """
    labels = _date_labels(panel)
    nested = generate_nested_folds(labels, request.mining_request.validation)
    if not nested:
        return
    StrategyBacktestService._build_regime_mask(
        labels,
        _REGIME_FILTERS["strong"],
        data_dir,
        required_start=date.fromisoformat(nested[0].outer.test_start),
        required_end=date.fromisoformat(nested[-1].outer.test_end),
    )


def _regime_date_count(
    panel: pl.DataFrame,
    validation_fold,
    regime_state: str,
    data_dir: Path,
) -> int:
    labels = tuple(label for label in _date_labels(panel) if label <= validation_fold.test_end)
    mask = StrategyBacktestService._build_regime_mask(
        labels,
        _REGIME_FILTERS[regime_state],
        data_dir,
        required_start=date.fromisoformat(validation_fold.test_start),
        required_end=date.fromisoformat(validation_fold.test_end),
    )
    if mask is None:
        return 0
    return sum(
        bool(allowed)
        for label, allowed in zip(labels, mask, strict=True)
        if validation_fold.test_start <= label <= validation_fold.test_end
    )
