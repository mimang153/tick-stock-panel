"""Internal split module for app.backtest.mining_runtime: _refine."""

from __future__ import annotations

import itertools
import json
import math
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import polars as pl

from app.backtest.mining import MiningCandidate, MiningResult, compute_candidate_signature
from app.backtest.robust_optimizer import robust_score

from ._common import (
    _REFINE_DEGRADATION_LIMIT,
    _REFINE_MAX_TRIALS,
    _REFINE_MAX_VARIANTS,
    _REFINE_TOP_N,
    _WEIGHT_RUNGS,
    CancelCheck,
    MiningRuntimeCancelledError,
    RuntimeRequest,
)

if TYPE_CHECKING:
    from ._panel import MatcherCandidateEvaluator


def _prepare_refine_weights(
    ranked_candidates: Sequence[MiningCandidate],
    request: RuntimeRequest,
    result: MiningResult,
    nested: Sequence[Any],
) -> (
    tuple[
        MiningCandidate,
        list[str],
        tuple[float, ...],
        list[tuple[float, ...]],
        list[tuple[str, ...]],
    ]
    | None
):
    champions = [
        candidate
        for candidate in ranked_candidates[:_REFINE_TOP_N]
        if candidate.kind == "factor_rank" and len(candidate.factor_names) >= 2
    ]
    if not champions:
        return None
    champion = champions[0]
    factors = list(champion.factor_names)
    original_weights = [float(weight) for weight in champion.weights]
    base = original_weights[0]
    if base <= 0.0:
        return None
    original_ratios = tuple(round(weight / base, 6) for weight in original_weights)

    variants: list[tuple[float, ...]] = []
    seen = {original_ratios}
    for combo in itertools.product(_WEIGHT_RUNGS, repeat=len(factors) - 1):
        ratios = (1.0, *combo)
        if ratios in seen:
            continue
        seen.add(ratios)
        variants.append(ratios)
    # 原权重(归一)始终作为对照基准参与评估
    all_variants = [original_ratios, *variants[: _REFINE_MAX_VARIANTS - 1]]

    test_label_sets = [fold.outer.test_labels for fold in nested]
    n_trials = len(all_variants) * len(test_label_sets) + 2  # +2 = stage2 全窗两次
    # 预算纪律 (设计 §4.2): 精调只花本 run 搜索阶段剩下的真实回测额度,
    # 不另开上限 — 搜索已耗尽预算 (balanced 96/strict 256) 时 4 因子冠军自然跳过。
    remaining = min(
        _REFINE_MAX_TRIALS,
        max(0, request.mining_request.budget.max_trials - result.trials_used),
    )
    if n_trials > remaining:
        return None
    return champion, factors, original_ratios, all_variants, test_label_sets


def _evaluate_refine_variants(
    panel: pl.DataFrame,
    champion: MiningCandidate,
    all_variants: Sequence[tuple[float, ...]],
    test_label_sets: Sequence[tuple[str, ...]],
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
) -> dict[tuple[float, ...], list[dict[str, Any] | None]]:
    fold_results: dict[tuple[float, ...], list[dict[str, Any] | None]] = {}
    for ratios in all_variants:
        _raise_if_cancelled(cancel_check)
        definition = _weighted_definition(champion, ratios)
        per_fold: list[dict[str, Any] | None] = []
        for labels in test_label_sets:
            evaluation = evaluator.evaluate_test(_panel_for_dates(panel, labels), definition)
            per_fold.append(
                None
                if evaluation.error is not None
                else {
                    key: _finite_or_none(evaluation.metrics.get(key))
                    if key != "n_trades"
                    else evaluation.metrics.get("n_trades")
                    for key in ("total_return", "sharpe", "max_drawdown", "n_trades")
                }
            )
        fold_results[ratios] = per_fold
    return fold_results


def _variant_robust_score(
    fold_results: dict[tuple[float, ...], list[dict[str, Any] | None]],
    ratios: tuple[float, ...],
) -> float:
    scores = [fold["sharpe"] if fold is not None else None for fold in fold_results[ratios]]
    value = robust_score(scores)["score"]
    return float(value)


def _refine_full_window_summary(
    panel: pl.DataFrame,
    champion: MiningCandidate,
    factors: list[str],
    original_ratios: tuple[float, ...],
    best_ratios: tuple[float, ...],
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
    fold_results: dict[tuple[float, ...], list[dict[str, Any] | None]],
) -> tuple[dict[str, Any], bool]:
    # stage2: 全窗口复验退化 (冠军变体 vs 原权重)
    _raise_if_cancelled(cancel_check)
    full_labels = _date_labels(panel)
    refined_full = evaluator.evaluate_test(
        _panel_for_dates(panel, full_labels),
        _weighted_definition(champion, best_ratios),
    )
    original_full = evaluator.evaluate_test(
        _panel_for_dates(panel, full_labels), champion.definition()
    )
    refined_full_sharpe = (
        _finite_or_none(refined_full.metrics.get("sharpe")) if refined_full.error is None else None
    )
    original_full_sharpe = (
        _finite_or_none(original_full.metrics.get("sharpe"))
        if original_full.error is None
        else None
    )
    summary: dict[str, Any] = {
        "candidate_signature": champion.candidate_id,
        "factor_names": factors,
        "original_weights": original_ratios,
        "refined_weights": best_ratios,
        "robust_before": _variant_robust_score(fold_results, original_ratios),
        "robust_after": _variant_robust_score(fold_results, best_ratios),
        "full_window_sharpe_before": original_full_sharpe,
        "full_window_sharpe_after": refined_full_sharpe,
    }
    if refined_full_sharpe is None or (
        original_full_sharpe is not None
        and original_full_sharpe > 0
        and refined_full_sharpe < (1.0 - _REFINE_DEGRADATION_LIMIT) * original_full_sharpe
    ):
        summary["applied"] = False
        summary["reason"] = "全窗口复验退化超过 20%, 放弃精调权重"
        return summary, False
    if refined_full_sharpe is None:
        summary["applied"] = False
        summary["reason"] = "精调变体全窗口回测失败"
        return summary, False
    return summary, True


def _refine_weight_row(
    request: RuntimeRequest,
    champion: MiningCandidate,
    factors: list[str],
    fold_results: dict[tuple[float, ...], list[dict[str, Any] | None]],
    best_ratios: tuple[float, ...],
    refined_definition: dict[str, Any],
    refined_signature: str,
) -> dict[str, Any]:
    folds = fold_results[best_ratios]
    successful = [fold for fold in folds if fold is not None]
    returns = [fold["total_return"] for fold in successful if fold["total_return"] is not None]
    sharpes = [fold["sharpe"] for fold in successful if fold["sharpe"] is not None]
    drawdowns = [fold["max_drawdown"] for fold in successful if fold["max_drawdown"] is not None]
    trades = [fold["n_trades"] for fold in successful if fold["n_trades"] is not None]
    return {
        "signature": refined_signature,
        "name": _candidate_name(champion) + " · 权重精调",
        "kind": "factor_combination",
        "factor_names_json": json.dumps(factors, ensure_ascii=False, separators=(",", ":")),
        "strategy_id": None,
        "definition_json": json.dumps(
            refined_definition,
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
        "skipped_folds": len(folds) - len(successful),
        "promoted_candidate_id": None,
        "published_strategy_id": None,
    }


def _refine_champion_weights(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    ranked_candidates: Sequence[MiningCandidate],
    evaluator: MatcherCandidateEvaluator,
    nested: Sequence[Any],
    cancel_check: CancelCheck | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """P2b: 对最终冠军组合做权重比率精调, 产出额外的"权重精调"候选行。

    两段式: stage1 全部变体 × 各外层测试窗 (复用 run 超集矩阵) 按稳健分选优;
    stage2 冠军变体与原权重全窗口复验, 退化超限则放弃 (宁缺毋滥)。
    只产出新行, 不改写原始候选 — 原始结构仍在 folds 工件中留档。
    """
    prepared = _prepare_refine_weights(ranked_candidates, request, result, nested)
    if prepared is None:
        return [], None
    champion, factors, original_ratios, all_variants, test_label_sets = prepared

    fold_results = _evaluate_refine_variants(
        panel, champion, all_variants, test_label_sets, evaluator, cancel_check
    )
    best_ratios = max(all_variants, key=lambda ratios: _variant_robust_score(fold_results, ratios))
    if best_ratios == original_ratios:
        return [], None

    summary, accepted = _refine_full_window_summary(
        panel,
        champion,
        factors,
        original_ratios,
        best_ratios,
        evaluator,
        cancel_check,
        fold_results,
    )
    if not accepted:
        return [], summary

    refined_definition = _weighted_definition(champion, best_ratios)
    refined_signature = compute_candidate_signature(refined_definition)
    if any(candidate.candidate_id == refined_signature for candidate in ranked_candidates):
        summary["applied"] = False
        summary["reason"] = "精调结果与既有候选重复"
        return [], summary

    row = _refine_weight_row(
        request,
        champion,
        factors,
        fold_results,
        best_ratios,
        refined_definition,
        refined_signature,
    )
    summary["applied"] = True
    return [row], summary


def _weighted_definition(candidate: MiningCandidate, ratios: tuple[float, ...]) -> dict[str, Any]:
    definition = candidate.definition()
    scoring = {
        factor: float(ratio) for factor, ratio in zip(candidate.factor_names, ratios, strict=True)
    }
    return {**definition, "scoring": scoring}


def _rank_artifact_candidates(
    candidates: Sequence[MiningCandidate],
    overall_rows: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> list[MiningCandidate]:
    return sorted(
        candidates,
        key=lambda candidate: _candidate_artifact_rank(candidate, overall_rows),
    )[:limit]


def _candidate_artifact_rank(
    candidate: MiningCandidate,
    overall_rows: Sequence[Mapping[str, Any]],
) -> tuple[float, str]:
    # mining-v3: 冠军按稳健分 (0.6×median + 0.4×worst) 排名, 单折运气不再能登顶;
    # 均值只保留在 oos_sharpe 列作绝对门槛输入 (gate 阈值口径不变)。
    sharpes = [
        row.get("sharpe")
        for row in overall_rows
        if row.get("candidate_signature") == candidate.candidate_id
        and not row.get("skipped")
        and row.get("sharpe") is not None
    ]
    score = _robust_or_none(cast(Sequence[float], sharpes))
    return (
        -(score if score is not None else float("-inf")),
        candidate.candidate_id,
    )


def _robust_or_none(values: Sequence[float]) -> float | None:
    if not values:
        return None
    score = robust_score(list(values))["score"]
    return float(score) if score == score and abs(score) != float("inf") else None


def _candidate_name(candidate: MiningCandidate) -> str:
    if candidate.kind == "existing_strategy":
        return f"已有策略 · {candidate.strategy_id}"
    return "因子组合 · " + " + ".join(candidate.factor_names)


def _empty_candidates_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "signature": pl.Utf8,
            "name": pl.Utf8,
            "kind": pl.Utf8,
            "factor_names_json": pl.Utf8,
            "strategy_id": pl.Utf8,
            "definition_json": pl.Utf8,
            "regime_state": pl.Utf8,
            "score": pl.Float64,
            "oos_return": pl.Float64,
            "oos_sharpe": pl.Float64,
            "oos_max_drawdown": pl.Float64,
            "oos_positive_fold_ratio": pl.Float64,
            "oos_n_trades": pl.Int64,
            "confidence": pl.Utf8,
            "valid_folds": pl.Int64,
            "skipped_folds": pl.Int64,
            "promoted_candidate_id": pl.Utf8,
            "published_strategy_id": pl.Utf8,
        }
    )


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def _panel_for_dates(panel: pl.DataFrame, labels: Sequence[str]) -> pl.DataFrame:
    return panel.filter(pl.col("date").cast(pl.Utf8).str.slice(0, 10).is_in(list(labels)))


def _date_labels(panel: pl.DataFrame) -> tuple[str, ...]:
    if panel.is_empty():
        return ()
    return tuple(
        panel.select(pl.col("date").cast(pl.Utf8).str.slice(0, 10).unique().sort())
        .to_series()
        .to_list()
    )


def _raise_if_cancelled(cancel_check: CancelCheck | None) -> None:
    if cancel_check is None:
        return
    cancelled = cancel_check() if callable(cancel_check) else cancel_check.is_set()
    if cancelled:
        raise MiningRuntimeCancelledError("mining cancelled")


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"invalid ISO date: {value!r}") from exc


def _bounded_float(
    value: Any,
    name: str,
    minimum: float,
    maximum: float,
    *,
    exclusive_min: bool = False,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    valid_min = number > minimum if exclusive_min else number >= minimum
    if not math.isfinite(number) or not valid_min or number > maximum:
        left = "(" if exclusive_min else "["
        raise ValueError(f"{name} must be in {left}{minimum}, {maximum}]")
    return number


def _finite_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _int_or_none(value: Any) -> int | None:
    number = _finite_or_none(value)
    return int(number) if number is not None else None


def _mean_or_none(values: Sequence[float]) -> float | None:
    return float(np.mean(values)) if values else None


def _confidence(profile: str) -> str:
    return {"exploratory": "low", "balanced": "standard", "strict": "high"}[profile]


def _budget_exhausted(result: MiningResult) -> bool:
    if result.proxy_trials_used >= result.request.budget.max_proxy_trials:
        return True
    if result.trials_used >= result.request.budget.max_trials:
        return True
    return any("budget exhausted" in (fold.error or "") for fold in result.folds)
