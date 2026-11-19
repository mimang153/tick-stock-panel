"""Internal split module for app.backtest.mining: _service."""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np
import polars as pl

import app.backtest.mining as _self

from ._candidates import (
    _benchmark_signature,
    _cancelled,
    _candidate_refit_key,
    _evaluate,
    _evaluate_labels,
    _panel_for_labels,
    _rank_candidates,
    _searchable_factors,
    _train_panel_for_fold,
    _validate_factor_names,
    benchmark_candidate,
)
from ._common import (
    MAX_FINALISTS,
    CancelCheck,
    CandidateEvaluation,
    CandidateEvaluator,
    FactorMetric,
    FactorMetricProvider,
    FoldMiningResult,
    MiningCandidate,
    MiningRequest,
    MiningResult,
)
from ._search import generate_nested_folds, prune_correlated_factors


@dataclass
class _MiningRunState:
    """一次 run 内跨 fold 传递的可变计数与结果 (仅结构性搬运原局部变量)。"""

    proxy_trials_used: int = 0
    trials_used: int = 0
    cancelled: bool = False
    selection_phases_remaining: int = 0
    evaluation_phases_remaining: int = 0
    fold_results: list[FoldMiningResult] = field(default_factory=list)
    labels_evaluator: Any | None = None


class MiningService:
    """Run leakage-bounded mining and delegate real backtests to a callback."""

    def _pruned_for_panel(self, train, request, factor_metrics, metric_provider):
        """对单个训练面板计算指标/相关性并剪枝 (原三处重复段落)。"""
        metrics = tuple(
            metric_provider(train, request.factor_names)
            if metric_provider is not None
            else factor_metrics or ()
        )
        correlation = _self.compute_rank_correlation(
            train,
            request.factor_names,
            date_column=request.date_column,
        )
        return prune_correlated_factors(
            metrics,
            correlation,
            request.correlation_threshold,
        )

    def _beam_for_fold(self, train, selected, request, max_trials, cancel_check):
        """在剪枝后的因子集合上跑一次 beam search (原三处重复段落)。"""
        return _self.beam_search_factor_combinations(
            train,
            _searchable_factors(selected, request.budget),
            target_column=request.target_column,
            date_column=request.date_column,
            max_combination_size=request.budget.max_combination_size,
            beam_width=request.budget.beam_width,
            max_trials=max_trials,
            cancel_check=cancel_check,
        )

    def _score_candidate(
        self,
        evaluator,
        labels_evaluator,
        candidate,
        *,
        panel_train=None,
        panel_test=None,
        train_labels=None,
        test_labels=None,
    ):
        """按 evaluator 形态选择 labels 或面板评估 (原分支段落)。"""
        if callable(labels_evaluator):
            return _evaluate_labels(labels_evaluator, train_labels, test_labels, candidate)
        return _evaluate(evaluator, panel_train, panel_test, candidate)

    def _run_benchmarks(self, nested_fold, panel, request, state, cancel_check, evaluator):
        """Score every user-selected strategy on one outer test window.

        Benchmarks document how existing strategies would have done on the
        same walk-forward windows, so they are evaluated even when the
        factor track fails on this fold and never join the winner race.
        """
        if evaluator is None:
            return ()
        benchmarks: list[tuple[str, CandidateEvaluation]] = []
        for strategy_id in request.existing_strategy_ids:
            if _cancelled(cancel_check):
                state.cancelled = True
                break
            if state.trials_used >= request.budget.max_trials:
                benchmarks.append(
                    (
                        _benchmark_signature(strategy_id),
                        CandidateEvaluation(
                            score=None,
                            error="real trial budget exhausted before benchmark evaluation",
                        ),
                    )
                )
                continue
            benchmark = benchmark_candidate(strategy_id)
            if callable(state.labels_evaluator):
                benchmark_evaluation = self._score_candidate(
                    evaluator,
                    state.labels_evaluator,
                    benchmark,
                    train_labels=nested_fold.outer.train_labels,
                    test_labels=nested_fold.outer.test_labels,
                )
            else:
                benchmark_evaluation = self._score_candidate(
                    evaluator,
                    state.labels_evaluator,
                    benchmark,
                    panel_train=_train_panel_for_fold(
                        panel, request.date_column, nested_fold.outer
                    ),
                    panel_test=_panel_for_labels(
                        panel, request.date_column, nested_fold.outer.test_labels
                    ),
                )
            state.trials_used += 1
            benchmarks.append((benchmark.candidate_id, benchmark_evaluation))
        return tuple(benchmarks)

    def _run_proxy_fold(
        self, panel, request, nested, state, factor_metrics, metric_provider, cancel_check
    ) -> bool:
        """evaluator 缺省时的纯代理分支; 返回是否应终止外层循环。"""
        outer_train = _train_panel_for_fold(panel, request.date_column, nested.outer)
        pruned = self._pruned_for_panel(outer_train, request, factor_metrics, metric_provider)
        proxy_remaining = request.budget.max_proxy_trials - state.proxy_trials_used
        if proxy_remaining <= 0:
            state.fold_results.append(
                FoldMiningResult(
                    outer_index=nested.outer.outer_index,
                    selected_factors=pruned.selected,
                    candidates=(),
                    selected_candidate_id=None,
                    inner_score=None,
                    outer_evaluation=None,
                    error="proxy trial budget exhausted",
                )
            )
            return True
        proxy_allowance = max(1, proxy_remaining // state.selection_phases_remaining)
        beam = self._beam_for_fold(
            outer_train, pruned.selected, request, proxy_allowance, cancel_check
        )
        state.proxy_trials_used += beam.trials_used
        state.selection_phases_remaining -= 1
        state.fold_results.append(
            FoldMiningResult(
                outer_index=nested.outer.outer_index,
                selected_factors=pruned.selected,
                candidates=tuple(beam.candidates),
                selected_candidate_id=None,
                inner_score=None,
                outer_evaluation=None,
            )
        )
        return False

    def _run_one_inner(
        self,
        panel,
        request,
        inner,
        state,
        factor_metrics,
        metric_provider,
        cancel_check,
        evaluator,
        all_candidates,
        selected_factor_union,
        winners,
    ) -> bool:
        """处理一个 inner fold; 返回是否应终止 inner 循环。"""
        inner_train = _train_panel_for_fold(panel, request.date_column, inner)
        inner_test = _panel_for_labels(panel, request.date_column, inner.test_labels)
        pruned = self._pruned_for_panel(inner_train, request, factor_metrics, metric_provider)
        selected_factor_union.update(pruned.selected)
        proxy_remaining = request.budget.max_proxy_trials - state.proxy_trials_used
        if proxy_remaining <= 0:
            return True
        proxy_allowance = max(1, proxy_remaining // state.selection_phases_remaining)
        beam = self._beam_for_fold(
            inner_train, pruned.selected, request, proxy_allowance, cancel_check
        )
        state.proxy_trials_used += beam.trials_used
        state.selection_phases_remaining -= 1
        for candidate in beam.candidates:
            all_candidates[candidate.candidate_id] = candidate

        real_remaining = request.budget.max_trials - state.trials_used
        if real_remaining <= 0:
            return True
        real_allowance = min(
            MAX_FINALISTS,
            max(1, real_remaining // state.evaluation_phases_remaining),
        )
        evaluated: list[tuple[float, MiningCandidate]] = []
        for candidate in beam.candidates[:real_allowance]:
            evaluation = self._score_candidate(
                evaluator,
                state.labels_evaluator,
                candidate,
                panel_train=inner_train,
                panel_test=inner_test,
                train_labels=inner.train_labels,
                test_labels=inner.test_labels,
            )
            state.trials_used += 1
            if evaluation.error is None and evaluation.score is not None:
                evaluated.append((evaluation.score, candidate))
        state.evaluation_phases_remaining -= 1
        if evaluated:
            winners.append(min(evaluated, key=lambda item: (-item[0], item[1].candidate_id)))
        return False

    def _run_inner_folds(
        self,
        panel,
        request,
        nested,
        state,
        factor_metrics,
        metric_provider,
        cancel_check,
        evaluator,
        all_candidates,
        selected_factor_union,
        winners,
    ) -> None:
        for inner in nested.inner:
            if _cancelled(cancel_check):
                state.cancelled = True
                break
            if self._run_one_inner(
                panel,
                request,
                inner,
                state,
                factor_metrics,
                metric_provider,
                cancel_check,
                evaluator,
                all_candidates,
                selected_factor_union,
                winners,
            ):
                break

    @staticmethod
    def _select_winner(winners, all_candidates):
        """跨 inner fold 的多数投票选候选 (原聚合段落)。"""
        by_candidate: dict[str, list[float]] = {}
        for score, candidate in winners:
            by_candidate.setdefault(candidate.candidate_id, []).append(score)
        selected_id, selected_scores = min(
            by_candidate.items(),
            key=lambda item: (-len(item[1]), -float(np.mean(item[1])), item[0]),
        )
        voted_candidate = all_candidates[selected_id]
        inner_score = float(np.mean(selected_scores))
        return voted_candidate, inner_score

    def _append_outer_error(
        self,
        nested,
        request,
        panel,
        state,
        evaluator,
        cancel_check,
        outer_pruned,
        candidates,
        inner_score,
        error,
        *,
        selected_candidate_id=None,
    ) -> bool:
        """外窗错误路径统一记一条 fold 结果; 返回应终止外层循环。"""
        state.fold_results.append(
            FoldMiningResult(
                outer_index=nested.outer.outer_index,
                selected_factors=outer_pruned.selected,
                candidates=candidates,
                selected_candidate_id=selected_candidate_id,
                inner_score=round(inner_score, 8),
                outer_evaluation=None,
                error=error,
                benchmark_evaluations=self._run_benchmarks(
                    nested, panel, request, state, cancel_check, evaluator
                ),
            )
        )
        return True

    def _prepare_outer_selection(
        self,
        panel,
        request,
        nested,
        state,
        factor_metrics,
        metric_provider,
        cancel_check,
        evaluator,
        all_candidates,
        voted_candidate,
        inner_score,
    ):
        """外窗重训并定位冠军候选; 错误路径已记账并返回 None。"""
        outer_train = _train_panel_for_fold(panel, request.date_column, nested.outer)
        outer_pruned = self._pruned_for_panel(outer_train, request, factor_metrics, metric_provider)
        proxy_remaining = request.budget.max_proxy_trials - state.proxy_trials_used
        if proxy_remaining <= 0:
            self._append_outer_error(
                nested,
                request,
                panel,
                state,
                evaluator,
                cancel_check,
                outer_pruned,
                tuple(_rank_candidates(list(all_candidates.values()))),
                inner_score,
                "proxy trial budget exhausted before outer retraining",
            )
            return None
        proxy_allowance = max(1, proxy_remaining // state.selection_phases_remaining)
        outer_beam = self._beam_for_fold(
            outer_train, outer_pruned.selected, request, proxy_allowance, cancel_check
        )
        state.proxy_trials_used += outer_beam.trials_used
        state.selection_phases_remaining -= 1
        outer_candidates = outer_beam.candidates
        by_refit_key = {
            _candidate_refit_key(candidate): candidate for candidate in outer_candidates
        }
        selected = by_refit_key.get(_candidate_refit_key(voted_candidate))
        if selected is None:
            self._append_outer_error(
                nested,
                request,
                panel,
                state,
                evaluator,
                cancel_check,
                outer_pruned,
                outer_candidates,
                inner_score,
                (
                    "outer retraining did not reproduce selected candidate structure"
                    if outer_candidates
                    else "outer training produced no candidate"
                ),
            )
            return None
        if state.trials_used >= request.budget.max_trials:
            self._append_outer_error(
                nested,
                request,
                panel,
                state,
                evaluator,
                cancel_check,
                outer_pruned,
                outer_candidates,
                inner_score,
                "real trial budget exhausted before outer evaluation",
                selected_candidate_id=selected.candidate_id,
            )
            return None
        return outer_train, outer_pruned, outer_candidates, selected

    def _run_outer_evaluation(
        self,
        panel,
        request,
        nested,
        state,
        factor_metrics,
        metric_provider,
        cancel_check,
        evaluator,
        all_candidates,
        voted_candidate,
        inner_score,
    ) -> bool:
        """外窗重训与评估; 返回是否应终止外层循环。"""
        prepared = self._prepare_outer_selection(
            panel,
            request,
            nested,
            state,
            factor_metrics,
            metric_provider,
            cancel_check,
            evaluator,
            all_candidates,
            voted_candidate,
            inner_score,
        )
        if prepared is None:
            return True
        outer_train, outer_pruned, outer_candidates, selected = prepared
        outer_test = _panel_for_labels(panel, request.date_column, nested.outer.test_labels)
        outer_evaluation = self._score_candidate(
            evaluator,
            state.labels_evaluator,
            selected,
            panel_train=outer_train,
            panel_test=outer_test,
            train_labels=nested.outer.train_labels,
            test_labels=nested.outer.test_labels,
        )
        state.trials_used += 1
        state.evaluation_phases_remaining -= 1
        state.fold_results.append(
            FoldMiningResult(
                outer_index=nested.outer.outer_index,
                selected_factors=outer_pruned.selected,
                candidates=outer_candidates,
                selected_candidate_id=selected.candidate_id,
                inner_score=round(inner_score, 8),
                outer_evaluation=outer_evaluation,
                benchmark_evaluations=self._run_benchmarks(
                    nested, panel, request, state, cancel_check, evaluator
                ),
            )
        )
        return state.cancelled or state.trials_used >= request.budget.max_trials

    def _run_evaluated_fold(
        self,
        panel,
        request,
        nested,
        state,
        factor_metrics,
        metric_provider,
        cancel_check,
        evaluator,
    ) -> bool:
        """evaluator 分支的单个外窗; 返回是否应终止外层循环。"""
        winners: list[tuple[float, MiningCandidate]] = []
        all_candidates: dict[str, MiningCandidate] = {}
        selected_factor_union: set[str] = set()
        self._run_inner_folds(
            panel,
            request,
            nested,
            state,
            factor_metrics,
            metric_provider,
            cancel_check,
            evaluator,
            all_candidates,
            selected_factor_union,
            winners,
        )
        if state.cancelled:
            return True
        if not winners:
            state.fold_results.append(
                FoldMiningResult(
                    outer_index=nested.outer.outer_index,
                    selected_factors=tuple(sorted(selected_factor_union)),
                    candidates=tuple(_rank_candidates(list(all_candidates.values()))),
                    selected_candidate_id=None,
                    inner_score=None,
                    outer_evaluation=None,
                    error="no candidate completed inner validation within budget",
                    benchmark_evaluations=self._run_benchmarks(
                        nested, panel, request, state, cancel_check, evaluator
                    ),
                )
            )
            return True
        voted_candidate, inner_score = self._select_winner(winners, all_candidates)
        return self._run_outer_evaluation(
            panel,
            request,
            nested,
            state,
            factor_metrics,
            metric_provider,
            cancel_check,
            evaluator,
            all_candidates,
            voted_candidate,
            inner_score,
        )

    def _cross_fold_evaluate(
        self, panel, request, nested_folds, state, cancel_check, evaluator
    ) -> None:
        """冠军跨外窗复核 (原循环后段落)。"""
        winner_union: dict[str, MiningCandidate] = {}
        for fold in state.fold_results:
            if fold.selected_candidate_id is None:
                continue
            if fold.selected_candidate_id in winner_union:
                continue
            winner = next(
                (
                    candidate
                    for candidate in fold.candidates
                    if candidate.candidate_id == fold.selected_candidate_id
                ),
                None,
            )
            if winner is not None:
                winner_union[winner.candidate_id] = winner
        if len(winner_union) <= 1:
            return

        extended_folds: list[FoldMiningResult] = []
        for nested, fold in zip(nested_folds, state.fold_results, strict=False):
            cross: list[tuple[str, CandidateEvaluation]] = []
            for candidate_id, candidate in winner_union.items():
                if candidate_id == fold.selected_candidate_id:
                    continue
                if state.trials_used >= request.budget.max_trials:
                    cross.append(
                        (
                            candidate_id,
                            CandidateEvaluation(
                                score=None,
                                error=("real trial budget exhausted before cross-fold evaluation"),
                            ),
                        )
                    )
                    continue
                if callable(state.labels_evaluator):
                    evaluation = self._score_candidate(
                        evaluator,
                        state.labels_evaluator,
                        candidate,
                        train_labels=nested.outer.train_labels,
                        test_labels=nested.outer.test_labels,
                    )
                else:
                    evaluation = self._score_candidate(
                        evaluator,
                        state.labels_evaluator,
                        candidate,
                        panel_train=_train_panel_for_fold(panel, request.date_column, nested.outer),
                        panel_test=_panel_for_labels(
                            panel, request.date_column, nested.outer.test_labels
                        ),
                    )
                state.trials_used += 1
                cross.append((candidate_id, evaluation))
            extended_folds.append(replace(fold, cross_evaluations=tuple(cross)))
        state.fold_results = extended_folds

    def _initialize_run(self, panel, request, factor_metrics, metric_provider, evaluator):
        """校验输入并生成嵌套 fold 与初始计数 (原 run 前置段落)。"""
        _validate_factor_names(panel, request.factor_names)
        required = {request.date_column, request.target_column}
        missing = sorted(required - set(panel.columns))
        if missing:
            raise ValueError(f"mining panel is missing required columns: {missing}")
        if evaluator is not None:
            if factor_metrics is not None:
                raise ValueError("factor_metrics must not be supplied with evaluator")
            if metric_provider is None:
                raise ValueError("metric_provider is required when evaluator is supplied")
        elif metric_provider is None and factor_metrics is None:
            raise ValueError("factor_metrics or metric_provider is required")

        labels = (
            panel.select(pl.col(request.date_column).cast(pl.Utf8).str.slice(0, 10).unique().sort())
            .to_series()
            .to_list()
        )
        nested_folds = generate_nested_folds(labels, request.validation)
        if request.profile != "exploratory" and len(nested_folds) < 3:
            raise ValueError(
                f"{request.profile} mining requires at least 3 outer folds; got {len(nested_folds)}"
            )

        state = _MiningRunState()
        state.labels_evaluator = (
            getattr(evaluator, "evaluate_candidate_labels", None) if evaluator is not None else None
        )
        if evaluator is None:
            state.selection_phases_remaining = len(nested_folds)
            state.evaluation_phases_remaining = 0
        else:
            state.selection_phases_remaining = sum(len(nested.inner) + 1 for nested in nested_folds)
            state.evaluation_phases_remaining = state.selection_phases_remaining
        return nested_folds, state

    def run(
        self,
        panel: pl.DataFrame,
        request: MiningRequest,
        *,
        factor_metrics: Sequence[FactorMetric] | None = None,
        metric_provider: FactorMetricProvider | None = None,
        evaluator: CandidateEvaluator | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> MiningResult:
        started = time.perf_counter()
        nested_folds, state = self._initialize_run(
            panel, request, factor_metrics, metric_provider, evaluator
        )

        for nested in nested_folds:
            if _cancelled(cancel_check):
                state.cancelled = True
                break
            if evaluator is None:
                should_break = self._run_proxy_fold(
                    panel,
                    request,
                    nested,
                    state,
                    factor_metrics,
                    metric_provider,
                    cancel_check,
                )
            else:
                should_break = self._run_evaluated_fold(
                    panel,
                    request,
                    nested,
                    state,
                    factor_metrics,
                    metric_provider,
                    cancel_check,
                    evaluator,
                )
            if should_break:
                break

        if not state.cancelled and evaluator is not None:
            self._cross_fold_evaluate(panel, request, nested_folds, state, cancel_check, evaluator)

        return MiningResult(
            request=request,
            folds=tuple(state.fold_results),
            proxy_trials_used=state.proxy_trials_used,
            trials_used=state.trials_used,
            cancelled=state.cancelled,
            elapsed_ms=round((time.perf_counter() - started) * 1000.0, 3),
        )
