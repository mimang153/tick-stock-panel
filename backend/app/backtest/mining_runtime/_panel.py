"""Internal split module for app.backtest.mining_runtime: _panel."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl

import app.backtest.mining_runtime as _self
from app.backtest.factor import FACTOR_METHODOLOGY_VERSION, FactorBacktestService, FactorBatchConfig
from app.backtest.fundamentals import (
    FUNDAMENTAL_FACTOR_NAMES,
    attach_fundamental_factors,
    load_fundamental_snapshot,
)
from app.backtest.mining import CandidateEvaluation, MiningResult, MiningService
from app.backtest.strategy import StrategyBacktestConfig, StrategyBacktestService
from app.enriched_generation import (
    EnrichedGenerationUnavailableError,
    enriched_publication_incomplete,
)
from app.factors.registry import factor_calculation_order, get_factor
from app.services.mining_jobs import get_run_store
from app.services.mining_schedule import MINING_ALGORITHM_VERSION, verify_mining_factor_snapshot
from app.strategy import config as strategy_config
from app.strategy.engine import StrategyEngine

from ._artifacts import _build_artifacts, _validate_regime_availability
from ._common import (
    _MINING_MATRIX_CACHE_BYTES,
    _REGIME_FILTERS,
    _RESULT_POLICY,
    _SNAPSHOT_MAX_ATTEMPTS,
    _SNAPSHOT_PUBLISH_WAIT_SECONDS,
    CancelCheck,
    MiningRuntimeCancelledError,
    ProgressCallback,
    RuntimeRequest,
    TrainingMetricProvider,
)
from ._forward import _decode_runtime_request, _prepare_base_market, attach_single_forward_return
from ._refine import (
    _atomic_write_parquet,
    _budget_exhausted,
    _confidence,
    _date_labels,
    _finite_or_none,
    _raise_if_cancelled,
)


class MatcherCandidateEvaluator:
    """Evaluate one fixed definition with the production matrix matcher."""

    def __init__(
        self,
        service: StrategyBacktestService,
        strategy_engine: StrategyEngine,
        data_dir: Path,
        request: RuntimeRequest,
        base_market,
        cancel_check: CancelCheck | None,
    ) -> None:
        self.service = service
        self.strategy_engine = strategy_engine
        self.data_dir = data_dir
        self.request = request
        self.base_market = base_market
        self.cancel_check = cancel_check
        self.backtest_count = 0
        self.peak_compute_cache_bytes = 0

    def evaluate_candidate(
        self,
        train: pl.DataFrame,
        test: pl.DataFrame,
        definition: Mapping[str, Any],
    ) -> CandidateEvaluation:
        del train
        return self.evaluate_test(test, definition)

    def evaluate_candidate_labels(
        self,
        train_labels: Sequence[str],
        test_labels: Sequence[str],
        definition: Mapping[str, Any],
    ) -> CandidateEvaluation:
        del train_labels
        return self._evaluate_labels(test_labels, definition)

    def evaluate_test(
        self,
        test: pl.DataFrame,
        definition: Mapping[str, Any],
        *,
        regime_state: str = "overall",
    ) -> CandidateEvaluation:
        return self._evaluate_labels(
            _date_labels(test),
            definition,
            regime_state=regime_state,
        )

    def _evaluate_labels(
        self,
        labels: Sequence[str],
        definition: Mapping[str, Any],
        *,
        regime_state: str = "overall",
    ) -> CandidateEvaluation:
        _raise_if_cancelled(self.cancel_check)
        if not labels:
            return CandidateEvaluation(score=None, error="test fold contains no dates")
        try:
            config = self._backtest_config(
                definition,
                date.fromisoformat(labels[0]),
                date.fromisoformat(labels[-1]),
                regime_state,
            )
            prepared = self.service.prepare_matrix_optimization(
                [config],
                matrix_cache_max_bytes=_MINING_MATRIX_CACHE_BYTES,
                market_data_override=self.base_market,
            )
            try:
                result = self.service.run(
                    config,
                    cancel_event=cast(Any, self.cancel_check),
                    prepared=prepared,
                    result_policy=_RESULT_POLICY,
                )
                self.peak_compute_cache_bytes = max(
                    self.peak_compute_cache_bytes,
                    prepared.compute_cache.snapshot()["peak_bytes"],
                )
            finally:
                prepared.compute_cache.close()
            self.backtest_count += 1
            if result.error:
                return CandidateEvaluation(score=None, error=result.error)
            metrics = {
                key: _finite_or_none(result.stats.get(key))
                for key in ("total_return", "sharpe", "max_drawdown", "n_trades")
            }
            score = _finite_or_none(metrics.get("sharpe"))
            if score is None:
                return CandidateEvaluation(
                    score=None,
                    metrics=metrics,
                    error="backtest did not return a finite sharpe",
                )
            return CandidateEvaluation(score=score, metrics=metrics)
        except (OSError, ValueError, TypeError) as exc:
            return CandidateEvaluation(score=None, error=str(exc))

    def _backtest_config(
        self,
        definition: Mapping[str, Any],
        start: date,
        end: date,
        regime_state: str,
    ) -> StrategyBacktestConfig:
        kind = str(definition.get("kind") or "")
        if kind == "existing_strategy":
            strategy_id = str(definition.get("strategy_id") or "")
            strategy = self.strategy_engine.get(strategy_id)
            overrides = strategy_config.load_override(self.data_dir, strategy_id)
            params = self.strategy_engine.resolve_params(strategy, overrides=overrides)
        elif kind == "factor_rank":
            strategy_id = "factor_rank_research"
            scoring = definition.get("scoring")
            directions = definition.get("directions")
            if not isinstance(scoring, Mapping) or not scoring:
                raise ValueError("factor candidate has no scoring definition")
            if not isinstance(directions, Mapping):
                raise ValueError("factor candidate has no direction definition")
            params = {
                "scoring": {str(key): float(value) for key, value in scoring.items()},
                "directions": {str(key): str(value) for key, value in directions.items()},
                "entry_score": 70.0,
                "exit_score": 40.0,
                "top_rank": 20,
            }
            overrides = {}
        else:
            raise ValueError(f"unsupported mining candidate kind: {kind!r}")

        regime_filter = None
        if regime_state != "overall":
            regime_filter = _REGIME_FILTERS.get(regime_state)
            if regime_filter is None:
                raise ValueError(f"unsupported regime state: {regime_state}")
        return StrategyBacktestConfig(
            strategy_id=strategy_id,
            symbols=self.request.symbols,
            start=start,
            end=end,
            params=params,
            overrides=overrides,
            matching="open_t+1",
            entry_fill="open_t+1",
            exit_fill="open_t+1",
            fees_pct=self.request.commission_pct,
            commission_pct=self.request.commission_pct,
            stamp_tax_pct=self.request.stamp_tax_pct,
            slippage_bps=self.request.slippage_bps,
            max_positions=10,
            max_exposure_pct=1.0,
            initial_capital=1_000_000.0,
            position_sizing="equal",
            mode="position",
            asset_type=self.request.asset_type,
            holding_days=self.request.forward_horizon,
            minute_fill=False,
            regime_filter=regime_filter,
        )


def _concat_compact_factor_batches(
    raw: pl.DataFrame,
    factor_service: FactorBacktestService,
    config: FactorBatchConfig,
    factor_names: Sequence[str],
    ordered: Sequence[str],
    compile_formula_cached: Callable[..., Any],
    cancel_check: CancelCheck | None,
) -> pl.DataFrame:
    output_by_date: dict[date, list[pl.DataFrame]] = {}
    group_sizes = raw.group_by("symbol", maintain_order=True).len().get_column("len").to_list()
    # 截面公式/组合必须同时看到全部标的; Python 表达式的分组方式不可静态穷尽。
    full_cross_section = any(
        spec.kind == "composite"
        or (
            spec.kind == "custom"
            and (spec.lang == "python" or compile_formula_cached(spec.formula_text).cross_sectional)
        )
        for name in ordered
        if (spec := get_factor(name)) is not None
    )
    batch_size = len(group_sizes) if full_cross_section else _self._SYMBOL_BATCH_SIZE
    row_offset = 0
    for offset in range(0, len(group_sizes), batch_size):
        _raise_if_cancelled(cancel_check)
        row_count = sum(group_sizes[offset : offset + batch_size])
        batch = raw.slice(row_offset, row_count)
        row_offset += row_count
        missing = set(factor_names) - set(batch.columns)
        if missing:
            batch = factor_service._compute_missing_factors(
                batch,
                missing,
                assume_sorted=True,
                data_dir=factor_service._fundamentals_data_dir(),
            )
        projected = batch.filter(
            (pl.col("date") >= config.start)
            & (pl.col("date") <= config.end)
            & pl.col("close").is_not_null()
            & (pl.col("close") > 0)
        ).select(
            [
                "symbol",
                "date",
                "close",
                *(pl.col(name).cast(pl.Float32, strict=False).alias(name) for name in factor_names),
            ]
        )
        for daily in projected.partition_by("date", maintain_order=True):
            output_by_date.setdefault(daily.item(0, "date"), []).append(daily)
        del batch, projected
    return pl.concat(
        [
            pl.concat(output_by_date[label], how="vertical", rechunk=False)
            for label in sorted(output_by_date)
        ],
        how="vertical",
        rechunk=False,
    )


def _load_compact_factor_panel(
    factor_service: FactorBacktestService,
    config: FactorBatchConfig,
    factor_names: Sequence[str],
    *,
    expected_generation: str,
    cancel_check: CancelCheck | None,
) -> pl.DataFrame:
    panel_columns = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover_rate",
    ]
    from app.factors.dsl import compile_formula_cached
    from app.indicators.pipeline import ENRICHED_STORAGE_COLS

    ordered = factor_calculation_order(factor_names)
    required = set(ordered)
    panel_columns.extend(sorted((required & set(ENRICHED_STORAGE_COLS)) - set(panel_columns)))
    load_start = factor_service._factor_load_start(config, factor_names)
    raw = factor_service.engine.load_panel(
        config.symbols,
        load_start,
        config.end,
        columns=panel_columns,
        asset_type=config.asset_type,
        expected_generation=expected_generation,
    )
    if raw.is_empty():
        return raw

    fundamental_names = sorted(required & FUNDAMENTAL_FACTOR_NAMES)
    if fundamental_names:
        data_dir = getattr(
            getattr(getattr(factor_service.engine, "repo", None), "store", None),
            "data_dir",
            None,
        )
        raw = attach_fundamental_factors(
            raw,
            load_fundamental_snapshot(data_dir),
            fundamental_names,
        )

    symbol = pl.col("symbol")
    day = pl.col("date")
    previous_symbol = symbol.shift(1)
    previous_day = day.shift(1)
    invalid_key = raw.select(
        (
            symbol.is_null()
            | day.is_null()
            | (symbol < previous_symbol).fill_null(False)
            | ((symbol == previous_symbol) & (day <= previous_day)).fill_null(False)
        ).any()
    ).item()
    if invalid_key:
        raise ValueError(
            "mining factor panel requires non-null, unique symbol/date keys "
            "sorted by symbol and strictly increasing date"
        )
    return _concat_compact_factor_batches(
        raw,
        factor_service,
        config,
        factor_names,
        ordered,
        compile_formula_cached,
        cancel_check,
    )


def _decode_request_and_generation(
    payload: Mapping[str, Any],
    data_dir: Path,
    strategy_engine: StrategyEngine,
) -> tuple[RuntimeRequest, str]:
    request = _decode_runtime_request(payload, data_dir, strategy_engine)
    fingerprint = payload.get("data_fingerprint")
    expected_generation = (
        fingerprint.get("generation") if isinstance(fingerprint, Mapping) else None
    )
    if not isinstance(expected_generation, str) or not expected_generation:
        raise ValueError("mining worker payload is missing its data generation")
    verify_mining_factor_snapshot(request.factor_names, fingerprint)
    return request, expected_generation


def _build_mining_factor_config(request: RuntimeRequest) -> FactorBatchConfig:
    return FactorBatchConfig(
        factor_names=list(request.factor_names),
        symbols=request.symbols,
        start=request.start,
        end=request.end,
        asset_type=request.asset_type,
        commission_pct=request.commission_pct,
        stamp_tax_pct=request.stamp_tax_pct,
        slippage_bps=request.slippage_bps,
    )


def _load_mining_panel(
    factor_service: FactorBacktestService,
    service: StrategyBacktestService,
    data_dir: Path,
    request: RuntimeRequest,
    factor_config: FactorBatchConfig,
    generation: str,
    cancel_check: CancelCheck | None,
    *,
    panel_started: float,
    emit: ProgressCallback,
    finish_phase: Callable[[str], None],
    phase_ms: dict[str, float],
) -> pl.DataFrame:
    source_panel = _load_compact_factor_panel(
        factor_service,
        factor_config,
        request.factor_names,
        expected_generation=generation,
        cancel_check=cancel_check,
    )
    if source_panel.is_empty():
        raise ValueError("mining date range contains no enriched data")
    service.engine.clear_panel_cache()
    trading_dates = _self.enriched_partition_dates(
        data_dir,
        request.asset_type,
        request.start,
        request.end,
    )
    factor_service._assert_data_generation(request.asset_type, generation)
    panel = attach_single_forward_return(
        source_panel,
        start=request.start,
        end=request.end,
        horizon=request.forward_horizon,
        trading_dates=trading_dates,
        factor_names=request.factor_names,
        target_column=request.mining_request.target_column,
        assume_unique_symbol_date=True,
    )
    del source_panel
    if panel.is_empty():
        raise ValueError("mining panel contains no valid price rows")
    phase_ms["panel"] = round((time.perf_counter() - panel_started) * 1000.0, 3)
    finish_phase("panel")
    emit(
        {
            "phase": "panel",
            "label": "因子面板已准备",
            "done": 1,
            "total": 1,
            "rows": panel.height,
            "factors": len(request.factor_names),
        }
    )
    if request.require_regime:
        emit({"phase": "panel", "label": "校验市场环境数据", "done": 0, "total": 1})
        _validate_regime_availability(panel, request, data_dir)
    return panel


def _prepare_mining_matrix(
    factor_service: FactorBacktestService,
    service: StrategyBacktestService,
    strategy_engine: StrategyEngine,
    data_dir: Path,
    request: RuntimeRequest,
    generation: str,
    cancel_check: CancelCheck | None,
    *,
    matrix_started: float,
    emit: ProgressCallback,
    finish_phase: Callable[[str], None],
    phase_ms: dict[str, float],
) -> np.ndarray:
    base_market = _prepare_base_market(
        service,
        strategy_engine,
        data_dir,
        request,
        expected_generation=generation,
        cancel_check=cancel_check,
    )
    factor_service._assert_data_generation(request.asset_type, generation)
    phase_ms["matrix"] = round((time.perf_counter() - matrix_started) * 1000.0, 3)
    finish_phase("matrix")
    emit(
        {
            "phase": "matrix",
            "label": "共享撮合矩阵已准备",
            "done": 1,
            "total": 1,
            "matrix_bytes": base_market.nbytes,
        }
    )
    return base_market


def _load_snapshot_and_matrix(
    data_dir: Path,
    service: StrategyBacktestService,
    strategy_engine: StrategyEngine,
    request: RuntimeRequest,
    expected_generation: str,
    cancel_check: CancelCheck | None,
    *,
    emit: ProgressCallback,
    start_phase: Callable[[], None],
    finish_phase: Callable[[str], None],
    phase_ms: dict[str, float],
) -> tuple[pl.DataFrame, np.ndarray]:
    emit({"phase": "panel", "label": "加载因子面板", "done": 0, "total": 1})
    start_phase()
    _raise_if_cancelled(cancel_check)
    factor_service = FactorBacktestService(service.engine)
    factor_config = _build_mining_factor_config(request)
    panel: pl.DataFrame | None = None
    base_market: np.ndarray | None = None
    generation: str | None = None
    for attempt in range(_SNAPSHOT_MAX_ATTEMPTS):
        if attempt:
            # 读取期间 enriched 发布了新世代, 面板可能新旧混合: 丢弃本轮,
            # 用新世代整体重读。排队指纹只在首轮校验; 数据在运行中前进,
            # 重试跟随新世代属于预期 (等价于"更新后立刻重跑")。
            emit({"phase": "panel", "label": "数据已更新, 重新读取快照", "done": 0, "total": 1})
            if enriched_publication_incomplete(data_dir):
                time.sleep(_SNAPSHOT_PUBLISH_WAIT_SECONDS)
            service.engine.clear_panel_cache()
        panel_started = time.perf_counter()
        try:
            generation = factor_service._data_generation(request.asset_type)
            if attempt == 0 and generation != expected_generation:
                raise ValueError("mining data generation changed after the run was queued")
            panel = _load_mining_panel(
                factor_service,
                service,
                data_dir,
                request,
                factor_config,
                cast(str, generation),
                cancel_check,
                panel_started=panel_started,
                emit=emit,
                finish_phase=finish_phase,
                phase_ms=phase_ms,
            )
            _raise_if_cancelled(cancel_check)
            start_phase()
            matrix_started = time.perf_counter()
            emit({"phase": "matrix", "label": "准备共享撮合矩阵", "done": 0, "total": 1})
            base_market = _prepare_mining_matrix(
                factor_service,
                service,
                strategy_engine,
                data_dir,
                request,
                cast(str, generation),
                cancel_check,
                matrix_started=matrix_started,
                emit=emit,
                finish_phase=finish_phase,
                phase_ms=phase_ms,
            )
            break
        except EnrichedGenerationUnavailableError as exc:
            if attempt + 1 >= _SNAPSHOT_MAX_ATTEMPTS:
                raise ValueError(
                    "行情数据正在更新: 挖掘读取期间 enriched 数据世代反复变化, "
                    "重试后仍拿不到稳定快照; 请等数据更新完成后再开始挖掘"
                ) from exc
    if panel is None or base_market is None or generation is None:
        raise RuntimeError("mining data snapshot did not settle")
    return panel, base_market


def _run_mining_search(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    base_market: np.ndarray,
    service: StrategyBacktestService,
    strategy_engine: StrategyEngine,
    data_dir: Path,
    cancel_check: CancelCheck | None,
    *,
    emit: ProgressCallback,
    start_phase: Callable[[], None],
    finish_phase: Callable[[str], None],
    phase_ms: dict[str, float],
) -> tuple[MiningResult, TrainingMetricProvider, MatcherCandidateEvaluator]:
    metric_provider = TrainingMetricProvider(request.mining_request.target_column)
    evaluator = MatcherCandidateEvaluator(
        service,
        strategy_engine,
        data_dir,
        request,
        base_market,
        cancel_check,
    )
    emit({"phase": "search", "label": "嵌套样本外搜索", "done": 0, "total": 1})
    start_phase()
    search_started = time.perf_counter()
    result = MiningService().run(
        panel,
        request.mining_request,
        metric_provider=metric_provider,
        evaluator=evaluator,
        cancel_check=cancel_check,
    )
    phase_ms["search"] = round((time.perf_counter() - search_started) * 1000.0, 3)
    finish_phase("search")
    if result.cancelled:
        raise MiningRuntimeCancelledError("mining cancelled")
    emit(
        {
            "phase": "search",
            "label": "候选搜索完成",
            "done": 1,
            "total": 1,
            "proxy_trials": result.proxy_trials_used,
            "real_trials": result.trials_used,
        }
    )
    return result, metric_provider, evaluator


def _write_artifacts(
    panel: pl.DataFrame,
    request: RuntimeRequest,
    result: MiningResult,
    metric_provider: TrainingMetricProvider,
    evaluator: MatcherCandidateEvaluator,
    cancel_check: CancelCheck | None,
    store: Any,
    *,
    emit: ProgressCallback,
    start_phase: Callable[[], None],
    finish_phase: Callable[[str], None],
    phase_ms: dict[str, float],
) -> tuple[dict[str, pl.DataFrame], dict[str, Any] | None]:
    _raise_if_cancelled(cancel_check)
    start_phase()
    artifact_started = time.perf_counter()
    emit({"phase": "artifacts", "label": "写入研究结果", "done": 0, "total": 4})
    artifact_frames, refinement_summary = _build_artifacts(
        panel,
        request,
        result,
        metric_provider,
        evaluator,
        cancel_check,
    )
    for done, (name, frame) in enumerate(artifact_frames.items(), start=1):
        _raise_if_cancelled(cancel_check)
        path = store.artifact_path(request.run_id, name)  # type: ignore[arg-type]
        _atomic_write_parquet(frame, path)
        store.register_artifact(request.run_id, name)  # type: ignore[arg-type]
        emit(
            {
                "phase": "artifacts",
                "label": f"已写入 {name}",
                "done": done,
                "total": 4,
            }
        )
    phase_ms["artifacts"] = round((time.perf_counter() - artifact_started) * 1000.0, 3)
    finish_phase("artifacts")
    return artifact_frames, refinement_summary


def _compact_summary(
    started: float,
    request: RuntimeRequest,
    result: MiningResult,
    evaluator: MatcherCandidateEvaluator,
    artifact_frames: dict[str, pl.DataFrame],
    refinement_summary: dict[str, Any] | None,
    panel: pl.DataFrame,
    base_market: np.ndarray,
    phase_ms: dict[str, float],
    phase_peak_rss_bytes: dict[str, int],
) -> dict[str, Any]:
    folds = artifact_frames["folds"]
    candidates = artifact_frames["candidates"]
    factors = artifact_frames["factors"]
    selected_overall = folds.filter(
        (pl.col("regime_state") == "overall")
        & ((pl.col("evaluation_kind") == "selected") | pl.col("candidate_signature").is_null())
    )
    valid_folds = selected_overall.filter(~pl.col("skipped")).height
    skipped_folds = selected_overall.filter(pl.col("skipped")).height
    budget_exhausted = _budget_exhausted(result)
    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
    phase_ms["total"] = elapsed_ms
    confidence = _confidence(request.profile)
    return {
        "status": ("succeeded_with_budget_exhausted" if budget_exhausted else "succeeded"),
        "factor_count": len(request.factor_names),
        "selected_factor_count": int(factors.filter(pl.col("selected")).height),
        "candidate_count": candidates.height,
        "valid_fold_count": valid_folds,
        "skipped_fold_count": skipped_folds,
        "confidence": confidence,
        "budget_exhausted": budget_exhausted,
        "elapsed_ms": elapsed_ms,
        "data_as_of": request.end.isoformat(),
        "algorithm_version": MINING_ALGORITHM_VERSION,
        "methodology_version": FACTOR_METHODOLOGY_VERSION,
        "refinement": refinement_summary,
        "proxy_trials_used": result.proxy_trials_used,
        "trials_used": result.trials_used + max(0, evaluator.backtest_count - result.trials_used),
        "panel_rows": panel.height,
        "panel_scans": 1,
        "matrix_bytes": base_market.nbytes,
        "matrix_compute_cache_peak_bytes": evaluator.peak_compute_cache_bytes,
        "phase_ms": phase_ms,
        "phase_peak_rss_bytes": phase_peak_rss_bytes,
        "artifacts": list(artifact_frames),
    }


def run_mining_runtime(
    payload: Mapping[str, Any],
    *,
    data_dir: Path,
    service: StrategyBacktestService,
    strategy_engine: StrategyEngine,
    progress_cb: ProgressCallback | None = None,
    cancel_check: CancelCheck | None = None,
    rss_sampler: Any | None = None,
) -> dict[str, Any]:
    """Run one persistent mining job and return only a compact IPC summary."""
    started = time.perf_counter()
    emit = progress_cb or (lambda _message: None)
    request, expected_generation = _decode_request_and_generation(
        payload, data_dir, strategy_engine
    )
    store = get_run_store(data_dir)
    phase_peak_rss_bytes: dict[str, int] = {}

    def start_phase() -> None:
        if rss_sampler is not None:
            rss_sampler.reset_phase()

    def finish_phase(name: str) -> None:
        if rss_sampler is not None:
            phase_peak_rss_bytes[name] = rss_sampler.phase_peak_rss_bytes()

    phase_ms: dict[str, float] = {}
    panel, base_market = _load_snapshot_and_matrix(
        data_dir,
        service,
        strategy_engine,
        request,
        expected_generation,
        cancel_check,
        emit=emit,
        start_phase=start_phase,
        finish_phase=finish_phase,
        phase_ms=phase_ms,
    )
    result, metric_provider, evaluator = _run_mining_search(
        panel,
        request,
        base_market,
        service,
        strategy_engine,
        data_dir,
        cancel_check,
        emit=emit,
        start_phase=start_phase,
        finish_phase=finish_phase,
        phase_ms=phase_ms,
    )
    artifact_frames, refinement_summary = _write_artifacts(
        panel,
        request,
        result,
        metric_provider,
        evaluator,
        cancel_check,
        store,
        emit=emit,
        start_phase=start_phase,
        finish_phase=finish_phase,
        phase_ms=phase_ms,
    )
    return _compact_summary(
        started,
        request,
        result,
        evaluator,
        artifact_frames,
        refinement_summary,
        panel,
        base_market,
        phase_ms,
        phase_peak_rss_bytes,
    )
