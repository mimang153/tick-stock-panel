from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    _SCORE_ASSET_CHUNK_SIZE,
    SCORING_DIRECTION_LOW,
    Mapping,
    MarketDataMatrix,
    Protocol,
    SignalMatrix,
    _activate_valid_bar_index,
    _cached_matrix_operation,
    active_matrix_compute_cache,
    dataclass,
    field,
    np,
    nullcontext,
    runtime_checkable,
    time,
)


@runtime_checkable
class MatrixStrategy(Protocol):
    def required_fields(self) -> frozenset[str]: ...

    def required_warmup_bars(self, params: dict) -> int: ...

    def compute_signals(
        self,
        market: MarketDataMatrix,
        params: dict,
    ) -> SignalMatrix: ...


@dataclass(frozen=True)
class MatrixPipelineConfig:
    basic_filter: dict
    scoring: dict[str, float]
    order_by: str | None
    descending: bool
    scoring_directions: dict[str, str] = field(default_factory=dict)
    asset_mask: np.ndarray | None = None
    protect_strategy_cache: bool = False


class MatrixStrategyPipeline:
    """Apply framework-owned filtering and scoring to a matrix strategy output."""

    def run(
        self,
        strategy: MatrixStrategy,
        market: MarketDataMatrix,
        params: dict,
        config: MatrixPipelineConfig,
        timing_ms: dict[str, float] | None = None,
    ) -> SignalMatrix:
        with _activate_valid_bar_index(market.valid_bars):
            return self._run_with_valid_bars(
                strategy,
                market,
                params,
                config,
                timing_ms,
            )

    def _run_with_valid_bars(
        self,
        strategy: MatrixStrategy,
        market: MarketDataMatrix,
        params: dict,
        config: MatrixPipelineConfig,
        timing_ms: dict[str, float] | None,
    ) -> SignalMatrix:
        strategy_started = time.perf_counter()
        signals = strategy.compute_signals(market, params)
        _root.validate_signal_matrix(signals, market.shape)
        if timing_ms is not None:
            timing_ms["strategy_signals"] = round(
                (time.perf_counter() - strategy_started) * 1000,
                1,
            )

        filter_started = time.perf_counter()
        cache = active_matrix_compute_cache()
        protect_cache = (
            cache is not None
            and config.protect_strategy_cache
            and not cache.has_cached_operation("basic_filter_mask")
            and cache.current_bytes + _estimate_pipeline_cache_bytes(market, config)
            > cache.max_bytes
        )
        cache_scope = cache.suspend() if protect_cache else nullcontext()  # type: ignore[union-attr]
        with cache_scope:
            basic_mask = build_pipeline_filter_mask(market, config)
            entry = (signals.entry.astype(bool) & basic_mask).astype(np.uint8)
            score = build_matrix_score(
                market,
                entry.astype(bool),
                config.scoring,
                config.order_by,
                config.descending,
                fallback=signals.score,
                directions=config.scoring_directions,
            )
            entry_codes = np.where(entry != 0, signals.entry_signal_code, -1).astype(np.int16)
            exit_codes = np.where(signals.exit != 0, signals.exit_signal_code, -1).astype(np.int16)
        if timing_ms is not None:
            timing_ms["filter_score"] = round(
                (time.perf_counter() - filter_started) * 1000,
                1,
            )
        return _root.make_signal_matrix(
            market.shape,
            entry=entry,
            exit=signals.exit,
            score=score,
            entry_signal_code=entry_codes,
            exit_signal_code=exit_codes,
            entry_signal_ids=signals.entry_signal_ids,
            exit_signal_ids=signals.exit_signal_ids,
        )


def _estimate_pipeline_cache_bytes(
    market: MarketDataMatrix,
    config: MatrixPipelineConfig,
) -> int:
    float_bytes = int(market.close.nbytes)
    bool_bytes = int(market.shape[0] * market.shape[1])
    estimated = bool_bytes
    if config.asset_mask is not None:
        estimated += bool_bytes

    feature_names = {name for name, weight in config.scoring.items() if float(weight) != 0.0}
    if not feature_names and config.order_by and config.order_by != "score":
        feature_names.add(str(config.order_by))
    for name in feature_names:
        if name in {"open", "high", "low", "close", "volume"} or name in market.fields:
            continue
        estimated += 5 * float_bytes
    return estimated


def build_pipeline_filter_mask(
    market: MarketDataMatrix,
    config: MatrixPipelineConfig,
) -> np.ndarray:
    basic_mask = build_basic_filter_mask(market, config.basic_filter)
    if config.asset_mask is None:
        return basic_mask

    asset_mask = np.asarray(config.asset_mask, dtype=bool)
    if asset_mask.shape != (market.shape[1],):
        raise ValueError("matrix strategy asset mask length does not match market")
    return _cached_matrix_operation(
        "pipeline_filter_mask",
        (basic_mask, asset_mask),
        {},
        lambda: basic_mask & asset_mask[None, :],
    )


def build_basic_filter_mask(market: MarketDataMatrix, config: dict) -> np.ndarray:
    cache = active_matrix_compute_cache()
    if cache is None:
        return _build_basic_filter_mask_uncached(market, config)
    return cache.get_or_compute(
        "basic_filter_mask",
        (),
        config,
        lambda: _build_basic_filter_mask_uncached(market, config),
        key_parts=cache.market_token(market),
    )


def _build_basic_filter_mask_uncached(market: MarketDataMatrix, config: dict) -> np.ndarray:
    if not config or not config.get("enabled", True):
        return np.ones(market.shape, dtype=bool)

    mask = np.ones(market.shape, dtype=bool)
    close = market.close
    if config.get("price_min") is not None:
        mask &= close >= float(config["price_min"])
    if config.get("price_max") is not None:
        mask &= close <= float(config["price_max"])

    _root._apply_bound(
        mask, close * _root._optional_field(market, "total_shares"), config, "market_cap"
    )
    _root._apply_bound(
        mask, close * _root._optional_field(market, "float_shares"), config, "float_cap"
    )
    _root._apply_bound(
        mask, _root._required_field_for_bound(market, config, "amount"), config, "amount"
    )
    _root._apply_bound(mask, _root._optional_field(market, "turnover_rate"), config, "turnover")

    if config.get("exclude_st"):
        asset_mask = np.array(
            [
                not any(token in name.upper() for token in ("ST", "*ST", "退"))
                for name in market.names
            ],
            dtype=bool,
        )
        mask &= asset_mask[None, :]

    boards = config.get("boards")
    if isinstance(boards, list) and boards:
        board_mask = np.zeros(len(market.symbols), dtype=bool)
        for asset_id, symbol in enumerate(market.symbols):
            board_mask[asset_id] = _root._symbol_in_boards(symbol, boards)
        mask &= board_mask[None, :]
    return mask


def _score_matrix_by_order(
    market: MarketDataMatrix,
    universe: np.ndarray,
    order_by: str,
    descending: bool,
) -> np.ndarray:
    """按单字段排序投影评分，保持分块步长与方向不变。"""
    values = _root.matrix_feature(market, order_by)
    result = np.zeros(market.shape, dtype=np.float32)
    direction = np.float32(1.0 if descending else -1.0)
    for start in range(0, market.shape[1], _SCORE_ASSET_CHUNK_SIZE):
        stop = min(start + _SCORE_ASSET_CHUNK_SIZE, market.shape[1])
        values_chunk = values[:, start:stop]
        valid = universe[:, start:stop] & np.isfinite(values_chunk)
        np.multiply(values_chunk, direction, out=result[:, start:stop], where=valid)
    return result


def build_matrix_score(
    market: MarketDataMatrix,
    universe: np.ndarray,
    scoring: dict[str, float],
    order_by: str | None,
    descending: bool,
    *,
    fallback: np.ndarray,
    directions: Mapping[str, str] | None = None,
) -> np.ndarray:
    weights = {name: float(weight) for name, weight in scoring.items() if float(weight) != 0.0}
    total_weight = sum(weights.values())
    if weights and total_weight > 0:
        score = np.zeros(market.shape, dtype=np.float32)
        all_finite = universe.copy()
        row_count, asset_count = market.shape
        chunk_size = min(_SCORE_ASSET_CHUNK_SIZE, asset_count)
        finite_scratch = np.empty((row_count, chunk_size), dtype=bool)
        work_mask = np.empty((row_count, chunk_size), dtype=bool)
        value_scratch = np.empty((row_count, chunk_size), dtype=np.float32)
        for name, weight in weights.items():
            values = _root.matrix_feature(market, name)
            row_min = np.full(row_count, np.inf, dtype=np.float32)
            row_max = np.full(row_count, -np.inf, dtype=np.float32)
            for start in range(0, asset_count, chunk_size):
                stop = min(start + chunk_size, asset_count)
                width = stop - start
                finite = finite_scratch[:, :width]
                values_chunk = values[:, start:stop]
                np.isfinite(values_chunk, out=finite)
                all_finite[:, start:stop] &= finite
                finite &= universe[:, start:stop]
                np.minimum(
                    row_min,
                    np.min(values_chunk, axis=1, where=finite, initial=np.inf),
                    out=row_min,
                )
                np.maximum(
                    row_max,
                    np.max(values_chunk, axis=1, where=finite, initial=-np.inf),
                    out=row_max,
                )
            row_range = row_max - row_min
            varying_rows = np.isfinite(row_range) & (row_range > 0)
            normalized_weight = np.float32(weight / total_weight)
            for start in range(0, asset_count, chunk_size):
                stop = min(start + chunk_size, asset_count)
                width = stop - start
                finite = finite_scratch[:, :width]
                mask = work_mask[:, :width]
                scratch = value_scratch[:, :width]
                values_chunk = values[:, start:stop]
                np.isfinite(values_chunk, out=finite)
                finite &= universe[:, start:stop]
                scratch.fill(0.0)
                np.logical_and(finite, varying_rows[:, None], out=mask)
                np.subtract(values_chunk, row_min[:, None], out=scratch, where=mask)
                np.divide(scratch, row_range[:, None], out=scratch, where=mask)
                np.logical_and(finite, ~varying_rows[:, None], out=mask)
                scratch[mask] = np.float32(0.5)
                if (directions or {}).get(name) == SCORING_DIRECTION_LOW:
                    scratch[finite] = np.float32(1.0) - scratch[finite]
                scratch *= normalized_weight
                score[:, start:stop] += scratch
        score *= np.float32(100.0)
        score[~universe | ~all_finite] = 0.0
        return score

    if order_by and order_by != "score":
        return _score_matrix_by_order(market, universe, order_by, descending)
    result = np.zeros(market.shape, dtype=np.float32)
    np.copyto(result, fallback, where=universe)
    return result
