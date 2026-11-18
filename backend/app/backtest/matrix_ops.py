from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    _ACTIVE_VALID_BAR_INDEX,
    _ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES,
    Callable,
    ValidBarIndex,
    _cached_matrix_operation,
    njit,
    np,
    prange,
    run_numba_parallel,
)


def shift(values: np.ndarray, periods: int = 1) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    out = np.full(source.shape, np.nan, dtype=np.float32)
    if periods == 0:
        out[:] = source
    elif periods > 0 and periods < source.shape[0]:
        out[periods:] = source[:-periods]
    elif periods < 0 and -periods < source.shape[0]:
        out[:periods] = source[-periods:]
    return out


def _resolve_valid_bar_index(
    source: np.ndarray,
    valid: np.ndarray,
    bar_index: ValidBarIndex | None,
) -> ValidBarIndex:
    if bar_index is not None:
        if bar_index.shape != source.shape:
            raise ValueError("valid bar index shape does not match values")
        return bar_index
    active = _ACTIVE_VALID_BAR_INDEX.get()
    if isinstance(active, ValidBarIndex) and active.shape == source.shape:
        return active
    return _root._build_valid_bar_index(valid)


@njit(cache=True, nogil=True, parallel=True)
def _valid_shift_kernel(
    source: np.ndarray,
    valid: np.ndarray,
    offsets: np.ndarray,
    rows: np.ndarray,
    periods: int,
) -> np.ndarray:
    out = np.full(source.shape, np.nan, dtype=np.float32)
    distance = abs(periods)
    for asset_id in prange(source.shape[1]):
        start = int(offsets[asset_id])
        stop = int(offsets[asset_id + 1])
        ring = np.empty(distance, dtype=np.int32)
        seen = 0
        if periods > 0:
            for position in range(start, stop):
                row = int(rows[position])
                if not valid[row, asset_id] or not np.isfinite(source[row, asset_id]):
                    continue
                slot = seen % distance
                if seen >= distance:
                    out[row, asset_id] = source[int(ring[slot]), asset_id]
                ring[slot] = row
                seen += 1
        else:
            for position in range(stop - 1, start - 1, -1):
                row = int(rows[position])
                if not valid[row, asset_id] or not np.isfinite(source[row, asset_id]):
                    continue
                slot = seen % distance
                if seen >= distance:
                    out[row, asset_id] = source[int(ring[slot]), asset_id]
                ring[slot] = row
                seen += 1
    return out


def valid_shift(
    values: np.ndarray,
    periods: int = 1,
    valid_mask: np.ndarray | None = None,
    *,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    """Shift by effective observations, skipping missing market bars.

    The matrix keeps a shared time axis, so a suspended asset can have NaN
    rows between two valid bars.  Standard per-symbol indicators must treat
    those rows as absent rather than as observations.
    """
    source = np.asarray(values, dtype=np.float32)
    valid = (
        np.isfinite(source)
        if valid_mask is None
        else np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    )
    if valid.shape != source.shape:
        raise ValueError("valid_shift mask shape does not match values")
    if periods == 0:
        out = np.full(source.shape, np.nan, dtype=np.float32)
        out[valid] = source[valid]
        return out
    index = _resolve_valid_bar_index(source, valid, bar_index)

    return _cached_matrix_operation(
        "valid_shift",
        (source, valid, index.offsets, index.rows),
        {"periods": int(periods)},
        lambda: run_numba_parallel(
            lambda: _valid_shift_kernel(
                source,
                valid,
                index.offsets,
                index.rows,
                int(periods),
            )
        ),
    )


def rolling_min(values: np.ndarray, window: int) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    return _cached_matrix_operation(
        "rolling_min",
        (source,),
        {"window": int(window)},
        lambda: _rolling_reduce(source, window, np.min),
    )


def rolling_max(values: np.ndarray, window: int) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    return _cached_matrix_operation(
        "rolling_max",
        (source,),
        {"window": int(window)},
        lambda: _rolling_reduce(source, window, np.max),
    )


def rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    return _cached_matrix_operation(
        "rolling_mean",
        (source,),
        {"window": int(window)},
        lambda: _rolling_reduce(source, window, np.mean),
    )


def rolling_sum(values: np.ndarray, window: int) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    return _cached_matrix_operation(
        "rolling_sum",
        (source,),
        {"window": int(window)},
        lambda: _rolling_reduce(source, window, np.sum),
    )


def rolling_std(values: np.ndarray, window: int, *, ddof: int = 0) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    return _cached_matrix_operation(
        "rolling_std",
        (source,),
        {"window": int(window), "ddof": int(ddof)},
        lambda: _rolling_reduce(
            source,
            window,
            lambda view, axis: np.std(view, axis=axis, ddof=int(ddof)),  # type: ignore
            materialized_window_budget_bytes=_ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES,
        ),
    )


_VALID_REDUCE_MIN = 0
_VALID_REDUCE_MAX = 1
_VALID_REDUCE_MEAN = 2
_VALID_REDUCE_STD = 3
_VALID_REDUCE_SUM = 4


def _valid_rolling_reduce(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    operation: int,
    *,
    ddof: int = 0,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool)
    if source.ndim != 2 or valid.shape != source.shape:
        raise ValueError("valid rolling inputs must be matching 2D arrays")
    if window <= 0:
        raise ValueError("valid rolling window must be positive")
    if ddof < 0 or ddof >= window:
        raise ValueError("valid rolling ddof must be in [0, window)")
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return run_numba_parallel(
        lambda: _root._valid_rolling_kernel(
            source,
            valid,
            index.offsets,
            index.rows,
            int(window),
            int(operation),
            int(ddof),
        )
    )


def valid_rolling_min(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    *,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return _cached_matrix_operation(
        "valid_rolling_min",
        (source, valid, index.offsets, index.rows),
        {"window": int(window)},
        lambda: _valid_rolling_reduce(
            source,
            valid,
            window,
            _VALID_REDUCE_MIN,
            bar_index=index,
        ),
    )


def valid_rolling_max(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    *,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return _cached_matrix_operation(
        "valid_rolling_max",
        (source, valid, index.offsets, index.rows),
        {"window": int(window)},
        lambda: _valid_rolling_reduce(
            source,
            valid,
            window,
            _VALID_REDUCE_MAX,
            bar_index=index,
        ),
    )


def valid_rolling_mean(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    *,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return _cached_matrix_operation(
        "valid_rolling_mean",
        (source, valid, index.offsets, index.rows),
        {"window": int(window)},
        lambda: _valid_rolling_reduce(
            source,
            valid,
            window,
            _VALID_REDUCE_MEAN,
            bar_index=index,
        ),
    )


def valid_rolling_std(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    *,
    ddof: int = 0,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return _cached_matrix_operation(
        "valid_rolling_std",
        (source, valid, index.offsets, index.rows),
        {"window": int(window), "ddof": int(ddof)},
        lambda: _valid_rolling_reduce(
            source,
            valid,
            window,
            _VALID_REDUCE_STD,
            ddof=int(ddof),
            bar_index=index,
        ),
    )


def valid_rolling_sum(
    values: np.ndarray,
    valid_mask: np.ndarray,
    window: int,
    *,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    index = _resolve_valid_bar_index(source, valid, bar_index)
    return _cached_matrix_operation(
        "valid_rolling_sum",
        (source, valid, index.offsets, index.rows),
        {"window": int(window)},
        lambda: _valid_rolling_reduce(
            source,
            valid,
            window,
            _VALID_REDUCE_SUM,
            bar_index=index,
        ),
    )


def rolling_quantile(values: np.ndarray, window: int, quantile: float) -> np.ndarray:
    source = np.asarray(values, dtype=np.float32)
    q = float(quantile)
    if not 0.0 <= q <= 1.0:
        raise ValueError("rolling quantile must be in [0, 1]")
    return _cached_matrix_operation(
        "rolling_quantile",
        (source,),
        {"window": int(window), "quantile": q},
        lambda: _rolling_reduce(
            source,
            window,
            lambda view, axis: np.quantile(view, q, axis=axis),
            materialized_window_budget_bytes=_ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES,
        ),
    )


def ewm_adjust_false(
    values: np.ndarray,
    *,
    span: int | None = None,
    alpha: float | None = None,
) -> np.ndarray:
    """Pandas-compatible EWM mean for ``adjust=False, ignore_na=False``."""
    if alpha is None:
        if span is None or span <= 0:
            raise ValueError("span must be positive when alpha is omitted")
        alpha = 2.0 / (float(span) + 1.0)
    if not 0.0 < float(alpha) <= 1.0:
        raise ValueError("alpha must be in (0, 1]")

    source = np.asarray(values, dtype=np.float32)
    alpha_value = float(alpha)

    def _compute() -> np.ndarray:
        out = np.full(source.shape, np.nan, dtype=np.float32)
        weighted = np.zeros(source.shape[1], dtype=np.float64)
        old_weight = np.ones(source.shape[1], dtype=np.float64)
        initialized = np.zeros(source.shape[1], dtype=bool)
        decay = 1.0 - alpha_value

        for time_id in range(source.shape[0]):
            row = source[time_id].astype(np.float64, copy=False)
            finite = np.isfinite(row)
            continuing = finite & initialized
            starting = finite & ~initialized

            old_weight[initialized] *= decay
            if continuing.any():
                denominator = old_weight[continuing] + alpha_value
                weighted[continuing] = (
                    old_weight[continuing] * weighted[continuing] + alpha_value * row[continuing]
                ) / denominator
                old_weight[continuing] = 1.0
            if starting.any():
                weighted[starting] = row[starting]
                old_weight[starting] = 1.0
                initialized[starting] = True
            out[time_id, initialized] = weighted[initialized].astype(np.float32)
        return out

    return _cached_matrix_operation(
        "ewm_adjust_false",
        (source,),
        {"alpha": alpha_value},
        _compute,
    )


@njit(cache=True, nogil=True, parallel=True)
def _valid_ewm_kernel(
    source: np.ndarray,
    valid: np.ndarray,
    offsets: np.ndarray,
    rows: np.ndarray,
    alpha: float,
) -> np.ndarray:
    out = np.full(source.shape, np.nan, dtype=np.float32)
    decay = 1.0 - alpha
    for asset_id in prange(source.shape[1]):
        start = int(offsets[asset_id])
        stop = int(offsets[asset_id + 1])
        initialized = False
        state = 0.0
        for position in range(start, stop):
            row = int(rows[position])
            value = source[row, asset_id]
            if not valid[row, asset_id] or not np.isfinite(value):
                continue
            if initialized:
                state = decay * state + alpha * float(value)
            else:
                state = float(value)
                initialized = True
            out[row, asset_id] = np.float32(state)
    return out


def valid_ewm_adjust_false(
    values: np.ndarray,
    valid_mask: np.ndarray,
    *,
    span: int | None = None,
    alpha: float | None = None,
    bar_index: ValidBarIndex | None = None,
) -> np.ndarray:
    """Pandas-compatible EWM that advances only on effective observations."""
    if alpha is None:
        if span is None or span <= 0:
            raise ValueError("span must be positive when alpha is omitted")
        alpha = 2.0 / (float(span) + 1.0)
    if not 0.0 < float(alpha) <= 1.0:
        raise ValueError("alpha must be in (0, 1]")
    source = np.asarray(values, dtype=np.float32)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(source)
    if valid.shape != source.shape:
        raise ValueError("valid_ewm mask shape does not match values")
    alpha_value = float(alpha)
    index = _resolve_valid_bar_index(source, valid, bar_index)

    return _cached_matrix_operation(
        "valid_ewm_adjust_false",
        (source, valid, index.offsets, index.rows),
        {"alpha": alpha_value},
        lambda: run_numba_parallel(
            lambda: _valid_ewm_kernel(
                source,
                valid,
                index.offsets,
                index.rows,
                alpha_value,
            )
        ),
    )


def safe_divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    out = np.full(np.broadcast_shapes(numerator.shape, denominator.shape), np.nan, dtype=np.float32)
    np.divide(
        numerator,
        denominator,
        out=out,
        where=np.isfinite(denominator) & (denominator != 0),
    )
    return out


def _rolling_reduce(
    values: np.ndarray,
    window: int,
    reducer: Callable[..., np.ndarray],
    *,
    asset_chunk_size: int = 256,
    materialized_window_budget_bytes: int | None = None,
) -> np.ndarray:
    if window <= 0:
        raise ValueError("rolling window must be positive")
    source = np.asarray(values, dtype=np.float32)
    if source.ndim != 2:
        raise ValueError("matrix rolling features require a 2D array")
    out = np.full(source.shape, np.nan, dtype=np.float32)
    if source.shape[0] < window:
        return out

    if materialized_window_budget_bytes is not None:
        output_rows = source.shape[0] - window + 1
        logical_bytes_per_asset = output_rows * window * source.itemsize
        if logical_bytes_per_asset > 0:
            budget_chunk_size = max(
                1,
                int(materialized_window_budget_bytes) // logical_bytes_per_asset,
            )
            asset_chunk_size = min(asset_chunk_size, budget_chunk_size)

    for start in range(0, source.shape[1], asset_chunk_size):
        stop = min(start + asset_chunk_size, source.shape[1])
        view = np.lib.stride_tricks.sliding_window_view(
            source[:, start:stop],
            window_shape=window,
            axis=0,
        )
        out[window - 1 :, start:stop] = reducer(view, axis=-1).astype(np.float32, copy=False)
    return out
