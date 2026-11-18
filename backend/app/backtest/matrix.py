"""Matrix structures, builders, NumPy features, and matrix-strategy contract."""

from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_axes import (
    _arrow_float_values,
    _arrow_numeric,
    _instrument_axis_values,
    _load_or_build_matrix_axes,
    _matrix_axis_cache_path,
    _refresh_matrix_axes_from_previous,
)
from .matrix_cache_build import (
    _fill_matrix_cache_arrays,
    _matrix_disk_cache_path,
    _matrix_time_axes,
    _MatrixCacheLayout,
    _populate_matrix_derived_arrays,
    _prepare_matrix_cache_layout,
    _publish_matrix_cache_snapshot,
    _write_tradable_matrix,
    _write_turnover_rate_matrix,
)
from .matrix_cache_io import (
    _MATRIX_CACHE_MANIFEST_SUFFIX,
    _close_matrix_memmaps,
    _instrument_fingerprint,
    _latest_partition_date,
    _load_market_data_matrix_cache,
    _mask_unseen_staging_core,
    _mask_unseen_staging_fields,
    _matrix_binary_layout,
    _matrix_cache_store,
    _MatrixDiskCacheLease,
    _open_matrix_memmap,
    _partition_date_bounds,
    _partition_fingerprints,
    _publish_matrix_cache,
    _restore_matrix_cache,
    _slice_and_project_market_data_matrix,
    _sync_matrix_cache_index,
    _try_delete_matrix_cache_path,
)
from .matrix_core import (
    _ACTIVE_MATRIX_CACHE,
    _ACTIVE_VALID_BAR_INDEX,
    _ARROW_BATCH_SIZE,
    _DIRECT_MATRIX_LOADER_VERSION,
    _MATRIX_AXIS_INDEX_VERSION,
    _MATRIX_CACHE_VERSION,
    _MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES,
    _MATRIX_DISK_CACHE_LEASES,
    _MATRIX_DISK_CACHE_LOCK,
    _MATRIX_DISK_CACHE_PENDING_DELETE,
    _ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES,
    _SCORE_ASSET_CHUNK_SIZE,
    SCORING_DIRECTION_LOW,
    Any,
    Callable,
    ContextVar,
    Iterator,
    Mapping,
    MappingProxyType,
    MarketDataMatrix,
    MarketMatrix,
    MatrixCacheProfile,
    MatrixComputeCache,
    MatrixPrewarmCancelledError,
    OrderedDict,
    Path,
    Protocol,
    Sequence,
    SignalMatrix,
    ValidBarIndex,
    _activate_valid_bar_index,
    _arrow_axis_ids,
    _batch_column,
    _build_valid_bar_index,
    _cached_matrix_operation,
    _collect_market_data_matrix_extras,
    _collect_parquet_axes,
    _freeze_cache_value,
    _matrix_filter_expression,
    _raise_if_matrix_cancelled,
    _scan_matrix_values,
    active_matrix_compute_cache,
    annotations,
    build_market_data_matrix,
    build_minute_exit_reference,
    contextmanager,
    dataclass,
    date,
    field,
    hashlib,
    io,
    json,
    load_market_data_matrix_from_parquet,
    logger,
    logging,
    njit,
    np,
    nullcontext,
    numpy_gem_pre_registration_vector,
    numpy_limit_pct_for_date,
    numpy_limit_pct_vectors,
    numpy_limit_price,
    os,
    pa,
    pads,
    pc,
    pl,
    prange,
    run_numba_parallel,
    runtime_checkable,
    shutil,
    suppress,
    threading,
    time,
    uuid,
    weakref,
    write_numpy_price_limit_matrix,
    zipfile,
)
from .matrix_features import (
    _MATRIX_COMPUTED_FEATURES,
    _apply_bound,
    _compute_matrix_feature,
    _matrix_band_feature,
    _matrix_base_feature,
    _matrix_ema,
    _matrix_extended_feature_a,
    _matrix_extended_feature_b,
    _matrix_flow_feature,
    _matrix_ma_feature,
    _matrix_macd,
    _matrix_oscillator_feature,
    _matrix_ratio,
    _matrix_relative,
    _matrix_rolling_corr,
    _matrix_rolling_skew,
    _matrix_stat_feature,
    _matrix_volume_amount_feature,
    _optional_field,
    _registered_matrix_feature,
    _required_field_for_bound,
    _symbol_in_boards,
    _valid_return_over_bars,
    apply_time_masks,
    matrix_feature,
)
from .matrix_loader import (
    _build_dataset_matrix_arrays,
    _build_market_data_matrix_cache_from_dataset,
    _build_market_data_matrix_from_dataset,
    _build_or_reuse_matrix_cache,
    _load_covering_matrix_cache,
    _MatrixLoadContext,
    _normalize_matrix_cache_fields,
    _normalize_symbol_request,
    _resolve_matrix_storage_fields,
    _validate_matrix_dataset_schema,
    _validate_matrix_load_request,
)
from .matrix_ops import (
    _VALID_REDUCE_MAX,
    _VALID_REDUCE_MEAN,
    _VALID_REDUCE_MIN,
    _VALID_REDUCE_STD,
    _VALID_REDUCE_SUM,
    _resolve_valid_bar_index,
    _rolling_reduce,
    _valid_ewm_kernel,
    _valid_rolling_reduce,
    _valid_shift_kernel,
    ewm_adjust_false,
    rolling_max,
    rolling_mean,
    rolling_min,
    rolling_quantile,
    rolling_std,
    rolling_sum,
    safe_divide,
    shift,
    valid_ewm_adjust_false,
    valid_rolling_max,
    valid_rolling_mean,
    valid_rolling_min,
    valid_rolling_std,
    valid_rolling_sum,
    valid_shift,
)
from .matrix_pipeline import (
    MatrixPipelineConfig,
    MatrixStrategy,
    MatrixStrategyPipeline,
    _build_basic_filter_mask_uncached,
    _estimate_pipeline_cache_bytes,
    _score_matrix_by_order,
    build_basic_filter_mask,
    build_matrix_score,
    build_pipeline_filter_mask,
)
from .matrix_realtime import (
    RealtimeMarketDataMatrix,
    _append_market_row,
    _bool_matrix,
    _coerce_array,
    _delay_signal_matrix,
    _encode_axes,
    _float_matrix,
    _live_target_asset_ids,
    _make_read_only,
    _market_array_fields,
    _normalize_signal,
    _overwrite_latest_market_row,
    _present_matrix,
    _readonly_market_view,
    _readonly_view,
    _scatter_bool_series,
    _signal_code_matrix,
    _timestamp_int64,
    _tradable_matrix,
    _writable_market_copy,
)
from .matrix_signals import (
    PanelMatrixEncoding,
    _finalize_signal_matrix,
    _limit_lock_matrices,
    _limit_lock_reference_price,
    _resolve_signal_reference_price,
    build_market_matrix,
    build_market_matrix_from_encoding,
    build_market_matrix_from_signals,
    encode_panel_matrix,
    make_signal_matrix,
    slice_market_data_matrix,
    slice_signal_matrix,
    validate_signal_matrix,
)

__all__ = [
    "SCORING_DIRECTION_LOW",
    "_ACTIVE_MATRIX_CACHE",
    "_ACTIVE_VALID_BAR_INDEX",
    "_ARROW_BATCH_SIZE",
    "_DIRECT_MATRIX_LOADER_VERSION",
    "_MATRIX_AXIS_INDEX_VERSION",
    "_MATRIX_CACHE_MANIFEST_SUFFIX",
    "_MATRIX_CACHE_VERSION",
    "_MATRIX_COMPUTED_FEATURES",
    "_MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES",
    "_MATRIX_DISK_CACHE_LEASES",
    "_MATRIX_DISK_CACHE_LOCK",
    "_MATRIX_DISK_CACHE_PENDING_DELETE",
    "_ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES",
    "_SCORE_ASSET_CHUNK_SIZE",
    "_VALID_REDUCE_MAX",
    "_VALID_REDUCE_MEAN",
    "_VALID_REDUCE_MIN",
    "_VALID_REDUCE_STD",
    "_VALID_REDUCE_SUM",
    "Any",
    "Callable",
    "ContextVar",
    "Iterator",
    "Mapping",
    "MappingProxyType",
    "MarketDataMatrix",
    "MarketMatrix",
    "MatrixCacheProfile",
    "MatrixComputeCache",
    "MatrixPipelineConfig",
    "MatrixPrewarmCancelledError",
    "MatrixStrategy",
    "MatrixStrategyPipeline",
    "OrderedDict",
    "PanelMatrixEncoding",
    "Path",
    "Protocol",
    "RealtimeMarketDataMatrix",
    "Sequence",
    "SignalMatrix",
    "ValidBarIndex",
    "_MatrixCacheLayout",
    "_MatrixDiskCacheLease",
    "_MatrixLoadContext",
    "_activate_valid_bar_index",
    "_append_market_row",
    "_apply_bound",
    "_arrow_axis_ids",
    "_arrow_float_values",
    "_arrow_numeric",
    "_batch_column",
    "_bool_matrix",
    "_build_basic_filter_mask_uncached",
    "_build_dataset_matrix_arrays",
    "_build_market_data_matrix_cache_from_dataset",
    "_build_market_data_matrix_from_dataset",
    "_build_or_reuse_matrix_cache",
    "_build_valid_bar_index",
    "_cached_matrix_operation",
    "_close_matrix_memmaps",
    "_coerce_array",
    "_collect_market_data_matrix_extras",
    "_collect_parquet_axes",
    "_compute_matrix_feature",
    "_delay_signal_matrix",
    "_encode_axes",
    "_estimate_pipeline_cache_bytes",
    "_fill_matrix_cache_arrays",
    "_finalize_signal_matrix",
    "_find_covering_matrix_cache",
    "_float_matrix",
    "_freeze_cache_value",
    "_instrument_axis_values",
    "_instrument_fingerprint",
    "_latest_partition_date",
    "_limit_lock_matrices",
    "_limit_lock_reference_price",
    "_live_target_asset_ids",
    "_load_covering_matrix_cache",
    "_load_market_data_matrix_cache",
    "_load_or_build_matrix_axes",
    "_make_read_only",
    "_market_array_fields",
    "_mask_unseen_staging_core",
    "_mask_unseen_staging_fields",
    "_matrix_axis_cache_path",
    "_matrix_band_feature",
    "_matrix_base_feature",
    "_matrix_binary_layout",
    "_matrix_cache_store",
    "_matrix_disk_cache_path",
    "_matrix_ema",
    "_matrix_extended_feature_a",
    "_matrix_extended_feature_b",
    "_matrix_filter_expression",
    "_matrix_flow_feature",
    "_matrix_ma_feature",
    "_matrix_macd",
    "_matrix_oscillator_feature",
    "_matrix_ratio",
    "_matrix_relative",
    "_matrix_rolling_corr",
    "_matrix_rolling_skew",
    "_matrix_stat_feature",
    "_matrix_time_axes",
    "_matrix_volume_amount_feature",
    "_normalize_matrix_cache_fields",
    "_normalize_signal",
    "_normalize_symbol_request",
    "_open_matrix_memmap",
    "_optional_field",
    "_overwrite_latest_market_row",
    "_partition_date_bounds",
    "_partition_fingerprints",
    "_populate_matrix_derived_arrays",
    "_prepare_matrix_cache_layout",
    "_present_matrix",
    "_prune_matrix_disk_cache",
    "_publish_matrix_cache",
    "_publish_matrix_cache_snapshot",
    "_raise_if_matrix_cancelled",
    "_readonly_market_view",
    "_readonly_view",
    "_refresh_matrix_axes_from_previous",
    "_registered_matrix_feature",
    "_required_field_for_bound",
    "_resolve_matrix_storage_fields",
    "_resolve_signal_reference_price",
    "_resolve_valid_bar_index",
    "_restore_matrix_cache",
    "_rolling_reduce",
    "_scan_matrix_values",
    "_scatter_bool_series",
    "_score_matrix_by_order",
    "_signal_code_matrix",
    "_slice_and_project_market_data_matrix",
    "_symbol_in_boards",
    "_sync_matrix_cache_index",
    "_timestamp_int64",
    "_tradable_matrix",
    "_try_delete_matrix_cache_path",
    "_valid_ewm_kernel",
    "_valid_return_over_bars",
    "_valid_rolling_kernel",
    "_valid_rolling_reduce",
    "_valid_shift_kernel",
    "_validate_matrix_dataset_schema",
    "_validate_matrix_load_request",
    "_writable_market_copy",
    "_write_tradable_matrix",
    "_write_turnover_rate_matrix",
    "active_matrix_compute_cache",
    "annotations",
    "apply_time_masks",
    "build_basic_filter_mask",
    "build_market_data_matrix",
    "build_market_matrix",
    "build_market_matrix_from_encoding",
    "build_market_matrix_from_signals",
    "build_matrix_score",
    "build_minute_exit_reference",
    "build_pipeline_filter_mask",
    "contextmanager",
    "dataclass",
    "date",
    "encode_panel_matrix",
    "ewm_adjust_false",
    "field",
    "hashlib",
    "io",
    "json",
    "load_market_data_matrix_from_parquet",
    "logger",
    "logging",
    "make_signal_matrix",
    "matrix_feature",
    "njit",
    "np",
    "nullcontext",
    "numpy_gem_pre_registration_vector",
    "numpy_limit_pct_for_date",
    "numpy_limit_pct_vectors",
    "numpy_limit_price",
    "os",
    "pa",
    "pads",
    "pc",
    "pl",
    "prange",
    "rolling_max",
    "rolling_mean",
    "rolling_min",
    "rolling_quantile",
    "rolling_std",
    "rolling_sum",
    "run_numba_parallel",
    "runtime_checkable",
    "safe_divide",
    "shift",
    "shutil",
    "slice_market_data_matrix",
    "slice_signal_matrix",
    "suppress",
    "threading",
    "time",
    "uuid",
    "valid_ewm_adjust_false",
    "valid_rolling_max",
    "valid_rolling_mean",
    "valid_rolling_min",
    "valid_rolling_std",
    "valid_rolling_sum",
    "valid_shift",
    "validate_signal_matrix",
    "weakref",
    "write_numpy_price_limit_matrix",
    "zipfile",
]


# ---- 高复杂度存量函数：保留在原模块，用带理由的行内豁免显式承认 ----


def _prune_matrix_disk_cache(  # noqa: C901 - 磁盘缓存淘汰含多档保留条件，拆分会改变清理顺序
    root: Path,
    *,
    keep: Path,
    max_bytes: int,
    current_source_generation: str | None = None,
) -> None:
    if max_bytes <= 0:
        raise ValueError("matrix disk cache max_bytes must be positive")
    with _MATRIX_DISK_CACHE_LOCK:
        pending = [Path(value) for value in _MATRIX_DISK_CACHE_PENDING_DELETE]
    for path in pending:
        with _MATRIX_DISK_CACHE_LOCK:
            leased = _MATRIX_DISK_CACHE_LEASES.get(str(path), 0) > 0
        if not leased:
            _root._try_delete_matrix_cache_path(path)

    entries: list[tuple[Path, int, int]] = []
    for path in root.glob("v*-*"):
        if not path.is_dir():
            continue
        try:
            size = sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
            entries.append((path, size, path.stat().st_mtime_ns))
        except OSError:
            continue
    if current_source_generation is not None:
        try:
            keep_manifest = json.loads((keep / "manifest.json").read_text(encoding="utf-8"))
            keep_parquet_root = keep_manifest.get("parquet_root")
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            keep_parquet_root = None
        for path, _size, _mtime in entries:
            if path == keep:
                continue
            try:
                manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
                same_universe = (
                    keep_parquet_root is not None
                    and manifest.get("parquet_root") == keep_parquet_root
                    and manifest.get("symbols_request") is None
                )
                old_generation = manifest.get("source_generation") != current_source_generation
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                same_universe = False
                old_generation = False
            if not (same_universe and old_generation):
                continue
            with _MATRIX_DISK_CACHE_LOCK:
                leased = _MATRIX_DISK_CACHE_LEASES.get(str(path), 0) > 0
                if leased:
                    _MATRIX_DISK_CACHE_PENDING_DELETE.add(str(path))
            if not leased:
                _root._try_delete_matrix_cache_path(path)
        entries = [entry for entry in entries if entry[0] == keep or entry[0].exists()]
    total = sum(size for _path, size, _mtime in entries)
    for path, size, _mtime in sorted(entries, key=lambda item: item[2]):
        if total <= max_bytes:
            break
        if path == keep:
            continue
        with _MATRIX_DISK_CACHE_LOCK:
            leased = _MATRIX_DISK_CACHE_LEASES.get(str(path), 0) > 0
            if leased:
                _MATRIX_DISK_CACHE_PENDING_DELETE.add(str(path))
        if leased:
            continue
        if _root._try_delete_matrix_cache_path(path):
            total -= size


def _find_covering_matrix_cache(  # noqa: C901 - 覆盖缓存匹配多分支，保持与原实现一致的优先级
    root: Path,
    parquet_root: Path,
    start: date,
    end: date,
    requested_fields: frozenset[str],
    symbols: tuple[str, ...] | None,
    source_partitions: Mapping[str, str],
    instrument_fingerprint: str,
    source_generation: str | None,
) -> tuple[Path, str] | None:
    matches: list[tuple[int, int, Path, str]] = []
    for path in root.glob(f"v{_root._DIRECT_MATRIX_LOADER_VERSION}-*"):
        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if int(manifest.get("version", -1)) != _root._DIRECT_MATRIX_LOADER_VERSION:
                continue
            if manifest.get("parquet_root") != str(parquet_root.resolve()):
                continue
            if manifest.get("symbols_request") != (None if symbols is None else list(symbols)):
                continue
            if manifest.get("instrument_fingerprint") != instrument_fingerprint:
                continue
            if source_generation is not None:  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
                if manifest.get("source_generation") != source_generation:
                    continue
            cached_start = date.fromisoformat(str(manifest["coverage_start"]))
            cached_end = date.fromisoformat(str(manifest["coverage_end"]))
            if cached_start > start or cached_end < end:
                continue
            cached_fields = frozenset(str(name) for name in manifest["cache_field_columns"])
            if not requested_fields.issubset(cached_fields):
                continue
            cached_partitions = manifest.get("source_partitions", {})
            if source_generation is None:
                relevant_partitions = {
                    key: value
                    for key, value in source_partitions.items()
                    if date.fromisoformat(key) >= cached_start
                }
                if any(
                    cached_partitions.get(key) != value
                    for key, value in relevant_partitions.items()
                ):
                    continue
            storage = path / str(manifest.get("storage", "matrix.bin"))
            # 本地可能只有从 CK 同步来的 manifest（本体未下载），用上传时记录的大小兜底
            size = (
                storage.stat().st_size
                if storage.exists()
                else int(manifest.get("_ck_payload_size", 0))
            )
            exact = (
                cached_start == start and cached_end == end and cached_fields == requested_fields
            )
            matches.append((size, -path.stat().st_mtime_ns, path, "exact" if exact else "covering"))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    if not matches:
        return None
    _size, _mtime, path, status = min(matches)
    return path, status


@njit(cache=True, nogil=True, parallel=True)
def _valid_rolling_kernel(  # noqa: C901 - numba 内核多分支，拆分影响 JIT 编译与向量化
    source: np.ndarray,
    valid: np.ndarray,
    offsets: np.ndarray,
    rows: np.ndarray,
    window: int,
    operation: int,
    ddof: int,
) -> np.ndarray:
    out = np.full(source.shape, np.nan, dtype=np.float32)
    window_value = float(window)
    denominator = float(window - ddof)
    for asset_id in prange(source.shape[1]):
        start = int(offsets[asset_id])
        stop = int(offsets[asset_id + 1])
        ring = np.empty(window, dtype=np.float32)
        seen = 0
        for position in range(start, stop):
            row = int(rows[position])
            value = source[row, asset_id]
            if not valid[row, asset_id] or not np.isfinite(value):
                continue
            ring[seen % window] = value
            seen += 1
            if seen < window:
                continue
            first = seen - window
            if operation == _VALID_REDUCE_MIN:
                result = ring[first % window]
                for offset in range(1, window):
                    candidate = ring[(first + offset) % window]
                    if candidate < result:
                        result = candidate
                out[row, asset_id] = result
            elif operation == _VALID_REDUCE_MAX:
                result = ring[first % window]
                for offset in range(1, window):
                    candidate = ring[(first + offset) % window]
                    if candidate > result:
                        result = candidate
                out[row, asset_id] = result
            else:
                total = 0.0
                for offset in range(window):
                    total += float(ring[(first + offset) % window])
                mean = total / window_value
                if operation == _VALID_REDUCE_MEAN:
                    out[row, asset_id] = mean
                elif operation == _VALID_REDUCE_SUM:
                    out[row, asset_id] = total
                else:
                    squared = 0.0
                    for offset in range(window):
                        delta = float(ring[(first + offset) % window]) - mean
                        squared += delta * delta
                    out[row, asset_id] = np.sqrt(squared / denominator)
    return out
