from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    Any,
    Mapping,
    Path,
    dataclass,
    date,
    hashlib,
    json,
    logger,
    np,
    os,
    pads,
    pl,
    shutil,
    threading,
    time,
    write_numpy_price_limit_matrix,
)


@dataclass(frozen=True)
class _MatrixCacheLayout:
    dates: list[date]
    symbols: list[str]
    arrays: dict[str, np.memmap]
    fields: dict[str, np.memmap]
    parquet_fields: list[str]
    vector_fields: list[str]
    array_specs: dict[str, dict[str, Any]]
    field_specs: dict[str, dict[str, Any]]


def _prepare_matrix_cache_layout(
    dataset: pads.Dataset,
    root: Path,
    start: date,
    end: date,
    symbols: tuple[str, ...] | None,
    source_partitions: Mapping[str, str],
    filter_expr,
    wanted_fields: frozenset[str],
    instruments: pl.DataFrame | None,
    temporary: Path,
    mapped: list[np.memmap],
    timing_ms: dict[str, float],
    *,
    batch_size: int,
    axis_cache_root: Path,
    cancel_event: threading.Event | None,
) -> _MatrixCacheLayout:
    stage_started = time.perf_counter()
    actual_dates, actual_symbols = _root._load_or_build_matrix_axes(
        dataset,
        root,
        start,
        end,
        symbols,
        source_partitions,
        filter_expr,
        batch_size=batch_size,
        cache_root=axis_cache_root,
        cancel_event=cancel_event,
    )
    _root._raise_if_matrix_cancelled(cancel_event)
    if not actual_dates or not actual_symbols:
        raise ValueError("matrix parquet range contains no market data")
    timing_ms["axes"] = round((time.perf_counter() - stage_started) * 1000, 1)
    stage_started = time.perf_counter()
    parquet_fields, matrix_fields, vector_fields = _root._resolve_matrix_storage_fields(
        dataset,
        wanted_fields,
        instruments,
    )
    shape = (len(actual_dates), len(actual_symbols))
    array_specs, field_specs, total_bytes = _root._matrix_binary_layout(
        shape,
        matrix_fields,
        vector_fields,
    )
    data_path = temporary / "matrix.bin"
    with data_path.open("wb") as stream:
        stream.truncate(total_bytes)
    arrays = {
        name: _root._open_matrix_memmap(data_path, spec, mapped)
        for name, spec in array_specs.items()
    }
    fields = {
        name: _root._open_matrix_memmap(data_path, spec, mapped)
        for name, spec in field_specs.items()
    }
    timestamps, session_ids = _matrix_time_axes(actual_dates)
    arrays["timestamps"][:] = timestamps
    arrays["session_ids"][:] = session_ids
    timing_ms["layout"] = round((time.perf_counter() - stage_started) * 1000, 1)
    return _MatrixCacheLayout(
        dates=actual_dates,
        symbols=actual_symbols,
        arrays=arrays,
        fields=fields,
        parquet_fields=parquet_fields,
        vector_fields=vector_fields,
        array_specs=array_specs,
        field_specs=field_specs,
    )


def _fill_matrix_cache_arrays(
    dataset: pads.Dataset,
    filter_expr,
    layout: _MatrixCacheLayout,
    wanted_fields: frozenset[str],
    instruments: pl.DataFrame | None,
    root: Path,
    timing_ms: dict[str, float],
    *,
    batch_size: int,
    cancel_event: threading.Event | None,
) -> list[str]:
    arrays, fields = layout.arrays, layout.fields
    stage_started = time.perf_counter()
    seen = np.zeros((len(layout.dates), len(layout.symbols)), dtype=bool)
    _root._scan_matrix_values(
        dataset,
        filter_expr,
        layout.dates,
        layout.symbols,
        arrays,
        fields,
        layout.parquet_fields,
        seen,
        batch_size=batch_size,
        cancel_event=cancel_event,
    )
    _root._raise_if_matrix_cancelled(cancel_event)
    if not seen.any():
        raise ValueError("matrix parquet range contains no requested market data")
    timing_ms["scan"] = round((time.perf_counter() - stage_started) * 1000, 1)
    stage_started = time.perf_counter()
    _root._mask_unseen_staging_core(arrays, fields, layout.parquet_fields, seen)
    names, latest_limits = _populate_matrix_derived_arrays(
        layout.symbols,
        arrays,
        fields,  # type: ignore[arg-type]
        wanted_fields,
        instruments,
        seen,
        parquet_fields=layout.parquet_fields,
        vector_fields=layout.vector_fields,
    )
    _root._mask_unseen_staging_fields(fields, seen)
    _root._raise_if_matrix_cancelled(cancel_event)
    if "price_limit_pct" in fields:
        write_numpy_price_limit_matrix(
            fields["price_limit_pct"],
            layout.dates,
            layout.symbols,
            names,
            valid=seen,
        )
    _write_tradable_matrix(
        arrays["tradable"],
        arrays["open"],
        arrays["high"],
        arrays["low"],
        arrays["close"],
        arrays["volume"],
    )
    _root._limit_lock_matrices(
        arrays["close"],
        fields.get("raw_close", arrays["close"]),
        seen,
        layout.dates,
        layout.symbols,
        names,
        latest_limits,
        out_up=arrays["limit_up_locked"],
        out_down=arrays["limit_down_locked"],
        apply_latest_limits=layout.dates[-1] == _root._latest_partition_date(root),
    )
    timing_ms["derived"] = round((time.perf_counter() - stage_started) * 1000, 1)
    return names


def _publish_matrix_cache_snapshot(
    cache_path: Path,
    temporary: Path,
    root: Path,
    start: date,
    end: date,
    symbols: tuple[str, ...] | None,
    source_partitions: Mapping[str, str],
    instrument_fingerprint: str,
    profile_generation: str,
    source_generation: str | None,
    wanted_fields: frozenset[str],
    layout: _MatrixCacheLayout,
    names: list[str],
    mapped: list[np.memmap],
    timing_ms: dict[str, float],
    build_started: float,
    cancel_event: threading.Event | None,
) -> None:
    _root._raise_if_matrix_cancelled(cancel_event)
    stage_started = time.perf_counter()
    for values in mapped:
        values.flush()
    _root._close_matrix_memmaps(mapped)
    mapped.clear()
    timing_ms["flush_close"] = round((time.perf_counter() - stage_started) * 1000, 1)
    timing_ms["total_before_publish"] = round(
        (time.perf_counter() - build_started) * 1000,
        1,
    )
    manifest = {
        "version": _root._DIRECT_MATRIX_LOADER_VERSION,
        "storage": "matrix.bin",
        "parquet_root": str(root.resolve()),
        "coverage_start": start.isoformat(),
        "coverage_end": end.isoformat(),
        "cache_field_columns": sorted(wanted_fields),
        "symbols_request": None if symbols is None else list(symbols),
        "source_partitions": dict(source_partitions),
        "instrument_fingerprint": instrument_fingerprint,
        "profile_generation": str(profile_generation),
        "source_generation": source_generation,
        "build_timing_ms": timing_ms,
        "timestamp_labels": [value.isoformat() for value in layout.dates],
        "symbols": list(layout.symbols),
        "names": list(names),
        "arrays": layout.array_specs,
        "fields": layout.field_specs,
    }
    (temporary / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    _root._raise_if_matrix_cancelled(cancel_event)
    try:
        os.replace(temporary, cache_path)
    except OSError:
        if (cache_path / "manifest.json").exists():
            shutil.rmtree(temporary, ignore_errors=True)
        else:
            raise


def _populate_matrix_derived_arrays(
    actual_symbols: list[str],
    arrays: Mapping[str, np.ndarray],
    fields: dict[str, np.ndarray],
    wanted_fields: frozenset[str],
    instruments: pl.DataFrame | None,
    seen: np.ndarray,
    *,
    parquet_fields: list[str],
    vector_fields: list[str],
) -> tuple[list[str], Mapping[str, np.ndarray]]:
    instrument_wanted = set(wanted_fields)
    if "turnover_rate" in wanted_fields and "turnover_rate" not in fields:
        instrument_wanted.add("float_shares")
    names, instrument_fields, latest_limits = _root._instrument_axis_values(
        actual_symbols,
        instrument_wanted,
        instruments,
        time_count=seen.shape[0],
    )
    for name, values in instrument_fields.items():
        if name in parquet_fields:
            continue
        if name not in fields:
            fields[name] = values
            continue
        if name in vector_fields:
            fields[name][:] = values[0]
        else:
            np.copyto(fields[name], values, where=seen)
    if "raw_close" in wanted_fields and "raw_close" not in parquet_fields:
        np.copyto(fields["raw_close"], arrays["close"])
    if "turnover_rate" in wanted_fields and "turnover_rate" not in parquet_fields:
        float_shares = fields.get("float_shares")
        if float_shares is None:
            # 非股票资产 (etf/index) 无股本数据: instruments 无 float_shares 列,
            # 也无法从 parquet 读到 turnover_rate (数据源不提供, ETF 无换手率口径)。
            # 此时矩阵中该字段保持全 NaN 列 (matrix_fields 已占位), 与运行期
            # _optional_field 的降级语义一致, 供不需要换手率的策略正常回测。
            # 若本应有股本 (vector_fields 含 float_shares) 却取不到值, 才是数据
            # 异常, 由 _root._resolve_matrix_storage_fields 的 vector 装载路径显式失败。
            if "float_shares" in vector_fields:
                raise ValueError("matrix turnover_rate requires float_shares")
            logger.debug(
                "turnover_rate unavailable (asset has no float_shares); keeping NaN column"
            )
        else:
            _write_turnover_rate_matrix(
                fields["turnover_rate"],
                arrays["volume"],
                float_shares,
            )
    return names, latest_limits


def _write_turnover_rate_matrix(
    target: np.ndarray,
    volume: np.ndarray,
    float_shares: np.ndarray,
) -> None:
    shares = (
        float_shares
        if float_shares.ndim == 2
        else np.broadcast_to(float_shares.reshape(1, -1), volume.shape)
    )
    rows_per_chunk = max(1, (32 * 1024 * 1024) // max(1, volume.shape[1] * 4))
    for start in range(0, volume.shape[0], rows_per_chunk):
        stop = min(volume.shape[0], start + rows_per_chunk)
        out = target[start:stop]
        np.multiply(volume[start:stop], np.float32(10_000.0), out=out)
        shares_chunk = shares[start:stop]
        valid = np.isfinite(shares_chunk) & (shares_chunk != 0)
        np.divide(
            out,
            shares_chunk,
            out=out,
            where=valid,
        )
        out[~valid] = np.nan


def _write_tradable_matrix(
    target: np.ndarray,
    open_: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
) -> None:
    rows_per_chunk = max(1, (32 * 1024 * 1024) // max(1, open_.shape[1] * 4))
    for start in range(0, open_.shape[0], rows_per_chunk):
        stop = min(open_.shape[0], start + rows_per_chunk)
        values = (
            np.isfinite(open_[start:stop])
            & np.isfinite(high[start:stop])
            & np.isfinite(low[start:stop])
            & np.isfinite(close[start:stop])
            & np.isfinite(volume[start:stop])
            & (volume[start:stop] > 0)
        )
        target[start:stop] = values.astype(np.uint8, copy=False)


def _matrix_time_axes(actual_dates: list[date]) -> tuple[np.ndarray, np.ndarray]:
    epoch = date(1970, 1, 1)
    timestamps = np.asarray(
        [(value - epoch).days * 86_400_000 for value in actual_dates],
        dtype=np.int64,
    )
    return timestamps, np.arange(len(actual_dates), dtype=np.int32)


def _matrix_disk_cache_path(
    cache_root: Path,
    parquet_root: Path,
    start: date,
    end: date,
    field_columns: frozenset[str],
    symbols: tuple[str, ...] | None,
    source_partitions: Mapping[str, str],
    instrument_fingerprint: str,
    profile_generation: str,
    source_generation: str | None,
) -> Path:
    payload = {
        "version": _root._DIRECT_MATRIX_LOADER_VERSION,
        "parquet_root": str(parquet_root.resolve()),
        "coverage_start": start.isoformat(),
        "coverage_end": end.isoformat(),
        "fields": sorted(field_columns),
        "symbols": symbols,
        "source_partitions": dict(source_partitions),
        "instrument_fingerprint": instrument_fingerprint,
        "profile_generation": str(profile_generation),
        "source_generation": source_generation,
    }
    digest = hashlib.blake2b(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        digest_size=20,
    )
    return cache_root / f"v{_root._DIRECT_MATRIX_LOADER_VERSION}-{digest.hexdigest()}"
