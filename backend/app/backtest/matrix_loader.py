from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    Mapping,
    MappingProxyType,
    MarketDataMatrix,
    Path,
    _collect_parquet_axes,
    _matrix_filter_expression,
    dataclass,
    date,
    json,
    logger,
    np,
    pa,
    pads,
    pl,
    shutil,
    threading,
    time,
    uuid,
    write_numpy_price_limit_matrix,
)


@dataclass(frozen=True)
class _MatrixLoadContext:
    root: Path
    effective_start: date
    effective_end: date
    requested_fields: frozenset[str]
    build_start: date
    build_end: date
    build_fields: frozenset[str]
    symbols: tuple[str, ...] | None
    instruments: pl.DataFrame | None
    instrument_fingerprint: str
    dataset: pads.Dataset


def _validate_matrix_load_request(
    parquet_root: Path,
    start: date,
    end: date,
    field_columns: set[str] | frozenset[str],
    symbols: list[str] | None,
    instruments: pl.DataFrame | None,
    coverage_start: date | None,
    coverage_end: date | None,
    cache_field_columns: set[str] | frozenset[str] | None,
) -> _MatrixLoadContext:
    if start > end:
        raise ValueError("matrix parquet range start must not exceed end")
    root = Path(parquet_root)
    if not root.exists():
        raise ValueError(f"matrix parquet root does not exist: {root}")
    available_start, available_end = _root._partition_date_bounds(root)
    if available_start is None or available_end is None:
        raise ValueError("本地指标数据为空，请先在数据页面同步日K并完成指标计算")
    effective_start = max(start, available_start)
    effective_end = min(end, available_end)
    if effective_start > effective_end:
        raise ValueError("matrix parquet range contains no market data")
    requested_fields = _normalize_matrix_cache_fields(field_columns)
    requested_coverage_start = coverage_start or start
    requested_coverage_end = coverage_end or end
    if requested_coverage_start > start or requested_coverage_end < end:
        raise ValueError("matrix cache coverage must include the requested range")
    build_start = max(requested_coverage_start, available_start)
    build_end = min(requested_coverage_end, available_end)
    build_fields = frozenset(
        requested_fields | _normalize_matrix_cache_fields(cache_field_columns or field_columns)
    )
    normalized_symbols = _normalize_symbol_request(symbols)
    instrument_fingerprint = _root._instrument_fingerprint(instruments).hex()
    partitioning = pads.partitioning(
        pa.schema([("date", pa.date32())]),
        flavor="hive",
    )
    dataset = pads.dataset(
        str(root),
        format="parquet",
        partitioning=partitioning,
    )
    _validate_matrix_dataset_schema(dataset)
    return _MatrixLoadContext(
        root=root,
        effective_start=effective_start,
        effective_end=effective_end,
        requested_fields=requested_fields,
        build_start=build_start,
        build_end=build_end,
        build_fields=build_fields,
        symbols=normalized_symbols,
        instruments=instruments,
        instrument_fingerprint=instrument_fingerprint,
        dataset=dataset,
    )


def _load_covering_matrix_cache(
    cache_dir: Path,
    context: _MatrixLoadContext,
    source_generation: str | None,
) -> MarketDataMatrix | None:
    requested_partitions = (
        {}
        if source_generation
        else _root._partition_fingerprints(
            context.root,
            context.effective_start,
            context.effective_end,
            include_predecessor=True,
        )
    )
    covering = _root._find_covering_matrix_cache(
        cache_dir,
        context.root,
        context.effective_start,
        context.effective_end,
        context.requested_fields,
        context.symbols,
        requested_partitions,
        context.instrument_fingerprint,
        source_generation,
    )
    if covering is None:
        return None
    path, status = covering
    try:
        market = _root._load_market_data_matrix_cache(path, cache_status=status)
        return _root._slice_and_project_market_data_matrix(
            market,
            context.effective_start,
            context.effective_end,
            context.requested_fields,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("invalid matrix disk cache %s: %s", path, exc)
        shutil.rmtree(path, ignore_errors=True)
    return None


def _build_or_reuse_matrix_cache(
    cache_dir: Path,
    context: _MatrixLoadContext,
    *,
    batch_size: int,
    cache_max_bytes: int,
    profile_generation: str,
    source_generation: str | None,
    cancel_event: threading.Event | None,
) -> MarketDataMatrix:
    build_partitions = _root._partition_fingerprints(
        context.root, context.build_start, context.build_end
    )
    if not build_partitions:
        raise ValueError("matrix parquet range contains no market data")
    cache_path = _root._matrix_disk_cache_path(
        cache_dir,
        context.root,
        context.build_start,
        context.build_end,
        context.build_fields,
        context.symbols,
        build_partitions,
        context.instrument_fingerprint,
        profile_generation,
        source_generation,
    )
    if (cache_path / "manifest.json").exists():
        market = _root._load_market_data_matrix_cache(cache_path, cache_status="exact")
        return _root._slice_and_project_market_data_matrix(
            market,
            context.effective_start,
            context.effective_end,
            context.requested_fields,
        )
    _build_market_data_matrix_cache_from_dataset(
        context.dataset,
        context.root,
        cache_path,
        context.build_start,
        context.build_end,
        context.build_fields,
        context.symbols,
        context.instruments,
        build_partitions,
        context.instrument_fingerprint,
        profile_generation,
        source_generation,
        batch_size=batch_size,
        axis_cache_root=cache_dir,
        cancel_event=cancel_event,
    )
    _root._raise_if_matrix_cancelled(cancel_event)
    _root._publish_matrix_cache(cache_path, int(cache_max_bytes))
    _root._prune_matrix_disk_cache(
        cache_dir,
        keep=cache_path,
        max_bytes=int(cache_max_bytes),
        current_source_generation=source_generation,
    )
    market = _root._load_market_data_matrix_cache(cache_path, cache_status="built")
    return _root._slice_and_project_market_data_matrix(
        market,
        context.effective_start,
        context.effective_end,
        context.requested_fields,
    )


def _validate_matrix_dataset_schema(dataset: pads.Dataset) -> None:
    available = set(dataset.schema.names)
    required = {"symbol", "date", "open", "high", "low", "close", "volume"}
    missing = required - available
    if missing:
        raise ValueError(f"matrix parquet missing columns: {sorted(missing)}")


def _normalize_matrix_cache_fields(
    field_columns: set[str] | frozenset[str],
) -> frozenset[str]:
    ignored = {
        "symbol",
        "date",
        "name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "signal_limit_up",
        "signal_limit_down",
    }
    return frozenset(str(name) for name in field_columns if str(name) not in ignored)


def _normalize_symbol_request(symbols: list[str] | None) -> tuple[str, ...] | None:
    if symbols is None:
        return None
    return tuple(sorted({str(symbol) for symbol in symbols}))


def _resolve_matrix_storage_fields(
    dataset: pads.Dataset,
    wanted_fields: frozenset[str],
    instruments: pl.DataFrame | None,
) -> tuple[list[str], list[str], list[str]]:
    available = set(dataset.schema.names)
    parquet_fields = sorted(
        name
        for name in wanted_fields
        if name != "price_limit_pct"
        and name in available
        and _root._arrow_numeric(dataset.schema.field(name).type)
    )
    instrument_columns = set(instruments.columns) if instruments is not None else set()
    matrix_fields = set(parquet_fields)
    vector_fields = {
        name
        for name in ("total_shares", "float_shares")
        if name in wanted_fields and name in instrument_columns and name not in parquet_fields
    }
    if "raw_close" in wanted_fields:
        matrix_fields.add("raw_close")
    if "turnover_rate" in wanted_fields:
        matrix_fields.add("turnover_rate")
        if "turnover_rate" not in parquet_fields and "float_shares" in instrument_columns:
            vector_fields.add("float_shares")
    if "price_limit_pct" in wanted_fields:
        matrix_fields.add("price_limit_pct")
    resolved = matrix_fields | vector_fields
    unresolved = wanted_fields - resolved
    if unresolved:
        raise ValueError(f"matrix parquet fields unavailable: {sorted(unresolved)}")
    return parquet_fields, sorted(matrix_fields), sorted(vector_fields)


def _build_dataset_matrix_arrays(
    dataset: pads.Dataset,
    filter_expr,
    actual_dates: list[date],
    actual_symbols: list[str],
    parquet_fields,
    matrix_fields,
    vector_fields,
    wanted_fields,
    instruments: pl.DataFrame | None,
    root: Path,
    batch_size: int,
    cancel_event: threading.Event | None,
):
    """扫描 parquet 并派生矩阵数组、涨跌停锁与名称。"""
    shape = (len(actual_dates), len(actual_symbols))
    arrays = {
        "open": np.full(shape, np.nan, dtype=np.float32),
        "high": np.full(shape, np.nan, dtype=np.float32),
        "low": np.full(shape, np.nan, dtype=np.float32),
        "close": np.full(shape, np.nan, dtype=np.float32),
        "volume": np.zeros(shape, dtype=np.float32),
    }
    fields = {name: np.full(shape, np.nan, dtype=np.float32) for name in matrix_fields}
    seen = np.zeros(shape, dtype=bool)
    _root._scan_matrix_values(
        dataset,
        filter_expr,
        actual_dates,
        actual_symbols,
        arrays,
        fields,
        parquet_fields,
        seen,
        batch_size=batch_size,
        cancel_event=cancel_event,
    )
    _root._raise_if_matrix_cancelled(cancel_event)
    names, latest_limits = _root._populate_matrix_derived_arrays(
        actual_symbols,
        arrays,
        fields,
        wanted_fields,
        instruments,
        seen,
        parquet_fields=parquet_fields,
        vector_fields=vector_fields,
    )
    if "price_limit_pct" in fields:
        write_numpy_price_limit_matrix(
            fields["price_limit_pct"], actual_dates, actual_symbols, names, valid=seen
        )
    for name in vector_fields:
        fields[name] = np.where(seen, fields[name], np.nan).astype(np.float32, copy=False)
    tradable = _root._tradable_matrix(
        arrays["open"], arrays["high"], arrays["low"], arrays["close"], arrays["volume"]
    )
    raw_close = fields.get("raw_close", arrays["close"])
    limit_up_locked, limit_down_locked = _root._limit_lock_matrices(
        arrays["close"],
        raw_close,
        seen,
        actual_dates,
        actual_symbols,
        names,
        latest_limits,
        apply_latest_limits=actual_dates[-1] == _root._latest_partition_date(root),
    )
    return arrays, fields, names, seen, tradable, limit_up_locked, limit_down_locked


def _build_market_data_matrix_from_dataset(
    dataset: pads.Dataset,
    root: Path,
    start: date,
    end: date,
    wanted_fields: frozenset[str],
    symbols: tuple[str, ...] | None,
    instruments: pl.DataFrame | None,
    *,
    batch_size: int,
    cache_status: str,
    cancel_event: threading.Event | None = None,
) -> MarketDataMatrix:
    _root._raise_if_matrix_cancelled(cancel_event)
    filter_expr = _matrix_filter_expression(start, end, symbols)
    actual_dates, actual_symbols = _collect_parquet_axes(
        dataset,
        filter_expr,
        batch_size=batch_size,
        cancel_event=cancel_event,
    )
    if not actual_dates or not actual_symbols:
        raise ValueError("matrix parquet range contains no market data")
    parquet_fields, matrix_fields, vector_fields = _resolve_matrix_storage_fields(
        dataset,
        wanted_fields,
        instruments,
    )
    (
        arrays,
        fields,
        names,
        _seen,
        tradable,
        limit_up_locked,
        limit_down_locked,
    ) = _build_dataset_matrix_arrays(
        dataset,
        filter_expr,
        actual_dates,
        actual_symbols,
        parquet_fields,
        matrix_fields,
        vector_fields,
        wanted_fields,
        instruments,
        root,
        batch_size,
        cancel_event,
    )
    timestamps, session_ids = _root._matrix_time_axes(actual_dates)
    _root._make_read_only(
        timestamps,
        session_ids,
        *arrays.values(),
        tradable,
        limit_up_locked,
        limit_down_locked,
        *fields.values(),
    )
    return MarketDataMatrix(
        timestamps=timestamps,
        timestamp_labels=tuple(value.isoformat() for value in actual_dates),
        session_ids=session_ids,
        symbols=tuple(actual_symbols),
        names=tuple(names),
        open=arrays["open"],
        high=arrays["high"],
        low=arrays["low"],
        close=arrays["close"],
        volume=arrays["volume"],
        tradable=tradable,
        limit_up_locked=limit_up_locked,
        limit_down_locked=limit_down_locked,
        fields=MappingProxyType(fields),
        cache_status=cache_status,
    )


def _build_market_data_matrix_cache_from_dataset(
    dataset: pads.Dataset,
    root: Path,
    cache_path: Path,
    start: date,
    end: date,
    wanted_fields: frozenset[str],
    symbols: tuple[str, ...] | None,
    instruments: pl.DataFrame | None,
    source_partitions: Mapping[str, str],
    instrument_fingerprint: str,
    profile_generation: str,
    source_generation: str | None,
    *,
    batch_size: int,
    axis_cache_root: Path,
    cancel_event: threading.Event | None = None,
) -> None:
    _root._raise_if_matrix_cancelled(cancel_event)
    build_started = time.perf_counter()
    timing_ms: dict[str, float] = {}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = cache_path.parent / f".{cache_path.name}.{uuid.uuid4().hex}.tmp"
    temporary.mkdir()
    mapped: list[np.memmap] = []
    try:
        filter_expr = _matrix_filter_expression(start, end, symbols)
        layout = _root._prepare_matrix_cache_layout(
            dataset,
            root,
            start,
            end,
            symbols,
            source_partitions,
            filter_expr,
            wanted_fields,
            instruments,
            temporary,
            mapped,
            timing_ms,
            batch_size=batch_size,
            axis_cache_root=axis_cache_root,
            cancel_event=cancel_event,
        )

        names = _root._fill_matrix_cache_arrays(
            dataset,
            filter_expr,
            layout,
            wanted_fields,
            instruments,
            root,
            timing_ms,
            batch_size=batch_size,
            cancel_event=cancel_event,
        )

        _root._publish_matrix_cache_snapshot(
            cache_path,
            temporary,
            root,
            start,
            end,
            symbols,
            source_partitions,
            instrument_fingerprint,
            profile_generation,
            source_generation,
            wanted_fields,
            layout,
            names,
            mapped,
            timing_ms,
            build_started,
            cancel_event,
        )
    except BaseException:
        _root._close_matrix_memmaps(mapped)
        shutil.rmtree(temporary, ignore_errors=True)
        raise
