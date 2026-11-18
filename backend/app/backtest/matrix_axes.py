from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    Any,
    Mapping,
    Path,
    _batch_column,
    _collect_parquet_axes,
    _matrix_filter_expression,
    date,
    hashlib,
    json,
    np,
    os,
    pa,
    pads,
    pl,
    threading,
    uuid,
)


def _matrix_axis_cache_path(
    cache_root: Path,
    parquet_root: Path,
    start: date,
    end: date,
    symbols: tuple[str, ...] | None,
) -> Path:
    payload = json.dumps(
        {
            "root": str(parquet_root.resolve()),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "symbols": symbols,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
    return cache_root / f".axes-v{_root._MATRIX_AXIS_INDEX_VERSION}-{digest}.json"


def _refresh_matrix_axes_from_previous(
    previous: dict[str, Any],
    dataset: pads.Dataset,
    source_partitions: Mapping[str, str],
    symbols: tuple[str, ...] | None,
    filter_expr,
    batch_size: int,
    cancel_event: threading.Event | None,
) -> tuple[list[date], list[str]]:
    """分区指纹变化时增量刷新日期/标的轴，保留旧缓存复用路径。"""
    previous_partitions = previous.get("source_partitions", {})
    changed_labels = {
        value
        for value, fingerprint in source_partitions.items()
        if previous_partitions.get(value) != fingerprint
    }
    removed_labels = set(previous_partitions) - set(source_partitions)
    rewritten_labels = {value for value in changed_labels if value in previous_partitions}
    if removed_labels or rewritten_labels:
        actual_dates, actual_symbols = _collect_parquet_axes(
            dataset, filter_expr, batch_size=batch_size, cancel_event=cancel_event
        )
        changed_labels = set()
        retained_dates = {value.isoformat() for value in actual_dates}
    else:
        retained_dates = {
            str(value)
            for value in previous.get("dates", [])
            if str(value) in source_partitions and str(value) not in changed_labels
        }
        actual_symbols = sorted({str(value) for value in previous.get("symbols", [])})
    if changed_labels:
        changed_dates = [date.fromisoformat(value) for value in sorted(changed_labels)]
        changed_filter = _matrix_filter_expression(
            min(changed_dates), max(changed_dates), symbols
        ) & pads.field("date").isin(changed_dates)
        scanner = dataset.scanner(
            columns=["date", "symbol"],
            filter=changed_filter,
            batch_size=int(batch_size),
            use_threads=True,
        )
        symbols_set = set(actual_symbols)
        for batch in scanner.to_batches():
            _root._raise_if_matrix_cancelled(cancel_event)
            retained_dates.update(
                value.isoformat()
                for value in _root.pc.unique(_batch_column(batch, "date")).to_pylist()
            )
            symbols_set.update(
                str(value) for value in _root.pc.unique(_batch_column(batch, "symbol")).to_pylist()
            )
        actual_symbols = sorted(symbols_set)
    return [date.fromisoformat(value) for value in sorted(retained_dates)], actual_symbols


def _load_or_build_matrix_axes(
    dataset: pads.Dataset,
    parquet_root: Path,
    start: date,
    end: date,
    symbols: tuple[str, ...] | None,
    source_partitions: Mapping[str, str],
    filter_expr,
    *,
    batch_size: int,
    cache_root: Path,
    cancel_event: threading.Event | None = None,
) -> tuple[list[date], list[str]]:
    _root._raise_if_matrix_cancelled(cancel_event)
    path = _matrix_axis_cache_path(cache_root, parquet_root, start, end, symbols)
    previous: dict[str, Any] | None = None
    if path.exists():
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
            if int(
                previous.get("version", -1)
            ) == _root._MATRIX_AXIS_INDEX_VERSION and previous.get("source_partitions") == dict(
                source_partitions
            ):
                return (
                    [date.fromisoformat(value) for value in previous["dates"]],
                    [str(value) for value in previous["symbols"]],
                )
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            previous = None

    if previous is not None:
        actual_dates, actual_symbols = _refresh_matrix_axes_from_previous(
            previous, dataset, source_partitions, symbols, filter_expr, batch_size, cancel_event
        )
    else:
        actual_dates, actual_symbols = _collect_parquet_axes(
            dataset,
            filter_expr,
            batch_size=batch_size,
            cancel_event=cancel_event,
        )

    _root._raise_if_matrix_cancelled(cancel_event)
    payload = {
        "version": _root._MATRIX_AXIS_INDEX_VERSION,
        "source_partitions": dict(source_partitions),
        "dates": [value.isoformat() for value in actual_dates],
        "symbols": list(actual_symbols),
    }
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return actual_dates, actual_symbols


def _arrow_float_values(values: pa.Array, *, null_fill: float) -> np.ndarray:
    casted = _root.pc.cast(values, pa.float32(), safe=False)
    if casted.null_count:
        casted = _root.pc.fill_null(casted, pa.scalar(null_fill, type=pa.float32()))
    result = np.array(
        casted.to_numpy(zero_copy_only=False),
        dtype=np.float32,
        copy=True,
    )
    replacement = np.float32(null_fill)
    result[~np.isfinite(result)] = replacement
    return result


def _arrow_numeric(value_type: pa.DataType) -> bool:
    return bool(
        pa.types.is_integer(value_type)
        or pa.types.is_floating(value_type)
        or pa.types.is_decimal(value_type)
    )


def _instrument_axis_values(
    symbols: list[str],
    wanted_fields: set[str],
    instruments: pl.DataFrame | None,
    *,
    time_count: int,
) -> tuple[list[str], dict[str, np.ndarray], dict[str, np.ndarray]]:
    names = [""] * len(symbols)
    fields: dict[str, np.ndarray] = {}
    limits = {
        "limit_up": np.full(len(symbols), np.nan, dtype=np.float32),
        "limit_down": np.full(len(symbols), np.nan, dtype=np.float32),
    }
    if instruments is None or instruments.is_empty() or "symbol" not in instruments.columns:
        return names, fields, limits

    by_symbol = {
        str(row["symbol"]): row
        for row in instruments.unique(subset=["symbol"]).iter_rows(named=True)
    }
    numeric_fields = [
        name
        for name in sorted(wanted_fields)
        if name in instruments.columns and instruments[name].dtype.is_numeric()
    ]
    vectors = {name: np.full(len(symbols), np.nan, dtype=np.float32) for name in numeric_fields}
    for asset_id, symbol in enumerate(symbols):
        row = by_symbol.get(symbol)
        if row is None:
            continue
        names[asset_id] = str(row.get("name") or "")
        for name, target in vectors.items():
            value = row.get(name)
            if value is not None:
                target[asset_id] = np.float32(value)
        for name, target in limits.items():
            value = row.get(name)
            if value is not None:
                target[asset_id] = np.float32(value)

    shape = (1, len(symbols))
    fields.update(
        {
            name: np.broadcast_to(values.reshape(shape), (time_count, len(symbols)))
            for name, values in vectors.items()
        }
    )
    return names, fields, limits
