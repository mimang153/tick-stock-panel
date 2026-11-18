from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    MappingProxyType,
    MarketDataMatrix,
    np,
    pl,
)


class RealtimeMarketDataMatrix:
    """Mutable staging buffer for one live asset universe.

    Historical rows are built once. Repeated snapshots for the current bar only
    overwrite the last row; a later timestamp appends one row. Strategies only
    receive read-only views through :meth:`snapshot`.
    """

    def __init__(
        self,
        panel: pl.DataFrame,
        *,
        field_columns: set[str] | frozenset[str],
        build_count: int = 1,
    ) -> None:
        self.field_columns = frozenset(field_columns)
        self.market = _writable_market_copy(
            _root.build_market_data_matrix(panel, field_columns=self.field_columns)
        )
        self.generation = 1
        self.build_count = int(build_count)
        self.update_count = 0

    def update(self, latest_panel: pl.DataFrame) -> None:
        if latest_panel.is_empty():
            raise ValueError("cannot update live matrix from an empty panel")
        timestamp_col = "datetime" if "datetime" in latest_panel.columns else "date"
        if timestamp_col not in latest_panel.columns:
            raise ValueError("live matrix update requires date or datetime")
        if latest_panel[timestamp_col].n_unique() != 1:
            raise ValueError("live matrix update must contain exactly one timestamp")

        latest = _root.build_market_data_matrix(
            latest_panel,
            field_columns=self.field_columns,
        )
        target_ids = _live_target_asset_ids(self.market.symbols, latest.symbols)
        latest_timestamp = int(latest.timestamps[0])
        current_timestamp = int(self.market.timestamps[-1])
        if latest_timestamp < current_timestamp:
            raise ValueError("live matrix update timestamp is older than current snapshot")

        if latest_timestamp == current_timestamp:
            _overwrite_latest_market_row(self.market, latest, target_ids)
        else:
            self.market = _append_market_row(self.market, latest, target_ids)
        self.generation += 1
        self.update_count += 1

    def snapshot(self) -> MarketDataMatrix:
        return _readonly_market_view(self.market)


def _live_target_asset_ids(
    symbols: tuple[str, ...],
    latest_symbols: tuple[str, ...],
) -> np.ndarray:
    symbol_array = np.asarray(symbols)
    latest_array = np.asarray(latest_symbols)
    target_ids = np.searchsorted(symbol_array, latest_array)
    valid = target_ids < len(symbol_array)
    if not valid.all() or not np.array_equal(symbol_array[target_ids], latest_array):
        raise ValueError("live matrix symbol axis changed; rebuild required")
    return target_ids.astype(np.int32, copy=False)


def _market_array_fields() -> tuple[str, ...]:
    return (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "tradable",
        "limit_up_locked",
        "limit_down_locked",
    )


def _writable_market_copy(market: MarketDataMatrix) -> MarketDataMatrix:
    fields = {name: np.array(values, copy=True) for name, values in market.fields.items()}
    return MarketDataMatrix(
        timestamps=np.array(market.timestamps, copy=True),
        timestamp_labels=market.timestamp_labels,
        session_ids=np.array(market.session_ids, copy=True),
        symbols=market.symbols,
        names=market.names,
        open=np.array(market.open, copy=True),
        high=np.array(market.high, copy=True),
        low=np.array(market.low, copy=True),
        close=np.array(market.close, copy=True),
        volume=np.array(market.volume, copy=True),
        tradable=np.array(market.tradable, copy=True),
        limit_up_locked=np.array(market.limit_up_locked, copy=True),
        limit_down_locked=np.array(market.limit_down_locked, copy=True),
        fields=MappingProxyType(fields),
    )


def _readonly_view(values: np.ndarray) -> np.ndarray:
    view = values.view()
    view.flags.writeable = False
    return view


def _readonly_market_view(market: MarketDataMatrix) -> MarketDataMatrix:
    fields = {name: _readonly_view(values) for name, values in market.fields.items()}
    return MarketDataMatrix(
        timestamps=_readonly_view(market.timestamps),
        timestamp_labels=market.timestamp_labels,
        session_ids=_readonly_view(market.session_ids),
        symbols=market.symbols,
        names=market.names,
        open=_readonly_view(market.open),
        high=_readonly_view(market.high),
        low=_readonly_view(market.low),
        close=_readonly_view(market.close),
        volume=_readonly_view(market.volume),
        tradable=_readonly_view(market.tradable),
        limit_up_locked=_readonly_view(market.limit_up_locked),
        limit_down_locked=_readonly_view(market.limit_down_locked),
        fields=MappingProxyType(fields),
    )


def _overwrite_latest_market_row(
    market: MarketDataMatrix,
    latest: MarketDataMatrix,
    target_ids: np.ndarray,
) -> None:
    for name in _market_array_fields():
        getattr(market, name)[-1, target_ids] = getattr(latest, name)[0]
    for name, values in market.fields.items():
        latest_values = latest.fields.get(name)
        if latest_values is None:
            raise ValueError(f"live matrix update missing field: {name}")
        values[-1, target_ids] = latest_values[0]
    object.__setattr__(market, "_valid_bars", None)


def _append_market_row(
    market: MarketDataMatrix,
    latest: MarketDataMatrix,
    target_ids: np.ndarray,
) -> MarketDataMatrix:
    asset_count = len(market.symbols)

    def aligned(values: np.ndarray, fill: float | int) -> np.ndarray:
        row = np.full(asset_count, fill, dtype=values.dtype)
        row[target_ids] = values[0]
        return row

    arrays: dict[str, np.ndarray] = {}
    for name in _market_array_fields():
        source = getattr(latest, name)
        fill = np.nan if np.issubdtype(source.dtype, np.floating) else 0
        arrays[name] = np.concatenate(
            [getattr(market, name), aligned(source, fill)[None, :]],
            axis=0,
        )

    fields: dict[str, np.ndarray] = {}
    for name, old_values in market.fields.items():
        latest_values = latest.fields.get(name)
        if latest_values is None:
            raise ValueError(f"live matrix update missing field: {name}")
        fields[name] = np.concatenate(
            [old_values, aligned(latest_values, np.nan)[None, :]],
            axis=0,
        )

    latest_label = latest.timestamp_labels[0]
    same_session = latest_label[:10] == market.timestamp_labels[-1][:10]
    next_session = int(market.session_ids[-1]) if same_session else int(market.session_ids[-1]) + 1
    return MarketDataMatrix(
        timestamps=np.concatenate([market.timestamps, latest.timestamps[:1]]),
        timestamp_labels=(*market.timestamp_labels, latest_label),
        session_ids=np.concatenate(
            [
                market.session_ids,
                np.array([next_session], dtype=np.int32),
            ]
        ),
        symbols=market.symbols,
        names=market.names,
        fields=MappingProxyType(fields),
        **arrays,  # type: ignore[arg-type]
    )


def _encode_axes(
    panel: pl.DataFrame,
) -> tuple[str, pl.Series, np.ndarray, np.ndarray, np.ndarray]:
    timestamp_col = "datetime" if "datetime" in panel.columns else "date"
    required = {timestamp_col, "symbol", "open", "high", "low", "close"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"MarketDataMatrix missing columns: {sorted(missing)}")

    timestamp_series = panel[timestamp_col]
    unique_timestamps = timestamp_series.unique().sort()
    symbol_series = panel["symbol"].cast(pl.Utf8)
    unique_symbols = symbol_series.unique().sort()
    row_timestamps = timestamp_series.to_numpy()
    timestamp_values = unique_timestamps.to_numpy()
    row_symbols = symbol_series.to_numpy()
    symbol_values = unique_symbols.to_numpy()
    time_id = np.searchsorted(timestamp_values, row_timestamps).astype(np.int32)
    asset_id = np.searchsorted(symbol_values, row_symbols).astype(np.int32)
    keys = time_id.astype(np.int64) * len(symbol_values) + asset_id
    if np.unique(keys).size != len(panel):
        raise ValueError("MarketDataMatrix requires unique timestamp/symbol rows")
    return timestamp_col, unique_timestamps, symbol_values, time_id, asset_id


def _float_matrix(
    panel: pl.DataFrame,
    column: str,
    shape: tuple[int, int],
    time_id: np.ndarray,
    asset_id: np.ndarray,
    default: float = np.nan,
    null_fill: float | None = None,
) -> np.ndarray:
    out = np.full(shape, default, dtype=np.float32)
    if column not in panel.columns:
        return out
    values = np.array(
        panel[column].cast(pl.Float32, strict=False).to_numpy(),
        dtype=np.float32,
        copy=True,
    )
    values[~np.isfinite(values)] = np.nan if null_fill is None else null_fill
    out[time_id, asset_id] = values
    return out


def _timestamp_int64(series: pl.Series) -> np.ndarray:
    if series.dtype == pl.Date:
        return series.cast(pl.Datetime("ms")).cast(pl.Int64).to_numpy()
    if isinstance(series.dtype, pl.Datetime):
        return series.cast(pl.Datetime("ms")).cast(pl.Int64).to_numpy()
    return series.cast(pl.Int64, strict=False).to_numpy()


def _bool_matrix(
    panel: pl.DataFrame,
    column: str,
    shape: tuple[int, int],
    time_id: np.ndarray,
    asset_id: np.ndarray,
) -> np.ndarray:
    out = np.zeros(shape, dtype=np.uint8)
    if column in panel.columns:
        out[time_id, asset_id] = panel[column].fill_null(False).cast(pl.UInt8).to_numpy()
    return out


def _tradable_matrix(
    open_: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    present = _present_matrix(open_, high, low, close, volume)
    valid_ohlc = present & ((open_ > 0) | (high > 0) | (low > 0) | (close > 0))
    max_price = np.array(open_, dtype=np.float32, copy=True)
    min_price = np.array(open_, dtype=np.float32, copy=True)
    for values in (high, low, close):
        np.fmax(max_price, values, out=max_price)
        np.fmin(min_price, values, out=min_price)
    spread = max_price - min_price
    tolerance = np.maximum(np.abs(close) * np.float32(1e-4), np.float32(0.01))
    suspended_zero_volume = ((volume <= 0) | np.isnan(volume)) & (spread <= tolerance)
    return (valid_ohlc & ~suspended_zero_volume).astype(np.uint8)


def _present_matrix(
    open_: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    del volume
    return np.isfinite(open_) | np.isfinite(high) | np.isfinite(low) | np.isfinite(close)


def _normalize_signal(signal: str) -> str:
    return signal if signal.startswith(("signal_", "csg_")) else f"signal_{signal}"


def _signal_code_matrix(
    panel: pl.DataFrame,
    signal_ids: list[str] | None,
    shape: tuple[int, int],
    time_id: np.ndarray,
    asset_id: np.ndarray,
) -> tuple[np.ndarray, tuple[str, ...]]:
    normalized = tuple(_normalize_signal(signal) for signal in (signal_ids or []))
    row_codes = np.full(len(panel), -1, dtype=np.int16)
    for code, column in enumerate(normalized):
        if column not in panel.columns:
            continue
        mask = panel[column].fill_null(False).cast(pl.Boolean).to_numpy()
        row_codes[(row_codes < 0) & mask] = code
    codes = np.full(shape, -1, dtype=np.int16)
    codes[time_id, asset_id] = row_codes
    return codes, normalized


def _scatter_bool_series(
    series: pl.Series | None,
    length: int,
    shape: tuple[int, int],
    time_id: np.ndarray,
    asset_id: np.ndarray,
) -> np.ndarray:
    out = np.zeros(shape, dtype=np.uint8)
    if series is None or len(series) != length:
        return out
    out[time_id, asset_id] = series.fill_null(False).cast(pl.UInt8).to_numpy()
    return out


def _delay_signal_matrix(
    raw: np.ndarray,
    codes: np.ndarray,
    present: np.ndarray,
    delay_bars: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    shape = raw.shape
    output = np.zeros(shape, dtype=np.uint8)
    signal_time = np.full(shape, -1, dtype=np.int32)
    signal_code = np.full(shape, -1, dtype=np.int16)
    for asset_id in range(shape[1]):
        rows = np.flatnonzero(present[:, asset_id])
        if len(rows) <= delay_bars:
            continue
        source_rows = rows[: len(rows) - delay_bars] if delay_bars else rows
        target_rows = rows[delay_bars:] if delay_bars else rows
        active = raw[source_rows, asset_id] != 0
        if not active.any():
            continue
        sources = source_rows[active]
        targets = target_rows[active]
        output[targets, asset_id] = 1
        signal_time[targets, asset_id] = sources.astype(np.int32)
        signal_code[targets, asset_id] = codes[sources, asset_id]
    return output, signal_time, signal_code


def _coerce_array(
    value: np.ndarray | None,
    shape: tuple[int, int],
    dtype: np.dtype | type,
    fill: int | float,
) -> np.ndarray:
    if value is None:
        return np.full(shape, fill, dtype=dtype)
    array = np.asarray(value, dtype=dtype)
    if array.shape != shape:
        raise ValueError(f"matrix shape {array.shape} does not match expected {shape}")
    return np.array(array, dtype=dtype, copy=True)


def _make_read_only(*arrays: np.ndarray) -> None:
    for array in arrays:
        array.flags.writeable = False
