"""Matrix structures, builders, NumPy features, and matrix-strategy contract."""

from __future__ import annotations

import app.backtest.matrix as _root

__all__ = [
    "SCORING_DIRECTION_LOW",
    "_ACTIVE_MATRIX_CACHE",
    "_ACTIVE_VALID_BAR_INDEX",
    "_ARROW_BATCH_SIZE",
    "_DIRECT_MATRIX_LOADER_VERSION",
    "_MATRIX_AXIS_INDEX_VERSION",
    "_MATRIX_CACHE_VERSION",
    "_MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES",
    "_MATRIX_DISK_CACHE_LEASES",
    "_MATRIX_DISK_CACHE_LOCK",
    "_MATRIX_DISK_CACHE_PENDING_DELETE",
    "_ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES",
    "_SCORE_ASSET_CHUNK_SIZE",
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
    "MatrixPrewarmCancelledError",
    "OrderedDict",
    "Path",
    "Protocol",
    "Sequence",
    "SignalMatrix",
    "ValidBarIndex",
    "_activate_valid_bar_index",
    "_arrow_axis_ids",
    "_batch_column",
    "_build_valid_bar_index",
    "_cached_matrix_operation",
    "_collect_market_data_matrix_extras",
    "_collect_parquet_axes",
    "_freeze_cache_value",
    "_matrix_filter_expression",
    "_raise_if_matrix_cancelled",
    "_scan_matrix_values",
    "active_matrix_compute_cache",
    "build_market_data_matrix",
    "build_minute_exit_reference",
    "contextmanager",
    "dataclass",
    "date",
    "field",
    "hashlib",
    "io",
    "json",
    "load_market_data_matrix_from_parquet",
    "logger",
    "logging",
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
    "run_numba_parallel",
    "runtime_checkable",
    "shutil",
    "suppress",
    "threading",
    "time",
    "uuid",
    "weakref",
    "write_numpy_price_limit_matrix",
    "zipfile",
]

import hashlib
import io
import json
import logging
import os
import shutil
import threading
import time
import uuid
import weakref
import zipfile
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

from app.backtest.minute_trigger import build_minute_exit_reference
from app.backtest.numba_runtime import run_numba_parallel
from app.price_limits import (
    numpy_gem_pre_registration_vector,
    numpy_limit_pct_for_date,
    numpy_limit_pct_vectors,
    numpy_limit_price,
    write_numpy_price_limit_matrix,
)
from app.strategy.scoring import SCORING_DIRECTION_LOW

try:
    from numba import njit, prange
except ImportError:

    def njit(*args, **kwargs):
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return args[0]

        def decorator(func):
            return func

        return decorator

    prange = range

_MATRIX_CACHE_VERSION = 1
# Price-limit rules are baked into the mmap fields and lock arrays.
_DIRECT_MATRIX_LOADER_VERSION = 5
_MATRIX_AXIS_INDEX_VERSION = 1
_ARROW_BATCH_SIZE = 131_072
_SCORE_ASSET_CHUNK_SIZE = 256
_ROLLING_MATERIALIZED_WINDOW_BUDGET_BYTES = 32 * 1024 * 1024
_MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES = 512 * 1024 * 1024

logger = logging.getLogger("app.backtest.matrix")


class MatrixPrewarmCancelledError(RuntimeError):
    """A matrix cache prewarm was cancelled during application shutdown."""


def _raise_if_matrix_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise MatrixPrewarmCancelledError("matrix cache prewarm cancelled")


_MATRIX_DISK_CACHE_LOCK = threading.RLock()
_MATRIX_DISK_CACHE_LEASES: dict[str, int] = {}
_MATRIX_DISK_CACHE_PENDING_DELETE: set[str] = set()
_ACTIVE_MATRIX_CACHE: ContextVar[MatrixComputeCache | None] = ContextVar(
    "active_matrix_compute_cache",
    default=None,
)
_ACTIVE_VALID_BAR_INDEX: ContextVar[Any] = ContextVar(
    "active_valid_bar_index",
    default=None,
)


def _freeze_cache_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, dict):
        return tuple(
            (str(key), _freeze_cache_value(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
    if isinstance(value, list | tuple):
        return tuple(_freeze_cache_value(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted((_freeze_cache_value(item) for item in value), key=repr))
    if isinstance(value, float):
        if np.isnan(value):
            return ("float", "nan")
        if np.isposinf(value):
            return ("float", "inf")
        if np.isneginf(value):
            return ("float", "-inf")
        return ("float", value)
    if isinstance(value, str | int | bool | bytes | type(None)):
        return value
    return (type(value).__qualname__, repr(value))


class MatrixComputeCache:
    """Job-scoped byte-bounded cache for deterministic matrix operations."""

    def __init__(
        self,
        *,
        max_bytes: int = 512 * 1024 * 1024,
        max_item_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("matrix cache max_bytes must be positive")
        if max_item_bytes <= 0:
            raise ValueError("matrix cache max_item_bytes must be positive")
        self.max_bytes = int(max_bytes)
        self.max_item_bytes = min(int(max_item_bytes), self.max_bytes)
        self._entries: OrderedDict[tuple, np.ndarray] = OrderedDict()
        self._lineage: dict[int, tuple[weakref.ReferenceType[np.ndarray], tuple]] = {}
        self._market_tokens: dict[int, tuple[MarketDataMatrix, tuple]] = {}
        self._market_counter = 0
        self._lock = threading.RLock()
        self._closed = False
        self._current_bytes = 0
        self._peak_bytes = 0
        self._calls = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._skipped = 0
        self._fingerprint_bytes = 0
        self._fingerprint_ms = 0.0
        self._operations: dict[str, dict[str, int | float]] = {}

    @contextmanager
    def activate(self, market: MarketDataMatrix) -> Iterator[MatrixComputeCache]:
        self.register_market(market)
        token = _ACTIVE_MATRIX_CACHE.set(self)
        try:
            yield self
        finally:
            _ACTIVE_MATRIX_CACHE.reset(token)

    def register_market(self, market: MarketDataMatrix) -> tuple:
        self._ensure_open()
        market_id = id(market)
        with self._lock:
            existing = self._market_tokens.get(market_id)
            if existing is not None and existing[0] is market:
                return existing[1]
            self._market_counter += 1
            token = ("market", self._market_counter)
            self._market_tokens[market_id] = (market, token)
            arrays = {
                "timestamps": market.timestamps,
                "session_ids": market.session_ids,
                "valid_bar_offsets": market.valid_bars.offsets,
                "valid_bar_rows": market.valid_bars.rows,
                "open": market.open,
                "high": market.high,
                "low": market.low,
                "close": market.close,
                "volume": market.volume,
                "tradable": market.tradable,
                "limit_up_locked": market.limit_up_locked,
                "limit_down_locked": market.limit_down_locked,
                **{f"field:{name}": values for name, values in market.fields.items()},
            }
            for name, values in arrays.items():
                self._register_lineage(values, (token, name))
            return token

    @contextmanager
    def suspend(self) -> Iterator[None]:
        """Temporarily bypass this cache without changing its retained entries."""
        token = _ACTIVE_MATRIX_CACHE.set(None)
        try:
            yield
        finally:
            _ACTIVE_MATRIX_CACHE.reset(token)

    def market_token(self, market: MarketDataMatrix) -> tuple:
        return self.register_market(market)

    def get_or_compute(
        self,
        operation: str,
        inputs: tuple[np.ndarray, ...],
        params: Any,
        compute: Callable[[], np.ndarray],
        *,
        key_parts: Any = (),
    ) -> np.ndarray:
        self._ensure_open()
        input_tokens = tuple(self._array_token(values) for values in inputs)
        key = (
            _MATRIX_CACHE_VERSION,
            str(operation),
            input_tokens,
            _freeze_cache_value(params),
            _freeze_cache_value(key_parts),
        )
        with self._lock:
            self._calls += 1
            op_stats = self._operations.setdefault(
                str(operation),
                {
                    "calls": 0,
                    "hits": 0,
                    "misses": 0,
                    "compute_ms": 0.0,
                    "computed_bytes": 0,
                },
            )
            op_stats["calls"] += 1
            cached = self._entries.get(key)
            if cached is not None:
                self._hits += 1
                op_stats["hits"] += 1
                self._entries.move_to_end(key)
                return cached
            self._misses += 1
            op_stats["misses"] += 1

        compute_started = time.perf_counter()
        result = np.asarray(compute())
        compute_ms = (time.perf_counter() - compute_started) * 1000.0
        with self._lock:
            op_stats = self._operations[str(operation)]
            op_stats["compute_ms"] = float(op_stats["compute_ms"]) + compute_ms
            op_stats["computed_bytes"] = int(op_stats["computed_bytes"]) + int(result.nbytes)
        if result.ndim == 0:
            raise ValueError(f"cached matrix operation {operation} returned a scalar")
        if result.flags.writeable:
            result.flags.writeable = False
        if result.nbytes > self.max_item_bytes or result.nbytes > self.max_bytes:
            with self._lock:
                self._skipped += 1
            return result

        with self._lock:
            existing = self._entries.get(key)
            if existing is not None:
                self._entries.move_to_end(key)
                return existing
            self._evict_for(result.nbytes)
            self._entries[key] = result
            self._current_bytes += int(result.nbytes)
            self._peak_bytes = max(self._peak_bytes, self._current_bytes)
            derived = hashlib.blake2b(repr(key).encode("utf-8"), digest_size=16).digest()
            self._register_lineage(result, ("derived", derived))
        return result

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            hit_rate = self._hits / self._calls if self._calls else 0.0
            return {
                "enabled": True,
                "max_bytes": self.max_bytes,
                "max_item_bytes": self.max_item_bytes,
                "current_bytes": self._current_bytes,
                "peak_bytes": self._peak_bytes,
                "entries": len(self._entries),
                "calls": self._calls,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "skipped": self._skipped,
                "hit_rate": round(float(hit_rate), 6),
                "fingerprint_bytes": self._fingerprint_bytes,
                "fingerprint_ms": round(self._fingerprint_ms, 3),
                "operations": {
                    name: {
                        **values,
                        "compute_ms": round(float(values["compute_ms"]), 3),
                    }
                    for name, values in sorted(self._operations.items())
                },
            }

    @property
    def current_bytes(self) -> int:
        with self._lock:
            return int(self._current_bytes)

    def has_cached_operation(self, operation: str) -> bool:
        with self._lock:
            return any(key[1] == operation for key in self._entries)

    def close(self) -> None:
        with self._lock:
            self._entries.clear()
            self._lineage.clear()
            self._market_tokens.clear()
            self._current_bytes = 0
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("matrix compute cache is closed")

    def _array_token(self, values: np.ndarray) -> tuple:
        array = np.asarray(values)
        with self._lock:
            existing = self._lineage.get(id(array))
            if existing is not None and existing[0]() is array:
                return existing[1]

        contiguous = array if array.flags.c_contiguous else np.ascontiguousarray(array)
        started = time.perf_counter()
        digest = hashlib.blake2b(contiguous.view(np.uint8), digest_size=16).digest()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with self._lock:
            self._fingerprint_bytes += int(contiguous.nbytes)
            self._fingerprint_ms += elapsed_ms
        return ("content", array.dtype.str, tuple(array.shape), digest)

    def _register_lineage(self, array: np.ndarray, token: tuple) -> None:
        array_id = id(array)

        def _remove(reference: weakref.ReferenceType[np.ndarray]) -> None:
            with self._lock:
                current = self._lineage.get(array_id)
                if current is not None and current[0] is reference:
                    self._lineage.pop(array_id, None)

        reference = weakref.ref(array, _remove)
        self._lineage[array_id] = (reference, token)

    def _evict_for(self, incoming_bytes: int) -> None:
        while self._entries and self._current_bytes + incoming_bytes > self.max_bytes:
            _, evicted = self._entries.popitem(last=False)
            self._current_bytes -= int(evicted.nbytes)
            self._evictions += 1


def active_matrix_compute_cache() -> MatrixComputeCache | None:
    return _ACTIVE_MATRIX_CACHE.get()


@contextmanager
def _activate_valid_bar_index(index: ValidBarIndex) -> Iterator[None]:
    token = _ACTIVE_VALID_BAR_INDEX.set(index)
    try:
        yield
    finally:
        _ACTIVE_VALID_BAR_INDEX.reset(token)


def _cached_matrix_operation(
    operation: str,
    inputs: tuple[np.ndarray, ...],
    params: Any,
    compute: Callable[[], np.ndarray],
    *,
    key_parts: Any = (),
) -> np.ndarray:
    cache = active_matrix_compute_cache()
    if cache is None:
        return compute()
    return cache.get_or_compute(
        operation,
        inputs,
        params,
        compute,
        key_parts=key_parts,
    )


@dataclass(frozen=True)
class MatrixCacheProfile:
    """Shared disk-cache boundary for one matrix-native asset universe."""

    field_columns: frozenset[str]
    warmup_bars: int
    forward_bars: int
    max_disk_bytes: int = _MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES
    generation: str = "default"


@dataclass(frozen=True)
class ValidBarIndex:
    """Asset-major CSR index of effective market bars."""

    shape: tuple[int, int]
    offsets: np.ndarray
    rows: np.ndarray

    @property
    def nbytes(self) -> int:
        return int(self.offsets.nbytes + self.rows.nbytes)


def _build_valid_bar_index(valid_mask: np.ndarray) -> ValidBarIndex:
    valid = np.asarray(valid_mask, dtype=bool)
    if valid.ndim != 2:
        raise ValueError("valid bar index requires a 2D mask")
    counts = np.count_nonzero(valid, axis=0).astype(np.int64, copy=False)
    offsets = np.empty(valid.shape[1] + 1, dtype=np.int64)
    offsets[0] = 0
    np.cumsum(counts, out=offsets[1:])
    rows = np.empty(int(offsets[-1]), dtype=np.int32)
    for asset_id in range(valid.shape[1]):
        start = int(offsets[asset_id])
        stop = int(offsets[asset_id + 1])
        rows[start:stop] = np.flatnonzero(valid[:, asset_id]).astype(
            np.int32,
            copy=False,
        )
    offsets.flags.writeable = False
    rows.flags.writeable = False
    return ValidBarIndex(shape=valid.shape, offsets=offsets, rows=rows)


@dataclass(frozen=True)
class MarketDataMatrix:
    """Compact base market data shared by matrix-native strategies and matchers."""

    timestamps: np.ndarray
    timestamp_labels: tuple[str, ...]
    session_ids: np.ndarray
    symbols: tuple[str, ...]
    names: tuple[str, ...]

    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    tradable: np.ndarray
    limit_up_locked: np.ndarray
    limit_down_locked: np.ndarray
    fields: Mapping[str, np.ndarray]
    cache_status: str = "memory"
    cache_path: str | None = None
    cache_lease: Any | None = field(default=None, compare=False, repr=False)
    vector_fields: frozenset[str] = field(default_factory=frozenset)
    cache_timing_ms: Mapping[str, float] = field(default_factory=dict)
    _valid_bars: ValidBarIndex | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    @property
    def shape(self) -> tuple[int, int]:
        return self.open.shape

    @property
    def nbytes(self) -> int:
        arrays = [
            self.timestamps,
            self.session_ids,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.tradable,
            self.limit_up_locked,
            self.limit_down_locked,
            *self.fields.values(),
        ]
        index_bytes = self._valid_bars.nbytes if self._valid_bars is not None else 0
        return int(sum(array.nbytes for array in arrays) + index_bytes)

    @property
    def valid_bars(self) -> ValidBarIndex:
        index = self._valid_bars
        if index is None:
            index = _root._build_valid_bar_index(np.isfinite(self.close))
            object.__setattr__(self, "_valid_bars", index)
        return index

    def field(self, name: str) -> np.ndarray:
        if name == "open":
            return self.open
        if name == "high":
            return self.high
        if name == "low":
            return self.low
        if name == "close":
            return self.close
        if name == "volume":
            return self.volume
        try:
            return self.fields[name]
        except KeyError as exc:
            raise ValueError(f"MarketDataMatrix missing field: {name}") from exc


@dataclass(frozen=True)
class SignalMatrix:
    """Strategy output before execution delays are applied."""

    entry: np.ndarray
    exit: np.ndarray
    score: np.ndarray
    entry_signal_code: np.ndarray
    exit_signal_code: np.ndarray
    entry_signal_ids: tuple[str, ...] = ()
    exit_signal_ids: tuple[str, ...] = ()

    @property
    def shape(self) -> tuple[int, int]:
        return self.entry.shape

    @property
    def nbytes(self) -> int:
        return int(
            sum(value.nbytes for value in self.__dict__.values() if isinstance(value, np.ndarray))
        )


@dataclass(frozen=True)
class MarketMatrix:
    """Execution matrix consumed by the Python matcher and future Numba kernel."""

    timestamps: np.ndarray
    timestamp_labels: tuple[str, ...]
    session_ids: np.ndarray
    symbols: tuple[str, ...]
    names: tuple[str, ...]

    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    score: np.ndarray
    entry: np.ndarray
    exit: np.ndarray
    tradable: np.ndarray
    limit_up_locked: np.ndarray
    limit_down_locked: np.ndarray
    reference_price: np.ndarray

    entry_signal_time: np.ndarray
    exit_signal_time: np.ndarray
    entry_signal_code: np.ndarray
    exit_signal_code: np.ndarray
    entry_signal_ids: tuple[str, ...]
    exit_signal_ids: tuple[str, ...]
    # 逐格入场价覆盖 (time x asset, NaN=回退 open/close 惯例)。分钟策略回测用:
    # 信号在盘中第 m 根触发, 入场价 = 触发分钟收盘价, 而非当日开盘/收盘。
    entry_price: np.ndarray | None = None

    @property
    def shape(self) -> tuple[int, int]:
        return self.open.shape

    @property
    def nbytes(self) -> int:
        return int(
            sum(value.nbytes for value in self.__dict__.values() if isinstance(value, np.ndarray))
        )


def _collect_market_data_matrix_extras(
    panel: pl.DataFrame,
    field_columns: set[str] | frozenset[str] | None,
    timestamp_col: str,
    float_matrix: Callable[..., np.ndarray],
    shape: tuple[int, int],
    unique_timestamps: pl.Series,
    symbol_values: pl.Series,
    asset_id: np.ndarray,
    close: np.ndarray,
) -> tuple[dict[str, np.ndarray], list[str]]:
    """收集基础面板的扩展数值字段与名称，含涨跌停幂等写入。"""
    core_columns = {
        timestamp_col,
        "symbol",
        "name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "signal_limit_up",
        "signal_limit_down",
    }
    wanted_fields = set(field_columns or ()) - core_columns
    fields: dict[str, np.ndarray] = {}
    for column in sorted(wanted_fields):
        if column == "price_limit_pct":
            continue
        if column in panel.columns and panel[column].dtype.is_numeric():
            fields[column] = float_matrix(column)
        elif column == "raw_close":
            fields[column] = np.array(close, copy=True)

    names = [""] * len(symbol_values)
    if "name" in panel.columns:
        row_names = panel["name"].fill_null("").cast(pl.Utf8).to_numpy()
        for row, aid in enumerate(asset_id):
            if not names[int(aid)] and row_names[row]:
                names[int(aid)] = str(row_names[row])

    if "price_limit_pct" in wanted_fields:
        trading_dates = unique_timestamps.cast(pl.Date).to_list()
        fields["price_limit_pct"] = write_numpy_price_limit_matrix(
            np.empty(shape, dtype=np.float32),
            trading_dates,
            symbol_values,  # type: ignore[arg-type]
            names,
            valid=np.isfinite(close),
        )
    return fields, names


def build_market_data_matrix(
    panel: pl.DataFrame,
    *,
    field_columns: set[str] | frozenset[str] | None = None,
) -> MarketDataMatrix:
    """Encode a long base panel into immutable ``time x asset`` arrays."""
    if panel.is_empty():
        raise ValueError("cannot build MarketDataMatrix from an empty panel")

    timestamp_col, unique_timestamps, symbol_values, time_id, asset_id = _root._encode_axes(panel)
    shape = (len(unique_timestamps), len(symbol_values))

    def float_matrix(
        column: str,
        default: float = np.nan,
        null_fill: float | None = None,
    ) -> np.ndarray:
        return _root._float_matrix(panel, column, shape, time_id, asset_id, default, null_fill)

    open_ = float_matrix("open")
    high = float_matrix("high")
    low = float_matrix("low")
    close = float_matrix("close")
    volume = float_matrix("volume", 0.0 if "volume" in panel.columns else 1.0, 0.0)
    limit_up_locked = _root._bool_matrix(panel, "signal_limit_up", shape, time_id, asset_id)
    limit_down_locked = _root._bool_matrix(panel, "signal_limit_down", shape, time_id, asset_id)
    tradable = _root._tradable_matrix(open_, high, low, close, volume)

    fields, names = _collect_market_data_matrix_extras(
        panel,
        field_columns,
        timestamp_col,
        float_matrix,
        shape,
        unique_timestamps,
        symbol_values,
        asset_id,
        close,
    )
    timestamp_labels = tuple(str(value)[:19] for value in unique_timestamps.to_numpy())
    timestamps = _root._timestamp_int64(unique_timestamps)
    session_dates = unique_timestamps.cast(pl.Date).to_numpy()
    session_values = np.unique(session_dates)
    session_ids = np.searchsorted(session_values, session_dates).astype(np.int32)

    arrays = (
        timestamps,
        session_ids,
        open_,
        high,
        low,
        close,
        volume,
        tradable,
        limit_up_locked,
        limit_down_locked,
        *fields.values(),
    )
    _root._make_read_only(*arrays)

    return MarketDataMatrix(
        timestamps=timestamps,
        timestamp_labels=timestamp_labels,
        session_ids=session_ids,
        symbols=tuple(str(value) for value in symbol_values),
        names=tuple(names),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        tradable=tradable,
        limit_up_locked=limit_up_locked,
        limit_down_locked=limit_down_locked,
        fields=MappingProxyType(fields),
    )


def load_market_data_matrix_from_parquet(
    parquet_root: Path,
    start: date,
    end: date,
    *,
    field_columns: set[str] | frozenset[str],
    symbols: list[str] | None = None,
    instruments: pl.DataFrame | None = None,
    batch_size: int = _ARROW_BATCH_SIZE,
    cache_root: Path | None = None,
    coverage_start: date | None = None,
    coverage_end: date | None = None,
    cache_field_columns: set[str] | frozenset[str] | None = None,
    cache_max_bytes: int = _MATRIX_DISK_CACHE_DEFAULT_MAX_BYTES,
    profile_generation: str = "default",
    source_generation: str | None = None,
    cancel_event: threading.Event | None = None,
) -> MarketDataMatrix:
    """Load a daily market matrix, reusing a covering read-only mmap when possible."""
    _root._raise_if_matrix_cancelled(cancel_event)
    context = _root._validate_matrix_load_request(
        parquet_root,
        start,
        end,
        field_columns,
        symbols,
        instruments,
        coverage_start,
        coverage_end,
        cache_field_columns,
    )

    if cache_root is None:
        return _root._build_market_data_matrix_from_dataset(
            context.dataset,
            context.root,
            context.effective_start,
            context.effective_end,
            context.requested_fields,
            context.symbols,
            instruments,
            batch_size=batch_size,
            cache_status="disabled",
            cancel_event=cancel_event,
        )

    cache_dir = Path(cache_root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    # CK 里的缓存索引先落到本地（只写 manifest），命中筛选逻辑保持不变
    _root._sync_matrix_cache_index(cache_dir)
    covering = _root._load_covering_matrix_cache(cache_dir, context, source_generation)
    if covering is not None:
        return covering
    return _root._build_or_reuse_matrix_cache(
        cache_dir,
        context,
        batch_size=batch_size,
        cache_max_bytes=cache_max_bytes,
        profile_generation=profile_generation,
        source_generation=source_generation,
        cancel_event=cancel_event,
    )


def _matrix_filter_expression(
    start: date,
    end: date,
    symbols: tuple[str, ...] | None,
):
    expression = (pads.field("date") >= pa.scalar(start)) & (pads.field("date") <= pa.scalar(end))
    if symbols is not None:
        expression &= pads.field("symbol").isin(list(symbols))
    return expression


def _scan_matrix_values(
    dataset: pads.Dataset,
    filter_expr,
    actual_dates: list[date],
    actual_symbols: list[str],
    arrays: Mapping[str, np.ndarray],
    fields: Mapping[str, np.ndarray],
    parquet_fields: list[str],
    seen: np.ndarray,
    *,
    batch_size: int,
    cancel_event: threading.Event | None = None,
) -> None:
    date_to_id = {value: index for index, value in enumerate(actual_dates)}
    symbol_to_id = {value: index for index, value in enumerate(actual_symbols)}
    scan_columns = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        *parquet_fields,
    ]
    scanner = dataset.scanner(
        columns=scan_columns,
        filter=filter_expr,
        batch_size=int(batch_size),
        use_threads=True,
    )
    flat_seen = seen.ravel()
    asset_count = len(actual_symbols)
    scan_targets = {
        "open": arrays["open"],
        "high": arrays["high"],
        "low": arrays["low"],
        "close": arrays["close"],
        "volume": arrays["volume"],
        **{name: fields[name] for name in parquet_fields},
    }
    for batch in scanner.to_batches():
        _root._raise_if_matrix_cancelled(cancel_event)
        time_ids = _arrow_axis_ids(_batch_column(batch, "date"), date_to_id)
        asset_ids = _arrow_axis_ids(_batch_column(batch, "symbol"), symbol_to_id)
        flat_ids = time_ids.astype(np.int64) * asset_count + asset_ids
        if np.unique(flat_ids).size != flat_ids.size or flat_seen[flat_ids].any():
            raise ValueError("MarketDataMatrix requires unique date/symbol rows")
        flat_seen[flat_ids] = True
        for name, target in scan_targets.items():
            values = _root._arrow_float_values(
                _batch_column(batch, name),
                null_fill=0.0 if name == "volume" else np.nan,
            )
            target[time_ids, asset_ids] = values


def _collect_parquet_axes(
    dataset: pads.Dataset,
    filter_expr,
    *,
    batch_size: int,
    cancel_event: threading.Event | None = None,
) -> tuple[list[date], list[str]]:
    dates: set[date] = set()
    symbols: set[str] = set()
    scanner = dataset.scanner(
        columns=["date", "symbol"],
        filter=filter_expr,
        batch_size=int(batch_size),
        use_threads=True,
    )
    read_s = 0.0
    unique_s = 0.0
    group: list[pa.RecordBatch] = []
    batches = scanner.to_batches()
    while True:
        step = time.perf_counter()
        try:
            batch = next(batches)
        except StopIteration:
            read_s += time.perf_counter() - step
            break
        read_s += time.perf_counter() - step
        _root._raise_if_matrix_cancelled(cancel_event)
        group.append(batch)
        if len(group) < 8:
            continue
        step = time.perf_counter()
        table = pa.Table.from_batches(group)
        dates.update(_root.pc.unique(table["date"]).to_pylist())
        symbols.update(str(value) for value in _root.pc.unique(table["symbol"]).to_pylist())
        unique_s += time.perf_counter() - step
        group.clear()
    if group:
        _root._raise_if_matrix_cancelled(cancel_event)
        step = time.perf_counter()
        table = pa.Table.from_batches(group)
        dates.update(_root.pc.unique(table["date"]).to_pylist())
        symbols.update(str(value) for value in _root.pc.unique(table["symbol"]).to_pylist())
        unique_s += time.perf_counter() - step
    logger.info("matrix axes scan read_ms=%.1f unique_ms=%.1f", read_s * 1000, unique_s * 1000)
    return sorted(dates), sorted(symbols)


def _arrow_axis_ids(values: pa.Array, mapping: Mapping[Any, int]) -> np.ndarray:
    encoded = _root.pc.dictionary_encode(values)
    dictionary_ids = np.asarray(
        [mapping[value] for value in encoded.dictionary.to_pylist()],
        dtype=np.int32,
    )
    indices = encoded.indices.to_numpy(zero_copy_only=False)
    return dictionary_ids[np.asarray(indices, dtype=np.int32)]


def _batch_column(batch: pa.RecordBatch, name: str) -> pa.Array:
    return batch.column(batch.schema.get_field_index(name))
