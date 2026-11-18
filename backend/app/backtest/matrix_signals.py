from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    Mapping,
    MappingProxyType,
    MarketDataMatrix,
    MarketMatrix,
    Sequence,
    SignalMatrix,
    build_minute_exit_reference,
    dataclass,
    date,
    np,
    numpy_gem_pre_registration_vector,
    numpy_limit_pct_for_date,
    numpy_limit_pct_vectors,
    numpy_limit_price,
    pl,
)


def _limit_lock_reference_price(
    current_adjustment: np.ndarray,
    previous_adjustment: np.ndarray,
    previous_close: np.ndarray,
    previous_raw: np.ndarray,
) -> np.ndarray:
    """除权变化时用前复权昨收还原交易所参考价，否则沿用 raw 昨收。"""
    adjustment_changed = (
        np.isfinite(current_adjustment)
        & np.isfinite(previous_adjustment)
        & (np.abs(current_adjustment - previous_adjustment) > 1e-6)
    )
    safe_adjustment = np.where(
        np.isfinite(current_adjustment) & (current_adjustment > 0), current_adjustment, 1.0
    )
    return np.where(adjustment_changed, previous_close / safe_adjustment, previous_raw)


def _limit_lock_matrices(
    close: np.ndarray,
    raw_close: np.ndarray,
    seen: np.ndarray,
    trading_dates: Sequence[date],
    symbols: list[str],
    names: list[str],
    latest_limits: Mapping[str, np.ndarray],
    *,
    out_up: np.ndarray | None = None,
    out_down: np.ndarray | None = None,
    apply_latest_limits: bool = True,
) -> tuple[np.ndarray, np.ndarray]:
    shape = close.shape
    up_locked = out_up if out_up is not None else np.zeros(shape, dtype=np.uint8)
    down_locked = out_down if out_down is not None else np.zeros(shape, dtype=np.uint8)
    if up_locked.shape != shape or down_locked.shape != shape:
        raise ValueError("limit lock output shape mismatch")
    if len(trading_dates) != shape[0]:
        raise ValueError("price-limit date axis mismatch")
    up_locked.fill(0)
    down_locked.fill(0)
    legacy_pct, current_pct = numpy_limit_pct_vectors(symbols, names)
    pre_gem_pct = numpy_gem_pre_registration_vector(symbols, legacy_pct, names)

    previous_close = np.full(shape[1], np.nan, dtype=np.float64)
    previous_raw = np.full(shape[1], np.nan, dtype=np.float64)
    previous_adjustment = np.full(shape[1], np.nan, dtype=np.float64)
    for time_id in range(shape[0]):
        limit_pct = numpy_limit_pct_for_date(
            trading_dates[time_id],
            pre_gem_pct,
            legacy_pct,
            current_pct,
        )
        present = seen[time_id]
        current_close = close[time_id].astype(np.float64, copy=False)
        current_raw = raw_close[time_id].astype(np.float64, copy=False)
        current_adjustment = np.full(shape[1], np.nan, dtype=np.float64)
        np.divide(
            current_close,
            current_raw,
            out=current_adjustment,
            where=np.isfinite(current_raw) & (current_raw != 0),
        )
        reference = _limit_lock_reference_price(
            current_adjustment, previous_adjustment, previous_close, previous_raw
        )
        valid = (
            present
            & np.isfinite(reference)
            & (reference > 0)
            & np.isfinite(current_raw)
            & (current_raw > 0)
        )
        if valid.any():
            up_price = numpy_limit_price(reference, limit_pct, up=True)
            down_price = numpy_limit_price(reference, limit_pct, up=False)
            if apply_latest_limits and time_id == shape[0] - 1:
                latest_up = latest_limits["limit_up"]
                latest_down = latest_limits["limit_down"]
                use_up = np.isfinite(latest_up) & (latest_up < 10_000.0)
                use_down = np.isfinite(latest_down) & (latest_down < 10_000.0)
                up_price = np.where(use_up, latest_up, up_price)
                down_price = np.where(use_down, latest_down, down_price)
            up_locked[time_id, valid] = (current_raw[valid] >= up_price[valid] - 0.005).astype(
                np.uint8
            )
            down_locked[time_id, valid] = (current_raw[valid] <= down_price[valid] + 0.005).astype(
                np.uint8
            )

        previous_close[present] = current_close[present]
        previous_raw[present] = current_raw[present]
        previous_adjustment[present] = current_adjustment[present]
    return up_locked, down_locked


def make_signal_matrix(
    shape: tuple[int, int],
    *,
    entry: np.ndarray | None = None,
    exit: np.ndarray | None = None,
    score: np.ndarray | None = None,
    entry_signal_code: np.ndarray | None = None,
    exit_signal_code: np.ndarray | None = None,
    entry_signal_ids: tuple[str, ...] = (),
    exit_signal_ids: tuple[str, ...] = (),
) -> SignalMatrix:
    """Create a compact read-only signal matrix with canonical dtypes."""
    entry_array = _root._coerce_array(entry, shape, np.uint8, 0)
    exit_array = _root._coerce_array(exit, shape, np.uint8, 0)
    score_array = _root._coerce_array(score, shape, np.float32, 0.0)
    entry_codes = _root._coerce_array(entry_signal_code, shape, np.int16, -1)
    exit_codes = _root._coerce_array(exit_signal_code, shape, np.int16, -1)
    return _finalize_signal_matrix(
        entry_array,
        exit_array,
        score_array,
        entry_codes,
        exit_codes,
        entry_signal_ids=entry_signal_ids,
        exit_signal_ids=exit_signal_ids,
    )


def _finalize_signal_matrix(
    entry: np.ndarray,
    exit_: np.ndarray,
    score: np.ndarray,
    entry_signal_code: np.ndarray,
    exit_signal_code: np.ndarray,
    *,
    entry_signal_ids: tuple[str, ...] = (),
    exit_signal_ids: tuple[str, ...] = (),
) -> SignalMatrix:
    shape = entry.shape
    _root._make_read_only(entry, exit_, score, entry_signal_code, exit_signal_code)
    result = SignalMatrix(
        entry=entry,
        exit=exit_,
        score=score,
        entry_signal_code=entry_signal_code,
        exit_signal_code=exit_signal_code,
        entry_signal_ids=tuple(entry_signal_ids),
        exit_signal_ids=tuple(exit_signal_ids),
    )
    validate_signal_matrix(result, shape)
    return result


def validate_signal_matrix(signals: SignalMatrix, shape: tuple[int, int]) -> None:
    """Fail explicitly when a matrix strategy violates the shared output contract."""
    specs = {
        "entry": (signals.entry, np.dtype(np.uint8)),
        "exit": (signals.exit, np.dtype(np.uint8)),
        "score": (signals.score, np.dtype(np.float32)),
        "entry_signal_code": (signals.entry_signal_code, np.dtype(np.int16)),
        "exit_signal_code": (signals.exit_signal_code, np.dtype(np.int16)),
    }
    for name, (array, dtype) in specs.items():
        if not isinstance(array, np.ndarray):
            raise TypeError(f"SignalMatrix.{name} must be a numpy array")
        if array.shape != shape:
            raise ValueError(
                f"SignalMatrix.{name} shape {array.shape} does not match market {shape}"
            )
        if array.dtype != dtype:
            raise TypeError(f"SignalMatrix.{name} must use {dtype}, got {array.dtype}")
        if array.flags.writeable:
            raise ValueError(f"SignalMatrix.{name} must be read-only")
    if not np.isfinite(signals.score).all():
        raise ValueError("SignalMatrix.score must contain only finite values")


def _resolve_signal_reference_price(
    market: MarketDataMatrix,
    signals: SignalMatrix,
    reference_price: np.ndarray | None,
    minute_exit_trigger: bool,
) -> np.ndarray:
    """解析显式基准价、均线回退与分钟卖出触发覆盖。"""
    if reference_price is not None:
        if reference_price.shape != market.shape:
            raise ValueError("reference_price shape does not match MarketDataMatrix")
        resolved = np.array(reference_price, dtype=np.float32, copy=True)
    else:
        # polars_expr / 长表路径不传 reference_price, 回退用 panel 均线。
        # enriched maN 含当根收盘; 盘中成交不可能知道当日收盘, 代数剔除
        # 与 build_minute_entry_reference / 卖出侧同一纪律 (#388 只修了
        # matrix_native 显式传入的那条路径)。
        resolved = np.full(market.shape, np.nan, dtype=np.float32)
        with np.errstate(divide="ignore", invalid="ignore"):
            for window, column in ((5, "ma5"), (10, "ma10"), (20, "ma20")):
                values = market.fields.get(column)
                if values is None:
                    continue
                stripped = (float(window) * values - market.close) / float(window - 1)
                use = ~np.isfinite(resolved) & np.isfinite(stripped) & (stripped > 0)
                resolved[use] = stripped[use].astype(np.float32, copy=False)

    if minute_exit_trigger:
        trigger_reference = build_minute_exit_reference(
            market.close,
            market.fields,  # type: ignore[arg-type]
            signals.exit_signal_code,
            signals.exit_signal_ids,
        )
        trigger_mask = signals.exit != 0
        resolved[trigger_mask] = trigger_reference[trigger_mask]
    return resolved


def build_market_matrix_from_signals(
    market: MarketDataMatrix,
    signals: SignalMatrix,
    *,
    entry_delay_bars: int = 0,
    exit_delay_bars: int = 0,
    reference_price: np.ndarray | None = None,
    minute_exit_trigger: bool = False,
    entry_price_override: np.ndarray | None = None,
) -> MarketMatrix:
    """Combine base data and strategy signals into the matcher input matrix."""
    if entry_delay_bars not in (0, 1) or exit_delay_bars not in (0, 1):
        raise ValueError("phase-two MarketMatrix supports only zero or one bar delay")
    validate_signal_matrix(signals, market.shape)
    if entry_price_override is not None and entry_price_override.shape != market.shape:
        raise ValueError("entry_price_override shape does not match MarketDataMatrix")

    present = _root._present_matrix(
        market.open, market.high, market.low, market.close, market.volume
    )
    entry, entry_signal_time, entry_signal_code = _root._delay_signal_matrix(
        signals.entry,
        signals.entry_signal_code,
        present,
        entry_delay_bars,
    )
    exit_, exit_signal_time, exit_signal_code = _root._delay_signal_matrix(
        signals.exit,
        signals.exit_signal_code,
        present,
        exit_delay_bars,
    )

    resolved_reference_price = _resolve_signal_reference_price(
        market, signals, reference_price, minute_exit_trigger
    )

    _root._make_read_only(
        entry,
        exit_,
        resolved_reference_price,
        entry_signal_time,
        exit_signal_time,
        entry_signal_code,
        exit_signal_code,
    )

    return MarketMatrix(
        timestamps=market.timestamps,
        timestamp_labels=market.timestamp_labels,
        session_ids=market.session_ids,
        symbols=market.symbols,
        names=market.names,
        open=market.open,
        high=market.high,
        low=market.low,
        close=market.close,
        volume=market.volume,
        score=signals.score,
        entry=entry,
        exit=exit_,
        tradable=market.tradable,
        limit_up_locked=market.limit_up_locked,
        limit_down_locked=market.limit_down_locked,
        reference_price=resolved_reference_price,
        entry_signal_time=entry_signal_time,
        exit_signal_time=exit_signal_time,
        entry_signal_code=entry_signal_code,
        exit_signal_code=exit_signal_code,
        entry_signal_ids=signals.entry_signal_ids,
        exit_signal_ids=signals.exit_signal_ids,
        entry_price=(
            np.array(entry_price_override, dtype=np.float32, copy=True)
            if entry_price_override is not None
            else None
        ),
    )


def build_market_matrix(
    panel: pl.DataFrame,
    entries: pl.Series | None,
    exits: pl.Series | None,
    *,
    entry_delay_bars: int = 0,
    exit_delay_bars: int = 0,
    entry_signal_ids: list[str] | None = None,
    exit_signal_ids: list[str] | None = None,
    minute_exit_trigger: bool = False,
) -> MarketMatrix:
    """Backward-compatible long-panel boundary used by legacy/Polars strategies."""
    if panel.is_empty():
        raise ValueError("cannot build MarketMatrix from an empty panel")
    encoding = encode_panel_matrix(
        panel,
        entry_signal_ids=entry_signal_ids,
        exit_signal_ids=exit_signal_ids,
    )
    score = (
        panel.get_column("score")
        if "score" in panel.columns and panel["score"].dtype.is_numeric()
        else None
    )
    return build_market_matrix_from_encoding(
        encoding,
        score=score,
        entries=entries,
        exits=exits,
        entry_delay_bars=entry_delay_bars,
        exit_delay_bars=exit_delay_bars,
        minute_exit_trigger=minute_exit_trigger,
    )


@dataclass(frozen=True)
class PanelMatrixEncoding:
    """长面板 → 矩阵的参数无关编码, 供优化器跨参数组复用 (P2a-2)。

    行情矩阵不带 score 场 — score 随参数组的 universe 掩码变化 (_apply_score
    把非候选行置空), 由 build_market_matrix_from_encoding 逐组散布进信号
    矩阵; 全部数组只读共享, 复用零拷贝。"""

    market: MarketDataMatrix
    time_id: np.ndarray
    asset_id: np.ndarray
    panel_rows: int
    entry_signal_code: np.ndarray
    entry_signal_ids: tuple[str, ...]
    exit_signal_code: np.ndarray
    exit_signal_ids: tuple[str, ...]
    # 编码所用的列序; 调用方与当组 sim_columns 比对, 不一致回退整建
    # (回退是优化放弃, 不是正确性闸门 — 整建结果总是正确)。
    sim_columns: tuple[str, ...]

    @property
    def shape(self) -> tuple[int, int]:
        return self.market.shape


def encode_panel_matrix(
    panel: pl.DataFrame,
    *,
    entry_signal_ids: list[str] | None = None,
    exit_signal_ids: list[str] | None = None,
) -> PanelMatrixEncoding:
    """编码面板的参数无关半边: 行情矩阵 + 轴 id + 信号码矩阵。

    score/entry/exit 掩码随参数组变化, 不在编码内 — 见
    build_market_matrix_from_encoding。"""
    market = _root.build_market_data_matrix(panel, field_columns={"ma5", "ma10", "ma20"})
    _, _, _, time_id, asset_id = _root._encode_axes(panel)
    shape = market.shape
    entry_codes, normalized_entry_ids = _root._signal_code_matrix(
        panel,
        entry_signal_ids,
        shape,
        time_id,
        asset_id,
    )
    exit_codes, normalized_exit_ids = _root._signal_code_matrix(
        panel,
        exit_signal_ids,
        shape,
        time_id,
        asset_id,
    )
    return PanelMatrixEncoding(
        market=market,
        time_id=time_id,
        asset_id=asset_id,
        panel_rows=len(panel),
        entry_signal_code=entry_codes,
        entry_signal_ids=normalized_entry_ids,
        exit_signal_code=exit_codes,
        exit_signal_ids=normalized_exit_ids,
        sim_columns=tuple(panel.columns),
    )


def build_market_matrix_from_encoding(
    encoding: PanelMatrixEncoding,
    *,
    score: pl.Series | None,
    entries: pl.Series | None,
    exits: pl.Series | None,
    entry_delay_bars: int = 0,
    exit_delay_bars: int = 0,
    minute_exit_trigger: bool = False,
) -> MarketMatrix:
    """共享编码 + 参数相关的 score/entry/exit → MarketMatrix (优化器试算路径)。

    散布与合成和 build_market_matrix 走同一套件 (_root._scatter_bool_series /
    make_signal_matrix / build_market_matrix_from_signals), score 的
    非有限值→nan 语义与 _float_matrix 一致。"""
    shape = encoding.shape
    raw_entry = _root._scatter_bool_series(
        entries,
        encoding.panel_rows,
        shape,
        encoding.time_id,
        encoding.asset_id,
    )
    raw_exit = _root._scatter_bool_series(
        exits,
        encoding.panel_rows,
        shape,
        encoding.time_id,
        encoding.asset_id,
    )
    score_matrix = None
    if score is not None and len(score) == encoding.panel_rows:
        score_matrix = np.full(shape, np.nan, dtype=np.float32)
        values = np.array(
            score.cast(pl.Float32, strict=False).to_numpy(),
            dtype=np.float32,
            copy=True,
        )
        values[~np.isfinite(values)] = np.nan
        score_matrix[encoding.time_id, encoding.asset_id] = values
    signals = make_signal_matrix(
        shape,
        entry=raw_entry,
        exit=raw_exit,
        score=np.nan_to_num(score_matrix, nan=0.0) if score_matrix is not None else None,
        entry_signal_code=encoding.entry_signal_code,
        exit_signal_code=encoding.exit_signal_code,
        entry_signal_ids=encoding.entry_signal_ids,
        exit_signal_ids=encoding.exit_signal_ids,
    )
    return build_market_matrix_from_signals(
        encoding.market,
        signals,
        entry_delay_bars=entry_delay_bars,
        exit_delay_bars=exit_delay_bars,
        minute_exit_trigger=minute_exit_trigger,
    )


def slice_market_data_matrix(market: MarketDataMatrix, start: int, stop: int) -> MarketDataMatrix:
    """Return a read-only time slice without copying the underlying market arrays."""
    fields = {name: values[start:stop] for name, values in market.fields.items()}
    result = MarketDataMatrix(
        timestamps=market.timestamps[start:stop],
        timestamp_labels=market.timestamp_labels[start:stop],
        session_ids=market.session_ids[start:stop],
        symbols=market.symbols,
        names=market.names,
        open=market.open[start:stop],
        high=market.high[start:stop],
        low=market.low[start:stop],
        close=market.close[start:stop],
        volume=market.volume[start:stop],
        tradable=market.tradable[start:stop],
        limit_up_locked=market.limit_up_locked[start:stop],
        limit_down_locked=market.limit_down_locked[start:stop],
        fields=MappingProxyType(fields),
        cache_status=market.cache_status,
        cache_path=market.cache_path,
        cache_lease=market.cache_lease,
        vector_fields=market.vector_fields,
        cache_timing_ms=market.cache_timing_ms,
    )
    _root._make_read_only(
        result.timestamps,
        result.session_ids,
        result.open,
        result.high,
        result.low,
        result.close,
        result.volume,
        result.tradable,
        result.limit_up_locked,
        result.limit_down_locked,
        *fields.values(),
    )
    return result


def slice_signal_matrix(signals: SignalMatrix, start: int, stop: int) -> SignalMatrix:
    return _finalize_signal_matrix(
        signals.entry[start:stop],
        signals.exit[start:stop],
        signals.score[start:stop],
        signals.entry_signal_code[start:stop],
        signals.exit_signal_code[start:stop],
        entry_signal_ids=signals.entry_signal_ids,
        exit_signal_ids=signals.exit_signal_ids,
    )
