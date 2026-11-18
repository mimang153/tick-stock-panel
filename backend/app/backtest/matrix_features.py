from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    MarketDataMatrix,
    SignalMatrix,
    _activate_valid_bar_index,
    _cached_matrix_operation,
    np,
    pl,
)

_MATRIX_COMPUTED_FEATURES = frozenset(
    {
        "prev_close",
        "change_pct",
        "change_amount",
        "amplitude",
        "boll_upper",
        "boll_lower",
        "boll_position",
        "boll_width",
        "high_60d",
        "low_60d",
        "annual_vol_20d",
        "macd_dif",
        "macd_dea",
        "macd_hist",
        "macd_dif_pct",
        "macd_dea_pct",
        "macd_hist_pct",
        "kdj_k",
        "kdj_d",
        "kdj_j",
        "atr_14",
        "atr_pct",
        "vol_ma5",
        "vol_ma10",
        "vol_ratio_5d",
        "vol_ratio_10d",
        "vol_trend_5_10",
        "turnover_ratio_5d",
        "log_amount",
        "amount_ratio_5d",
        "gap_return",
        "intraday_return",
        "close_position",
        "distance_to_high_60d",
        "distance_from_low_60d",
        "max_ret_20d",
        "ret_skew_20d",
        "up_days_20d",
        "amihud_20d",
        "turnover_z_60d",
        "vol_price_corr_20d",
        "vwap_bias",
        "vol_trend_5_60",
        "limit_up_count_20d",
        "limit_up_count_60d",
        # --- 扩充批次 (2026-09-05): 与注册表/scoring 口径一致的 16 个新虚拟因子 ---
        "log_float_mv",
        "mom_accel_20_60",
        "rsi_14_delta_5d",
        "overnight_ret_20d",
        "intraday_ret_20d",
        "downside_vol_20d",
        "vol_regime_5_60",
        "amplitude_trend_20_60",
        "obv_trend_20d",
        "amount_mean_20d",
        "turnover_mean_20d",
        "turnover_std_20d",
        "position_240d",
        "distance_to_high_240d",
        "kdj_kd_diff",
    }
)


def matrix_feature(market: MarketDataMatrix, name: str) -> np.ndarray:
    if name in {"open", "high", "low", "close", "volume"} or name in market.fields:
        return market.field(name)
    from app.factors.registry import get_factor

    spec = get_factor(name)
    if spec is not None and spec.kind in {"custom", "composite"}:
        return _registered_matrix_feature(market, name)
    supported = (
        name in _MATRIX_COMPUTED_FEATURES
        or (name.startswith("ma") and name.endswith("_bias") and name[2:-5].isdigit())
        or (name.startswith("ema") and name.endswith("_bias") and name[3:-5].isdigit())
        or (name.startswith("ma") and name[2:].isdigit())
        or (name.startswith("ema") and name[3:].isdigit())
        or (name.startswith("rsi_") and name[4:].isdigit())
        or (name.startswith("momentum_") and name.endswith("d"))
    )
    if not supported:
        raise ValueError(f"unsupported matrix feature: {name}")
    with _activate_valid_bar_index(market.valid_bars):
        return _cached_matrix_operation(
            "matrix_feature",
            (market.close,),
            {"name": name},
            lambda: _compute_matrix_feature(market, name),
        )


def _registered_matrix_feature(market: MarketDataMatrix, name: str) -> np.ndarray:
    from app.factors.registry import factor_calculation_order, get_factor
    from app.strategy.scoring import materialize_scoring_columns

    ordered = factor_calculation_order([name])
    definitions = []
    inputs = []
    for fid in ordered:
        spec = get_factor(fid)
        if spec is not None and spec.kind in {"custom", "composite"}:
            definitions.append((fid, spec.version, spec.lang, spec.formula_text, spec.components))
        elif fid not in {"symbol", "date"}:
            inputs.append(fid)
    arrays = tuple(matrix_feature(market, fid) for fid in inputs)

    def compute() -> np.ndarray:
        # 只还原真实 K 线, 保持停牌期间时序窗口与 Polars 检验路径一致。
        assets, times = np.nonzero(np.isfinite(market.close).T)
        frame = pl.DataFrame(
            {
                "symbol": np.asarray(market.symbols)[assets],
                "date": np.asarray(market.timestamp_labels, dtype="datetime64[D]")[times],
                **{
                    fid: pl.Series(fid, values[times, assets], dtype=pl.Float64, nan_to_null=True)
                    for fid, values in zip(inputs, arrays, strict=True)
                },
            }
        )
        frame = materialize_scoring_columns(frame, [name])
        if name not in frame.columns:
            raise ValueError(f"factor {name} is missing required inputs")
        result = np.full(market.shape, np.nan, dtype=np.float32)
        result[times, assets] = frame[name].cast(pl.Float32).to_numpy()
        return result

    # 把全部依赖的定义与输入纳入键, 子因子改公式后父因子也必须重新计算。
    return _cached_matrix_operation(
        "registered_factor",
        (market.close, *arrays),
        {"definitions": definitions, "inputs": inputs},
        compute,
    )


def _compute_matrix_feature(market: MarketDataMatrix, name: str) -> np.ndarray:
    close_valid = np.isfinite(market.close)
    if (values := _matrix_base_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_ma_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_volume_amount_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_band_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_oscillator_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_stat_feature(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_flow_feature(market, name, close_valid)) is not None:
        return values
    # --- 扩充批次 (2026-09-05): numpy 内核实现, 口径与 strategy/scoring.py 一致 ---
    if (values := _matrix_extended_feature_a(market, name, close_valid)) is not None:
        return values
    if (values := _matrix_extended_feature_b(market, name, close_valid)) is not None:
        return values
    raise ValueError(f"unsupported matrix feature: {name}")


def _matrix_base_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name == "prev_close":
        return _root.valid_shift(market.close, 1, close_valid)
    if name == "change_pct":
        return _valid_return_over_bars(market.close, close_valid, 1)
    if name == "change_amount":
        previous = _root.valid_shift(market.close, 1, close_valid)
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.subtract(market.close, previous, out=out, where=np.isfinite(previous))
        return out
    if name == "amplitude":
        previous = _root.valid_shift(market.close, 1, close_valid)
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.divide(
            market.high - market.low,
            previous,
            out=out,
            where=np.isfinite(previous) & (previous > 0),
        )
        return out
    if name.startswith("momentum_") and name.endswith("d"):
        try:
            bars = int(name.removeprefix("momentum_").removesuffix("d"))
        except ValueError as exc:
            raise ValueError(f"unsupported matrix feature: {name}") from exc
        return _valid_return_over_bars(market.close, close_valid, bars)
    return None


def _matrix_ma_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name.startswith("ma") and name.endswith("_bias"):
        period = int(name.removeprefix("ma").removesuffix("_bias"))
        return _matrix_relative(
            market.close, _root.valid_rolling_mean(market.close, close_valid, period)
        )
    if name.startswith("ema") and name.endswith("_bias"):
        period = int(name.removeprefix("ema").removesuffix("_bias"))
        return _matrix_relative(market.close, _matrix_ema(market.close, close_valid, period))
    if name.startswith("ma") and name[2:].isdigit():
        return _root.valid_rolling_mean(market.close, close_valid, int(name[2:]))
    if name.startswith("ema") and name[3:].isdigit():
        return _matrix_ema(market.close, close_valid, int(name[3:]))
    if name in {"macd_dif", "macd_dea", "macd_hist"}:
        dif, dea = _matrix_macd(market.close, close_valid)
        if name == "macd_dif":
            return dif
        if name == "macd_dea":
            return dea
        return ((dif - dea) * np.float32(2.0)).astype(np.float32, copy=False)
    if name in {"macd_dif_pct", "macd_dea_pct", "macd_hist_pct"}:
        source = matrix_feature(market, name.removesuffix("_pct"))
        return _matrix_ratio(source, market.close)
    return None


def _matrix_volume_amount_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name in {"vol_ratio_5d", "vol_ratio_10d"}:
        window = 5 if name == "vol_ratio_5d" else 10
        volume_valid = close_valid & np.isfinite(market.volume)
        previous_volume = _root.valid_shift(market.volume, 1, volume_valid)
        previous_mean = _root.valid_rolling_mean(
            previous_volume,
            np.isfinite(previous_volume),
            window,
        )
        return _matrix_ratio(market.volume, previous_mean)
    if name in {"vol_ma5", "vol_ma10"}:
        window = 5 if name == "vol_ma5" else 10
        volume_valid = close_valid & np.isfinite(market.volume)
        return _root.valid_rolling_mean(market.volume, volume_valid, window)
    if name == "vol_trend_5_10":
        return _matrix_relative(
            matrix_feature(market, "vol_ma5"),
            matrix_feature(market, "vol_ma10"),
        )
    if name == "turnover_ratio_5d":
        turnover = market.field("turnover_rate")
        valid = close_valid & np.isfinite(turnover)
        previous = _root.valid_shift(turnover, 1, valid)
        return _matrix_relative(
            turnover,
            _root.valid_rolling_mean(previous, np.isfinite(previous), 5),
        )
    if name == "log_amount":
        amount = market.field("amount")
        out = np.full(market.shape, np.nan, dtype=np.float32)
        valid = close_valid & np.isfinite(amount) & (amount >= 0)
        np.log(amount + np.float32(1.0), out=out, where=valid)
        return out
    if name == "amount_ratio_5d":
        amount = market.field("amount")
        valid = close_valid & np.isfinite(amount)
        previous = _root.valid_shift(amount, 1, valid)
        return _matrix_relative(
            amount,
            _root.valid_rolling_mean(previous, np.isfinite(previous), 5),
        )
    return None


def _matrix_band_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name == "boll_upper" or name == "boll_lower":
        middle = _root.valid_rolling_mean(market.close, close_valid, 20)
        deviation = _root.valid_rolling_std(market.close, close_valid, 20, ddof=1)
        offset = np.float32(2.0) * deviation
        return middle + offset if name == "boll_upper" else middle - offset
    if name == "boll_position":
        return _matrix_ratio(
            market.close - matrix_feature(market, "boll_lower"),
            matrix_feature(market, "boll_upper") - matrix_feature(market, "boll_lower"),
        )
    if name == "boll_width":
        return _matrix_ratio(
            matrix_feature(market, "boll_upper") - matrix_feature(market, "boll_lower"),
            matrix_feature(market, "ma20"),
        )
    if name == "high_60d":
        return _root.valid_rolling_max(market.close, close_valid, 60)
    if name == "low_60d":
        return _root.valid_rolling_min(market.close, close_valid, 60)
    if name == "annual_vol_20d":
        daily = _valid_return_over_bars(market.close, close_valid, 1)
        return _root.valid_rolling_std(
            daily,
            np.isfinite(daily),
            20,
            ddof=1,
        ) * np.float32(252**0.5)
    return None


def _matrix_oscillator_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name in {"kdj_k", "kdj_d", "kdj_j"}:
        low_valid = close_valid & np.isfinite(market.low)
        high_valid = close_valid & np.isfinite(market.high)
        low_9 = _root.valid_rolling_min(market.low, low_valid, 9)
        high_9 = _root.valid_rolling_max(market.high, high_valid, 9)
        rsv = _matrix_ratio(market.close - low_9, high_9 - low_9) * np.float32(100.0)
        k = _root.valid_ewm_adjust_false(rsv, np.isfinite(rsv), alpha=1.0 / 3.0)
        if name == "kdj_k":
            return k
        d = _root.valid_ewm_adjust_false(k, np.isfinite(k), alpha=1.0 / 3.0)
        if name == "kdj_d":
            return d
        return (np.float32(3.0) * k - np.float32(2.0) * d).astype(np.float32, copy=False)
    if name in {"atr_14", "atr_pct"}:
        previous = _root.valid_shift(market.close, 1, close_valid)
        true_range = np.fmax.reduce(  # type: ignore[call-overload]
            [
                market.high - market.low,
                np.abs(market.high - previous),
                np.abs(market.low - previous),
            ]
        ).astype(np.float32, copy=False)
        true_range[~close_valid] = np.nan
        atr = _root.valid_ewm_adjust_false(true_range, np.isfinite(true_range), alpha=1.0 / 14.0)
        return atr if name == "atr_14" else _matrix_ratio(atr, market.close)
    if name.startswith("rsi_") and name[4:].isdigit():
        window = int(name[4:])
        delta = market.close - _root.valid_shift(market.close, 1, close_valid)
        delta_valid = close_valid
        gain = np.where(delta > 0, delta, 0.0).astype(np.float32, copy=False)
        loss = np.where(delta < 0, -delta, 0.0).astype(np.float32, copy=False)
        gain[~delta_valid] = np.nan
        loss[~delta_valid] = np.nan
        average_gain = _root.valid_ewm_adjust_false(
            gain,
            delta_valid,
            alpha=1.0 / window,
        )
        average_loss = _root.valid_ewm_adjust_false(
            loss,
            delta_valid,
            alpha=1.0 / window,
        )
        denominator = np.where(average_loss == 0, np.float32(1e-12), average_loss)
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.divide(average_gain, denominator, out=out, where=np.isfinite(denominator))
        out = np.float32(100.0) - np.float32(100.0) / (np.float32(1.0) + out)
        return out
    if name == "gap_return":
        return _matrix_relative(market.open, _root.valid_shift(market.close, 1, close_valid))
    if name == "intraday_return":
        return _matrix_relative(market.close, market.open)
    if name == "close_position":
        return _matrix_ratio(market.close - market.low, market.high - market.low)
    if name == "distance_to_high_60d":
        return _matrix_relative(market.close, matrix_feature(market, "high_60d"))
    if name == "distance_from_low_60d":
        return _matrix_relative(market.close, matrix_feature(market, "low_60d"))
    return None


def _matrix_stat_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name == "max_ret_20d":
        daily = matrix_feature(market, "change_pct")
        return _root.valid_rolling_max(daily, np.isfinite(daily), 20)
    if name == "ret_skew_20d":
        return _matrix_rolling_skew(matrix_feature(market, "change_pct"), 20)
    if name == "up_days_20d":
        daily = matrix_feature(market, "change_pct")
        up = np.where(daily > 0, np.float32(1.0), np.float32(0.0)).astype(np.float32)
        up[~np.isfinite(daily)] = np.nan
        return _root.valid_rolling_sum(up, np.isfinite(up), 20)
    if name == "amihud_20d":
        daily = matrix_feature(market, "change_pct")
        amount = market.field("amount")
        amount_yi = amount / np.float32(1e8)
        illiquidity = _matrix_ratio(np.abs(daily), amount_yi)
        return _root.valid_rolling_mean(
            illiquidity,
            close_valid & np.isfinite(illiquidity),
            20,
        )
    if name == "turnover_z_60d":
        turnover = market.field("turnover_rate")
        valid = close_valid & np.isfinite(turnover)
        previous = _root.valid_shift(turnover, 1, valid)
        baseline_valid = np.isfinite(previous)
        mean = _root.valid_rolling_mean(previous, baseline_valid, 60)
        std = _root.valid_rolling_std(previous, baseline_valid, 60, ddof=1)
        deviation = _matrix_ratio(turnover - mean, std)
        deviation[np.isfinite(std) & (std <= 0)] = np.nan
        return deviation
    if name == "vol_price_corr_20d":
        daily = matrix_feature(market, "change_pct")
        return _matrix_rolling_corr(daily, market.volume, close_valid, 20)
    return None


def _matrix_flow_feature(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name == "vwap_bias":
        amount = market.field("amount")
        shares = market.volume * np.float32(100.0)
        valid = close_valid & np.isfinite(amount) & (market.volume > 0) & (amount > 0)
        vwap = np.full(market.shape, np.nan, dtype=np.float32)
        np.divide(amount, shares, out=vwap, where=valid)
        return _matrix_relative(market.close, vwap)
    if name == "vol_trend_5_60":
        volume_valid = close_valid & np.isfinite(market.volume)
        fast = _root.valid_rolling_mean(market.volume, volume_valid, 5)
        slow = _root.valid_rolling_mean(market.volume, volume_valid, 60)
        return _matrix_relative(fast, slow)
    if name in {"limit_up_count_20d", "limit_up_count_60d"}:
        window = 20 if name == "limit_up_count_20d" else 60
        consecutive = market.field("consecutive_limit_ups")
        hits = np.where(
            np.isfinite(consecutive) & (consecutive > 0), np.float32(1.0), np.float32(0.0)
        )
        hits = hits.astype(np.float32)
        return _root.valid_rolling_sum(hits, close_valid, window)
    return None


def _matrix_extended_feature_a(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    # --- 扩充批次 (2026-09-05): numpy 内核实现, 口径与 strategy/scoring.py 一致 ---
    if name == "log_float_mv":
        turnover = market.field("turnover_rate")
        valid = close_valid & np.isfinite(turnover) & (turnover > 0) & (market.volume > 0)
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.multiply(market.close, market.volume, out=out, where=valid)
        np.divide(out, turnover, out=out, where=valid)
        np.log(out, out=out, where=valid)
        return out
    if name == "mom_accel_20_60" or name == "kdj_kd_diff":
        left, right = (
            (matrix_feature(market, "momentum_20d"), matrix_feature(market, "momentum_60d"))
            if name == "mom_accel_20_60"
            else (matrix_feature(market, "kdj_k"), matrix_feature(market, "kdj_d"))
        )
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.subtract(left, right, out=out, where=np.isfinite(left) & np.isfinite(right))
        return out
    if name == "rsi_14_delta_5d":
        rsi = matrix_feature(market, "rsi_14")
        return rsi - _root.valid_shift(rsi, 5, np.isfinite(rsi))
    if name == "overnight_ret_20d":
        overnight = _matrix_relative(market.open, matrix_feature(market, "prev_close"))
        return _root.valid_rolling_sum(overnight, np.isfinite(overnight), 20)
    if name == "intraday_ret_20d":
        intraday = _matrix_relative(market.close, market.open)
        return _root.valid_rolling_sum(intraday, np.isfinite(intraday), 20)
    if name == "downside_vol_20d":
        daily = matrix_feature(market, "change_pct")
        downside = np.where(
            np.isfinite(daily),
            np.minimum(daily, np.float32(0.0)),
            np.nan,
        ).astype(np.float32)
        mean_sq = _root.valid_rolling_mean(
            np.square(downside, dtype=np.float32), np.isfinite(downside), 20
        )
        out = np.full(market.shape, np.nan, dtype=np.float32)
        np.sqrt(mean_sq, out=out, where=np.isfinite(mean_sq))
        return out
    return None


def _matrix_extended_feature_b(
    market: MarketDataMatrix, name: str, close_valid: np.ndarray
) -> np.ndarray | None:
    if name == "vol_regime_5_60":
        daily = matrix_feature(market, "change_pct")
        valid = np.isfinite(daily)
        return _matrix_ratio(
            _root.valid_rolling_std(daily, valid, 5, ddof=1),
            _root.valid_rolling_std(daily, valid, 60, ddof=1),
        )
    if name == "amplitude_trend_20_60":
        amplitude = matrix_feature(market, "amplitude")
        valid = np.isfinite(amplitude)
        return _matrix_relative(
            _root.valid_rolling_mean(amplitude, valid, 20),
            _root.valid_rolling_mean(amplitude, valid, 60),
        )
    if name == "obv_trend_20d":
        daily = matrix_feature(market, "change_pct")
        volume_valid = close_valid & np.isfinite(market.volume)
        signed = np.where(
            np.isfinite(daily),
            np.sign(daily) * market.volume,
            np.nan,
        ).astype(np.float32)
        total = _root.valid_rolling_sum(signed, volume_valid & np.isfinite(daily), 20)
        scale = _root.valid_rolling_mean(market.volume, volume_valid, 20) * np.float32(20.0)
        return _matrix_ratio(total, scale)
    if name == "amount_mean_20d":
        amount = market.field("amount")
        return _root.valid_rolling_mean(amount / np.float32(1e8), np.isfinite(amount), 20)
    if name == "turnover_mean_20d" or name == "turnover_std_20d":
        turnover = market.field("turnover_rate")
        valid = np.isfinite(turnover)
        mean = _root.valid_rolling_mean(turnover, valid, 20)
        if name == "turnover_mean_20d":
            return mean
        return _matrix_ratio(_root.valid_rolling_std(turnover, valid, 20, ddof=1), mean)
    if name == "position_240d":
        high = _root.valid_rolling_max(market.close, close_valid, 240)
        low = _root.valid_rolling_min(market.close, close_valid, 240)
        return _matrix_ratio(market.close - low, high - low)
    if name == "distance_to_high_240d":
        return _matrix_relative(
            market.close, _root.valid_rolling_max(market.close, close_valid, 240)
        )
    return None


def _matrix_rolling_skew(values: np.ndarray, window: int) -> np.ndarray:
    valid = np.isfinite(values)
    first = _root.valid_rolling_mean(values, valid, window)
    second = _root.valid_rolling_mean(np.square(values, dtype=np.float32), valid, window)
    third = _root.valid_rolling_mean((values * values * values).astype(np.float32), valid, window)
    variance = second - np.square(first, dtype=np.float32)
    central_third = third - np.float32(3.0) * first * second + np.float32(2.0) * np.power(first, 3)
    out = _matrix_ratio(central_third, np.sqrt(np.power(variance, 3)))
    out[np.isfinite(variance) & (variance <= 0)] = np.nan
    return out


def _matrix_rolling_corr(
    left: np.ndarray, right: np.ndarray, valid_mask: np.ndarray, window: int
) -> np.ndarray:
    valid = valid_mask & np.isfinite(left) & np.isfinite(right)
    product = (left * right).astype(np.float32)
    mean_left = _root.valid_rolling_mean(left, valid, window)
    mean_right = _root.valid_rolling_mean(right, valid, window)
    mean_product = _root.valid_rolling_mean(product, valid, window)
    mean_left_sq = _root.valid_rolling_mean(np.square(left, dtype=np.float32), valid, window)
    mean_right_sq = _root.valid_rolling_mean(np.square(right, dtype=np.float32), valid, window)
    covariance = mean_product - mean_left * mean_right
    variance_left = mean_left_sq - np.square(mean_left, dtype=np.float32)
    variance_right = mean_right_sq - np.square(mean_right, dtype=np.float32)
    denominator = np.sqrt(variance_left * variance_right)
    out = _matrix_ratio(covariance, denominator)
    degenerate = (
        np.isfinite(variance_left)
        & np.isfinite(variance_right)
        & ((variance_left <= 0) | (variance_right <= 0))
    )
    out[degenerate] = np.nan
    return out


def _matrix_ema(values: np.ndarray, valid: np.ndarray, period: int) -> np.ndarray:
    return _root.valid_ewm_adjust_false(values, valid, alpha=2.0 / (period + 1.0))


def _matrix_macd(values: np.ndarray, valid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dif = _matrix_ema(values, valid, 12) - _matrix_ema(values, valid, 26)
    dif = dif.astype(np.float32, copy=False)
    dea = _root.valid_ewm_adjust_false(dif, np.isfinite(dif), alpha=2.0 / 10.0)
    return dif, dea


def _matrix_ratio(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    out = np.full(numerator.shape, np.nan, dtype=np.float32)
    valid = np.isfinite(numerator) & np.isfinite(denominator) & (denominator != 0)
    np.divide(numerator, denominator, out=out, where=valid)
    return out


def _matrix_relative(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    out = _matrix_ratio(numerator, denominator)
    out[np.isfinite(out)] -= np.float32(1.0)
    return out


def apply_time_masks(
    signals: SignalMatrix,
    entry_time_mask: np.ndarray,
    exit_time_mask: np.ndarray,
) -> SignalMatrix:
    if entry_time_mask.shape != (signals.shape[0],) or exit_time_mask.shape != (signals.shape[0],):
        raise ValueError("time mask length does not match SignalMatrix")
    entry_mask = np.asarray(entry_time_mask, dtype=bool)
    exit_mask = np.asarray(exit_time_mask, dtype=bool)
    entry = np.array(signals.entry, dtype=np.uint8, copy=True)
    exit_ = np.array(signals.exit, dtype=np.uint8, copy=True)
    entry[~entry_mask] = 0
    exit_[~exit_mask] = 0
    entry_codes = np.array(signals.entry_signal_code, dtype=np.int16, copy=True)
    exit_codes = np.array(signals.exit_signal_code, dtype=np.int16, copy=True)
    entry_codes[entry == 0] = -1
    exit_codes[exit_ == 0] = -1
    return _root._finalize_signal_matrix(
        entry,
        exit_,
        signals.score,
        entry_codes,
        exit_codes,
        entry_signal_ids=signals.entry_signal_ids,
        exit_signal_ids=signals.exit_signal_ids,
    )


def _valid_return_over_bars(
    values: np.ndarray,
    valid_mask: np.ndarray,
    bars: int,
) -> np.ndarray:
    previous = _root.valid_shift(values, bars, valid_mask)
    out = np.full(values.shape, np.nan, dtype=np.float32)
    np.divide(values, previous, out=out, where=np.isfinite(previous) & (previous != 0))
    out -= np.float32(1.0)
    return out


def _optional_field(market: MarketDataMatrix, name: str) -> np.ndarray:
    values = market.fields.get(name)
    if values is None:
        return np.full(market.shape, np.nan, dtype=np.float32)
    return values


def _required_field_for_bound(
    market: MarketDataMatrix,
    config: dict,
    name: str,
) -> np.ndarray:
    if config.get(f"{name}_min") is None and config.get(f"{name}_max") is None:
        return np.full(market.shape, np.nan, dtype=np.float32)
    return market.field(name)


def _apply_bound(mask: np.ndarray, values: np.ndarray, config: dict, prefix: str) -> None:
    minimum = config.get(f"{prefix}_min")
    maximum = config.get(f"{prefix}_max")
    if minimum is not None and np.isfinite(values).any():
        mask &= values >= float(minimum)
    if maximum is not None and np.isfinite(values).any():
        mask &= values <= float(maximum)


def _symbol_in_boards(symbol: str, boards: list[str]) -> bool:
    for board in boards:
        if board == "沪主板" and symbol.startswith("60"):
            return True
        if board == "深主板" and symbol.startswith(("00", "001")):
            return True
        if board == "创业板" and symbol.startswith(("300", "301", "302")):
            return True
        if board == "科创板" and symbol.startswith(("688", "689")):
            return True
        if board == "北交所" and symbol.endswith(".BJ"):
            return True
    return False
