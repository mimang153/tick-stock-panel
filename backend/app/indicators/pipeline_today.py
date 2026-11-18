"""pipeline_today: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root

# ================================================================
# 盘中增量计算: 只算今天 5500 行 (不复算历史)
# ================================================================


def _cet_prepare(
    live_agg: _root.pl.DataFrame, today_ohlcv: _root.pl.DataFrame
) -> tuple[_root.pl.DataFrame, _root.pl.Expr]:
    """盘中帧准备: JOIN 递推状态 + 前复权 + 基础涨跌 (保留原始价列)。"""
    import app.indicators.pipeline as _root

    # 当日行情是主表, 复牌/新上市股票不能因无历史状态被静默删除。
    live_state = live_agg.with_columns(_root.pl.lit(True).alias("_has_history_state"))
    df = today_ohlcv.join(live_state, on="symbol", how="left")
    has_history_state = _root.pl.col("_has_history_state").fill_null(False)
    df = df.with_columns(
        [
            _root.pl.col("close").alias("raw_close"),
            _root.pl.col("high").alias("raw_high"),
            _root.pl.col("low").alias("raw_low"),
        ]
    )
    if "_adj_factor" in df.columns:
        af = _root.pl.col("_adj_factor").fill_null(1.0)
        df = df.with_columns(
            [
                (_root.pl.col("open") * af).alias("open"),
                (_root.pl.col("high") * af).alias("high"),
                (_root.pl.col("low") * af).alias("low"),
                (_root.pl.col("close") * af).alias("close"),
            ]
        )
    df = df.with_columns(_root.pl.col("volume").cast(_root.pl.Float64))
    df = df.with_columns(_root.pl.lit(False).alias("ex_rights"))
    if "prev_close" not in df.columns:
        prev_close = (
            _root.pl.col("close_right") if "close_right" in df.columns else _root.pl.col("close")
        )
        df = df.with_columns(prev_close.alias("prev_close"))
    elif "_adj_factor" in df.columns:
        df = df.with_columns(_root.pl.col("prev_close").alias("_prev_close_raw"))
        df = df.with_columns(
            (_root.pl.col("prev_close") * _root.pl.col("_adj_factor").fill_null(1.0)).alias(
                "prev_close"
            )
        )
    if "change_pct" not in df.columns:
        df = df.with_columns(
            (_root.pl.col("close") / _root.pl.col("prev_close") - 1).alias("change_pct")
        )
    if "change_amount" not in df.columns:
        df = df.with_columns(
            (_root.pl.col("close") - _root.pl.col("prev_close")).alias("change_amount")
        )
    if "amplitude" not in df.columns:
        df = df.with_columns(
            _root.pl.when(_root.pl.col("prev_close") > 0)
            .then((_root.pl.col("high") - _root.pl.col("low")) / _root.pl.col("prev_close"))
            .otherwise(None)
            .alias("amplitude"),
        )
    return df, has_history_state


def _cet_trend(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """递推 EMA / MACD / MA (用部分和)。"""
    import app.indicators.pipeline as _root

    alpha = _root._ema_alpha
    df = df.with_columns(
        [
            (alpha(5) * _root.pl.col("close") + (1 - alpha(5)) * _root.pl.col("ema5")).alias(
                "ema5"
            ),
            (alpha(10) * _root.pl.col("close") + (1 - alpha(10)) * _root.pl.col("ema10")).alias(
                "ema10"
            ),
            (alpha(20) * _root.pl.col("close") + (1 - alpha(20)) * _root.pl.col("ema20")).alias(
                "ema20"
            ),
            (alpha(30) * _root.pl.col("close") + (1 - alpha(30)) * _root.pl.col("ema30")).alias(
                "ema30"
            ),
            (alpha(60) * _root.pl.col("close") + (1 - alpha(60)) * _root.pl.col("ema60")).alias(
                "ema60"
            ),
        ]
    )
    ema12 = alpha(12) * _root.pl.col("close") + (1 - alpha(12)) * _root.pl.col("_ema12")
    ema26 = alpha(26) * _root.pl.col("close") + (1 - alpha(26)) * _root.pl.col("_ema26")
    dif = ema12 - ema26
    dea = alpha(9) * dif + (1 - alpha(9)) * _root.pl.col("macd_dea")
    df = df.with_columns(
        [dif.alias("macd_dif"), dea.alias("macd_dea"), ((dif - dea) * 2).alias("macd_hist")]
    )
    return df.with_columns(
        [
            ((_root.pl.col("_ma5_partial_sum") + _root.pl.col("close")) / 5).alias("ma5"),
            ((_root.pl.col("_ma10_partial_sum") + _root.pl.col("close")) / 10).alias("ma10"),
            ((_root.pl.col("_ma20_partial_sum") + _root.pl.col("close")) / 20).alias("ma20"),
            ((_root.pl.col("_ma30_partial_sum") + _root.pl.col("close")) / 30).alias("ma30"),
            ((_root.pl.col("_ma60_partial_sum") + _root.pl.col("close")) / 60).alias("ma60"),
        ]
    )


def _cet_oscillators(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """递推 BOLL / KDJ / ATR / RSI。"""
    import app.indicators.pipeline as _root

    boll_sum = _root.pl.col("_boll_partial_sum") + _root.pl.col("close")
    boll_sq_sum = _root.pl.col("_boll_partial_sq_sum") + _root.pl.col("close") ** 2
    boll_ma = boll_sum / 20
    boll_var = (boll_sq_sum - boll_sum**2 / 20) / 19  # 样本方差 ddof=1
    boll_std = _root.pl.when(boll_var > 0).then(boll_var.sqrt()).otherwise(0.0)
    df = df.with_columns(
        [(boll_ma + 2 * boll_std).alias("boll_upper"), (boll_ma - 2 * boll_std).alias("boll_lower")]
    )
    kdj_ln = _root.pl.min_horizontal(_root.pl.col("_kdj_8d_low"), _root.pl.col("low"))
    kdj_hn = _root.pl.max_horizontal(_root.pl.col("_kdj_8d_high"), _root.pl.col("high"))
    rsv = (_root.pl.col("close") - kdj_ln) / (kdj_hn - kdj_ln).fill_null(1e-12) * 100
    k_today = rsv / 3 + _root.pl.col("kdj_k") * 2 / 3
    d_today = k_today / 3 + _root.pl.col("kdj_d") * 2 / 3
    df = df.with_columns(
        [k_today.alias("kdj_k"), d_today.alias("kdj_d"), (3 * k_today - 2 * d_today).alias("kdj_j")]
    )
    tr = _root.pl.max_horizontal(
        _root.pl.col("high") - _root.pl.col("low"),
        (_root.pl.col("high") - _root.pl.col("prev_close")).abs(),
        (_root.pl.col("low") - _root.pl.col("prev_close")).abs(),
    )
    df = df.with_columns((tr / 14 + _root.pl.col("atr_14") * 13 / 14).alias("atr_14"))
    delta = _root.pl.col("close") - _root.pl.col("prev_close")
    gain = _root.pl.when(delta > 0).then(delta).otherwise(0.0)
    loss = _root.pl.when(delta < 0).then(-delta).otherwise(0.0)
    for n in (6, 14, 24):
        a = 1.0 / n
        avg_gain = (1 - a) * _root.pl.col(f"_rsi_avg_gain_{n}") + a * gain
        avg_loss = (1 - a) * _root.pl.col(f"_rsi_avg_loss_{n}") + a * loss
        df = df.with_columns(
            [
                avg_gain.alias(f"_rsi_avg_gain_{n}"),
                avg_loss.alias(f"_rsi_avg_loss_{n}"),
                (
                    100
                    - 100
                    / (1 + avg_gain / _root.pl.when(avg_loss == 0).then(1e-12).otherwise(avg_loss))
                ).alias(f"rsi_{n}"),
            ]
        )
    return df


def _cet_volume_momentum(
    df: _root.pl.DataFrame, has_history_state: _root.pl.Expr, elapsed_minutes: float | None
) -> _root.pl.DataFrame:
    """递推量比 / 极值 / 动量 / 年化波动率。"""
    import app.indicators.pipeline as _root

    vol_ma5 = (_root.pl.col("_vol_ma5_partial_sum") + _root.pl.col("volume")) / 5
    vol_ma10 = (_root.pl.col("_vol_ma10_partial_sum") + _root.pl.col("volume")) / 10
    vol_ma5_prev = _root.pl.col("_vol_ma5_prev_sum") / 5
    if elapsed_minutes and elapsed_minutes > 0:  # noqa: SIM108 - if/else 分支含副作用, 不宜三元化
        time_factor = 240.0 / elapsed_minutes
    else:
        time_factor = 1.0
    df = df.with_columns(
        [
            vol_ma5.alias("vol_ma5"),
            vol_ma10.alias("vol_ma10"),
            ((_root.pl.col("volume") * time_factor) / vol_ma5_prev).alias("vol_ratio_5d"),
        ]
    )
    df = df.with_columns(
        [
            _root.pl.when(has_history_state)
            .then(_root.pl.max_horizontal(_root.pl.col("_high_59d"), _root.pl.col("close")))
            .otherwise(None)
            .alias("high_60d"),
            _root.pl.when(has_history_state)
            .then(_root.pl.min_horizontal(_root.pl.col("_low_59d"), _root.pl.col("close")))
            .otherwise(None)
            .alias("low_60d"),
        ]
    )
    for n in (5, 10, 20, 30, 60):
        df = df.with_columns(
            (_root.pl.col("close") / _root.pl.col(f"_close_{n}d_ago") - 1).alias(f"momentum_{n}d")
        )
    if "_close_3d_ago" in df.columns:
        df = df.with_columns(
            (_root.pl.col("close") / _root.pl.col("_close_3d_ago") - 1).alias("momentum_3d")
        )
    today_ret = _root.pl.col("close") / _root.pl.col("prev_close") - 1
    total_sum = _root.pl.col("_vol_19d_pct_sum").fill_null(0.0) + today_ret
    total_sq_sum = _root.pl.col("_vol_19d_pct_sq_sum").fill_null(0.0) + today_ret**2
    vol_var = (total_sq_sum - total_sum**2 / 20) / 19  # 样本方差 ddof=1
    return df.with_columns(
        _root.pl.when(has_history_state & (vol_var > 0))
        .then(vol_var.sqrt() * (252**0.5))
        .otherwise(None)
        .alias("annual_vol_20d"),
    )


def _cet_window_gate(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """窗口不满置空: live_agg 的 tail(N) 在次新股上取到残缺窗口, 同口径置空。"""
    import app.indicators.pipeline as _root

    min_history_bars = {
        "ma5": 4,
        "ma10": 9,
        "ma20": 19,
        "ma30": 29,
        "ma60": 59,
        "vol_ma5": 4,
        "vol_ma10": 9,
        "vol_ratio_5d": 5,
        "boll_upper": 19,
        "boll_lower": 19,
        "high_60d": 59,
        "low_60d": 59,
        "momentum_3d": 3,
        "momentum_5d": 5,
        "momentum_10d": 10,
        "momentum_20d": 20,
        "momentum_30d": 30,
        "momentum_60d": 60,
        "annual_vol_20d": 20,
    }
    if "_window_len" in df.columns:
        df = df.with_columns(
            [
                _root.pl.when(_root.pl.col("_window_len") >= need)
                .then(_root.pl.col(column))
                .otherwise(None)
                .alias(column)
                for column, need in min_history_bars.items()
                if column in df.columns
            ]
        )
    return df


def _cet_prev_fields(
    df: _root.pl.DataFrame, prev_enriched: _root.pl.DataFrame
) -> _root.pl.DataFrame:
    """昨日指标值 join (交叉类自定义信号的前1日条件用)。"""
    import app.indicators.pipeline as _root

    prev_fields = _root._custom_prev_fields_today()
    if prev_enriched.is_empty() or not prev_fields:
        return df
    available = [f for f in sorted(prev_fields) if f in prev_enriched.columns]
    if not available:
        return df
    sig_prev = prev_enriched.select(
        "symbol", *[_root.pl.col(f).alias(f"_prev_{f}") for f in available]
    )
    return df.join(sig_prev, on="symbol", how="left")


def _cet_cleanup(
    df: _root.pl.DataFrame, instruments: _root.pl.DataFrame | None
) -> _root.pl.DataFrame:
    """涨跌停判定价 + 换手率 + 连板计算, 并清理内部列。"""
    import app.indicators.pipeline as _root

    if instruments is not None and not instruments.is_empty():
        df = _root._compute_limit_signals_today(df, instruments)
    drop_cols = [
        "close_right",
        "high_right",
        "low_right",
        "_prev_close_raw",
        "_ma5_partial_sum",
        "_ma10_partial_sum",
        "_ma20_partial_sum",
        "_ma30_partial_sum",
        "_ma60_partial_sum",
        "_boll_partial_sum",
        "_boll_partial_sq_sum",
        "_high_59d",
        "_low_59d",
        "_close_5d_ago",
        "_close_10d_ago",
        "_close_20d_ago",
        "_close_30d_ago",
        "_close_60d_ago",
        "_vol_ma5_partial_sum",
        "_vol_ma10_partial_sum",
        "_vol_ma5_prev_sum",
        "_kdj_8d_low",
        "_kdj_8d_high",
        "_window_len",
        "_rsi_avg_gain_6",
        "_rsi_avg_loss_6",
        "_rsi_avg_gain_14",
        "_rsi_avg_loss_14",
        "_rsi_avg_gain_24",
        "_rsi_avg_loss_24",
        "_ema12",
        "_ema26",
        "_adj_factor",
        "_vol_19d_pct_sum",
        "_vol_19d_pct_sq_sum",
        "_prev_consec_up",
        "_prev_consec_down",
        "_has_history_state",
    ]
    return df.drop([c for c in drop_cols if c in df.columns])


def _cet_finalize(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """扩展表/自定义信号注入 + 昨日值列清理 + NaN/Inf 清理。"""
    import app.indicators.pipeline as _root
    from app.factors import ext_factors
    from app.strategy import custom_signals

    df = ext_factors.attach_ext_columns(df, include_snapshot=True)
    df = custom_signals.inject(df, _root._get_custom_signal_exprs_today())
    df = df.drop(
        [
            c
            for c in df.columns
            if c.startswith("_prev_") and c not in {"_prev_consec_up", "_prev_consec_down"}
        ]
    )
    float_cols = [c for c in df.columns if df[c].dtype.is_float()]
    if float_cols:
        df = df.with_columns(
            [
                _root.pl.when(_root.pl.col(c).is_nan() | _root.pl.col(c).is_infinite())
                .then(None)
                .otherwise(_root.pl.col(c))
                .alias(c)
                for c in float_cols
            ]
        )
    return df


def compute_enriched_today(
    live_agg: _root.pl.DataFrame,
    prev_enriched: _root.pl.DataFrame,
    today_ohlcv: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame | None = None,
    elapsed_minutes: float | None = None,
) -> _root.pl.DataFrame:
    """用昨天的递推状态 + 今天的 OHLCV 增量计算今天的 enriched 数据。

    只处理 ~5500 行, 耗时 ~10-50ms (替代全量 compute_enriched 的 1.5-2s)。

    参数:
        live_agg:       repo.get_live_agg() — 包含所有递推状态 + 窗口聚合
        prev_enriched:  repo.get_enriched_latest() — 昨天的完整 enriched (用于信号交叉判断)
        today_ohlcv:    今天的 OHLCV (symbol, date, open, high, low, close, volume, amount)
        instruments:    维表 (涨跌停/换手率需要)
        elapsed_minutes: 当日已交易分钟数(用于标准量比的时间折算)。
            None 或 0 表示不折算(盘后或时间不可用, 此时 volume 已是全天量)。

    返回:
        今天的 enriched DataFrame (~5500 行, 64 列)
    """
    import app.indicators.pipeline as _root

    if today_ohlcv.is_empty() or live_agg.is_empty():
        return _root.pl.DataFrame()

    df, has_history_state = _root._cet_prepare(live_agg, today_ohlcv)

    df = _root._cet_trend(df)
    df = _root._cet_oscillators(df)
    df = _root._cet_volume_momentum(df, has_history_state, elapsed_minutes)

    df = _root._cet_window_gate(df)
    df = _root._cet_prev_fields(df, prev_enriched)

    df = _root._cet_cleanup(df, instruments)
    return _root._cet_finalize(df)


def _clst_prepare_instruments(instruments: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """盘中版 instruments 子集: ST 标记 + 无涨跌幅窗口。"""
    import app.indicators.pipeline as _root

    inst_cols = ["symbol"]
    for c in ["float_shares", "limit_up", "limit_down", "listing_date"]:
        if c in instruments.columns:
            inst_cols.append(c)  # noqa: PERF401 - 循环内含多行处理, 改写推导式损可读性
    if "as_of" in instruments.columns:
        inst_cols.append(
            _root.pl.col("as_of").cast(_root.pl.Date, strict=False).alias("_instrument_as_of")
        )
    inst_subset = instruments.select(inst_cols).unique(subset=["symbol"])
    if "name" in instruments.columns:
        st_flag = instruments.select(
            "symbol",
            _root.polars_is_risk_warning_name(_root.pl.col("name")).alias("_is_st"),
        ).unique(subset=["symbol"])
        inst_subset = inst_subset.join(st_flag, on="symbol", how="left")
    return _root._attach_no_limit_window(inst_subset)


def _clst_turnover(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """换手率: API 有则直接用, 无则从 float_shares 计算。"""
    import app.indicators.pipeline as _root

    if "turnover_rate" not in df.columns:  # noqa: SIM102 - 嵌套if表达独立前置条件, 合并损可读性
        if "float_shares" in df.columns and "volume" in df.columns:
            df = df.with_columns(
                _root.pl.when(_root.pl.col("float_shares") > 0)
                .then(_root.pl.col("volume") * 10000.0 / _root.pl.col("float_shares"))
                .otherwise(None)
                .alias("turnover_rate")
            )
    return df


def _clst_basis(
    df: _root.pl.DataFrame,
) -> tuple[_root.pl.Expr, _root.pl.Expr, _root.pl.Expr, _root.pl.Expr]:
    """盘中版参考昨收 + 涨跌幅比例 + 理论涨跌停价表达式。"""
    import app.indicators.pipeline as _root

    if "_prev_close_raw" in df.columns:
        if "close_right" in df.columns:
            prev_raw = (
                _root.pl.when(_root.pl.col("_prev_close_raw").is_not_null())
                .then(_root.pl.col("_prev_close_raw"))
                .otherwise(_root.pl.col("close_right"))
            )
        else:
            prev_raw = _root.pl.col("_prev_close_raw")
    elif "close_right" in df.columns:
        prev_raw = _root.pl.col("close_right")
    else:
        prev_raw = _root.pl.col("raw_close")
    is_risk_warning = _root.pl.col("_is_st") if "_is_st" in df.columns else _root.pl.lit(False)
    trade_date = _root.pl.col("date") if "date" in df.columns else _root.pl.lit(_root.cn_today())
    limit_pct = _root.polars_price_limit_pct(
        _root.pl.col("symbol"), trade_date, is_risk_warning
    ).alias("_limit_pct")
    return (
        prev_raw,
        _root.polars_limit_price(prev_raw, limit_pct, up=True),
        _root.polars_limit_price(prev_raw, limit_pct, up=False),
        trade_date,
    )


def _clst_effective_prices(
    df: _root.pl.DataFrame,
    prev_raw: _root.pl.Expr,
    limit_up_price: _root.pl.Expr,
    limit_down_price: _root.pl.Expr,
    trade_date: _root.pl.Expr,
) -> _root.pl.DataFrame:
    """盘中版生效涨跌停价 + 无涨跌幅限制 + 判定价 (含容差)。"""
    import app.indicators.pipeline as _root

    _SENTINEL = 10000.0  # noqa: N806 - N806: 大写名为领域约定常量语义
    authoritative_date = (
        _root.pl.col("_instrument_as_of") == trade_date.cast(_root.pl.Date, strict=False)
        if "_instrument_as_of" in df.columns
        else _root.pl.lit(True)
    )
    has_authoritative_up = _root.pl.lit(False)
    has_authoritative_down = _root.pl.lit(False)
    no_price_limit = _root.pl.lit(False)
    if "limit_up" in df.columns:
        has_authoritative_up = (
            authoritative_date
            & _root.pl.col("limit_up").is_not_null()
            & (_root.pl.col("limit_up") > 0)
            & (_root.pl.col("limit_up") < _SENTINEL)
        )
        no_price_limit = (
            authoritative_date
            & _root.pl.col("limit_up").is_not_null()
            & (_root.pl.col("limit_up") >= _SENTINEL)
        )
        effective_limit_up = (
            _root.pl.when(has_authoritative_up)
            .then(_root.pl.col("limit_up"))
            .otherwise(limit_up_price)
        )
    else:
        effective_limit_up = limit_up_price
    if "limit_down" in df.columns:
        has_authoritative_down = (
            authoritative_date
            & _root.pl.col("limit_down").is_not_null()
            & (_root.pl.col("limit_down") > 0)
            & (_root.pl.col("limit_down") < _SENTINEL)
        )
        effective_limit_down = (
            _root.pl.when(has_authoritative_down)
            .then(_root.pl.col("limit_down"))
            .otherwise(limit_down_price)
        )
    else:
        effective_limit_down = limit_down_price
    if "_no_limit_until" in df.columns:
        window_no_limit = (_root.pl.col("_no_limit_days").fill_null(0) > 0) & (
            trade_date.cast(_root.pl.Date, strict=False) <= _root.pl.col("_no_limit_until")
        )
        no_price_limit = no_price_limit | window_no_limit.fill_null(False)
    valid_prev_raw = prev_raw.is_not_null() & (prev_raw > 0)
    up_ok = valid_prev_raw | has_authoritative_up
    down_ok = valid_prev_raw | has_authoritative_down
    return df.with_columns(
        [
            _root.pl.when(no_price_limit | ~up_ok)
            .then(None)
            .otherwise(effective_limit_up - 0.005)
            .alias("limit_up_price"),
            _root.pl.when(no_price_limit | ~down_ok)
            .then(None)
            .otherwise(effective_limit_down + 0.005)
            .alias("limit_down_price"),
        ]
    )


def _clst_consecutive(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """盘中版连板数: 同向 +1, 不同向归零。"""
    import app.indicators.pipeline as _root

    is_limit_up = (
        _root.pl.when(_root.pl.col("limit_up_price").is_null())
        .then(None)
        .when(_root.pl.col("raw_close") > 0)
        .then(_root.pl.col("raw_close") >= _root.pl.col("limit_up_price"))
        .otherwise(None)
        .cast(_root.pl.Boolean)
    )
    is_limit_down = (
        _root.pl.when(_root.pl.col("limit_down_price").is_null())
        .then(None)
        .when(_root.pl.col("raw_close") > 0)
        .then(_root.pl.col("raw_close") <= _root.pl.col("limit_down_price"))
        .otherwise(None)
        .cast(_root.pl.Boolean)
    )
    if "_prev_consec_up" not in df.columns:
        df = df.with_columns(_root.pl.lit(0).cast(_root.pl.UInt32).alias("_prev_consec_up"))
    if "_prev_consec_down" not in df.columns:
        df = df.with_columns(_root.pl.lit(0).cast(_root.pl.UInt32).alias("_prev_consec_down"))
    prev_up = _root.pl.col("_prev_consec_up").fill_null(0).cast(_root.pl.UInt32)
    prev_down = _root.pl.col("_prev_consec_down").fill_null(0).cast(_root.pl.UInt32)
    return df.with_columns(
        [
            _root.pl.when(is_limit_up.fill_null(False))
            .then((prev_up + 1).cast(_root.pl.UInt32))
            .otherwise(_root.pl.lit(0).cast(_root.pl.UInt32))
            .alias("consecutive_limit_ups"),
            _root.pl.when(is_limit_down.fill_null(False))
            .then((prev_down + 1).cast(_root.pl.UInt32))
            .otherwise(_root.pl.lit(0).cast(_root.pl.UInt32))
            .alias("consecutive_limit_downs"),
        ]
    )


def _clst_cleanup(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """盘中版临时列清理。"""

    cleanup = [
        "_limit_pct",
        "_is_st",
        "limit_up",
        "limit_down",
        "_instrument_as_of",
        "_no_limit_days",
        "_no_limit_until",
        "listing_date",
    ]
    for c in df.columns:
        if c.endswith("_inst"):
            cleanup.append(c)  # noqa: PERF401 - 循环内含多行处理, 改写推导式损可读性
    for c in ["name", "float_shares"]:
        if c in df.columns:
            cleanup.append(c)  # noqa: PERF401 - 循环内含多行处理, 改写推导式损可读性
    return df.drop([c for c in cleanup if c in df.columns])


def _compute_limit_signals_today(
    df: _root.pl.DataFrame, instruments: _root.pl.DataFrame
) -> _root.pl.DataFrame:
    """盘中增量版的涨跌停/换手率/炸板/连板计算。"""
    import app.indicators.pipeline as _root

    df = df.join(
        _root._clst_prepare_instruments(instruments), on="symbol", how="left", suffix="_inst"
    )
    df = _root._clst_turnover(df)
    prev_raw, up_price, down_price, trade_date = _root._clst_basis(df)
    df = _root._clst_effective_prices(df, prev_raw, up_price, down_price, trade_date)
    df = _root._clst_consecutive(df)
    return _root._clst_cleanup(df)
    return _root._clst_cleanup(df)
