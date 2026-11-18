"""pipeline_indicators_core: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root


def _ema_alpha(span: int) -> float:
    return 2.0 / (span + 1)


def _math_half_up(expr: _root.pl.Expr, decimals: int = 2) -> _root.pl.Expr:
    """交易所四舍五入 (round half up)，替代 Python round()（银行家舍入）。

    round(2.625, 2) = 2.62  ← Python 银行家舍入
    exchange_round(2.625) = 2.63  ← 交易所四舍五入
    """

    factor = 10**decimals
    return (expr * factor + 0.5).floor() / factor


def _apply_adj_factor(raw: _root.pl.DataFrame, factors: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """对 raw K 线应用前复权 (forward adjustment)。

    adj_factor 结构: symbol, trade_date, ex_factor
    ex_factor 含义: 每次除权事件的 pre/post 比值(个股级,非累积)。

    前复权原理:
      - 保持最新价格不变,将历史价格向下调整以消除除权缺口
      - adjusted = raw × cumprod_at_D / total_cumprod
      - 等价于: adjusted = raw / (该日期之后所有事件的 ex_factor 乘积)
    """
    import app.indicators.pipeline as _root

    if factors.is_empty():
        return raw

    # 确保类型一致
    factors = (
        factors.with_columns(
            _root.pl.col("trade_date").cast(_root.pl.Date, strict=False),
            _root.pl.col("ex_factor").cast(_root.pl.Float64, strict=False),
        )
        .select("symbol", "trade_date", "ex_factor")
        .drop_nulls()
    )

    if factors.is_empty():
        return raw

    # 去重 + 排序 + 累积乘积 (一趟完成)
    factors_sorted = (
        factors.sort(["symbol", "trade_date"])
        .unique(subset=["symbol", "trade_date"])
        .sort(["symbol", "trade_date"])
        .with_columns(
            _root.pl.col("ex_factor").cum_prod().over("symbol").alias("cum_factor"),
        )
    )

    # 每个 symbol 的总累积因子
    total_factors = factors_sorted.group_by("symbol").agg(
        _root.pl.col("cum_factor").last().alias("total_factor")
    )

    raw_sorted = raw.sort(["symbol", "date"])

    # join_asof backward: 每根 K 线取 <= 其 date 的最新累积因子
    # 同时带 trade_date 列用于判断除权日标记
    df = raw_sorted.join_asof(
        factors_sorted.select("symbol", "trade_date", "cum_factor"),
        left_on="date",
        right_on="trade_date",
        by="symbol",
        strategy="backward",
    )

    # 补充 total_factor + 前复权 + 除权标记,一次 with_columns 完成
    df = df.join(total_factors, on="symbol", how="left")

    is_ex = _root.pl.col("trade_date") == _root.pl.col("date")
    ratio = _root.pl.col("cum_factor").fill_null(1.0) / _root.pl.col("total_factor").fill_null(1.0)
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]

    df = df.with_columns(
        [_root.pl.col(c) * ratio for c in price_cols]
        + [
            is_ex.alias("ex_rights"),
        ]
    ).drop(["trade_date", "cum_factor", "total_factor"])

    return df


def _resolve_needed(needed: set[str] | None) -> set[str]:
    """把 needed 展开为闭包 (含所依赖的中间列)。needed=None → 全集。"""
    import app.indicators.pipeline as _root

    if needed is None:
        return set(_root._ALL_INDICATOR_COLS)
    want = set(needed)
    changed = True
    while changed:
        changed = False
        for target in list(want):
            deps = _root._INDICATOR_DEPS.get(target)
            if deps and not deps <= want:
                want |= deps
                changed = True
    return want


def _ci_ma_exprs(want: set[str], prev_close: _root.pl.Expr) -> list[_root.pl.Expr]:
    """compute_indicators Pass 1: prev_close/MA 基础列表达式。"""
    import app.indicators.pipeline as _root

    exprs: list[_root.pl.Expr] = []
    if "prev_close" in want:
        exprs.append(prev_close.alias("prev_close"))
    if "ma5" in want:
        exprs.append(_root.pl.col("close").rolling_mean(5).over("symbol").alias("ma5"))
    if "ma10" in want:
        exprs.append(_root.pl.col("close").rolling_mean(10).over("symbol").alias("ma10"))
    if "ma20" in want:
        exprs.append(_root.pl.col("close").rolling_mean(20).over("symbol").alias("ma20"))
    if "ma30" in want:
        exprs.append(_root.pl.col("close").rolling_mean(30).over("symbol").alias("ma30"))
    if "ma60" in want:
        exprs.append(_root.pl.col("close").rolling_mean(60).over("symbol").alias("ma60"))
    return exprs


def _ci_ema_exprs(want: set[str]) -> list[_root.pl.Expr]:
    """compute_indicators Pass 1: EMA/MACD 基础列表达式。"""
    import app.indicators.pipeline as _root

    exprs: list[_root.pl.Expr] = []
    if "ema5" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(5), adjust=False)
            .over("symbol")
            .alias("ema5")
        )
    if "ema10" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(10), adjust=False)
            .over("symbol")
            .alias("ema10")
        )
    if "ema20" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(20), adjust=False)
            .over("symbol")
            .alias("ema20")
        )
    if "ema30" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(30), adjust=False)
            .over("symbol")
            .alias("ema30")
        )
    if "ema60" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(60), adjust=False)
            .over("symbol")
            .alias("ema60")
        )
    if "_ema12" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(12), adjust=False)
            .over("symbol")
            .alias("_ema12")
        )
    if "_ema26" in want:
        exprs.append(
            _root.pl.col("close")
            .ewm_mean(alpha=_root._ema_alpha(26), adjust=False)
            .over("symbol")
            .alias("_ema26")
        )
    return exprs


def _ci_p1_rest_exprs(want: set[str], prev_close: _root.pl.Expr) -> list[_root.pl.Expr]:
    """compute_indicators Pass 1 后半: BOLL/KDJ/ATR 基础 + 量价 + 极值表达式。"""
    import app.indicators.pipeline as _root

    exprs: list[_root.pl.Expr] = []
    if "_boll_std" in want:
        exprs.append(_root.pl.col("close").rolling_std(20).over("symbol").alias("_boll_std"))
    if "_kdj_ln" in want:
        exprs.append(_root.pl.col("low").rolling_min(9).over("symbol").alias("_kdj_ln"))
    if "_kdj_hn" in want:
        exprs.append(_root.pl.col("high").rolling_max(9).over("symbol").alias("_kdj_hn"))
    if "_tr" in want:
        exprs.append(
            _root.pl.max_horizontal(
                _root.pl.col("high") - _root.pl.col("low"),
                (_root.pl.col("high") - prev_close).abs(),
                (_root.pl.col("low") - prev_close).abs(),
            ).alias("_tr")
        )
    if "vol_ma5" in want:
        exprs.append(_root.pl.col("volume").rolling_mean(5).over("symbol").alias("vol_ma5"))
    if "vol_ma10" in want:
        exprs.append(_root.pl.col("volume").rolling_mean(10).over("symbol").alias("vol_ma10"))
    if "_vol_ma5" in want:
        exprs.append(_root.pl.col("volume").rolling_mean(5).over("symbol").alias("_vol_ma5"))
    if "vol_ratio_5d" in want:
        # 前5日平均成交量(不含当天), 标准量比分母: volume.shift(1).rolling_mean(5)
        exprs.append(
            _root.pl.col("volume").shift(1).rolling_mean(5).over("symbol").alias("_vol_ma5_prev")
        )
    if "high_60d" in want:
        exprs.append(_root.pl.col("close").rolling_max(60).over("symbol").alias("high_60d"))
    if "low_60d" in want:
        exprs.append(_root.pl.col("close").rolling_min(60).over("symbol").alias("low_60d"))
    return exprs


def _ci_pass1(df: _root.pl.DataFrame, want: set[str]) -> _root.pl.DataFrame:
    """compute_indicators Pass 1: 均线/EMA/MACD 基础/BOLL 基础/KDJ 基础/ATR 基础/量价/极值。"""
    import app.indicators.pipeline as _root

    prev_close = _root.pl.col("close").shift(1).over("symbol")
    exprs = (
        _root._ci_ma_exprs(want, prev_close)
        + _root._ci_ema_exprs(want)
        + _root._ci_p1_rest_exprs(want, prev_close)
    )
    return df.with_columns(exprs) if exprs else df


def _ci_pass2(df: _root.pl.DataFrame, want: set[str]) -> _root.pl.DataFrame:
    """compute_indicators Pass 2: MACD / BOLL 派生列。"""
    import app.indicators.pipeline as _root

    p2: list[_root.pl.Expr] = []
    if "macd_dif" in want:
        p2.append((_root.pl.col("_ema12") - _root.pl.col("_ema26")).alias("macd_dif"))
    if "boll_upper" in want:
        p2.append((_root.pl.col("ma20") + 2 * _root.pl.col("_boll_std")).alias("boll_upper"))
    if "boll_lower" in want:
        p2.append((_root.pl.col("ma20") - 2 * _root.pl.col("_boll_std")).alias("boll_lower"))
    if p2:
        df = df.with_columns(p2)
    if "macd_dea" in want:
        df = df.with_columns(
            _root.pl.col("macd_dif")
            .ewm_mean(alpha=_root._ema_alpha(9), adjust=False)
            .over("symbol")
            .alias("macd_dea"),
        )
    if "macd_hist" in want:
        df = df.with_columns(
            ((_root.pl.col("macd_dif") - _root.pl.col("macd_dea")) * 2).alias("macd_hist")
        )
    return df


def _ci_pass3(df: _root.pl.DataFrame, want: set[str]) -> _root.pl.DataFrame:
    """compute_indicators Pass 3: KDJ。"""
    import app.indicators.pipeline as _root

    if "kdj_k" in want:
        # 9 日内最高价=最低价时分母是 0 而不是空值, 0/0 得 NaN 会永久传染;
        # 该日 RSV 置空, EWM 跳过空值后继续递推 (与矩阵路径口径一致)。
        kdj_range = _root.pl.col("_kdj_hn") - _root.pl.col("_kdj_ln")
        kdj_rsv = _root.pl.when(kdj_range > 0).then(
            100 * (_root.pl.col("close") - _root.pl.col("_kdj_ln")) / kdj_range
        )
        df = df.with_columns(
            kdj_rsv.ewm_mean(alpha=1.0 / 3, adjust=False, ignore_nulls=True)
            .over("symbol")
            .alias("kdj_k")
        )
    if "kdj_d" in want:
        df = df.with_columns(
            _root.pl.col("kdj_k")
            .ewm_mean(alpha=1.0 / 3, adjust=False, ignore_nulls=True)
            .over("symbol")
            .alias("kdj_d")
        )
    if "kdj_j" in want:
        df = df.with_columns((3 * _root.pl.col("kdj_k") - 2 * _root.pl.col("kdj_d")).alias("kdj_j"))
    return df


def _ci_pass4(df: _root.pl.DataFrame, want: set[str]) -> _root.pl.DataFrame:
    """compute_indicators Pass 4: ATR / 量比 / 动量 / 涨跌幅 / 振幅 / 波动。"""
    import app.indicators.pipeline as _root

    if "atr_14" in want:
        df = df.with_columns(
            _root.pl.col("_tr")
            .ewm_mean(alpha=1.0 / 14, adjust=False)
            .over("symbol")
            .alias("atr_14"),
        )
    if "vol_ratio_5d" in want:
        # 盘中快照量按已交易分钟折算到全天量级, 与实时增量路径同口径,
        # 否则量比被低估约 240/elapsed 倍。
        from app.market_time import cn_today, trading_minutes_elapsed

        vr = _root.pl.col("volume") / _root.pl.col("_vol_ma5_prev")
        elapsed = trading_minutes_elapsed()
        if 0 < elapsed < 240 and "date" in df.columns and df["date"].max() == cn_today():
            vr = (
                _root.pl.when(_root.pl.col("date") == cn_today())
                .then(_root.pl.col("volume") * (240.0 / elapsed) / _root.pl.col("_vol_ma5_prev"))
                .otherwise(vr)
            )
        df = df.with_columns(vr.alias("vol_ratio_5d"))
    p4mom: list[_root.pl.Expr] = []
    for n in (5, 10, 20, 30, 60):
        name = f"momentum_{n}d"
        if name in want:
            p4mom.append(
                (_root.pl.col("close") / _root.pl.col("close").shift(n).over("symbol") - 1).alias(
                    name
                )
            )
    if "change_pct" in want:
        p4mom.append(
            (_root.pl.col("close") / _root.pl.col("close").shift(1).over("symbol") - 1).alias(
                "change_pct"
            )
        )
    if p4mom:
        df = df.with_columns(p4mom)
    if "change_amount" in want:
        df = df.with_columns(
            (_root.pl.col("close") - _root.pl.col("close").shift(1).over("symbol")).alias(
                "change_amount"
            ),
        )
    if "amplitude" in want:
        df = df.with_columns(
            _root.pl.when(_root.pl.col("close").shift(1).over("symbol") > 0)
            .then(
                (_root.pl.col("high") - _root.pl.col("low"))
                / _root.pl.col("close").shift(1).over("symbol")
            )
            .otherwise(None)
            .alias("amplitude"),
        )
    if "_daily_pct" in want:
        df = df.with_columns(_root.pl.col("close").pct_change().over("symbol").alias("_daily_pct"))
    if "annual_vol_20d" in want:
        df = df.with_columns(
            (_root.pl.col("_daily_pct").rolling_std(20).over("symbol") * (252**0.5)).alias(
                "annual_vol_20d"
            ),
        )
    return df


def _ci_pass5(df: _root.pl.DataFrame, want: set[str]) -> _root.pl.DataFrame:
    """compute_indicators Pass 5: RSI (6/14/24)。"""
    import app.indicators.pipeline as _root

    if not want & {"rsi_6", "rsi_14", "rsi_24"}:
        return df
    df = df.with_columns(_root.pl.col("close").diff().over("symbol").alias("_delta")).with_columns(
        [
            _root.pl.when(_root.pl.col("_delta") > 0)
            .then(_root.pl.col("_delta"))
            .otherwise(0.0)
            .alias("_gain"),
            _root.pl.when(_root.pl.col("_delta") < 0)
            .then(-_root.pl.col("_delta"))
            .otherwise(0.0)
            .alias("_loss"),
        ]
    )
    for n in (6, 14, 24):
        if f"rsi_{n}" not in want:
            continue
        a = 1.0 / n
        df = df.with_columns(
            [
                _root.pl.col("_gain")
                .ewm_mean(alpha=a, adjust=False)
                .over("symbol")
                .alias(f"_rsi_avg_gain_{n}"),
                _root.pl.col("_loss")
                .ewm_mean(alpha=a, adjust=False)
                .over("symbol")
                .alias(f"_rsi_avg_loss_{n}"),
            ]
        ).with_columns(
            (
                100
                - 100
                / (
                    1
                    + _root.pl.col(f"_rsi_avg_gain_{n}")
                    / _root.pl.when(_root.pl.col(f"_rsi_avg_loss_{n}") == 0)
                    .then(1e-12)
                    .otherwise(_root.pl.col(f"_rsi_avg_loss_{n}"))
                )
            ).alias(f"rsi_{n}"),
        )
    return df


def compute_indicators(
    df: _root.pl.DataFrame,
    needed: set[str] | None = None,
    *,
    assume_sorted: bool = False,
) -> _root.pl.DataFrame:
    """从 OHLCV 数据计算全套技术指标。

    输入必须包含: symbol, date, open, high, low, close, volume
    返回添加了所有指标列的 DataFrame。

    needed:
        None (默认) — 计算全部指标, 行为与历史逐位一致 (所有 gate 为真, 表达式/顺序不变)。
        列名集合   — 仅计算这些列及其依赖闭包, 跳过无关的 EMA/KDJ/RSI 等 pass;
                     输出保留输入列 + 所需指标列。被保留列的数值与全量计算逐位一致
                     (逐列 window/rolling 相互独立, 跳过其它列不影响保留列)。
    """
    import app.indicators.pipeline as _root

    if df.is_empty():
        return df

    import time as _time

    _t0 = _time.perf_counter()

    want = _root._resolve_needed(needed)

    df = df if assume_sorted else df.sort(["symbol", "date"])

    # Pass 1: 均线 + EMA + MACD 基础 + BOLL 基础 + KDJ 基础 + ATR 基础 + 量价 + 极值
    df = _root._ci_pass1(df, want)

    # Pass 2: MACD + BOLL (基于 Pass 1 基础列)
    df = _root._ci_pass2(df, want)

    # Pass 3: KDJ
    df = _root._ci_pass3(df, want)

    # Pass 4: ATR + 量比 + 动量 + 波动 + 涨跌幅 + 涨跌额 + 振幅
    df = _root._ci_pass4(df, want)

    # Pass 5: RSI
    df = _root._ci_pass5(df, want)

    # Pass 6: 换手率 (需要 float_shares, 后续在 compute_all 中 JOIN instruments 后补充)

    # 清理临时列 (只丢弃实际存在的临时列)
    _temp_cols = [
        "_boll_std",
        "_tr",
        "_ema12",
        "_ema26",
        "_kdj_ln",
        "_kdj_hn",
        "_vol_ma5",
        "_vol_ma5_prev",
        "_daily_pct",
        "_delta",
        "_gain",
        "_loss",
        "_rsi_avg_gain_6",
        "_rsi_avg_loss_6",
        "_rsi_avg_gain_14",
        "_rsi_avg_loss_14",
        "_rsi_avg_gain_24",
        "_rsi_avg_loss_24",
    ]
    df = df.drop([c for c in _temp_cols if c in df.columns])

    _elapsed = (_time.perf_counter() - _t0) * 1000
    import logging as _logging

    _logging.getLogger(__name__).debug("compute_indicators: %.1fms, %d rows", _elapsed, len(df))

    return df
