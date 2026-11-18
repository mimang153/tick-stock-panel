"""pipeline_signals: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root


def get_signal_dependencies() -> dict[str, frozenset[str]]:
    """返回 JSON 自定义信号(含迁移自内置的 signal_* 定义)的唯一依赖映射。"""
    import app.indicators.pipeline as _root
    from app.strategy import custom_signals

    return custom_signals.expression_dependencies(_root._get_custom_signal_exprs())


def _split_custom_exprs_by_limit_deps(
    exprs: dict[str, _root.pl.Expr],
) -> tuple[dict[str, _root.pl.Expr], dict[str, _root.pl.Expr]]:
    """按根依赖是否含涨跌停判定价列, 把自定义信号表达式拆成 (前置, 后置)。

    后置组必须在 compute_limit_signals 产出 limit_*_price 列之后注入。
    """
    import app.indicators.pipeline as _root
    from app.strategy import custom_signals

    pre: dict[str, _root.pl.Expr] = {}
    post: dict[str, _root.pl.Expr] = {}
    for name, expr in exprs.items():
        roots = custom_signals.expression_dependencies({name: expr})[name]
        (post if roots & _root._LIMIT_PRICE_COLUMNS else pre)[name] = expr
    return pre, post


def _needed_references_limit_prices(needed: set[str]) -> bool:
    """请求列里是否有自定义信号(定义)的根依赖引用涨跌停判定价。"""
    import app.indicators.pipeline as _root
    from app.strategy import custom_signals

    deps = custom_signals.expression_dependencies(_root._get_custom_signal_exprs())
    for name in needed:
        dep = deps.get(name)
        if dep and (dep & _root._LIMIT_PRICE_COLUMNS):
            return True
    return False


def compute_signals(
    df: _root.pl.DataFrame,
    needed: set[str] | None = None,
    *,
    attach_extensions: bool = True,
) -> _root.pl.DataFrame:
    """注入自定义信号布尔列 (含迁移自内置的 signal_* 定义)。

    输入必须包含 compute_indicators() 产出的指标列。
    """
    import app.indicators.pipeline as _root

    if df.is_empty():
        return df

    # 自定义信号（用户配置的字段+运算符+值组合，编译为布尔列）。
    # 扩展表数值列先行 join (ext_ 因子列 = 帧上已有列): 信号条件与评分引用
    # 都按列存在性解析。历史多日帧仅注入时序模式 —— 快照代表"最新值",
    # 历史回看注入会引入未来数据 (CONTRIBUTING §5.3)。
    from app.factors import ext_factors

    if attach_extensions:
        df = ext_factors.attach_ext_columns(df, include_snapshot=False)
    # 条件引用的注册表因子列先复用评分物化管线补算 (虚拟/自定义/复合均可)。
    from app.strategy import custom_signals

    exprs = _root._get_custom_signal_exprs()
    # 依赖涨跌停判定价的信号 (signal_limit_* 等) 由 compute_limit_signals 末尾注入,
    # 此处只注入指标列可满足的部分 (避免每帧告警缺列)。
    pre, _post = _root._split_custom_exprs_by_limit_deps(exprs)
    df = custom_signals.materialize_factor_columns(df, pre, needed=needed)
    df = custom_signals.inject(df, pre, needed=needed)

    return df


def _attach_no_limit_window(inst_subset: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """给 instruments 子集附上无涨跌幅窗口列 (单一事实源: app.price_limits)。

    _no_limit_days: 窗口交易日数 (0/null = 不适用, 改革前老股或无上市日)
    _no_limit_until: 窗口边界日历日 = listing_date + 保守边际 (供 date <= 边界
                    的向量化比较; 精确交易日序号门控由调用方补足)
    """
    import app.indicators.pipeline as _root

    if inst_subset.is_empty() or "listing_date" not in inst_subset.columns:
        return inst_subset
    info: list[dict] = []
    for sym, raw in zip(
        inst_subset["symbol"].to_list(),
        inst_subset["listing_date"].to_list(),
        strict=True,
    ):
        listing = _root.parse_listing_date(raw)
        days = _root.no_limit_window_days(sym, listing)
        if days > 0 and listing is not None:
            info.append(
                {
                    "symbol": sym,
                    "_no_limit_days": days,
                    "_no_limit_until": listing
                    + _root._timedelta(days=_root.no_limit_window_margin_days(sym)),
                }
            )
    if not info:
        return inst_subset
    return inst_subset.join(
        _root.pl.DataFrame(
            info,
            schema={
                "symbol": _root.pl.Utf8,
                "_no_limit_days": _root.pl.Int64,
                "_no_limit_until": _root.pl.Date,
            },
        ),
        on="symbol",
        how="left",
    )


def _limit_prepare_instruments(
    df: _root.pl.DataFrame,
    need_price_limits: bool,
    want: set[str],
    instruments: _root.pl.DataFrame,
) -> _root.pl.DataFrame:
    """从 instruments 取 ST 标记/流通股本/涨跌停价, JOIN 到行情帧。"""
    import app.indicators.pipeline as _root

    inst_cols = ["symbol"]
    instrument_needs: set[str] = set()
    if need_price_limits:
        instrument_needs.add("name")
        # limit_up 哨兵值 (>= 10000) 同时标记跌停侧「无涨跌停限制」
        instrument_needs.add("limit_up")
        instrument_needs.add("limit_down")
        instrument_needs.add("listing_date")
    if "turnover_rate" in want:
        instrument_needs.add("float_shares")
    for c in ["name", "float_shares", "limit_up", "limit_down", "listing_date"]:
        if c not in instrument_needs:
            continue
        if c in instruments.columns:
            inst_cols.append(c)
    if need_price_limits and "as_of" in instruments.columns:
        inst_cols.append(
            _root.pl.col("as_of").cast(_root.pl.Date, strict=False).alias("_instrument_as_of")
        )
    inst_subset = instruments.select(inst_cols).unique(subset=["symbol"])

    if need_price_limits and "name" in instruments.columns:
        st_flag = instruments.select(
            "symbol",
            _root.polars_is_risk_warning_name(_root.pl.col("name")).alias("_is_st"),
        ).unique(subset=["symbol"])
        inst_subset = inst_subset.join(st_flag, on="symbol", how="left")

    if need_price_limits:
        inst_subset = _root._attach_no_limit_window(inst_subset)

    return df.join(inst_subset, on="symbol", how="left", suffix="_inst")


def _limit_turnover(
    df: _root.pl.DataFrame, want: set[str], historical_shares: _root.pl.DataFrame | None
) -> _root.pl.DataFrame:
    """换手率: 历史流通股本补全 + 百分比换算。"""
    import app.indicators.pipeline as _root

    if "turnover_rate" in want:
        df = _root.apply_historical_float_shares(df, historical_shares, today=_root.cn_today())
    if "turnover_rate" in want and "float_shares" in df.columns and "volume" in df.columns:
        df = df.with_columns(
            _root.pl.when(_root.pl.col("float_shares") > 0)
            .then(_root.pl.col("volume") * 10000.0 / _root.pl.col("float_shares"))
            .otherwise(None)
            .alias("turnover_rate")
        )
    elif "turnover_rate" in want and "turnover_rate" not in df.columns:
        df = df.with_columns(_root.pl.lit(None).cast(_root.pl.Float64).alias("turnover_rate"))
    return df


def _limit_price_foundations(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """涨跌停判价基础: 除权参考昨收 + 涨跌幅比例 + 理论涨跌停价。"""
    import app.indicators.pipeline as _root

    _adj_today = _root.pl.col("close") / _root.pl.col("raw_close")
    _adj_yesterday = _root.pl.col("close").shift(1).over("symbol") / _root.pl.col(
        "raw_close"
    ).shift(1).over("symbol")
    # raw_close=0 等脏行会让 _adj_today 变 inf: 要求比例有限且为正, 脏行退化原始昨收。
    _adj_changed = (
        ((_adj_today - _adj_yesterday).abs() > 1e-6) & _adj_today.is_finite() & (_adj_today > 0)
    )
    df = df.with_columns(
        _root.pl.when(_adj_changed)
        .then(_root.pl.col("close").shift(1).over("symbol") / _adj_today)
        .otherwise(_root.pl.col("raw_close").shift(1).over("symbol"))
        .alias("_prev_raw_close")
    )
    is_risk_warning = _root.pl.col("_is_st") if "_is_st" in df.columns else _root.pl.lit(False)
    df = df.with_columns(
        _root.polars_price_limit_pct(
            _root.pl.col("symbol"), _root.pl.col("date"), is_risk_warning
        ).alias("_limit_pct")
    )
    df = df.with_columns(
        _root.polars_limit_price(
            _root.pl.col("_prev_raw_close"), _root.pl.col("_limit_pct"), up=True
        ).alias("_theoretical_limit_up")
    )
    df = df.with_columns(
        _root.polars_limit_price(
            _root.pl.col("_prev_raw_close"), _root.pl.col("_limit_pct"), up=False
        ).alias("_theoretical_limit_down")
    )
    return df


def _limit_price_effective(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """生效涨跌停价 + 无涨跌幅限制标记 + 判定价 (含交易所容差)。"""
    import app.indicators.pipeline as _root

    _SENTINEL = 10000.0  # noqa: N806 - N806: 大写名为领域约定常量语义
    if "_instrument_as_of" in df.columns:
        authoritative_date = _root.pl.col("_instrument_as_of") == _root.pl.col("date").cast(
            _root.pl.Date, strict=False
        )
    else:
        authoritative_date = _root.pl.col("date") == _root.pl.col("date").max()
    if "limit_up" in df.columns:
        # 维表 limit_up=0 是占位值, 不是权威价, 直接采用会全市场误判涨停。
        effective_limit_up = (
            _root.pl.when(
                authoritative_date
                & _root.pl.col("limit_up").is_not_null()
                & (_root.pl.col("limit_up") > 0)
                & (_root.pl.col("limit_up") < _SENTINEL)
            )
            .then(_root.pl.col("limit_up"))
            .otherwise(_root.pl.col("_theoretical_limit_up"))
        )
    else:
        effective_limit_up = _root.pl.col("_theoretical_limit_up")
    if "limit_down" in df.columns:
        effective_limit_down = (
            _root.pl.when(
                authoritative_date
                & _root.pl.col("limit_down").is_not_null()
                & (_root.pl.col("limit_down") > 0)
                & (_root.pl.col("limit_down") < _SENTINEL)
            )
            .then(_root.pl.col("limit_down"))
            .otherwise(_root.pl.col("_theoretical_limit_down"))
        )
    else:
        effective_limit_down = _root.pl.col("_theoretical_limit_down")
    no_price_limit = _root.pl.lit(False)
    if "limit_up" in df.columns:
        no_price_limit = (
            authoritative_date
            & _root.pl.col("limit_up").is_not_null()
            & (_root.pl.col("limit_up") >= _SENTINEL)
        )
    if "_no_limit_days" in df.columns:
        # listing_date 窗口兜底: 用 symbol 内日期序号门控, 避免老股裁剪历史被误标记。
        _day_rank = _root.pl.col("date").rank("ordinal").over("symbol")
        _first_date = _root.pl.col("date").min().over("symbol")
        window_no_limit = (
            (_root.pl.col("_no_limit_days").fill_null(0) > 0)
            & (_first_date <= _root.pl.col("_no_limit_until"))
            & (_root.pl.col("date") <= _root.pl.col("_no_limit_until"))
            & (_day_rank <= _root.pl.col("_no_limit_days"))
        )
        no_price_limit = no_price_limit | window_no_limit.fill_null(False)
    df = df.with_columns(no_price_limit.fill_null(False).alias("_no_price_limit"))
    _prev_ok = _root.pl.col("_prev_raw_close").is_not_null() & (_root.pl.col("_prev_raw_close") > 0)
    return df.with_columns(
        _root.pl.when(_root.pl.col("_no_price_limit") | ~_prev_ok)
        .then(None)
        .otherwise(effective_limit_up - 0.005)
        .alias("limit_up_price"),
        _root.pl.when(_root.pl.col("_no_price_limit") | ~_prev_ok)
        .then(None)
        .otherwise(effective_limit_down + 0.005)
        .alias("limit_down_price"),
    )


def _limit_consecutive(df: _root.pl.DataFrame, direction: str) -> _root.pl.DataFrame:
    """连板数: 同向 +1, 不同向归零 (内部用判定价直接判定, 不依赖信号列)。"""
    import app.indicators.pipeline as _root

    col = f"limit_{direction}_price"
    flag = f"_is_limit_{direction}"
    grp = f"_grp_{direction}"
    out = f"consecutive_limit_{direction}s"
    cmp = (
        _root.pl.col("raw_close") >= _root.pl.col(col)
        if direction == "up"
        else _root.pl.col("raw_close") <= _root.pl.col(col)
    )
    return (
        df.with_columns(
            _root.pl.when(_root.pl.col("_no_price_limit") | _root.pl.col(col).is_null())
            .then(None)
            .when(_root.pl.col("raw_close") > 0)
            .then(cmp)
            .otherwise(None)
            .cast(_root.pl.Boolean)
            .alias(flag)
        )
        .with_columns(
            (~_root.pl.col(flag).fill_null(False))
            .cast(_root.pl.UInt32)
            .cum_sum()
            .over("symbol")
            .alias(grp)
        )
        .with_columns(
            _root.pl.col(flag)
            .cast(_root.pl.UInt32)
            .cum_sum()
            .over("symbol", grp)
            .cast(_root.pl.UInt32)
            .alias(out)
        )
        .with_columns(
            _root.pl.when(_root.pl.col(flag).fill_null(False))
            .then(_root.pl.col(out))
            .otherwise(0)
            .cast(_root.pl.UInt32)
            .alias(out)
        )
    )


def _limit_cleanup(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """清理临时列 + JOIN 引入的 instruments 列 (不存入 enriched)。"""

    cleanup = [
        "_prev_raw_close",
        "_limit_pct",
        "_theoretical_limit_up",
        "_theoretical_limit_down",
        "_no_price_limit",
        "_grp_up",
        "_grp_down",
        "_instrument_as_of",
        "_no_limit_days",
        "_no_limit_until",
        "_is_limit_up",
        "_is_limit_down",
    ]
    if "_is_st" in df.columns:
        cleanup.append("_is_st")
    for c in df.columns:
        if c.endswith("_inst"):
            cleanup.append(c)  # noqa: PERF401 - 循环中含多行处理, 改写推导式损可读性
    for c in ["name", "float_shares", "limit_up", "limit_down", "listing_date"]:
        if c in df.columns and c != "turnover_rate":
            cleanup.append(c)  # noqa: PERF401 - 循环中含多行处理, 改写推导式损可读性
    return df.drop([c for c in cleanup if c in df.columns])


def compute_limit_signals(
    df: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame,
    needed: set[str] | None = None,
    historical_shares: _root.pl.DataFrame | None = None,
) -> _root.pl.DataFrame:
    """计算涨跌停判定价与连板状态；详细口径见价格 helper 注释。"""
    import app.indicators.pipeline as _root

    if df.is_empty():
        return df

    want = (
        set(_root.LIMIT_SIGNAL_OUTPUTS)
        if needed is None
        else set(needed) & set(_root.LIMIT_SIGNAL_OUTPUTS)
    )
    # 请求里可能含自定义信号列 (signal_limit_* 等), 其根依赖引用判定价列 →
    # 也需要计算价格。仅显式输出为空且无信号引用时短路返回。
    needed_limit_signals = needed is not None and _root._needed_references_limit_prices(set(needed))
    if not want and not needed_limit_signals:
        return df

    need_price_limits = (
        needed is None
        or bool(
            want
            & {
                "limit_up_price",
                "limit_down_price",
                "consecutive_limit_ups",
                "consecutive_limit_downs",
            }
        )
        or needed_limit_signals
    )

    # 从 instruments 取 ST 标记、流通股本(换手率用)以及最新日涨跌停价
    df = _root._limit_prepare_instruments(df, need_price_limits, want, instruments)
    # 计算换手率(%) = volume(手) * 10000 / float_shares(股)
    df = _root._limit_turnover(df, want, historical_shares)

    # 前一日参考收盘价（交易所涨跌停基准价）
    # 仅在 adj_factor 发生变化（除权除息 XD/DR）时用 前复权昨收/当日复权比例 还原到
    # 当日口径 (即交易所除权参考价 = raw_prev / ex_factor); 否则使用原始
    # raw_close.shift(1) 以避免浮点精度误差。注意不能直接用前复权昨收 —— 它还被
    # 更晚的除权事件缩放过, 会把限价压低 F(prev)/F(today) 倍 (除权日误判封板)。
    if not need_price_limits:
        cleanup = [
            c
            for c in ("name", "float_shares", "limit_up", "limit_down", "listing_date")
            if c in df.columns
        ]
        return df.drop(cleanup)

    df = _root._limit_price_foundations(df)
    df = _root._limit_price_effective(df)

    # ── 连板数 (内部用判定价直接判定涨停/跌停, 不依赖信号列) ──
    if "consecutive_limit_ups" in want:
        df = _root._limit_consecutive(df, "up")
    if "consecutive_limit_downs" in want:
        df = _root._limit_consecutive(df, "down")

    # 清理临时列 + JOIN 引入的 instruments 列 (不存入 enriched)
    df = _root._limit_cleanup(df)

    # 依赖涨跌停判定价的自定义信号 (signal_limit_up / signal_limit_down /
    # signal_limit_down_recovery / signal_broken_limit_up) 在价格列产出后注入。
    if need_price_limits:
        from app.strategy import custom_signals

        _pre, post = _root._split_custom_exprs_by_limit_deps(_root._get_custom_signal_exprs())
        if post:
            df = custom_signals.inject(df, post, needed=needed)

    return df


def compute_all(
    df: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame | None = None,
    historical_shares: _root.pl.DataFrame | None = None,
) -> _root.pl.DataFrame:
    """从 OHLCV 计算全套指标 + 信号。一站式调用。

    输入: symbol, date, open, high, low, close, volume, amount, raw_close
    """
    import app.indicators.pipeline as _root

    df = _root.compute_indicators(df)
    df = _root.compute_signals(df)
    if instruments is not None and not instruments.is_empty():
        df = _root.compute_limit_signals(df, instruments, historical_shares=historical_shares)

    # 清理 NaN / Inf
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
