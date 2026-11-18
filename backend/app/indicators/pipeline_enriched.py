"""pipeline_enriched: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root


def filter_halt_days(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """过滤停牌日。

    停牌日的 open/high 必然为 0 (无集合竞价)。注意 close 可能被数据源
    填充为前收盘价而非 0, 因此不能用 "OHLC 全零" 判断, 否则会漏过这类
    停牌记录 (如 *ST 撤销风险警示的停牌日), 污染 MA/ATR 等指标。旧版实时
    落盘还会先把 open/high=0 填成 close, 对这类历史数据用零成交量和零成交额
    作为兼容判据。
    """
    import app.indicators.pipeline as _root

    if df.is_empty() or "open" not in df.columns or "high" not in df.columns:
        return df
    halted = (_root.pl.col("open") == 0) & (_root.pl.col("high") == 0)
    if "volume" in df.columns and "amount" in df.columns:
        halted = halted | ((_root.pl.col("volume") == 0) & (_root.pl.col("amount") == 0))
    return df.filter(~halted)


# ================================================================
# Pipeline: 盘后全量计算 + 写入
# ================================================================


def compute_enriched(
    raw: _root.pl.DataFrame,
    factors: _root.pl.DataFrame | None = None,
    instruments: _root.pl.DataFrame | None = None,
    historical_shares: _root.pl.DataFrame | None = None,
) -> _root.pl.DataFrame:
    """对原始日 K 应用前复权 + 全量计算指标 + 信号, 产出完整 enriched (含全部指标列)。

    输入应包含至少: symbol, date, open, high, low, close, volume (可选 amount)。
    如果提供了 factors, 先应用前复权再算指标。
    如果提供了 instruments, 计算涨跌停信号和换手率。
    """
    import app.indicators.pipeline as _root

    if raw.is_empty():
        return raw

    # 过滤停牌日 (会污染指标计算)
    raw = _root.filter_halt_days(raw)

    if raw.is_empty():
        return raw

    # 保留不复权原始价格（涨停/炸板/跌停判断需用不复权价格）
    raw = raw.with_columns(
        _root.pl.col("close").alias("raw_close"),
        _root.pl.col("high").alias("raw_high"),
        _root.pl.col("low").alias("raw_low"),
    )

    # 应用前复权（只改 open/high/low/close，raw_close 不受影响）
    if factors is not None and not factors.is_empty():
        raw = _root._apply_adj_factor(raw, factors)

    # 排序
    df = raw.sort(["symbol", "date"])

    # 全量计算指标 + 信号
    df = _root.compute_all(
        df,
        instruments=instruments,
        historical_shares=historical_shares,
    )

    return df


def _select_storage_cols(df: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """写入 parquet 前裁剪到存储列 (14 列)。"""
    import app.indicators.pipeline as _root

    cols = [c for c in _root.ENRICHED_STORAGE_COLS if c in df.columns]
    return df.select(cols)


def _as_date(value):
    import app.indicators.pipeline as _root

    return value.date() if hasattr(value, "date") and not isinstance(value, _root.date) else value


def _upsert_enriched_day(
    store,
    day,
    frame: _root.pl.DataFrame,
    *,
    symbols: _root.Sequence[str] | None = None,
    publication=None,
    out: _root.Path | None = None,
) -> None:
    """单日 enriched 写入：CK 为读源（按 symbol 覆写或整日替换），Parquet 仅镜像。

    ``symbols=None`` 表示整日重建；否则只替换这些标的的行（增量/除权重算）。
    Parquet 镜像从 CK 读回整日内容，避免"读旧分区再合并"的 read-modify-write。
    """
    import app.indicators.pipeline as _root

    day = _root._as_date(day)
    if symbols:
        store.delete_daily(_root._ENRICHED_TABLE, day=day, symbols=list(symbols))
    else:
        store.delete_daily_date(_root._ENRICHED_TABLE, day=day)
    store.write_daily(_root._ENRICHED_TABLE, frame)
    if publication is None or out is None:
        return
    day_full = store.read_daily(_root._ENRICHED_TABLE, start=day, end=day)
    if day_full.is_empty():
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    publication.write_parquet(day_full.sort(["symbol"]), out)


def _pick_benchmark_pairs(df_idx: _root.pl.DataFrame, wanted: list[str]) -> list[tuple[str, str]]:
    """每个板块选最高优先级可用基准；全缺时回退任一可用基准。"""
    import app.indicators.pipeline as _root

    available = set(df_idx["symbol"].to_list())
    picked = [s for s in wanted if s in available]
    pairs: list[tuple[str, str]] = []
    for bench_key, candidates in _root._BENCHMARK_PREFERENCE.items():
        hit = next((s for s in candidates if s in available), None)
        if hit is None and picked:
            hit = picked[0]
        if hit is not None:
            pairs.append((hit, bench_key))
    return pairs


def load_benchmark_momentum(data_dir: _root.Path) -> _root.pl.DataFrame | None:
    """读取指数日K并计算基准动量；无数据时返回 None，按 data_dir 缓存。"""
    import time as _time

    import app.indicators.pipeline as _root

    now = _time.monotonic()
    key = str(_root.Path(data_dir).resolve())
    cached = _root._benchmark_cache.get(key)
    if cached is not None and now - cached[0] < _root._BENCHMARK_CACHE_TTL:
        return cached[1]

    frame: _root.pl.DataFrame | None = None
    try:
        wanted: list[str] = []
        bench_of: dict[str, str] = {}
        for bench_key, candidates in _root._BENCHMARK_PREFERENCE.items():
            for sym in candidates:
                if sym not in bench_of:
                    wanted.append(sym)
                    bench_of[sym] = bench_key
        df_idx = (
            _root._daily_store()
            .read_daily(
                "kline_index_daily",
                symbols=wanted,
                columns=["symbol", "date", "close"],
            )
            .sort(["symbol", "date"])
        )
        if not df_idx.is_empty():
            pairs = _root._pick_benchmark_pairs(df_idx, wanted)
            df_bench = df_idx.filter(_root.pl.col("symbol").is_in([p[0] for p in pairs]))
            if not df_bench.is_empty():
                df_bench = (
                    df_bench.with_columns(
                        _root.pl.col("close").cast(_root.pl.Float64, strict=False)
                    )
                    .with_columns(
                        [
                            (
                                _root.pl.col("close")
                                / _root.pl.col("close").shift(n).over("symbol")
                                - 1
                            ).alias(f"_bm{n}")
                            for n in _root.DEVIATION_WINDOWS
                        ]
                    )
                    .rename({f"_bm{n}": f"bench_mom{n}d" for n in _root.DEVIATION_WINDOWS})
                )
                key_map = _root.pl.DataFrame(
                    {
                        "symbol": [p[0] for p in pairs],
                        "bench_key": [p[1] for p in pairs],
                    }
                )
                frame = (
                    df_bench.join(key_map, on="symbol", how="inner")
                    .select(
                        [
                            "date",
                            "bench_key",
                            "close",
                            *[f"bench_mom{n}d" for n in _root.DEVIATION_WINDOWS],
                        ]
                    )
                    .rename({"close": "bench_close"})
                    .unique(subset=["date", "bench_key"])
                )
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("基准指数偏离数据加载失败: %s", exc)
        frame = None

    _root._benchmark_cache[key] = (now, frame)
    return frame


def _bench_key_expr() -> _root.pl.Expr:
    """symbol → 板块基准键 (SH/STAR/SZ/GEM/BJ), 无法识别时 null。

    北交所按后缀; 沪市按 68 前缀区分科创板; 深市按 30 前缀区分创业板。
    与 abnormal_moves.board_of 的板块判定同口径。
    """
    import app.indicators.pipeline as _root

    code = _root.pl.col("symbol").str.slice(0, 6)
    suffix = _root.pl.col("symbol").str.slice(-2).str.to_uppercase()
    return (
        _root.pl.when(suffix == "BJ")
        .then(_root.pl.lit("BJ"))
        .when((suffix == "SH") & code.str.starts_with("68"))
        .then(_root.pl.lit("STAR"))
        .when(suffix == "SH")
        .then(_root.pl.lit("SH"))
        .when((suffix == "SZ") & code.str.starts_with("30"))
        .then(_root.pl.lit("GEM"))
        .when(suffix == "SZ")
        .then(_root.pl.lit("SZ"))
        .otherwise(_root.pl.lit(None, dtype=_root.pl.Utf8))
    )


def attach_deviation_columns(df: _root.pl.DataFrame, data_dir: _root.Path) -> _root.pl.DataFrame:
    """为已含 momentum_Nd 的 enriched 帧附着 deviate_Nd 偏离列 (全量/冷路径)。

    缺失的动量列 (如 momentum_3d 不在指标全集里) 就地按 close 补算,
    与 compute_indicators 在同一帧上的 shift 语义一致。
    基准按 symbol 后缀分交易所匹配, join 不上的行 (新上市/基准缺失) 置 null。
    """
    import app.indicators.pipeline as _root

    if df.is_empty():
        return df
    bench = _root.load_benchmark_momentum(data_dir)
    dev_cols = [f"deviate_{n}d" for n in _root.DEVIATION_WINDOWS]
    if bench is None or bench.is_empty():
        return df.with_columns(
            [_root.pl.lit(None, dtype=_root.pl.Float64).alias(c) for c in dev_cols]
        )
    if "close" not in df.columns:
        _root.logger.warning("偏离列附着跳过: 缺少 close 列")
        return df.with_columns(
            [_root.pl.lit(None, dtype=_root.pl.Float64).alias(c) for c in dev_cols]
        )
    missing = [n for n in _root.DEVIATION_WINDOWS if f"momentum_{n}d" not in df.columns]
    if missing:
        df = df.sort(["symbol", "date"]).with_columns(
            [
                (_root.pl.col("close") / _root.pl.col("close").shift(n).over("symbol") - 1).alias(
                    f"momentum_{n}d"
                )
                for n in missing
            ]
        )
    out = (
        df.with_columns(_root._bench_key_expr().alias("_bench_ex"))
        .join(bench, left_on=["_bench_ex", "date"], right_on=["bench_key", "date"], how="left")
        .with_columns(
            [
                (_root.pl.col(f"momentum_{n}d") - _root.pl.col(f"bench_mom{n}d")).alias(
                    f"deviate_{n}d"
                )
                for n in _root.DEVIATION_WINDOWS
            ]
        )
        .drop(["_bench_ex", "bench_close", *[f"bench_mom{n}d" for n in _root.DEVIATION_WINDOWS]])
    )
    return out


def _bench_rt_pct_of(index_quotes: _root.pl.DataFrame | None, candidates: list[str]) -> float:
    """从实时指数行情取某交易所首选基准的今日涨跌 (小数制), 缺数据时 0。

    入参 index_quotes 来自 quote_service 的指数展示缓存, 其 change_pct/pct/pct_change
    列为百分数口径 (CONTRIBUTING §3.1), 消费前必须显式 /100 (#232);
    close/prev_close 兜底路径本身就是小数, 不转换。
    """
    import app.indicators.pipeline as _root

    if index_quotes is None or index_quotes.is_empty():
        return 0.0
    df = index_quotes.filter(_root.pl.col("symbol").is_in(candidates))
    if df.is_empty():
        return 0.0
    # 候选按优先级排序, 取第一个有有效涨跌的
    by_sym = {r["symbol"]: r for r in df.iter_rows(named=True)}
    for sym in candidates:
        row = by_sym.get(sym)
        if row is None:
            continue
        for col in ("change_pct", "pct", "pct_change"):
            v = row.get(col)
            if v is not None:
                return float(v) / 100.0
        if row.get("close") is not None and row.get("prev_close") is not None and row["prev_close"]:
            return float(row["close"] / row["prev_close"] - 1)
    return 0.0


def bench_rt_pct_for(index_quotes: _root.pl.DataFrame | None, bench_key: str) -> float:
    """板块基准键的指数今日实时涨跌 (小数制), 供异动总览实时叠加等外部消费。"""
    import app.indicators.pipeline as _root

    return _root._bench_rt_pct_of(index_quotes, _root._BENCHMARK_PREFERENCE.get(bench_key, []))


def benchmark_momentum_today(
    data_dir: _root.Path,
    index_quotes: _root.pl.DataFrame | None = None,
) -> _root.pl.DataFrame | None:
    """各板块基准指数的「今日」N 日动量 (盘中实时外推)。

    基准日K parquet 盘中不含今日, 今日基准收盘 = 昨收 × (1 + 实时涨跌)。
    N 日动量 = 今日基准收盘 / N 个交易日前的收盘 - 1; 板块与
    load_benchmark_momentum 的选基逻辑一致 (同一 TTL 缓存帧)。
    返回小表: bench_key, bench_mom3d, bench_mom10d, bench_mom30d。
    无基准数据时 None。
    """
    import app.indicators.pipeline as _root

    bench = _root.load_benchmark_momentum(data_dir)
    if bench is None or bench.is_empty():
        return None
    # 指数监控 (mode=all) 盘中会向 kline_index_daily 写入今日行;
    # 「昨收」必须排除今日, 否则实时涨跌被重复叠加
    today = _root.cn_today()
    bench = bench.filter(_root.pl.col("date") < today)
    if bench.is_empty():
        return None
    rows: list[dict[str, float | str | None]] = []
    for k in sorted(bench["bench_key"].unique().to_list()):
        sub = bench.filter(_root.pl.col("bench_key") == k).sort("date")
        closes = sub["bench_close"]
        if closes.len() == 0:
            continue
        yesterday_close = closes[-1]
        rt = _root._bench_rt_pct_of(index_quotes, _root._BENCHMARK_PREFERENCE.get(k, []))
        row: dict[str, float | str | None] = {
            "bench_key": k,
        }
        for n in _root.DEVIATION_WINDOWS:
            base = closes[-n] if closes.len() >= n else None  # N 个交易日前 (不含今日)
            row[f"bench_mom{n}d"] = (
                (yesterday_close * (1.0 + rt)) / base - 1.0
                if base is not None and yesterday_close is not None and base > 0
                else None
            )
        rows.append(row)
    if not rows:
        return None
    schema = {
        "bench_key": _root.pl.Utf8,
        **{f"bench_mom{n}d": _root.pl.Float64 for n in _root.DEVIATION_WINDOWS},
    }
    return _root.pl.DataFrame(rows, schema=schema)


def attach_deviation_columns_today(
    df: _root.pl.DataFrame,
    data_dir: _root.Path,
    index_quotes: _root.pl.DataFrame | None = None,
) -> _root.pl.DataFrame:
    """为盘中单日 enriched 帧附着 deviate_Nd 偏离列 (增量热路径)。

    与 attach_deviation_columns 的区别: 入参是「仅今日」的单日帧, 无法用
    shift 补算动量, 直接使用帧上已有的 momentum_Nd (compute_enriched_today
    产出); 基准动量用 benchmark_momentum_today 的实时外推值。
    缺失动量的窗口 (如全量回退路径无 momentum_3d) 置 null, 不阻塞主流程。
    """
    import app.indicators.pipeline as _root

    dev_cols = [f"deviate_{n}d" for n in _root.DEVIATION_WINDOWS]
    if df.is_empty():
        return df
    bench = _root.benchmark_momentum_today(data_dir, index_quotes)
    if bench is None or bench.is_empty():
        return df.with_columns(
            [
                _root.pl.lit(None, dtype=_root.pl.Float64).alias(c)
                for c in dev_cols
                if c not in df.columns
            ]
        )
    exprs = [
        (_root.pl.col(f"momentum_{n}d") - _root.pl.col(f"bench_mom{n}d")).alias(f"deviate_{n}d")
        if f"momentum_{n}d" in df.columns
        else _root.pl.lit(None, dtype=_root.pl.Float64).alias(f"deviate_{n}d")
        for n in _root.DEVIATION_WINDOWS
    ]
    return (
        df.with_columns(_root._bench_key_expr().alias("_bench_ex"))
        .join(bench, left_on="_bench_ex", right_on="bench_key", how="left")
        .with_columns(exprs)
        .drop(["_bench_ex", *[f"bench_mom{n}d" for n in _root.DEVIATION_WINDOWS]])
    )
