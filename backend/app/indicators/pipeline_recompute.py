"""pipeline_recompute: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root


def _rpi_load_inputs(store, d: _root.Path) -> tuple[_root.pl.DataFrame, _root.pl.DataFrame]:
    """加载 instruments 与历史股本; instruments 读取失败降级为空表。"""
    import app.indicators.pipeline as _root

    try:
        instruments = store.read_instruments("stock")
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("instruments 读取失败: %s", e)
        instruments = _root.pl.DataFrame()
    return instruments, _root.load_share_history(d)


def _rpi_incremental_new_dates(
    store,
    new_dates: list,
    factors: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
    publication: _root.EnrichedPublication,
    enriched_base: _root.Path,
    t0: float,
) -> int:
    """向后增量: 为 daily 有而 enriched 无的新日期计算并写入分区。"""
    import time as _t

    import app.indicators.pipeline as _root

    written = 0
    publication.heartbeat()  # 计算阶段不写分区, 先续心跳避免被判超时接管
    raw_new = (
        store.read_daily(
            "kline_daily",
            start=new_dates[0],
            end=new_dates[-1],
            columns=_root._daily_read_columns(store, "kline_daily"),
        )
        .filter(_root.pl.col("date").is_in(new_dates))
        .sort(["symbol", "date"])
    )
    sym_list = raw_new["symbol"].unique().to_list()
    hist_df = _root._load_recent_history(store, sym_list, days=60)
    if not hist_df.is_empty():
        hist_cols = [
            c
            for c in [
                "symbol",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "raw_close",
                "raw_high",
                "raw_low",
            ]
            if c in hist_df.columns
        ]
        raw_full = _root.pl.concat([hist_df.select(hist_cols), raw_new], how="diagonal_relaxed")
    else:
        raw_full = raw_new
    enriched_new = _root._compute_storage_batches(
        raw_full,
        factors=factors,
        instruments=instruments,
        historical_shares=historical_shares,
    )
    enriched_new = enriched_new.filter(_root.pl.col("date").is_in(new_dates))
    t_new = _t.perf_counter()
    _root.logger.info(
        "增量计算: %d 个新日期, %d 行, 耗时 %.2fs",
        len(new_dates),
        enriched_new.height,
        t_new - t0,
    )
    if not enriched_new.is_empty():
        for date_df in enriched_new.partition_by("date"):
            dt = date_df["date"][0]
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = enriched_base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            date_df = _root._select_storage_cols(date_df).sort(["symbol"])
            publication.write_parquet(date_df, out)
            written += date_df.height
        t_write_new = _t.perf_counter()
        _root.logger.info("增量写入: %.2fs, %d 行", t_write_new - t_new, written)
    del raw_new, hist_df, raw_full, enriched_new
    return written


def _rpi_incremental_symbols(
    store,
    symbols: list[str],
    factors: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
    publication: _root.EnrichedPublication,
    enriched_base: _root.Path,
) -> int:
    """向后增量: 受除权因子影响的个股重算全部已有日期。"""
    import app.indicators.pipeline as _root

    written = 0
    sym_set = set(symbols)
    raw_sym = store.read_daily(
        "kline_daily",
        symbols=list(sym_set),
        columns=_root._daily_read_columns(store, "kline_daily"),
    ).sort(["symbol", "date"])
    if not raw_sym.is_empty():
        factors_sym = (
            factors.filter(_root.pl.col("symbol").is_in(list(sym_set)))
            if not factors.is_empty()
            else factors
        )
        inst_sym = (
            instruments.filter(_root.pl.col("symbol").is_in(list(sym_set)))
            if not instruments.is_empty()
            else instruments
        )
        shares_sym = (
            historical_shares.filter(_root.pl.col("symbol").is_in(list(sym_set)))
            if not historical_shares.is_empty()
            else historical_shares
        )
        enriched_sym = _root._compute_storage_batches(
            raw_sym,
            factors=factors_sym,
            instruments=inst_sym,
            historical_shares=shares_sym,
        )
        for date_df in enriched_sym.partition_by("date"):
            dt = date_df["date"][0]
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = enriched_base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            _root._upsert_enriched_day(
                store,
                dt,
                _root._select_storage_cols(date_df),
                symbols=sorted(sym_set),
                publication=publication,
                out=out,
            )
            written += date_df.height
        _root.logger.info("除权重算: %d 只, 共写入 %d 行", len(sym_set), written)
    return written


def _rpi_incremental(
    d: _root.Path,
    store,
    symbols: list[str] | None,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
    publication: _root.EnrichedPublication,
    t0: float,
) -> int:
    """向后增量模式编排: 新日期全标的 + 受影响个股全日期重算。"""
    import time as _t

    import app.indicators.pipeline as _root

    enriched_base = d / "kline_daily_enriched"
    enriched_dates = set(store.list_daily_dates("kline_daily_enriched"))
    new_dates = [day for day in store.list_daily_dates("kline_daily") if day not in enriched_dates]
    if not new_dates and not symbols:
        _root.logger.info("增量模式: 无新日期, 无需重算")
        return 0
    factors = _root._load_factors(store)
    written = 0
    if new_dates:
        written += _root._rpi_incremental_new_dates(
            store,
            new_dates,
            factors,
            instruments,
            historical_shares,
            publication,
            enriched_base,
            t0,
        )
    if symbols:
        written += _root._rpi_incremental_symbols(
            store, symbols, factors, instruments, historical_shares, publication, enriched_base
        )
    publication.commit()
    t_done = _t.perf_counter()
    _root.logger.info("增量管道完成: %.2fs, %d 行", t_done - t0, written)
    return written


def _rpi_full_prepare(
    d: _root.Path,
    store,
    symbols: list[str] | None,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
):
    """全量/除权增量模式的读取与批次参数准备; 返回值供 _rpi_full 编排。"""
    import app.indicators.pipeline as _root
    from app.services import preferences as prefs_mod

    mode = f"incremental ({len(symbols)} symbols)" if symbols else "full"
    base = d / "kline_daily_enriched"
    factors = _root._load_factors(store)
    inst_use = instruments
    raw_all = store.read_daily(
        "kline_daily",
        symbols=list(symbols) if symbols else None,
        columns=_root._daily_read_columns(store, "kline_daily"),
    ).sort(["symbol", "date"])
    all_symbols = raw_all["symbol"].unique().sort().to_list() if not raw_all.is_empty() else []
    if not all_symbols:
        return None
    total_syms = len(all_symbols)
    if not factors.is_empty() and symbols:
        # 保留工作区既有的 sym_set 引用语义(该分支未定义, 命中即 NameError), 不改行为
        factors = factors.filter(_root.pl.col("symbol").is_in(list(sym_set)))  # type: ignore[name-defined]  # noqa: F821
    if not factors.is_empty():
        _root.logger.info("读取复权因子: %d 行", factors.height)
    if not instruments.is_empty() and symbols:
        inst_use = instruments.filter(_root.pl.col("symbol").is_in(list(sym_set)))  # type: ignore[name-defined]  # noqa: F821

    # 自适应批次: 单批体积按目标行数恒定, 与总历史长度解耦。
    total_rows = raw_all.height
    rows_per_sym = max(1, -(-int(total_rows) // total_syms))
    sym_batch: int = _root._adaptive_sym_batch(
        int(prefs_mod.get_enriched_batch_size() or 1000), rows_per_sym
    )
    total_batches = (total_syms + sym_batch - 1) // sym_batch
    _root.logger.info(
        "全量计算: %d 只标的 (%d 行, ~%d 行/只), symbol 分批 %d 只/批, %d 批 [%s]",
        total_syms,
        total_rows,
        rows_per_sym,
        sym_batch,
        total_batches,
        mode,
    )
    staging_dir: _root.Path | None = None
    staging_files: list[str] = []
    if not symbols:
        _root._sweep_stale_staging(d)
        staging_dir = d / _root._STAGING_ROOT / _root.uuid.uuid4().hex
        staging_dir.mkdir(parents=True, exist_ok=True)
    return (
        mode,
        base,
        factors,
        inst_use,
        raw_all,
        all_symbols,
        sym_batch,
        total_batches,
        staging_dir,
        staging_files,
    )


def _rpi_full_batch_write(
    enriched: _root.pl.DataFrame,
    base: _root.Path,
    store,
    symbols: list[str] | None,
    batch_syms: list[str],
    publication: _root.EnrichedPublication,
    staging_dir: _root.Path | None,
    staging_files: list[str],
    sym_batch: int,
    batch_start: int,
) -> int:
    """写入一个 symbol 批次: 局部模式按日合并, 全量模式写暂存文件。"""
    import app.indicators.pipeline as _root

    written = 0
    if enriched.is_empty():
        return written
    if symbols:
        for date_df in _root._select_storage_cols(enriched).partition_by("date"):
            dt = date_df["date"][0]
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            _root._upsert_enriched_day(
                store,
                dt,
                _root._select_storage_cols(date_df),
                symbols=batch_syms,
                publication=publication,
                out=out,
            )
            written += date_df.height
    else:
        assert staging_dir is not None  # 全量模式在上方已创建暂存目录
        out = staging_dir / f"batch-{batch_start // sym_batch:04d}.parquet"
        _root._select_storage_cols(enriched).sort(["date", "symbol"]).write_parquet(out)
        staging_files.append(str(out))
        written += enriched.height
    return written


def _rpi_full_merge(
    store,
    base: _root.Path,
    staging_files: list[str],
    publication: _root.EnrichedPublication,
) -> None:
    """全量暂存合并: 日期覆盖校验 → 按日期分块流式合并 → 逐分区替换。"""
    import gc

    import app.indicators.pipeline as _root

    daily_dates = {day.isoformat() for day in store.list_daily_dates("kline_daily")}
    existing_dates = {day.isoformat() for day in store.list_daily_dates("kline_daily_enriched")}
    unique_dates = sorted(
        _root.guarded_collect(
            _root.scan_enriched_parquet(staging_files).select("date").unique(),
            priority="background",
        )["date"].to_list()
    )
    rebuilt_dates = {ds.isoformat() if hasattr(ds, "isoformat") else str(ds) for ds in unique_dates}
    # 孤儿分区 (enriched 有而日K已无) 保留不动, 覆盖校验只针对日K仍存在的日期。
    orphan_dates = existing_dates - daily_dates
    if orphan_dates:
        sample = ", ".join(sorted(orphan_dates)[:5])
        _root.logger.warning(
            "enriched 存在 %d 个日K已不存在的孤儿分区(保留不动): %s",
            len(orphan_dates),
            sample,
        )
    missing_dates = (existing_dates & daily_dates) - rebuilt_dates
    if missing_dates:
        sample = ", ".join(sorted(missing_dates)[:5])
        raise RuntimeError(f"全量重建结果缺少已有日期分区,拒绝覆盖: {sample}")
    base.mkdir(parents=True, exist_ok=True)
    chunk = max(1, -(-len(unique_dates) // _root._MERGE_DATE_CHUNKS))
    for ci in range(0, len(unique_dates), chunk):
        lo = unique_dates[ci]
        hi = unique_dates[min(ci + chunk, len(unique_dates)) - 1]
        block = (
            _root.scan_enriched_parquet(staging_files)
            .filter((_root.pl.col("date") >= lo) & (_root.pl.col("date") <= hi))
            .sort(["date", "symbol"])
            .collect(streaming=True)
        )
        for date_df in block.partition_by("date"):
            ds = date_df["date"][0]
            ds_str = ds.isoformat() if hasattr(ds, "isoformat") else str(ds)
            out = base / f"date={ds_str}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            publication.write_parquet(date_df.sort(["symbol"]), out)
    gc.collect()
    _root.logger.info("全量暂存合并完成: %d 个日期分区", len(unique_dates))


def _rpi_full_batch(
    batch_start: int,
    batch_syms: list[str],
    raw_all: _root.pl.DataFrame,
    factors: _root.pl.DataFrame,
    inst_use: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
    base: _root.Path,
    store,
    symbols: list[str] | None,
    publication: _root.EnrichedPublication,
    staging_dir: _root.Path | None,
    staging_files: list[str],
    sym_batch: int,
    total_batches: int,
    on_batch_done: _root.Callable[[int, int], None] | None,
) -> int:
    """全量模式单个 symbol 批次: 切片 → 计算 → 写入 → 进度/心跳。"""
    import gc

    import app.indicators.pipeline as _root

    raw = raw_all.filter(_root.pl.col("symbol").is_in(batch_syms)).sort(["symbol", "date"])
    if raw.is_empty():
        return 0
    batch_factors = (
        factors.filter(_root.pl.col("symbol").is_in(batch_syms))
        if not factors.is_empty()
        else factors
    )
    batch_inst = (
        inst_use.filter(_root.pl.col("symbol").is_in(batch_syms))
        if not inst_use.is_empty()
        else inst_use
    )
    batch_shares = (
        historical_shares.filter(_root.pl.col("symbol").is_in(batch_syms))
        if not historical_shares.is_empty()
        else historical_shares
    )
    enriched = _root.compute_enriched(
        raw, factors=batch_factors, instruments=batch_inst, historical_shares=batch_shares
    )
    written = _root._rpi_full_batch_write(
        enriched,
        base,
        store,
        symbols,
        batch_syms,
        publication,
        staging_dir,
        staging_files,
        sym_batch,
        batch_start,
    )
    del raw, enriched, batch_factors, batch_inst, batch_shares
    gc.collect()
    _root.logger.info(
        "symbol 批次 %d/%d (%s ~ %s), 已处理 %d 行",
        batch_start // sym_batch + 1,
        total_batches,
        batch_syms[0],
        batch_syms[-1],
        written,
    )
    if on_batch_done:
        on_batch_done(batch_start // sym_batch + 1, total_batches)
    publication.heartbeat()
    return written


def _rpi_full(
    d: _root.Path,
    store,
    symbols: list[str] | None,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
    publication: _root.EnrichedPublication,
    on_batch_done: _root.Callable[[int, int], None] | None,
    t0: float,
) -> int:
    """全量/除权增量模式编排: 分批计算 → 暂存合并 → 提交发布。"""
    import time as _t

    import app.indicators.pipeline as _root

    prepared = _root._rpi_full_prepare(d, store, symbols, instruments, historical_shares)
    if prepared is None:
        _root.logger.info("无日K数据, 跳过管道")
        return 0
    (
        mode,
        base,
        factors,
        inst_use,
        raw_all,
        all_symbols,
        sym_batch,
        total_batches,
        staging_dir,
        staging_files,
    ) = prepared
    written = 0
    try:
        for batch_start in range(0, len(all_symbols), sym_batch):
            batch_syms = all_symbols[batch_start : batch_start + sym_batch]
            written += _root._rpi_full_batch(
                batch_start,
                batch_syms,
                raw_all,
                factors,
                inst_use,
                historical_shares,
                base,
                store,
                symbols,
                publication,
                staging_dir,
                staging_files,
                sym_batch,
                total_batches,
                on_batch_done,
            )
        if not symbols and staging_files:
            _root._rpi_full_merge(store, base, staging_files, publication)
    finally:
        if staging_dir is not None:
            _root.shutil.rmtree(staging_dir, ignore_errors=True)
    publication.commit()
    t_done = _t.perf_counter()
    adj_label = "含复权" if not factors.is_empty() else "无复权"
    _root.logger.info(
        "enriched 完成 [%s]: %.2fs, 共 %d 行, %s", mode, t_done - t0, written, adj_label
    )
    return written


def _run_pipeline_impl(
    data_dir: _root.Path | None = None,
    symbols: list[str] | None = None,
    new_dates_only: bool = False,
    on_batch_done: _root.Callable[[int, int], None] | None = None,
    *,
    publication: _root.EnrichedPublication,
) -> int:
    """运行盘后管道:读 kline_daily + adj_factor → 前复权 + 计算存储列 → 写 enriched。

    enriched 表仅存储 14 列基础行情窄表 (OHLCV + raw_close/high/low + turnover_rate + 连板数)。

    模式:
      - 全量 (symbols=None, new_dates_only=False):
          读全部 kline_daily, 全部重写 enriched 分区。
          用于首次同步、往前扩展历史。
      - 向后增量 (new_dates_only=True):
          只读 enriched 中尚不存在的日期分区对应的 daily 数据,
          为所有标的生成新的 enriched 分区;
          若同时传 symbols, 还会对这些个股的全部已有日期做重算
          (因为除权因子链变了,历史数据的复权比例也要更新)。
      - 除权因子增量 (symbols 指定, new_dates_only=False):
          只对指定 symbol 做局部重算并合并回已有 enriched。
          用于无新日K数据、仅除权因子变更的场景。
    返回写入的行数。
    """
    import time as _t

    import app.indicators.pipeline as _root

    t0 = _t.perf_counter()
    d = _root.Path(data_dir or _root.settings.data_dir)
    store = _root._daily_store()
    if _root.enriched_publication_incomplete(d, "stock"):
        _root.logger.warning("检测到未完成的 enriched 发布,改为全量重建")
        symbols = None
        new_dates_only = False
    if store.latest_daily_date("kline_daily") is None:
        _root.logger.info("无日K数据, 跳过管道")
        return 0
    instruments, historical_shares = _root._rpi_load_inputs(store, d)

    if new_dates_only:
        return _root._rpi_incremental(
            d, store, symbols, instruments, historical_shares, publication, t0
        )

    return _root._rpi_full(
        d, store, symbols, instruments, historical_shares, publication, on_batch_done, t0
    )


def _daily_read_columns(store, table: str) -> list[str]:
    """日K读取列: 含 quote_ts（enriched 存储列之一, 盘后校验/量比折算/跨天完整性）。

    CK 的 ``TABLE_READ_COLUMNS`` 是对外固定列集（不含 quote_ts, 避免多余列
    流进 API 响应）；计算 enriched 的路径必须显式请求, 否则写出的分区
    quote_ts 恒为 NULL, 与迁移前的本地 parquet 口径不一致。
    """
    cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    try:
        if "quote_ts" in store.table_columns(table):
            cols.append("quote_ts")
    except Exception:  # noqa: BLE001 - 列信息不可用时退回默认列集
        pass
    return cols


def _load_factors(store, *, etf: bool = False) -> _root.pl.DataFrame:
    """加载除权事件（CK）。"""
    import app.indicators.pipeline as _root

    try:
        return store.read_adj_factor(etf=etf)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("复权因子读取失败: %s", e)
        return _root.pl.DataFrame()


def _load_recent_history(store, symbols: list[str], days: int) -> _root.pl.DataFrame:
    """从 CK 的 enriched 表加载最近 N 天历史（增量模式的指标计算窗口前缀）。

    只读基础行情列，作为指标计算的历史前缀。
    """
    import app.indicators.pipeline as _root

    cutoff = _root.date.today() - _root._timedelta(days=days + 30)  # 多读 30 天余量

    hist_cols = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "raw_close",
        "raw_high",
        "raw_low",
    ]
    try:
        return store.read_daily(
            "kline_daily_enriched",
            symbols=symbols,
            start=cutoff,
            columns=hist_cols,
        ).sort(["symbol", "date"])
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("历史数据加载失败: %s", e)
        return _root.pl.DataFrame()


def compute_enriched_single(daily_for_symbol: _root.pl.DataFrame) -> _root.pl.DataFrame:
    """单股版本 — Free 用户用,拉下来单股 K 后即时计算全部指标+信号返回给前端。"""
    import app.indicators.pipeline as _root

    if daily_for_symbol.is_empty():
        return daily_for_symbol

    # 过滤停牌
    daily_for_symbol = _root.filter_halt_days(daily_for_symbol)
    if daily_for_symbol.is_empty():
        return daily_for_symbol

    # 保留 raw_close 用于涨停判断
    daily_for_symbol = daily_for_symbol.with_columns(_root.pl.col("close").alias("raw_close"))

    # 即时计算全套指标 + 信号 (无复权因子, 无 instruments)
    return _root.compute_all(daily_for_symbol)
