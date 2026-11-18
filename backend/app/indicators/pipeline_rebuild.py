"""pipeline_rebuild: pipeline 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.indicators.pipeline as _root


def _total_ram_bytes() -> int | None:
    try:
        from app.services.memory_budget import memory_budget

        return memory_budget().total_bytes
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return None


def _adaptive_sym_batch(default_batch: int, rows_per_symbol: int) -> int:
    """小内存机器按单批目标行数收缩批次; 大内存机器保持原值 (#208)。"""
    import app.indicators.pipeline as _root

    if (_root._total_ram_bytes() or 0) >= _root._RAM_LARGE_BYTES:
        return default_batch
    return max(
        _root._BATCH_MIN_SYMBOLS,
        min(default_batch, _root._BATCH_TARGET_ROWS // max(rows_per_symbol, 1)),
    )


def _sweep_stale_staging(data_dir: _root.Path) -> None:
    """清理崩溃/取消运行残留的暂存目录 (按 mtime 判定, 不碰活跃目录)。"""
    import app.indicators.pipeline as _root

    root = data_dir / _root._STAGING_ROOT
    if not root.exists():
        return
    cutoff = _root.time.time() - _root._STALE_STAGING_MAX_AGE_S
    for run_dir in root.iterdir():
        try:
            if run_dir.is_dir() and run_dir.stat().st_mtime < cutoff:
                _root.shutil.rmtree(run_dir, ignore_errors=True)
        except OSError:
            pass


def compute_enriched_history_window(
    df_hist: _root.pl.DataFrame,
    data_dir: _root.Path,
    instruments: _root.pl.DataFrame | None = None,
    historical_shares: _root.pl.DataFrame | None = None,
    sym_batch: int | None = None,
    *,
    include_instrument_metadata: bool = False,
) -> _root.pl.DataFrame:
    """按 symbol 分批执行历史窗口计算: 指标 → 偏离列 → 信号 → 涨跌停。

    分批约束计算中间表, 最终完整历史仍常驻内存。排序和可选元数据关联
    均在单批完成, 避免合并后再复制整张宽表。
    sym_batch 显式传入时跳过自适应 (测试用)。
    """
    import app.indicators.pipeline as _root

    if df_hist.is_empty() or "symbol" not in df_hist.columns:
        return df_hist
    symbols = df_hist["symbol"].unique().sort().to_list()
    if sym_batch is None:
        rows_per_sym = max(1, df_hist.height // max(len(symbols), 1))
        sym_batch = _root._adaptive_sym_batch(2000, rows_per_sym)
    parts: list[_root.pl.DataFrame] = []
    for bs in range(0, len(symbols), sym_batch):
        batch = symbols[bs : bs + sym_batch]
        part = df_hist.filter(_root.pl.col("symbol").is_in(batch)).sort(["symbol", "date"])
        part = _root.compute_indicators(part)
        part = _root.attach_deviation_columns(part, data_dir)
        part = _root.compute_signals(part)
        if instruments is not None and not instruments.is_empty():
            inst_batch = instruments.filter(_root.pl.col("symbol").is_in(batch))
            shares_batch = (
                historical_shares.filter(_root.pl.col("symbol").is_in(batch))
                if historical_shares is not None and not historical_shares.is_empty()
                else historical_shares
            )
            part = _root.compute_limit_signals(part, inst_batch, historical_shares=shares_batch)
            if include_instrument_metadata:
                inst_cols = [
                    c
                    for c in ("name", "total_shares", "float_shares")
                    if c in inst_batch.columns and c not in part.columns
                ]
                if inst_cols:
                    part = part.join(
                        inst_batch.select("symbol", *inst_cols).unique(subset=["symbol"]),
                        on="symbol",
                        how="left",
                    )
        # 连续、互不重叠的已排序 symbol 批次, 拼接后天然有序。
        parts.append(part.sort(["symbol", "date"]))
    return (
        parts[0]
        if len(parts) == 1
        else _root.pl.concat(parts, how="diagonal_relaxed", rechunk=False)
    )


def _compute_storage_batches(
    raw: _root.pl.DataFrame,
    *,
    factors: _root.pl.DataFrame,
    instruments: _root.pl.DataFrame,
    historical_shares: _root.pl.DataFrame,
) -> _root.pl.DataFrame:
    """保留完整标的历史输入, 单批计算宽表后仅累积落盘窄表。"""
    import app.indicators.pipeline as _root
    from app.services import preferences

    if raw.is_empty():
        return _root._select_storage_cols(raw)
    symbols = raw["symbol"].unique().sort().to_list()
    rows_per_sym = max(1, -(-raw.height // len(symbols)))
    batch_size = _root._adaptive_sym_batch(preferences.get_enriched_batch_size(), rows_per_sym)
    parts = []
    for start in range(0, len(symbols), batch_size):
        batch = symbols[start : start + batch_size]
        part = _root.compute_enriched(
            raw.filter(_root.pl.col("symbol").is_in(batch)),
            factors=factors.filter(_root.pl.col("symbol").is_in(batch))
            if not factors.is_empty()
            else factors,
            instruments=(
                instruments.filter(_root.pl.col("symbol").is_in(batch))
                if not instruments.is_empty()
                else instruments
            ),
            historical_shares=(
                historical_shares.filter(_root.pl.col("symbol").is_in(batch))
                if not historical_shares.is_empty()
                else historical_shares
            ),
        )
        # 下一批开始前释放宽表; 分区发布仍在所有计算批次成功之后。
        if not part.is_empty():
            parts.append(_root._select_storage_cols(part))
        del part
    if not parts:
        return _root._select_storage_cols(raw.head(0))
    return _root.pl.concat(parts, how="diagonal_relaxed", rechunk=False)


def _validate_repair_batch(root: _root.Path, asset_type: str, files, raw, daily_dirname) -> None:
    """修补重算前校验原始历史；异常时写健康报告并抛出。"""
    from app.services import daily_health, data_integrity

    bad = data_integrity.invalid_daily_rows(raw)
    if bad.is_empty():
        return
    issues = data_integrity.inspect_invalid_history(root, daily_dirname(asset_type), files, bad)
    daily_health.record_issues(root, issues)
    if issues:
        first = issues[0]
        codes = "、".join(first.symbols[:5]) or "无法定位具体标的"
        raise ValueError(
            f"{daily_health.ASSET_LABELS[asset_type]}在 {first.day} 的原始历史日K仍异常"
            f"（{codes}：{first.reason}）, 未发布指标。"
            "已加入自检报告，请到 数据→自检/修补 处理后重试"
        )
    raise ValueError(
        "原始日K未通过校验或在检查期间发生变化，请重新自检或完成当日日K同步后重试，未发布指标"
    )


def recompute_daily_symbols(
    repo,
    asset_type: str,
    *,
    start: _root.date,
    symbols: list[str] | None,
    on_batch_done: _root.Callable[[int, int], None] | None = None,
) -> int:
    """按股票重算并只发布 start 起受影响行；口径与正式全量计算一致。"""
    import app.indicators.pipeline as _root
    from app.services import preferences
    from app.tickflow.repository import daily_dirname, enriched_dirname

    root = _root.Path(repo.store.data_dir)
    files = repo.daily_repair_files(asset_type)
    if not files:
        raise ValueError("没有可用于重算的原始日K")
    store = _root._daily_store()
    daily_table = daily_dirname(asset_type)
    selected = (
        sorted(set(symbols))
        if symbols is not None
        else store.read_daily(daily_table, columns=["symbol"])["symbol"].unique().to_list()
    )  # fmt: skip
    if not selected:
        raise ValueError("没有可用于重算的股票")
    factors = (
        _root._load_factors(store, etf=(asset_type == "etf"))
        if asset_type != "index"
        else _root.pl.DataFrame()
    )  # fmt: skip
    instruments = (
        repo.get_instruments_asset("stock") if asset_type == "stock" else _root.pl.DataFrame()
    )
    shares = _root.load_share_history(root) if asset_type == "stock" else _root.pl.DataFrame()
    batch_size = max(1, min(preferences.get_enriched_batch_size(), 200))
    publication = _root.EnrichedPublication(
        root,
        asset_type,
        recover=True,
        claim_wait_s=_root._PUBLICATION_CLAIM_WAIT_S,
        purpose="daily_repair",
    )
    written = 0
    total = (len(selected) + batch_size - 1) // batch_size
    try:
        for offset in range(0, len(selected), batch_size):
            batch = selected[offset : offset + batch_size]
            raw = store.read_daily(
                daily_table,
                symbols=batch,
                columns=_root._daily_read_columns(store, daily_table),
            ).sort(["symbol", "date"])
            _root._validate_repair_batch(root, asset_type, files, raw, daily_dirname)
            in_batch = _root.pl.col("symbol").is_in(batch)
            batch_factors = factors.filter(in_batch) if not factors.is_empty() else factors
            batch_inst = instruments.filter(in_batch) if not instruments.is_empty() else instruments
            batch_shares = shares.filter(in_batch) if not shares.is_empty() else shares
            enriched = _root.compute_enriched(
                raw,
                factors=batch_factors,
                instruments=batch_inst,
                historical_shares=batch_shares,
            )
            output = _root._select_storage_cols(enriched.filter(_root.pl.col("date") >= start))
            written += repo.replace_enriched_rows(
                asset_type, output, start=start, symbols=batch, publication=publication
            )
            publication.heartbeat()
            if on_batch_done:
                on_batch_done(offset // batch_size + 1, total)
        publication.commit()
    except BaseException:
        publication.abandon()
        raise
    enriched_dirname(asset_type)
    repo.invalidate_enriched_asset(asset_type)
    return written


def run_pipeline(
    data_dir: _root.Path | None = None,
    symbols: list[str] | None = None,
    new_dates_only: bool = False,
    on_batch_done: _root.Callable[[int, int], None] | None = None,
) -> int:
    """运行盘后管道; 异常中断时释放尚未写入的发布认领。

    真正实现见 _run_pipeline_impl。包装层单独存在, 是为了保证任何异常都
    能结束发布会话: 计算/校验阶段失败时磁盘没有新数据, abandon 直接把标记
    恢复 ready; 已写入部分分区时保持 fail-closed, 由心跳超时后的读取方接管。
    没有这层收尾时, 一次异常就会把 publishing 标记永久留在磁盘上, 之后所有
    读写持续失败, 且停止实时行情 (运行期唯一频繁的写入方) 也无法自愈。
    """
    import app.indicators.pipeline as _root

    d = _root.Path(data_dir or _root.settings.data_dir)
    publication = _root.EnrichedPublication(
        d,
        "stock",
        recover=True,
        claim_wait_s=_root._PUBLICATION_CLAIM_WAIT_S,
    )
    try:
        return _root._run_pipeline_impl(
            data_dir=data_dir,
            symbols=symbols,
            new_dates_only=new_dates_only,
            on_batch_done=on_batch_done,
            publication=publication,
        )
    except BaseException:
        publication.abandon()
        raise
