"""daily_pipeline_sync: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


def _run_daily_live(s: _root._RunState) -> None:
    """付费档: 今天已有数据 → 实时行情覆写, 确保最新。"""
    import app.jobs.daily_pipeline as _root

    s.emit("sync_daily", 12, f"获取日K [{s.today} ~ {s.today}] 实时行情…")
    written_daily = _root.kline_sync.sync_daily_by_quotes(s.repo)
    s.new_daily_days = 1
    s.emit("sync_daily", 45, f"日K 完成,{written_daily} 只标的")
    _root.logger.info(
        "sync_daily: [%s ~ %s] live quotes, %d symbols", s.today, s.today, written_daily
    )


def _run_daily_gapfill(s: _root._RunState) -> None:
    """有历史 → batch 补齐缺口 (也覆盖无实时权限的当日刷新)。"""
    from datetime import datetime as _dt

    import app.jobs.daily_pipeline as _root

    start_date = min(d for d in (s.latest_daily, s.stale_day) if d is not None)
    s.daily_range_start = start_date
    s.emit("sync_daily", 12, f"获取日K [{start_date} ~ {s.today}]…")
    _root.logger.info(
        "sync_daily: [%s ~ %s] %s",
        start_date,
        s.today,
        "refresh today" if s.today_exists else "gap fill",
    )
    _root.kline_sync.sync_and_persist_daily_batch(
        s.universe,
        s.repo,
        s.capset,
        start_date=_dt.combine(start_date, _dt.min.time()),
        end_date=_dt.combine(s.today, _dt.min.time()),
        on_chunk_done=_root._daily_chunk_progress(s.emit),
    )
    gap_days = (s.today - start_date).days
    s.new_daily_days = gap_days
    s.emit("sync_daily", 45, f"日K 完成,覆盖 {gap_days} 天")
    _root.logger.info("sync_daily: [%s ~ %s] done, %d days", start_date, s.today, gap_days)


def _run_daily_initial(s: _root._RunState) -> None:
    """首次: 无任何数据 → batch 拉 1 年。"""
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    import app.jobs.daily_pipeline as _root

    start_date = s.today - _td(days=365)
    s.daily_range_start = start_date
    s.emit("sync_daily", 12, f"获取日K [{start_date} ~ {s.today}]…")
    _root.logger.info("sync_daily: [%s ~ %s] initial fetch", start_date, s.today)
    _root.kline_sync.sync_and_persist_daily_batch(
        s.universe,
        s.repo,
        s.capset,
        start_date=_dt.combine(start_date, _dt.min.time()),
        end_date=_dt.combine(s.today, _dt.min.time()),
        on_chunk_done=_root._daily_chunk_progress(s.emit),
    )
    s.new_daily_days = 365
    s.emit("sync_daily", 45, "日K 完成")
    _root.logger.info("sync_daily: [%s ~ %s] done", start_date, s.today)


def _run_sync_daily(s: _root._RunState) -> None:
    """Step 1: 日K同步编排 (修正/覆写/补缺口/首次四分支)。"""
    import app.jobs.daily_pipeline as _root

    s.latest_daily = s.repo.latest_daily_date()
    # 管道「今天」必须是北京日期, 否则会把昨日日K当成已齐, 官方收盘价永远拉不进来。
    s.today = _root.cn_today()
    s.today_exists = bool(s.latest_daily and s.latest_daily >= s.today)
    s.daily_range_start = None
    s.pull_a_share = _root._prefs.get_pipeline_pull_a_share()
    if not s.pull_a_share and not s.override_start_date:
        s.emit("sync_daily", 45, "已跳过 A 股日K同步(拉取内容未勾选)")
        _root.logger.info("sync_daily: skipped (pipeline_pull_a_share=False)")
    elif s.override_start_date:
        _root._run_daily_override(s)
    elif (
        s.today_exists
        and s.stale_day is None
        and s.capset.has(_root.Cap.QUOTE_POOL)
        and _root._prefs.get_daily_data_provider() == "tickflow"
    ):
        _root._run_daily_live(s)
    elif s.latest_daily or s.stale_day:
        _root._run_daily_gapfill(s)
    else:
        _root._run_daily_initial(s)
    _root._invalidate("daily")


def _run_prune_enriched(s: _root._RunState) -> None:
    """完整性修复时删除股票 enriched 的坏分区, 让 Step 2 当新日期重算。"""
    import app.jobs.daily_pipeline as _root

    if s.repair_start is None:
        return
    try:
        from app.services.data_integrity import prune_enriched_partitions

        pruned = prune_enriched_partitions(
            s.repo.store.data_dir, s.repair_start, "kline_daily_enriched"
        )
        if pruned:
            _root.logger.info(
                "integrity: 已删除 %d 个待重算的 enriched 分区 (≥ %s)", pruned, s.repair_start
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("enriched prune failed (soft): %s", e)


def _run_detect_lagging(s: _root._RunState) -> None:
    """单标的新鲜度可见化 (WARNING + 计入结果), 让掉队标的不再隐形。"""
    import app.jobs.daily_pipeline as _root

    if not s.pull_a_share or not s.latest_daily:
        return
    try:
        s.lagging_symbols = s.repo.symbols_lagging(s.today, min_gap_days=3)
        if s.lagging_symbols:
            _root.logger.warning(
                "日K新鲜度: %d 只标的落后 >3 日 (停牌/退市/拉取失败; 样例: %s)",
                len(s.lagging_symbols),
                s.lagging_symbols[:10],
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("laggard detection failed: %s", e)
        s.stage_errors.append(f"laggard detection: {e}")


def _run_sync_adj(s: _root._RunState) -> None:
    """Step 1.5: 同步除权因子, 范围与日K拉取方式对齐。"""
    import app.jobs.daily_pipeline as _root

    adj_provider = _root._prefs.get_adj_factor_provider()
    can_sync_adj = s.capset.has(_root.Cap.ADJ_FACTOR) or adj_provider != "tickflow"
    if not can_sync_adj:
        s.skipped.append("sync_adj")
        _root.logger.info("sync_adj skipped: no ADJ_FACTOR capability")
        return
    from datetime import datetime, timedelta

    adj_end = datetime.now()
    if s.daily_range_start is not None:
        adj_start = datetime.combine(s.daily_range_start, datetime.min.time())
    else:
        adj_start = adj_end - timedelta(days=15)
    adj_start_str = adj_start.strftime("%Y-%m-%d")
    adj_end_str = adj_end.strftime("%Y-%m-%d")
    s.emit("sync_adj", 50, f"获取除权因子 [{adj_start_str} ~ {adj_end_str}]…")
    _root.logger.info("sync_adj: [%s ~ %s] start", adj_start_str, adj_end_str)

    def _adj_chunk_progress(cur: int, tot: int) -> None:
        s.emit(
            "sync_adj",
            50 + int(10 * cur / tot),
            f"除权因子批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot),
            skip_log=True,
        )

    s.written_adj, s.affected_symbols = _root.kline_sync.sync_adj_factor(
        s.universe,
        s.repo,
        s.capset,
        start_time=adj_start,
        end_time=adj_end,
        on_chunk_done=_adj_chunk_progress,
    )
    if s.affected_symbols:
        s.emit("sync_adj", 60, f"除权因子完成,新增 {len(s.affected_symbols)} 只个股")
        _root.logger.info(
            "sync_adj: [%s ~ %s] done, %d symbols",
            adj_start_str,
            adj_end_str,
            len(s.affected_symbols),
        )
    else:
        s.emit("sync_adj", 60, "除权因子完成,无新增")
        _root.logger.info("sync_adj: [%s ~ %s] no new factors", adj_start_str, adj_end_str)
    _root._invalidate("adj_factor")


def _run_enriched_prepare(s: _root._RunState) -> tuple:
    """Step 2 准备: 读日期集合、修异常分区、判新日期方向。"""
    import app.jobs.daily_pipeline as _root
    from app.storage.runtime import require_storage

    store = require_storage().store
    enriched_table, daily_table = "kline_daily_enriched", "kline_daily"
    daily_date_list = store.list_daily_dates(daily_table)
    enriched_date_list = store.list_daily_dates(enriched_table)
    enriched_exists = bool(enriched_date_list)
    daily_days = len(daily_date_list)
    prev_enriched_days = len(enriched_date_list)
    if enriched_exists:
        partial_pruned = _root._prune_partial_enriched_partitions(
            store, daily_table, enriched_table
        )
        stale_pruned = _root._prune_stale_price_partitions(store, daily_table, enriched_table)
        pruned_dates = sorted(set(partial_pruned) | set(stale_pruned))
        if pruned_dates:
            _root.logger.warning(
                "compute_enriched: 发现 %d 个异常 enriched 分区 (覆盖不全 %d / 收盘价过期 %d), "
                "已删除待重算: %s",
                len(pruned_dates),
                len(partial_pruned),
                len(stale_pruned),
                ", ".join(pruned_dates[:10]),
            )
            enriched_date_list = store.list_daily_dates(enriched_table)
            enriched_exists = bool(enriched_date_list)
            prev_enriched_days = len(enriched_date_list)
    forward_incremental = False
    backward_extension = False
    if daily_days > prev_enriched_days and enriched_exists:
        new_dates = set(daily_date_list) - set(enriched_date_list)
        if new_dates:
            if any(d < enriched_date_list[0] for d in new_dates):
                backward_extension = True
            forward_incremental = True
    return (
        store,
        enriched_table,
        daily_table,
        enriched_exists,
        forward_incremental,
        backward_extension,
        daily_days,
        prev_enriched_days,
    )


def _run_compute_enriched(s: _root._RunState) -> None:
    """Step 2: enriched 全量/增量/除权局部/跳过 四分支计算。"""
    import app.jobs.daily_pipeline as _root

    (
        store,
        enriched_table,
        _daily_table,
        enriched_exists,
        forward_incremental,
        backward_extension,
        daily_days,
        prev_enriched_days,
    ) = _root._run_enriched_prepare(s)

    def _enriched_batch_progress(cur: int, tot: int) -> None:
        s.emit(
            "compute_enriched",
            65 + int(23 * cur / tot),
            f"计算指标 批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot),
            skip_log=True,
        )

    if not enriched_exists or backward_extension:
        s.emit("compute_enriched", 65, "全量计算 enriched…")
        _root.logger.info(
            "compute_enriched: full rebuild (first=%s, backward=%s, daily=%d, enriched=%d)",
            not enriched_exists,
            backward_extension,
            daily_days,
            prev_enriched_days,
        )
        s.written_enriched = _root.run_pipeline(on_batch_done=_enriched_batch_progress)
        new_enriched_days = len(store.list_daily_dates(enriched_table))
        s.emit("compute_enriched", 88, f"enriched 完成,覆盖 {new_enriched_days} 天")
        _root.logger.info("compute_enriched: full rebuild done, %d days", new_enriched_days)
    elif forward_incremental:
        symbols_to_recompute = list(set(s.affected_symbols)) if s.affected_symbols else []
        s.emit(
            "compute_enriched",
            65,
            f"增量计算 enriched (新日期 + {len(symbols_to_recompute)} 只个股重算)…"
            if symbols_to_recompute
            else "增量计算 enriched (新日期)…",
        )
        _root.logger.info(
            "compute_enriched: forward incremental, %d symbols to recompute",
            len(symbols_to_recompute),
        )
        s.written_enriched = _root.run_pipeline(
            new_dates_only=True,
            symbols=symbols_to_recompute or None,
            on_batch_done=_enriched_batch_progress,
        )
        new_enriched_days = len(store.list_daily_dates(enriched_table))
        s.emit("compute_enriched", 88, f"enriched 完成,覆盖 {new_enriched_days} 天")
        _root.logger.info("compute_enriched: forward incremental done, %d days", new_enriched_days)
    elif s.affected_symbols:
        s.emit("compute_enriched", 65, f"增量计算 enriched ({len(s.affected_symbols)} 只个股)…")
        _root.logger.info(
            "compute_enriched: adj_factor incremental, %d symbols", len(s.affected_symbols)
        )
        s.written_enriched = _root.run_pipeline(
            symbols=s.affected_symbols, on_batch_done=_enriched_batch_progress
        )
        s.emit("compute_enriched", 88, f"enriched 完成,{len(s.affected_symbols)} 只个股")
    else:
        s.written_enriched = 0
        _root.logger.info("compute_enriched: skip (no new daily, no adj_factor changes)")
    _root._invalidate("enriched")


def _run_sync_index(s: _root._RunState) -> tuple[int, int]:
    """指数维表 + 指数日K同步; 返回 (指数只数, 写入行数)。"""
    import app.jobs.daily_pipeline as _root

    s.emit("sync_index", 88, "同步指数维表…")
    index_count = _root.index_sync.sync_index_instruments(s.repo, pull_index=True, pull_etf=False)
    s.emit("sync_index", 88, f"指数维表完成,{index_count} 只")
    from datetime import date as _date
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    storage = getattr(s.repo, "storage", None)
    if storage is not None:
        index_dates = storage.list_daily_dates("kline_index_enriched")
    else:
        index_dir = s.repo.store.data_dir / "kline_index_enriched"
        raw_dates = (
            sorted(
                d.name[5:]
                for d in index_dir.glob("date=*")
                if d.is_dir() and d.name.startswith("date=")
            )
            if index_dir.exists()
            else []
        )
        index_dates = [_date.fromisoformat(ds) for ds in raw_dates]
    if s.override_start_date:
        index_start = s.override_start_date
    else:
        index_start = index_dates[-1] if index_dates else s.today - _td(days=365)
    if s.index_stale_day is not None and index_start > s.index_stale_day:
        index_start = s.index_stale_day

    def _index_chunk(cur: int, tot: int) -> None:
        s.emit(
            "sync_index",
            88,
            f"指数日K批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot) if tot else 100,
            skip_log=cur < tot,
        )

    written_index_daily = _root.index_sync.sync_and_persist_index_daily(
        s.repo,
        s.capset,
        start_date=_dt.combine(index_start, _dt.min.time()),
        end_date=_dt.combine(s.today, _dt.min.time()),
        on_chunk_done=_index_chunk,
    )
    s.emit("sync_index", 88, f"指数日K完成,{written_index_daily} 行")
    _root._invalidate("index_instruments")
    _root._invalidate("index_daily")
    _root._invalidate("index_enriched")
    return index_count, written_index_daily
