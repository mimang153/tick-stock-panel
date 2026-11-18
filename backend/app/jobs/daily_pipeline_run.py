"""daily_pipeline_run: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


def _run_sync_etf(s: _root._RunState) -> tuple[int, int, int]:
    """ETF 维表 + 除权 + 日K同步; 返回 (ETF只数, 写入行数, 除权只数)。"""
    from datetime import date as _date
    from datetime import datetime, timedelta
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    import app.jobs.daily_pipeline as _root

    s.emit("sync_index", 88, "同步 ETF 维表…")
    etf_count = _root.index_sync.sync_etf_instruments(s.repo)
    s.emit("sync_index", 88, f"ETF 维表完成,{etf_count} 只")
    etf_symbols: list[str] = []
    etf_inst = s.repo.get_etf_instruments()
    if not etf_inst.is_empty() and "symbol" in etf_inst.columns:
        etf_symbols = sorted(set(etf_inst["symbol"].to_list()))
    etf_adj_symbols = 0
    if etf_symbols and s.capset.has(_root.Cap.ADJ_FACTOR):
        try:
            s.emit("sync_index", 88, "同步 ETF 除权因子…")
            adj_end = datetime.now()
            adj_path = s.repo.store.data_dir / "adj_factor_etf" / "all.parquet"
            history_start = adj_end - timedelta(days=365)
            adj_start = _root.index_sync.etf_adj_factor_window_start(adj_path, history_start)
            _, affected_etfs = _root.index_sync.sync_etf_adj_factor(
                etf_symbols,
                s.repo,
                s.capset,
                start_time=adj_start,
                end_time=adj_end,
            )
            etf_adj_symbols = len(affected_etfs)
            s.emit("sync_index", 88, f"ETF 除权因子完成,{etf_adj_symbols} 只")
        except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
            _root.logger.warning("ETF adj_factor skipped: %s", e)
            s.stage_errors.append(f"ETF adj_factor: {e}")
    etf_dir = s.repo.store.data_dir / "kline_etf_enriched"
    etf_dates = (
        sorted(
            d.name[5:] for d in etf_dir.glob("date=*") if d.is_dir() and d.name.startswith("date=")
        )
        if etf_dir.exists()
        else []
    )
    if s.override_start_date is not None:
        etf_start = s.override_start_date
    else:
        etf_start = _date.fromisoformat(etf_dates[-1]) if etf_dates else s.today - _td(days=365)
    if s.etf_stale_day is not None and etf_start > s.etf_stale_day:
        etf_start = s.etf_stale_day

    def _etf_chunk(cur: int, tot: int) -> None:
        s.emit(
            "sync_index",
            88,
            f"ETF 日K批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot) if tot else 100,
            skip_log=cur < tot,
        )

    written_etf_daily = _root.index_sync.sync_and_persist_etf_daily(
        s.repo,
        s.capset,
        start_date=_dt.combine(etf_start, _dt.min.time()),
        end_date=_dt.combine(s.today, _dt.min.time()),
        on_chunk_done=_etf_chunk,
    )
    s.emit("sync_index", 88, f"ETF 日K完成,{written_etf_daily} 行")
    _root._invalidate("etf_instruments")
    _root._invalidate("etf_daily")
    return etf_count, written_etf_daily, etf_adj_symbols


def _run_sync_index_etf(s: _root._RunState) -> None:
    """Step 2.3: 指数/ETF 同步编排 (物理分开存储; ETF 可复权, 指数不复权)。"""
    import app.jobs.daily_pipeline as _root

    pull_index = _root._prefs.get_pipeline_pull_index()
    pull_etf = _root._prefs.get_pipeline_pull_etf()
    from app.data_providers import custom as custom_sources

    daily_provider = _root._prefs.get_daily_data_provider()
    daily_allowed = _root.capset_has_daily(s.capset, daily_provider, custom_sources)
    if not daily_allowed or not (pull_index or pull_etf):
        s.skipped.append("sync_index")
        return
    _types = []
    if pull_index:
        _types.append("指数")
    if pull_etf:
        _types.append("ETF")
    s.emit("sync_index", 88, f"同步{'+'.join(_types)}日K…")
    try:
        if pull_index:
            s.index_count, s.written_index_daily = _root._run_sync_index(s)
        if pull_etf:
            s.etf_count, s.written_etf_daily, s.etf_adj_symbols = _root._run_sync_etf(s)
        s.emit(
            "sync_index",
            89,
            f"同步完成,指数 {s.index_count} 只/{s.written_index_daily} 行, "
            f"ETF {s.etf_count} 只/{s.written_etf_daily} 行"
            + (f", ETF复权 {s.etf_adj_symbols} 只" if s.etf_adj_symbols else ""),
        )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("sync_index/etf failed: %s", e)
        s.emit("sync_index", 89, f"指数/ETF同步失败:{e}")
        s.stage_errors.append(f"index/etf sync: {e}")


def capset_has_daily(capset: _root.CapabilitySet, daily_provider: str, custom_sources) -> bool:
    """日K批量拉取能力判定 (TickFlow 需能力, 自定义源需声明 daily 数据集)。"""
    import app.jobs.daily_pipeline as _root

    return capset.has(_root.Cap.KLINE_DAILY_BATCH) or (
        daily_provider != "tickflow"
        and custom_sources.provider_has_dataset(daily_provider, "daily")
    )


def _run_sync_minute(s: _root._RunState) -> None:
    """Step 2.5: 分钟 K 同步 (未启用或无能力时静默跳过, 不 emit)。"""
    import app.jobs.daily_pipeline as _root
    from app.services import preferences

    minute_on = preferences.get_minute_sync_enabled()
    minute_days = preferences.get_minute_sync_days()
    _, minute_fallback, _minute_resolve_err = _root.kline_sync._resolve_minute_provider(
        preferences.get_minute_data_provider()
    )
    minute_allowed = (not minute_fallback) or s.capset.has(_root.Cap.KLINE_MINUTE_BATCH)
    if not (minute_on and minute_allowed):
        s.skipped.append("sync_minute")
        if minute_on:
            _root.logger.info("sync_minute skipped: minute provider has no capability/dataset")
        else:
            _root.logger.info("sync_minute skipped: user disabled")
        return
    from datetime import timedelta as _td

    minute_start = s.today - _td(days=minute_days)
    s.emit("sync_minute", 90, f"获取分钟K [{minute_start} ~ {s.today}]…")
    _root.logger.info("sync_minute: [%s ~ %s] start", minute_start, s.today)
    minute_symbols = _root._resolve_minute_symbols(s.capset, s.repo)

    def _minute_chunk_progress(cur: int, tot: int, seg_label: str = "") -> None:
        s.emit(
            "sync_minute",
            90 + int(3 * cur / tot),
            f"分钟K 批次 {cur}/{tot}" + (f" [{seg_label}]" if seg_label else ""),
            stage_pct=int(100 * cur / tot),
            skip_log=True,
        )

    s.written_minute = _root.kline_sync.sync_and_persist_minute(
        minute_symbols,
        s.repo,
        s.capset,
        days=minute_days,
        on_chunk_done=_minute_chunk_progress,
    )
    minute_dir = s.repo.store.data_dir / "kline_minute"
    minute_cover_days = len(list(minute_dir.glob("date=*"))) if minute_dir.exists() else 0
    s.emit("sync_minute", 93, f"分钟K完成,覆盖 {minute_cover_days} 天")
    _root.logger.info(
        "sync_minute: [%s ~ %s] done, %d days", minute_start, s.today, minute_cover_days
    )
    _root._invalidate("minute")


def _run_compute_regime(s: _root._RunState) -> None:
    """Step 2.6: 市场环境增量计算 (默认关闭; 软失败不阻断主管道)。"""
    import app.jobs.daily_pipeline as _root
    from app.services import preferences as _prefs_regime

    if not _prefs_regime.get_pipeline_regime_enabled():
        s.skipped.append("regime")
        _root.logger.info("compute_regime skipped: user disabled (pipeline_regime_enabled=False)")
        return
    try:
        s.emit("compute_regime", 90, "计算市场环境…")
        from app.api.regime import invalidate_regime_cache
        from app.services import regime_builder

        new_regime = regime_builder.compute_regime_incremental(s.repo, s.repo.store.data_dir)
        s.regime_days = new_regime.height if not new_regime.is_empty() else 0
        if s.regime_days:
            invalidate_regime_cache()
            _root.logger.info("compute_regime: %d days", s.regime_days)
        s.emit("compute_regime", 92, f"市场环境 {s.regime_days} 天")
        if s.regime_days:
            try:
                _root._push_phase_change_alert(s.repo.store.data_dir)
            except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
                _root.logger.warning("phase change alert failed (soft): %s", e)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("compute_regime failed (soft): %s", e)
        s.stage_errors.append(f"compute_regime: {e}")
        s.skipped.append("regime")


def _run_compute_mainline(s: _root._RunState) -> None:
    """Step 2.7: 市场主线增量计算 (与 regime 同开关; 软失败不阻断)。"""
    import app.jobs.daily_pipeline as _root
    from app.services import preferences as _prefs_regime

    if not _prefs_regime.get_pipeline_regime_enabled():
        s.skipped.append("mainline")
        return
    try:
        s.emit("compute_mainline", 93, "计算市场主线…")
        from app.services import market_mainline

        for _kind in ("concept", "industry"):
            rows = market_mainline.compute_mainline_incremental(
                s.repo, s.repo.store.data_dir, kind=_kind
            )
            s.mainline_rows += rows.height if not rows.is_empty() else 0
        if s.mainline_rows:
            _root.logger.info("compute_mainline: %d rows", s.mainline_rows)
        s.emit("compute_mainline", 94, f"市场主线 {s.mainline_rows} 行")
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("compute_mainline failed (soft): %s", e)
        s.stage_errors.append(f"compute_mainline: {e}")
        s.skipped.append("mainline")


def _run_paper_settle(s: _root._RunState) -> None:
    """Step 2.8: 模拟盘结算 (恒运行; 软失败不阻断主管道)。"""
    import app.jobs.daily_pipeline as _root

    try:
        s.emit("paper_settle", 94, "模拟盘结算…")
        from app.strategy import paper as paper_trading

        totals = {"filled": 0, "expired": 0, "corp_actions": 0, "nav": None, "accounts": []}  # type: ignore[var-annotated]
        for acc_id in paper_trading.list_account_ids(s.repo.store.data_dir):
            st = paper_trading.settle_day(
                s.repo.store.data_dir, s.today.isoformat(), account_id=acc_id
            )
            totals["filled"] += st.get("filled", 0)
            totals["expired"] += st.get("expired", 0)
            totals["corp_actions"] += st.get("corp_actions", 0)
            if st.get("nav") is not None:
                totals["nav"] = st["nav"]
            totals["accounts"].append(  # type: ignore[union-attr]
                {"account": acc_id, **{k: st.get(k) for k in ("filled", "expired", "corp_actions")}}
            )
        s.paper_summary = totals
        if s.paper_summary.get("filled") or s.paper_summary.get("corp_actions"):
            _root.logger.info("paper_settle: %s", s.paper_summary)
        if totals["filled"]:
            try:
                from app.services import alert_store

                settle_events = []
                for acc_id in paper_trading.list_account_ids(s.repo.store.data_dir):
                    settle_events.extend(
                        paper_trading.day_fill_events(
                            s.repo.store.data_dir, s.today.isoformat(), account_id=acc_id
                        )
                    )
                if settle_events:
                    alert_store.append_many(s.repo.store.data_dir, settle_events)
            except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
                _root.logger.warning("模拟盘结算成交留痕失败: %s", e)
        s.emit("paper_settle", 94, "模拟盘结算完成")
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("paper_settle failed (soft): %s", e)
        s.stage_errors.append(f"paper_settle: {e}")


def _run_build_result(s: _root._RunState) -> dict:
    """汇总 result; 列表字段直接引用 s 上的同一对象 (与内联语义一致)。"""

    return {
        "universe_size": len(s.universe),
        "daily_days": s.new_daily_days,
        "adj_factor_symbols": len(s.affected_symbols),
        "enriched_days": s.written_enriched,
        "index_count": s.index_count,
        "index_daily_rows": s.written_index_daily,
        "etf_count": s.etf_count,
        "etf_daily_rows": s.written_etf_daily,
        "etf_adj_factor_symbols": s.etf_adj_symbols,
        "minute_rows": s.written_minute,
        "regime_days": s.regime_days,
        "mainline_rows": s.mainline_rows,
        "paper_settle": s.paper_summary,
        "lagging_symbols": len(s.lagging_symbols),
        "enriched_total_days": s.enriched_total_days,
        "integrity_repair_from": s.repair_start.isoformat() if s.repair_start else None,
        "integrity_issues": len(s.integrity_issues),
        "skipped_stages": s.skipped,
        "stage_errors": s.stage_errors,
    }


def _run_post_repair_check(s: _root._RunState) -> None:
    """修复路径后置校验: 修完重扫, 仍有坏分区时如实报失败。"""
    import app.jobs.daily_pipeline as _root

    if s.override_start_date is None:
        return
    try:
        from app.services import data_integrity as _di

        remaining_issues = _di.scan_recent_integrity(s.repo.store.data_dir, today=s.today)
        checked_tables = set(_di.active_daily_tables())
        remaining_issues = [i for i in remaining_issues if i.table in checked_tables]
        if remaining_issues:
            s.stage_errors.append(
                f"数据修复后完整性仍不通过: {_di.describe_issues(remaining_issues)}"
                f" (剩余 {len(remaining_issues)} 个不完整分区;"
                " 请查看同步日志, 或到 数据→修正数据 手动执行)"
            )
            _root.logger.warning(
                "integrity: post-repair scan found %d remaining issues: %s",
                len(remaining_issues),
                _di.describe_issues(remaining_issues),
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("integrity: post-repair scan failed (soft): %s", e)


def run_now(
    repo: _root.KlineRepository,
    capset: _root.CapabilitySet,
    on_progress: _root.ProgressCb | None = None,
    override_start_date: _root.date | None = None,
    override_symbols: list[str] | None = None,
) -> dict:
    """立即执行一次盘后管道,支持进度回调。

    跳过的 stage **不 emit**,避免前端把"无 capability"的卡片错误标记为 active/done。
    result 里带 skipped_stages 列表供前端展示。

    override_start_date: 传入时强制走 batch 拉取分支,用该日期作为日K/除权/指数的
        拉取起点(到今天),用于「数据修正/补数据」场景。None 时走原有自动判定逻辑。
    override_symbols: 数据修正时只重拉这些股票的日K (issue #514: 修复只补坏标的,
        不再全市场重拉)。None = 全标的池。仅作用于日K (kline_daily) 阶段;
        ETF/指数族沿用各自的全量口径。
    """
    import app.jobs.daily_pipeline as _root

    s = _root._RunState(
        repo=repo,
        capset=capset,
        emit=on_progress or _root._noop,
        override_start_date=override_start_date,
        override_symbols=override_symbols,
    )
    # Step 0: 先同步个股维表, 再解析标的池 — 确保标的池基于最新 instruments
    _root._run_prepare(s)

    # Step 1: 日 K 同步 + 完整性自愈 (命中缺口则放弃实时覆写, 降级范围拉取)
    s.today = _root.cn_today()  # 管道「今天」必须是北京日期, 不能用 date.today()
    _root._run_scan_integrity(s)
    # A 股日K拉取开关(默认开);关闭时跳过日K同步,保留已有数据。
    # 数据修正(override_start_date)时即使关闭开关也强制拉取 — 修正就是来补数据的。
    _root._run_sync_daily(s)

    # 完整性修复时删除坏分区 + 掉队标的可见化
    s.repair_start = s.override_start_date if s.override_start_date is not None else s.stale_day
    _root._run_prune_enriched(s)
    _root._run_detect_lagging(s)

    # Step 1.5: 同步除权因子 — 范围与日K拉取方式对齐
    _root._run_sync_adj(s)

    # Step 2: 计算 enriched (全量/增量/除权局部/跳过 四分支)
    _root._run_compute_enriched(s)

    # Step 2.1: 数据充足性可见化 (#303) — 暖机不足时 WARN 引导全量回填
    s.enriched_total_days = _root.warn_if_enriched_too_thin(s.repo.store.data_dir)

    # Step 2.3: 指数 / ETF 同步 — 物理分开存储; ETF 可复权, 指数不复权
    _root._run_sync_index_etf(s)

    # Step 2.5: 分钟 K 同步(可选) — 未启用或无能力时静默跳过(不 emit)
    _root._run_sync_minute(s)

    # Step 2.6/2.7: 市场环境 + 市场主线增量计算 (默认关闭; 软失败不阻断)
    _root._run_compute_regime(s)
    _root._run_compute_mainline(s)

    # Step 2.8: 模拟盘结算 (恒运行; 软失败不阻断主管道)
    _root._run_paper_settle(s)

    # Step 3: 视图刷新已移除 (DuckDB 删除, B5.1)
    s.emit("done", 100, "完成")
    _root._invalidate(None)  # 兜底:全清
    result = _root._run_build_result(s)
    _root._run_post_repair_check(s)

    # 有阶段软失败: 进度协议已走完(done/100), 但数据可能陈旧, 抛给 job_store 标记 failed
    if s.stage_errors:
        raise _root.PipelineStageError(s.stage_errors)

    return result
