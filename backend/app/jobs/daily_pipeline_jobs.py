"""daily_pipeline_jobs: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


def _recompute_enriched_for_affected(affected, emit) -> int:
    """独立除权同步后的受影响个股 enriched 增量重算。"""
    import app.jobs.daily_pipeline as _root
    from app.storage.runtime import require_storage

    enriched_exists = bool(require_storage().store.list_daily_dates("kline_daily_enriched"))
    written_enriched = 0
    if affected and enriched_exists:

        def _enriched_batch_progress(cur: int, tot: int) -> None:
            emit(
                "compute_enriched",
                65 + int(30 * cur / tot),
                f"计算指标 批次 {cur}/{tot}",
                stage_pct=int(100 * cur / tot),
                skip_log=True,
            )

        emit("compute_enriched", 65, f"增量计算 enriched ({len(affected)} 只个股)…")
        _root.logger.info(
            "compute_enriched: adj_factor incremental (standalone), %d symbols", len(affected)
        )
        written_enriched = _root.run_pipeline(
            symbols=affected, on_batch_done=_enriched_batch_progress
        )
        emit("compute_enriched", 95, f"enriched 完成,{len(affected)} 只个股")
        _root._invalidate("enriched")
    elif affected:
        _root.logger.info(
            "standalone adj sync: enriched 未构建, 跳过局部重算 (交由盘后管道全量计算)"
        )
    return written_enriched


def run_adj_factor_sync(
    repo: _root.KlineRepository,
    capset: _root.CapabilitySet,
    on_progress: _root.ProgressCb | None = None,
) -> dict:
    """独立同步除权因子；拉全历史，并按管道同语义刷新视图与局部重算。"""
    from datetime import datetime

    import app.jobs.daily_pipeline as _root

    emit = on_progress or _root._noop
    adj_provider = _root._prefs.get_adj_factor_provider()
    if not (capset.has(_root.Cap.ADJ_FACTOR) or adj_provider != "tickflow"):
        return {
            "adj_factor_symbols": 0,
            "adj_written": 0,
            "enriched_days": 0,
            "skipped_stages": ["sync_adj"],
            "reason": "无可用除权因子数据源",
        }

    emit("resolve_universe", 5, "解析标的池…")
    universe = _root._resolve_universe(capset, repo)
    emit("resolve_universe", 10, f"标的池规模:{len(universe)} 只")

    def _adj_chunk_progress(cur: int, tot: int) -> None:
        emit(
            "sync_adj",
            10 + int(55 * cur / tot),
            f"除权因子批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot),
            skip_log=True,
        )

    # 按需窗口: 钳到本地日K起点。前复权里早于任一根已存K线的事件在
    # cum/total 两侧同时出现、恰好抵消, 对所有已存价格零影响 (见
    # indicators.pipeline._apply_adj_factor) — 拉它们只会为远古事件
    # 逐只回补历史K线, 全市场一次曾多花 ~15 分钟。
    store_min = repo.earliest_daily_date()
    start = datetime(store_min.year, store_min.month, store_min.day) if store_min else None
    window_label = f"自 {store_min.isoformat()}" if store_min else "全历史"
    _root.logger.info(
        "standalone adj sync: %d symbols, event window %s",
        len(universe),
        window_label,
    )
    emit("sync_adj", 10, f"获取除权因子 [{window_label}, {len(universe)} 只]…")
    written_adj, affected = _root.kline_sync.sync_adj_factor(
        universe,
        repo,
        capset,
        start_time=start,
        end_time=datetime.now(),
        on_chunk_done=_adj_chunk_progress,
    )
    affected = sorted(set(affected))
    _root._refresh_single_view(repo, "adj_factor")
    emit("sync_adj", 65, f"除权因子完成,新增 {written_adj} 行,因子变更 {len(affected)} 只")
    _root._invalidate("adj_factor")

    # enriched 局部重算: 与管道「无新日期 + 有新除权因子」分支同语义 (只重算受影响个股)
    written_enriched = _root._recompute_enriched_for_affected(affected, emit)

    emit("done", 100, "完成")
    return {
        "universe_size": len(universe),
        "adj_written": written_adj,
        "adj_factor_symbols": len(affected),
        "enriched_days": written_enriched,
        "skipped_stages": [],
    }


def warn_if_enriched_too_thin(data_dir: _root.Path) -> int:
    """enriched 总覆盖天数; 低于常见指标暖机窗口时 WARN 引导全量回填 (#303)。

    返回天数供管道 result 上报。目录列举 O(天数), 不在热路径。
    """
    import app.jobs.daily_pipeline as _root
    from app.services.screener import MIN_INDICATOR_WARMUP_DAYS, enriched_history_days

    days = enriched_history_days(data_dir)
    if days < MIN_INDICATOR_WARMUP_DAYS:
        _root.logger.warning(
            "enriched 仅覆盖 %d 个交易日 (<%d): 指标暖机不足, 策略可能全部 0 命中且无提示 — "
            "建议全量回填 (同步标的维表 → 日K批量同步(带后缀符号) → 重算 enriched)",
            days,
            MIN_INDICATOR_WARMUP_DAYS,
        )
    return days


def _resolve_minute_symbols(capset: _root.CapabilitySet, repo=None) -> list[str]:
    """分钟 K 同步标的 — 与日K共用同一标的池。"""
    import app.jobs.daily_pipeline as _root

    return _root._resolve_universe(capset, repo)


def _refresh_single_view(repo: _root.KlineRepository, name: str) -> None:
    """兼容占位：DuckDB 视图已移除 (B5.1)，保留供测试 monkeypatch。"""

    return None


def _refresh_views(repo: _root.KlineRepository) -> None:
    """兼容占位：DuckDB 视图已移除 (B5.1)，保留供测试 monkeypatch。"""

    return None


def _push_phase_change_alert(data_dir) -> None:
    """情绪周期阶段切换 → 推送监控通知(SSE toast + 监控中心)。

    阶段切换(如 退潮→冰点)是重要的市场信号, 原先只有打开市场环境页才能看到。
    复用 quote_service.push_alerts 广播通道; 未发生切换静默返回。
    """
    import app.jobs.daily_pipeline as _root
    from app.services.market_phase import PHASE_LABELS
    from app.services.regime_builder import latest_phase_transition

    tr = latest_phase_transition(data_dir)
    if not tr:
        return
    prev, cur, d = tr
    msg = f"情绪周期阶段切换: {PHASE_LABELS.get(prev, prev)} → {PHASE_LABELS.get(cur, cur)} ({d})"
    severity = "warn" if cur in ("ebb", "ice") else "info"
    app_state = _root._get_app_state()
    qs = getattr(app_state, "quote_service", None) if app_state else None
    if qs:
        qs.push_alerts(
            [
                {
                    "source": "market",
                    "type": "phase_change",
                    "message": msg,
                    "severity": severity,
                }
            ]
        )
    _root.logger.info("phase change alert: %s (severity=%s)", msg, severity)


def _run_tracked(fn, job_label: str) -> bool:
    """调度触发时包装 JobStore 跟踪，确保同步历史有记录。

    单飞: 若已有活跃(pending∨running)任务(手动同步中), 本次调度直接跳过, 不并发。
    重任务执行槽: 再挡一层僵尸并发(reap 后线程仍活时不得并行写 parquet)。
    返回 True 仅表示任务已成功并且执行槽已释放。
    """
    import app.jobs.daily_pipeline as _root
    from app.services.pipeline_jobs import (
        JobCancelledError,
        job_store,
        release_run_slot,
        run_with_capacity,
        try_acquire_run_slot,
    )

    job_id, is_new = job_store.create()
    if not is_new:
        _root.logger.info("scheduled %s 跳过: 已有活跃任务在运行 (job_id=%s)", job_label, job_id)
        return False
    if not try_acquire_run_slot(job_id):
        _root.logger.warning("scheduled %s 跳过: 重任务执行槽被占用(疑似上次任务卡死)", job_label)
        job_store.fail(job_id, f"scheduled {job_label} skipped: 已有数据任务在运行")
        return False

    def progress(
        stage: str, pct: int, msg: str, stage_pct: int | None = None, skip_log: bool = False
    ) -> None:
        job_store.progress(job_id, stage, pct, msg, stage_pct=stage_pct, skip_log=skip_log)

    succeeded = False
    try:
        result = run_with_capacity(job_id, lambda: fn(on_progress=progress))
        job_store.succeed(job_id, result)
        succeeded = True
        _root.logger.info("scheduled %s completed: job_id=%s", job_label, job_id)
    except JobCancelledError:
        # 已由 terminate() 标记失败(卡死/手动取消), 拉取线程在分块回调处自行退出
        _root.logger.warning("scheduled %s cancelled: job_id=%s", job_label, job_id)
    except Exception:  # noqa: BLE001 - 记录异常并置任务失败，非静默吞异常
        _root.logger.exception("scheduled %s failed: job_id=%s", job_label, job_id)
        job_store.fail(job_id, f"scheduled {job_label} failed")
    finally:
        release_run_slot(job_id)
    return succeeded


def _scheduled_pipeline_task(pipeline_fn) -> None:
    """Run the scheduled lab cycle (weekly mining + daily guardian) after pipeline success."""
    import app.jobs.daily_pipeline as _root

    if not _root._run_tracked(pipeline_fn, "daily_pipeline"):
        return
    try:
        from app.services.lab_schedule import run_scheduled_lab

        result = run_scheduled_lab(_root._get_app_state())
        _root.logger.info("scheduled lab result: %s", result)
    except Exception:  # noqa: BLE001 - 复盘入队失败不影响主管道，记录后继续
        _root.logger.exception("scheduled lab enqueue failed; daily pipeline remains succeeded")
