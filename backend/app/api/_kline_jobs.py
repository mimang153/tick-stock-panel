"""K 线后台任务：分钟同步、日 K 修复与 enriched 重算的任务体。"""

from __future__ import annotations

from datetime import date
from functools import partial

from fastapi import HTTPException

from app.api._root_ref import bind as _bind
from app.services import kline_sync, minute_gaps

_root = _bind("app.api.kline")
logger = _root.logger


def _minute_sync_universe(repo, repair_symbols: list[str] | None, get_pool) -> list[str]:
    """解析分钟同步标的集：优先用修复目标，否则取自选∪标的库并剔除无本地分钟K的指数。"""
    if repair_symbols is not None:
        return repair_symbols
    all_symbols = set(get_pool("watchlist"))
    try:
        from app.services.instrument_sync import load_instruments

        all_symbols.update(load_instruments(repo.storage)["symbol"].to_list())
    except Exception:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        pass
    # 指数分钟K无本地存储, 剔除后避免污染 kline_minute
    index_set = repo.get_index_symbol_set()
    return sorted(s for s in all_symbols if s not in index_set)


def _run_minute_sync_fetch(
    repo,
    capset,
    *,
    repair_symbols: list[str] | None,
    repair_targets: dict[date, list[str]] | None,
    universe: list[str],
    days,
    extend_backward: bool,
    progress,
    on_chunk,
) -> dict:
    """执行分钟同步取数；补拉路径失败也刷新已写入分区。"""
    from app.jobs.daily_pipeline import _refresh_single_view

    try:
        if repair_targets:
            added = remaining = 0
            source = kline_sync.minute_source_stamp()
            for group_index, (day, group) in enumerate(repair_targets.items()):
                group_on_chunk = partial(on_chunk, group_index=group_index)
                group_on_chunk(0, 1, day.isoformat())
                added += kline_sync.sync_and_persist_minute(
                    group,
                    repo,
                    capset,
                    repair_date=day,
                    asset_type="stock",
                    on_chunk_done=group_on_chunk,
                    expected_source=source,
                )
                remaining += minute_gaps.count_remaining_gaps(
                    repo.store.data_dir / "kline_minute", day, group, _root.cn_now()
                )
            return {
                "minute_rows": added,
                "remaining_gaps": remaining,
                "requested_gaps": sum(map(len, repair_targets.values())),
            }
        if repair_symbols is not None:
            progress("sync_minute", 10, f"补拉 {len(universe)} 只缺口标的的分钟K…")
        written = kline_sync.sync_and_persist_minute(
            universe,
            repo,
            capset,
            days=days,
            extend_backward=extend_backward,
            force_full_days=repair_symbols is not None,
            on_chunk_done=on_chunk,
        )
        return {"minute_rows": written}
    finally:
        if repair_symbols is not None:
            _refresh_single_view(repo, "kline_minute")


def _minute_sync_message(result: dict, repair_targets, repair_symbols) -> str:
    written = result["minute_rows"]
    if repair_targets:
        return (
            f"新增 {written} 根分钟 K，复查 {result['requested_gaps']} 条，"
            f"仍有 {result['remaining_gaps']} 条缺口"
        )
    if repair_symbols is not None:
        return f"缺口分钟补拉完成, 写入 {written} 行; 是否补齐以体检结果为准"
    return f"分钟 K 同步完成,{written} 行"


async def _run_minute_sync_job(
    repo,
    capset,
    job_id,
    override_days,
    extend_flag,
    repair_symbols: list[str] | None,
    repair_targets: dict[date, list[str]] | None,
) -> None:
    """分钟同步后台任务：容量、进度、取消与缓存失效顺序保持原样。"""
    import asyncio

    from app.api.data import invalidate_storage_cache
    from app.services.pipeline_jobs import (
        JobCancelledError,
        job_store,
        release_run_slot,
        run_with_capacity,
        try_acquire_run_slot,
    )
    from app.services.preferences import get_minute_sync_days
    from app.tickflow.pools import get_pool

    if not try_acquire_run_slot(job_id):
        job_store.fail(job_id, "已有数据任务在运行(或上一次任务卡死未结束),请稍后再试")
        return
    loop = asyncio.get_event_loop()

    def progress(stage: str, pct: int, msg: str) -> None:
        job_store.progress(job_id, stage, pct, msg)

    try:
        progress(
            "sync_minute",
            5,
            "准备补拉缺口标的…" if repair_symbols is not None else "解析标的池…",
        )
        universe = _minute_sync_universe(repo, repair_symbols, get_pool)
        progress("sync_minute", 10, f"标的池 {len(universe)} 只")

        days = override_days if override_days else get_minute_sync_days()
        extend_backward = bool(extend_flag) or days >= 365
        group_count = len(repair_targets) if repair_targets else 1

        def _on_chunk(done: int, total: int, seg_label: str, *, group_index: int = 0) -> None:
            pct = 10 + int((group_index + done / max(total, 1)) / group_count * 85)
            action = "补拉缺口分钟K" if repair_symbols is not None else "拉取分钟K"
            progress("sync_minute", pct, f"{action}… {done}/{total} 批 [{seg_label}]")

        result = await loop.run_in_executor(
            _long_task_executor,
            run_with_capacity,
            job_id,
            lambda: _run_minute_sync_fetch(
                repo,
                capset,
                repair_symbols=repair_symbols,
                repair_targets=repair_targets,
                universe=universe,
                days=days,
                extend_backward=extend_backward,
                progress=progress,
                on_chunk=_on_chunk,
            ),
        )
        if repair_symbols is None:
            from app.jobs.daily_pipeline import _refresh_single_view

            _refresh_single_view(repo, "kline_minute")
        progress("done", 100, _minute_sync_message(result, repair_targets, repair_symbols))
        job_store.succeed(job_id, {**result, "universe_size": len(universe)})
        invalidate_storage_cache()
    except JobCancelledError:
        invalidate_storage_cache()
    except Exception as e:  # noqa: BLE001 - 外部任务失败需写入 job 状态
        job_store.fail(job_id, str(e))
        invalidate_storage_cache()
    finally:
        release_run_slot(job_id)


async def trigger_minute_sync(
    repo,
    capset,
    *,
    override_days=None,
    extend_flag=None,
    repair_symbols: list[str] | None = None,
    repair_targets: dict[date, list[str]] | None = None,
) -> dict:
    """复用分钟同步的分块落盘、容量限制和取消机制; 缺口补拉限定标的且强制回溯。"""
    import asyncio

    from app.services.pipeline_jobs import job_store

    if not _root._minute_allowed(capset):
        if repair_symbols is not None:
            raise HTTPException(
                status_code=403, detail="当前分钟数据源不可用, 请先在设置中配置可用数据源"
            )
        raise HTTPException(status_code=403, detail="需要 Pro+ 权限")
    # 分钟K全市场同步是长任务(数据量是日K的 ~240 倍),用更宽松的卡死阈值
    job_id, is_new = job_store.create(long_running=True)
    if not is_new:
        if repair_symbols is not None:
            raise HTTPException(
                status_code=409, detail="已有数据任务正在运行, 请等待完成后再批量补拉"
            )
        return {"status": "reused", "job_id": job_id}

    asyncio.create_task(  # noqa: RUF006 - 后台任务引用需保持存活, 由框架管理生命周期
        _run_minute_sync_job(
            repo,
            capset,
            job_id,
            override_days,
            extend_flag,
            repair_symbols,
            repair_targets,
        )
    )
    return {"status": "started", "job_id": job_id}


async def _run_repair_daily_job(request, repo, capset, start_date, job_id) -> None:
    """后台执行日K修复任务；保留原暂停行情/容量/缓存失效顺序。"""
    import asyncio

    from app.api.data import invalidate_storage_cache
    from app.services.pipeline_jobs import (
        JobCancelledError,
        job_store,
        release_run_slot,
        run_with_capacity,
        try_acquire_run_slot,
    )
    from app.services.repair_daily import run_repair_daily

    if not try_acquire_run_slot(job_id):
        job_store.fail(job_id, "已有数据任务在运行(或上一次任务卡死未结束),请稍后再试")
        return
    loop = asyncio.get_event_loop()
    qs = getattr(request.app.state, "quote_service", None)

    def progress(stage, pct, msg, stage_pct=None, skip_log=False) -> None:
        job_store.progress(job_id, stage, pct, msg, stage_pct=stage_pct, skip_log=skip_log)

    def _run() -> dict:
        if qs:
            with qs.paused():
                return run_repair_daily(repo, capset, start_date, on_progress=progress)
        return run_repair_daily(repo, capset, start_date, on_progress=progress)

    try:
        result = await loop.run_in_executor(_long_task_executor, run_with_capacity, job_id, _run)
        if "error" in result:
            job_store.fail(job_id, result["error"])
        else:
            job_store.succeed(job_id, result)
        invalidate_storage_cache()
    except JobCancelledError:
        invalidate_storage_cache()
    except Exception as e:
        logger.exception("repair_daily failed: job_id=%s", job_id)
        job_store.fail(job_id, str(e))
        invalidate_storage_cache()
    finally:
        release_run_slot(job_id)


async def _run_rebuild_enriched_job(repo, job_id) -> None:
    """后台重算 enriched；缓存刷新在 executor 内执行避免阻塞事件循环。"""
    import asyncio

    from app.api.data import invalidate_storage_cache
    from app.services.pipeline_jobs import (
        JobCancelledError,
        job_store,
        release_run_slot,
        run_with_capacity,
        try_acquire_run_slot,
    )

    if not try_acquire_run_slot(job_id):
        job_store.fail(job_id, "已有数据任务在运行(或上一次任务卡死未结束),请稍后再试")
        return
    loop = asyncio.get_event_loop()

    def progress(stage, pct, msg, stage_pct=None, skip_log=False) -> None:
        job_store.progress(job_id, stage, pct, msg, stage_pct=stage_pct, skip_log=skip_log)

    try:
        progress("rebuild_enriched", 10, "全量计算 enriched…")
        from app.indicators.pipeline import run_pipeline

        def _batch_progress(cur: int, tot: int) -> None:
            pct = 10 + int(85 * cur / tot)
            progress(
                "rebuild_enriched",
                pct,
                f"计算指标 批次 {cur}/{tot}",
                stage_pct=int(100 * cur / tot),
                skip_log=True,
            )

        written = await loop.run_in_executor(
            _long_task_executor,
            run_with_capacity,
            job_id,
            lambda: run_pipeline(on_batch_done=_batch_progress),
        )
        enriched_dir = repo.store.data_dir / "kline_daily_enriched"
        enriched_days = len(list(enriched_dir.glob("date=*"))) if enriched_dir.exists() else 0

        def _reload_memory_cache() -> None:
            repo.clear_cache()
            repo.refresh_cache()

        await loop.run_in_executor(_long_task_executor, _reload_memory_cache)
        progress("rebuild_enriched", 100, f"完成,覆盖 {enriched_days} 天")
        job_store.succeed(job_id, {"enriched_days": enriched_days, "enriched_rows": written})
        invalidate_storage_cache()
    except JobCancelledError:
        invalidate_storage_cache()
    except Exception as e:
        logger.exception("rebuild_enriched failed: job_id=%s", job_id)
        job_store.fail(job_id, str(e))
        invalidate_storage_cache()
    finally:
        release_run_slot(job_id)


# 长时间任务专用线程池（隔离于 FastAPI 默认线程池，防止阻塞请求处理）
import concurrent.futures as _cf  # noqa: E402 - E402: 延迟导入避免循环依赖/降低启动开销

_long_task_executor = _cf.ThreadPoolExecutor(max_workers=2, thread_name_prefix="long-task")
