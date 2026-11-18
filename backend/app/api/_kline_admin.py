"""K 线运维路由：同步、缺口、迁移、清空、历史扩展与重算。"""

from __future__ import annotations

from datetime import date

import anyio
from fastapi import HTTPException, Query, Request

from app.api._root_ref import bind as _bind
from app.services import kline_sync, minute_adjust, minute_gaps

from ._kline_jobs import _long_task_executor, _run_rebuild_enriched_job, _run_repair_daily_job

_root = _bind("app.api.kline")
logger = _root.logger


@_root.router.post("/sync")
def sync_symbol(
    request: Request,
    symbol: str = Query(...),
    days: int = Query(250, ge=10, le=2000),
):
    """手动触发单股同步(Free 用户在 K 线页用)。"""
    repo = request.app.state.repo
    capset = request.app.state.capabilities
    zero: list[str] = []
    n = kline_sync.sync_and_persist_daily_batch(
        [symbol], repo, capset, count=days, zero_row_out=zero
    )
    resp = {"symbol": symbol, "rows_written": n}
    if zero:
        resp["zero_row_symbols"] = zero
    return resp


@_root.router.post("/sync_batch")
def sync_batch(
    request: Request,
    symbols: list[str],
    days: int = Query(250, ge=10, le=2000),
):
    repo = request.app.state.repo
    capset = request.app.state.capabilities
    zero: list[str] = []
    n = kline_sync.sync_and_persist_daily_batch(
        symbols, repo, capset, count=days, zero_row_out=zero
    )
    resp = {"symbols": symbols, "rows_written": n}
    if zero:
        # fail-loud (#302): 裸符号被跳过/上游 200 空数据的标的显式列出,
        # 不再"回填显示成功、实际全库 0 行"
        resp["zero_row_symbols"] = zero
    return resp


@_root.router.post("/refresh_views")
def refresh_views(request: Request):
    """刷新所有 DuckDB 视图(解决视图状态不一致问题)。"""
    from app.jobs.daily_pipeline import _refresh_views

    repo = request.app.state.repo
    _refresh_views(repo)
    return {"status": "ok"}


@_root.router.post("/sync_minute")
async def sync_minute(request: Request):
    """手动触发分钟 K 同步(全市场)。返回 pipeline job_id 可轮询进度。

    body 可选: { "days": int } — 指定拉取天数 (不传则用偏好设置)。
    """
    # 可选 body: { "days": int, "extend": bool }
    # days: 拉取天数; extend: 向前扩展模式 (从最早数据往前补)
    body = {}
    try:  # noqa: SIM105 - SIM105: 显式except表达降级路径, contextlib写法更晦涩
        body = await request.json()
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        pass
    return await _root.trigger_minute_sync(
        request.app.state.repo,
        request.app.state.capabilities,
        override_days=body.get("days"),
        extend_flag=body.get("extend"),
    )


@_root.router.post("/repair_minute_gaps")
async def repair_minute_gaps(request: Request, body: dict):
    """定向批量补拉缺口标的, 返回现有管道任务 ID; 不承诺源窗口外的缺口可补齐。"""
    repair_targets: dict[date, list[str]] = {}
    if "targets" in body:
        targets = body["targets"]
        if (
            "symbols" in body
            or not isinstance(targets, list)
            or not 1 <= len(targets) <= minute_gaps.MAX_GAP_ROWS
        ):
            raise HTTPException(
                status_code=400,
                detail=f"targets 必须单独传入 1 至 {minute_gaps.MAX_GAP_ROWS} 条缺口",
            )
        today = _root.cn_today()
        for target in targets:
            if (
                not isinstance(target, dict)
                or not isinstance(target.get("symbol"), str)
                or not target["symbol"].strip()
            ):
                raise HTTPException(status_code=400, detail="缺口必须包含有效标的代码")
            value = target.get("date")
            try:
                day = date.fromisoformat(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="缺口日期必须为 YYYY-MM-DD") from None
            if day.isoformat() != value or day > today:
                raise HTTPException(
                    status_code=400, detail="缺口日期必须为 YYYY-MM-DD 且不能在未来"
                )
            repair_targets.setdefault(day, []).append(target["symbol"].strip())
        repair_targets = {day: list(dict.fromkeys(group)) for day, group in repair_targets.items()}
        symbols = [s for group in repair_targets.values() for s in group]
    else:
        # 兼容旧客户端只按标的补拉最近窗口。
        symbols = body.get("symbols")  # type: ignore[assignment]
        if not isinstance(symbols, list) or not symbols or len(symbols) > minute_gaps.MAX_GAP_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"symbols 必须是 1 至 {minute_gaps.MAX_GAP_ROWS} 只标的的列表",
            )
    if any(not isinstance(symbol, str) or not symbol.strip() for symbol in symbols):
        raise HTTPException(status_code=400, detail="标的代码必须是非空字符串")
    symbols = list(dict.fromkeys(symbol.strip() for symbol in symbols))
    repo = request.app.state.repo
    if any(repo.resolve_asset_type(symbol) != "stock" for symbol in symbols):
        raise HTTPException(
            status_code=400, detail="分钟缺口补拉仅支持股票, ETF 和指数请使用各自的数据入口"
        )
    return await _root.trigger_minute_sync(
        repo,
        request.app.state.capabilities,
        override_days=10,
        repair_symbols=symbols,
        repair_targets=repair_targets or None,
    )


@_root.router.post("/sync_minute_single")
async def sync_minute_single(request: Request, body: dict):
    """手动拉取单只股票的分钟K并落库 (口径随基准标记: 未迁移=前复权, 已迁移=原始)。

    body: { "symbol": "000001.SZ" }
    用于个股分时图"获取数据"按钮: 本地无数据时单独拉取并持久化。
    """
    from app.services.preferences import get_minute_sync_days

    symbol = body.get("symbol", "").strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol 不能为空")

    requested_days = body.get("days")
    if requested_days is not None:
        if isinstance(requested_days, bool) or not isinstance(requested_days, int):
            raise HTTPException(status_code=400, detail="days 必须是整数")
        if requested_days < 1 or requested_days > 30:
            raise HTTPException(status_code=400, detail="days 必须在 1 到 30 之间")

    repo = request.app.state.repo
    capset = request.app.state.capabilities

    # 指数分钟K无本地存储, 落库会污染股票分钟表 kline_minute;
    # 指数分钟数据走 /api/index/minute 实时读取, 此端点显式拒绝。
    asset_type = repo.resolve_asset_type(symbol)
    if asset_type == "index":
        raise HTTPException(
            status_code=400,
            detail="指数分钟K不支持落库同步 (指数分钟数据走 /api/index/minute 实时读取)",
        )

    if not _root._minute_allowed(capset):
        raise HTTPException(status_code=403, detail="需要 Pro+ 权限")

    days = requested_days if requested_days is not None else get_minute_sync_days()
    from app.services.minute_shared_pull import sync_history

    qs = getattr(request.app.state, "quote_service", None)
    written = await anyio.to_thread.run_sync(
        lambda: sync_history(
            repo,
            capset,
            symbol,
            asset_type,
            days,
            on_updated=qs.notify_data_updated if qs else None,
        )
    )

    # 刷新视图
    from app.jobs.daily_pipeline import _refresh_single_view

    _refresh_single_view(repo, "kline_etf_minute" if asset_type == "etf" else "kline_minute")

    return {"status": "ok", "symbol": symbol, "rows": written}


@_root.router.get("/minute-gaps")
def get_minute_gaps(
    request: Request,
    symbols: str | None = Query(None, description="逗号分隔, 只看这些标的"),
    dates: str | None = Query(None, description="逗号分隔 YYYY-MM-DD, 只看这些交易日"),
    limit: int = Query(500, ge=1, le=2000, description="最多返回的缺口行数"),
):
    """分钟数据缺口体检: 列出本地分区中分钟 K 不完整的 (标的, 交易日)。

    只做**发现**, 不做补齐 —— 上游分钟源通常只提供最近 N 天, 超出窗口的历史
    缺口补不回来, 盲目补拉会白等且返回空。前端据此展示残缺清单并标注
    「可补 / 超出源窗口」, 避免用户误以为「同步完成 = 数据完整」。
    """
    from app.services.minute_gaps import scan_minute_gaps

    repo = request.app.state.repo
    now = _root.cn_now()
    return scan_minute_gaps(
        repo.store.data_dir / "kline_minute",
        today=_root.cn_today(),
        now_hm=(now.hour, now.minute),
        symbols=[s.strip() for s in symbols.split(",") if s.strip()] if symbols else None,
        dates=[d.strip() for d in dates.split(",") if d.strip()] if dates else None,
        limit=limit,
    )


@_root.router.post("/minute-migrate")
async def minute_migrate(request: Request):
    """存量分钟K迁移为原始口径并启用读取时复权 (幂等, 见 services/minute_adjust)。

    - 用日K原始收盘价做锚点, 把历史"拉取时前复权"的分区换算回原始价;
    已是原始价的行 (全量分钟落盘) 锚点 k≈1 自动跳过;
    - 全部分区成功后创建 .raw_basis 标记: 此后读取自动复权投影、拉取改取原始价;
    - 失败不标记, 可重复调用续跑 (已换算分区二次运行为 no-op)。
    返回 { partitions, converted_symbols, skipped_no_daily, failed, marked }。
    """
    import asyncio

    repo = request.app.state.repo
    loop = asyncio.get_event_loop()
    stats = await loop.run_in_executor(
        _long_task_executor,
        lambda: minute_adjust.migrate_minute_to_raw(
            repo.store.data_dir,
            write_lock=repo._write_lock,
        ),
    )
    if stats.get("marked"):
        from app.jobs.daily_pipeline import _refresh_single_view

        _refresh_single_view(repo, "kline_minute")
        _refresh_single_view(repo, "kline_etf_minute")
    return {"status": "ok", **stats}


@_root.router.post("/clear_minute")
async def clear_minute(request: Request):
    """清空全部分钟K数据 (仅 kline_minute, 不影响其他数据)。

    删除 data/kline_minute/ 下所有分区 parquet, 刷新视图。
    需二次确认: body { "confirm": true }。
    """
    import shutil

    body = await request.json() if request.method == "POST" else {}
    if not body.get("confirm"):
        raise HTTPException(status_code=400, detail="需传 confirm: true 以确认清空")

    repo = request.app.state.repo
    minute_dir = repo.store.data_dir / "kline_minute"

    # 统计待删除行数 (用于返回) — 从 CK 读，无 DuckDB 句柄钉住分区文件
    removed = 0
    if minute_dir.exists():
        try:
            from app.storage.runtime import get_storage

            runtime = get_storage()
            if runtime is not None:
                removed = int(runtime.store.table_stats("kline_minute").get("rows", 0))
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            pass
        # 仅删 kline_minute 目录, 绝不触碰其他目录
        shutil.rmtree(minute_dir, ignore_errors=True)

    # 刷新视图 (重建空视图)
    from app.jobs.daily_pipeline import _refresh_single_view

    _refresh_single_view(repo, "kline_minute")

    from app.api.data import invalidate_storage_cache

    invalidate_storage_cache()

    logger.info("minute K cleared: %d rows removed", removed)
    return {"status": "ok", "removed": removed}


@_root.router.post("/extend_history")
async def extend_history(request: Request):  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
    """向前扩展历史日K数据 — 独立于盘后管道。

    body: { "value": int, "unit": "day"|"month"|"year" }
    返回 job_id,可轮询 /api/pipeline/jobs 查看进度。
    """
    import asyncio
    import traceback as _tb  # noqa: F401 - F401: 导入用于类型检查/再导出

    try:
        body = await request.json()
        value = body.get("value")
        unit = body.get("unit", "month")
        if not value or value <= 0:
            raise HTTPException(status_code=400, detail="value 必须为正整数")
        if unit not in ("day", "month", "year"):
            raise HTTPException(status_code=400, detail="unit 只支持 day/month/year")

        repo = request.app.state.repo
        capset = request.app.state.capabilities

        from app.tickflow.capabilities import Cap

        if not capset.has(Cap.KLINE_DAILY_BATCH):
            raise HTTPException(status_code=403, detail="需要 Pro+ 权限 (batch K-line)")

        from app.api.data import invalidate_storage_cache
        from app.services.extend_history import run_extend_history
        from app.services.pipeline_jobs import (
            JobCancelledError,
            job_store,
            release_run_slot,
            run_with_capacity,
            try_acquire_run_slot,
        )

        job_id, is_new = job_store.create()
        if not is_new:
            return {"status": "reused", "job_id": job_id}

        async def task() -> None:
            if not try_acquire_run_slot(job_id):
                job_store.fail(job_id, "已有数据任务在运行(或上一次任务卡死未结束),请稍后再试")
                return
            loop = asyncio.get_event_loop()

            def progress(
                stage: str, pct: int, msg: str, stage_pct: int | None = None, skip_log: bool = False
            ) -> None:
                job_store.progress(job_id, stage, pct, msg, stage_pct=stage_pct, skip_log=skip_log)

            try:
                result = await loop.run_in_executor(
                    _long_task_executor,
                    run_with_capacity,
                    job_id,
                    lambda: run_extend_history(repo, capset, value, unit, on_progress=progress),
                )
                if "error" in result:
                    job_store.fail(job_id, result["error"])
                else:
                    job_store.succeed(job_id, result)
                invalidate_storage_cache()
            except JobCancelledError:
                # 已由 terminate() 标记失败, 拉取线程在分块回调处自行退出
                invalidate_storage_cache()
            except Exception as e:
                logger.exception("extend_history failed: job_id=%s", job_id)
                job_store.fail(job_id, str(e))
                invalidate_storage_cache()
            finally:
                release_run_slot(job_id)

        asyncio.create_task(task())  # noqa: RUF006 - RUF006: 后台任务引用需保持存活, 由框架管理生命周期
        return {"status": "started", "job_id": job_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("extend_history error: %s", e)  # noqa: TRY401 - TRY401: 已保留原始异常类型与消息
        raise HTTPException(status_code=500, detail=str(e)) from e


@_root.router.post("/repair_daily")
async def repair_daily(request: Request):
    """修正 / 补全日K数据 — 从指定起始日期重拉到今天。

    典型场景: 昨天没看盘 / 服务挂了,本地日K缺了若干天。
    用户选起始日期,复用盘后管道全流程重拉 [start_date ~ 今天]。

    body: { "start_date": "YYYY-MM-DD" }
    返回 job_id,可轮询 /api/pipeline/jobs 查看进度。
    """
    import asyncio
    import traceback as _tb  # noqa: F401 - F401: 导入用于类型检查/再导出
    from datetime import date as _date

    try:
        body = await request.json()
        raw = body.get("start_date")
        if not raw:
            raise HTTPException(status_code=400, detail="start_date 必填 (YYYY-MM-DD)")
        try:
            start_date = _date.fromisoformat(str(raw))
        except ValueError:
            raise HTTPException(status_code=400, detail="start_date 格式错误 (应为 YYYY-MM-DD)")  # noqa: B904 - B904: 此处无需异常链, 上游已有上下文

        if start_date > _root.cn_today():
            raise HTTPException(status_code=400, detail="起始日期不能晚于今天")

        repo = request.app.state.repo
        capset = request.app.state.capabilities

        from app.tickflow.capabilities import Cap

        if not capset.has(Cap.KLINE_DAILY_BATCH):
            raise HTTPException(status_code=403, detail="需要 Pro+ 权限 (batch K-line)")

        from app.services.pipeline_jobs import job_store

        job_id, is_new = job_store.create()
        if not is_new:
            return {"status": "reused", "job_id": job_id}

        asyncio.create_task(  # noqa: RUF006 - 后台任务引用需保持存活, 由框架管理生命周期
            _run_repair_daily_job(request, repo, capset, start_date, job_id)
        )
        return {"status": "started", "job_id": job_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("repair_daily error: %s", e)  # noqa: TRY401 - TRY401: 已保留原始异常类型与消息
        raise HTTPException(status_code=500, detail=str(e)) from e


@_root.router.post("/rebuild_enriched")
async def rebuild_enriched(request: Request):
    """全量重算 enriched 表 — 不获取任何数据,仅基于已有 kline_daily + adj_factor 重算复权+指标。

    返回 job_id,可轮询 /api/pipeline/jobs 查看进度。
    """
    import asyncio

    try:
        repo = request.app.state.repo

        from app.services.pipeline_jobs import job_store

        job_id, is_new = job_store.create()
        if not is_new:
            return {"status": "reused", "job_id": job_id}

        asyncio.create_task(  # noqa: RUF006 - 后台任务引用需保持存活, 由框架管理生命周期
            _run_rebuild_enriched_job(repo, job_id)
        )
        return {"status": "started", "job_id": job_id}
    except Exception as e:
        import traceback as _tb  # noqa: F401 - F401: 导入用于类型检查/再导出

        logger.exception("rebuild_enriched error: %s", e)  # noqa: TRY401 - TRY401: 已保留原始异常类型与消息
        raise HTTPException(status_code=500, detail=str(e)) from e
