"""筛选器批量执行：渐进式 run_all 与缓存写入辅助。"""

from __future__ import annotations

import contextlib
import copy
import time
from dataclasses import asdict, dataclass, replace
from typing import Any

from fastapi import HTTPException, Request

from app.api._root_ref import bind as _bind
from app.services import screener_work, strategy_cache, strategy_run_queue
from app.services.screener import ScreenerService
from app.services.shared_work import WorkBusyError
from app.strategy import config as strategy_config

from ._screener_ext import _results_with_ext, _safe

_root = _bind("app.api.screener")
logger = _root.logger


def _coverage_warnings(
    svc,
    as_of,
    *,
    engine=None,
    strategy_ids=None,
    params_map=None,
    overrides_map=None,
) -> list[str]:
    """数据充足性提示 (#303): enriched 覆盖低于暖机需求时给出人话警告。

    advisory 元数据: 任何计算失败都静默返回 [], 绝不影响策略主流程。
    引擎/服务无该方法时 (测试 Fake) 同样跳过, 与 build_shared_matrix 的
    可选能力探测同风格。
    """
    try:
        required: int | None = None
        rhb = getattr(engine, "required_history_bars", None)
        if engine is not None and strategy_ids and callable(rhb):
            required = rhb(strategy_ids, params_map=params_map, overrides_map=overrides_map)
        cw = getattr(svc, "coverage_warnings", None)
        if callable(cw):
            return cw(as_of, required_bars=required)
    except Exception:  # noqa: BLE001 - 覆盖告警不可用时按无告警继续
        logger.debug("coverage warnings unavailable", exc_info=True)
    return []


def _update_cache_strategy(
    data_dir, as_of: str, strategy_id: str, safe_data: dict, *, validate=None
) -> None:
    """单跑后更新缓存中该策略的结果，保持缓存与最新计算一致。"""
    from app.services import strategy_cache

    cached = strategy_cache.read_cache(data_dir)
    if cached and cached.get("as_of") == as_of:
        # read_cache 返回进程内共享镜像, 先拷贝再改 (write_cache 的 read-merge-write
        # 以这份拷贝为准, 不污染镜像)
        results = dict(cached.get("results") or {})
        results[strategy_id] = {
            "total": safe_data.get("total", 0),
            "as_of": as_of,
            "rows": safe_data.get("rows", []),
        }
        if safe_data.get("warnings"):
            # 数据不足提示 (#303) 随缓存下发 (get_cached 原样读出),
            # 单跑刷新不得冲掉 run_all 写入的提示
            results[strategy_id]["warnings"] = safe_data["warnings"]
        # write_cache already merges by strategy under its lock; do not replay other
        # strategies read before a concurrent writer published a newer result.
        try:
            strategy_cache.write_cache(
                data_dir,
                as_of,
                {strategy_id: results[strategy_id]},
                validate=validate,
                strict=validate is not None,
            )
        except OSError as exc:
            raise WorkBusyError("策略结果暂时无法保存，请稍后重试") from exc


def _attach_preset_warnings_and_cache(
    svc, engine, repo, req, as_of, params, overrides, identity, data_dir, safe_data
) -> None:
    """日线预设结果附加数据不足提示并写盘后缓存。"""
    warnings = _coverage_warnings(
        svc,
        as_of,
        engine=engine,
        strategy_ids=[req.strategy_id],
        params_map={req.strategy_id: params},
        overrides_map={req.strategy_id: overrides or {}},
    )
    if warnings:
        safe_data["warnings"] = warnings
    screener_work.assert_current(repo, engine, req.asset_type, identity)
    _root._update_cache_strategy(
        data_dir,
        str(as_of),
        req.strategy_id,
        safe_data,
        validate=lambda: screener_work.assert_current(repo, engine, req.asset_type, identity),
    )


def _prepare_progressive_context(
    repo,
    engine,
    svc,
    as_of,
    ordered_ids,
    asset_type,
    timeframe,
    params_map,
    overrides_map,
    identity,
):
    """构建渐进式 run_all 共享上下文与可用共享矩阵。"""
    screener_work.assert_current(repo, engine, asset_type, identity)
    context = svc.build_strategy_context(
        engine,
        as_of,
        ordered_ids,
        timeframe=timeframe,
        params_map=params_map,
        overrides_map=overrides_map,
    )
    if getattr(context, "market", None) is None:
        build_matrix = getattr(engine, "build_shared_matrix", None)
        if callable(build_matrix):
            matrix = build_matrix(
                context,
                [(sid, engine.get(sid)) for sid in ordered_ids],
                params_map,
                overrides_map,
            )
            if matrix is not None:
                context = replace(context, market=matrix)
    return context


def _finalize_progressive_cache(
    repo, engine, asset_type, identity, data_dir, as_of, all_results, timeframe
) -> None:
    """日线渐进结果的整体重写；写缓存失败按旧语义静默忽略。"""
    if all_results and timeframe == "1d":
        screener_work.assert_current(repo, engine, asset_type, identity)
        with contextlib.suppress(Exception):
            strategy_cache.write_cache(
                data_dir,
                str(as_of),
                all_results,
                validate=lambda: screener_work.assert_current(repo, engine, asset_type, identity),
            )


def _write_one_progressive_cache(
    sid, payload, all_results, handle, *, repo, engine, asset_type, identity, data_dir, as_of
) -> bool:
    """单策略增量写盘；失败时从结果集移除并标记失败。"""
    try:
        strategy_cache.write_cache(
            data_dir,
            str(as_of),
            {sid: payload},
            validate=lambda: screener_work.assert_current(repo, engine, asset_type, identity),
            strict=True,
        )
        return True
    except WorkBusyError:
        raise
    except Exception:  # noqa: BLE001 - 单策略缓存发布失败不得影响其他策略
        logger.warning("run_all 渐进写入缓存失败: %s", sid, exc_info=True)
        all_results.pop(sid, None)
        handle.fail_one(sid, "策略结果暂时无法保存，请稍后重试")
        return False


def _run_one_progressive_strategy(
    sid,
    context,
    handle,
    all_results,
    elapsed_map,
    *,
    repo,
    engine,
    svc,
    as_of,
    asset_type,
    timeframe,
    params_map,
    overrides_map,
    identity,
    data_dir,
    ensure,
    version,
) -> None:
    """单策略隔离执行与增量落盘；结果/耗时写回传入容器。"""
    t0 = time.perf_counter()

    def compute():
        single = engine.run_all(
            context,
            params_map=params_map,
            overrides_map=overrides_map,
            strategy_ids=[sid],
            parallel=False,
        )
        result = single[sid]
        return {
            "total": result.total,
            "as_of": str(as_of),
            "rows": _safe(asdict(result)).get("rows", []),
            "computed_at": int(time.time() * 1000),
        }

    try:
        payload = copy.deepcopy(
            screener_work.batch_results.run(
                (identity, asset_type, timeframe, str(as_of), sid, version),
                compute,
                ttl_s=6 if ensure else 0,
            )
        )
    except Exception as e:  # noqa: BLE001 - 单策略执行失败需隔离，不中断 run_all
        logger.warning("run_all: 策略 %s 执行失败, 跳过: %s", sid, e, exc_info=True)
        handle.fail_one(sid, str(e))
        return
    screener_work.assert_current(repo, engine, asset_type, identity)
    payload["available_at"] = int(time.time() * 1000)
    if timeframe == "1d":
        warnings = _coverage_warnings(
            svc,
            as_of,
            engine=engine,
            strategy_ids=[sid],
            params_map=params_map,
            overrides_map=overrides_map,
        )
        if warnings:
            payload["warnings"] = warnings
    all_results[sid] = payload
    elapsed_map[sid] = (time.perf_counter() - t0) * 1000
    if timeframe == "1d" and not _write_one_progressive_cache(
        sid,
        payload,
        all_results,
        handle,
        repo=repo,
        engine=engine,
        asset_type=asset_type,
        identity=identity,
        data_dir=data_dir,
        as_of=as_of,
    ):
        return
    handle.complete(sid, {k: v for k, v in payload.items() if k != "rows"})


def _progressive_job(
    handle,
    *,
    repo,
    engine,
    svc,
    as_of,
    asset_type,
    timeframe,
    ordered_ids,
    params_map,
    overrides_map,
    identity,
    ensure,
    version,
    data_dir,
) -> None:
    """渐进式后台执行：准备上下文、逐策略运行、收尾写缓存与耗时。"""
    context = _prepare_progressive_context(
        repo,
        engine,
        svc,
        as_of,
        ordered_ids,
        asset_type,
        timeframe,
        params_map,
        overrides_map,
        identity,
    )
    all_results: dict[str, dict] = {}
    elapsed_map: dict[str, float] = {}
    for sid in ordered_ids:
        _run_one_progressive_strategy(
            sid,
            context,
            handle,
            all_results,
            elapsed_map,
            repo=repo,
            engine=engine,
            svc=svc,
            as_of=as_of,
            asset_type=asset_type,
            timeframe=timeframe,
            params_map=params_map,
            overrides_map=overrides_map,
            identity=identity,
            data_dir=data_dir,
            ensure=ensure,
            version=version,
        )
    _finalize_progressive_cache(
        repo, engine, asset_type, identity, data_dir, as_of, all_results, timeframe
    )
    strategy_run_queue.record_run_timings(data_dir, elapsed_map)


def _submit_progressive_job(key, ordered_ids, job, *, ensure, version, on_submit):
    """提交渐进式任务并保持复用窗口/版本校验参数不变。"""
    return strategy_run_queue.MANAGER.get_or_submit(
        key,
        ordered_ids,
        job,
        reuse_completed_ms=6000 if ensure else 0,
        source_version=version,
        on_submit=on_submit,
    )


def _run_all_progressive(
    *,
    repo,
    engine,
    svc: ScreenerService,
    as_of,
    asset_type: str,
    timeframe: str,
    all_ids: list[str],
    params_map: dict,
    overrides_map: dict,
    first_return_s: float,
    t_total: float,
    ensure: bool = False,
    on_submit=None,
) -> dict:
    """run_all 渐进式执行: 快策略随响应先返回, 慢策略后台算完逐个落缓存。

    执行全程在单飞执行器里 (见 services/strategy_run_queue.py): 相同请求
    搭车现有执行, 不同请求排队; HTTP 侧只轮询状态快照到首返时限。
    """
    data_dir = repo.store.data_dir
    identity = screener_work.identity_for_inputs(repo, engine, asset_type, overrides_map)
    version = screener_work.source_version(repo, asset_type, timeframe, as_of)
    key = (asset_type, timeframe, str(as_of), tuple(sorted(all_ids)), identity)
    ordered_ids = strategy_run_queue.order_strategy_ids(
        all_ids, strategy_run_queue.load_run_timings(data_dir)
    )

    def job(handle: strategy_run_queue.StrategyRunHandle) -> None:
        _progressive_job(
            handle,
            repo=repo,
            engine=engine,
            svc=svc,
            as_of=as_of,
            asset_type=asset_type,
            timeframe=timeframe,
            ordered_ids=ordered_ids,
            params_map=params_map,
            overrides_map=overrides_map,
            identity=identity,
            ensure=ensure,
            version=version,
            data_dir=data_dir,
        )

    handle = _submit_progressive_job(
        key, ordered_ids, job, ensure=ensure, version=version, on_submit=on_submit
    )
    deadline = time.perf_counter() + (min(first_return_s, 0.25) if ensure else first_return_s)
    snap = handle.snapshot()
    while not snap["done"] and time.perf_counter() < deadline:
        time.sleep(0.2)
        snap = handle.snapshot()

    done_results = snap["results"]
    if snap["error"] and not done_results:
        raise HTTPException(
            status_code=snap["error_status"],
            detail=snap["error"],
            headers={"Retry-After": "3"} if snap["error_status"] == 503 else None,
        )
    logger.info(
        "run_all: first return %.1fms (%d done, %d pending)",
        (time.perf_counter() - t_total) * 1000,
        len(done_results),
        len(snap["pending"]),
    )
    return {
        "as_of": str(as_of),
        "results": done_results,
        "pending": snap["pending"],
        "errors": snap["errors"],
        "complete": snap["done"] and not snap["error"],
        "error": snap["error"],
        "started_at": snap["started_at_ms"],
        "task_id": handle.id,
    }


@dataclass
class _RunAllContext:
    request: Request
    repo: Any
    engine: Any
    svc: Any
    asset_type: str
    timeframe: str
    as_of: Any
    all_ids: list[str]
    params_map: dict
    overrides_map: dict
    data_dir: Any
    t_total: float
    identity: Any = None


def _run_all_ctx(ns: dict) -> _RunAllContext:
    return _RunAllContext(
        ns["request"],
        ns["repo"],
        ns["engine"],
        ns["svc"],
        ns["asset_type"],
        ns["timeframe"],
        ns["as_of"],
        ns["all_ids"],
        ns["params_map"],
        ns["overrides_map"],
        ns["data_dir"],
        ns["t_total"],
    )


def _run_all_as_of(raw_date, svc, date_type):
    if raw_date:
        # 与 /custom、/preset 的 `as_of: date` 同口径: 只收 ISO 日期字符串。
        # 非字符串原样透传会让 str(as_of) 把 "20260904" 之类写进 strategy_cache.json,
        # 与其它入口写的 "2026-09-04" 不是同一格式, 后续按 as_of 比对缓存永远失配。
        if not isinstance(raw_date, str):
            raise HTTPException(status_code=400, detail="as_of 必须是 YYYY-MM-DD 日期字符串")
        try:
            return date_type.fromisoformat(raw_date)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    return svc.latest_date()  # type: ignore[assignment]


def _run_all_resolve_ids(engine, requested_ids, asset_type, timeframe):
    skipped_unknown: list[str] = []
    if requested_ids and isinstance(requested_ids, list):
        all_ids = list(dict.fromkeys(str(sid) for sid in requested_ids))
        # 宽容降级: 策略池可能残留已不存在的策略 ID (如内置策略移除后的
        # 旧数据目录) —— 跳过未知项继续跑, 响应带回 skipped_unknown 供前端
        # 清池; 全部未知才整体 404 (避免空跑被当成成功)。
        skipped_unknown = [
            sid
            for sid in all_ids
            if not engine.has(sid) or engine.get(sid).meta.get("research_only")
        ]
        if skipped_unknown:
            if len(skipped_unknown) == len(all_ids):
                raise HTTPException(
                    status_code=404, detail=f"unknown strategies: {skipped_unknown}"
                )
            known = [sid for sid in all_ids if sid not in set(skipped_unknown)]
            logger.warning(
                "run_all: 跳过 %d 个不存在/不可用的策略 ID: %s",
                len(skipped_unknown),
                skipped_unknown,
            )
            all_ids = known
    else:
        all_ids = [
            meta["id"]
            for meta in engine.list_strategies()
            if not meta.get("research_only")
            and asset_type in meta.get("asset_types", ["stock"])
            and timeframe in meta.get("timeframes", ["1d"])
        ]
    return all_ids, skipped_unknown


def _run_all_overrides(data_dir, all_ids):
    # 批量预加载所有 override 配置
    t0 = time.perf_counter()
    all_overrides = strategy_config.list_overrides(data_dir)
    logger.info(
        "run_all: list_overrides took %.1fms (%d overrides)",
        (time.perf_counter() - t0) * 1000,
        len(all_overrides),
    )
    params_map = {sid: dict((all_overrides.get(sid) or {}).get("params") or {}) for sid in all_ids}
    overrides_map = {sid: all_overrides.get(sid, {}) for sid in all_ids}
    return params_map, overrides_map


def _run_all_progressive_response(ctx, body, first_return_s, skipped_unknown):
    # 渐进式返回 (页面首屏路径): 按历史耗时升序执行, 首返时限内算完的随响应
    # 返回, 慢策略转后台继续算并逐个写入策略缓存, 前端轮询 cached-summary 点亮。
    # 日线与分钟都启用: 分钟策略结果不落盘, 后台算完经 handle 快照合入
    # cached-summary (见 get_cached_summary) — 请求不再整段占用线程等计算。
    progressive = _run_all_progressive(
        repo=ctx.repo,
        engine=ctx.engine,
        svc=ctx.svc,
        as_of=ctx.as_of,
        asset_type=ctx.asset_type,
        timeframe=ctx.timeframe,
        all_ids=ctx.all_ids,
        params_map=ctx.params_map,
        overrides_map=ctx.overrides_map,
        first_return_s=first_return_s,
        t_total=ctx.t_total,
        ensure=body.get("ensure") is True,
        on_submit=lambda: _root._admit_submission(ctx.request),
    )
    if skipped_unknown:
        progressive["skipped_unknown"] = skipped_unknown
    return progressive


def _run_all_results(engine_results, svc, as_of, engine, params_map, overrides_map, timeframe):
    results: dict[str, dict] = {}
    for sid, result in engine_results.items():
        safe_rows = _safe(asdict(result)).get("rows", [])
        results[sid] = {"total": result.total, "as_of": str(as_of), "rows": safe_rows}
        if timeframe == "1d":
            w = _coverage_warnings(
                svc,
                as_of,
                engine=engine,
                strategy_ids=[sid],
                params_map=params_map,
                overrides_map=overrides_map,
            )
            if w:
                results[sid]["warnings"] = w
    return results


def _run_all_write_cache(
    data_dir, as_of, results, repo, engine, asset_type, identity, summary_only
):
    try:
        strategy_cache.write_cache(
            data_dir,
            str(as_of),
            results,
            validate=lambda: screener_work.assert_current(repo, engine, asset_type, identity),
            strict=True,
        )
    except WorkBusyError:
        raise
    except Exception as exc:  # Legacy full-detail callers still receive computed rows.
        if summary_only:
            raise WorkBusyError("策略结果暂时无法保存，请稍后重试") from exc
        logger.warning("策略结果缓存写入失败", exc_info=True)


def _run_all_final_response(results, as_of, body, skipped_unknown, repo):
    if body.get("summary_only"):
        return {
            "as_of": str(as_of),
            "results": {
                sid: {"total": result["total"], "as_of": result["as_of"]}
                for sid, result in results.items()
            },
            "skipped_unknown": skipped_unknown,
        }
    # ext 投影取本次结果日分区 (与过滤同口径)
    ext_values = _root._load_ext_value_maps(repo, body.get("ext_columns"), as_of=str(as_of))
    return {
        "as_of": str(as_of),
        "results": _results_with_ext(results, ext_values),
        "skipped_unknown": skipped_unknown,
    }


def _run_all_finish(ctx, engine_results, body, skipped_unknown):
    results = _run_all_results(
        engine_results,
        ctx.svc,
        ctx.as_of,
        ctx.engine,
        ctx.params_map,
        ctx.overrides_map,
        ctx.timeframe,
    )
    elapsed = (time.perf_counter() - ctx.t_total) * 1000
    logger.info("run_all: total took %.1fms (%d strategies)", elapsed, len(ctx.all_ids))
    if results and ctx.timeframe == "1d":
        _run_all_write_cache(
            ctx.data_dir,
            ctx.as_of,
            results,
            ctx.repo,
            ctx.engine,
            ctx.asset_type,
            ctx.identity,
            body.get("summary_only") is True,
        )
    return _run_all_final_response(results, ctx.as_of, body, skipped_unknown, ctx.repo)
