"""backtest API 共享定义 _optimize_runtime（由 backtest.py 机械拆分）。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import date, timedelta

from fastapi import HTTPException, Request

from app.api.auth import COOKIE_NAME
from app.config import settings
from app.services.workload import admit_request, workload

from ._common import (
    BACKTEST_MAX_SERVER_DAYS,
    BACKTEST_SERVER_GUARD_MESSAGE,
    FACTOR_DEFAULT_DAYS,
    STRATEGY_DEFAULT_DAYS,
    _api_pkg,
    _stamp_daily_data,
    logger,
)
from ._job_runtime import (
    _acquire_or_evict_job,
    _BacktestJob,
    _claim_job_start,
    _cleanup_stale_jobs,
    _finish_job,
    _start_job_thread,
)


def _json_safe(obj):
    """递归把 nan/inf 置 None —— json.dumps(default=str) 处理不了它们, 会输出非法 JSON
    字面量 NaN/Infinity 让前端 JSON.parse 崩。优化器/WF 结果嵌套深 (逐组/逐折的
    sortino 等零波动场景可能算出 nan), 序列化前统一清洗。"""
    import math

    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj


_OPT_BT_FIELDS = [
    "matching",
    "fees_pct",
    "commission_pct",
    "stamp_tax_pct",
    "slippage_bps",
    "max_positions",
    "max_exposure_pct",
    "initial_capital",
    "position_sizing",
    "mode",
    "holding_days",
]


def _make_opt_job_key(
    strategy_id,
    symbols,
    start,
    end,
    param_grid,
    objective,
    direction,
    bt_sig,
    params=None,
    overrides=None,
    matrix_cache_max_mb=512,
) -> str:
    """生成稳健优化 SSE 任务的缓存键。

    键覆盖策略、参数网格、日期区间与数据修订标记，避免复用过期任务。
    """
    raw = (
        f"OPT|{strategy_id}|{symbols}|{start}|{end}|{param_grid}|{objective}|"
        f"{direction}|{bt_sig}|{params}|{overrides}|cache={matrix_cache_max_mb}"
    )
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _opt_backtest_kwargs(
    matching,
    fees_pct,
    commission_pct,
    stamp_tax_pct,
    slippage_bps,
    max_positions,
    max_exposure_pct,
    initial_capital,
    position_sizing,
    mode,
    holding_days,
) -> dict:
    """把请求参数转换成回测引擎可直接消费的关键字参数。"""
    return {
        "matching": matching,
        "fees_pct": fees_pct,
        "commission_pct": commission_pct,
        "stamp_tax_pct": stamp_tax_pct,
        "slippage_bps": slippage_bps,
        "max_positions": int(max_positions),
        "max_exposure_pct": float(max_exposure_pct),
        "initial_capital": float(initial_capital),
        "position_sizing": position_sizing,
        "mode": mode,
        "holding_days": int(holding_days),
    }


def _validate_robust_stream_params(
    request: Request,
    start: str | None,
    end: str | None,
    objective: str,
    k_segments: int,
):
    """校验稳健优化流式的日期区间与参数上限。"""
    try:
        end_date = date.fromisoformat(end) if end else _api_pkg.cn_today()
        start_date = date.fromisoformat(start) if start else None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    if start_date is None:
        earliest = request.app.state.repo.earliest_daily_date()
        start_date = earliest or (end_date - timedelta(days=FACTOR_DEFAULT_DAYS))

    # 主目标白名单 (护栏): 稳健排名不容退化目标
    try:
        from app.backtest.guards import validate_primary_objective

        validate_primary_objective(objective)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    from app.backtest.robust_optimizer import split_validation_windows

    try:
        split_validation_windows(start_date, end_date, int(k_segments))  # 仅校验可切窗
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    guard_violated = False
    if (
        settings.backtest_range_guard
        and (end_date - start_date).days + 1 > BACKTEST_MAX_SERVER_DAYS
    ):
        guard_violated = True
    return start_date, end_date, guard_violated


def _acquire_robust_stream_job(
    request: Request,
    strategy_id: str,
    param_grid: str,
    objective: str,
    direction: str | None,
    k_segments: int,
    min_trades_total: int,
    stage2_top_n: int,
    matrix_cache_max_mb: int,
    params: str | None,
    overrides: str | None,
    symbols: str | None,
    start: str | None,
    end: str | None,
    matching: str,
    fees_pct: float,
    commission_pct: float | None,
    stamp_tax_pct: float | None,
    slippage_bps: float,
    max_positions: int,
    max_exposure_pct: float,
    initial_capital: float,
    position_sizing: str,
    mode: str,
    holding_days: int,
    guard_violated: bool,
):
    """为稳健优化流式请求申请或复用后台任务槽位。"""
    direction = direction or None
    bt_kwargs = _opt_backtest_kwargs(
        matching,
        fees_pct,
        commission_pct,
        stamp_tax_pct,
        slippage_bps,
        max_positions,
        max_exposure_pct,
        initial_capital,
        position_sizing,
        mode,
        holding_days,
    )
    bt_sig = "|".join(f"{k}={bt_kwargs[k]}" for k in _OPT_BT_FIELDS)
    job_key = (
        "robust:"
        + _make_opt_job_key(
            strategy_id,
            symbols,
            start,
            end,
            param_grid,
            objective,
            direction,
            bt_sig,
            params,
            overrides,
            matrix_cache_max_mb,
        )
        + f"|k={int(k_segments)}|mt={int(min_trades_total)}|s2={int(stage2_top_n)}"
    )

    _cleanup_stale_jobs()
    job, _ = (
        (_BacktestJob(job_key), False)
        if guard_violated
        else _acquire_or_evict_job(
            job_key,
            force=False,
            on_submit=lambda: admit_request(request, COOKIE_NAME),
        )
    )
    return job_key, job, bt_kwargs, direction


def _acquire_robust_stream_job_for_context(values: dict, guard_violated: bool) -> tuple:
    """按预检结果获取或复用稳健优化任务，返回任务上下文四元组。"""
    return _acquire_robust_stream_job(
        values["request"],
        values["strategy_id"],
        values["param_grid"],
        values["objective"],
        values["direction"],
        values["k_segments"],
        values["min_trades_total"],
        values["stage2_top_n"],
        values["matrix_cache_max_mb"],
        values["params"],
        values["overrides"],
        values["symbols"],
        values["start"],
        values["end"],
        values["matching"],
        values["fees_pct"],
        values["commission_pct"],
        values["stamp_tax_pct"],
        values["slippage_bps"],
        values["max_positions"],
        values["max_exposure_pct"],
        values["initial_capital"],
        values["position_sizing"],
        values["mode"],
        values["holding_days"],
        guard_violated,
    )


def _prepare_robust_stream_context(
    request: Request,
    strategy_id: str,
    param_grid: str,
    objective: str,
    direction: str | None,
    k_segments: int,
    min_trades_total: int,
    stage2_top_n: int,
    max_workers: int,
    matrix_cache_max_mb: int,
    params: str | None,
    overrides: str | None,
    symbols: str | None,
    start: str | None,
    end: str | None,
    matching: str,
    fees_pct: float,
    commission_pct: float | None,
    stamp_tax_pct: float | None,
    slippage_bps: float,
    max_positions: int,
    max_exposure_pct: float,
    initial_capital: float,
    position_sizing: str,
    mode: str,
    holding_days: int,
):
    """准备稳健优化流式所需的数据上下文与预检结果。"""
    from app.backtest.robust_optimizer import RobustOptimizeConfig
    from app.backtest.worker import make_worker_task, run_worker_task

    start_date, end_date, guard_violated = _validate_robust_stream_params(
        request, start, end, objective, k_segments
    )
    job_key, job, bt_kwargs, direction = _acquire_robust_stream_job_for_context(
        locals(), guard_violated
    )
    return (
        request,
        strategy_id,
        param_grid,
        objective,
        direction,
        k_segments,
        min_trades_total,
        stage2_top_n,
        max_workers,
        matrix_cache_max_mb,
        params,
        overrides,
        symbols,
        start_date,
        end_date,
        job_key,
        job,
        guard_violated,
        bt_kwargs,
        RobustOptimizeConfig,
        make_worker_task,
        run_worker_task,
    )


def _parse_robust_job_inputs(param_grid, params, overrides):
    """解析并规范化稳健优化任务入参。"""
    try:
        grid = json.loads(param_grid)
    except (json.JSONDecodeError, TypeError):
        grid = None
    if not isinstance(grid, dict) or not grid:
        return None, None, None

    try:
        base_params = json.loads(params) if params else {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("optimize-robust: params JSON 解析失败, 降级为空: %r", params)
        base_params = {}
    try:
        ov = json.loads(overrides) if overrides else None
    except (json.JSONDecodeError, TypeError):
        logger.warning("optimize-robust: overrides JSON 解析失败, 降级为 None: %r", overrides)
        ov = None
    return grid, base_params, ov


def _build_robust_optimize_config(
    config_cls,
    strategy_id,
    symbols,
    start_date,
    end_date,
    grid,
    objective,
    direction,
    k_segments,
    min_trades_total,
    stage2_top_n,
    max_workers,
    matrix_cache_max_mb,
    base_params,
    ov,
    bt_kwargs,
):
    """根据请求构造稳健优化器配置。"""
    return config_cls(
        strategy_id=strategy_id,
        symbols=[s.strip() for s in symbols.split(",") if s.strip()] if symbols else None,
        start=start_date,
        end=end_date,
        param_grid=grid,
        objective=objective,
        direction=direction,
        k_segments=int(k_segments),
        min_trades_total=int(min_trades_total),
        stage2_top_n=int(stage2_top_n),
        max_workers=int(max_workers),
        matrix_cache_max_mb=int(matrix_cache_max_mb),
        base_params=base_params if isinstance(base_params, dict) else {},
        overrides=ov if isinstance(ov, dict) else None,
        backtest_kwargs=bt_kwargs,
    )


def _run_robust_optimize_task(job, rcfg, make_worker_task, run_worker_task):
    """在后台线程执行稳健优化并写入任务状态。"""

    def _run_robust():
        """线程入口：捕获异常并确保任务最终完成。"""
        from app.services.heavy_job_limiter import (
            HeavyJobCancelledError,
            shared_heavy_job_limiter,
        )

        try:
            with shared_heavy_job_limiter.slot(
                "normal",
                cancel_event=job.cancel_event,
            ):
                task = make_worker_task("robust_optimize", settings.data_dir, rcfg)
                workload.record("backtest.task", "executed")
                result = run_worker_task(
                    task,
                    lambda d: job.progress.append(d),
                    job.cancel_event,
                )
            _finish_job(job, result=result)
        except HeavyJobCancelledError:
            _finish_job(job, error="优化已取消")
        except Exception as e:  # noqa: BLE001 - 优化后台任务外部依赖不可控, 捕获后转错误事件
            _finish_job(job, error=str(e))

    _start_job_thread(job, _run_robust)


def _start_robust_optimize_job(
    job,
    strategy_id,
    param_grid,
    objective,
    direction,
    k_segments,
    min_trades_total,
    stage2_top_n,
    max_workers,
    matrix_cache_max_mb,
    params,
    overrides,
    symbols,
    start_date,
    end_date,
    bt_kwargs,
    config_cls,
    make_worker_task,
    run_worker_task,
):
    """启动稳健优化后台任务并登记任务键。"""
    if not _claim_job_start(job):
        return
    grid, base_params, ov = _parse_robust_job_inputs(param_grid, params, overrides)
    if grid is None:
        _finish_job(job, error="param_grid 必须是非空的参数网格对象")
        return

    rcfg = _build_robust_optimize_config(
        config_cls,
        strategy_id,
        symbols,
        start_date,
        end_date,
        grid,
        objective,
        direction,
        k_segments,
        min_trades_total,
        stage2_top_n,
        max_workers,
        matrix_cache_max_mb,
        base_params,
        ov,
        bt_kwargs,
    )
    _run_robust_optimize_task(job, rcfg, make_worker_task, run_worker_task)


async def _iter_robust_job_events(request, job):
    """迭代稳健优化任务的 SSE 事件，附带数据修订标记。"""
    cursor = 0
    tick = 0
    try:
        while True:
            if job.done:
                if job.error:
                    yield f"event: error\ndata: {json.dumps({'message': job.error}, ensure_ascii=False)}\n\n"
                elif job.cancel_event.is_set():
                    yield f"event: error\ndata: {json.dumps({'message': '优化已取消'}, ensure_ascii=False)}\n\n"
                elif job.result is not None:
                    yield f"event: done\ndata: {json.dumps(_json_safe(_stamp_daily_data(job.result, job.daily_data_revisions)), ensure_ascii=False, default=str)}\n\n"
                return
            tick += 1
            if tick % 4 == 0 and await request.is_disconnected():
                break
            while cursor < len(job.progress):
                msg = job.progress[cursor]
                cursor += 1
                yield f"event: progress\ndata: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:  # noqa: TRY203 - 显式保留 SSE 取消传播语义
        raise


async def _robust_optimize_stream_events(
    request,
    strategy_id,
    param_grid,
    objective,
    direction,
    k_segments,
    min_trades_total,
    stage2_top_n,
    max_workers,
    matrix_cache_max_mb,
    params,
    overrides,
    symbols,
    start_date,
    end_date,
    job_key,
    job,
    guard_violated,
    bt_kwargs,
    config_cls,
    make_worker_task,
    run_worker_task,
):
    """生成稳健优化流式事件序列，直至完成或取消。"""
    yield f"event: job\ndata: {json.dumps({'key': job_key}, ensure_ascii=False)}\n\n"

    if guard_violated:
        yield f"event: error\ndata: {json.dumps({'message': BACKTEST_SERVER_GUARD_MESSAGE}, ensure_ascii=False)}\n\n"
        return

    _start_robust_optimize_job(
        job,
        strategy_id,
        param_grid,
        objective,
        direction,
        k_segments,
        min_trades_total,
        stage2_top_n,
        max_workers,
        matrix_cache_max_mb,
        params,
        overrides,
        symbols,
        start_date,
        end_date,
        bt_kwargs,
        config_cls,
        make_worker_task,
        run_worker_task,
    )
    async for event in _iter_robust_job_events(request, job):
        yield event


def _make_wf_job_key(
    strategy_id,
    symbols,
    start,
    end,
    param_grid,
    objective,
    direction,
    windows,
    bt_sig,
    params=None,
    overrides=None,
    matrix_cache_max_mb=512,
) -> str:
    """生成步进优化（walk-forward）任务键。"""
    raw = (
        f"WF|{strategy_id}|{symbols}|{start}|{end}|{param_grid}|{objective}|"
        f"{direction}|{windows}|{bt_sig}|{params}|{overrides}|cache={matrix_cache_max_mb}"
    )
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _validate_walkforward_objective(objective: str) -> None:
    """校验步进优化目标函数是否受支持。"""
    from app.backtest.guards import validate_primary_objective

    try:
        validate_primary_objective(objective)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _walkforward_resolve_dates(request: Request, start: str | None, end: str | None):
    """解析步进优化的起止日期与默认窗口。"""
    try:
        end_date = date.fromisoformat(end) if end else _api_pkg.cn_today()
        start_date = date.fromisoformat(start) if start else None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    if start_date is None:
        earliest = request.app.state.repo.earliest_daily_date()
        start_date = earliest or (end_date - timedelta(days=STRATEGY_DEFAULT_DAYS))
    return start_date, end_date


def _walkforward_acquire_job(job_key, wf_guard_violated, request):
    """为步进优化申请后台任务槽位。"""
    _cleanup_stale_jobs()
    if wf_guard_violated:
        return _BacktestJob(job_key)
    job, _ = _acquire_or_evict_job(
        job_key,
        force=False,
        on_submit=lambda: admit_request(request, COOKIE_NAME),
    )
    return job


def _walkforward_parse_grid(job, param_grid):
    """解析步进优化参数网格。"""
    try:
        grid = json.loads(param_grid)
    except (json.JSONDecodeError, TypeError):
        grid = None
    if not isinstance(grid, dict) or not grid:
        job.error = "param_grid 必须是非空的参数网格对象"
        job.done = True
        job.finish_ts = time.time()
        return None
    return grid


def _walkforward_build_config(
    config_cls,
    job,
    param_grid,
    params,
    overrides,
    strategy_id,
    symbols,
    start_date,
    end_date,
    objective,
    direction,
    train_days,
    test_days,
    step_days,
    max_workers,
    bt_kwargs,
    matrix_cache_max_mb,
):
    """构造步进优化配置。"""
    grid = _walkforward_parse_grid(job, param_grid)
    if grid is None:
        return None
    try:
        base_params = json.loads(params) if params else {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("walkforward: params JSON 解析失败, 降级为空 params: %r", params)
        base_params = {}
    try:
        ov = json.loads(overrides) if overrides else None
    except (json.JSONDecodeError, TypeError):
        logger.warning("walkforward: overrides JSON 解析失败, 降级为 None: %r", overrides)
        ov = None
    return config_cls(
        strategy_id=strategy_id,
        symbols=[s.strip() for s in symbols.split(",") if s.strip()] if symbols else None,
        start=start_date,
        end=end_date,
        param_grid=grid,
        objective=objective,
        direction=direction,
        train_days=int(train_days),
        test_days=int(test_days),
        step_days=int(step_days),
        max_workers=int(max_workers),
        base_params=base_params if isinstance(base_params, dict) else {},
        overrides=ov if isinstance(ov, dict) else None,
        backtest_kwargs=bt_kwargs,
        matrix_cache_max_mb=int(matrix_cache_max_mb),
    )


def _walkforward_launch_worker(job, wf_cfg, make_worker_task, run_worker_task):
    """启动步进优化 worker 线程。"""

    def _run_wf():
        """线程入口：执行步进优化并落盘结果。"""
        from app.services.heavy_job_limiter import (
            HeavyJobCancelledError,
            shared_heavy_job_limiter,
        )

        try:
            with shared_heavy_job_limiter.slot(
                "normal",
                cancel_event=job.cancel_event,
            ):
                task = make_worker_task("walkforward", settings.data_dir, wf_cfg)
                workload.record("backtest.task", "executed")
                result = run_worker_task(
                    task,
                    lambda d: job.progress.append(d),
                    job.cancel_event,
                )
            _finish_job(job, result=result)
        except HeavyJobCancelledError:
            _finish_job(job, error="walk-forward 已取消")
        except Exception as e:  # noqa: BLE001 - 优化后台任务外部依赖不可控, 捕获后转错误事件
            _finish_job(job, error=str(e))

    _start_job_thread(job, _run_wf)


def _walkforward_start_job(
    job,
    config_cls,
    make_worker_task,
    run_worker_task,
    param_grid,
    strategy_id,
    symbols,
    start_date,
    end_date,
    objective,
    direction,
    train_days,
    test_days,
    step_days,
    max_workers,
    params,
    overrides,
    bt_kwargs,
    matrix_cache_max_mb,
):
    """完成步进优化任务启动登记。"""
    wf_cfg = _walkforward_build_config(
        config_cls,
        job,
        param_grid,
        params,
        overrides,
        strategy_id,
        symbols,
        start_date,
        end_date,
        objective,
        direction,
        train_days,
        test_days,
        step_days,
        max_workers,
        bt_kwargs,
        matrix_cache_max_mb,
    )
    if wf_cfg is None:
        return
    _walkforward_launch_worker(job, wf_cfg, make_worker_task, run_worker_task)


async def _walkforward_poll_events(request, job):
    """轮询步进优化任务事件并产出 SSE 帧。"""
    cursor = 0
    tick = 0
    while True:
        if job.done:
            if job.error:
                yield f"event: error\ndata: {json.dumps({'message': job.error}, ensure_ascii=False)}\n\n"
            elif job.cancel_event.is_set():
                yield f"event: error\ndata: {json.dumps({'message': 'walk-forward 已取消'}, ensure_ascii=False)}\n\n"
            elif job.result is not None:
                yield f"event: done\ndata: {json.dumps(_json_safe(_stamp_daily_data(job.result, job.daily_data_revisions)), ensure_ascii=False, default=str)}\n\n"
            return
        tick += 1
        if tick % 4 == 0 and await request.is_disconnected():
            break
        while cursor < len(job.progress):
            msg = job.progress[cursor]
            cursor += 1
            yield f"event: progress\ndata: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"
        await asyncio.sleep(0.5)
