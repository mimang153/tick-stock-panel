"""backtest API 路由分组 _routes_optimize（由 backtest.py 机械拆分，顺序不变）。"""

from __future__ import annotations

import json
from datetime import date

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.config import settings

from ._common import BACKTEST_MAX_SERVER_DAYS, _api_pkg
from ._job_runtime import _claim_job_start
from ._optimize_runtime import (
    _OPT_BT_FIELDS,
    _make_wf_job_key,
    _opt_backtest_kwargs,
    _prepare_robust_stream_context,
    _robust_optimize_stream_events,
    _validate_walkforward_objective,
    _walkforward_acquire_job,
    _walkforward_poll_events,
    _walkforward_resolve_dates,
    _walkforward_start_job,
)

router = APIRouter(prefix="/api/backtest")


@router.get("/optimize-robust/preview")
async def optimize_robust_preview(
    start: str,
    end: str,
    k_segments: int = 4,
):
    """稳健优化折预览: 运行前查看 K 段验证窗切分与告警 (不回测)。"""
    from app.backtest.robust_optimizer import split_validation_windows

    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="结束日期不能早于起始日期")
    try:
        windows = split_validation_windows(start_date, end_date, int(k_segments))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    warnings: list[str] = []
    if len(windows) < 3:
        warnings.append(f"验证段数 {len(windows)} < 3, 稳健分统计意义不足, 建议扩大区间")
    return {
        "k_segments": len(windows),
        "windows": [
            {"start": ws.isoformat(), "end": we.isoformat(), "days": (we - ws).days + 1}
            for ws, we in windows
        ],
        "warnings": warnings,
    }


@router.get("/walkforward/preview")
async def walkforward_preview(
    start: str,
    end: str,
    train_days: int = 252,
    test_days: int = 63,
    step_days: int = 63,
):
    """滚动样本外检验折预览: 运行前查看折数/窗口切分与告警 (不回测)。"""
    from app.backtest.walkforward import generate_folds

    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="结束日期不能早于起始日期")
    try:
        folds = generate_folds(
            start_date, end_date, int(train_days), int(test_days), int(step_days)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    warnings: list[str] = []
    if len(folds) < 3:
        warnings.append(f"折数 {len(folds)} < 3, 聚合统计意义不足, 建议扩大区间或减小 step")
    return {
        "n_folds": len(folds),
        "folds": [
            {
                "index": f.index,
                "train_start": f.train_start.isoformat(),
                "train_end": f.train_end.isoformat(),
                "test_start": f.test_start.isoformat(),
                "test_end": f.test_end.isoformat(),
            }
            for f in folds
        ],
        "warnings": warnings,
    }


@router.get("/optimize-robust/stream")
async def optimize_robust_stream(
    request: Request,
    strategy_id: str,
    param_grid: str,
    objective: str = "sortino",
    direction: str | None = None,
    k_segments: int = 4,
    min_trades_total: int = 30,
    stage2_top_n: int = 20,
    max_workers: int = 4,
    matrix_cache_max_mb: int = 512,
    params: str | None = None,
    overrides: str | None = None,
    symbols: str | None = None,
    start: str | None = None,
    end: str | None = None,
    matching: str = "open_t+1",
    fees_pct: float = 0.0002,
    commission_pct: float | None = None,
    stamp_tax_pct: float | None = None,
    slippage_bps: float = 5.0,
    max_positions: int = 10,
    max_exposure_pct: float = 1.0,
    initial_capital: float = 1_000_000.0,
    position_sizing: str = "equal",
    mode: str = "position",
    holding_days: int = 5,
):
    """SSE 流式稳健优化: 每组参数在 K 个互不重叠验证段上评估, 按稳健分排名。

    设计: docs/robust-optimizer-plan.md。事件类型与 /optimize/stream 同形:
      - job / progress(robust_optimizer_progress) / done / error
    """
    context = _prepare_robust_stream_context(
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
        start,
        end,
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

    return StreamingResponse(
        _robust_optimize_stream_events(*context),
        media_type="text/event-stream",
    )


def _prepare_walkforward_stream(request: Request, values: dict) -> dict:
    """准备 walk-forward 流式任务的预检、job 与事件上下文。"""
    from app.backtest.walkforward import WalkForwardConfig
    from app.backtest.worker import make_worker_task, run_worker_task

    strategy_id = values["strategy_id"]
    param_grid = values["param_grid"]
    objective = values["objective"]
    direction = values["direction"] or None
    train_days = values["train_days"]
    test_days = values["test_days"]
    step_days = values["step_days"]
    max_workers = values["max_workers"]
    matrix_cache_max_mb = values["matrix_cache_max_mb"]
    params = values["params"]
    overrides = values["overrides"]
    symbols = values["symbols"]

    _validate_walkforward_objective(objective)
    start_date, end_date = _walkforward_resolve_dates(request, values["start"], values["end"])

    bt_kwargs = _opt_backtest_kwargs(
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
    )
    job_key = _make_wf_job_key(
        strategy_id,
        symbols,
        values["start"],
        values["end"],
        param_grid,
        objective,
        direction,
        f"{train_days}/{test_days}/{step_days}",
        "|".join(f"{k}={bt_kwargs[k]}" for k in _OPT_BT_FIELDS),
        params,
        overrides,
        matrix_cache_max_mb,
    )
    wf_guard_violated = (
        settings.backtest_range_guard
        and max(int(train_days), int(test_days)) > BACKTEST_MAX_SERVER_DAYS
    )
    job = _walkforward_acquire_job(job_key, wf_guard_violated, request)
    return {
        "job": job,
        "job_key": job_key,
        "wf_guard_violated": wf_guard_violated,
        "WalkForwardConfig": WalkForwardConfig,
        "make_worker_task": make_worker_task,
        "run_worker_task": run_worker_task,
        "param_grid": param_grid,
        "strategy_id": strategy_id,
        "symbols": symbols,
        "start_date": start_date,
        "end_date": end_date,
        "objective": objective,
        "direction": direction,
        "train_days": train_days,
        "test_days": test_days,
        "step_days": step_days,
        "max_workers": max_workers,
        "params": params,
        "overrides": overrides,
        "bt_kwargs": bt_kwargs,
        "matrix_cache_max_mb": matrix_cache_max_mb,
    }


async def _walkforward_stream_events(request: Request, context: dict):
    """逐折推送 walk-forward 任务的进度与结果事件。"""
    job = context["job"]
    yield f"event: job\ndata: {json.dumps({'key': context['job_key']}, ensure_ascii=False)}\n\n"

    if context["wf_guard_violated"]:
        msg = f"单折窗口最多 {BACKTEST_MAX_SERVER_DAYS} 天 (当前 train/test 更大), 请减小训练/测试窗口或在更大内存环境运行。"
        yield f"event: error\ndata: {json.dumps({'message': msg}, ensure_ascii=False)}\n\n"
        return

    if _claim_job_start(job):
        _walkforward_start_job(
            job,
            context["WalkForwardConfig"],
            context["make_worker_task"],
            context["run_worker_task"],
            context["param_grid"],
            context["strategy_id"],
            context["symbols"],
            context["start_date"],
            context["end_date"],
            context["objective"],
            context["direction"],
            context["train_days"],
            context["test_days"],
            context["step_days"],
            context["max_workers"],
            context["params"],
            context["overrides"],
            context["bt_kwargs"],
            context["matrix_cache_max_mb"],
        )

    async for payload in _walkforward_poll_events(request, job):
        yield payload


@router.get("/walkforward/stream")
async def walkforward_stream(
    request: Request,
    strategy_id: str,
    param_grid: str,
    objective: str = "sortino",
    direction: str | None = None,
    train_days: int = 252,
    test_days: int = 63,
    step_days: int = 63,
    max_workers: int = 4,
    matrix_cache_max_mb: int = 512,
    params: str | None = None,  # JSON: 未扫描参数固定为用户当前值 (base_params)
    overrides: str | None = None,  # JSON: 策略当前的 basic_filter/signals/风控等覆盖
    symbols: str | None = None,
    start: str | None = None,
    end: str | None = None,
    matching: str = "open_t+1",
    fees_pct: float = 0.0002,
    commission_pct: float | None = None,
    stamp_tax_pct: float | None = None,
    slippage_bps: float = 5.0,
    max_positions: int = 10,
    max_exposure_pct: float = 1.0,
    initial_capital: float = 1_000_000.0,
    position_sizing: str = "equal",
    mode: str = "position",
    holding_days: int = 5,
):
    """SSE 流式 walk-forward: 每折训练区间网格优化 -> 测试区间 OOS 回测。

    事件: job {key} / progress {type:walkforward_progress,done,total,fold} / done {result} / error {message}
    """
    context = _prepare_walkforward_stream(request, locals())
    return StreamingResponse(
        _walkforward_stream_events(request, context), media_type="text/event-stream"
    )


@router.post("/optimize-robust/cancel")
async def optimize_robust_cancel(request: Request):
    """取消稳健优化任务 — 与 /optimize/cancel 同机制 (回吐 job_key 查表)。"""
    body = await request.json()
    job_key = body.get("job_key", "")
    job = _api_pkg._running_jobs.get(job_key)
    if job and not job.done:
        job.cancel_event.set()
        return {"ok": True}
    return {"ok": False, "message": "任务不存在或已完成"}


@router.post("/walkforward/cancel")
async def walkforward_cancel(request: Request):
    """取消 walk-forward 任务 — 传 stream 首事件回吐的 job_key。"""
    body = await request.json()
    job_key = body.get("job_key", "")
    job = _api_pkg._running_jobs.get(job_key)
    if job and not job.done:
        job.cancel_event.set()
        return {"ok": True}
    return {"ok": False, "message": "任务不存在或已完成"}
