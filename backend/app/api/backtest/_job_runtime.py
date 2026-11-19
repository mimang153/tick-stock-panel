"""backtest API 共享定义 _job_runtime（由 backtest.py 机械拆分）。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
import weakref
from datetime import date, timedelta

from fastapi import HTTPException, Request

from app.api.auth import COOKIE_NAME
from app.config import settings
from app.services.workload import admit_request, workload

from ._common import (
    BACKTEST_MAX_SERVER_DAYS,
    BACKTEST_SERVER_GUARD_MESSAGE,
    FACTOR_DEFAULT_DAYS,
    _api_pkg,
    _guard_minute_strategy_backtest,
    _stamp_daily_data,
)


class _BacktestJob:
    """单个回测任务的状态, 存模块级供重连使用。"""

    __slots__ = (
        "__weakref__",
        "cancel_event",
        "daily_data_revisions",
        "done",
        "error",
        "expiry_timer",
        "finish_ts",
        "key",
        "progress",
        "result",
        "started",
    )

    def __init__(self, key: str):
        self.key = key
        self.cancel_event = threading.Event()
        self.progress: list[dict] = []  # 进度历史 (新连接可回放)
        self.result = None  # 完成后的结果
        self.error: str | None = None
        self.done = False
        self.started = False
        self.expiry_timer = None
        self.finish_ts: float = 0.0
        self.daily_data_revisions = _api_pkg._daily_data_revisions()


_running_jobs: dict[str, _BacktestJob] = {}


_jobs_lock = threading.Lock()


_JOB_TTL = 300  # 完成后保留 5 分钟


_MAX_ACTIVE_JOBS = 32


_MAX_FINISHED_JOBS = 16


def _drop_job(key: str) -> None:
    """Caller holds _jobs_lock; evict timers along with their cache entries."""
    job = _api_pkg._running_jobs.pop(key, None)
    if job is not None and job.expiry_timer is not None:
        job.expiry_timer.cancel()


def _cleanup_stale_jobs():
    """清理过期任务 (完成超过 TTL 的)。全程持 _jobs_lock: 迭代+pop 与其他访问互斥。"""
    now = time.time()
    revisions = _api_pkg._daily_data_revisions()
    with _jobs_lock:
        stale = [
            k
            for k, j in _api_pkg._running_jobs.items()
            if j.done and (now - j.finish_ts > _JOB_TTL or j.daily_data_revisions != revisions)
        ]
        for k in stale:
            _drop_job(k)
        completed = sorted((j.finish_ts, k) for k, j in _api_pkg._running_jobs.items() if j.done)
        for _, key in completed[: -_api_pkg._MAX_FINISHED_JOBS]:
            _drop_job(key)


def _acquire_or_evict_job(
    job_key: str, *, force: bool, on_submit=None
) -> tuple[_BacktestJob, bool]:
    """获取或创建任务, 返回 (job, is_new)。

    force 强制重跑: 同 key 已完成任务 (5 分钟 TTL 内) 直接丢弃重建, 不回放旧
    结果/旧报错 — force 任务自己的结果同样进任务缓存, 不丢弃的话 TTL 内重复
    强制重跑只是重放上一次。运行中的任务仍允许订阅 (重连/取消按同 key 对齐,
    语义不变); 非 force 连接保持既有缓存复用行为。
    """
    with _jobs_lock:
        job = _api_pkg._running_jobs.get(job_key)
        if job is not None and force and job.done:
            job = None
        if job is None:
            if (
                sum(not current.done for current in _api_pkg._running_jobs.values())
                >= _api_pkg._MAX_ACTIVE_JOBS
            ):
                from app.services.shared_work import WorkBusyError

                workload.record("backtest.task", "rejected")
                raise WorkBusyError("回测与优化等待队列已满，请稍后重试")
            if on_submit is not None:
                on_submit()
            _drop_job(job_key)
            job = _BacktestJob(job_key)
            _api_pkg._running_jobs[job_key] = job
            return job, True
        workload.record("backtest.task", "cache_hit" if job.done else "shared")
        return job, False


def _claim_job_start(job: _BacktestJob) -> bool:
    """A disconnected creator must not prevent another subscriber starting the job."""
    with _jobs_lock:
        if job.done or job.started:
            return False
        job.started = True
        return True


def _discard_unstarted_job(job: _BacktestJob) -> None:
    with _jobs_lock:
        if not job.started and _api_pkg._running_jobs.get(job.key) is job:
            _api_pkg._running_jobs.pop(job.key, None)


def _start_job_thread(job: _BacktestJob, target) -> None:
    try:
        threading.Thread(target=target, daemon=True).start()
    except RuntimeError:
        _finish_job(job, error="无法启动计算任务，请稍后重试")


def _finish_job(job: _BacktestJob, *, result=None, error: str | None = None) -> None:
    """Publish the terminal state and proactively drop the reconnect entry after TTL."""
    finished_at = time.time()
    if result is not None and job.daily_data_revisions != _api_pkg._daily_data_revisions():
        result = None
        error = "日K数据已修正, 请重新运行计算"
    with _jobs_lock:
        job.result = result
        job.error = error
        job.done = True
        job.finish_ts = finished_at
    workload.record("backtest.task", "failed" if error else "completed")

    reference, key = weakref.ref(job), job.key

    def _expire() -> None:
        with _jobs_lock:
            current = _api_pkg._running_jobs.get(key)
            if (
                current is not None
                and current.done
                and current.finish_ts == finished_at
                and current is reference()
            ):
                _api_pkg._running_jobs.pop(key, None)

    timer = threading.Timer(_JOB_TTL, _expire)
    timer.daemon = True
    with _jobs_lock:
        if _api_pkg._running_jobs.get(key) is not job:
            return
        if job.expiry_timer is not None:
            job.expiry_timer.cancel()
        job.expiry_timer = timer
    timer.start()
    _cleanup_stale_jobs()


def _make_job_key(
    strategy_id: str,
    symbols: str | None,
    start: str | None,
    end: str | None,
    matching: str,
    entry_fill: str | None,
    exit_fill: str | None,
    fees_pct: float,
    slippage_bps: float,
    max_positions: int,
    max_exposure_pct: float,
    initial_capital: float,
    position_sizing: str,
    params: str | None,
    overrides: str | None,
    mode: str = "position",
    holding_days: int = 5,
    commission_pct: float | None = None,
    stamp_tax_pct: float | None = None,
    asset_type: str = "stock",
    minute_fill: bool = False,
    regime_filter: str | None = None,
    position_policy: str | None = None,
    position_policy_params: str | None = None,
    force: bool = False,
) -> str:
    raw = f"{strategy_id}|{symbols}|{start}|{end}|{matching}|{entry_fill}|{exit_fill}|{fees_pct}|{slippage_bps}|{max_positions}|{max_exposure_pct}|{initial_capital}|{position_sizing}|{params}|{overrides}|{mode}|{holding_days}|{commission_pct}|{stamp_tax_pct}|{asset_type}|{minute_fill}|{regime_filter}|{position_policy}|{position_policy_params}|force={force}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _validate_stream_json_params(values: dict) -> None:
    """校验流式回测的 JSON 类参数，避免非法 JSON 进入后台任务。"""
    for raw in (
        values["params"],
        values["overrides"],
        values["regime_filter"],
        values["position_policy_params"],
    ):
        if raw:
            try:
                json.loads(raw)
            except json.JSONDecodeError as exc:
                raise HTTPException(400, "回测参数必须是有效 JSON") from exc


def _prepare_strategy_stream_job(request: Request, values: dict):
    from app.backtest.strategy import StrategyBacktestConfig
    from app.backtest.worker import make_worker_task, run_worker_task

    context = dict(values)
    context["StrategyBacktestConfig"] = StrategyBacktestConfig
    context["make_worker_task"] = make_worker_task
    context["run_worker_task"] = run_worker_task

    try:
        end_date = date.fromisoformat(values["end"]) if values["end"] else _api_pkg.cn_today()
        start_date = date.fromisoformat(values["start"]) if values["start"] else None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {e}") from e
    if start_date is None:
        # 空 start = 全部历史: 用本地最早日K日期, 查不到再回退到默认窗口
        earliest = request.app.state.repo.earliest_daily_date()
        start_date = earliest or (end_date - timedelta(days=FACTOR_DEFAULT_DAYS))
    _guard_minute_strategy_backtest(
        request, values["strategy_id"], start_date, values["asset_type"]
    )

    # 服务端范围保护
    guard_violated = False
    if settings.backtest_range_guard:
        days = (end_date - start_date).days + 1
        if days > BACKTEST_MAX_SERVER_DAYS:
            guard_violated = True

    job_key = _make_job_key(
        values["strategy_id"],
        values["symbols"],
        values["start"],
        values["end"],
        values["matching"],
        values["entry_fill"],
        values["exit_fill"],
        values["fees_pct"],
        values["slippage_bps"],
        values["max_positions"],
        values["max_exposure_pct"],
        values["initial_capital"],
        values["position_sizing"],
        values["params"],
        values["overrides"],
        values["mode"],
        values["holding_days"],
        values["commission_pct"],
        values["stamp_tax_pct"],
        asset_type=values["asset_type"],
        minute_fill=values["minute_fill"],
        regime_filter=values["regime_filter"],
        position_policy=values["position_policy"],
        position_policy_params=values["position_policy_params"],
        force=values["force"],
    )

    _cleanup_stale_jobs()

    _validate_stream_json_params(values)
    job, _ = (
        (_BacktestJob(job_key), False)
        if guard_violated
        else _acquire_or_evict_job(
            job_key,
            force=values["force"],
            on_submit=lambda: admit_request(request, COOKIE_NAME),
        )
    )
    context.update(start_date=start_date, end_date=end_date, guard_violated=guard_violated, job=job)
    return context


def _strategy_stream_preflight_error(request: Request, context: dict) -> str | None:
    if context["guard_violated"]:
        return BACKTEST_SERVER_GUARD_MESSAGE

    if context["minute_fill"]:
        capset = request.app.state.capabilities
        from app.tickflow.capabilities import Cap

        if not capset.has(Cap.KLINE_MINUTE_BATCH):
            _discard_unstarted_job(context["job"])
            return "分钟K精确回测需要 Pro+ 权限 (kline.minute.batch)"
        repo = request.app.state.repo
        earliest_minute = (
            repo.earliest_minute_date() if hasattr(repo, "earliest_minute_date") else None
        )
        if earliest_minute is not None and context["start_date"] < earliest_minute:
            msg = (
                f"本地分钟K历史最早到 {earliest_minute}, 无法覆盖回测起始日 {context['start_date']}。"
                f"请先用「扩展分钟K历史」功能拉取更多数据, 或缩小回测区间。"
            )
            _discard_unstarted_job(context["job"])
            return msg
    return None


def _start_strategy_stream_backtest(context: dict):
    job = context["job"]
    if not _claim_job_start(job):
        return

    make_worker_task = context["make_worker_task"]
    run_worker_task = context["run_worker_task"]
    cfg = context["StrategyBacktestConfig"](
        strategy_id=context["strategy_id"],
        symbols=[s.strip() for s in context["symbols"].split(",") if s.strip()]
        if context["symbols"]
        else None,
        start=context["start_date"],
        end=context["end_date"],
        params=json.loads(context["params"]) if context["params"] else None,
        overrides=json.loads(context["overrides"]) if context["overrides"] else None,
        matching=context["matching"],
        entry_fill=context["entry_fill"],
        exit_fill=context["exit_fill"],
        fees_pct=context["fees_pct"],
        commission_pct=context["commission_pct"],
        stamp_tax_pct=context["stamp_tax_pct"],
        slippage_bps=context["slippage_bps"],
        max_positions=int(context["max_positions"]),
        max_exposure_pct=float(context["max_exposure_pct"]),
        initial_capital=float(context["initial_capital"]),
        position_sizing=context["position_sizing"],
        position_policy=context["position_policy"],
        position_policy_params=(
            json.loads(context["position_policy_params"])
            if context["position_policy_params"]
            else None
        ),
        mode=context["mode"],
        holding_days=int(context["holding_days"]),
        asset_type=context["asset_type"],
        minute_fill=context["minute_fill"],
        regime_filter=json.loads(context["regime_filter"]) if context["regime_filter"] else None,
    )

    def _run_backtest():
        from app.services.heavy_job_limiter import (
            HeavyJobCancelledError,
            shared_heavy_job_limiter,
        )

        try:
            with shared_heavy_job_limiter.slot(
                "normal",
                cancel_event=job.cancel_event,
            ):
                task = make_worker_task("backtest", settings.data_dir, cfg)
                workload.record("backtest.task", "executed")
                result = run_worker_task(
                    task,
                    lambda d: job.progress.append(d),
                    job.cancel_event,
                )
            _finish_job(job, result=result)
        except HeavyJobCancelledError:
            _finish_job(job, error="回测已取消")
        except Exception as e:  # noqa: BLE001 - 回测后台任务外部依赖不可控, 捕获后转错误事件
            _finish_job(job, error=str(e))

    _start_job_thread(job, _run_backtest)


async def _strategy_stream_events(request: Request, context: dict):
    error = _strategy_stream_preflight_error(request, context)
    if error is not None:
        yield f"event: error\ndata: {json.dumps({'message': error}, ensure_ascii=False)}\n\n"
        return

    _start_strategy_stream_backtest(context)

    job = context["job"]
    cursor = 0
    tick = 0

    while True:
        if job.done:
            if job.error:
                yield f"event: error\ndata: {json.dumps({'message': job.error}, ensure_ascii=False)}\n\n"
            elif job.result is not None:
                r = job.result
                error = r.get("error") if isinstance(r, dict) else getattr(r, "error", None)
                if error == "cancelled":
                    yield f"event: error\ndata: {json.dumps({'message': '回测已取消'}, ensure_ascii=False)}\n\n"
                elif error:
                    yield f"event: error\ndata: {json.dumps({'message': error}, ensure_ascii=False)}\n\n"
                else:
                    payload = _stamp_daily_data(r, job.daily_data_revisions)
                    yield f"event: done\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"
            return

        tick += 1
        if tick % 4 == 0 and await request.is_disconnected():
            break

        prog_list = job.progress
        while cursor < len(prog_list):
            msg = prog_list[cursor]
            cursor += 1
            yield f"event: progress\ndata: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"

        await asyncio.sleep(0.5)
