"""backtest API 路由分组 _routes_core（由 backtest.py 机械拆分，顺序不变）。"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.api.auth import COOKIE_NAME
from app.config import settings
from app.enriched_generation import EnrichedGenerationUnavailableError
from app.services.backtest import BacktestConfig, VectorbtUnavailable
from app.services.daily_health import DailyIntegrityError
from app.services.workload import admit_request, workload

from ._common import (
    FACTOR_DEFAULT_DAYS,
    FACTOR_MAX_SYMBOLS,
    BacktestRequest,
    CandidateCreateRequest,
    CandidateUpdateRequest,
    FactorBacktestRequest,
    FactorBatchRequest,
    StrategyBacktestRequest,
    _api_pkg,
    _candidate_store,
    _guard_minute_strategy_backtest,
    _raise_candidate_error,
    _resolve_start,
    _stamp_daily_data,
)
from ._job_runtime import (
    _jobs_lock,
    _make_job_key,
    _prepare_strategy_stream_job,
    _strategy_stream_events,
)

router = APIRouter(prefix="/api/backtest")


@router.get("/status")
def status():
    """前端可用此接口判断回测页是否要灰显。"""
    return {"available": True}


@router.post("/run")
def run(req: BacktestRequest, request: Request):
    """信号回测 — 现有接口，向后兼容。"""
    repo = request.app.state.repo
    svc = _api_pkg.BacktestService(repo)
    end = req.end or _api_pkg.cn_today()
    start = req.start or (end - timedelta(days=365 * 3))

    cfg = BacktestConfig(
        symbols=req.symbols,
        start=start,
        end=end,
        entries=req.entries,
        exits=req.exits,
        stop_loss_pct=req.stop_loss_pct,
        max_hold_days=req.max_hold_days,
        fees_pct=req.fees_pct,
        slippage_bps=req.slippage_bps,
        matching=req.matching,
        asset_type=req.asset_type,
    )
    try:
        admit_request(request, COOKIE_NAME)
        result = svc.run(cfg)
    except VectorbtUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    return _stamp_daily_data(result)


@router.get("/factor/columns")
def factor_columns(request: Request):
    """返回可用的因子列列表 (含运行期注册的自定义/复合因子)。"""
    from app.factors import store
    from app.factors.registry import factor_columns_view

    archived = {
        item["id"]: item.get("archived") is True
        for item in store.load_all(request.app.state.repo.store.data_dir)
    }
    return {
        "columns": [
            {**item, "archived": archived.get(item["id"], False)} for item in factor_columns_view()
        ]
    }


@router.post("/factor/run")
def factor_run(req: FactorBacktestRequest, request: Request):
    """因子回测 — IC/IR 分析 + 分层回测。"""
    from app.backtest.factor import FactorBacktestService, FactorConfig
    from app.factors.registry import missing_factor_ids

    invalid = missing_factor_ids([req.factor_name])
    if invalid:
        raise HTTPException(status_code=400, detail=f"不支持的因子: {', '.join(invalid)}")

    engine = _api_pkg._get_engine(request)
    svc = FactorBacktestService(engine)

    end = req.end or _api_pkg.cn_today()
    start = _resolve_start(req, end, FACTOR_DEFAULT_DAYS)
    _api_pkg._guard_server_backtest_range(start, end)
    symbols = req.symbols if req.symbols else None
    if symbols is not None and len(symbols) > FACTOR_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"指定标的最多支持 {FACTOR_MAX_SYMBOLS} 只，请缩小标的范围。",
        )

    cfg = FactorConfig(
        factor_name=req.factor_name,
        symbols=symbols,
        start=start,
        end=end,
        n_groups=req.n_groups,
        rebalance=req.rebalance,
        weight=req.weight,
        fees_pct=req.fees_pct,
        slippage_bps=req.slippage_bps,
        asset_type=req.asset_type,
    )
    try:
        admit_request(request, COOKIE_NAME)
        result = svc.run(cfg)
    except DailyIntegrityError:
        raise
    except EnrichedGenerationUnavailableError as exc:
        # 盘中实时刷新换新 generation, 重试后仍撞上发布 (api/mining.py 同款映射)
        raise HTTPException(
            status_code=400,
            detail="行情数据正在更新(盘中实时刷新), 请稍后重试",
        ) from exc
    return _stamp_daily_data(result)


@router.post("/factor/batch")
def factor_batch(req: FactorBatchRequest, request: Request):
    """批量筛选因子, 同一批次只加载并计算一次数据面板。"""
    from app.backtest.factor import (
        FactorBacktestService,
        FactorBatchConfig,
    )
    from app.factors.registry import missing_factor_ids

    factor_names = list(dict.fromkeys(req.factor_names))
    invalid = missing_factor_ids(factor_names)
    if invalid:
        raise HTTPException(status_code=400, detail=f"不支持的因子: {', '.join(invalid)}")

    end = req.end or _api_pkg.cn_today()
    start = _resolve_start(req, end, FACTOR_DEFAULT_DAYS)
    _api_pkg._guard_server_backtest_range(start, end)
    symbols = req.symbols if req.symbols else None
    if symbols is not None and len(symbols) > FACTOR_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"指定标的最多支持 {FACTOR_MAX_SYMBOLS} 只, 请缩小标的范围。",
        )

    svc = FactorBacktestService(_api_pkg._get_engine(request))
    try:
        admit_request(request, COOKIE_NAME)
        result = svc.run_batch(
            FactorBatchConfig(
                factor_names=factor_names,
                symbols=symbols,
                start=start,
                end=end,
                n_groups=req.n_groups,
                rebalance=req.rebalance,
                weight=req.weight,
                fees_pct=req.fees_pct,
                slippage_bps=req.slippage_bps,
                asset_type=req.asset_type,
            )
        )
    except DailyIntegrityError:
        raise
    except EnrichedGenerationUnavailableError as exc:
        raise HTTPException(
            status_code=400,
            detail="行情数据正在更新(盘中实时刷新), 请稍后重试",
        ) from exc
    return _stamp_daily_data(result)


@router.get("/candidates")
def candidates_list():
    try:
        return {"items": _candidate_store().list()}
    except Exception as exc:  # noqa: BLE001 - 回测候选计算外部输入不可控, 统一转 400 错误
        _raise_candidate_error(exc)


@router.post("/candidates")
def candidate_create(req: CandidateCreateRequest):
    try:
        return _candidate_store().create(
            kind=req.kind,
            name=req.name,
            source_id=req.source_id,
            config=req.config,
            metrics=req.metrics,
            data_as_of=req.data_as_of.isoformat() if req.data_as_of else None,
            status=req.status,
        )
    except Exception as exc:  # noqa: BLE001 - 回测候选计算外部输入不可控, 统一转 400 错误
        _raise_candidate_error(exc)


@router.patch("/candidates/{candidate_id}")
def candidate_update(candidate_id: str, req: CandidateUpdateRequest):
    if req.name is None and req.status is None:
        raise HTTPException(status_code=400, detail="至少提供一个需要更新的字段")
    try:
        return _candidate_store().update(candidate_id, name=req.name, status=req.status)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="候选方案不存在") from exc
    except Exception as exc:  # noqa: BLE001 - 回测候选计算外部输入不可控, 统一转 400 错误
        _raise_candidate_error(exc)


@router.delete("/candidates/{candidate_id}")
def candidate_delete(candidate_id: str):
    try:
        _candidate_store().delete(candidate_id)
        return {"ok": True}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="候选方案不存在") from exc
    except Exception as exc:  # noqa: BLE001 - 回测候选计算外部输入不可控, 统一转 400 错误
        _raise_candidate_error(exc)


@router.post("/strategy/run")
def strategy_run(req: StrategyBacktestRequest, request: Request):
    """策略回测 — 复用 StrategyDef 体系做全周期回测。"""
    from app.backtest.strategy import StrategyBacktestConfig
    from app.backtest.worker import make_worker_task, run_worker_task

    end = req.end or _api_pkg.cn_today()
    start = _resolve_start(req, end, FACTOR_DEFAULT_DAYS)
    _api_pkg._guard_server_backtest_range(start, end)
    _guard_minute_strategy_backtest(request, req.strategy_id, start, req.asset_type)

    if req.position_sizing == "policy":
        from app.backtest import position_policies

        if (
            not req.position_policy
            or position_policies.get_policy(settings.data_dir, req.position_policy) is None
        ):
            raise HTTPException(
                status_code=400,
                detail=f"仓位策略不存在或不可用: {req.position_policy!r}",
            )
    cfg = StrategyBacktestConfig(
        strategy_id=req.strategy_id,
        symbols=req.symbols if req.symbols else None,
        start=start,
        end=end,
        params=req.params,
        overrides=req.overrides,
        matching=req.matching,
        entry_fill=req.entry_fill,
        exit_fill=req.exit_fill,
        fees_pct=req.fees_pct,
        commission_pct=req.commission_pct,
        stamp_tax_pct=req.stamp_tax_pct,
        slippage_bps=req.slippage_bps,
        max_positions=req.max_positions,
        max_exposure_pct=req.max_exposure_pct,
        initial_capital=req.initial_capital,
        position_sizing=req.position_sizing,
        position_policy=req.position_policy,
        position_policy_params=req.position_policy_params,
        mode=req.mode,
        holding_days=req.holding_days,
        asset_type=req.asset_type,
        minute_fill=req.minute_fill,
        regime_filter=req.regime_filter,
    )
    task = make_worker_task("backtest", settings.data_dir, cfg)
    from app.services.heavy_job_limiter import shared_heavy_job_limiter

    admit_request(request, COOKIE_NAME)
    with shared_heavy_job_limiter.slot("normal"):
        workload.record("backtest.task", "executed")
        return _stamp_daily_data(run_worker_task(task))


@router.get("/strategy/runs/{run_id}/evidence")
def strategy_run_evidence(run_id: str):
    from app.backtest.evidence import load_evidence

    try:
        return load_evidence(settings.data_dir, run_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, detail="该回测没有版本记录，旧结果可重新回测生成") from exc
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(503, detail="暂时无法读取回测版本记录") from exc


@router.get("/strategy/stream")
async def strategy_stream(
    request: Request,
    strategy_id: str,
    symbols: str | None = None,
    start: str | None = None,
    end: str | None = None,
    matching: str = "open_t+1",
    entry_fill: str | None = None,
    exit_fill: str | None = None,
    fees_pct: float = 0.0002,
    commission_pct: float | None = None,
    stamp_tax_pct: float | None = None,
    slippage_bps: float = 5.0,
    max_positions: int = 10,
    max_exposure_pct: float = 1.0,
    initial_capital: float = 1_000_000.0,
    position_sizing: str = "equal",
    position_policy: str | None = None,
    position_policy_params: str | None = None,
    params: str | None = None,
    overrides: str | None = None,
    mode: str = "position",
    holding_days: int = 5,
    asset_type: str = "stock",
    minute_fill: bool = False,
    regime_filter: str | None = None,
    force: bool = False,
):
    """SSE 流式策略回测: 实时推送进度, 完成后推送结果, 支持重连 (刷新/切页后恢复)。

    - 相同参数的任务只启动一次, 多次连接订阅同一个任务
    - 断开连接不会取消任务 (除非显式调用 cancel)
    - 结果保留 5 分钟供重连
    - force=1: 掺入 job_key 使其不命中普通 (非 force) 参数的结果缓存;
      同 key 已完成的 force 任务也会被丢弃重建 — 5 分钟内重复强制重跑
      都真正重算 (重连/取消仍按 qs 重算同一 key, 语义不变)

    事件类型:
      - progress: {day, total, date, equity}
      - done: {result} (完整回测结果)
      - error: {message}
    """
    context = _prepare_strategy_stream_job(request, locals())

    start_date = context["start_date"]
    end_date = context["end_date"]
    guard_violated = context["guard_violated"]
    job = context["job"]
    strategy_backtest_config = context["StrategyBacktestConfig"]
    make_worker_task = context["make_worker_task"]
    run_worker_task = context["run_worker_task"]
    strategy_id = context["strategy_id"]
    symbols = context["symbols"]
    params = context["params"]
    overrides = context["overrides"]
    matching = context["matching"]
    entry_fill = context["entry_fill"]
    exit_fill = context["exit_fill"]
    fees_pct = context["fees_pct"]
    commission_pct = context["commission_pct"]
    stamp_tax_pct = context["stamp_tax_pct"]
    slippage_bps = context["slippage_bps"]
    max_positions = context["max_positions"]
    max_exposure_pct = context["max_exposure_pct"]
    initial_capital = context["initial_capital"]
    position_sizing = context["position_sizing"]
    position_policy = context["position_policy"]
    position_policy_params = context["position_policy_params"]
    mode = context["mode"]
    holding_days = context["holding_days"]
    asset_type = context["asset_type"]
    minute_fill = context["minute_fill"]
    regime_filter = context["regime_filter"]

    def event_generator():
        return _strategy_stream_events(request, context)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/strategy/cancel")
async def strategy_cancel(request: Request):
    """取消正在运行的回测任务 (前端传 query string, 后端算 job_key)。"""
    body = await request.json()
    qs = body.get("qs", "")
    # 解析 qs 得到参数
    from urllib.parse import parse_qs

    p = parse_qs(qs)

    def _get(key: str, default: str = "") -> str:
        return p.get(key, [default])[0]

    def _get_opt_float(key: str) -> float | None:
        # 可选成本参数: 缺省或空串 → None (与 stream 侧 float | None 口径一致, 保证 job_key 对齐)。
        v = _get(key)
        return float(v) if v else None

    def _get_opt_bool(key: str) -> bool:
        # 与 FastAPI/pydantic v2 查询参数 bool 解析口径一致 (真值别名集合),
        # 否则 stream/cancel 两端算出不同 job_key, 停止按钮失效
        return _get(key).lower() in ("1", "true", "on", "yes", "t", "y")

    job_key = _make_job_key(
        _get("strategy_id"),
        _get("symbols") or None,
        _get("start") or None,
        _get("end") or None,
        _get("matching", "open_t+1"),
        _get("entry_fill") or None,
        _get("exit_fill") or None,
        float(_get("fees_pct", "0.0002")),
        float(_get("slippage_bps", "5")),
        int(_get("max_positions", "10")),
        float(_get("max_exposure_pct", "1")),
        float(_get("initial_capital", "1000000")),
        _get("position_sizing", "equal"),
        _get("params") or None,
        _get("overrides") or None,
        _get("mode", "position"),
        int(_get("holding_days", "5")),
        commission_pct=_get_opt_float("commission_pct"),
        stamp_tax_pct=_get_opt_float("stamp_tax_pct"),
        asset_type=_get("asset_type", "stock"),
        minute_fill=_get_opt_bool("minute_fill"),
        regime_filter=_get("regime_filter") or None,
        position_policy=_get("position_policy") or None,
        position_policy_params=_get("position_policy_params") or None,
        force=_get_opt_bool("force"),
    )
    # 持锁读任务表: 与 _cleanup_stale_jobs 的 pop、stream 的写入互斥
    with _jobs_lock:
        job = _api_pkg._running_jobs.get(job_key)
    if job and not job.done:
        job.cancel_event.set()
        return {"ok": True}
    return {"ok": False, "message": "任务不存在或已完成"}
