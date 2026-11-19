"""settings API 路由分组 _routes_system（由 settings.py 机械拆分，顺序不变）。"""

from __future__ import annotations

import time
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from ._common import (
    _FALLBACK_ENDPOINTS,
    _SOUND_PACK_EXTS,
    _SOUND_PACK_MEDIA_TYPES,
    ENDPOINTS_TTL,
    ENDPOINTS_URL,
    ApiTokenCreateIn,
    BacktestWorkerRssLimitIn,
    BatchSymbolsIn,
    DepthFinalizeTimeIn,
    DepthPollingIntervalIn,
    EnrichedBatchSizeIn,
    IndexDailyBatchSizeIn,
    LimitLadderMonitorIn,
    MarketRecapPromptIn,
    PipelineScheduleIn,
    ReviewPushIn,
    ReviewScheduleIn,
    TestEndpointIn,
    _api_pkg,
    _safe_pack_path,
    _sound_pack_dir,
    logger,
)

router = APIRouter(prefix="/api/settings")


@router.get("/endpoints")
def list_endpoints() -> dict:
    """代理拉取 tickflow.org/endpoints.json 并返回规范化端点列表。

    前端无法跨域直连该 URL(无 CORS 头),故由本接口代理。带 8s 超时、
    5 分钟内存缓存,远程失败时回退到内置列表,保证 UI 始终有内容。
    返回结构与原始 endpoints.json 一致(透传 schema/version 等元信息)。
    """
    import httpx

    now = time.monotonic()
    cached = _api_pkg._endpoints_cache.get("data")
    if cached is not None and (now - _api_pkg._endpoints_cache["ts"]) < ENDPOINTS_TTL:
        return cached

    source = "remote"
    data: dict | None = None
    try:
        resp = httpx.get(ENDPOINTS_URL, timeout=8.0, follow_redirects=True)
        if resp.status_code == 200:
            parsed = resp.json()
            eps = parsed.get("endpoints")
            # 校验:必须是列表且每项含必要字段,否则视为无效
            if isinstance(eps, list) and all(isinstance(e, dict) and "url" in e for e in eps):
                data = {
                    "version": parsed.get("version", 1),
                    "description": parsed.get("description", "TickFlow API 端点配置"),
                    "healthPath": parsed.get("healthPath", "/health"),
                    "testRounds": parsed.get("testRounds", 5),
                    "endpoints": eps,
                }
    except (httpx.HTTPError, ValueError):
        logger.warning("拉取 endpoints.json 失败，使用内置回退列表", exc_info=True)

    if data is None:
        source = "fallback"
        data = {
            "version": 1,
            "description": "TickFlow API 端点配置",
            "healthPath": "/health",
            "testRounds": 5,
            "endpoints": _FALLBACK_ENDPOINTS,
        }

    # 标记数据来源,便于前端提示(回退时显示"内置列表")。
    data["source"] = source
    _api_pkg._endpoints_cache["ts"] = now
    _api_pkg._endpoints_cache["data"] = data
    return data


@router.post("/test_endpoint")
async def test_endpoint(req: TestEndpointIn) -> dict:
    """测试端点网络延迟:对 /health 多轮探测取中位数。

    参考 TickFlow 官方 latency_test.py:
    - 路径用 /health(公开、轻量),反映真实网络延迟而非业务接口耗时
    - 多轮探测(默认 5 轮,取自 endpoints.json 的 testRounds),间隔 0.3s
    - 返回 median/min/max/success,前端显示中位数
    - 异步实现,保证"全部测速"时多端点真正并行
    """
    import asyncio
    import statistics

    base = req.url.rstrip("/")
    # 缓存初值的 "data" 是 None(键存在, get 的默认值不生效), 端点清单未预热时要兜底
    manifest = _api_pkg._endpoints_cache.get("data") or {}
    rounds = max(1, min(10, req.rounds or manifest.get("testRounds", 5)))
    health_url = base + "/health"

    latencies: list[float] = []
    for _ in range(rounds):
        ms = await _api_pkg._http_ping(health_url)
        if ms is not None:
            latencies.append(ms)
        # 官方脚本间隔 0.3s;末轮无需等待
        await asyncio.sleep(0.3)

    success = len(latencies)
    if success == 0:
        return {
            "ok": False,
            "error": "不可达",
            "url": req.url,
            "rounds": rounds,
            "success": 0,
            "median_ms": None,
            "min_ms": None,
            "max_ms": None,
        }

    median = round(statistics.median(latencies), 2)
    return {
        "ok": True,
        "url": req.url,
        "rounds": rounds,
        "success": success,
        "median_ms": median,
        "min_ms": round(min(latencies), 2),
        "max_ms": round(max(latencies), 2),
        # 兼容旧字段:取中位数作为代表延迟
        "latency_ms": median,
    }


@router.put("/preferences/pipeline-schedule")
def update_pipeline_schedule(req: PipelineScheduleIn, request: Request) -> dict:
    """保存盘后管道调度时间并立即 reschedule。"""
    from app.services import preferences

    sched = preferences.set_pipeline_schedule(req.hour, req.minute)

    # 动态 reschedule
    from apscheduler.triggers.cron import CronTrigger

    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.reschedule_job(
            "daily_pipeline",
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=sched["hour"],
                minute=sched["minute"],
                timezone="Asia/Shanghai",
            ),
        )
        logger.info("pipeline rescheduled to %02d:%02d mon-fri", sched["hour"], sched["minute"])

    return sched


@router.put("/preferences/instruments-schedule")
def update_instruments_schedule(req: PipelineScheduleIn, request: Request) -> dict | None:
    """保存盘前标的维表调度时间并立即 reschedule（无 scheduler 时返回 None）。"""
    from app.services import preferences

    sched = preferences.set_instruments_schedule(req.hour, req.minute)

    from apscheduler.triggers.cron import CronTrigger

    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.reschedule_job(
            "pre_market_instruments",
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=sched["hour"],
                minute=sched["minute"],
                timezone="Asia/Shanghai",
            ),
        )
        return sched
    return None  # 未注入 scheduler 时保持原有 None 返回


@router.put("/preferences/enriched-batch-size")
def update_enriched_batch_size(req: EnrichedBatchSizeIn) -> dict:
    """保存 enriched 全量计算批次大小。"""
    from app.services import preferences

    size = preferences.set_enriched_batch_size(req.size)
    return {"enriched_batch_size": size}


@router.put("/preferences/index-daily-batch-size")
def update_index_daily_batch_size(req: IndexDailyBatchSizeIn) -> dict:
    """保存指数日 K 同步批次大小。"""
    from app.services import preferences

    size = preferences.set_index_daily_batch_size(req.size)
    return {"index_daily_batch_size": size}


@router.put("/preferences/daily-batch-symbols")
def update_daily_batch_symbols(req: BatchSymbolsIn) -> dict:
    """保存 daily-batch 单请求标的数上限 (同时是前端分块大小)。"""
    from app.services import preferences

    return {"daily_batch_symbols": preferences.set_daily_batch_symbols(req.size)}


@router.put("/preferences/minute-batch-symbols")
def update_minute_batch_symbols(req: BatchSymbolsIn) -> dict:
    """保存 minute-batch 单请求标的数上限 (同时是前端分块大小)。"""
    from app.services import preferences

    return {"minute_batch_symbols": preferences.set_minute_batch_symbols(req.size)}


@router.put("/preferences/backtest-worker-rss-limit")
def update_backtest_worker_rss_limit(req: BacktestWorkerRssLimitIn) -> dict:
    """保存回测/优化 worker RSS 内存上限 (GiB)。对下一个任务即生效, 无需重启。"""
    from app.services import preferences

    return {"backtest_worker_rss_limit_gib": preferences.set_backtest_worker_rss_limit_gib(req.gib)}


@router.put("/preferences/limit-ladder-monitor")
def update_limit_ladder_monitor(req: LimitLadderMonitorIn, request: Request) -> dict:
    """连板梯队 5 档监控开关。开启→启动 depth 轮询, 关闭→停止。"""
    from app.services import preferences

    preferences.save({"limit_ladder_monitor_enabled": req.enabled})

    # 立即应用: 启停 depth 轮询线程
    depth_svc = getattr(request.app.state, "depth_service", None)
    if depth_svc:
        depth_svc.apply_monitor_toggle(req.enabled)

    return {"limit_ladder_monitor_enabled": req.enabled}


@router.post("/preferences/limit-ladder-monitor/run")
def run_limit_ladder_fix(request: Request) -> dict:
    """立即手动修正一次真假板(拉取五档盘口 + 更新缓存)。需 Pro+。"""
    from app.tickflow.capabilities import Cap

    capset = request.app.state.capabilities
    capset.require(Cap.DEPTH5_BATCH)  # 无能力抛 CapabilityDenied(403)

    depth_svc = getattr(request.app.state, "depth_service", None)
    if not depth_svc:
        raise HTTPException(status_code=503, detail="depth 服务未初始化")
    result = depth_svc.run_once()
    # sealed 数据变了, 清看板总览缓存, 否则看板在 TTL 窗口内仍返回旧的 limit_up/fake 等
    if result.get("ok"):
        from app.api.overview import invalidate_overview_cache

        invalidate_overview_cache()
    return result


@router.put("/preferences/depth-polling-interval")
def update_depth_polling_interval(req: DepthPollingIntervalIn, request: Request) -> dict:
    """保存五档盘口盘中轮询间隔(秒)。需 Pro+。"""
    from app.tickflow.capabilities import Cap

    request.app.state.capabilities.require(Cap.DEPTH5_BATCH)

    from app.services import preferences

    interval = preferences.set_depth_polling_interval(req.interval)
    return {"depth_polling_interval": interval}


@router.put("/preferences/depth-finalize-time")
def update_depth_finalize_time(req: DepthFinalizeTimeIn, request: Request) -> dict:
    """保存盘后 sealed 定版时间(范围15:01~18:00)并立即 reschedule。需 Pro+。"""
    from app.tickflow.capabilities import Cap

    request.app.state.capabilities.require(Cap.DEPTH5_BATCH)

    from app.services import preferences

    sched = preferences.set_depth_finalize_time(req.hour, req.minute)

    from apscheduler.triggers.cron import CronTrigger

    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.reschedule_job(
            "depth_finalize",
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=sched["hour"],
                minute=sched["minute"],
                timezone="Asia/Shanghai",
            ),
        )
        logger.info(
            "depth_finalize rescheduled to %02d:%02d mon-fri", sched["hour"], sched["minute"]
        )

    return sched


@router.put("/preferences/review-schedule")
def update_review_schedule(req: ReviewScheduleIn, request: Request) -> dict:
    """保存定时复盘调度并立即更新 APScheduler job。

    - enabled=True: 注册/更新 job(工作日定时生成复盘报告)
    - enabled=False: 移除 job(停止定时复盘)
    - 校验: 开启时若 AI Key 未配置则拒绝(复盘依赖 AI), 提示用户先配置。
    - 时间下限 15:00(A股收盘), 由 preferences 层强制。
    """
    from app.services import preferences

    if req.enabled:
        # 复盘必须有 AI Key, 否则每日报错刷日志
        from app import secrets_store

        if not secrets_store.get_ai_key():
            raise HTTPException(
                status_code=400,
                detail="复盘依赖 AI,请先在「设置 → AI」配置 API Key 后再开启定时复盘",
            )

    sched = preferences.set_review_schedule(req.enabled, req.hour, req.minute)

    # 动态操作 APScheduler job
    from app.jobs.daily_pipeline import REVIEW_JOB_ID, _register_review_job

    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        if sched["enabled"]:
            _register_review_job(scheduler, request.app.state.repo, sched["hour"], sched["minute"])
            logger.info(
                "scheduled_review enabled @%02d:%02d mon-fri", sched["hour"], sched["minute"]
            )
        else:
            try:
                scheduler.remove_job(REVIEW_JOB_ID)
                logger.info("scheduled_review disabled (job removed)")
            except Exception:  # noqa: BLE001 - 移除定时job失败可忽略(job本就不存在)
                pass

    return sched


@router.put("/preferences/review-push")
def update_review_push(req: ReviewPushIn) -> dict:
    """复盘推送设置(渠道多选 + 触发方式)。

    纯偏好, 与定时复盘 / 实时行情完全独立, 常驻可单独设置。空数组=不推送。
    实际推送由归档端点(POST /api/market-recap/reports)与定时任务(_run_scheduled_review)
    在归档后读取渠道列表, 并按 review_push_mode 决定是否外发:
      - manual: 定时复盘只归档不推送, 手动保存需显式 push=true
      - auto: 归档即推(行为与旧逻辑一致)
    白名单外的渠道会被过滤掉, 白名单外的 mode 值回退 manual。
    """
    from app.services import preferences

    saved = preferences.set_review_push_channels(req.channels)
    mode = preferences.get_review_push_mode()
    if req.mode is not None:
        mode = preferences.set_review_push_mode(req.mode)
    return {"review_push_channels": saved, "review_push_mode": mode}


@router.get("/preferences/market-recap-prompt")
def get_market_recap_prompt() -> dict:
    """复盘系统提示词: 自定义值 + 内置默认全文。

    默认全文单独按需拉取 (约 2.4k 字符), 不塞进 GET /preferences 大 dict。
    """
    from app.services import preferences
    from app.services.market_recap import _SYSTEM_PROMPT

    custom = preferences.get_market_recap_system_prompt()
    return {"custom": custom, "default": _SYSTEM_PROMPT, "customized": bool(custom)}


@router.put("/preferences/market-recap-prompt")
def update_market_recap_prompt(req: MarketRecapPromptIn) -> dict:
    """保存自定义复盘系统提示词; 空串恢复默认模板。超长返回 400。"""
    from app.services import preferences

    try:
        saved = preferences.set_market_recap_system_prompt(req.prompt)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"custom": saved, "customized": bool(saved)}


@router.get("/api-tokens")
def api_tokens_list(request: Request) -> dict:
    from app.services import api_tokens as svc

    return {"tokens": svc.list_tokens(request.app.state.repo.store.data_dir)}


@router.post("/api-tokens")
def api_tokens_create(body: ApiTokenCreateIn, request: Request) -> dict:
    """创建 Token — 明文只在本次响应出现一次, 前端弹窗提示立即保存。"""
    from app.services import api_tokens as svc

    try:
        record, plaintext = svc.create_token(
            request.app.state.repo.store.data_dir, body.name, body.scopes
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"token": record, "plaintext": plaintext}


@router.delete("/api-tokens/{token_id}")
def api_tokens_revoke(token_id: str, request: Request) -> dict:
    from app.services import api_tokens as svc

    if not svc.revoke_token(request.app.state.repo.store.data_dir, token_id):
        raise HTTPException(status_code=404, detail=f"Token '{token_id}' 不存在")
    return {"status": "revoked", "id": token_id}


@router.get("/sound-packs")
def sound_packs_list(request: Request) -> dict:
    pack_dir = _sound_pack_dir(request)
    packs: list[dict] = []
    if pack_dir.is_dir():
        packs = [
            {
                "name": f.name,
                "size": f.stat().st_size,
                "ext": f.suffix.lower().lstrip("."),
            }
            for f in sorted(pack_dir.iterdir())
            if f.is_file() and f.suffix.lower() in _SOUND_PACK_EXTS
        ]
    return {"packs": packs}


@router.post("/sound-packs")
async def sound_packs_upload(
    request: Request,
    file: UploadFile = File(...),  # noqa: B008 - FastAPI 依赖注入惯例
) -> dict:
    import re

    raw = file.filename or ""
    suffix = Path(raw).suffix.lower()
    if suffix not in _SOUND_PACK_EXTS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的音频格式 '{suffix or '(无后缀)'}', 可选: {', '.join(sorted(_SOUND_PACK_EXTS))}",
        )
    # 词干只保留安全字符 (中文/字母/数字/_-.), 其余替换为 _; 控制长度防文件名失控
    stem = re.sub(r"[^\w\-.()\u4e00-\u9fff]", "_", Path(raw).stem).strip("._")[:40] or "pack"
    name = f"{stem}{suffix}"

    data = await file.read()
    if len(data) > _api_pkg._SOUND_PACK_MAX_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"文件超过 {_api_pkg._SOUND_PACK_MAX_BYTES // 1024 // 1024}MB 上限 (当前 {len(data) / 1024 / 1024:.1f}MB)",
        )
    if not data:
        raise HTTPException(status_code=400, detail="空文件")

    pack_dir = _sound_pack_dir(request)
    pack_dir.mkdir(parents=True, exist_ok=True)
    target = _safe_pack_path(pack_dir, name)
    # 同名重传 = 替换 (用户自定义音的自然语义)
    target.write_bytes(data)
    return {"status": "saved", "pack": {"name": name, "size": len(data), "ext": suffix.lstrip(".")}}


@router.delete("/sound-packs/{name}")
def sound_packs_delete(name: str, request: Request) -> dict:
    path = _safe_pack_path(_sound_pack_dir(request), name)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"语音包 '{name}' 不存在")
    path.unlink()
    return {"status": "deleted", "name": name}


@router.get("/sound-packs/{name}/file")
def sound_packs_file(name: str, request: Request) -> FileResponse:
    path = _safe_pack_path(_sound_pack_dir(request), name)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"语音包 '{name}' 不存在")
    media_type = _SOUND_PACK_MEDIA_TYPES.get(path.suffix.lower(), "application/octet-stream")
    return FileResponse(path, media_type=media_type)
