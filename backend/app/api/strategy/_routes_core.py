"""strategy API 路由分组 _routes_core（由 strategy.py 机械拆分，顺序不变）。"""

from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Request

from app.services import strategy_pool
from app.strategy import config as strategy_config

from ._common import (
    RunAllRequest,
    RunRequest,
    SaveConfigRequest,
    _data_dir,
    _get_engine,
    _get_public_strategy,
    _invalidate_strategy_runtime,
    _overlay_changed,
    _safe,
    _strategy_detail,
    _strip_defaults,
    _validate_overlay_filter,
    _validate_scoring_config,
)

router = APIRouter(prefix="/api/strategies")


@router.get("")
def list_strategies(
    request: Request,
    asset_type: str | None = None,
    timeframe: str | None = None,
    include_research: bool = False,
):
    engine = _get_engine(request)
    data_dir = _data_dir(request)
    all_overrides = strategy_config.list_overrides(data_dir)
    library = strategy_pool.library_metadata(data_dir)

    result = []
    # include_research=True 时返回 research_only 草稿(供前端「草稿」分区展示/发布)。
    # 默认 False 保持既有行为: 草稿不进公开列表。
    for meta in engine.list_strategies(include_research=include_research):
        if meta.get("research_only") and not include_research:
            continue
        if asset_type and asset_type not in meta.get("asset_types", ["stock"]):
            continue
        if timeframe and timeframe not in meta.get("timeframes", ["1d"]):
            continue
        sid = meta["id"]
        s = engine.get(sid)
        overrides = all_overrides.get(sid)
        result.append({**_strategy_detail(s, overrides, engine), "library": library.get(sid)})
    payload: dict = {"strategies": result, "load_errors": engine.load_errors()}
    # 去内置化一次性迁移摘要 (启动时执行过才有): 前端提示一次后本地记忆
    migration = getattr(request.app.state, "legacy_migration", None)
    if migration:
        payload["legacy_migration"] = migration
    return payload


@router.get("/{strategy_id}")
def get_strategy(strategy_id: str, request: Request):
    engine = _get_engine(request)
    s = _get_public_strategy(engine, strategy_id)
    overrides = strategy_config.load_override(_data_dir(request), strategy_id)
    library = strategy_pool.library_metadata(_data_dir(request))
    return {**_strategy_detail(s, overrides or None, engine), "library": library.get(strategy_id)}


@router.post("/run")
def run_strategy(req: RunRequest, request: Request):
    engine = _get_engine(request)
    _get_public_strategy(engine, req.strategy_id)
    data_dir = _data_dir(request)

    # 读取用户覆盖配置
    overrides = strategy_config.load_override(data_dir, req.strategy_id)
    params = req.params or {}
    # 合并用户保存的策略参数
    if overrides.get("params"):
        merged = dict(overrides["params"])
        merged.update(params)  # 请求里的优先
        params = merged

    # 确定日期
    as_of = req.as_of
    if not as_of:
        from app.services.screener import ScreenerService

        svc = ScreenerService(request.app.state.repo, asset_type=req.asset_type)
        as_of = svc.latest_date()
    if not as_of:
        raise HTTPException(status_code=400, detail="无可用数据日期")

    try:
        from app.services.screener import ScreenerService

        svc = ScreenerService(request.app.state.repo, asset_type=req.asset_type)
        context = svc.build_strategy_context(
            engine,
            as_of,
            [req.strategy_id],
            timeframe=req.timeframe,
            params_map={req.strategy_id: params},
            overrides_map={req.strategy_id: overrides or {}},
        )
        result = engine.run(
            req.strategy_id,
            context,
            pool=req.pool,
            params=params,
            overrides=overrides or None,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    return _safe(asdict(result))


@router.post("/run-all")
def run_all(req: RunAllRequest, request: Request):
    engine = _get_engine(request)
    data_dir = _data_dir(request)

    as_of = req.as_of
    if not as_of:
        from app.services.screener import ScreenerService

        svc = ScreenerService(request.app.state.repo, asset_type=req.asset_type)
        as_of = svc.latest_date()
    if not as_of:
        return {"as_of": None, "results": {}}

    all_overrides = strategy_config.list_overrides(data_dir)
    strategy_ids = [
        meta["id"]
        for meta in engine.list_strategies()
        if not meta.get("research_only")
        and req.asset_type in meta.get("asset_types", ["stock"])
        and req.timeframe in meta.get("timeframes", ["1d"])
    ]
    from app.services.screener import ScreenerService

    svc = ScreenerService(request.app.state.repo, asset_type=req.asset_type)
    params_map = {
        sid: dict((all_overrides.get(sid) or {}).get("params") or {}) for sid in strategy_ids
    }
    context = svc.build_strategy_context(
        engine,
        as_of,
        strategy_ids,
        timeframe=req.timeframe,
        params_map=params_map,
        overrides_map={sid: all_overrides.get(sid, {}) for sid in strategy_ids},
    )
    results: dict[str, dict] = {}
    for sid, result in engine.run_all(
        context,
        params_map=params_map,
        overrides_map={sid: all_overrides.get(sid, {}) for sid in strategy_ids},
        strategy_ids=strategy_ids,
    ).items():
        results[sid] = {"total": result.total, "as_of": str(as_of)}

    return {"as_of": str(as_of), "results": results}


@router.post("/config")
def save_config(req: SaveConfigRequest, request: Request):
    engine = _get_engine(request)
    strategy = _get_public_strategy(engine, req.strategy_id)
    data_dir = _data_dir(request)

    _validate_scoring_config(req.overrides)
    _validate_overlay_filter(strategy, req.overrides)
    # 剥离与策略默认值相同的字段，只保存用户真正修改过的值
    overrides = _strip_defaults(req.strategy_id, req.overrides, engine)

    old = strategy_config.load_override(data_dir, req.strategy_id) or {}
    strategy_config.save_override(data_dir, req.strategy_id, overrides)
    # 叠加条件改变策略结果, 而策略结果缓存键不含 overrides → 变化时必须清,
    # 否则策略页读到旧缓存。仅在实际变化时清, 不波及普通保存。
    if _overlay_changed(old.get("overlay_filter"), overrides.get("overlay_filter")):
        _invalidate_strategy_runtime(request)
    return {"ok": True}


@router.patch("/config")
def patch_config(req: SaveConfigRequest, request: Request):
    engine = _get_engine(request)
    strategy = _get_public_strategy(engine, req.strategy_id)
    data_dir = _data_dir(request)
    overrides = strategy_config.load_override(data_dir, req.strategy_id)
    old_overlay = overrides.get("overlay_filter")
    overrides.update(req.overrides)
    _validate_scoring_config(overrides)
    _validate_overlay_filter(strategy, overrides)
    strategy_config.save_override(
        data_dir,
        req.strategy_id,
        _strip_defaults(req.strategy_id, overrides, engine),
    )
    if _overlay_changed(old_overlay, overrides.get("overlay_filter")):
        _invalidate_strategy_runtime(request)
    return {"ok": True}


@router.delete("/config/{strategy_id}")
def reset_config(strategy_id: str, request: Request):
    _get_public_strategy(_get_engine(request), strategy_id)
    strategy_config.delete_override(_data_dir(request), strategy_id)
    return {"ok": True}
