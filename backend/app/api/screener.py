"""Screener API。"""

from __future__ import annotations

import copy
import importlib
import logging
import math
import time
from dataclasses import asdict
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.config import settings
from app.services import screener_work, strategy_cache, strategy_pool, strategy_run_queue
from app.services.screener import ScreenerService
from app.strategy import config as strategy_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/screener", tags=["screener"])


def _admit_submission(request: Request) -> None:
    from app.api.auth import COOKIE_NAME
    from app.services.workload import admit_request

    admit_request(request, COOKIE_NAME)


class CustomRequest(BaseModel):
    conditions: list[str]
    order_by: str | None = None
    limit: int = 30
    pool: list[str] | None = None
    as_of: date | None = None
    ext_columns: str | None = None
    asset_type: str = "stock"


class PresetRequest(BaseModel):
    strategy_id: str
    pool: list[str] | None = None
    as_of: date | None = None
    ext_columns: str | None = None
    asset_type: str = "stock"
    timeframe: str = "1d"
    ensure: bool = False


def _safe_ext_value(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


# 标识符安全原语 (转义 + 白名单) 集中在 app.db_safe, 见 Issue #150 注入防护。


def _load_ext_value_maps(
    repo, ext_columns: str | None, as_of: str | None = None
) -> dict[str, dict[str, Any]]:
    """按请求加载扩展列，返回 {输出列名: {symbol: value}}。

    策略结果缓存是共享文件，不能被不同 ext_columns 组合污染；因此扩展列只在
    返回前通过该投影映射追加到结果副本中。

    as_of 给定时, timeseries 扩展表按该日期的分区取值 —— 与策略结果同日,
    保证「结果里展示的扩展值(如人气)」与「策略过滤时实际使用的值」同一口径;
    该日期无分区 (扩展表上线前的历史日期等) 时回退最新分区。
    as_of=None 维持"当前值"语义 (自选/个股详情/告警等实时场景), 取最新分区。

    基于 config 的路径按 parquet 文件 mtime 签名 memoize, 文件未变时跳过磁盘重读。
    """
    ext_specs = _parse_ext_columns(ext_columns) if ext_columns else []
    if not ext_specs:
        return {}

    from app.api.ext_data import _read_ext_dataframe
    from app.services.ext_data import ExtConfigStore, collapse_ext_rows_at

    data_dir = repo.store.data_dir
    ext_store = ExtConfigStore(data_dir)
    configs = {c.id: c for c in ext_store.load_all()}
    value_maps: dict[str, dict[str, Any]] = {}
    want_date = str(as_of) if as_of else None

    for config_id, field_name, at in ext_specs:
        out_col = f"{config_id}__{field_name}"
        cfg = configs.get(config_id)
        cache_key = (config_id, field_name, want_date or "", at or "")
        sig = _ext_parquet_signature(cfg, data_dir) if cfg else None
        try:
            if cfg:
                # 命中缓存 (文件签名一致) → 复用, 免去磁盘重读
                cached = _ext_value_map_cache.get(cache_key)  # type: ignore[arg-type]
                if cached is not None and sig is not None and cached[0] == sig:
                    value_maps[out_col] = cached[1]
                    continue
                if want_date and cfg.mode == "timeseries":
                    # 与结果同日的历史分区: 展示口径 == 过滤口径
                    ext_df, _ = _read_ext_dataframe(cfg, data_dir, snapshot_date=want_date)
                    if ext_df.is_empty():
                        # 该日期无分区 → 回退最新, 避免整列空白
                        ext_df, _ = _read_ext_dataframe(cfg, data_dir)
                else:
                    # 时序扩展表默认只取最新分区，避免历史分区把同一 symbol JOIN 放大。
                    ext_df, _ = _read_ext_dataframe(cfg, data_dir)
            else:
                # 配置未注册（无 CK 扩展表）→ 跳过该列；原 DuckDB 视图 fallback 已移除
                continue

            if (
                ext_df.is_empty()
                or "symbol" not in ext_df.columns
                or field_name not in ext_df.columns
            ):
                continue

            # 日内多盘: @时刻 → 该时刻或之前最近一盘; 缺省 → 末盘。
            # 收敛须在 select 之前 — 时间列被裁掉后 collapse 检测不到会静默回退末行
            time_field = cfg.pull.time_field if cfg and cfg.pull else None
            ext_df = collapse_ext_rows_at(ext_df, at, time_field).select(["symbol", field_name])
            vmap = {
                str(row["symbol"]): _safe_ext_value(row.get(field_name))
                for row in ext_df.to_dicts()
                if row.get("symbol")
            }
            value_maps[out_col] = vmap
            if cfg and sig is not None:
                _ext_value_map_cache[cache_key] = (sig, vmap)  # type: ignore[index]
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("screener ext column join skipped for %s.%s: %s", config_id, field_name, e)

    return value_maps


@router.get("/strategies")
def strategies(
    request: Request,
    asset_type: str = Query("stock"),
    timeframe: str = Query("1d"),
):
    """兼容策略清单端点；唯一数据源为 StrategyEngine。"""
    data_dir = request.app.state.repo.store.data_dir
    engine = getattr(request.app.state, "strategy_engine", None)
    if engine is None:
        raise HTTPException(status_code=503, detail="策略引擎未初始化")
    presets = []
    library = strategy_pool.library_metadata(data_dir)
    for meta in engine.list_strategies():
        if meta.get("research_only"):
            continue
        if asset_type not in meta.get("asset_types", ["stock"]):
            continue
        if timeframe not in meta.get("timeframes", ["1d"]):
            continue
        sid = meta["id"]
        overrides = strategy_config.load_override(data_dir, sid)
        presets.append(
            {
                **meta,
                "name": overrides.get("name") or meta["name"],
                "description": overrides.get("description") or meta.get("description", ""),
                "library": library.get(sid),
            }
        )

    return {"presets": presets, "load_errors": engine.load_errors()}


@router.post("/run")
def run_custom(req: CustomRequest, request: Request):
    repo = request.app.state.repo
    svc = ScreenerService(repo, asset_type=req.asset_type)
    as_of = req.as_of or svc.latest_date()
    if not as_of:
        raise HTTPException(
            status_code=400, detail="无可用数据日期 — enriched 表为空,请先运行盘后管道"
        )
    identity = screener_work.hard_identity(repo, None, req.asset_type)
    key = (
        str(as_of),
        identity,
        screener_work.source_version(repo, req.asset_type, "1d", as_of),
        screener_work.digest(req.model_dump()),
    )

    def compute():
        from app.services.heavy_job_limiter import shared_heavy_job_limiter

        _admit_submission(request)
        with shared_heavy_job_limiter.slot("exclusive"):
            result = svc.run(
                as_of=as_of,
                conditions=req.conditions,
                order_by=req.order_by,
                limit=req.limit,
                pool=req.pool,
            )
            screener_work.assert_current(repo, None, req.asset_type, identity)
            return _safe(asdict(result))

    safe_data = copy.deepcopy(screener_work.custom_work.run(key, compute))
    warnings = _coverage_warnings(svc, as_of)
    if warnings:
        safe_data["warnings"] = warnings
    # 投影取结果日分区 (与过滤同口径), 而非最新分区
    ext_values = _load_ext_value_maps(repo, req.ext_columns, as_of=str(as_of))
    return _result_with_ext(safe_data, ext_values)


@router.post("/run_preset")
def run_preset(req: PresetRequest, request: Request):
    repo = request.app.state.repo
    svc = ScreenerService(repo, asset_type=req.asset_type)
    as_of = req.as_of or svc.latest_date()
    if not as_of:
        raise HTTPException(status_code=400, detail="无可用数据日期")

    # 加载用户保存的策略配置
    data_dir = request.app.state.repo.store.data_dir
    # ext 投影取结果日分区: 结果表里展示的扩展值(如人气) 必须与叠加条件过滤时用的同一天
    ext_values = _load_ext_value_maps(repo, req.ext_columns, as_of=str(as_of))
    overrides = strategy_config.load_override(data_dir, req.strategy_id)
    identity = screener_work.identity_for_inputs(
        repo,
        getattr(request.app.state, "strategy_engine", None),
        req.asset_type,
        {req.strategy_id: overrides},
        normalized_inputs=True,
    )
    engine = getattr(request.app.state, "strategy_engine", None)
    if not engine:
        raise HTTPException(
            status_code=404, detail=f"策略引擎未初始化或策略 {req.strategy_id} 不存在"
        )

    try:
        if not engine.has(req.strategy_id):
            raise ValueError(f"unknown strategy: {req.strategy_id}")
        if engine.get(req.strategy_id).meta.get("research_only"):
            raise ValueError(f"unknown strategy: {req.strategy_id}")
        params = dict(overrides.get("params") or {})
        key = (
            str(as_of),
            identity,
            screener_work.source_version(repo, req.asset_type, req.timeframe, as_of),
            screener_work.digest(req.model_dump(exclude={"ensure"})),
        )

        def compute():
            _admit_submission(request)
            with strategy_run_queue.MANAGER.execution():
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
                screener_work.assert_current(repo, engine, req.asset_type, identity)
                return result

        result = copy.deepcopy(
            screener_work.single_work.run(key, compute, ttl_s=6 if req.ensure else 0)
        )
    except ValueError as e:
        status_code = 404 if "unknown strategy" in str(e) else 400
        raise HTTPException(status_code=status_code, detail=str(e)) from e

    safe_data = _safe(asdict(result))
    if req.timeframe == "1d":
        _attach_preset_warnings_and_cache(
            svc, engine, repo, req, as_of, params, overrides, identity, data_dir, safe_data
        )
    return _result_with_ext(safe_data, ext_values)


def _cached_with_realtime(request: Request) -> dict:
    """读取盘后缓存，并用监控引擎的实时结果覆盖同策略。"""
    data_dir = request.app.state.repo.store.data_dir
    from app.services.daily_health import assert_usable

    assert_usable(data_dir)
    cached = strategy_cache.read_cache(data_dir)
    if cached is None:
        cached = {"as_of": None, "results": {}, "updated_at": None}

    # 叠加监控引擎内存里的实时结果 (若有), 用新鲜数据覆盖同策略的盘后结果
    monitor_engine = getattr(request.app.state, "monitor_engine", None)
    if monitor_engine is not None:
        realtime_results = monitor_engine.latest_strategy_results()
        if realtime_results:
            results = dict(cached.get("results") or {})
            results.update(realtime_results)
            cached = dict(cached)
            cached["results"] = results
            # 有实时数据时, 以最新时间戳为准
            import time as _time

            cached["updated_at"] = int(_time.time() * 1000)

    return cached


@router.get("/cached")
def get_cached(
    request: Request,
    ext_columns: str | None = Query(None, description="逗号分隔: config_id.field_name"),
):
    """读取策略结果缓存, 并叠加监控引擎本轮实时算出的结果。

    - 盘后缓存 (strategy_cache.json): 非监控策略 / 页面秒加载用, run_all 写入。
    - 监控引擎内存结果 (latest_strategy_results): 实时行情每轮对「加入监控的策略」算出,
      不落盘 (避免与 read_cache 的 mtime 校验冲突), 在此直接叠加覆盖盘后结果。
      被监控的策略拿到新鲜数据, 非监控策略仍用盘后缓存。
    - 价格类展示字段统一用当前实时快照覆盖 (同日才覆盖), 避免列表显示扫描时刻
      价格、详情显示实时价格造成的"同一只票两个涨幅"。
    """
    cached = _cached_with_live_quotes(request)

    # 无任何数据 (盘后缓存空 + 无实时结果) → 返回空标记, 前端据此提示
    if not cached.get("results") and cached.get("as_of") is None:
        return {"as_of": None, "results": {}, "updated_at": None}

    # 结果与「今日曾命中」可能跨日期 (盘后缓存 + 监控实时叠加) → 按各自日期取
    # ext 投影, 保证展示的扩展值与结果计算/过滤所用同日。
    result_dates = {
        r.get("as_of") for r in (cached.get("results") or {}).values() if isinstance(r, dict)
    }
    ever_rows = cached.get("today_ever_rows")
    if isinstance(ever_rows, dict):
        for sym_map in ever_rows.values():
            if isinstance(sym_map, dict):
                result_dates.update(
                    row.get("date") for row in sym_map.values() if isinstance(row, dict)
                )
    result_dates.add(None)  # 无日期的结果 → 最新分区兜底
    maps_by_date = _ext_maps_for_dates(request.app.state.repo, ext_columns, result_dates)
    return _cache_payload_with_ext(cached, maps_by_date)


@router.get("/cached-summary")
def get_cached_summary(request: Request, task_id: str | None = None):
    """返回策略卡片所需的轻量摘要，不序列化股票明细。"""
    # Observe task completion before reading its published cache, so a result
    # committed during this request cannot stop polling with an older snapshot.
    task = strategy_run_queue.MANAGER.task_status(task_id) if task_id else None
    cached = _cached_with_realtime(request)
    results = cached.get("results") or {}
    summary = {
        sid: {
            "total": int(result.get("total") or 0),
            "as_of": result.get("as_of"),
            # 渐进式 run_all 写入的计算时间戳; 监控实时叠加/旧缓存无此字段 → None,
            # 前端视为新鲜 (有值即为最新一轮实时结果)
            "computed_at": result.get("computed_at"),
            **(
                {"available_at": result["available_at"]}
                if result.get("available_at") is not None
                else {}
            ),
        }
        for sid, result in results.items()
        if isinstance(result, dict)
    }

    cached_as_of = cached.get("as_of")
    ever_rows = cached.get("today_ever_rows") or {}
    ever_counts = {}
    for sid, result in results.items():
        if not isinstance(result, dict) or result.get("as_of") != cached_as_of:
            continue
        current_symbols = {
            str(row["symbol"])
            for row in result.get("rows") or []
            if isinstance(row, dict) and row.get("symbol")
        }
        ever_counts[sid] = len(set((ever_rows.get(sid) or {}).keys()) | current_symbols)

    # 分钟策略结果不落 strategy_cache (日线语义), 渐进式转后台算完后在这里
    # 以 handle 快照合入 (仅 as_of 与日线缓存同日的), 前端轮询本端点点亮卡片。
    minute_pending: list[str] = []
    identity = screener_work.hard_identity(
        request.app.state.repo, getattr(request.app.state, "strategy_engine", None), "stock"
    )
    for m in strategy_run_queue.MANAGER.minute_summaries(identity=identity, asset_type="stock"):
        if m.get("as_of") != cached_as_of:
            continue
        for sid, item in (m.get("results") or {}).items():
            previous = summary.get(sid) or {}
            if (item.get("computed_at") or 0) >= (previous.get("computed_at") or 0):
                summary[sid] = item
        minute_pending.extend(m.get("pending") or [])

    return {
        "as_of": cached_as_of,
        "results": summary,
        "today_ever_counts": ever_counts,
        "updated_at": cached.get("updated_at"),
        "minute_pending": minute_pending,
        **({"task": task} if task_id else {}),
    }


@router.get("/cached-result/{strategy_id}")
def get_cached_result(
    strategy_id: str,
    request: Request,
    ext_columns: str | None = Query(None, description="逗号分隔: config_id.field_name"),
):
    """按需返回单个策略的完整明细及其今日失效行。

    与 /cached 一致: 价格类展示字段用当前实时快照覆盖 (同日才覆盖), 保证
    详情里的涨跌幅与列表、以及个股详情弹窗同源。
    """
    cached = _cached_with_live_quotes(request)
    raw_result = (cached.get("results") or {}).get(strategy_id)
    if not isinstance(raw_result, dict):
        return {
            "result": None,
            "today_ever_rows": None,
            "strategy_ids_by_symbol": {},
            "updated_at": cached.get("updated_at"),
        }

    # 结果行按该策略结果的 as_of 日取值 (与过滤同口径); 今日曾命中行按行日期取值
    repo = request.app.state.repo
    result_date = raw_result.get("as_of")
    ext_values = _load_ext_value_maps(
        repo, ext_columns, as_of=str(result_date) if result_date else None
    )
    result = {
        "as_of": raw_result.get("as_of"),
        "strategy": strategy_id,
        "rows": _rows_with_ext(raw_result.get("rows") or [], ext_values),
        "total": int(raw_result.get("total") or 0),
        "elapsed_ms": 0.0,
    }

    ever_rows = None
    if cached.get("as_of") == result["as_of"]:
        strategy_ever_rows = (cached.get("today_ever_rows") or {}).get(strategy_id)
        if isinstance(strategy_ever_rows, dict):
            ever_maps = _ext_maps_for_dates(
                repo,
                ext_columns,
                {row.get("date") for row in strategy_ever_rows.values() if isinstance(row, dict)}
                | {result_date},
            )
            ever_rows = {
                symbol: _row_with_ext(
                    row, _maps_for_date(ever_maps, row.get("date")), symbol=symbol
                )
                for symbol, row in strategy_ever_rows.items()
                if isinstance(row, dict)
            }

    selected_symbols = {
        str(row["symbol"])
        for row in raw_result.get("rows") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    strategy_ids_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in selected_symbols}
    for sid, cached_result in (cached.get("results") or {}).items():
        if not isinstance(cached_result, dict) or cached_result.get("as_of") != result["as_of"]:
            continue
        for row in cached_result.get("rows") or []:
            symbol = str(row.get("symbol")) if isinstance(row, dict) and row.get("symbol") else None
            if symbol in strategy_ids_by_symbol:
                strategy_ids_by_symbol[symbol].append(sid)

    return {
        "result": result,
        "today_ever_rows": ever_rows,
        "strategy_ids_by_symbol": strategy_ids_by_symbol,
        "updated_at": cached.get("updated_at"),
    }


@router.get("/market-snapshot")
def market_snapshot(request: Request):
    """最新全市场轻量行情快照，供板块/概念聚合分析使用。"""
    import polars as pl

    repo = request.app.state.repo
    svc = ScreenerService(repo)
    as_of = svc.latest_date()
    if not as_of:
        return {"as_of": None, "rows": []}

    df = svc._load_enriched_for_date(as_of)
    if df.is_empty():
        return {"as_of": str(as_of), "rows": []}

    if "close" in df.columns and "total_shares" in df.columns and "market_cap" not in df.columns:
        df = df.with_columns((pl.col("close") * pl.col("total_shares")).alias("market_cap"))
    if (
        "close" in df.columns
        and "float_shares" in df.columns
        and "float_market_cap" not in df.columns
    ):
        df = df.with_columns((pl.col("close") * pl.col("float_shares")).alias("float_market_cap"))

    cols = [
        "symbol",
        "name",
        "close",
        "change_pct",
        "amount",
        "volume",
        "turnover_rate",
        "vol_ratio_5d",
        "total_shares",
        "float_shares",
        "market_cap",
        "float_market_cap",
        "consecutive_limit_ups",
    ]
    df = df.select([c for c in cols if c in df.columns])
    rows = df.to_dicts()
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, float) and not math.isfinite(v):
                r[k] = None

    return {"as_of": str(as_of), "rows": rows}


@router.post("/run_all")
def run_all(request: Request, body: dict | None = None):
    """批量运行指定策略；注册、路由和执行均由 StrategyEngine 负责。"""
    from datetime import date as date_type

    t_total = time.perf_counter()

    body = body or {}
    repo = request.app.state.repo
    asset_type = str(body.get("asset_type") or "stock")
    timeframe = str(body.get("timeframe") or "1d")
    svc = ScreenerService(repo, asset_type=asset_type)
    engine = getattr(request.app.state, "strategy_engine", None)
    if engine is None:
        raise HTTPException(status_code=503, detail="策略引擎未初始化")

    as_of = _run_all_as_of(body.get("as_of"), svc, date_type)
    if not as_of:
        return {"as_of": None, "results": {}}

    data_dir = request.app.state.repo.store.data_dir

    all_ids, skipped_unknown = _run_all_resolve_ids(
        engine, body.get("strategy_ids"), asset_type, timeframe
    )
    if not all_ids:
        return {"as_of": str(as_of), "results": {}}

    params_map, overrides_map = _run_all_overrides(data_dir, all_ids)
    ctx = _run_all_ctx(locals())

    first_return_s = settings.strategy_run_all_first_return_s
    if body.get("summary_only") and first_return_s > 0:
        return _run_all_progressive_response(ctx, body, first_return_s, skipped_unknown)

    identity = screener_work.identity_for_inputs(repo, engine, asset_type, overrides_map)
    ctx.identity = identity
    version = screener_work.source_version(repo, asset_type, timeframe, as_of)

    def compute():
        _admit_submission(request)
        with strategy_run_queue.MANAGER.execution():
            context = svc.build_strategy_context(
                engine,
                as_of,
                all_ids,
                timeframe=timeframe,
                params_map=params_map,
                overrides_map=overrides_map,
            )
            result = engine.run_all(
                context, params_map=params_map, overrides_map=overrides_map, strategy_ids=all_ids
            )
            screener_work.assert_current(repo, engine, asset_type, identity)
            return result

    try:
        engine_results = screener_work.full_batch_work.run(
            (identity, asset_type, timeframe, str(as_of), tuple(sorted(all_ids)), version),
            compute,
            ttl_s=6 if body.get("ensure") is True else 0,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return _run_all_finish(ctx, engine_results, body, skipped_unknown)


@router.get("/limit-ladder")
def limit_ladder(
    request: Request,
    as_of: date | None = None,
    direction: str = Query("up", description="up=涨停梯队 | down=跌停梯队"),
    ext_columns: str | None = Query(None, description="逗号分隔: config_id.field_name"),
):
    """连板/连跌梯队 — 按连板数分组, 含三状态。
    返回: tiers = [{ boards, count, stocks: [{symbol,name,change_pct,status,...}] }]

    direction=up (默认):
      status: limit_up=涨停 | broken=炸板(摸板未封) | failed=断板(晋级失败)
    direction=down:
      status: limit_down=跌停 | recovery=翘板(跌停后回升,含收阳条件) | failed=止跌(昨日跌停今日未跌停也未翘板)

    ext_columns: 动态 JOIN 扩展数据, 如 "concept.concept,industry.industry"
    """
    import polars as pl

    is_down = direction == "down"
    sig_col, consec_col, broken_col, status_main, status_broken, status_failed = (
        _limit_ladder_fields(is_down)
    )

    repo = request.app.state.repo
    svc = ScreenerService(repo)
    as_of = as_of or svc.latest_date()
    if not as_of:
        raise HTTPException(status_code=400, detail="无可用数据日期")

    df = svc._load_enriched_for_date(as_of)
    if df.is_empty():
        return {"as_of": str(as_of), "tiers": [], "counts": {"up": 0, "down": 0}}

    # 双方向涨跌停计数(不论当前 direction, 前端始终同时显示)
    count_up_raw = (
        int(df.filter(pl.col("signal_limit_up").fill_null(False)).height)
        if "signal_limit_up" in df.columns
        else 0
    )
    count_down_raw = (
        int(df.filter(pl.col("signal_limit_down").fill_null(False)).height)
        if "signal_limit_down" in df.columns
        else 0
    )

    sealed = _limit_ladder_sealed(
        getattr(request.app.state, "depth_service", None),
        as_of,
        is_down,
        count_up_raw,
        count_down_raw,
    )

    df = _limit_ladder_status(
        df,
        svc,
        as_of,
        sig_col,
        consec_col,
        broken_col,
        status_main,
        status_broken,
        status_failed,
    )

    df = _limit_ladder_sealed_overlay(df, sealed.sealed_map, status_main, status_broken)

    df = df.with_columns(_one_word_limit_expr(status_main, df.columns).alias("is_one_word"))

    ext_specs = _parse_ext_columns(ext_columns) if ext_columns else []
    ext_col_names: list[str] = []
    if ext_specs:
        df, ext_col_names = _limit_ladder_ext_join(df, ext_specs, repo.store.data_dir)

    tier_list = _limit_ladder_tier_list(df, consec_col, status_main, status_broken, ext_col_names)
    return _limit_ladder_response(as_of, tier_list, sealed, count_up_raw, count_down_raw)


# ---------------------------------------------------------------------------
# 子模块 re-export（保持原公开 API 与 monkeypatch 可观察性）
# ---------------------------------------------------------------------------
# 用赋值式 re-export：子模块的路由注册在 importlib.import_module 时完成；
# 赋值目标进入本模块 globals，函数体与测试 monkeypatch 仍按原模块名解析。

__screener_ext_mod = importlib.import_module("._screener_ext", __package__)
_LIVE_QUOTE_FIELDS = __screener_ext_mod._LIVE_QUOTE_FIELDS
_cache_payload_with_ext = __screener_ext_mod._cache_payload_with_ext
_cached_with_live_quotes = __screener_ext_mod._cached_with_live_quotes
_ext_maps_for_dates = __screener_ext_mod._ext_maps_for_dates
_ext_parquet_signature = __screener_ext_mod._ext_parquet_signature
_ext_value_map_cache = __screener_ext_mod._ext_value_map_cache
_live_quote_overlay = __screener_ext_mod._live_quote_overlay
_maps_for_date = __screener_ext_mod._maps_for_date
_parse_ext_columns = __screener_ext_mod._parse_ext_columns
_quote_date_matches = __screener_ext_mod._quote_date_matches
_result_with_ext = __screener_ext_mod._result_with_ext
_results_with_ext = __screener_ext_mod._results_with_ext
_row_with_ext = __screener_ext_mod._row_with_ext
_rows_with_ext = __screener_ext_mod._rows_with_ext
_rows_with_live_quotes = __screener_ext_mod._rows_with_live_quotes
_safe = __screener_ext_mod._safe
del __screener_ext_mod

__screener_runall_mod = importlib.import_module("._screener_runall", __package__)
_RunAllContext = __screener_runall_mod._RunAllContext
_attach_preset_warnings_and_cache = __screener_runall_mod._attach_preset_warnings_and_cache
_coverage_warnings = __screener_runall_mod._coverage_warnings
_finalize_progressive_cache = __screener_runall_mod._finalize_progressive_cache
_prepare_progressive_context = __screener_runall_mod._prepare_progressive_context
_progressive_job = __screener_runall_mod._progressive_job
_run_all_as_of = __screener_runall_mod._run_all_as_of
_run_all_ctx = __screener_runall_mod._run_all_ctx
_run_all_final_response = __screener_runall_mod._run_all_final_response
_run_all_finish = __screener_runall_mod._run_all_finish
_run_all_overrides = __screener_runall_mod._run_all_overrides
_run_all_progressive = __screener_runall_mod._run_all_progressive
_run_all_progressive_response = __screener_runall_mod._run_all_progressive_response
_run_all_resolve_ids = __screener_runall_mod._run_all_resolve_ids
_run_all_results = __screener_runall_mod._run_all_results
_run_all_write_cache = __screener_runall_mod._run_all_write_cache
_run_one_progressive_strategy = __screener_runall_mod._run_one_progressive_strategy
_submit_progressive_job = __screener_runall_mod._submit_progressive_job
_update_cache_strategy = __screener_runall_mod._update_cache_strategy
_write_one_progressive_cache = __screener_runall_mod._write_one_progressive_cache
del __screener_runall_mod

__screener_ladder_mod = importlib.import_module("._screener_ladder", __package__)
_LadderSealed = __screener_ladder_mod._LadderSealed
_count_sealed = __screener_ladder_mod._count_sealed
_dedup_ext = __screener_ladder_mod._dedup_ext
_limit_ladder_ext_join = __screener_ladder_mod._limit_ladder_ext_join
_limit_ladder_fields = __screener_ladder_mod._limit_ladder_fields
_limit_ladder_response = __screener_ladder_mod._limit_ladder_response
_limit_ladder_sealed = __screener_ladder_mod._limit_ladder_sealed
_limit_ladder_sealed_overlay = __screener_ladder_mod._limit_ladder_sealed_overlay
_limit_ladder_status = __screener_ladder_mod._limit_ladder_status
_limit_ladder_tier_list = __screener_ladder_mod._limit_ladder_tier_list
_one_word_limit_expr = __screener_ladder_mod._one_word_limit_expr
del __screener_ladder_mod
