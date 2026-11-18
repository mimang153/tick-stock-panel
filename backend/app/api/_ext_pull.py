"""扩展数据定时拉取：配置、API Key、手动执行与历史回补路由。"""

from __future__ import annotations

from datetime import UTC, date

from fastapi import HTTPException, Query, Request

from app.api._root_ref import bind as _bind
from app.api.ext_data import ApiKeyReq
from app.services.ext_data import (
    PullConfig,
    ext_api_key_field,
    get_ext_api_key,
    run_ext_data_io,
)
from app.services.ext_data_schema import PullAuthReq as PullAuthReq
from app.services.ext_data_schema import PullConfigReq
from app.services.ext_pull import _request_json, fetch_and_ingest, pull_scheduler

_root = _bind("app.api.ext_data")
logger = _root.logger


# ---------------------------------------------------------------------------
# 定时拉取
# ---------------------------------------------------------------------------


@_root.router.put("/{config_id}/pull")
def configure_pull(request: Request, config_id: str, body: PullConfigReq):
    """配置（或更新）定时拉取。"""
    store = _root._store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    # 保留历史状态字段; auth 缺省时沿用现有配置 (关闭鉴权需显式传 {"type":"none"})
    old_pull = config.pull
    # auth.key_ref / fanout: UI 未暴露的键在保存时沿用旧值, 避免一次普通保存
    # 就把出厂配方里的凭据引用与两级请求配方抹掉。
    old_auth = (old_pull.auth or {}) if old_pull else {}
    if body.auth is not None:
        new_auth: dict | None = body.auth.model_dump()
        if new_auth and old_auth.get("key_ref") and not new_auth.get("key_ref"):
            new_auth["key_ref"] = old_auth["key_ref"]
    else:
        new_auth = old_pull.auth if old_pull else None
    # 会话 Cookie 不能当服务间长期凭据: 保存时就拒绝, 让问题在配置阶段暴露,
    # 而不是等 24 小时会话过期后定时拉取集体 401。
    if new_auth:
        from app.services.ext_pull import session_cookie_rejection

        rejected = session_cookie_rejection(new_auth, get_ext_api_key(config_id))
        if rejected:
            raise HTTPException(400, rejected)
    config.pull = PullConfig(
        url=body.url,
        method=body.method,
        headers=body.headers,
        body=body.body,
        response_path=body.response_path,
        field_map=body.field_map,
        schedule_minutes=body.schedule_minutes,
        schedule_seconds=body.schedule_seconds,
        enabled=body.enabled,
        schedule_time=body.schedule_time,
        schedule_period=body.schedule_period,
        schedule_day=body.schedule_day,
        schedule_trading_only=body.schedule_trading_only,
        time_window_start=body.time_window_start,
        time_window_end=body.time_window_end,
        date_param=body.date_param,
        date_format=body.date_format,
        time_field=body.time_field,
        since_param=body.since_param,
        granularity=body.granularity,
        auth=new_auth,
        timeout_seconds=body.timeout_seconds,
        page_param=body.page_param,
        page_size_param=body.page_size_param,
        page_size=body.page_size,
        page_start=body.page_start,
        max_pages=body.max_pages,
        fanout=body.fanout if body.fanout is not None else (old_pull.fanout if old_pull else None),
        last_run=old_pull.last_run if old_pull else None,
        last_status=old_pull.last_status if old_pull else None,
        last_message=old_pull.last_message if old_pull else None,
        last_rows=old_pull.last_rows if old_pull else None,
    )
    store.upsert(config)

    # 刷新调度器
    pull_scheduler.refresh(_root._data_dir(request))

    # 关闭定时拉取时清理残留的 next_run, 避免前端展示一个永不执行的"下次"
    if not config.pull.enabled:
        cleared = store.get(config_id)
        if cleared and cleared.pull and cleared.pull.next_run:
            cleared.pull.next_run = None
            store.upsert(cleared)

    return {"status": "ok", "pull": config.pull.to_dict()}


@_root.router.get("/{config_id}/api-key")
def get_pull_api_key(request: Request, config_id: str):
    """查询拉取接口 API Key 状态。只返回脱敏值, 不返回明文。"""
    store = _root._store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    from app import secrets_store

    key = get_ext_api_key(config_id)
    return {"key_set": bool(key), "masked_key": secrets_store.mask(key) if key else ""}


@_root.router.put("/{config_id}/api-key")
def set_pull_api_key(request: Request, config_id: str, body: ApiKeyReq):
    """设置 (或空串清除) 拉取接口的 API Key, 存 secrets.json (权限 0600)。"""
    store = _root._store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    from app import secrets_store

    value = body.key.strip()
    if value:
        from app.services.ext_pull import session_cookie_rejection

        rejected = session_cookie_rejection(config.pull.auth if config.pull else None, value)
        if rejected:
            raise HTTPException(400, rejected)
        secrets_store.save({ext_api_key_field(config_id): value})
    else:
        secrets_store.clear(ext_api_key_field(config_id))
    return {
        "status": "ok",
        "key_set": bool(value),
        "masked_key": secrets_store.mask(value) if value else "",
    }


@_root.router.post("/{config_id}/pull/test")
async def test_pull(request: Request, config_id: str):
    """测试拉取：请求外部 API 并返回预览数据，不写入。"""
    store = _root._store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    if not config.pull or not config.pull.url:
        raise HTTPException(400, "拉取未配置或 URL 为空")

    # 复用正式拉取的请求实现 (UA 标识头 + 鉴权注入同一套口径), 不带日期参数
    from app.services.ext_pull import _apply_field_map, _extract_rows

    pull = config.pull
    try:
        data = await _request_json(pull, config.id)
        rows = _extract_rows(data, pull.response_path)
        preview = _apply_field_map(rows[:5], pull.field_map)
        return {
            "status": "ok",
            "total_rows": len(rows),
            "preview": preview,
            "has_symbol": bool(rows and "symbol" in rows[0]),
        }
    except Exception as e:
        raise HTTPException(400, f"测试失败: {e}") from e


@_root.router.post("/{config_id}/pull/run")
async def run_pull(request: Request, config_id: str):
    """手动触发一次拉取并写入。"""
    store = _root._store(request)
    config = await run_ext_data_io(store.get, config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    if not config.pull or not config.pull.url:
        raise HTTPException(400, "拉取未配置或 URL 为空")

    try:
        n, d = await fetch_and_ingest(config, _root._data_dir(request))
        await run_ext_data_io(_root._refresh_views, request)
        # 写回执行状态, 让前端"上次执行"面板立即反映
        updated = await run_ext_data_io(store.get, config_id)
        if updated and updated.pull:
            from datetime import datetime

            updated.pull.last_run = datetime.now(UTC).isoformat()
            updated.pull.last_status = "success"
            updated.pull.last_message = f"{n} rows @ {d}"
            updated.pull.last_rows = n
            await run_ext_data_io(store.upsert, updated)
        return {"status": "ok", "rows": n, "date": d}
    except Exception as e:
        # 失败也写回状态, 记录错误信息
        failed = await run_ext_data_io(store.get, config_id)
        if failed and failed.pull:
            from datetime import datetime

            failed.pull.last_run = datetime.now(UTC).isoformat()
            failed.pull.last_status = "error"
            failed.pull.last_message = str(e)[:200]
            await run_ext_data_io(store.upsert, failed)
        raise HTTPException(400, f"拉取失败: {e}") from e


@_root.router.post("/{config_id}/backfill")
async def backfill_history_ep(
    request: Request,
    config_id: str,
    start: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end: str = Query(..., description="结束日期 YYYY-MM-DD (含)"),
):
    """历史回补: 按本地交易日逐日拉取并写入 timeseries 分区。

    前提: 配置为 timeseries 模式且拉取配置了 date_param (接口支持按日期
    查询)。幂等 —— 已存在的分区跳过, 失败单日不中断, 结果逐项返回。
    """
    store = _root._store(request)
    config = await run_ext_data_io(store.get, config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    try:
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)
    except ValueError as e:
        raise HTTPException(422, f"日期格式错误 (应为 YYYY-MM-DD): {e}") from e

    from app.services.ext_pull import backfill_history

    try:
        result = await backfill_history(config, _root._data_dir(request), start_d, end_d)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    await run_ext_data_io(_root._refresh_views, request)
    return {"status": "ok", **result}
