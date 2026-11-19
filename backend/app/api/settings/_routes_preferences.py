"""settings API 路由分组 _routes_preferences（由 settings.py 机械拆分，顺序不变）。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app import secrets_store

from ._common import (
    CustomSourceIn,
    CustomSourceTestIn,
    DailyBatchCompressPrefs,
    DashboardLayoutIn,
    DataProvidersIn,
    DataSourceJobTimeoutPrefs,
    MiningSchedulePrefs,
    MinuteBatchCompressPrefs,
    MinuteSyncPrefs,
    NavHiddenIn,
    NavLayoutIn,
    NavOrderIn,
    PluginKeyIn,
    UiPrefsIn,
    _api_pkg,
    _minute_history_days,
    _realtime_allowed,
)

router = APIRouter(prefix="/api/settings")


@router.get("/preferences")
def get_preferences() -> dict:
    """返回用户偏好设置。"""
    from app.services import preferences

    return {
        "realtime_quotes_enabled": preferences.get_realtime_quotes_enabled(),
        "realtime_allowed": _realtime_allowed(),
        "watchlist_groups_in_nav": preferences.get_watchlist_groups_in_nav(),
        "minute_sync_enabled": preferences.get_minute_sync_enabled(),
        "minute_sync_days": preferences.get_minute_sync_days(),
        "minute_sync_segment_days": preferences.get_minute_sync_segment_days(),
        "minute_refresh_enabled": preferences.get_minute_refresh_enabled(),
        "minute_refresh_interval": preferences.get_minute_refresh_interval(),
        "daily_data_provider": preferences.get_daily_data_provider(),
        "adj_factor_provider": preferences.get_adj_factor_provider(),
        "minute_data_provider": preferences.get_minute_data_provider(),
        "full_minute_data_provider": preferences.get_full_minute_data_provider(),
        "minute_history_days": _minute_history_days(),
        "depth5_data_provider": preferences.get_depth5_data_provider(),
        "realtime_data_provider": preferences.get_realtime_data_provider(),
        "financial_data_provider": preferences.get_financial_provider(),
        "data_source_job_timeout_s": preferences.get_data_source_job_timeout_s(),
        "data_source_long_job_timeout_s": preferences.get_data_source_long_job_timeout_s(),
        "minute_batch_compress": preferences.get_minute_batch_compress(),
        "daily_batch_compress": preferences.get_daily_batch_compress(),
        **preferences.get_realtime_quote_scope(),
        "pipeline_pull_a_share": preferences.get_pipeline_pull_a_share(),
        "pipeline_pull_etf": preferences.get_pipeline_pull_etf(),
        "pipeline_pull_index": preferences.get_pipeline_pull_index(),
        "pipeline_regime_enabled": preferences.get_pipeline_regime_enabled(),
        "regime_batch_days": preferences.get_regime_batch_days(),
        "regime_warmup_days": preferences.get_regime_warmup_days(),
        "pipeline_index_symbols": preferences.get_pipeline_index_symbols(),
        "pipeline_schedule": preferences.get_pipeline_schedule(),
        "instruments_schedule": preferences.get_instruments_schedule(),
        "enriched_batch_size": preferences.get_enriched_batch_size(),
        "index_daily_batch_size": preferences.get_index_daily_batch_size(),
        "daily_batch_symbols": preferences.get_daily_batch_symbols(),
        "minute_batch_symbols": preferences.get_minute_batch_symbols(),
        "backtest_worker_rss_limit_gib": preferences.get_backtest_worker_rss_limit_gib(),
        "watchlist_columns": preferences.get_watchlist_columns(),
        "screener_result_columns": preferences.get_screener_result_columns(),
        "sse_refresh_pages": preferences.get_sse_refresh_pages(),
        "strategy_monitor_enabled": preferences.get_strategy_monitor_enabled(),
        "strategy_monitor_ids": preferences.get_strategy_monitor_ids(),
        "system_notify_enabled": preferences.get_system_notify_enabled(),
        "feishu_webhook_url": preferences.get_feishu_webhook_url(),
        "feishu_webhook_secret": preferences.get_feishu_webhook_secret(),
        "wecom_webhook_url": preferences.get_wecom_webhook_url(),
        "custom_webhook_url": preferences.get_custom_webhook_url(),
        "custom_webhook_secret_set": bool(secrets_store.get_custom_webhook_secret()),
        "email_smtp_config": preferences.get_email_smtp_config(),
        "email_smtp_password_set": bool(secrets_store.get_email_smtp_password()),
        "wecom_bot_id": preferences.get_wecom_bot_id(),
        "wecom_bot_secret": preferences.get_wecom_bot_secret(),
        "wecom_bot_enabled": preferences.get_wecom_bot_enabled(),
        "webhook_enabled_default": preferences.get_webhook_enabled_default(),
        "webhook_default_channels": preferences.get_webhook_default_channels(),
        "minute_intraday_refresh": preferences.get_minute_intraday_refresh(),
        "minute_intraday_refresh_interval": preferences.get_minute_intraday_refresh_interval(),
        "monitor_ext_fields": preferences.get_monitor_ext_fields(),
        "nav_order": preferences.get_nav_order(),
        "nav_hidden": preferences.get_nav_hidden(),
        "nav_layout": preferences.get_nav_layout(),
        "dashboard_layout": preferences.get_dashboard_layout(),
        "screener_auto_run": preferences.get_screener_auto_run(),
        "assistant_auto_approve_actions": preferences.get_assistant_auto_approve_actions(),
        "assistant_mask_enabled": preferences.get_assistant_mask_enabled(),
        "limit_ladder_monitor_enabled": preferences.get_limit_ladder_monitor_enabled(),
        "depth_polling_interval": preferences.get_depth_polling_interval(),
        "depth_finalize_time": preferences.get_depth_finalize_time(),
        "review_schedule": preferences.get_review_schedule(),
        "review_push_channels": preferences.get_review_push_channels(),
        "review_push_mode": preferences.get_review_push_mode(),
        "market_recap_system_prompt": preferences.get_market_recap_system_prompt(),
        # 界面偏好 (通知弹窗/声效/语音播报/外链模板/界面缩放): 历史只存 localStorage,
        # 桌面版端口变化后 origin 改变导致读取不到 — 后端保存权威副本, 前端启动时回填。
        "ui_prefs": preferences.get_ui_prefs(),
        **preferences.get_mining_schedule(),
    }


@router.get("/data-sources")
def list_data_sources() -> dict:
    """列出已加载的数据源 (内置 / 插件 / 用户自定义)。"""
    from app.data_providers import custom as custom_sources

    return {
        "builtin": [
            {
                "name": "tickflow",
                "display_name": "TickFlow",
                "datasets": ["daily", "adj_factor", "realtime", "minute"],
            }
        ],
        "plugins": custom_sources.list_plugins(),
        "custom": custom_sources.list_sources(),
        "errors": custom_sources.errors(),
        "config_dir": str(custom_sources.data_sources_dir()),
    }


@router.get("/capability-matrix")
def get_capability_matrix() -> dict:
    """能力 x 源路由矩阵: 能力注册表 + 各源能力声明 + 当前路由偏好, 设置页一次拉全。

    偏好值经 preferences getters 注入 (自带合法源校验, 非法值回退默认),
    TickFlow 当前档位由 tickflow policy 注入 (候选按档位过滤),
    组装逻辑在 data_providers.capabilities, 本层保持薄。
    """
    from app.data_providers.capabilities import build_capability_matrix
    from app.services import preferences
    from app.tickflow import policy

    return build_capability_matrix(
        {
            "realtime_data_provider": preferences.get_realtime_data_provider(),
            "daily_data_provider": preferences.get_daily_data_provider(),
            "minute_data_provider": preferences.get_minute_data_provider(),
            "full_minute_data_provider": preferences.get_full_minute_data_provider(),
            "depth5_data_provider": preferences.get_depth5_data_provider(),
            "adj_factor_provider": preferences.get_adj_factor_provider(),
            "financial_data_provider": preferences.get_financial_provider(),
        },
        tickflow_tier=policy.base_tier_name(),
    )


@router.post("/plugin-key")
def save_plugin_key(req: PluginKeyIn) -> dict:
    """保存插件 API Key(先探后存, 对齐 /tickflow-key 语义)。

    流程: probe_plugin_key 用候选 Key 实探 → 有效才写 secrets.json
    ({plugin}_api_key, 优先级高于 .env) → 定向刷新当前插件。
    """
    from app.data_providers import custom as custom_sources

    name = req.plugin.strip().lower()
    key = req.api_key.strip()
    if not key:
        return {"ok": False, "error": "key empty"}
    from contextlib import nullcontext

    from app.data_providers.custom import packages

    external = packages.read_state(name) is not None
    with packages.mutation() if external else nullcontext():
        ok, message = custom_sources.probe_plugin_key(name, key)
        if not ok:
            return {"ok": False, "reason": "invalid", "error": message}
        previous = secrets_store.load().get(f"{name}_api_key") if external else None
        secrets_store.save({f"{name}_api_key": key})
        try:
            if external:
                custom_sources.refresh_plugin(name, strict=True)
            else:
                custom_sources.refresh_plugin(name)
        except ValueError:
            if previous is None:
                secrets_store.clear(f"{name}_api_key")
            else:
                secrets_store.save({f"{name}_api_key": previous})
            return {"ok": False, "error": "插件初始化失败，已保留原配置"}
    status = next((p for p in custom_sources.list_plugins() if p["name"] == name), None)
    return {
        "ok": True,
        "api_key_masked": secrets_store.mask(key),
        "plugin_available": bool(status and status.get("available")),
        "plugin": status,
    }


@router.delete("/plugin-key/{name}")
def clear_plugin_key(name: str) -> dict:
    """清除插件的界面配置 Key(secrets.json);.env 里的同名变量仍然生效。"""
    from app.data_providers import custom as custom_sources

    manifest = custom_sources.plugin_manifest(name)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"插件 '{name}' 不存在")
    if not manifest.get("api_key_env"):
        raise HTTPException(status_code=400, detail=f"插件 '{name}' 不支持在界面配置 Key")
    secrets_store.clear(f"{name.lower()}_api_key")
    custom_sources.refresh_plugin(name)
    status = next((p for p in custom_sources.list_plugins() if p["name"] == name), None)
    return {
        "ok": True,
        "plugin_available": bool(status and status.get("available")),
        "plugin": status,
    }


@router.post("/data-sources/reload")
def reload_data_sources() -> dict:
    """重新加载 data_sources/*.yaml。"""
    from app.data_providers import custom as custom_sources

    custom_sources.load_all()
    return list_data_sources()


@router.post("/plugins/{name}/install")
def install_plugin(name: str) -> dict:
    """安装指定插件的依赖 (npm install / pip install), 完成后重新扫描。

    根据 plugin.yaml 的 runtime 字段决定安装方式。安装可能耗时较长 (网络下载),
    客户端需设较长超时。
    """
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_builtin(name):
        raise HTTPException(status_code=404, detail=f"插件 '{name}' 不存在")
    ok, message = custom_sources.install_plugin(name)
    # 无论成功失败都重新扫描, 刷新插件状态 (安装可能部分成功)
    custom_sources.load_all()
    result = list_data_sources()
    result["install_ok"] = ok
    result["install_message"] = message
    return result


@router.delete("/plugins/{name}/install")
def uninstall_plugin(name: str) -> dict:
    """卸载指定插件的依赖 (删除 node_modules / pip uninstall), 完成后重新扫描。

    如果该插件当前正被使用, 自动回退到 tickflow。
    """
    from app.data_providers import custom as custom_sources
    from app.services import preferences

    if not custom_sources.is_builtin(name):
        raise HTTPException(status_code=404, detail=f"插件 '{name}' 不存在")
    ok, message = custom_sources.uninstall_plugin(name)
    # 卸载后若该插件正被使用, 回退 tickflow
    for getter, key, default in [
        (preferences.get_daily_data_provider, "daily_data_provider", "tickflow"),
        (preferences.get_minute_data_provider, "minute_data_provider", "tickflow"),
        (preferences.get_realtime_data_provider, "realtime_data_provider", "tickflow"),
        (preferences.get_financial_provider, "financial_data_provider", "tickflow"),
    ]:
        if getter() == name:
            preferences.save({key: default})
    custom_sources.load_all()
    result = list_data_sources()
    result["uninstall_ok"] = ok
    result["uninstall_message"] = message
    return result


@router.get("/data-sources/{name}")
def get_data_source(name: str) -> dict:
    """读取一个自定义数据源的完整配置(用于前端编辑回填)。"""
    from app.data_providers import custom as custom_sources

    cfg = custom_sources.get_config_dict(name)
    if cfg is None:
        raise HTTPException(status_code=404, detail=f"数据源 '{name}' 不存在")
    return cfg


@router.post("/data-sources")
def save_data_source(req: CustomSourceIn) -> dict:
    """创建或更新一个自定义数据源 yaml, 保存后自动 reload。

    先验后存 (对齐插件 Key 的先探后存语义): 用与加载同一套校验试建临时
    provider, 不通过直接 400 且不落盘 — 否则 YAML 写入但 load_all 校验失败,
    源从列表静默消失, 前端还弹"已保存"成功提示 (用户反馈: 全量分钟缺
    amount 映射被拒, 以为存上了)。
    """
    from app.data_providers import custom as custom_sources

    config = req.model_dump()
    config["name"] = (config.get("name") or "").lower()
    probe = None
    try:
        probe = custom_sources.create_provider(config)  # 失败自抛 ValueError(含全部校验原因)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    finally:
        if probe is not None:
            probe.close()  # 临时探针(仅校验用), 不留连接资源
    try:
        custom_sources.save_config(config["name"], config)
        custom_sources.load_all()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return list_data_sources()


@router.delete("/data-sources/{name}")
def delete_data_source(name: str, request: Request) -> dict:
    """删除一个自定义数据源 yaml, 保存后自动 reload。

    若当前总开关选中的就是被删的源, 回退到 tickflow。
    """
    from app.data_providers import custom as custom_sources
    from app.services import preferences

    try:
        custom_sources.delete_config(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    custom_sources.load_all()
    # 回退被删源的偏好
    updates: dict = {}
    if preferences.get_daily_data_provider() == name:
        updates["daily_data_provider"] = "tickflow"
    if preferences.get_realtime_data_provider() == name:
        updates["realtime_data_provider"] = "tickflow"
    if preferences.get_financial_provider() == name:
        updates["financial_data_provider"] = "tickflow"
    if preferences.get_adj_factor_provider() == name:
        updates["adj_factor_provider"] = "tickflow"
    if updates:
        preferences.save(updates)
    # 删除源可能触发偏好回退 tickflow, 同步刷新能力快照
    request.app.state.capabilities = _api_pkg.detect_capabilities()
    return list_data_sources()


@router.post("/data-sources/test")
def test_data_source(req: CustomSourceTestIn) -> dict:
    """试拉自定义数据源，不写盘。"""
    from app.data_providers import custom as custom_sources

    temporary = req.config is not None
    provider: Any = None  # 自定义配置创建与预置 provider 两种来源, 统一按鸭子类型调用
    try:
        if req.config:
            config = req.config.model_dump()
            dataset_config = config["datasets"].get(req.dataset)
            if dataset_config is None:
                raise ValueError(f"dataset '{req.dataset}' is not configured")
            config["datasets"] = {req.dataset: dataset_config}
            provider = custom_sources.create_provider(config)
        else:
            provider = custom_sources.get_provider(req.provider)
        return provider.test_dataset(req.dataset, req.symbols)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"自定义数据源测试失败: {e}") from e
    finally:
        if temporary and provider is not None:
            provider.close()


@router.put("/preferences/data-providers")
def update_data_providers(req: DataProvidersIn, request: Request) -> dict:
    """保存数据源选择。"""
    from app.services import preferences

    updates = req.model_dump(exclude_none=True)
    with preferences.mutation():
        from app.data_providers import custom as custom_sources

        removing = {p["name"] for p in custom_sources.list_plugins() if p.get("pending_remove")}
        if any(provider.lower() in removing for provider in updates.values()):
            raise HTTPException(400, "该插件正在等待卸载，请选择其他数据源")
        if updates:
            preferences.save(updates)
        # 与插件卸载共用写锁，避免并发请求重新引用已移除的数据源。
        request.app.state.capabilities = _api_pkg.detect_capabilities()
    return {
        "daily_data_provider": preferences.get_daily_data_provider(),
        "adj_factor_provider": preferences.get_adj_factor_provider(),
        "minute_data_provider": preferences.get_minute_data_provider(),
        "full_minute_data_provider": preferences.get_full_minute_data_provider(),
        "depth5_data_provider": preferences.get_depth5_data_provider(),
        "realtime_data_provider": preferences.get_realtime_data_provider(),
        "financial_data_provider": preferences.get_financial_provider(),
    }


@router.put("/preferences/data-source-job-timeouts")
def update_data_source_job_timeouts(req: DataSourceJobTimeoutPrefs) -> dict:
    """保存普通与长数据后台任务的卡死判定时间。"""
    from app.services import preferences

    preferences.save(req.model_dump())
    return req.model_dump()


@router.put("/preferences/minute-batch-compress")
def update_minute_batch_compress(req: MinuteBatchCompressPrefs) -> dict:
    """保存分时详情与批量响应的 gzip 传输压缩开关。逐请求即时读取, 保存后立即生效。"""
    from app.services import preferences

    preferences.save({"minute_batch_compress": req.minute_batch_compress})
    return {"minute_batch_compress": preferences.get_minute_batch_compress()}


@router.put("/preferences/daily-batch-compress")
def update_daily_batch_compress(req: DailyBatchCompressPrefs) -> dict:
    """保存日K详情与批量响应的 gzip 传输压缩开关 (与分时独立)。逐请求即时读取。"""
    from app.services import preferences

    preferences.save({"daily_batch_compress": req.daily_batch_compress})
    return {"daily_batch_compress": preferences.get_daily_batch_compress()}


@router.put("/preferences/mining-schedule")
def update_mining_schedule(req: MiningSchedulePrefs) -> dict:
    """一次更新周度自动 mining 配置。"""
    from app.services import preferences

    return preferences.set_mining_schedule(
        req.mining_schedule_enabled,
        req.mining_schedule_weekday,
        req.mining_budget_profile,
    )


@router.get("/preferences/watchlist-columns")
def get_watchlist_columns() -> dict:
    """返回自选列表列配置。"""
    from app.services import preferences

    cols = preferences.get_watchlist_columns()
    return {"columns": cols}


@router.put("/preferences/nav-layout")
def update_nav_layout(req: NavLayoutIn) -> dict:
    from app.services import preferences

    try:
        layout = preferences.set_nav_layout(req.nav_layout, req.nav_hidden)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {
        "nav_layout": layout,
        "nav_order": preferences.get_nav_order(),
        "nav_hidden": preferences.get_nav_hidden(),
    }


@router.put("/preferences/nav-order")
def update_nav_order(req: NavOrderIn) -> dict:
    """保存左侧菜单排序（内置页面 path + 扩展分析菜单 id 的有序列表）。"""
    from app.services import preferences

    saved = preferences.set_nav_order(req.nav_order)
    return {"nav_order": saved}


@router.put("/preferences/nav-hidden")
def update_nav_hidden(req: NavHiddenIn) -> dict:
    """保存左侧菜单隐藏项。"""
    from app.services import preferences

    saved = preferences.set_nav_hidden(req.nav_hidden)
    return {"nav_hidden": saved}


@router.put("/preferences/dashboard-layout")
def update_dashboard_layout(req: DashboardLayoutIn) -> dict:
    """保存看板自定义布局(网格 blob); null = 清除回默认布局。"""
    from app.services import preferences

    try:
        saved = preferences.set_dashboard_layout(req.layout)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"dashboard_layout": saved}


@router.put("/preferences/watchlist-columns")
def update_watchlist_columns(req: dict) -> dict:
    """保存自选列表列配置。"""
    from app.services import preferences

    columns = req.get("columns", [])
    saved = preferences.set_watchlist_columns(columns)
    return {"columns": saved}


@router.get("/preferences/screener-result-columns")
def get_screener_result_columns() -> dict:
    """返回策略结果列表列配置。"""
    from app.services import preferences

    cols = preferences.get_screener_result_columns()
    return {"columns": cols}


@router.put("/preferences/ui-prefs")
def update_ui_prefs(req: UiPrefsIn) -> dict:
    """保存界面偏好 (通知弹窗/声效/语音播报/外链模板/界面缩放)。

    键集合由 services.preferences 的白名单约束 (未知键忽略); 用 extra=allow 的
    透传模型而非逐字段声明, 新增一项只需前后端各加一个键, 不产生两处易漂移的清单。
    """
    from app.services import preferences

    try:
        saved = preferences.set_ui_prefs(req.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ui_prefs": saved}


@router.put("/preferences/screener-result-columns")
def update_screener_result_columns(req: dict) -> dict:
    """保存策略结果列表列配置。"""
    from app.services import preferences

    columns = req.get("columns", [])
    saved = preferences.set_screener_result_columns(columns)
    return {"columns": saved}


@router.put("/preferences/minute-sync")
def update_minute_sync(req: MinuteSyncPrefs) -> dict:
    """保存分钟 K 同步偏好。

    minute_sync_segment_days 为可选:未传(None)时不覆盖现有值,便于开关/天数
    与段大小各自独立更新。
    """
    from app.services import preferences

    days = max(1, min(30, req.minute_sync_days))
    updates: dict = {
        "minute_sync_enabled": req.minute_sync_enabled,
        "minute_sync_days": days,
    }
    if req.minute_sync_segment_days is not None:
        updates["minute_sync_segment_days"] = max(5, min(30, req.minute_sync_segment_days))
    preferences.save(updates)
    return {
        "minute_sync_enabled": req.minute_sync_enabled,
        "minute_sync_days": days,
        "minute_sync_segment_days": preferences.get_minute_sync_segment_days(),
    }


@router.get("/minute-refresh/status")
def minute_refresh_status(request: Request) -> dict:
    """盘中分钟增量刷新服务状态 (开关/能力门控/最近一轮/下一轮)。"""
    svc = getattr(request.app.state, "minute_refresh", None)
    if svc is None:
        return {"available": False}
    return {"available": True, **svc.status()}
