"""用户偏好设置拆分 part 2（原 app.services.preferences）。"""

from __future__ import annotations

from typing import cast

import app.services.preferences as _self

from ._state import (
    _EMAIL_SMTP_DEFAULTS,
    _INTRADAY_REFRESH_INTERVAL_MAX,
    _INTRADAY_REFRESH_INTERVAL_MIN,
    _MINUTE_REFRESH_INTERVAL_MAX,
    _MINUTE_REFRESH_INTERVAL_MIN,
    _SAVE_LOCK,
    MARKET_RECAP_PROMPT_MAX_CHARS,
    MINING_BUDGET_PROFILES,
    PUSH_CHANNELS,
    REVIEW_PUSH_MODES,
    SSE_REFRESH_PAGES_DEFAULT,
)


def set_market_recap_system_prompt(text: str | None) -> str:
    """保存自定义复盘系统提示词。空串/None = 恢复默认模板 (删除该键)。

    超过 MARKET_RECAP_PROMPT_MAX_CHARS 字符时抛 ValueError, 由 API 层转 400。
    """
    cleaned = text.strip() if isinstance(text, str) else ""
    if not cleaned:
        from app.state import delete_preference_keys

        with _SAVE_LOCK:
            delete_preference_keys(["market_recap_system_prompt"])
            _self._invalidate_cache()
        return ""
    if len(cleaned) > MARKET_RECAP_PROMPT_MAX_CHARS:
        raise ValueError(f"复盘提示词过长 ({len(cleaned)} > {MARKET_RECAP_PROMPT_MAX_CHARS} 字符)")
    _self.save({"market_recap_system_prompt": cleaned})
    return cleaned


def get_mining_schedule() -> dict:
    """返回周度自动 mining 配置。历史配置缺字段时默认关闭。"""
    data = _self.load()
    weekday = data.get("mining_schedule_weekday", 4)
    if isinstance(weekday, bool) or not isinstance(weekday, int) or not 0 <= weekday <= 4:
        weekday = 4
    profile = data.get("mining_budget_profile", "balanced")
    if not isinstance(profile, str) or profile not in MINING_BUDGET_PROFILES:
        profile = "balanced"
    enabled = data.get("mining_schedule_enabled", False)
    if not isinstance(enabled, bool):
        enabled = False
    return {
        "mining_schedule_enabled": enabled,
        "mining_schedule_weekday": weekday,
        "mining_budget_profile": profile,
    }


def set_mining_schedule(enabled: bool, weekday: int, profile: str) -> dict:
    """校验并一次写入周度自动 mining 的整组配置。"""
    if isinstance(weekday, bool) or not isinstance(weekday, int) or not 0 <= weekday <= 4:
        raise ValueError("mining schedule weekday must be between 0 and 4")
    if profile not in MINING_BUDGET_PROFILES:
        raise ValueError("mining budget profile must be balanced or strict")
    result = {
        "mining_schedule_enabled": bool(enabled),
        "mining_schedule_weekday": weekday,
        "mining_budget_profile": profile,
    }
    _self.save(result)
    return result


def get_guardian_schedule() -> dict:
    """策略守护 (发布后持续复验) 配置。历史缺键时整体回退默认 (默认关闭)。"""
    data = _self.load()
    enabled = data.get("guardian_schedule_enabled", False)
    if not isinstance(enabled, bool):
        enabled = False
    watch = _self._guardian_pct(data.get("guardian_watch_pct"), 0.2)
    retire = _self._guardian_pct(data.get("guardian_retire_pct"), 0.5)
    if not 0.0 < watch < retire <= 1.0:
        watch, retire = 0.2, 0.5
    k_max = data.get("guardian_k_max", 4)
    if isinstance(k_max, bool) or not isinstance(k_max, int) or not 2 <= k_max <= 4:
        k_max = 4
    slot_limit = data.get("lab_mined_slot_limit", 10)
    if isinstance(slot_limit, bool) or not isinstance(slot_limit, int) or not 0 <= slot_limit <= 50:
        slot_limit = 10
    return {
        "guardian_schedule_enabled": enabled,
        "guardian_watch_pct": watch,
        "guardian_retire_pct": retire,
        "guardian_k_max": k_max,
        "lab_mined_slot_limit": slot_limit,
    }


def set_guardian_schedule(
    enabled: bool,
    watch_pct: float,
    retire_pct: float,
    k_max: int,
    lab_mined_slot_limit: int = 10,
) -> dict:
    """校验并一次写入守护/实验室配置整组 (阈值语义与 backtest.guardian 一致)。"""
    from app.backtest.guardian import GuardianThresholds

    thresholds = GuardianThresholds(watch_pct=watch_pct, retire_pct=retire_pct, k_max=k_max)
    if (
        isinstance(lab_mined_slot_limit, bool)
        or not isinstance(lab_mined_slot_limit, int)
        or not 0 <= lab_mined_slot_limit <= 50
    ):
        raise ValueError("lab mined slot limit must be between 0 and 50 (0 = unlimited)")
    result = {
        "guardian_schedule_enabled": bool(enabled),
        "guardian_watch_pct": thresholds.watch_pct,
        "guardian_retire_pct": thresholds.retire_pct,
        "guardian_k_max": thresholds.k_max,
        "lab_mined_slot_limit": lab_mined_slot_limit,
    }
    _self.save(result)
    return result


def _guardian_pct(value, default: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not 0.0 < float(value) <= 1.0
    ):
        return default
    return float(value)


def get_review_push_channels() -> list[str]:
    """复盘推送渠道(多选) — 选定的外部工具列表, 复盘归档后逐个推送。

    与 review_schedule / 实时行情完全独立, 常驻可单独设置。
    空列表 = 不推送; ['feishu'] = 推送到飞书(复用监控中心全局 feishu_webhook_url/secret)。

    向后兼容:
      - 老多版本单选 review_push_channel=='feishu' → ['feishu']
      - 更老布尔 review_push_enabled==True → ['feishu']
    """
    d = _self.load()
    raw = d.get("review_push_channels")
    if isinstance(raw, list):
        return [c for c in raw if c in PUSH_CHANNELS]
    # 兼容老单选字符串
    if d.get("review_push_channel") == "feishu":
        return ["feishu"]
    # 兼容更老布尔开关
    if d.get("review_push_enabled") is True:
        return ["feishu"]
    return []


def set_review_push_channels(channels: list[str]) -> list[str]:
    """保存复盘推送渠道(多选)。过滤白名单外的值、去重、保序。空列表 = 不推送。"""
    seen: set[str] = set()
    cleaned: list[str] = []
    for c in channels or []:
        if c in PUSH_CHANNELS and c not in seen:
            seen.add(c)
            cleaned.append(c)
    _self.save({"review_push_channels": cleaned})
    return cleaned


def get_review_push_mode() -> str:
    """复盘推送触发方式: auto=归档后自动推; manual=仅显式 push。默认 manual。

    定时复盘与手动保存复盘共用此开关。manual 时定时路径只归档不推送,
    手动路径需 save_report 显式传 push=True 才推。
    """
    mode = _self.load().get("review_push_mode", "manual")
    return mode if mode in REVIEW_PUSH_MODES else "manual"


def set_review_push_mode(mode: str) -> str:
    """保存复盘推送触发方式, 白名单外的值回退 manual。"""
    mode = mode if mode in REVIEW_PUSH_MODES else "manual"
    _self.save({"review_push_mode": mode})
    return mode


# ===== 实时监控 =====

# 页面 SSE 刷新配置: { "watchlist": true, "monitor": true, ... }
# 可刷新的页面列表及其默认值


# ===== 盘中实时行情范围 (独立于盘后管道范围) =====
# 指数不在其中: 展示层固定核心四只 (app.services.index_const), 不开放配置。


def get_realtime_pull_stock() -> bool:
    return _self.load().get("realtime_pull_stock", True)


def get_realtime_pull_etf() -> bool:
    # 老用户兼容: ETF 实时默认关闭，避免升级后请求量/写盘量突然增加。
    return _self.load().get("realtime_pull_etf", False)


def set_realtime_quote_scope(cfg: dict) -> dict:
    updates = {}
    for key in ("realtime_pull_stock", "realtime_pull_etf"):
        if key in cfg and cfg[key] is not None:
            updates[key] = bool(cfg[key])
    if updates:
        _self.save(updates)
    return _self.get_realtime_quote_scope()


def get_realtime_quote_scope() -> dict:
    return {
        "realtime_pull_stock": _self.get_realtime_pull_stock(),
        "realtime_pull_etf": _self.get_realtime_pull_etf(),
    }


def get_sse_refresh_pages() -> dict[str, bool]:
    """返回每个页面的 SSE 刷新开关。"""
    stored = _self.load().get("sse_refresh_pages", {})
    # 合并默认值 (新增页面自动出现)
    result = dict(SSE_REFRESH_PAGES_DEFAULT)
    result.update(stored)
    return result


def set_sse_refresh_pages(pages: dict[str, bool]) -> dict[str, bool]:
    """保存页面 SSE 刷新配置。"""
    _self.save({"sse_refresh_pages": pages})
    return _self.get_sse_refresh_pages()


def get_strategy_monitor_enabled() -> bool:
    """策略告警评估总开关。"""
    return _self.load().get("strategy_monitor_enabled", False)


def get_system_notify_enabled() -> bool:
    """系统通知开关 — 开启后监控告警同时推送到操作系统通知中心。"""
    return _self.load().get("system_notify_enabled", False)


def set_system_notify_enabled(enabled: bool) -> bool:
    """保存系统通知开关。"""
    _self.save({"system_notify_enabled": bool(enabled)})
    return bool(enabled)


def get_feishu_webhook_url() -> str:
    """飞书自定义机器人 Webhook 地址 — 全局共用一处, 所有启用推送的规则都推到这一个群。"""
    return _self.load().get("feishu_webhook_url", "")


def get_feishu_webhook_secret() -> str:
    """飞书自定义机器人签名密钥 — 机器人启用「签名校验」时必填, 留空表示不验签。"""
    return _self.load().get("feishu_webhook_secret", "")


def set_feishu_webhook_url(url: str) -> str:
    """保存飞书 Webhook 地址。传入空串表示清空配置。"""
    _self.save({"feishu_webhook_url": str(url or "").strip()})
    return _self.get_feishu_webhook_url()


def set_feishu_webhook_secret(secret: str) -> str:
    """保存飞书签名密钥。传入空串表示不验签。"""
    _self.save({"feishu_webhook_secret": str(secret or "").strip()})
    return _self.get_feishu_webhook_secret()


def get_wecom_webhook_url() -> str:
    """企业微信群推送 Webhook 地址 — 与飞书并列的第二推送通道。

    存储完整 URL (https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx);
    用户也可只填 key, 由 webhook_adapter.normalize_wecom_url 自动补全。
    """
    return _self.load().get("wecom_webhook_url", "")


def set_wecom_webhook_url(url: str) -> str:
    """保存企业微信 Webhook 地址。传入空串表示清空配置。

    存储时统一补全为完整 URL, 避免后续每次推送都要再判一次。
    """
    from app.services.webhook_adapter import normalize_wecom_url

    _self.save({"wecom_webhook_url": normalize_wecom_url(url)})
    return _self.get_wecom_webhook_url()


def get_custom_webhook_url() -> str:
    """Generic third-party JSON Webhook URL shared by enabled rules and reviews."""
    return str(_self.load().get("custom_webhook_url") or "")


def set_custom_webhook_url(url: str) -> str:
    """Persist or clear the generic third-party JSON Webhook URL."""
    value = str(url or "").strip()
    _self.save({"custom_webhook_url": value})
    return value


def get_email_smtp_config() -> dict:
    """Return non-secret SMTP settings for the email notification channel."""
    raw = _self.load().get("email_smtp_config")
    if not isinstance(raw, dict):
        raw = {}
    security = raw.get("security", _EMAIL_SMTP_DEFAULTS["security"])
    if security not in {"ssl", "starttls", "none"}:
        security = _EMAIL_SMTP_DEFAULTS["security"]
    try:
        port = int(raw.get("port", _EMAIL_SMTP_DEFAULTS["port"]))
    except (TypeError, ValueError):
        port = int(cast(int, _EMAIL_SMTP_DEFAULTS["port"]))
    if not 1 <= port <= 65535:
        port = int(cast(int, _EMAIL_SMTP_DEFAULTS["port"]))
    recipients = raw.get("to_addresses")
    if not isinstance(recipients, list):
        recipients = []
    return {
        "host": str(raw.get("host") or "").strip(),
        "port": port,
        "security": security,
        "username": str(raw.get("username") or "").strip(),
        "from_address": str(raw.get("from_address") or "").strip(),
        "to_addresses": [str(item).strip() for item in recipients if str(item).strip()],
    }


def set_email_smtp_config(config: dict) -> dict:
    """Atomically persist the non-secret SMTP configuration group."""
    normalized = {
        "host": str(config.get("host") or "").strip(),
        "port": int(config.get("port", 465)),
        "security": str(config.get("security") or "ssl"),
        "username": str(config.get("username") or "").strip(),
        "from_address": str(config.get("from_address") or "").strip(),
        "to_addresses": [
            str(item).strip() for item in config.get("to_addresses", []) if str(item).strip()
        ],
    }
    _self.save({"email_smtp_config": normalized})
    return _self.get_email_smtp_config()


# ===== 企业微信智能机器人 (API 模式 / 长连接) =====


def get_wecom_bot_id() -> str:
    """企业微信智能机器人 BotID — 机器人的唯一标识。"""
    return _self.load().get("wecom_bot_id", "")


def set_wecom_bot_id(bot_id: str) -> str:
    """保存智能机器人 BotID。传入空串表示清空。"""
    _self.save({"wecom_bot_id": (bot_id or "").strip()})
    return _self.get_wecom_bot_id()


def get_wecom_bot_secret() -> str:
    """企业微信智能机器人 Secret — 长连接专用密钥。"""
    return _self.load().get("wecom_bot_secret", "")


def set_wecom_bot_secret(secret: str) -> str:
    """保存智能机器人 Secret。传入空串表示清空。"""
    _self.save({"wecom_bot_secret": (secret or "").strip()})
    return _self.get_wecom_bot_secret()


def get_wecom_bot_enabled() -> bool:
    """智能机器人长连接是否启用。默认 False(需用户配置凭证后手动开启)。"""
    return _self.load().get("wecom_bot_enabled", False)


def set_wecom_bot_enabled(enabled: bool) -> bool:
    """保存智能机器人启用状态。"""
    _self.save({"wecom_bot_enabled": bool(enabled)})
    return _self.get_wecom_bot_enabled()


def get_webhook_enabled_default() -> bool:
    """新建监控规则时是否默认勾选推送 (老布尔, 已由 webhook_default_channels 取代)。

    保留向后兼容: 读取 webhook_default_channels 非空时返回 True。
    """
    return bool(_self.get_webhook_default_channels())


def set_webhook_enabled_default(enabled: bool) -> bool:
    """保存推送默认勾选态 (老布尔兼容入口)。

    新数据模型为渠道数组; 此处把老布尔转译: True→['feishu','wecom'], False→[]。
    """
    _self.set_webhook_default_channels(["feishu", "wecom"] if enabled else [])
    return _self.get_webhook_enabled_default()


def get_webhook_default_channels() -> list[str]:
    """新建监控规则时默认勾选的推送渠道 (多选)。

    空列表 = 新建规则默认不推送; ['feishu'] = 默认推飞书。
    此默认值供规则编辑器新建规则时预填, 单条规则仍可独立修改。

    向后兼容: 老版本只有布尔 webhook_enabled_default (勾选即飞书+企业微信双推),
    这里把 True 迁移为 ['feishu','wecom'], 还原当时的实际行为。
    """
    d = _self.load()
    raw = d.get("webhook_default_channels")
    if isinstance(raw, list):
        return [c for c in raw if c in PUSH_CHANNELS]
    # 兼容老布尔开关 (勾选即双推)
    if d.get("webhook_enabled_default") is True:
        return ["feishu", "wecom"]
    return []


def set_webhook_default_channels(channels: list[str]) -> list[str]:
    """保存新建规则默认推送渠道 (多选)。过滤白名单外、去重、保序。空列表 = 不推送。"""
    seen: set[str] = set()
    cleaned: list[str] = []
    for c in channels or []:
        if c in PUSH_CHANNELS and c not in seen:
            seen.add(c)
            cleaned.append(c)
    _self.save({"webhook_default_channels": cleaned})
    return cleaned


def get_screener_auto_run() -> bool:
    """策略页进入时是否自动运行所有策略 (获取命中数)。默认开。"""
    return _self.load().get("screener_auto_run", True)


def get_assistant_auto_approve_actions() -> bool:
    """AI 助手的动作工具是否免确认直接执行。默认关。

    默认关是安全底线: 打开后 create_signal_strategy / run_backtest /
    add_to_watchlist / sync_data 会不经用户点按就执行 (写信号文件、改自选、
    触发数据补全)。存量用户不能因为升级而静默变成全自动。
    只影响动作确认, 不影响轮次检查点 (那是防跑飞的安全阀, 见 chat_service)。
    """
    return bool(_self.load().get("assistant_auto_approve_actions", False))


def set_assistant_auto_approve_actions(enabled: bool) -> bool:
    """保存 AI 助手免确认开关。"""
    _self.save({"assistant_auto_approve_actions": bool(enabled)})
    return bool(enabled)


def get_assistant_mask_enabled() -> bool:
    """AI 助手打开时是否在页面主体上盖毛玻璃遮罩 (点击遮罩关闭助手)。默认开。

    纯前端视觉/交互偏好, 后端只负责持久化; 关闭后恢复非模态体验
    (页面主体可正常交互, 边看行情边问)。
    """
    return bool(_self.load().get("assistant_mask_enabled", True))


def set_assistant_mask_enabled(enabled: bool) -> bool:
    """保存 AI 助手毛玻璃遮罩开关。"""
    _self.save({"assistant_mask_enabled": bool(enabled)})
    return _self.get_assistant_mask_enabled()


def get_strategy_monitor_ids() -> list[str]:
    """返回监控池中的策略 ID。"""
    return _self.load().get("strategy_monitor_ids", [])


def set_realtime_monitor_config(cfg: dict) -> dict:
    """批量更新实时监控配置。"""
    updates = {}
    if "sse_refresh_pages" in cfg:
        updates["sse_refresh_pages"] = cfg["sse_refresh_pages"]
    if "strategy_monitor_enabled" in cfg:
        updates["strategy_monitor_enabled"] = cfg["strategy_monitor_enabled"]
    if "strategy_monitor_ids" in cfg:
        updates["strategy_monitor_ids"] = cfg["strategy_monitor_ids"]
    if "screener_auto_run" in cfg:
        updates["screener_auto_run"] = bool(cfg["screener_auto_run"])
    if "minute_intraday_refresh" in cfg:
        updates["minute_intraday_refresh"] = bool(cfg["minute_intraday_refresh"])
    if "minute_intraday_refresh_interval" in cfg:
        # clamp 到 [5, 60], 与 getter 一致, 防前端传越界值
        updates["minute_intraday_refresh_interval"] = max(
            _INTRADAY_REFRESH_INTERVAL_MIN,
            min(_INTRADAY_REFRESH_INTERVAL_MAX, int(cfg["minute_intraday_refresh_interval"])),
        )
    if "minute_refresh_enabled" in cfg:
        updates["minute_refresh_enabled"] = bool(cfg["minute_refresh_enabled"])
    if "minute_refresh_interval" in cfg:
        # clamp 到 [3, 120], 与 getter 一致, 防前端传越界值
        updates["minute_refresh_interval"] = max(
            _MINUTE_REFRESH_INTERVAL_MIN,
            min(_MINUTE_REFRESH_INTERVAL_MAX, int(cfg["minute_refresh_interval"])),
        )
    if "monitor_ext_fields" in cfg:
        raw = cfg["monitor_ext_fields"] or {}
        updates["monitor_ext_fields"] = {
            "concept": _self._normalize_ext_field(raw.get("concept")),
            "industry": _self._normalize_ext_field(raw.get("industry")),
        }
    if updates:
        _self.save(updates)
    return _self.get_realtime_monitor_config()


def get_realtime_monitor_config() -> dict:
    """返回完整的实时监控配置。"""
    return {
        "sse_refresh_pages": _self.get_sse_refresh_pages(),
        "strategy_monitor_enabled": _self.get_strategy_monitor_enabled(),
        "strategy_monitor_ids": _self.get_strategy_monitor_ids(),
        "screener_auto_run": _self.get_screener_auto_run(),
        "minute_intraday_refresh": _self.get_minute_intraday_refresh(),
        "minute_intraday_refresh_interval": _self.get_minute_intraday_refresh_interval(),
        "minute_refresh_enabled": _self.get_minute_refresh_enabled(),
        "minute_refresh_interval": _self.get_minute_refresh_interval(),
        "monitor_ext_fields": _self.get_monitor_ext_fields(),
    }


def get_nav_order() -> list[str]:
    """返回左侧菜单的自定义排序（内置页面 path + 扩展分析菜单 id）。"""
    return _self.load().get("nav_order", [])


def get_nav_layout() -> dict | None:
    from app.services.nav_layout import validate_nav_layout

    raw = _self.load().get("nav_layout")
    if raw is None:
        return None
    try:
        return validate_nav_layout(raw)
    except ValueError:
        return None


def set_nav_layout(value: dict | None, hidden: list[str] | None = None) -> dict | None:
    from app.services.nav_layout import flat_nav_order, validate_nav_layout

    cleaned = validate_nav_layout(value) if value is not None else None
    with _self.mutation():
        raw = _self.load().get("nav_layout")
        if isinstance(raw, dict) and raw.get("v", 1) != 1:
            raise ValueError("导航布局由更高版本创建，请升级后再修改")
        updates: dict = {"nav_layout": cleaned}
        if cleaned is not None:
            # 平铺副本供旧版本回退，归属和顺序始终在同一次原子写入中更新。
            updates["nav_order"] = flat_nav_order(cleaned)
        if hidden is not None:
            updates["nav_hidden"] = list(dict.fromkeys(x for x in hidden if x != "/"))
        _self.save(updates)
    return cleaned


def set_nav_order(order: list[str]) -> list[str]:
    """保存左侧菜单排序。"""
    _self.save({"nav_order": order})
    return _self.get_nav_order()


def get_nav_hidden() -> list[str]:
    """返回左侧菜单中隐藏的项 id 列表。"""
    return _self.load().get("nav_hidden", [])


def set_nav_hidden(hidden: list[str]) -> list[str]:
    """保存左侧菜单隐藏项。"""
    _self.save({"nav_hidden": hidden})
    return _self.get_nav_hidden()


def get_watchlist_columns() -> list[dict] | None:
    """返回自选列表列配置。"""
    return _self.load().get("watchlist_columns")


def set_watchlist_columns(columns: list[dict]) -> list[dict]:
    """保存自选列表列配置。"""
    _self.save({"watchlist_columns": columns})
    return columns


def get_screener_result_columns() -> list[dict] | None:
    """返回策略结果列表列配置。"""
    return _self.load().get("screener_result_columns")


def set_screener_result_columns(columns: list[dict]) -> list[dict]:
    """保存策略结果列表列配置。"""
    _self.save({"screener_result_columns": columns})
    return columns


# ===== 首次使用引导 =====


def get_onboarding_completed() -> bool:
    """是否已完成首次使用向导。默认 False（新用户）。"""
    return bool(_self.load().get("onboarding_completed", False))


def set_onboarding_completed(done: bool = True) -> bool:
    """标记首次使用向导完成状态。"""
    _self.save({"onboarding_completed": bool(done)})
    return bool(done)


# ===== 财务数据同步时间(持久化,重启不丢失) =====
# 结构: { "metrics": "2026-06-25T10:00:00+08:00", "income": ..., ... }


def get_financial_sync_times() -> dict[str, str]:
    """返回各财务表的最后同步时间(ISO 字符串)。未同步过的表不在返回值中。"""
    return _self.load().get("financial_sync_times", {}) or {}


def set_financial_sync_time(table: str, iso_ts: str) -> None:
    """更新单张财务表的最后同步时间(合并写入,不清除其他表)。"""
    times = _self.get_financial_sync_times()
    times[table] = iso_ts
    _self.save({"financial_sync_times": times})


# ---------------------------------------------------------------- 看板自定义布局
# blob 形如 {"v": 1, "items": [{"i","t","x","y","w","h","p?"}, ...]}; None = 未自定义(前端用内置默认布局)。


def get_dashboard_layout() -> dict | None:
    """看板自定义布局 blob; None = 未自定义。"""
    raw = _self.load().get("dashboard_layout")
    return raw if isinstance(raw, dict) else None
