"""settings API 路由分组 _routes_notifications（由 settings.py 机械拆分，顺序不变）。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app import secrets_store

from ._common import (
    AssistantAutoApproveIn,
    AssistantMaskIn,
    CustomWebhookPrefsIn,
    EmailSmtpPrefsIn,
    EmotionScoreConfigIn,
    FeishuWebhookPrefsIn,
    MainlineFilterIn,
    PipelineIndexSymbolsIn,
    PipelinePullTypesIn,
    PipelineRegimeEnabledIn,
    QuoteIntervalIn,
    RealtimeMonitorConfigIn,
    RealtimeQuoteScopePrefs,
    RealtimeQuotesPrefs,
    RegimeBatchParamsIn,
    SystemNotifyPrefsIn,
    WatchlistGroupsInNavPrefs,
    WebhookDefaultChannelsIn,
    WebhookEnabledDefaultIn,
    WebhookTestIn,
    WecomBotPrefsIn,
    WecomBotToggleIn,
    WecomWebhookPrefsIn,
    _apply_realtime_prefs,
    _check_realtime_enable_gates,
    _force_disable_realtime_quotes,
)

router = APIRouter(prefix="/api/settings")


@router.put("/preferences/realtime-quotes")
def update_realtime_quotes(req: RealtimeQuotesPrefs, request: Request) -> dict:
    """保存全局实时行情开关。

    无实时能力的档位(TickFlow none/free)开关回弹强制关闭;
    starter+ 或自定义实时源(如 fuyao)为全市场实时。前端据此把开关置灰 / 回弹。
    """
    from app.services import preferences

    qs = getattr(request.app.state, "quote_service", None)
    depth_svc = getattr(request.app.state, "depth_service", None)
    allowed = qs.is_realtime_allowed() if qs else True
    if req.realtime_quotes_enabled and not allowed:
        return _force_disable_realtime_quotes(preferences, qs, depth_svc)
    if req.realtime_quotes_enabled:
        _check_realtime_enable_gates(req, request, qs)
    _apply_realtime_prefs(req, preferences, qs, depth_svc)
    return {"realtime_quotes_enabled": req.realtime_quotes_enabled, "realtime_allowed": allowed}


@router.put("/preferences/realtime-quote-scope")
def update_realtime_quote_scope(req: RealtimeQuoteScopePrefs) -> dict:
    """保存盘中实时行情范围；独立于盘后管道范围。"""
    from app.services import preferences

    cfg = req.model_dump(exclude_none=True)
    return preferences.set_realtime_quote_scope(cfg)


@router.put("/preferences/assistant-auto-approve")
def update_assistant_auto_approve(req: AssistantAutoApproveIn) -> dict:
    """保存 AI 助手「动作工具免确认」开关。

    打开后 create_signal_strategy / run_backtest / add_to_watchlist / sync_data
    不再弹确认卡, 直接执行。默认关闭; 轮次检查点不受影响。
    """
    from app.services import preferences

    saved = preferences.set_assistant_auto_approve_actions(req.enabled)
    return {"assistant_auto_approve_actions": saved}


@router.put("/preferences/assistant-mask")
def update_assistant_mask(req: AssistantMaskIn) -> dict:
    """保存 AI 助手「毛玻璃遮罩」开关 (默认开启)。

    开启时助手抽屉背后覆盖毛玻璃遮罩, 点击遮罩关闭助手;
    关闭则恢复非模态体验 (页面主体可正常交互)。纯视觉偏好, 无副作用。
    """
    from app.services import preferences

    saved = preferences.set_assistant_mask_enabled(req.enabled)
    return {"assistant_mask_enabled": saved}


@router.put("/preferences/watchlist-groups-in-nav")
def update_watchlist_groups_in_nav(req: WatchlistGroupsInNavPrefs) -> dict:
    """保存自选分组是否显示在侧边栏开关。"""
    from app.services import preferences

    preferences.save({"watchlist_groups_in_nav": req.watchlist_groups_in_nav})
    return {"watchlist_groups_in_nav": req.watchlist_groups_in_nav}


@router.put("/preferences/realtime-monitor")
def update_realtime_monitor_config(req: RealtimeMonitorConfigIn, request: Request) -> dict:
    """更新实时监控配置。策略监控统一迁移为 MonitorRule,由监控引擎评估。"""
    from app.services import preferences

    cfg = req.model_dump(exclude_none=True)
    result = preferences.set_realtime_monitor_config(cfg)

    # 策略监控开关/池变化 → 同步迁移为 type=strategy 规则 + reload 引擎
    if req.strategy_monitor_ids is not None or req.strategy_monitor_enabled is not None:
        monitor_engine = getattr(request.app.state, "monitor_engine", None)
        strategy_engine = getattr(request.app.state, "strategy_engine", None)
        data_dir = request.app.state.repo.store.data_dir
        if monitor_engine is not None and strategy_engine is not None:
            from app.strategy import monitor_rules as mr_store

            try:
                if preferences.get_strategy_monitor_enabled():
                    ids = preferences.get_strategy_monitor_ids()
                    names = {s.id: s.name for s in strategy_engine.list_strategies()}
                    mr_store.migrate_strategy_monitors(data_dir, ids, names)
                else:
                    # 关闭策略监控: 停用所有策略规则
                    mr_store.migrate_strategy_monitors(data_dir, [], {})
                # reload 规则到引擎
                monitor_engine.set_rules(mr_store.load_all(data_dir))
            except Exception:  # noqa: BLE001 - 重设监控规则失败不阻断设置保存
                pass

    return result


@router.put("/preferences/pipeline-pull-types")
def update_pipeline_pull_types(req: PipelinePullTypesIn) -> dict:
    """更新盘后管道拉取内容开关。"""
    from app.services import preferences

    cfg = req.model_dump(exclude_none=True)
    return preferences.set_pipeline_pull_types(cfg)


@router.put("/preferences/pipeline-regime-enabled")
def update_pipeline_regime_enabled(req: PipelineRegimeEnabledIn) -> dict:
    """更新盘后管道 regime 自动计算开关。"""
    from app.services import preferences

    preferences.save({"pipeline_regime_enabled": bool(req.pipeline_regime_enabled)})
    return {"pipeline_regime_enabled": preferences.get_pipeline_regime_enabled()}


@router.put("/preferences/regime-batch-params")
def update_regime_batch_params(req: RegimeBatchParamsIn) -> dict:
    """更新 regime 分批参数。仅在传入字段时保存对应项(支持部分更新)。"""
    from app.services import preferences

    updates: dict = {}
    if req.batch_days is not None:
        updates["regime_batch_days"] = req.batch_days
    if req.warmup_days is not None:
        updates["regime_warmup_days"] = req.warmup_days
    if updates:
        preferences.save(updates)
    return {
        "regime_batch_days": preferences.get_regime_batch_days(),
        "regime_warmup_days": preferences.get_regime_warmup_days(),
    }


@router.get("/emotion-score/config")
def get_emotion_score_config() -> dict:
    """情绪评分口径: 当前配置 + 出厂默认 + 维度顺序 + 档位名。

    看板「情绪雷达」卡片的配置对话框一次拉全 —— 前端不硬编码任何权重/区间/
    阈值字面量, 表单全部由本接口返回的结构渲染。
    """
    from app.services import preferences
    from app.services.emotion_score import public_config

    payload = public_config(preferences.get_emotion_score_config())
    payload["customized"] = preferences.get_emotion_score_config() is not None
    return payload


@router.put("/emotion-score/config")
def update_emotion_score_config(req: EmotionScoreConfigIn) -> dict:
    """保存情绪评分口径; config 传 null/{} = 恢复出厂。

    形状校验在 preferences 层 (经 emotion_score.merge_config), 值的合法性
    (负权重/区间倒置/阈值乱序) 由计算时归一化兜底, 故此处不做逐字段白名单。
    """
    from app.services import preferences
    from app.services.emotion_score import public_config

    saved = preferences.set_emotion_score_config(req.config)
    payload = public_config(saved)
    payload["customized"] = saved is not None
    return payload


@router.put("/preferences/mainline-filter")
def update_mainline_filter(req: MainlineFilterIn) -> dict:
    """更新市场主线过滤配置。部分更新; 修改后需重算主线(POST /api/regime/mainline/recompute)生效。

    exclude_st 同步控制市场环境(regime)统计口径 — 切换后需全量重算 regime。
    """
    from app.services import preferences

    payload = req.model_dump()
    return preferences.set_mainline_filter_config(payload)


@router.put("/preferences/pipeline-index-symbols")
def update_pipeline_index_symbols(req: PipelineIndexSymbolsIn) -> dict:
    """保存指数自定义拉取代码。"""
    from app.services import preferences

    symbols = preferences.set_pipeline_index_symbols(req.symbols)
    return {"pipeline_index_symbols": symbols}


@router.put("/preferences/system-notify")
def update_system_notify(req: SystemNotifyPrefsIn) -> dict:
    """系统通知开关 — 开启后监控告警同时推送到操作系统通知中心。

    纯偏好, 无副作用 (不像策略监控要迁移规则), 直接落盘即可。
    quote_service 在每轮告警评估时读此开关决定是否发系统通知。
    """
    from app.services import preferences

    saved = preferences.set_system_notify_enabled(req.enabled)
    return {"system_notify_enabled": saved}


@router.put("/preferences/feishu-webhook")
def update_feishu_webhook(req: FeishuWebhookPrefsIn) -> dict:
    """飞书 Webhook 地址 + 签名密钥 — 全局一处配置, 所有启用推送的监控规则共用。

    - url: 传入空串表示清空配置; 非空则需为合法的飞书自定义机器人地址。
    - secret: 机器人启用了「签名校验」时填密钥, 留空表示不验签。
    """
    from app.services import preferences, webhook_adapter

    url = (req.url or "").strip()
    if url and not webhook_adapter.is_valid_feishu_url(url):
        raise HTTPException(
            status_code=400,
            detail="Webhook 地址非法, 需为飞书自定义机器人地址 "
            "(https://open.feishu.cn/open-apis/bot/v2/hook/...)",
        )
    saved_url = preferences.set_feishu_webhook_url(url)
    saved_secret = preferences.set_feishu_webhook_secret((req.secret or "").strip())
    return {"feishu_webhook_url": saved_url, "feishu_webhook_secret": saved_secret}


@router.put("/preferences/wecom-webhook")
def update_wecom_webhook(req: WecomWebhookPrefsIn) -> dict:
    """企业微信群推送 Webhook 地址 — 与飞书并列的第二推送通道。

    - url: 传入空串表示清空配置; 非空需为合法企业微信群推送 Webhook 地址, 或纯 key。
    - 用户可只填 key (webhook/send?key=xxx 的 xxx 部分), 后端自动补全为完整 URL。
    """
    from app.services import preferences, webhook_adapter

    url = (req.url or "").strip()
    if url and not webhook_adapter.is_valid_wecom_url(url):
        raise HTTPException(
            status_code=400,
            detail="Webhook 地址非法, 需为企业微信群推送 Webhook 地址 "
            "(https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=... 或纯 key)",
        )
    saved_url = preferences.set_wecom_webhook_url(url)
    return {"wecom_webhook_url": saved_url}


@router.put("/preferences/custom-webhook")
def update_custom_webhook(req: CustomWebhookPrefsIn) -> dict:
    """Configure the generic third-party JSON webhook and optional HMAC secret."""
    from app.services import preferences, webhook_adapter

    url = (req.url or "").strip()
    if url and not webhook_adapter.is_valid_custom_url(url):
        raise HTTPException(status_code=400, detail="Webhook 地址必须是完整的 HTTP(S) URL")
    saved_url = preferences.set_custom_webhook_url(url)
    if not saved_url:
        secrets_store.set_custom_webhook_secret("")
    elif req.secret is not None:
        secrets_store.set_custom_webhook_secret(req.secret)
    return {
        "custom_webhook_url": saved_url,
        "custom_webhook_secret_set": bool(secrets_store.get_custom_webhook_secret()),
    }


@router.put("/preferences/email-smtp")
def update_email_smtp(req: EmailSmtpPrefsIn) -> dict:
    """Configure the SMTP transport shared by monitor alerts and review reports."""
    from app.services import email_adapter, preferences

    host = (req.host or "").strip()
    username = (req.username or "").strip()
    from_address = (req.from_address or username).strip()
    recipients = list(dict.fromkeys(item.strip() for item in req.to_addresses if item.strip()))
    if host:
        if not from_address or not email_adapter.is_valid_email(from_address):
            raise HTTPException(status_code=400, detail="请填写有效的发件人邮箱")
        if not recipients or any(not email_adapter.is_valid_email(item) for item in recipients):
            raise HTTPException(status_code=400, detail="请至少填写一个有效的收件人邮箱")
        effective_password = (
            secrets_store.get_email_smtp_password() if req.password is None else req.password
        )
        if username and not effective_password:
            raise HTTPException(
                status_code=400, detail="已填写 SMTP 登录用户名, 请同时填写密码或授权码"
            )
    else:
        username = ""
        from_address = ""
        recipients = []

    config = preferences.set_email_smtp_config(
        {
            "host": host,
            "port": req.port,
            "security": req.security,
            "username": username,
            "from_address": from_address,
            "to_addresses": recipients,
        }
    )
    if not host or not username:
        secrets_store.set_email_smtp_password("")
    elif req.password is not None:
        secrets_store.set_email_smtp_password(req.password)
    return {
        "email_smtp_config": config,
        "email_smtp_password_set": bool(secrets_store.get_email_smtp_password()),
    }


@router.post("/preferences/webhook-test")
def test_webhook(req: WebhookTestIn) -> dict:
    """向已保存的 Webhook 地址发送一条测试消息，验证配置是否正确。

    只测试已保存的配置（与生产推送同源），不测试未保存草稿。
    未配置 / 地址非法 / 发送失败均返回 HTTP 200 + {ok: False}，
    前端统一读 detail 渲染绿/红，不抛 400。
    """
    from app.services import preferences, webhook_adapter

    title = "TickFlow Stock Panel 推送测试"
    body = "如果你看到这条消息，说明推送配置正确 🎉"

    if req.channel == "feishu":
        url = preferences.get_feishu_webhook_url()
        if not url:
            return {"ok": False, "detail": "尚未配置飞书 Webhook，请先保存"}
        if not webhook_adapter.is_valid_feishu_url(url):
            return {"ok": False, "detail": "已保存的飞书 Webhook 地址非法，请重新保存"}
        secret = preferences.get_feishu_webhook_secret()
        # 诊断用途单次尝试: 失败即返回, 不等生产退避重试 (~17s)
        ok = webhook_adapter.send_feishu(url, title, body, secret, max_attempts=1)
    elif req.channel == "wecom":
        url = preferences.get_wecom_webhook_url()
        if not url:
            return {"ok": False, "detail": "尚未配置企业微信 Webhook，请先保存"}
        if not webhook_adapter.is_valid_wecom_url(url):
            return {"ok": False, "detail": "已保存的企业微信 Webhook 地址非法，请重新保存"}
        ok = webhook_adapter.send_wecom(url, title, body)
    elif req.channel == "custom":
        url = preferences.get_custom_webhook_url()
        if not url:
            return {"ok": False, "detail": "尚未配置第三方 Webhook, 请先保存"}
        if not webhook_adapter.is_valid_custom_url(url):
            return {"ok": False, "detail": "已保存的第三方 Webhook 地址非法, 请重新保存"}
        ok = webhook_adapter.send_custom(
            url,
            title,
            body,
            event_type="test",
            secret=secrets_store.get_custom_webhook_secret(),
            max_attempts=1,
        )
    else:  # email
        from app.services import email_adapter

        config = preferences.get_email_smtp_config()
        if not email_adapter.is_configured(config):
            return {"ok": False, "detail": "尚未完整配置邮件 SMTP, 请先保存"}
        ok = email_adapter.send_email(
            config,
            secrets_store.get_email_smtp_password(),
            title,
            body,
            max_attempts=1,
        )

    if ok:
        return {"ok": True, "detail": "测试消息已发送, 请检查对应接收端"}
    return {"ok": False, "detail": "推送失败：网络不可达或地址/密钥不正确，详情见后端日志"}


@router.put("/preferences/wecom-bot")
def update_wecom_bot(req: WecomBotPrefsIn, request: Request) -> dict:
    """企业微信智能机器人(BotID + Secret)配置 — 长连接通道。

    保存凭证后立即重建连接(stop→start), 因每机器人仅允许 1 条长连接。
    - bot_id/secret 均传空串表示清空配置并断开连接。
    - enabled 控制是否启用长连接(凭证齐全时生效)。
    """
    from app.services import preferences

    bot_id = (req.bot_id or "").strip()
    secret = (req.secret or "").strip()
    preferences.set_wecom_bot_id(bot_id)
    preferences.set_wecom_bot_secret(secret)
    # 凭证不齐时强制关闭(避免 enabled=True 但连不上)
    enabled = req.enabled and bool(bot_id) and bool(secret)
    preferences.set_wecom_bot_enabled(enabled)

    # 立即应用: 重建连接
    bot_svc = getattr(request.app.state, "wecom_bot_service", None)
    status: dict = {}
    if bot_svc:
        bot_svc.apply_credential_change()
        status = bot_svc.status()
    return {
        "wecom_bot_id": preferences.get_wecom_bot_id(),
        "wecom_bot_secret": preferences.get_wecom_bot_secret(),
        "wecom_bot_enabled": preferences.get_wecom_bot_enabled(),
        "wecom_bot_status": status,
    }


@router.put("/preferences/wecom-bot-toggle")
def toggle_wecom_bot(req: WecomBotToggleIn, request: Request) -> dict:
    """独立开关: 启用/禁用智能机器人长连接(不改动凭证)。

    凭证不齐时强制返回未启用(无法连接)。
    """
    from app.services import preferences

    bot_id = preferences.get_wecom_bot_id()
    secret = preferences.get_wecom_bot_secret()
    enabled = req.enabled and bool(bot_id) and bool(secret)
    preferences.set_wecom_bot_enabled(enabled)

    bot_svc = getattr(request.app.state, "wecom_bot_service", None)
    status: dict = {}
    if bot_svc:
        bot_svc.apply_credential_change()
        status = bot_svc.status()
    return {"wecom_bot_enabled": enabled, "wecom_bot_status": status}


@router.put("/preferences/webhook-enabled-default")
def update_webhook_enabled_default(req: WebhookEnabledDefaultIn) -> dict:
    """新建监控规则时是否默认勾选推送 (老布尔接口, 兼容旧前端)。

    新数据模型为渠道数组 (webhook_default_channels); 此处转译为
    True→['feishu','wecom'], False→[]。新前端请改用 webhook-default-channels 接口。
    """
    from app.services import preferences

    saved = preferences.set_webhook_enabled_default(req.enabled)
    return {"webhook_enabled_default": saved}


@router.put("/preferences/webhook-default-channels")
def update_webhook_default_channels(req: WebhookDefaultChannelsIn) -> dict:
    """新建监控规则时默认勾选的推送渠道 (多选)。

    作为新建规则的默认推送渠道预填, 单条规则仍可在规则编辑页独立修改。
    空数组=默认不推送。白名单外的渠道会被过滤掉。
    """
    from app.services import preferences

    saved = preferences.set_webhook_default_channels(req.channels)
    return {"webhook_default_channels": saved}


@router.put("/preferences/quote-interval")
def update_quote_interval(req: QuoteIntervalIn, request: Request) -> dict:
    """更新行情轮询间隔。按档位自动 clamp。"""
    qs = getattr(request.app.state, "quote_service", None)
    if not qs:
        return {"interval": req.interval, "min_interval": 6.0, "max_interval": 60.0}
    clamped = qs.set_interval(req.interval)
    return {
        "interval": clamped,
        "min_interval": qs.get_min_interval(),
        "max_interval": qs.MAX_INTERVAL,
    }


@router.get("/preferences/quote-interval")
def get_quote_interval(request: Request) -> dict:
    """获取当前行情轮询间隔和档位限制。"""
    qs = getattr(request.app.state, "quote_service", None)
    if not qs:
        return {"interval": 6.0, "min_interval": 6.0, "max_interval": 60.0}
    return {
        "interval": qs._interval,
        "min_interval": qs.get_min_interval(),
        "max_interval": qs.MAX_INTERVAL,
    }
