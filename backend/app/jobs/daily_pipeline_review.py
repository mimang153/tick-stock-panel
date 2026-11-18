"""daily_pipeline_review: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


async def _run_scheduled_review(repo) -> None:
    """定时复盘 job: 流式生成复盘 → 实时推 SSE(开着页面可见) → 落盘归档 → 推飞书。

    与手动「生成复盘」体验一致: 流式事件经 quote_service.push_review_event →
    /api/intraday/stream 的 review_progress 事件 → 前端 reviewStore, 用户开着复盘页
    即可看到报告边生成边显示, 切走再回来也能看到生成中/已生成。
    LLM 偶发断流(peer closed connection)时自动重试最多 2 次。
    任何异常都吞掉只记日志, 绝不影响调度器主循环。
    """
    import json

    import app.jobs.daily_pipeline as _root

    try:
        from app import secrets_store as ss
        from app.services import market_recap_reports

        # AI Key 未配置时跳过(避免每日报错刷日志)
        if not ss.get_ai_key():
            _root.logger.info("scheduled review skipped: AI key not configured")
            return

        app_state = _root._get_app_state()
        quote_service = getattr(app_state, "quote_service", None) if app_state else None
        depth_service = getattr(app_state, "depth_service", None) if app_state else None

        content, meta = await _root._stream_review_with_retry(repo, quote_service, depth_service)
        if not content:
            _root.logger.warning("scheduled review produced no content (meta=%s)", meta)
            # 通知前端进入 error 态(若有页面在听)
            if quote_service:
                quote_service.push_review_event(
                    json.dumps(
                        {"type": "error", "message": "复盘生成失败,请稍后手动重试"},
                        ensure_ascii=False,
                    )
                )
            return

        # 落盘: 与手动生成完全相同的归档格式
        market_recap_reports.save_report(
            {
                "as_of": meta.get("as_of"),
                "focus": "",
                "content": content,
                "summary": meta.get("summary", ""),
                "emotion_score": meta.get("emotion_score"),
                "emotion_label": meta.get("emotion_label", ""),
            }
        )
        _root.logger.info("scheduled review saved: as_of=%s", meta.get("as_of"))

        # 通知前端: 生成完成且已归档(archived=true 让前端只刷新列表, 不重复归档)
        if quote_service:
            quote_service.push_review_event(
                json.dumps({"type": "done", "archived": True}, ensure_ascii=False)
            )

        # 推送门控: review_push_mode=manual 时定时复盘只归档不推送,
        # 由用户对当日报告显式确认后才推; auto 时保持既有自动推送行为。
        # 失败静默降级, 不影响已归档的报告。
        if _root._prefs.get_review_push_mode() == "auto":
            _root._maybe_push_review(content, meta)
    except Exception as e:  # noqa: BLE001, F841 - 兜底记录并通知前端，异常变量保留用于调试
        _root.logger.exception("scheduled review failed")
        # 兜底: 异常时通知前端停止「生成中」状态, 避免页面卡在 streaming
        try:
            app_state = _root._get_app_state()
            qs = getattr(app_state, "quote_service", None) if app_state else None
            if qs:
                import json as _json

                qs.push_review_event(
                    _json.dumps(
                        {"type": "error", "message": "复盘生成异常,请稍后手动重试"},
                        ensure_ascii=False,
                    )
                )
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            pass


async def _stream_review_with_retry(repo, quote_service, depth_service) -> tuple[str, dict]:
    """流式生成复盘, 每个事件推 SSE + 累积内容。LLM 断流时最多重试 2 次。

    返回 (content, meta)。重试时推一个 retry 事件让前端清空已累积内容重新开始。
    成功(收到 done/无 error)或耗尽重试后返回。
    """
    import asyncio
    import json

    import app.jobs.daily_pipeline as _root
    from app.services.market_recap import recap_market_stream

    max_attempts = 3  # 初次 + 2 次重试
    last_meta: dict = {}
    content_parts: list[str] = []

    for attempt in range(1, max_attempts + 1):
        content_parts = []  # 每次重试重新累积
        failed = False
        try:
            async for evt_json in recap_market_stream(repo, quote_service, depth_service):
                evt = json.loads(evt_json)
                t = evt.get("type")

                # 推给前端(让开着页面的用户实时看到, 与手动一致)
                if quote_service:
                    quote_service.push_review_event(evt_json)

                if t == "meta":
                    last_meta = evt
                elif t == "delta" and evt.get("content"):
                    content_parts.append(evt["content"])
                elif t == "error":
                    failed = True
                    _root.logger.warning(
                        "scheduled review stream error (attempt %d/%d): %s",
                        attempt,
                        max_attempts,
                        evt.get("message"),
                    )
                    break  # 触发重试
                elif t == "done":
                    # 正常完成
                    return "".join(content_parts), last_meta
            # 流自然结束(无 done 事件)且有内容, 视为成功
            if content_parts and not failed:
                return "".join(content_parts), last_meta
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            # LLM 断流等异常(httpx.RemoteProtocolError)落到这里
            failed = True
            _root.logger.warning(
                "scheduled review stream exception (attempt %d/%d): %s", attempt, max_attempts, e
            )

        # 失败: 决定是否重试
        if attempt < max_attempts:
            _root.logger.info(
                "scheduled review retrying in 3s (attempt %d → %d)", attempt, attempt + 1
            )
            # 通知前端: 即将重试, 清空已累积内容重新开始
            if quote_service:
                quote_service.push_review_event(
                    json.dumps({"type": "retry", "attempt": attempt + 1}, ensure_ascii=False)
                )
            await asyncio.sleep(3)

    # 耗尽重试, 返回已累积内容(可能为空)和最后 meta
    return "".join(content_parts), last_meta


def _maybe_push_review(content: str, meta: dict) -> None:
    """复盘报告归档后, 按 review_push_channels 选定的外部工具逐个推送完整报告。

    定时生成与手动生成共用本函数 (手动归档端点 POST /api/market-recap/reports 也会调用)。
    channels 为空则不推送; 复用监控中心的全局外部渠道配置。
    推送失败静默降级 (Webhook 是辅助通道), 不影响已归档的报告。
    """
    import app.jobs.daily_pipeline as _root

    try:
        from app import secrets_store
        from app.services import email_adapter, preferences, webhook_adapter

        channels = preferences.get_review_push_channels()
        if not channels:
            return

        emotion = f"{meta.get('emotion_label') or ''}".strip()
        as_of = meta.get("as_of") or ""
        subtitle = as_of + (f" · 情绪 {emotion}" if emotion else "")

        for ch in channels:
            if ch == "feishu":
                url = preferences.get_feishu_webhook_url()
                if not url:
                    _root.logger.info("review push(feishu) skipped: webhook not configured")
                    continue
                secret = preferences.get_feishu_webhook_secret()
                ok = webhook_adapter.send_feishu_card(url, "每日复盘", subtitle, content, secret)
                _root.logger.info("review push(feishu) %s", "sent" if ok else "failed")
            elif ch == "wecom":
                url = preferences.get_wecom_webhook_url()
                if not url:
                    _root.logger.info("review push(wecom) skipped: webhook not configured")
                    continue
                # 企业微信 markdown 标题已含一级标题, subtitle 拼到正文首行
                full_body = f"**{subtitle}**\n\n{content}" if subtitle else content
                ok = webhook_adapter.send_wecom_markdown(url, "每日复盘", full_body)
                _root.logger.info("review push(wecom) %s", "sent" if ok else "failed")
            elif ch == "custom":
                url = preferences.get_custom_webhook_url()
                if not url:
                    _root.logger.info("review push(custom) skipped: webhook not configured")
                    continue
                ok = webhook_adapter.send_custom(
                    url,
                    "每日复盘",
                    content,
                    "market_review",
                    meta,
                    secrets_store.get_custom_webhook_secret(),
                )
                _root.logger.info("review push(custom) %s", "sent" if ok else "failed")
            elif ch == "email":
                config = preferences.get_email_smtp_config()
                if not email_adapter.is_configured(config):
                    _root.logger.info("review push(email) skipped: SMTP not configured")
                    continue
                email_body = f"{subtitle}\n\n{content}" if subtitle else content
                ok = email_adapter.send_email(
                    config,
                    secrets_store.get_email_smtp_password(),
                    "每日复盘",
                    email_body,
                )
                _root.logger.info("review push(email) %s", "sent" if ok else "failed")
            # 未来更多渠道在此追加分支
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("review push error: %s", e)


def _register_review_job(scheduler, repo, hour: int, minute: int) -> None:
    """注册/更新定时复盘 job(工作日 mon-fri, Asia/Shanghai)。

    供 start_scheduler(启动时) 和 settings API(改时间时) 共用。
    用 replace_existing=True, 重复注册只更新 trigger。

    注意: _run_scheduled_review 是协程函数, 必须把函数对象本身(配合 args)传给
    add_job, 而非用 lambda 包裹 —— 否则 APScheduler 会把 lambda 当同步函数在线程池
    执行, 仅得到一个未 await 的协程对象, 复盘实际不会运行。
    """
    import app.jobs.daily_pipeline as _root

    scheduler.add_job(
        _root._run_scheduled_review,
        args=[repo],
        trigger=_root.CronTrigger(
            day_of_week="mon-fri", hour=hour, minute=minute, timezone="Asia/Shanghai"
        ),
        id=_root.REVIEW_JOB_ID,
        misfire_grace_time=7200,  # 复盘非关键, 允许 2 小时内补跑
        replace_existing=True,
    )


def _weekday_cron(hour: int, minute: int) -> _root.CronTrigger:
    """工作日指定时点的北京时间 cron 触发器。"""
    import app.jobs.daily_pipeline as _root

    return _root.CronTrigger(
        day_of_week="mon-fri", hour=hour, minute=minute, timezone="Asia/Shanghai"
    )


def _sched_instruments_task(repo: _root.KlineRepository, on_progress=None) -> dict:
    """盘前 instruments 同步 job 体。"""
    import app.jobs.daily_pipeline as _root

    emit = on_progress or _root._noop
    emit("sync_instruments", 0, "同步个股维表…")
    result = _root.run_instruments_sync(repo)
    emit("done", 100, f"个股维表同步完成,{result.get('instruments_rows', 0)} 只标的")
    return result


def _sched_pipeline_task(
    repo: _root.KlineRepository, capset: _root.CapabilitySet, on_progress=None
) -> dict:
    """盘后管道 job 体: 用实时 capset, 运行期间暂停行情取数, finally 刷缓存。"""
    import app.jobs.daily_pipeline as _root

    # 用 app.state 上的实时 capset(周期重探会热更新), 而非启动时捕获的旧 capset ——
    # 否则 Key 中途过期/续费后, 调度管道仍按旧档位打端点。
    app_state = _root._get_app_state()
    capset_live = getattr(app_state, "capabilities", None) or capset
    # 管道运行期间暂停实时行情取数, 防止覆写同一批 parquet 竞态
    qs = getattr(app_state, "quote_service", None)
    try:
        if qs:
            with qs.paused():
                result = _root.run_now(repo, capset_live, on_progress=on_progress)
        else:
            result = _root.run_now(repo, capset_live, on_progress=on_progress)
    finally:
        # 即便有阶段软失败(run_now 抛 PipelineStageError), 已落盘的日K/enriched
        # 仍需刷进内存缓存; 放 finally 保证部分成功也生效, 随后异常继续上抛。
        repo.refresh_cache()
    return result


def _sched_depth_finalize() -> None:
    """五档盘口 sealed 定版 job 体。"""
    import app.jobs.daily_pipeline as _root

    app_state = _root._get_app_state()
    depth_svc = getattr(app_state, "depth_service", None) if app_state else None
    if depth_svc:
        depth_svc.finalize()
