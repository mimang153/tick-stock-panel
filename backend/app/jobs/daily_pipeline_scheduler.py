"""daily_pipeline_scheduler: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


def _sched_reprobe_capabilities() -> None:
    """周期性能力重探: Key 过期/续费无需重启即可被发现。"""
    import app.jobs.daily_pipeline as _root
    from app.tickflow.policy import detect_capabilities, tier_label

    app_state = _root._get_app_state()
    if app_state is None:
        return
    try:
        old = getattr(app_state, "capabilities", None)
        old_n = len(old.all()) if old else -1
        new_capset = detect_capabilities(force=True)
        app_state.capabilities = new_capset
        new_n = len(new_capset.all())
        if old_n != new_n:
            _root.logger.warning(
                "能力集变化: %d → %d capabilities (档位=%s)。Key 过期/续费或端点波动, "
                "已热更新 app.state.capabilities。",
                old_n,
                new_n,
                tier_label(),
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("周期能力重探失败(保留现有能力集): %s", e)


def start_scheduler(
    repo: _root.KlineRepository, capset: _root.CapabilitySet
) -> _root.AsyncIOScheduler:
    """启动调度器。

    工作日 09:10 — 同步个股维表
    工作日 HH:MM — 盘后管道（时间由用户偏好决定，默认 15:35）
    """
    import app.jobs.daily_pipeline as _root
    from app.services import preferences

    sched = preferences.get_pipeline_schedule()
    inst_sched = preferences.get_instruments_schedule()
    scheduler = _root.AsyncIOScheduler(timezone="Asia/Shanghai")

    # 盘前: 同步 instruments（时间由偏好决定）
    scheduler.add_job(
        lambda: _root._run_tracked(
            lambda on_progress=None: _root._sched_instruments_task(repo, on_progress),
            "instruments_sync",
        ),
        trigger=_root._weekday_cron(inst_sched["hour"], inst_sched["minute"]),
        id="pre_market_instruments",
        misfire_grace_time=1800,
        replace_existing=True,
    )

    # 盘后: 日 K + enriched（时间由偏好决定）
    scheduler.add_job(
        lambda: _root._scheduled_pipeline_task(
            lambda on_progress=None: _root._sched_pipeline_task(repo, capset, on_progress)
        ),
        trigger=_root._weekday_cron(sched["hour"], sched["minute"]),
        id="daily_pipeline",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # 盘后: 五档盘口 sealed 定版(时间由偏好决定, 默认15:02)
    depth_sched = preferences.get_depth_finalize_time()
    scheduler.add_job(
        _root._sched_depth_finalize,
        trigger=_root._weekday_cron(depth_sched["hour"], depth_sched["minute"]),
        id="depth_finalize",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # 周期性能力重探: 付费 Key 中途过期/续费无需重启即可被发现。
    scheduler.add_job(
        _root._sched_reprobe_capabilities,
        trigger=_root.IntervalTrigger(minutes=60),
        id="reprobe_capabilities",
        misfire_grace_time=600,
        replace_existing=True,
    )

    # 定时复盘 (AI 大盘复盘报告): 默认关闭, 仅用户开启时注册
    review_sched = preferences.get_review_schedule()
    if review_sched["enabled"]:
        _root._register_review_job(scheduler, repo, review_sched["hour"], review_sched["minute"])
        _root.logger.info(
            "scheduled_review enabled @%02d:%02d mon-fri",
            review_sched["hour"],
            review_sched["minute"],
        )

    scheduler.start()
    _root.logger.info(
        "scheduler started; instruments@%02d:%02d, pipeline@%02d:%02d, depth@%02d:%02d mon-fri",
        inst_sched["hour"],
        inst_sched["minute"],
        sched["hour"],
        sched["minute"],
        depth_sched["hour"],
        depth_sched["minute"],
    )
    return scheduler
