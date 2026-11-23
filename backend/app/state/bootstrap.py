"""用户状态存储的启动/关闭引导（DES-006）。

把 PG/Redis 初始化、shared 目录校验、跨进程事件订阅与中断任务恢复从
main.py 抽出，保持启动编排文件精简。所有入口幂等，可被多 worker 重复调用。
"""

from __future__ import annotations

import logging
import os

from app.startup_diagnostics import startup_phase

logger = logging.getLogger(__name__)


def verify_shared_writable() -> None:
    """确认 shared strategies/plugins 可写；不可写抛 RuntimeError（启动失败）。"""
    from app.state import shared_plugin_root, shared_strategy_root

    for directory in (shared_strategy_root(), shared_plugin_root()):
        probe = directory / f".write-probe-{os.getpid()}"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except OSError as exc:
            raise RuntimeError(f"shared 目录不可写: {directory}") from exc


def bootstrap_user_state() -> None:
    """初始化用户状态存储：PG 建表、Redis 连通、shared 校验、订阅线程。

    PG/Redis 不可达时抛异常让启动 fail-closed，避免请求热路径才暴露配置错误。
    """
    with startup_phase(logger, "user state PG initialization"):
        from app.state import init_state_pg

        init_state_pg()
    with startup_phase(logger, "user state Redis connectivity"):
        from app.state import check_persistence, ping

        ping()
        check_persistence()
    with startup_phase(logger, "user state shared directory check"):
        verify_shared_writable()
    with startup_phase(logger, "user state event listener"):
        from app.state import start_listener

        start_listener()


def recover_interrupted_jobs_at_startup() -> None:
    """从 CK 恢复中断任务到 Redis live state（DES-006 §9 步骤 8）。

    单 worker 执行，避免多 worker 互把对方 ACTIVE job 标 interrupted。
    """
    from app.services.coordination import coordination

    if not coordination.once("startup:user_state_job_recover"):
        return
    with startup_phase(logger, "interrupted job recovery"):
        from app.services.pipeline_jobs import recover_interrupted_jobs

        recovered = recover_interrupted_jobs()
    if recovered:
        logger.warning("recovered %d interrupted pipeline jobs from CK", recovered)
