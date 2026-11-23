"""协调态审计与重建（DES-004 §2.3 / 方案 B）。

Redis 升格为协调态权威（JobState / CancelFlag / LimiterCounter）并开启 AOF；
AOF 只缩短重启恢复时间，不保证业务持久。本模块把协调态的"事实"以审计形式
写入 CK，Redis 丢失后可从 CK 重建三套后端。

分层约束：
- ``coordination`` 是模块级单例、导入即初始化，早于 CK runtime 就绪，故审计
  写入不放在 coordination 内部，而由业务层（pipeline_jobs / heavy_job_limiter）
  在运行时调用本模块；
- 本模块延迟导入 storage.runtime，避免循环依赖；
- CK 不可达时审计写入 fail-closed（抛错），不静默跳过——否则会形成"Redis 有
  状态但 CK 无审计"的不可恢复窗口。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

_JOB_STATE_TABLE = "ck_coord_job_state"
_CANCEL_TABLE = "ck_coord_cancel_request"
_LEASE_TABLE = "ck_coord_limiter_lease"
_ACTOR = "coordination_audit"
# 与 Redis LimiterCounter TTL 对齐：超过该时长的未释放租约视为进程崩溃遗留，
# 重建时不恢复其容量，否则一次异常退出会永久占满 exclusive 槽位。
_LEASE_REBUILD_MAX_AGE_S = 3600


def enabled() -> bool:
    """是否启用 CK 审计：仅 redis 协调模式且 CK runtime 已就绪。

    memory 模式或测试未初始化 storage 时返回 False，写入点据此跳过审计，
    保护既有单进程语义与大量不建 CK 的单元测试。
    """
    if settings.coordination_backend != "redis":
        return False
    from app.storage.runtime import get_storage

    return get_storage() is not None


def _client():
    """延迟取 CK client；未初始化时抛错（审计需要 CK 就绪）。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        raise RuntimeError("协调态审计需要已初始化的 ClickHouse storage runtime")
    return runtime.client


def _now() -> datetime:
    return datetime.now()


def record_job_state(job: dict[str, Any]) -> None:
    """记录任务状态快照；按 job_id 保留最新（ReplacingMergeTree + updated_at）。"""
    _client().insert_rows(
        _client().table(_JOB_STATE_TABLE),
        [
            [
                job["id"],
                json.dumps(job, ensure_ascii=False, default=str),
                str(job.get("status") or ""),
                _now(),
                _ACTOR,
                _now(),
                _ACTOR,
            ]
        ],
        column_names=[
            "job_id",
            "state_json",
            "status",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ],
    )


def record_cancel(job_id: str, actor: str = _ACTOR) -> None:
    """记录取消请求（consumed=0）；重建时恢复该标志。"""
    now = _now()
    _client().insert_rows(
        _client().table(_CANCEL_TABLE),
        [[job_id, now, actor, 0, now, actor, now, actor]],
        column_names=[
            "job_id",
            "requested_at",
            "actor",
            "consumed",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ],
    )


def record_lease_acquire(lease_id: str, owner_id: str, kind: str, weight: int) -> None:
    """记录容量租约获取（released_at=NULL）。"""
    now = _now()
    _client().insert_rows(
        _client().table(_LEASE_TABLE),
        [
            [
                lease_id,
                owner_id,
                kind,
                weight,
                now,
                None,
                now,
                _ACTOR,
                now,
                _ACTOR,
            ]
        ],
        column_names=[
            "lease_id",
            "owner_id",
            "kind",
            "weight",
            "acquired_at",
            "released_at",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ],
    )


def record_lease_release(lease_id: str) -> None:
    """标记租约释放：再插一行带 released_at，更新 updated_at 覆盖旧行。"""
    now = _now()
    _client().insert_rows(
        _client().table(_LEASE_TABLE),
        [
            [
                lease_id,
                "",
                "",
                0,
                now,
                now,
                now,
                _ACTOR,
                now,
                _ACTOR,
            ]
        ],
        column_names=[
            "lease_id",
            "owner_id",
            "kind",
            "weight",
            "acquired_at",
            "released_at",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ],
    )


def _query(sql: str, parameters: Any = None):
    return _client().query(sql, parameters)


def rebuild_coordination() -> dict[str, int]:
    """从 CK 审计重建 Redis 三套协调态，返回各类重建条数。

    仅在 ``COORDINATION_BACKEND=redis`` 时执行；非 redis 模式是空操作
    （本地协调态由进程内 Event / 计数器自洽，无需重建）。CK 不可达时
    由 ``_client`` 抛错，fail-closed，不静默跳过。
    """
    from app.services.coordination import coordination

    if settings.coordination_backend != "redis":
        return {"job_state": 0, "cancel": 0, "lease": 0}

    counts = {"job_state": 0, "cancel": 0, "lease": 0}

    job_df = _query(f"SELECT job_id, state_json FROM {_client().table(_JOB_STATE_TABLE)} FINAL")
    for row in job_df.iter_rows(named=True):
        coordination.job_state.put(row["job_id"], json.loads(row["state_json"]))
        counts["job_state"] += 1

    cancel_df = _query(
        f"SELECT job_id FROM {_client().table(_CANCEL_TABLE)} FINAL "
        "WHERE consumed = 0 AND requested_at > now() - INTERVAL 1800 SECOND"
    )
    for row in cancel_df.iter_rows(named=True):
        coordination.cancel_flags.set(row["job_id"])
        counts["cancel"] += 1

    lease_df = _query(
        f"SELECT lease_id, weight, acquired_at FROM {_client().table(_LEASE_TABLE)} FINAL "
        "WHERE released_at IS NULL"
    )
    cutoff = datetime.now() - timedelta(seconds=_LEASE_REBUILD_MAX_AGE_S)
    used = 0
    stale_leases: list[str] = []
    for row in lease_df.iter_rows(named=True):
        acquired = row.get("acquired_at")
        if isinstance(acquired, datetime) and acquired >= cutoff:
            used += int(row.get("weight") or 0)
            continue
        stale_leases.append(str(row.get("lease_id") or ""))
    if stale_leases:
        logger.warning(
            "skip stale limiter leases on rebuild: count=%d leases=%s",
            len(stale_leases),
            stale_leases[:10],
        )
    coordination.limiter_counter.restore(used)
    counts["lease"] = used

    logger.info("协调态已从 CK 审计重建: %s", counts)
    return counts
