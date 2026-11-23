"""任务状态门面（DES-006）。Redis live state/progress，CK 历史。"""

from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.state import cache

logger = logging.getLogger(__name__)
_STATE_PREFIX = "job:"
_PROGRESS_PREFIX = "job:progress:"
_INDEX_KEY = "job:index"


def put_job_state(job: dict[str, Any]) -> None:
    """写入任务当前态并加入最近任务索引。"""
    job_id = str(job["id"])
    cache.set_json(_STATE_PREFIX + job_id, job, _ttl(job))
    index = cache.get_json(_INDEX_KEY) or {"ids": []}
    ids = [str(item) for item in index.get("ids", []) if str(item) != job_id]
    ids.insert(0, job_id)
    cache.set_json(_INDEX_KEY, {"ids": ids[:500]})


def get_job_state(job_id: str) -> dict | None:
    """读取任务当前态。"""
    value = cache.get_json(_STATE_PREFIX + job_id)
    return dict(value) if isinstance(value, dict) else None


def put_job_progress(job_id: str, progress: dict[str, Any]) -> None:
    """持久化任务 progress。"""
    cache.set_json(_PROGRESS_PREFIX + job_id, progress, _ttl({}))


def get_job_progress(job_id: str) -> dict | None:
    """读取任务 progress。"""
    value = cache.get_json(_PROGRESS_PREFIX + job_id)
    return dict(value) if isinstance(value, dict) else None


def list_recent_jobs(limit: int = 50) -> list[dict[str, Any]]:
    """按最近索引读取任务当前态。"""
    index = cache.get_json(_INDEX_KEY) or {"ids": []}
    jobs: list[dict[str, Any]] = []
    for job_id in list(index.get("ids", []))[:limit]:
        job = get_job_state(str(job_id))
        if job is not None:
            jobs.append(job)
    return jobs


def clear_jobs() -> None:
    """删除所有任务 live state/progress；用于测试与清库。"""
    cache.delete_prefix(_STATE_PREFIX)
    cache.delete_prefix(_PROGRESS_PREFIX)
    cache.delete_key(_INDEX_KEY)


def write_job_history(job: dict[str, Any]) -> None:
    """把任务终态写入 CK 历史；失败只记日志。

    复用 coordination_audit 的 ck_coord_job_state 写入契约（列与协调态重建一致），
    避免在门面里重复拼列导致 schema 漂移。
    """
    try:
        from app.services.coordination_audit import record_job_state

        record_job_state(job)
    except Exception as exc:  # noqa: BLE001 - 历史写入失败不能影响任务状态主流程
        logger.warning("job history write failed: %s", exc)


def _ttl(job: dict[str, Any]) -> int:
    """按配置返回 live state TTL。"""
    return max(1, settings.job_live_ttl_days * 24 * 3600)
