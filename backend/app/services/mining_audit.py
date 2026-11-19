"""mining run 状态机审计与重建（DES-004 方案 C）。

Redis 是 mining run 状态机权威（Lua 原子），本模块把每次状态转换的快照写入
CK ``ck_mining_run_audit``；Redis 状态丢失后从审计表重建 run 的 manifest /
summary / eventseq 到 Redis，保证方案 C 的"状态可恢复"。

分层：写入点在 ``RedisMiningRunStore.transition_status``（运行时 CK 已就绪）；
本模块延迟导入 storage.runtime 避免循环。CK 不可达时审计写入 fail-closed。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

_TABLE = "ck_mining_run_audit"
_ACTOR = "mining_audit"


def enabled() -> bool:
    """是否写审计：仅 redis mining store + CK runtime 就绪时。"""
    if settings.mining_store_backend != "redis":
        return False
    from app.storage.runtime import get_storage

    return get_storage() is not None


def _client():
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        raise RuntimeError("mining 审计需要已初始化的 ClickHouse storage runtime")
    return runtime.client


def record_transition(
    namespace: str,
    run_id: str,
    from_status: str,
    to_status: str,
    manifest: dict[str, Any],
    error: str | None = None,
) -> None:
    """记录一次状态转换后的完整 manifest 快照。"""
    now = datetime.now()
    _client().insert_rows(
        _client().table(_TABLE),
        [
            [
                namespace,
                run_id,
                from_status,
                to_status,
                json.dumps(manifest, ensure_ascii=False, default=str),
                error,
                now,
                _ACTOR,
                now,
                _ACTOR,
            ]
        ],
        column_names=[
            "namespace",
            "run_id",
            "from_status",
            "to_status",
            "manifest_json",
            "error",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ],
    )


def rebuild_mining_runs() -> dict[str, int]:
    """从 CK 审计重建 Redis mining run manifest，返回各 namespace 重建条数。"""
    from app.services.mining_run_store_redis import RedisMiningRunStore

    if settings.mining_store_backend != "redis":
        return {}

    client = _client()
    rows = client.query(
        f"SELECT namespace, run_id, argMax(manifest_json, updated_at) AS manifest "
        f"FROM {client.table(_TABLE)} GROUP BY namespace, run_id"
    )
    counts: dict[str, int] = {}
    for row in rows.iter_rows(named=True):
        namespace = row["namespace"]
        store = RedisMiningRunStore(namespace=namespace)
        manifest = json.loads(row["manifest"])
        store._client.hset(store._runs_key, row["run_id"], json.dumps(manifest, default=str))
        store._client.sadd(store._index_key, row["run_id"])
        counts[namespace] = counts.get(namespace, 0) + 1
    logger.info("mining run 已从 CK 审计重建: %s", counts)
    return counts
