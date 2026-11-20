"""mining run 状态机审计与重建（DES-004 方案 C）。

Redis 是 mining run 状态机权威（Lua 原子），本模块把每次状态转换的快照写入
CK ``ck_mining_run_audit``；Redis 状态丢失后从审计表重建 run 的 manifest /
summary / eventseq 到 Redis，保证方案 C 的"状态可恢复"。

分层：``RedisMiningRunStore.transition_status`` 在 Lua 中把审计事件写入 Redis outbox，
本模块负责幂等 drain 到 CK；CK 不可达时状态机不阻塞，事件滞留 outbox 等待重试。
启动重建前用 barrier 保证审计已落表。
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

_TABLE = "ck_mining_run_audit"
_ACTOR = "mining_audit"
_OUTBOX_PREFIX = "tickflow:mining:audit:outbox"
_OUTBOX_INDEX = f"{_OUTBOX_PREFIX}:index"
_FLUSH_LOCK_TTL_S = 60


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
    event_id: str | None = None,
) -> None:
    """记录一次状态转换后的完整 manifest 快照。"""
    now = datetime.now()
    _client().insert_rows(
        _client().table(_TABLE),
        [
            [
                event_id or uuid.uuid4().hex,
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
            "event_id",
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


def outbox_key(namespace: str) -> str:
    """Return the Redis list key holding pending audit events."""
    return f"{_OUTBOX_PREFIX}:{namespace}"


def _redis_client():
    from app.services.mining_run_store_redis import _redis_client as store_redis_client

    return store_redis_client()


def _audit_row(event: dict[str, Any]) -> list[Any]:
    now = datetime.now()
    return [
        event["event_id"],
        event["namespace"],
        event["run_id"],
        event["from_status"],
        event["to_status"],
        json.dumps(event["manifest"], ensure_ascii=False, default=str),
        event.get("error"),
        now,
        _ACTOR,
        now,
        _ACTOR,
    ]


_AUDIT_COLUMNS = [
    "event_id",
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
]


def flush(namespace: str | None = None) -> int:
    """Drain pending audit events to CK and return the flushed row count."""
    if settings.mining_store_backend != "redis":
        return 0
    redis_client = _redis_client()
    if namespace is not None:
        namespaces = [namespace]
    else:
        members = redis_client.smembers(_OUTBOX_INDEX)
        namespaces = [m.decode() if isinstance(m, bytes) else str(m) for m in members]
    flushed = 0
    for ns in namespaces:
        key = outbox_key(ns)
        lock_key = f"{_OUTBOX_PREFIX}:flush:{ns}"
        token = uuid.uuid4().hex.encode()
        if not redis_client.set(lock_key, token, nx=True, ex=_FLUSH_LOCK_TTL_S):
            continue
        try:
            raw_items = redis_client.lrange(key, 0, -1)
            if not raw_items:
                continue
            rows = [_audit_row(json.loads(raw)) for raw in raw_items]
            ck_client = _client()
            ck_client.insert_rows(ck_client.table(_TABLE), rows, column_names=_AUDIT_COLUMNS)
            redis_client.ltrim(key, len(raw_items), -1)
            if redis_client.llen(key) == 0:
                redis_client.srem(_OUTBOX_INDEX, ns)
            flushed += len(rows)
        finally:
            if redis_client.get(lock_key) == token:
                redis_client.delete(lock_key)
    return flushed


def pending(namespace: str | None = None) -> int:
    """Return the number of audit events waiting in the Redis outbox."""
    if settings.mining_store_backend != "redis":
        return 0
    redis_client = _redis_client()
    if namespace is not None:
        return int(redis_client.llen(outbox_key(namespace)))
    members = redis_client.smembers(_OUTBOX_INDEX)
    return sum(
        int(redis_client.llen(outbox_key(m.decode() if isinstance(m, bytes) else str(m))))
        for m in members
    )


def barrier(namespace: str | None = None) -> None:
    """Flush pending audits and fail if any event is still undelivered."""
    flush(namespace)
    remaining = pending(namespace)
    if remaining:
        raise RuntimeError(f"mining 审计 outbox 仍有 {remaining} 条待写 CK")


def rebuild_mining_runs() -> dict[str, int]:
    """从 CK 审计重建 Redis mining run manifest，返回各 namespace 重建条数。"""
    from app.services.mining_run_store_redis import RedisMiningRunStore

    if settings.mining_store_backend != "redis":
        return {}

    barrier()
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
