"""能力探测缓存门面（DES-006）。PG last-known-good + Redis 短缓存。"""

from __future__ import annotations

import json
import uuid
from typing import Any

from app.config import settings
from app.state import cache
from app.state.pg import execute, fetch_one

_SCHEMA = settings.user_state_pg_schema
_CACHE_PREFIX = "cap:"
_TTL_S = 300


def get_capability_cache() -> dict[str, Any] | None:
    """读取 schema_version=6 的能力缓存；不存在返回 None。"""
    cached = cache.get_json(_CACHE_PREFIX + "default")
    if isinstance(cached, dict):
        return cached
    row = fetch_one(
        f"SELECT payload FROM {_SCHEMA}.capability_cache "
        "WHERE singleton_key=%s AND schema_version=%s",
        ("default", 6),
    )
    if row is None:
        return None
    payload = dict(row["payload"]) if isinstance(row["payload"], dict) else {}
    if payload:
        cache.set_json(_CACHE_PREFIX + "default", payload, _TTL_S)
    return payload or None


def save_capability_cache(payload: dict[str, Any], *, actor: str = "api") -> None:
    """UPSERT 能力缓存并刷新 Redis 短缓存。"""
    execute(
        f"INSERT INTO {_SCHEMA}.capability_cache "
        "(id, singleton_key, schema_version, payload, probe_log, missing_caps, invalid_key, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s) "
        "ON CONFLICT (singleton_key) DO UPDATE SET schema_version=EXCLUDED.schema_version, "
        "payload=EXCLUDED.payload, probe_log=EXCLUDED.probe_log, "
        "missing_caps=EXCLUDED.missing_caps, invalid_key=EXCLUDED.invalid_key, "
        "updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            uuid.uuid4().hex,
            "default",
            int(payload.get("schema_version", 6)),
            json.dumps(payload, ensure_ascii=False),
            list(payload.get("probe_log") or []),
            list(payload.get("missing_caps") or []),
            bool(payload.get("invalid_key", False)),
            actor,
            actor,
        ),
    )
    cache.set_json(_CACHE_PREFIX + "default", payload, _TTL_S)
