"""用户偏好状态门面（DES-006）。PG JSONB 权威，Redis 承载版本与短缓存。"""

from __future__ import annotations

import json
import uuid
from typing import Any

from app.config import settings
from app.state import cache
from app.state.pg import execute, fetch_one, transaction

_SCHEMA = settings.user_state_pg_schema
_CACHE_KEY = "pref:cache"
_TTL_S = 600


def get_preferences() -> dict[str, Any]:
    """读取偏好 JSON；Redis 缓存未命中时查 PG。"""
    cached = cache.get_json(_CACHE_KEY)
    if isinstance(cached, dict):
        return dict(cached)
    row = fetch_one(
        f"SELECT payload FROM {_SCHEMA}.user_preference WHERE singleton_key=%s",
        ("default",),
    )
    payload = dict(row["payload"]) if row and isinstance(row["payload"], dict) else {}
    cache.set_json(_CACHE_KEY, payload, _TTL_S)
    return payload


def save_preferences(updates: dict[str, Any], *, actor: str = "api") -> dict[str, Any]:
    """在 PG 行锁内合并写入并递增 version；返回合并后的快照。"""
    with transaction() as conn:
        row = conn.execute(
            f"SELECT id, payload FROM {_SCHEMA}.user_preference WHERE singleton_key=%s FOR UPDATE",
            ("default",),
        ).fetchone()
        current = dict(row[1]) if row and isinstance(row[1], dict) else {}
        current.update(updates)
        if row:
            conn.execute(
                f"UPDATE {_SCHEMA}.user_preference SET payload=%s::jsonb, version=version+1, "
                "updated_by=%s, updated_at=now() WHERE id=%s",
                (json.dumps(current, ensure_ascii=False), actor, row[0]),
            )
        else:
            conn.execute(
                f"INSERT INTO {_SCHEMA}.user_preference "
                "(id, singleton_key, payload, version, created_by, updated_by) "
                "VALUES (%s, %s, %s::jsonb, 1, %s, %s)",
                (
                    uuid.uuid4().hex,
                    "default",
                    json.dumps(current, ensure_ascii=False),
                    actor,
                    actor,
                ),
            )
    cache.bump_generation("pref")
    cache.set_json(_CACHE_KEY, current, _TTL_S)
    from app.state.audit import write_config_audit

    write_config_audit("pref", "update", ",".join(sorted(updates)), actor)
    return current


def delete_preference_keys(keys: list[str], *, actor: str = "api") -> None:
    """从偏好文档中删除指定键；键不存在时保持幂等。"""
    if not keys:
        return
    with transaction() as conn:
        row = conn.execute(
            f"SELECT id, payload FROM {_SCHEMA}.user_preference WHERE singleton_key=%s FOR UPDATE",
            ("default",),
        ).fetchone()
        if not row or not isinstance(row[1], dict):
            return
        current = dict(row[1])
        for key in keys:
            current.pop(key, None)
        conn.execute(
            f"UPDATE {_SCHEMA}.user_preference SET payload=%s::jsonb, version=version+1, "
            "updated_by=%s, updated_at=now() WHERE id=%s",
            (json.dumps(current, ensure_ascii=False), actor, row[0]),
        )
    cache.bump_generation("pref")
    cache.set_json(_CACHE_KEY, current, _TTL_S)


def preferences_version() -> str:
    """返回 preferences 版本号，用于 data_source_stamp。"""
    row = fetch_one(
        f"SELECT version FROM {_SCHEMA}.user_preference WHERE singleton_key=%s",
        ("default",),
    )
    return str(row["version"]) if row else "0"


def replace_preferences(payload: dict[str, Any], *, actor: str = "migration") -> None:
    """迁移/恢复场景整表替换偏好。"""
    row = fetch_one(
        f"SELECT id FROM {_SCHEMA}.user_preference WHERE singleton_key=%s",
        ("default",),
    )
    execute(
        f"INSERT INTO {_SCHEMA}.user_preference "
        "(id, singleton_key, payload, version, created_by, updated_by) VALUES (%s, %s, %s::jsonb, 1, %s, %s) "
        "ON CONFLICT (singleton_key) DO UPDATE SET payload=EXCLUDED.payload, "
        "version=user_preference.version+1, updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            row["id"] if row else uuid.uuid4().hex,
            "default",
            json.dumps(payload, ensure_ascii=False),
            actor,
            actor,
        ),
    )
    cache.bump_generation("pref")
    cache.set_json(_CACHE_KEY, payload, _TTL_S)
