"""API Token 状态门面（DES-006）。Token 元数据落 PG，last_used 走 Redis 节流。"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterable

from app.config import settings
from app.state import cache
from app.state.pg import execute, fetch_all, fetch_one, transaction

_SCHEMA = settings.user_state_pg_schema
_LAST_USED_TTL = 24 * 3600


def list_api_tokens() -> list[dict]:
    """列出 token 元数据，不返回 token_hash。"""
    rows = fetch_all(
        f"SELECT token_id, name, scopes, revoked, created_at, last_used_at "
        f"FROM {_SCHEMA}.api_token ORDER BY created_at DESC"
    )
    return [_public(row) for row in rows]


def create_api_token(
    name: str, token_hash: str, scopes: Iterable[str], *, actor: str = "api"
) -> str:
    """创建 token 并返回业务 id；明文只在调用方一次性返回。"""
    token_id = "tok_" + uuid.uuid4().hex[:8]
    execute(
        f"INSERT INTO {_SCHEMA}.api_token "
        "(id, token_id, name, token_hash, scopes, revoked, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s, %s::jsonb, FALSE, %s, %s)",
        (
            uuid.uuid4().hex,
            token_id,
            name,
            token_hash,
            json.dumps(sorted(set(scopes))),
            actor,
            actor,
        ),
    )
    cache.bump_generation("token")
    return token_id


def revoke_api_token(token_id: str, *, actor: str = "api") -> bool:
    """撤销 token 并清除其 verify 短缓存；不存在返回 False。

    先取 token_hash 再改状态：get_api_token_by_hash 的 300s 缓存不校验
    generation，必须显式删除对应 key 才能让吊销即时生效。
    """
    row = fetch_one(
        f"SELECT token_hash FROM {_SCHEMA}.api_token WHERE token_id=%s AND revoked=FALSE",
        (token_id,),
    )
    if row is None:
        return False
    changed = execute(
        f"UPDATE {_SCHEMA}.api_token SET revoked=TRUE, updated_by=%s, updated_at=now() "
        "WHERE token_id=%s AND revoked=FALSE",
        (actor, token_id),
    )
    if changed:
        cache.bump_generation("token")
        cache.delete_key("token:" + str(row["token_hash"]))
    return bool(changed)


def get_api_token_by_hash(token_hash: str) -> dict | None:
    """按哈希读取有效 token；缓存未命中时查 PG。"""
    cached = cache.get_json("token:" + token_hash)
    if isinstance(cached, dict):
        return cached
    row = fetch_one(
        f"SELECT token_id, name, token_hash, scopes, revoked, created_at, last_used_at "
        f"FROM {_SCHEMA}.api_token WHERE token_hash=%s AND revoked=FALSE",
        (token_hash,),
    )
    if row is None:
        return None
    result = _public(row)
    result["token_hash"] = str(row["token_hash"])
    cache.set_json("token:" + token_hash, result, 300)
    return result


def touch_api_token(token_id: str) -> None:
    """节流更新 last_used_at，先写 Redis 再按条件落 PG。"""
    key = "token:last_used:" + token_id
    now = time.time()
    cached = cache.get_json(key)
    if isinstance(cached, dict) and now - float(cached.get("ts", 0)) < 60:
        return
    cache.set_json(key, {"ts": now}, _LAST_USED_TTL)
    execute(
        f"UPDATE {_SCHEMA}.api_token SET last_used_at=to_timestamp(%s), updated_at=now() "
        "WHERE token_id=%s",
        (now, token_id),
    )


def _public(row: dict) -> dict:
    """把 PG 行转为对外 token 视图。"""
    scopes = row.get("scopes")
    if isinstance(scopes, str):
        try:
            scopes = json.loads(scopes)
        except json.JSONDecodeError:
            scopes = []
    return {
        "id": row.get("token_id"),
        "name": row.get("name"),
        "scopes": scopes or [],
        "revoked": bool(row.get("revoked")),
        "created_at": str(row.get("created_at") or ""),
        "last_used_at": str(row.get("last_used_at") or "") if row.get("last_used_at") else None,
    }


def replace_tokens(rows: list[dict], *, actor: str = "migration") -> None:
    """迁移/恢复场景整表替换 token；调用方持有事务外一致性责任。"""
    with transaction() as conn:
        conn.execute(f"TRUNCATE TABLE {_SCHEMA}.api_token")
        for row in rows:
            conn.execute(
                f"INSERT INTO {_SCHEMA}.api_token "
                "(id, token_id, name, token_hash, scopes, revoked, last_used_at, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s::jsonb, %s, NULL, %s, %s)",
                (
                    uuid.uuid4().hex,
                    row["id"],
                    row["name"],
                    row["token_hash"],
                    json.dumps(sorted(set(row.get("scopes") or []))),
                    bool(row.get("revoked", False)),
                    actor,
                    actor,
                ),
            )
    cache.bump_generation("token")
