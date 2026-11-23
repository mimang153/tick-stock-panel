"""认证状态门面（DES-006）。密码存 PG，会话存 Redis。"""

from __future__ import annotations

import uuid

from app.config import settings
from app.state.cache import bump_generation, delete_prefix
from app.state.pg import execute, fetch_one

_SCHEMA = settings.user_state_pg_schema


def get_auth() -> dict | None:
    """读取单例密码配置；未配置返回 None。"""
    return fetch_one(
        f"SELECT password_hash, password_salt, hash_algo, iterations FROM {_SCHEMA}.user_auth "
        "WHERE singleton_key = %s",
        ("default",),
    )


def save_auth(
    password_hash: str, password_salt: str, *, hash_algo: str, iterations: int, actor: str = "api"
) -> None:
    """UPSERT 单例密码配置并失效全部会话。"""
    execute(
        f"INSERT INTO {_SCHEMA}.user_auth "
        "(id, singleton_key, password_hash, password_salt, hash_algo, iterations, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (singleton_key) DO UPDATE SET password_hash=EXCLUDED.password_hash, "
        "password_salt=EXCLUDED.password_salt, hash_algo=EXCLUDED.hash_algo, "
        "iterations=EXCLUDED.iterations, updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            uuid.uuid4().hex,
            "default",
            password_hash,
            password_salt,
            hash_algo,
            iterations,
            actor,
            actor,
        ),
    )
    invalidate_sessions()


def invalidate_sessions() -> None:
    """删除全部会话 key 并递增 auth generation。"""
    delete_prefix("session:")
    bump_generation("auth")
