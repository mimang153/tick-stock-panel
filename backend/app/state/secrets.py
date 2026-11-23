"""密钥状态门面（DES-006）。PG AES-GCM 加密存储，Redis 不缓存明文。"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import uuid
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.config import settings
from app.state.cache import bump_generation
from app.state.pg import execute, fetch_all, is_ready

logger = logging.getLogger(__name__)
_SCHEMA = settings.user_state_pg_schema
secret_cache: dict[str, Any] | None = None
secret_cache_generation: str | None = None


def _aes_key() -> bytes:
    """解析 32 字节加密密钥；缺失时由 PG URL 派生开发密钥。"""
    configured = settings.user_state_encryption_key or os.environ.get(
        "USER_STATE_ENCRYPTION_KEY", ""
    )
    if configured:
        try:
            key = base64.b64decode(configured, validate=True)
            if len(key) == 32:
                return key
        except (ValueError, TypeError):
            logger.warning("invalid USER_STATE_ENCRYPTION_KEY, deriving fallback")
    logger.warning("USER_STATE_ENCRYPTION_KEY missing; deriving development key from PG URL")
    return hashlib.sha256(settings.user_state_pg_url.encode("utf-8")).digest()


def _encrypt(value: Any) -> bytes:
    """AES-GCM 加密 JSON 值，输出 12B nonce + ciphertext。"""
    nonce = os.urandom(12)
    plaintext = json.dumps(value, ensure_ascii=False).encode("utf-8")
    return nonce + AESGCM(_aes_key()).encrypt(nonce, plaintext, None)


def _decrypt(payload: bytes) -> Any:
    """解密 AES-GCM 值；兼容历史无 nonce 的短数据为空。"""
    if len(payload) <= 12:
        return None
    plaintext = AESGCM(_aes_key()).decrypt(payload[:12], payload[12:], None)
    return json.loads(plaintext.decode("utf-8"))


def get_secrets() -> dict[str, Any]:
    """读取全部密钥；generation 变化时刷新进程缓存。

    PG/Redis 未初始化（模块导入期插件自检）时返回空，真实读取由 lifespan
    后的调用完成；写路径仍 fail-closed。
    """
    global secret_cache, secret_cache_generation
    if not is_ready():
        return {}
    from app.state.cache import generation

    current = generation("secret")
    if secret_cache is not None and current == secret_cache_generation:
        return dict(secret_cache)
    rows = fetch_all(f"SELECT secret_key, value_encrypted FROM {_SCHEMA}.user_secret")
    result: dict[str, Any] = {}
    for row in rows:
        try:
            result[str(row["secret_key"])] = _decrypt(bytes(row["value_encrypted"]))
        except Exception:  # noqa: BLE001 - 单条解密失败仅告警跳过，不阻断其余密钥加载
            logger.warning("decrypt secret failed: %s", row["secret_key"], exc_info=True)
    secret_cache, secret_cache_generation = result, current
    return dict(result)


def save_secrets(updates: dict[str, Any], *, actor: str = "api") -> dict[str, Any]:
    """逐 key UPSERT 密钥；None 值视为删除。"""
    for key, value in updates.items():
        if value is None:
            delete_secrets(key)
            continue
        execute(
            f"INSERT INTO {_SCHEMA}.user_secret "
            "(id, secret_key, value_encrypted, created_by, updated_by) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (secret_key) DO UPDATE SET value_encrypted=EXCLUDED.value_encrypted, "
            "updated_by=EXCLUDED.updated_by, updated_at=now()",
            (uuid.uuid4().hex, key, _encrypt(value), actor, actor),
        )
    if updates:
        bump_generation("secret")
        global secret_cache, secret_cache_generation
        secret_cache, secret_cache_generation = None, None
    return get_secrets()


def delete_secrets(*keys: str) -> None:
    """删除指定密钥。"""
    for key in keys:
        execute(f"DELETE FROM {_SCHEMA}.user_secret WHERE secret_key=%s", (key,))
    if keys:
        bump_generation("secret")
        global secret_cache, secret_cache_generation
        secret_cache, secret_cache_generation = None, None


def get_secret(key: str) -> Any:
    """读取单个密钥值；不存在返回空字符串以保持 getter 语义。"""
    value = get_secrets().get(key)
    return "" if value is None else value


def replace_secrets(values: dict[str, Any], *, actor: str = "migration") -> None:
    """迁移/恢复场景整表替换密钥。"""
    execute(f"TRUNCATE TABLE {_SCHEMA}.user_secret")
    save_secrets(values, actor=actor)
