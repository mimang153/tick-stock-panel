"""访问密码认证 — 单用户, 自托管场景 (DES-006)。

设计:
  - 密码用 PBKDF2-HMAC-SHA256 哈希(标准库 hashlib, 无新依赖), 加随机 salt;
    哈希与 salt 存 PG user_auth (经 app.state 门面), 不再读写 auth.json。
  - 会话用随机 token(token_urlsafe) 存 Redis (TTL=SESSION_TTL, 经 app.state 门面);
    Redis 丢失后按 D-03 重新登录, 不做本地持久化。
  - 「是否已设密码」以 generation('auth') 为缓存键, 改密码后 generation 递增自然失效。

安全要点:
  - 设密码接口必须限制本机/内网(见 auth router), 防黑客抢占域名抢先设密码。
  - 登录限流: 错5次锁5分钟(见 auth router 内存计数)。
  - 单密码, 不做多用户(避免重构全项目数据层)。
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets as _secrets
import time

from app.state import (
    delete_session,
    generation,
    get_auth,
    get_session,
    invalidate_sessions,
    save_auth,
    set_session,
)

logger = logging.getLogger(__name__)

# PBKDF2 参数(NIST 推荐, 单次校验 ~100ms, 兼顾安全与响应)
_PBKDF2_ITER = 200_000
_SALT_LEN = 16
_TOKEN_BYTES = 32
_HASH_ALGO = "pbkdf2_sha256"

# 会话有效期: 30 天(自托管单用户, 长一点减少重登频率)
SESSION_TTL = 30 * 24 * 3600
# 会话 cookie 名(与 app.api.auth.COOKIE_NAME 保持一致, 供下游直接引用)
COOKIE_NAME = "tf_session"

# 「是否已设密码」缓存: (generation('auth'), 是否已配置)。
# 每个 /api/ 请求都要判定, generation 变化时重读 PG; set_password 会 bump generation。
_configured_cache: tuple[str, bool] | None = None


def _hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    """返回 (salt_hex, hash_hex)。salt 为 None 时生成新 salt。"""
    if salt is None:
        salt = os.urandom(_SALT_LEN)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITER)
    return salt.hex(), dk.hex()


def _verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    """恒定时间比较, 防时序攻击。"""
    try:
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except ValueError:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITER)
    return _secrets.compare_digest(actual, expected)


def invalidate_runtime_caches() -> None:
    """配置备份导入后失效本进程「是否已设密码」缓存。

    DES-006: 会话在 Redis, 无需本地恢复; 密码真值在 PG, 下次判定按新
    generation 重读, 保证恢复后的密码在本进程内立即可见。
    """
    global _configured_cache
    _configured_cache = None


def is_configured() -> bool:
    """是否已设置访问密码 (以 generation('auth') 为缓存键)。

    热路径 (auth_middleware 每请求调用): generation 未变则直接返回缓存值;
    首次或 set_password / 恢复配置 bump generation 后重读 PG, 返回最新真值。
    """
    global _configured_cache
    current = generation("auth")
    if _configured_cache is None or _configured_cache[0] != current:
        data = get_auth()
        _configured_cache = (current, bool(data and data.get("password_hash")))
    return _configured_cache[1]


def set_password(password: str) -> None:
    """设置/修改访问密码: 哈希写 PG 并清空全部会话 (强制重新登录)。

    Args:
        password: 明文密码, 至少 6 位。
    Raises:
        ValueError: 密码短于 6 位。
    """
    global _configured_cache
    if len(password) < 6:
        raise ValueError("密码至少 6 位")
    salt_hex, hash_hex = _hash_password(password)
    save_auth(hash_hex, salt_hex, hash_algo=_HASH_ALGO, iterations=_PBKDF2_ITER)
    invalidate_sessions()
    _configured_cache = None
    logger.info("access password set")


def bootstrap_from_env() -> bool:
    """首次初始化: AUTH_PASSWORD 已配置且未设过密码时用它设密码, 返回是否初始化。

    公网部署免去 SSH 端口转发; 明文仅在内存/配置, 哈希后写 PG。设置成功后重启不再覆盖
    (用户改密码走 UI), 避免环境变量反复重置。
    """
    from app.config import _ENV_FILE, settings

    pwd = (settings.auth_password or "").strip()
    # Compose 会对 env_file 中未加单引号的 $VAR 做插值。Docker 部署时同时
    # 只读挂载原始 .env,首次初始化密码直接按 dotenv 语义读取,避免特殊字符被截断。
    if _ENV_FILE.is_file():
        from dotenv import dotenv_values

        raw_pwd = dotenv_values(_ENV_FILE, encoding="utf-8", interpolate=False).get("AUTH_PASSWORD")
        if isinstance(raw_pwd, str) and raw_pwd.strip():
            pwd = raw_pwd.strip()
    if not pwd:
        return False
    if is_configured():
        # 已设过密码, 不覆盖 (避免环境变量反复重置用户在 UI 改的密码)
        return False
    try:
        set_password(pwd)
        logger.info("access password bootstrapped from AUTH_PASSWORD env (one-time)")
        return True
    except ValueError as e:
        # 密码不合规 (< 6 位), 记日志但不阻断启动
        logger.warning("AUTH_PASSWORD bootstrap skipped: %s", e)
        return False


def verify_and_create_session(password: str) -> str | None:
    """验证密码, 成功则创建 Redis 会话并返回 token, 失败返回 None。"""
    data = get_auth()
    if not data or not data.get("password_hash"):
        return None
    if not _verify_password(password, str(data.get("password_salt") or ""), data["password_hash"]):
        return None
    token = _secrets.token_urlsafe(_TOKEN_BYTES)
    set_session(token, {"created_at": int(time.time())}, SESSION_TTL)
    return token


def revoke_session(token: str) -> None:
    """注销会话(登出): 删除 Redis 会话 key。"""
    if token:
        delete_session(token)


def is_valid_session(token: str) -> bool:
    """检查会话是否有效: Redis 中存在且未过期 (TTL 由 Redis 自动管理)。"""
    return bool(token) and get_session(token) is not None
