"""Key / 凭据存储 (DES-006)。

密钥存 PG user_secret (AES-GCM 加密, 经 app.state 门面), 不再读写 secrets.json。
优先级: PG 密钥 > .env > 空(Free 模式)。
取消 secrets.json 外部编辑语义 (D-05): 统一走 API/UI 配置。
"""

from __future__ import annotations

import copy
import logging
import os
from pathlib import Path

from app.state import get_secrets, save_secrets

logger = logging.getLogger(__name__)


def _path() -> Path:
    """返回虚拟路径标识 (DES-006 后密钥不再落本地文件, 仅兼容展示与日志)。"""
    from app.config import settings

    return settings.data_dir / "user_data" / "secrets.json"


def file_path() -> Path:
    """虚拟路径标识 (设置页展示用); DES-006 后该路径不存在实际文件。"""
    return _path()


def load() -> dict:
    """读取 PG 密钥配置 (经门面)。返回深拷贝, 调用方可自由修改。"""
    return copy.deepcopy(get_secrets())


def save(updates: dict) -> dict:
    """合并写入 (不会清掉未提及的字段)。返回新内容。"""
    save_secrets({key: value for key, value in updates.items() if value is not None})
    return load()


def clear(*keys: str) -> dict:
    """清掉指定字段 (留空清全部)。返回新内容。

    门面 save_secrets 将 None 值视为删除, 故用其表达清理语义。
    """
    targets = keys or tuple(get_secrets().keys())
    if targets:
        save_secrets({key: None for key in targets})
    return load()


def get_tickflow_key() -> str:
    """取当前 TickFlow Key:PG 密钥优先,否则 .env。"""
    val = load().get("tickflow_api_key")
    if val:
        return val
    from app.config import settings

    return settings.tickflow_api_key or ""


def get_ai_key() -> str:
    """取当前 AI Key:PG 密钥优先,否则 .env。"""
    val = load().get("ai_api_key")
    if val:
        return val
    from app.config import settings

    return settings.ai_api_key or ""


def get_ai_config(key: str, default: str = "") -> str:
    """取 AI 配置项:PG 密钥优先,否则 config。"""
    val = load().get(key)
    if val:
        return val
    from app.config import settings

    return getattr(settings, key, default) or default


def get_ai_config_int(key: str, default: int) -> int:
    """取 AI 数值配置项 (如 ai_max_output_tokens): PG 密钥优先,否则 config。"""
    val = load().get(key)
    if val is not None:
        try:
            return int(val)
        except (TypeError, ValueError):
            logger.warning("ai config %s is not an int: %r", key, val)
    from app.config import settings

    return int(getattr(settings, key, default) or default)


def get_custom_webhook_secret() -> str:
    """Return the optional HMAC secret for the generic outbound webhook."""
    return str(load().get("custom_webhook_secret") or "")


def set_custom_webhook_secret(secret: str) -> str:
    """Persist or clear the generic outbound webhook HMAC secret."""
    value = (secret or "").strip()
    if value:
        save({"custom_webhook_secret": value})
    else:
        clear("custom_webhook_secret")
    return value


def get_email_smtp_password() -> str:
    """Return the SMTP password used by the email notification channel."""
    return str(load().get("email_smtp_password") or "")


def set_email_smtp_password(password: str) -> str:
    """Persist or clear the SMTP password used by email notifications."""
    value = password or ""
    if value:
        save({"email_smtp_password": value})
    else:
        clear("email_smtp_password")
    return value


def get_env_backed_secret(field: str, env_name: str) -> str:
    """取环境变量后备的密钥(插件 API Key 等):PG 密钥优先,否则环境变量。

    与 get_tickflow_key 同优先级语义:UI 写入 PG 密钥后即覆盖 .env。
    """
    val = load().get(field)
    if val:
        return str(val).strip()
    return os.environ.get(env_name, "").strip()


def mask(key: str, prefix: int = 4, suffix: int = 4) -> str:
    """脱敏显示。"""
    if not key:
        return ""
    if len(key) <= prefix + suffix:
        return "•" * len(key)
    return f"{key[:prefix]}{'•' * 6}{key[-suffix:]}"
