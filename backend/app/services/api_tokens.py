"""API Token 域模块 — 外部调用方的第二认证通道 (与访问密码并行)。

设计 (DES-006):
  - 元数据/哈希存 PG api_token (经 app.state 门面), 不再读写 api_tokens.json;
    明文 Token (tsp_ + 32B hex) 仅在创建响应中出现一次。
  - scope: 固定五档, 见 SCOPES; 管理接口无对应 scope, Token 永远进不去。
  - last_used_at 走 Redis 节流 (touch_api_token), 不再本地落盘。
  - data_dir 参数仅为兼容旧调用方保留, 内部忽略。
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from pathlib import Path

from app.market_time import cn_now
from app.state import (
    create_api_token,
    get_api_token_by_hash,
    list_api_tokens,
    revoke_api_token,
    touch_api_token,
)

logger = logging.getLogger(__name__)

# 对外开放的 scope 全集 (admin 刻意不存在 — 管理面永不开放给 Token)
SCOPES = (
    "read:market",
    "read:ext",
    "write:ext",
    "read:analysis",
    "run:backtest",
    "paper:trade",
)

TOKEN_PREFIX = "tsp_"


def _hash(plaintext: str) -> str:
    """SHA-256 哈希明文 Token, 供存储与校验使用。"""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def load_tokens(data_dir: Path) -> list[dict]:
    """列出 token 元数据视图 (data_dir 保留兼容, DES-006 后内部走 PG 门面)。"""
    return list_api_tokens()


def list_tokens(data_dir: Path) -> list[dict]:
    """管理列表视图: 返回 id/name/scopes/created_at/last_used_at/revoked。"""
    return load_tokens(data_dir)


def create_token(data_dir: Path, name: str, scopes: list[str]) -> tuple[dict, str]:
    """创建 Token。返回 (记录视图, 明文) — 明文只此一次, 调用方负责展示。"""
    bad = [s for s in scopes if s not in SCOPES]
    if bad:
        raise ValueError(f"未知 scope: {bad} (可用: {list(SCOPES)})")
    plaintext = TOKEN_PREFIX + secrets.token_hex(32)
    token_name = name.strip() or "未命名"
    clean_scopes = sorted(set(scopes))
    token_id = create_api_token(token_name, _hash(plaintext), clean_scopes)
    view = {
        "id": token_id,
        "name": token_name,
        "scopes": clean_scopes,
        "created_at": cn_now().isoformat(timespec="seconds"),
        "last_used_at": None,
        "revoked": False,
    }
    return view, plaintext


def revoke_token(data_dir: Path, token_id: str) -> bool:
    """撤销 Token; 不存在或已撤销返回 False。"""
    return revoke_api_token(token_id)


def verify_token(data_dir: Path, plaintext: str) -> dict | None:
    """明文 → 有效记录 (revoked/不匹配/哈希对不上 → None)。

    命中时经 touch_api_token 节流更新 last_used_at (Redis 节流后落 PG)。
    """
    if not plaintext.startswith(TOKEN_PREFIX):
        return None
    record = get_api_token_by_hash(_hash(plaintext))
    if record is None or record.get("revoked"):
        return None
    touch_api_token(str(record["id"]))
    return record
