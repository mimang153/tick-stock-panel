"""PG 二进制对象门面（DES-006）。

小对象按 SHA-256 内容寻址，统一以 pgblob://{hash} 暴露。
"""

from __future__ import annotations

import hashlib
import logging
import uuid

from app.config import settings
from app.state.pg import execute, fetch_one

logger = logging.getLogger(__name__)


def put_blob(data: bytes, mime_type: str = "application/octet-stream") -> str:
    """写入二进制对象并返回内容 hash。"""
    if len(data) > settings.user_blob_max_bytes:
        raise ValueError(f"blob exceeds {settings.user_blob_max_bytes} bytes")
    content_hash = hashlib.sha256(data).hexdigest()
    if fetch_one(
        f"SELECT content_hash FROM {settings.user_state_pg_schema}.user_blob "
        "WHERE content_hash = %s",
        (content_hash,),
    ):
        return content_hash
    execute(
        f"INSERT INTO {settings.user_state_pg_schema}.user_blob "
        "(id, content_hash, payload, size_bytes, mime_type, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            uuid.uuid4().hex,
            content_hash,
            data,
            len(data),
            mime_type,
            "system",
            "system",
        ),
    )
    return content_hash


def get_blob(content_hash: str) -> bytes | None:
    """按内容 hash 读取对象；不存在返回 None。"""
    row = fetch_one(
        f"SELECT payload FROM {settings.user_state_pg_schema}.user_blob WHERE content_hash = %s",
        (content_hash,),
    )
    if row is None:
        return None
    return bytes(row["payload"])


def delete_blob(content_hash: str) -> None:
    """按内容 hash 删除对象。"""
    execute(
        f"DELETE FROM {settings.user_state_pg_schema}.user_blob WHERE content_hash = %s",
        (content_hash,),
    )


def resolve_blob_uri(uri: str) -> bytes | None:
    """解析 pgblob:// 或 file:// URI；其他协议返回 None。"""
    if uri.startswith("pgblob://"):
        return get_blob(uri.removeprefix("pgblob://"))
    if uri.startswith("file://"):
        from pathlib import Path
        from urllib.parse import unquote, urlparse

        parsed = urlparse(uri)
        path = Path(unquote(parsed.path))
        return path.read_bytes() if path.exists() else None
    return None
