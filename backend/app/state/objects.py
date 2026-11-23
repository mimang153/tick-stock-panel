"""配置对象门面（DES-006）。data_sources/ext_pages/sound_packs/integrity_backups。"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from app.config import settings
from app.state import blob, cache
from app.state.pg import execute, fetch_all, fetch_one, is_ready

_SCHEMA = settings.user_state_pg_schema


def list_data_source_configs() -> list[dict[str, Any]]:
    """列出数据源配置；PG 未初始化（模块导入期）时返回空列表。"""
    if not is_ready():
        return []
    rows = fetch_all(
        f"SELECT config_name, kind, content FROM {_SCHEMA}.data_source_config ORDER BY config_name"
    )
    return [
        {"name": str(row["config_name"]), "kind": str(row["kind"]), "content": str(row["content"])}
        for row in rows
    ]


def get_data_source_config(name: str) -> str | None:
    """读取指定数据源配置正文；PG 未初始化（模块导入期）时返回 None。"""
    if not is_ready():
        return None
    row = fetch_one(
        f"SELECT content FROM {_SCHEMA}.data_source_config WHERE config_name=%s",
        (name,),
    )
    return str(row["content"]) if row else None


def save_data_source_config(
    name: str, content: str, *, kind: str = "yaml", actor: str = "api"
) -> None:
    """UPSERT 数据源配置。"""
    execute(
        f"INSERT INTO {_SCHEMA}.data_source_config "
        "(id, config_name, kind, content, created_by, updated_by) VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (config_name) DO UPDATE SET kind=EXCLUDED.kind, content=EXCLUDED.content, "
        "updated_by=EXCLUDED.updated_by, updated_at=now()",
        (uuid.uuid4().hex, name, kind, content, actor, actor),
    )
    cache.bump_generation("config")


def delete_data_source_config(name: str) -> None:
    """删除数据源配置。"""
    execute(f"DELETE FROM {_SCHEMA}.data_source_config WHERE config_name=%s", (name,))
    cache.bump_generation("config")


def list_ext_pages() -> list[dict[str, Any]]:
    """列出扩展页面元数据。"""
    rows = fetch_all(f"SELECT page_id, config, bundle_hash FROM {_SCHEMA}.ext_page")
    return [
        {
            "id": str(row["page_id"]),
            "config": dict(row["config"]) if isinstance(row["config"], dict) else {},
            "bundle_hash": str(row["bundle_hash"] or ""),
        }
        for row in rows
    ]


def save_ext_page(
    page_id: str, config: dict[str, Any], bundle: bytes | None = None, *, actor: str = "api"
) -> None:
    """保存扩展页面配置与可选 bundle。"""
    bundle_hash = blob.put_blob(bundle, "text/javascript") if bundle is not None else None
    execute(
        f"INSERT INTO {_SCHEMA}.ext_page "
        "(id, page_id, config, bundle_hash, created_by, updated_by) VALUES (%s, %s, %s::jsonb, %s, %s, %s) "
        "ON CONFLICT (page_id) DO UPDATE SET config=EXCLUDED.config, bundle_hash=EXCLUDED.bundle_hash, "
        "updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            uuid.uuid4().hex,
            page_id,
            json.dumps(config, ensure_ascii=False),
            bundle_hash,
            actor,
            actor,
        ),
    )
    cache.bump_generation("config")


def delete_ext_page(page_id: str) -> None:
    """删除扩展页面配置。"""
    execute(f"DELETE FROM {_SCHEMA}.ext_page WHERE page_id=%s", (page_id,))
    cache.bump_generation("config")


def list_sound_packs() -> list[dict[str, Any]]:
    """列出声音包。"""
    rows = fetch_all(
        f"SELECT pack_name, blob_hash, size_bytes, ext FROM {_SCHEMA}.sound_pack ORDER BY pack_name"
    )
    return [
        {
            "name": str(row["pack_name"]),
            "blob_hash": str(row["blob_hash"]),
            "size_bytes": int(row["size_bytes"]),
            "ext": str(row["ext"]),
        }
        for row in rows
    ]


def save_sound_pack(name: str, data: bytes, ext: str, *, actor: str = "api") -> None:
    """保存声音文件到 user_blob。"""
    content_hash = blob.put_blob(data, _mime(ext))
    execute(
        f"INSERT INTO {_SCHEMA}.sound_pack "
        "(id, pack_name, blob_hash, size_bytes, ext, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (pack_name) DO UPDATE SET blob_hash=EXCLUDED.blob_hash, "
        "size_bytes=EXCLUDED.size_bytes, ext=EXCLUDED.ext, updated_by=EXCLUDED.updated_by, updated_at=now()",
        (uuid.uuid4().hex, name, content_hash, len(data), ext, actor, actor),
    )


def read_sound_pack(name: str) -> tuple[bytes, str] | None:
    """读取声音文件与扩展名。"""
    row = fetch_one(
        f"SELECT blob_hash, ext FROM {_SCHEMA}.sound_pack WHERE pack_name=%s",
        (name,),
    )
    if row is None:
        return None
    data = blob.get_blob(str(row["blob_hash"]))
    return (data, str(row["ext"])) if data is not None else None


def delete_sound_pack(name: str) -> None:
    """删除声音包。"""
    execute(f"DELETE FROM {_SCHEMA}.sound_pack WHERE pack_name=%s", (name,))


def save_integrity_backup(
    backup_id: str, manifest: dict[str, Any], files: dict[str, bytes], *, actor: str = "system"
) -> None:
    """保存完整性备份文件到 user_blob。"""
    hashes = [blob.put_blob(data, "application/octet-stream") for data in files.values()]
    manifest = {**manifest, "files": list(files)}
    execute(
        f"INSERT INTO {_SCHEMA}.integrity_backup "
        "(id, backup_id, manifest, blob_hashes, created_by, updated_by) VALUES (%s, %s, %s::jsonb, %s, %s, %s) "
        "ON CONFLICT (backup_id) DO UPDATE SET manifest=EXCLUDED.manifest, "
        "blob_hashes=EXCLUDED.blob_hashes, updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            uuid.uuid4().hex,
            backup_id,
            json.dumps(manifest, ensure_ascii=False),
            hashes,
            actor,
            actor,
        ),
    )


def read_integrity_backup(backup_id: str) -> dict[str, bytes] | None:
    """读取完整性备份文件集合。"""
    row = fetch_one(
        f"SELECT manifest, blob_hashes FROM {_SCHEMA}.integrity_backup WHERE backup_id=%s",
        (backup_id,),
    )
    if row is None:
        return None
    manifest = row["manifest"] if isinstance(row["manifest"], dict) else {}
    names = list(manifest.get("files") or [])
    hashes = list(row["blob_hashes"] or [])
    result: dict[str, bytes] = {}
    for name, content_hash in zip(names, hashes, strict=False):
        data = blob.get_blob(str(content_hash))
        if data is not None:
            result[str(name)] = data
    return result


def shared_plugin_path(name: str, *parts: str) -> Path:
    """返回用户插件目录内路径。"""
    from app.state.shared import shared_plugin_root

    return shared_plugin_root().joinpath(name, *parts)


def _mime(ext: str) -> str:
    """按扩展名返回 MIME。"""
    return {
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
        "ogg": "audio/ogg",
    }.get(ext.lower(), "application/octet-stream")
