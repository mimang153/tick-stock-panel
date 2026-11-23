"""旧 data 用户状态迁移工具（DES-006）。

把旧文件态一次性导入 PG/Redis/CK 与 shared 目录；--dry-run 只扫描，
--verify 做目标计数校验。迁移只读旧文件，验证通过前不删除任何源文件。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings
from app.state import (
    get_daily_integrity_snapshot,
    get_latest_integrity_report,
    get_preferences,
    get_secrets,
    init_state_pg,
    list_api_tokens,
    put_daily_integrity_snapshot,
    record_migration_audit,
    record_repair_attempt,
    save_capability_cache,
    save_integrity_report,
    shared_plugin_root,
    shared_strategy_root,
)
from app.state.pg import fetch_one
from app.state.preferences import replace_preferences
from app.state.secrets import replace_secrets
from app.state.tokens import replace_tokens

Migration = Callable[[Path, bool], dict[str, Any]]


def _read_json(path: Path, default: Any) -> Any:
    """读取 JSON，不存在或损坏时返回默认值。"""
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _checksum(path: Path) -> str:
    """返回文件 SHA-256，不存在返回空串。"""
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""


def _copy_tree(source: Path, target: Path, apply: bool) -> int:
    """复制目录；dry-run 只统计文件数。"""
    if not source.is_dir():
        return 0
    files = [path for path in source.rglob("*") if path.is_file()]
    if apply:
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target, dirs_exist_ok=True)
    return len(files)


def migrate_auth(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 auth.json 的密码哈希；会话不迁移。"""
    path = source / "user_data" / "auth.json"
    doc = _read_json(path, {})
    if apply and doc.get("password_hash"):
        from app.state.auth import save_auth

        save_auth(
            str(doc["password_hash"]),
            str(doc.get("password_salt", "")),
            hash_algo=str(doc.get("hash_algo", "pbkdf2_sha256")),
            iterations=int(doc.get("iterations", 200000)),
            actor="migration",
        )
    return {"source": str(path), "checksum": _checksum(path), "count": int(bool(doc))}


def migrate_tokens(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 api_tokens.json。"""
    path = source / "user_data" / "api_tokens.json"
    doc = _read_json(path, {})
    tokens = doc.get("tokens", []) if isinstance(doc, dict) else doc
    if apply and tokens:
        replace_tokens(list(tokens))
    return {"source": str(path), "checksum": _checksum(path), "count": len(tokens)}


def migrate_secrets(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 secrets.json 到 PG 加密存储。"""
    path = source / "user_data" / "secrets.json"
    doc = _read_json(path, {})
    if apply and doc:
        replace_secrets(dict(doc))
    return {"source": str(path), "checksum": _checksum(path), "count": len(doc)}


def migrate_preferences(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 preferences.json。"""
    path = source / "user_data" / "preferences.json"
    doc = _read_json(path, {})
    if apply and doc:
        replace_preferences(dict(doc))
    return {"source": str(path), "checksum": _checksum(path), "count": len(doc)}


def migrate_capabilities(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 capabilities.json（schema_version=6）。"""
    path = source / "capabilities.json"
    doc = _read_json(path, {})
    schema_version = int(doc.get("schema_version", 0))
    count = len(doc) if schema_version == 6 else 0
    if apply and count:
        save_capability_cache(dict(doc), actor="migration")
    return {"source": str(path), "checksum": _checksum(path), "count": count}


def migrate_watchlist(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 watchlist.parquet 与分组 JSON 到 PG 三表。

    兼容旧 schema：单列 group_id 提升为 group_ids 列表；缺列补默认值。
    只读旧文件，apply 时整表替换 state.watchlist 并审计登记。
    """
    import polars as pl

    entries_path = source / "user_data" / "watchlist.parquet"
    groups_path = source / "user_data" / "watchlist_groups.json"

    rows: list[dict[str, Any]] = []
    if entries_path.exists():
        entries = pl.read_parquet(entries_path)
        if "group_ids" not in entries.columns:
            legacy = (
                entries["group_id"].to_list()
                if "group_id" in entries.columns
                else [None] * len(entries)
            )
            entries = entries.with_columns(
                pl.Series("group_ids", [[g] if g else [] for g in legacy], dtype=pl.List(pl.Utf8))
            )
        for index, item in enumerate(entries.to_dicts()):
            rows.append(
                {
                    "symbol": str(item.get("symbol") or ""),
                    "note": str(item.get("note") or ""),
                    "added_at": str(item.get("added_at") or ""),
                    "position": index,
                    "group_ids": [str(g) for g in (item.get("group_ids") or [])],
                }
            )
    groups = _read_json(groups_path, [])
    normalized_groups: list[dict[str, Any]] = []
    if isinstance(groups, list):
        for index, group in enumerate(groups):
            if isinstance(group, dict) and group.get("id") and group.get("name"):
                normalized_groups.append(
                    {
                        "id": str(group["id"]),
                        "name": str(group["name"]),
                        "color": group.get("color", "sky"),
                        "position": index,
                    }
                )

    if apply and (rows or normalized_groups):
        from app.services.watchlist import _GROUP_COLOR_INDEX
        from app.state.watchlist import (
            save_watchlist_groups,
            save_watchlist_items,
        )

        for group in normalized_groups:
            group["color"] = _GROUP_COLOR_INDEX.get(str(group["color"]), 0)
        save_watchlist_items(rows, actor="migration")
        save_watchlist_groups(normalized_groups, actor="migration")
        record_migration_audit(
            "watchlist",
            str(entries_path),
            "state.watchlist",
            "applied",
            _checksum(entries_path),
            f"{len(rows)} entries, {len(normalized_groups)} groups",
        )
    return {
        "source": str(entries_path),
        "checksum": _checksum(entries_path),
        "count": len(rows) + len(normalized_groups),
    }


def migrate_integrity(source: Path, apply: bool) -> dict[str, Any]:
    """迁移完整性状态与报告。"""
    state_path = source / "user_data" / "daily_integrity_state.json"
    report_path = source / "user_data" / "integrity_report.json"
    state_doc = _read_json(state_path, {})
    report = _read_json(report_path, {})
    if apply and state_doc:
        put_daily_integrity_snapshot(dict(state_doc), actor="migration")
    if apply and report:
        save_integrity_report(dict(report), actor="migration")
    return {
        "source": str(state_path),
        "checksum": _checksum(state_path),
        "count": len(state_doc.get("entries", [])) + int(bool(report)),
    }


def migrate_backoff(source: Path, apply: bool) -> dict[str, Any]:
    """迁移修补冷却；旧结构只有 last_attempt 单键。"""
    path = source / ".integrity_repair_backoff.json"
    doc = _read_json(path, {})
    last_attempt = doc.get("last_attempt") or {}
    if apply:
        for key, value in last_attempt.items():
            try:
                day = date.fromisoformat(str(key))
            except ValueError:
                continue
            record_repair_attempt(day)
            _ = value
    return {"source": str(path), "checksum": _checksum(path), "count": len(last_attempt)}


def migrate_jobs(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 job_store/*.json：终态写 CK 历史，未终态标 interrupted 写 Redis。"""
    job_dir = source / "job_store"
    files = sorted(job_dir.glob("*.json")) if job_dir.is_dir() else []
    terminal = 0
    interrupted = 0
    if apply and files:
        from app.state.jobs import put_job_state, write_job_history

        for path in files:
            job = _read_json(path, {})
            if not isinstance(job, dict) or not job.get("id"):
                continue
            if job.get("status") in ("pending", "running"):
                job["status"] = "failed"
                job["error"] = "后端迁移后重启，任务中断"
                interrupted += 1
            else:
                terminal += 1
            put_job_state(job)
            write_job_history(job)
        record_migration_audit(
            "job_store",
            str(job_dir),
            "state.jobs",
            "applied",
            "",
            f"{terminal} terminal to CK history, {interrupted} interrupted to Redis live",
        )
    return {
        "source": str(job_dir),
        "checksum": "",
        "count": len(files),
        "terminal": terminal,
        "interrupted": interrupted,
    }


def migrate_data_sources(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 data_sources 配置为 PG。"""
    directory = source / "data_sources"
    files = list(directory.glob("*")) if directory.is_dir() else []
    if apply:
        from app.state.objects import save_data_source_config

        for path in files:
            if path.is_file():
                save_data_source_config(
                    path.stem, path.read_text(encoding="utf-8"), actor="migration"
                )
    return {"source": str(directory), "checksum": "", "count": len(files)}


def migrate_shared(source: Path, apply: bool) -> dict[str, Any]:
    """迁移 strategies/plugins 到 shared 目录。"""
    strategy_count = _copy_tree(source / "strategies", shared_strategy_root(), apply)
    plugin_count = _copy_tree(source / "plugins", shared_plugin_root(), apply)
    return {
        "source": str(source),
        "checksum": "",
        "count": strategy_count + plugin_count,
    }


def _migrations() -> dict[str, Migration]:
    """返回有序迁移项。"""
    return {
        "auth": migrate_auth,
        "api_tokens": migrate_tokens,
        "secrets": migrate_secrets,
        "preferences": migrate_preferences,
        "capabilities": migrate_capabilities,
        "watchlist": migrate_watchlist,
        "integrity": migrate_integrity,
        "repair_backoff": migrate_backoff,
        "jobs": migrate_jobs,
        "data_sources": migrate_data_sources,
        "shared": migrate_shared,
    }


def verify() -> dict[str, Any]:
    """验证关键目标可读。"""
    return {
        "auth": bool(
            fetch_one(
                f"SELECT id FROM {settings.user_state_pg_schema}.user_auth WHERE singleton_key=%s",
                ("default",),
            )
        ),
        "preferences": bool(get_preferences()),
        "secrets": bool(get_secrets()),
        "tokens": len(list_api_tokens()),
        "integrity": bool(get_daily_integrity_snapshot().get("entries"))
        or bool(get_latest_integrity_report()),
    }


def main() -> int:
    """执行迁移/校验。"""
    parser = argparse.ArgumentParser(description="migrate user state to PG/Redis/CK")
    parser.add_argument("--source-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if not (args.dry_run or args.apply or args.verify):
        parser.error("one of --dry-run/--apply/--verify is required")

    if not args.dry_run:
        init_state_pg()
    result: dict[str, Any] = {"source": str(args.source_dir), "items": {}}
    if args.dry_run or args.apply:
        for name, migration in _migrations().items():
            item = migration(args.source_dir, args.apply)
            result["items"][name] = item
            if args.apply:
                record_migration_audit(
                    name,
                    str(item.get("source", "")),
                    name,
                    "applied",
                    str(item.get("checksum", "")),
                    json.dumps(item, ensure_ascii=False),
                )
    if args.verify:
        result["verify"] = verify()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
