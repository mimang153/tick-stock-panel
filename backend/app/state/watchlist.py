"""自选状态门面（DES-006）。PG 三表承载全量自选与分组，Redis 存版本号。"""

from __future__ import annotations

import uuid
from typing import Any

from app.config import settings
from app.state import cache
from app.state.pg import fetch_all, transaction

_SCHEMA = settings.user_state_pg_schema


def list_watchlist_items() -> list[dict[str, Any]]:
    """读取全部自选标的，保持 position 顺序。"""
    rows = fetch_all(
        f"SELECT symbol, note, position, added_at FROM {_SCHEMA}.watchlist_item "
        "ORDER BY position ASC, added_at ASC"
    )
    membership = fetch_all(f"SELECT symbol, group_id FROM {_SCHEMA}.watchlist_membership")
    grouped: dict[str, list[str]] = {}
    for row in membership:
        grouped.setdefault(str(row["symbol"]), []).append(str(row["group_id"]))
    return [
        {
            "symbol": str(row["symbol"]),
            "note": str(row["note"] or ""),
            "group_ids": grouped.get(str(row["symbol"]), []),
            "added_at": str(row["added_at"] or ""),
        }
        for row in rows
    ]


def save_watchlist_items(rows: list[dict[str, Any]], *, actor: str = "system") -> None:
    """在一个事务中整表替换自选及分组关系。"""
    with transaction() as conn:
        conn.execute(f"TRUNCATE TABLE {_SCHEMA}.watchlist_membership")
        conn.execute(f"TRUNCATE TABLE {_SCHEMA}.watchlist_item")
        for index, row in enumerate(rows):
            symbol = str(row["symbol"])
            conn.execute(
                f"INSERT INTO {_SCHEMA}.watchlist_item "
                "(id, symbol, note, position, added_at, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, COALESCE(%s::timestamptz, now()), %s, %s)",
                (
                    uuid.uuid4().hex,
                    symbol,
                    str(row.get("note") or ""),
                    int(row.get("position", index)),
                    row.get("added_at"),
                    actor,
                    actor,
                ),
            )
            for group_id in row.get("group_ids") or []:
                conn.execute(
                    f"INSERT INTO {_SCHEMA}.watchlist_membership "
                    "(id, symbol, group_id, created_by, updated_by) VALUES (%s, %s, %s, %s, %s)",
                    (uuid.uuid4().hex, symbol, str(group_id), actor, actor),
                )
    bump_watchlist_version()


def list_watchlist_groups() -> list[dict[str, Any]]:
    """读取全部分组，保持 position 顺序。"""
    rows = fetch_all(
        f"SELECT group_key, name, color, position FROM {_SCHEMA}.watchlist_group "
        "ORDER BY position ASC, group_key ASC"
    )
    return [
        {
            "id": str(row["group_key"]),
            "name": str(row["name"]),
            "color": int(row["color"] or 0),
            "position": int(row["position"] or 0),
        }
        for row in rows
    ]


def save_watchlist_groups(groups: list[dict[str, Any]], *, actor: str = "system") -> None:
    """在一个事务中整表替换分组。"""
    with transaction() as conn:
        conn.execute(f"TRUNCATE TABLE {_SCHEMA}.watchlist_group")
        for index, group in enumerate(groups):
            conn.execute(
                f"INSERT INTO {_SCHEMA}.watchlist_group "
                "(id, group_key, name, color, position, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    uuid.uuid4().hex,
                    str(group["id"]),
                    str(group["name"]),
                    int(group.get("color", 0)),
                    int(group.get("position", index)),
                    actor,
                    actor,
                ),
            )
    bump_watchlist_version()


def get_watchlist_version() -> int:
    """读取 Redis 中的自选版本号。"""
    cached = cache.get_json("watch:version")
    if isinstance(cached, dict):
        return int(cached.get("value", 0))
    return 0


def bump_watchlist_version() -> int:
    """递增自选版本号并发布失效事件。"""
    value = cache.bump_generation("watch")
    cache.set_json("watch:version", {"value": int(value)})
    cache.publish("watchlist-change", value)
    return int(value)
