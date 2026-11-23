"""自选与分组服务。

自选与分组的持久化经 ``app.state`` 门面落到 PG 三表, 版本号存 Redis; 本模块
只负责业务顺序与成员关系, 不再直接读写本地 parquet/json 文件。

成员关系为多值 (M:N): 每条自选带 ``group_ids: list[str]``, 同一标的可同时
属于多个分组; 移出分组只摘标签(标的仍在自选), 移出自选才删除实体。
"""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl

from app.config import settings
from app.state import (
    get_watchlist_version,
    list_watchlist_groups,
    list_watchlist_items,
    save_watchlist_groups,
    save_watchlist_items,
)
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.client import get_client
from app.tickflow.rate_limits import chunked, resolve_limit

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()


def revision() -> int:
    """自选/分组数据版本号, 每次写操作由门面递增。"""
    return get_watchlist_version()


_MAX_GROUP_NAME_LENGTH = 24
DEFAULT_GROUP_COLOR = "sky"
# 颜色编号与前端 WATCHLIST_GROUP_COLORS 顺序一致; 门面存编号, 服务层对外仍是颜色名。
_GROUP_COLOR_SEQUENCE = (
    "sky",
    "blue",
    "indigo",
    "violet",
    "fuchsia",
    "rose",
    "orange",
    "amber",
    "lime",
    "emerald",
    "teal",
    "cyan",
)
GROUP_COLORS = frozenset(_GROUP_COLOR_SEQUENCE)
_GROUP_COLOR_INDEX = {name: index for index, name in enumerate(_GROUP_COLOR_SEQUENCE)}
_ENTRY_SCHEMA: pl.Schema = cast(
    "pl.Schema",
    {
        "symbol": pl.Utf8,
        "added_at": pl.Utf8,
        "note": pl.Utf8,
        "group_ids": pl.List(pl.Utf8),
    },
)


def _path() -> Path:
    """兼容旧调用的虚拟路径；门面持久化后不再读写该文件。"""
    return settings.data_dir / "user_data" / "watchlist.parquet"


def _groups_path() -> Path:
    """兼容旧调用的虚拟路径；门面持久化后不再读写该文件。"""
    return settings.data_dir / "user_data" / "watchlist_groups.json"


def _empty_entries() -> pl.DataFrame:
    return pl.DataFrame(schema=_ENTRY_SCHEMA)


def _read_entries() -> pl.DataFrame:
    """从状态门面读取全部自选并归一化为服务层列结构。"""
    rows = list_watchlist_items()
    if not rows:
        return _empty_entries()
    normalized = [
        {
            "symbol": str(row.get("symbol") or ""),
            "added_at": str(row.get("added_at") or ""),
            "note": str(row.get("note") or ""),
            "group_ids": [str(group_id) for group_id in (row.get("group_ids") or [])],
        }
        for row in rows
    ]
    return pl.DataFrame(normalized, schema=_ENTRY_SCHEMA)


def _write_entries(df: pl.DataFrame) -> None:
    """把自选整表写回门面；行序即展示顺序，由 position 固化。"""
    rows = df.select(list(_ENTRY_SCHEMA)).to_dicts()
    save_watchlist_items(
        [
            {
                "symbol": row["symbol"],
                "note": row.get("note") or "",
                "position": index,
                "added_at": row.get("added_at") or None,
                "group_ids": row.get("group_ids") or [],
            }
            for index, row in enumerate(rows)
        ]
    )


def _color_from_state(value: object) -> str:
    """把门面存储的颜色编号还原为服务层颜色名。"""
    if isinstance(value, str) and value in GROUP_COLORS:
        return value
    if isinstance(value, int | float) and 0 <= int(value) < len(_GROUP_COLOR_SEQUENCE):
        return _GROUP_COLOR_SEQUENCE[int(value)]
    return DEFAULT_GROUP_COLOR


def _read_groups() -> list[dict]:
    """从状态门面读取分组；内部颜色编号对外仍暴露颜色名。"""
    return [
        {
            "id": str(row["id"]),
            "name": str(row["name"]),
            "color": _color_from_state(row.get("color")),
        }
        for row in list_watchlist_groups()
    ]


def _write_groups(groups: list[dict]) -> None:
    """把分组整表写回门面；颜色名转编号，入参顺序即展示顺序。"""
    save_watchlist_groups(
        [
            {
                "id": group["id"],
                "name": group["name"],
                "color": _GROUP_COLOR_INDEX.get(group.get("color") or DEFAULT_GROUP_COLOR, 0),
                "position": index,
            }
            for index, group in enumerate(groups)
        ]
    )


def _normalize_group_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise ValueError("分组名称不能为空")
    if len(normalized) > _MAX_GROUP_NAME_LENGTH:
        raise ValueError(f"分组名称不能超过 {_MAX_GROUP_NAME_LENGTH} 个字符")
    return normalized


def _normalize_group_color(color: str | None) -> str:
    normalized = (color or DEFAULT_GROUP_COLOR).strip().lower()
    if normalized not in GROUP_COLORS:
        raise ValueError("不支持的分组颜色")
    return normalized


def _validate_group_id(group_id: str | None, groups: list[dict]) -> None:
    if group_id is not None and not any(group["id"] == group_id for group in groups):
        raise ValueError("自选分组不存在")


def list_symbols() -> list[dict]:
    with _LOCK:
        df = _read_entries()
        return [] if df.is_empty() else df.to_dicts()


def add(symbol: str, note: str = "", group_id: str | None = None) -> list[dict]:
    rows, _ = add_batch([symbol], note=note, group_id=group_id)
    return rows


def add_batch(
    symbols: list[str],
    note: str = "",
    group_id: str | None = None,
    group_ids: list[str] | None = None,
) -> tuple[list[dict], int]:
    """批量添加并保持既有语义：每个新处理的标的移动到列表最前面。

    分组为可选的初始分组：``group_id`` 单组（如从某分组页添加）或 ``group_ids``
    多组（如批量导入同时并入多个分组）。重复添加的标的保留既有全部分组，
    仅把尚未属于的传入分组并入；二者可同时使用、内部去重。
    """
    with _LOCK:
        groups = _read_groups()
        # 合并单/多组参数并去重；逐组校验存在性
        apply_ids: list[str] = []
        for gid in (group_ids or []) + ([group_id] if group_id is not None else []):
            if gid in apply_ids:
                continue
            _validate_group_id(gid, groups)
            apply_ids.append(gid)
        rows = _read_entries().to_dicts()
        added = 0
        for symbol in symbols:
            existing = next((row for row in rows if row["symbol"] == symbol), None)
            if existing is None:
                added += 1
            rows = [row for row in rows if row["symbol"] != symbol]
            gids = list((existing or {}).get("group_ids") or [])
            for gid in apply_ids:
                if gid not in gids:
                    gids.append(gid)
            rows.insert(
                0,
                {
                    "symbol": symbol,
                    "added_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "note": note,
                    "group_ids": gids,
                },
            )
        out = pl.DataFrame(rows, schema=_ENTRY_SCHEMA) if rows else _empty_entries()
        _write_entries(out)
        return out.to_dicts(), added


def remove(symbol: str) -> list[dict]:
    with _LOCK:
        df = _read_entries().filter(pl.col("symbol") != symbol)
        _write_entries(df)
        return df.to_dicts()


def move_to_top(symbol: str) -> list[dict]:
    with _LOCK:
        df = _read_entries()
        if df.is_empty() or symbol not in df["symbol"].to_list():
            return df.to_dicts()
        target = df.filter(pl.col("symbol") == symbol)
        rest = df.filter(pl.col("symbol") != symbol)
        out = pl.concat([target, rest], how="diagonal_relaxed")
        _write_entries(out)
        return out.to_dicts()


def clear() -> int:
    """清空自选列表。返回移除的数量。"""
    with _LOCK:
        df = _read_entries()
        count = df.height
        if count > 0:
            _write_entries(_empty_entries())
        return count


def list_groups() -> list[dict]:
    with _LOCK:
        return _read_groups()


def create_group(name: str, color: str | None = None) -> tuple[list[dict], dict]:
    with _LOCK:
        normalized = _normalize_group_name(name)
        normalized_color = _normalize_group_color(color)
        groups = _read_groups()
        if any(group["name"].casefold() == normalized.casefold() for group in groups):
            raise ValueError("分组名称已存在")
        group = {
            "id": uuid.uuid4().hex,
            "name": normalized,
            "color": normalized_color,
        }
        groups.append(group)
        _write_groups(groups)
        return groups, group


def rename_group(group_id: str, name: str, color: str | None = None) -> list[dict]:
    with _LOCK:
        normalized = _normalize_group_name(name)
        groups = _read_groups()
        target = next((group for group in groups if group["id"] == group_id), None)
        if target is None:
            raise KeyError(group_id)
        if any(
            group["id"] != group_id and group["name"].casefold() == normalized.casefold()
            for group in groups
        ):
            raise ValueError("分组名称已存在")
        target["name"] = normalized
        if color is not None:
            target["color"] = _normalize_group_color(color)
        _write_groups(groups)
        return groups


def reorder_groups(ordered_ids: list[str]) -> list[dict]:
    """按给定 id 顺序重排分组 (json 数组顺序即定义顺序)。"""
    with _LOCK:
        groups = _read_groups()
        by_id = {group["id"]: group for group in groups}
        if len(ordered_ids) != len(groups) or set(ordered_ids) != set(by_id):
            raise ValueError("分组顺序与现有分组不一致")
        reordered = [by_id[group_id] for group_id in ordered_ids]
        _write_groups(reordered)
        return reordered


def delete_group(group_id: str) -> tuple[list[dict], list[dict]]:
    """删除分组定义，原分组内的自选保留并转为未分组(仅摘掉该组标签)。"""
    with _LOCK:
        groups = _read_groups()
        if not any(group["id"] == group_id for group in groups):
            raise KeyError(group_id)
        df = _strip_group(_read_entries(), group_id)
        remaining = [group for group in groups if group["id"] != group_id]
        _write_entries(df)
        _write_groups(remaining)
        return remaining, df.to_dicts()


def set_group(symbol: str, group_id: str | None) -> list[dict]:
    """互斥设定: 该标的只保留这一个分组(group_id=None 即全部移出, 变未分组)。

    多组模型的日常操作走 add_to_group / remove_from_group; 本函数服务于
    「仅保留此组」的显式场景。
    """
    with _LOCK:
        groups = _read_groups()
        _validate_group_id(group_id, groups)
        rows = _read_entries().to_dicts()
        if not any(row["symbol"] == symbol for row in rows):
            raise KeyError(symbol)
        for row in rows:
            if row["symbol"] == symbol:
                row["group_ids"] = [group_id] if group_id is not None else []
        out = pl.DataFrame(rows, schema=_ENTRY_SCHEMA)
        _write_entries(out)
        return out.to_dicts()


def add_to_group(symbol: str, group_id: str) -> list[dict]:
    """把标的加入一个分组(多组成员关系: 不影响已属于的其他分组)。"""
    with _LOCK:
        groups = _read_groups()
        _validate_group_id(group_id, groups)
        rows = _read_entries().to_dicts()
        if not any(row["symbol"] == symbol for row in rows):
            raise KeyError(symbol)
        for row in rows:
            if row["symbol"] == symbol:
                gids = row["group_ids"] or []
                if group_id not in gids:
                    gids.append(group_id)
                    row["group_ids"] = gids
        out = pl.DataFrame(rows, schema=_ENTRY_SCHEMA)
        _write_entries(out)
        return out.to_dicts()


def transfer_members(
    source_group_id: str,
    target_group_id: str,
    symbols: list[str],
    action: str,
) -> tuple[list[dict], int]:
    """一次保存整批成员关系；过期的来源或选择不产生部分写入。"""
    wanted = set(symbols)
    if not wanted or action not in {"move", "copy"}:
        raise ValueError("请选择自选标的及有效的分组操作")
    if source_group_id == target_group_id:
        raise ValueError("请选择其他目标分组")
    if source_group_id == "all" and action != "copy":
        raise ValueError("请先选中要移出的分组")
    if source_group_id == "ungrouped" and action != "move":
        raise ValueError("未分组标的请选择移动到目标分组")

    with _LOCK:
        groups = _read_groups()
        _validate_group_id(target_group_id, groups)
        if source_group_id not in {"all", "ungrouped"}:
            _validate_group_id(source_group_id, groups)
        rows = _read_entries().to_dicts()
        selected = [row for row in rows if row["symbol"] in wanted]
        if len(selected) != len(wanted):
            raise ValueError("所选标的已不在自选中，请刷新后重试")
        changed = 0
        for row in selected:
            previous = row["group_ids"] or []
            in_source = (
                source_group_id == "all"
                or (source_group_id == "ungrouped" and not previous)
                or source_group_id in previous
            )
            if not in_source:
                raise ValueError("分组成员已变化，请刷新后重新选择")
            members = [g for g in previous if action != "move" or g != source_group_id]
            if target_group_id not in members:
                members.append(target_group_id)
            if members != previous:
                row["group_ids"] = members
                changed += 1
        if changed:
            _write_entries(pl.DataFrame(rows, schema=_ENTRY_SCHEMA))
        return rows, changed


def remove_from_group(symbol: str, group_id: str) -> list[dict]:
    """把标的移出一个分组(仅摘本组标签; 标的仍在自选, 可能落入未分组)。"""
    with _LOCK:
        groups = _read_groups()
        _validate_group_id(group_id, groups)
        rows = _read_entries().to_dicts()
        if not any(row["symbol"] == symbol for row in rows):
            raise KeyError(symbol)
        for row in rows:
            if row["symbol"] == symbol:
                row["group_ids"] = [g for g in (row["group_ids"] or []) if g != group_id]
        out = pl.DataFrame(rows, schema=_ENTRY_SCHEMA)
        _write_entries(out)
        return out.to_dicts()


def _strip_group(df: pl.DataFrame, group_id: str) -> pl.DataFrame:
    """从所有条目的 group_ids 中摘掉指定分组(删除分组/清空分组共用)。"""
    rows = df.to_dicts()
    for row in rows:
        gids = row.get("group_ids") or []
        if group_id in gids:
            row["group_ids"] = [g for g in gids if g != group_id]
    return pl.DataFrame(rows, schema=_ENTRY_SCHEMA) if rows else _empty_entries()


def clear_group(group_id: str) -> list[dict]:
    """清空分组成员:把该分组标签从所有条目摘掉(变未分组),保留分组定义。"""
    with _LOCK:
        groups = _read_groups()
        if not any(group["id"] == group_id for group in groups):
            raise KeyError(group_id)
        df = _strip_group(_read_entries(), group_id)
        _write_entries(df)
        return df.to_dicts()


def fetch_quotes(symbols: list[str], capset: CapabilitySet, timeout_s: float = 8.0) -> list[dict]:
    """拉取实时行情。

    优先用 quote.batch;否则降级为 quote.by_symbol 单股请求。
    timeout_s: 单批次请求超时(秒)，防止 API 卡死阻塞整个请求。
    """
    if not symbols:
        return []

    tf = get_client()
    quotes: list[dict] = []

    # 走 batch
    if capset.has(Cap.QUOTE_BATCH):
        batch_size = resolve_limit(capset, Cap.QUOTE_BATCH, default_batch=50).batch
    elif capset.has(Cap.QUOTE_BY_SYMBOL):
        batch_size = resolve_limit(capset, Cap.QUOTE_BY_SYMBOL, default_batch=5).batch
    else:
        # 无任何实时行情能力(none/free 档走 free-api 服务器,不提供实时行情)
        # 提前返回空,避免发起注定失败的请求
        return []

    chunks = chunked(symbols, batch_size)

    # 用线程池为每个批次加超时保护
    pool = ThreadPoolExecutor(max_workers=1)
    for chunk in chunks:
        try:
            future = pool.submit(tf.quotes.get, symbols=chunk, as_dataframe=True)
            raw = future.result(timeout=timeout_s)
            if raw is None or len(raw) == 0:
                continue
            df = pl.from_pandas(raw)
            rename_map = {
                "last_price": "price",
                "ext.change_pct": "pct",
                "ext.name": "name",
            }
            df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})
            quotes.extend(df.to_dicts())
        except FuturesTimeout:
            logger.warning("quote fetch timeout (%.1fs) for %d symbols", timeout_s, len(chunk))
            break  # 超时后不再尝试后续批次
        except Exception as e:  # noqa: BLE001
            logger.warning("quote fetch failed for %d symbols: %s", len(chunk), e)
    pool.shutdown(wait=False)

    return quotes
