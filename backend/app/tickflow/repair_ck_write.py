"""CK 修补写入的跨实例租约与审计封装。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

import polars as pl

_DiffBuilder = Callable[[pl.DataFrame, pl.DataFrame, list[str] | None, set[str]], pl.DataFrame]


def apply_ck_repair(
    store: Any,
    table: str,
    day: date,
    day_rows: pl.DataFrame,
    before: pl.DataFrame,
    incoming: pl.DataFrame,
    merged: pl.DataFrame,
    symbols: list[str] | None,
    removals: set[str],
    actor: str,
    diff_builder: _DiffBuilder,
) -> None:
    """在租约内重读比对，然后写快照、审计与事实表。"""
    from app.services.coordination import coordination

    # 同分区修补由 Redis/memory 租约串行化，防止跨实例读-改-写竞态。
    with coordination.lock(f"repair:{table}:{day}") as acquired:
        if not acquired:
            raise ValueError("同一分区已有修补在执行, 请稍后重试")
        # 租约内以全日快照比对，保留原有"变化即拒绝"语义。
        current = store.read_daily(table, start=day, end=day)
        if not current.equals(day_rows):
            raise ValueError("修补期间发生变化, 请重新自检")
        # fencing：写快照/审计/事实表前确认租约未被接管（DIST-13）。若本 worker
        # 曾卡到租约过期、另一实例已接管同分区，此处拒绝迟到写入，避免双写污染。
        coordination.fence(f"repair:{table}:{day}", acquired)
        repair_id = store.begin_repair(table, day, "precise", actor)
        try:
            store.append_repair_snapshot(repair_id, table, day, before, actor)
            store.append_repair_audit(
                repair_id, table, day, diff_builder(before, incoming, symbols, removals), actor
            )
            if symbols is None:
                store.delete_daily_date(table, day=day)
            else:
                store.delete_daily(table, day=day, symbols=list(symbols))
            store.write_daily(table, merged)
            store.set_repair_status(repair_id, "committed", actor)
        except BaseException:
            store.set_repair_status(repair_id, "failed", actor)
            raise
