from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any

import polars as pl

from app.storage.protocol import DAILY_TABLES

# 修补单状态机（DES-002 §9 CAS）。begin_repair 直接写 started，不经 set_repair_status；
# rollback 允许从 started（测试/手工）或 committed 发起；failed/rollback 为终态。
_REPAIR_STATUS_TRANSITIONS: dict[str, frozenset[str]] = {
    "started": frozenset({"committed", "failed", "rollback"}),
    "committed": frozenset({"rollback"}),
}


class ClickHouseRepairMixin:
    """数据修复快照与审计。"""

    # 以下属性/方法由组装类 ClickHouseStore / CoreMixin / MarketMixin 提供
    _client: Any
    _table: Any
    _row_json: Any
    _require: Any
    delete_daily: Any
    write: Any

    def begin_repair(self, dataset: str, day: date, reason: str, actor: str) -> str:
        """开启一次数据修复记录。
        写入修复范围，供后续快照与审计关联。"""
        repair_id = str(uuid.uuid4())
        now = datetime.now()
        self._client.insert_rows(
            self._table("ck_repair_audit"),
            [
                [
                    repair_id,
                    dataset,
                    day,
                    "*",
                    "begin",
                    None,
                    None,
                    reason,
                    "started",
                    actor,
                    now,
                    actor,
                    now,
                ]
            ],
            column_names=[
                "repair_id",
                "dataset",
                "day",
                "symbol",
                "change_type",
                "before_json",
                "after_json",
                "reason",
                "status",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )
        return repair_id

    def append_repair_snapshot(
        self, repair_id: str, dataset: str, day: date, rows: pl.DataFrame, actor: str
    ) -> None:
        """追加修复快照行。
        保存修复前原始数据以便回滚。"""
        if rows.is_empty():
            return
        now = datetime.now()
        values = []
        for idx in range(rows.height):
            payload = rows.row(idx, named=True)
            values.append(
                [
                    repair_id,
                    dataset,
                    day,
                    str(payload.get("symbol", "*")),
                    self._row_json(payload),
                    actor,
                    now,
                    actor,
                    now,
                ]
            )
        self._client.insert_rows(
            self._table("ck_repair_snapshot"),
            values,
            column_names=[
                "repair_id",
                "dataset",
                "day",
                "symbol",
                "payload_json",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def append_repair_audit(
        self, repair_id: str, dataset: str, day: date, diffs: pl.DataFrame, actor: str
    ) -> None:
        """追加修复审计行。
        留下操作人与时间等审计信息。"""
        if diffs.is_empty():
            return
        required = {"symbol", "before_json", "after_json"}
        if not required.issubset(diffs.columns):
            raise ValueError(f"审计差异缺少列: {sorted(required - set(diffs.columns))}")
        now = datetime.now()
        values = []
        for idx in range(diffs.height):
            row = diffs.row(idx, named=True)
            values.append(
                [
                    repair_id,
                    dataset,
                    day,
                    str(row["symbol"]),
                    str(row.get("change_type") or "update"),
                    row["before_json"],
                    row["after_json"],
                    "repair",
                    "committed",
                    actor,
                    now,
                    actor,
                    now,
                ]
            )
        self._client.insert_rows(
            self._table("ck_repair_audit"),
            values,
            column_names=[
                "repair_id",
                "dataset",
                "day",
                "symbol",
                "change_type",
                "before_json",
                "after_json",
                "reason",
                "status",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def read_repair_status(self, repair_id: str) -> str | None:
        """读取修补单最新状态（MergeTree 追加日志按 updated_at 取最后一条）。"""
        df = self._client.query(
            f"SELECT status FROM {self._table('ck_repair_audit')} "
            "WHERE repair_id = {repair_id:String} AND change_type IN ('begin', 'status') "
            "ORDER BY updated_at DESC LIMIT 1",
            {"repair_id": repair_id},
        )
        return str(df["status"][0]) if not df.is_empty() else None

    def set_repair_status(self, repair_id: str, status: str, actor: str) -> None:
        """更新修复任务状态。

        CAS 语义：按 repair_id 读最新状态并校验流转合法，终态不可再次写入，
        防止跨实例/迟到的状态更新覆盖已结束的修补单（DES-002 §9）。
        read-then-write 用跨实例租约串行化（redis 后端），单进程用实例锁；
        否则两个 worker 同时读到 started 可各自写终态，CAS 形同虚设（DIST-06）。
        """
        from app.services.coordination import coordination

        def _apply() -> None:
            current = self.read_repair_status(repair_id)
            allowed = _REPAIR_STATUS_TRANSITIONS.get(current or "", frozenset())
            if status not in allowed:
                raise ValueError(f"非法修补状态流转: {current!r} -> {status!r}")
            self._write_repair_status(repair_id, status, actor)

        with coordination.lock(f"repair_status:{repair_id}") as acquired:
            if not acquired:
                raise ValueError(f"修补状态更新正在被其他实例执行: {repair_id}")
            _apply()

    def _write_repair_status(self, repair_id: str, status: str, actor: str) -> None:
        """在已持有 repair_id 租约的前提下追加一条状态审计行。"""
        now = datetime.now()
        self._client.insert_rows(
            self._table("ck_repair_audit"),
            [
                [
                    repair_id,
                    "",
                    date(1970, 1, 1),
                    "*",
                    "status",
                    None,
                    None,
                    "",
                    status,
                    actor,
                    now,
                    actor,
                    now,
                ]
            ],
            column_names=[
                "repair_id",
                "dataset",
                "day",
                "symbol",
                "change_type",
                "before_json",
                "after_json",
                "reason",
                "status",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def read_repair_snapshot(self, repair_id: str) -> pl.DataFrame:
        """读取修复快照。"""
        return self._client.query(
            f"SELECT dataset, day, symbol, payload_json FROM {self._table('ck_repair_snapshot')} "
            "WHERE repair_id = {repair_id:String} ORDER BY symbol",
            {"repair_id": repair_id},
        )

    def rollback_repair(self, repair_id: str, actor: str) -> None:
        """回滚已应用的修复。"""
        snapshot = self.read_repair_snapshot(repair_id)
        if snapshot.is_empty():
            raise ValueError(f"修补单 {repair_id} 没有可回滚快照")
        dataset = snapshot["dataset"][0]
        day = snapshot["day"][0]
        self._require(dataset, DAILY_TABLES)
        symbols = snapshot["symbol"].drop_nulls().to_list()
        rows = [json.loads(payload) for payload in snapshot["payload_json"].to_list()]
        frame = pl.DataFrame(rows)
        if "date" in frame.columns:
            frame = frame.with_columns(pl.col("date").str.to_date(strict=False))
        if "updated_at" in frame.columns:
            frame = frame.with_columns(pl.col("updated_at").str.to_datetime(strict=False))
        self.delete_daily(dataset, day=day, symbols=symbols)
        self.write(dataset, frame)
        self.set_repair_status(repair_id, "rollback", actor)
