from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any


class ClickHouseGenerationMixin:
    """数据 generation 生命周期。"""

    # 以下属性由组装类 ClickHouseStore / CoreMixin 提供
    _client: Any
    _table: Any

    def _generation_row(self, asset_type: str, generation_id: str) -> dict:
        """构造 generation 表的一行记录。"""
        df = self._client.query(
            "SELECT asset_type, generation_id, state, changed_from, heartbeat_at, "
            "committed_at, created_by, created_at, updated_by, updated_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} AND generation_id = {generation_id:String} "
            "ORDER BY updated_at DESC LIMIT 1",
            {"asset_type": asset_type, "generation_id": generation_id},
        )
        return df.row(0, named=True) if not df.is_empty() else {}

    def begin_generation(self, asset_type: str, actor: str, changed_from: date | None) -> str:
        """开启一次数据 generation。"""
        generation_id = str(uuid.uuid4())
        now = datetime.now()
        self._client.insert_rows(
            self._table("ck_enriched_generation"),
            [
                [
                    generation_id,
                    asset_type,
                    "publishing",
                    changed_from,
                    now,
                    None,
                    actor,
                    now,
                    actor,
                    now,
                ]
            ],
            column_names=[
                "generation_id",
                "asset_type",
                "state",
                "changed_from",
                "heartbeat_at",
                "committed_at",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )
        return generation_id

    def heartbeat_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """上报 generation 心跳。"""
        now = datetime.now()
        current = self._generation_row(asset_type, generation_id)
        self._client.insert_rows(
            self._table("ck_enriched_generation"),
            [
                [
                    generation_id,
                    asset_type,
                    "publishing",
                    current.get("changed_from"),
                    now,
                    None,
                    current.get("created_by", actor),
                    current.get("created_at", now),
                    actor,
                    now,
                ]
            ],
            column_names=[
                "generation_id",
                "asset_type",
                "state",
                "changed_from",
                "heartbeat_at",
                "committed_at",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def commit_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """提交 generation 成功。"""
        now = datetime.now()
        current = self._generation_row(asset_type, generation_id)
        self._client.insert_rows(
            self._table("ck_enriched_generation"),
            [
                [
                    generation_id,
                    asset_type,
                    "ready",
                    current.get("changed_from"),
                    now,
                    now,
                    current.get("created_by", actor),
                    current.get("created_at", now),
                    actor,
                    now,
                ]
            ],
            column_names=[
                "generation_id",
                "asset_type",
                "state",
                "changed_from",
                "heartbeat_at",
                "committed_at",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def fail_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """标记 generation 失败。"""
        now = datetime.now()
        current = self._generation_row(asset_type, generation_id)
        self._client.insert_rows(
            self._table("ck_enriched_generation"),
            [
                [
                    generation_id,
                    asset_type,
                    "failed",
                    current.get("changed_from"),
                    now,
                    None,
                    current.get("created_by", actor),
                    current.get("created_at", now),
                    actor,
                    now,
                ]
            ],
            column_names=[
                "generation_id",
                "asset_type",
                "state",
                "changed_from",
                "heartbeat_at",
                "committed_at",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def current_generation(self, asset_type: str) -> dict:
        """读取当前 generation。"""
        df = self._client.query(
            "SELECT asset_type, generation_id, state, changed_from, heartbeat_at, "
            "committed_at, created_by, created_at, updated_by, updated_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} AND state = 'ready' "
            "ORDER BY updated_at DESC LIMIT 1",
            {"asset_type": asset_type},
        )
        return df.row(0, named=True) if not df.is_empty() else {}

    def generation_history(self, asset_type: str, limit: int) -> list[dict]:
        """按提交时间倒序返回 ready generation 历史，供重建 range 语义快照。"""
        df = self._client.query(
            "SELECT generation_id, changed_from, committed_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} AND state = 'ready' "
            "ORDER BY committed_at DESC LIMIT {limit:UInt32}",
            {"asset_type": asset_type, "limit": limit},
        )
        return df.to_dicts()

    def publishing_generations(self, asset_type: str) -> list[dict]:
        """返回未终结的 generation（含心跳时间），供在线判定发布是否仍在进行。"""
        df = self._client.query(
            "SELECT generation_id, state, heartbeat_at, updated_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} AND state = 'publishing' "
            "ORDER BY updated_at DESC",
            {"asset_type": asset_type},
        )
        return df.to_dicts()
