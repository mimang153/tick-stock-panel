from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Any

_LEASE_SECONDS = 600


class ClickHouseGenerationMixin:
    """数据 generation 生命周期。"""

    # 以下属性由组装类 ClickHouseStore / CoreMixin 提供
    _client: Any
    _table: Any

    def _latest_generation_row(self, asset_type: str) -> dict:
        """读取某资产最新 generation 的租约与 fencing 信息。"""
        df = self._client.query(
            "SELECT state, owner_id, fencing_token, lease_expires_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} "
            "ORDER BY updated_at DESC LIMIT 1",
            {"asset_type": asset_type},
        )
        return df.row(0, named=True) if not df.is_empty() else {}

    def _assert_lease(self, current: dict, owner_id: str, fencing_token: int) -> None:
        """拒绝非租约持有者或已过期租约的迟到写入。"""
        if str(current.get("owner_id") or "") != owner_id:
            raise PermissionError("generation owner mismatch")
        if int(current.get("fencing_token") or 0) != fencing_token:
            raise PermissionError("generation fencing token mismatch")
        # 租约到期后旧 owner 的迟到心跳/提交必须失效。
        expires = current.get("lease_expires_at")
        if expires is not None and expires <= datetime.now():
            raise TimeoutError("generation lease expired")

    def _generation_row(self, asset_type: str, generation_id: str) -> dict:
        """构造 generation 表的一行记录。"""
        df = self._client.query(
            "SELECT asset_type, generation_id, state, changed_from, heartbeat_at, "
            "owner_id, fencing_token, lease_expires_at, "
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

    def _insert_generation(
        self,
        *,
        generation_id: str,
        asset_type: str,
        state: str,
        changed_from: date | None,
        heartbeat_at: datetime,
        committed_at: datetime | None,
        owner_id: str,
        fencing_token: int,
        lease_expires_at: datetime | None,
        created_by: str,
        created_at: datetime,
        updated_by: str,
        updated_at: datetime,
    ) -> None:
        """写入一行 generation 事实；ReplacingMergeTree 按 updated_at 收敛。"""
        self._client.insert_rows(
            self._table("ck_enriched_generation"),
            [
                [
                    generation_id,
                    asset_type,
                    state,
                    changed_from,
                    heartbeat_at,
                    owner_id,
                    fencing_token,
                    lease_expires_at,
                    committed_at,
                    created_by,
                    created_at,
                    updated_by,
                    updated_at,
                ]
            ],
            column_names=[
                "generation_id",
                "asset_type",
                "state",
                "changed_from",
                "heartbeat_at",
                "owner_id",
                "fencing_token",
                "lease_expires_at",
                "committed_at",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

    def begin_generation_leased(
        self,
        asset_type: str,
        actor: str,
        changed_from: date | None,
        owner_id: str,
        lease_seconds: int = _LEASE_SECONDS,
    ) -> tuple[str, int]:
        """抢占未过期租约并返回 generation 与 fencing token。

        SELECT-then-INSERT 用按 asset_type 的跨实例租约串行化（DIST-05）：
        否则两 worker 同时看到过期租约会各插一行、拿到相同 token，fencing 失去意义。
        """
        from app.services.coordination import coordination

        with coordination.lock(f"generation_lease:{asset_type}") as acquired:
            if not acquired:
                raise RuntimeError(f"generation 抢占正在被其他实例执行: {asset_type}")
            latest = self._latest_generation_row(asset_type)
            expires = latest.get("lease_expires_at")
            # 只抢占已过期或无租约的 publishing；未过期租约由原 owner 独占。
            if (
                latest.get("state") == "publishing"
                and expires is not None
                and expires > datetime.now()
                and str(latest.get("owner_id") or "") != owner_id
            ):
                raise PermissionError("generation lease is held by another owner")
            token = int(latest.get("fencing_token") or 0) + 1
            generation_id = str(uuid.uuid4())
            now = datetime.now()
            self._insert_generation(
                generation_id=generation_id,
                asset_type=asset_type,
                state="publishing",
                changed_from=changed_from,
                heartbeat_at=now,
                committed_at=None,
                owner_id=owner_id,
                fencing_token=token,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                created_by=actor,
                created_at=now,
                updated_by=actor,
                updated_at=now,
            )
            return generation_id, token

    def heartbeat_generation_leased(
        self,
        asset_type: str,
        generation_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> None:
        """仅租约持有者可以续期心跳。"""
        current = self._generation_row(asset_type, generation_id)
        self._assert_lease(current, owner_id, fencing_token)
        now = datetime.now()
        self._insert_generation(
            generation_id=generation_id,
            asset_type=asset_type,
            state="publishing",
            changed_from=current.get("changed_from"),
            heartbeat_at=now,
            committed_at=None,
            owner_id=owner_id,
            fencing_token=fencing_token,
            lease_expires_at=now + timedelta(seconds=_LEASE_SECONDS),
            created_by=current.get("created_by", owner_id),
            created_at=current.get("created_at", now),
            updated_by=owner_id,
            updated_at=now,
        )

    def _finish_generation_leased(
        self,
        asset_type: str,
        generation_id: str,
        owner_id: str,
        fencing_token: int,
        state: str,
    ) -> None:
        """终结租约持有者的 generation，并释放 lease。"""
        current = self._generation_row(asset_type, generation_id)
        self._assert_lease(current, owner_id, fencing_token)
        now = datetime.now()
        self._insert_generation(
            generation_id=generation_id,
            asset_type=asset_type,
            state=state,
            changed_from=current.get("changed_from"),
            heartbeat_at=now,
            committed_at=now if state == "ready" else None,
            owner_id=owner_id,
            fencing_token=fencing_token,
            lease_expires_at=None,
            created_by=current.get("created_by", owner_id),
            created_at=current.get("created_at", now),
            updated_by=owner_id,
            updated_at=now,
        )

    def commit_generation_leased(
        self,
        asset_type: str,
        generation_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> None:
        """仅租约持有者可以提交 generation。"""
        self._finish_generation_leased(asset_type, generation_id, owner_id, fencing_token, "ready")

    def fail_generation_leased(
        self,
        asset_type: str,
        generation_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> None:
        """仅租约持有者可以失败 generation。"""
        self._finish_generation_leased(asset_type, generation_id, owner_id, fencing_token, "failed")

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
            "SELECT generation_id, state, heartbeat_at, lease_expires_at, owner_id, "
            "fencing_token, updated_at "
            f"FROM {self._table('ck_enriched_generation')} FINAL "
            "WHERE asset_type = {asset_type:String} AND state = 'publishing' "
            "ORDER BY updated_at DESC",
            {"asset_type": asset_type},
        )
        return df.to_dicts()
