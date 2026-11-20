"""DES-002 修补契约：审计/快照回滚/generation 的存储层回归。"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest


def _daily_frame(day: date) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "date": [day],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.0],
            "volume": [100.0],
            "amount": [1000.0],
            "updated_at": [datetime(2026, 1, 2, 16)],
        }
    )


def test_repair_snapshot_can_rollback_deleted_rows(ck_runtime):
    day = date(2026, 1, 2)
    original = _daily_frame(day)
    ck_runtime.write_daily("kline_daily", original)

    repair_id = ck_runtime.begin_repair("kline_daily", day, "integrity", "tester")
    ck_runtime.append_repair_snapshot(repair_id, "kline_daily", day, original, "tester")
    ck_runtime.append_repair_audit(
        repair_id,
        "kline_daily",
        day,
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "change_type": ["delete"],
                "before_json": ['{"close": 10.0}'],
                "after_json": [None],
            }
        ),
        "tester",
    )

    ck_runtime.delete_daily("kline_daily", day=day, symbols=["600000.SH"])
    assert ck_runtime.read_daily("kline_daily", symbols=["600000.SH"]).is_empty()

    ck_runtime.rollback_repair(repair_id, "tester")

    restored = ck_runtime.read_daily("kline_daily", symbols=["600000.SH"])
    assert restored["close"].to_list() == [10.0]


def test_repair_audit_has_standard_actor_fields(ck_runtime):
    day = date(2026, 1, 2)
    repair_id = ck_runtime.begin_repair("kline_daily", day, "manual", "alice")

    audit = ck_runtime.client.query(
        f"SELECT repair_id, status, created_by, updated_by "
        f"FROM {ck_runtime.client.table('ck_repair_audit')} "
        "WHERE repair_id = {repair_id:String} ORDER BY updated_at",
        {"repair_id": repair_id},
    )

    assert audit.height == 1
    assert audit["status"].to_list() == ["started"]
    assert audit["created_by"].to_list() == ["alice"]
    assert audit["updated_by"].to_list() == ["alice"]


def test_repair_status_cas_rejects_illegal_transitions(ck_runtime):
    """DES-002 §9：按 repair_id 的 CAS 拒绝非法/迟到状态流转。"""
    day = date(2026, 1, 2)
    ck_runtime.write_daily("kline_daily", _daily_frame(day))
    repair_id = ck_runtime.begin_repair("kline_daily", day, "integrity", "tester")

    assert ck_runtime.read_repair_status(repair_id) == "started"
    ck_runtime.set_repair_status(repair_id, "committed", "tester")
    assert ck_runtime.read_repair_status(repair_id) == "committed"

    # committed 只允许 rollback；迟到的 failed 覆盖必须被 CAS 拒绝。
    with pytest.raises(ValueError, match="非法修补状态流转"):
        ck_runtime.set_repair_status(repair_id, "failed", "tester")
    ck_runtime.set_repair_status(repair_id, "rollback", "tester")
    assert ck_runtime.read_repair_status(repair_id) == "rollback"

    # 终态不可再写。
    with pytest.raises(ValueError, match="非法修补状态流转"):
        ck_runtime.set_repair_status(repair_id, "committed", "tester")


def test_generation_lifecycle_uses_ck_metadata(ck_runtime):
    day = date(2026, 1, 2)
    assert ck_runtime.current_generation("stock") == {}

    generation_id = ck_runtime.begin_generation("stock", "alice", day)
    assert ck_runtime.current_generation("stock") == {}

    ck_runtime.commit_generation("stock", generation_id, "bob")
    current = ck_runtime.current_generation("stock")

    assert current["generation_id"] == generation_id
    assert current["state"] == "ready"
    assert current["created_by"] == "alice"
    assert current["updated_by"] == "bob"


def test_generation_lease_fencing_rejects_stale_owner(ck_runtime) -> None:
    generation_id, token = ck_runtime.begin_generation_leased(
        "stock", "alice", None, "owner-a", lease_seconds=600
    )

    with pytest.raises(PermissionError):
        ck_runtime.heartbeat_generation_leased("stock", generation_id, "owner-b", token)
    with pytest.raises(PermissionError):
        ck_runtime.commit_generation_leased("stock", generation_id, "owner-a", token - 1)

    ck_runtime.commit_generation_leased("stock", generation_id, "owner-a", token)
    assert ck_runtime.current_generation("stock")["generation_id"] == generation_id
