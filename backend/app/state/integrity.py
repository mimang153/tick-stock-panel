"""完整性状态门面（DES-006）。当前态在 PG，历史在 CK，版本经 Redis 失效。"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any

from app.config import settings
from app.state import cache
from app.state.pg import execute, fetch_all, fetch_one, transaction

_SCHEMA = settings.user_state_pg_schema


def get_daily_integrity_snapshot() -> dict[str, Any]:
    """读取当前完整性状态，返回 daily_health 兼容 snapshot。"""
    rows = fetch_all(
        f"SELECT asset_type, day, symbol, kind, reason, needs_raw, needs_enriched, revision "
        f"FROM {_SCHEMA}.daily_integrity ORDER BY asset_type, day, symbol, kind"
    )
    revisions = fetch_all(f"SELECT asset_type, revision FROM {_SCHEMA}.integrity_revision")
    return {
        "version": 1,
        "revisions": {str(row["asset_type"]): str(row["revision"]) for row in revisions},
        "entries": [
            {
                "asset_type": str(row["asset_type"]),
                "day": str(row["day"]),
                "symbol": str(row["symbol"] or ""),
                "kind": str(row["kind"]),
                "reason": str(row["reason"] or ""),
                "needs_raw": bool(row["needs_raw"]),
                "needs_enriched": bool(row["needs_enriched"]),
            }
            for row in rows
        ],
    }


def put_daily_integrity_snapshot(snapshot: dict[str, Any], *, actor: str = "system") -> None:
    """按资产族整表替换完整性 entries 并更新 revisions。"""
    rows = list(snapshot.get("entries") or [])
    revisions = dict(snapshot.get("revisions") or {})
    with transaction() as conn:
        conn.execute(f"TRUNCATE TABLE {_SCHEMA}.daily_integrity")
        for row in rows:
            conn.execute(
                f"INSERT INTO {_SCHEMA}.daily_integrity "
                "(id, asset_type, day, symbol, kind, reason, needs_raw, needs_enriched, "
                "revision, created_by, updated_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    uuid.uuid4().hex,
                    row["asset_type"],
                    row["day"],
                    row.get("symbol") or "",
                    row["kind"],
                    row.get("reason", ""),
                    bool(row.get("needs_raw", False)),
                    bool(row.get("needs_enriched", False)),
                    str(revisions.get(row["asset_type"], "0")),
                    actor,
                    actor,
                ),
            )
        for asset_type, revision in revisions.items():
            conn.execute(
                f"INSERT INTO {_SCHEMA}.integrity_revision "
                "(id, asset_type, revision, created_by, updated_by) VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (asset_type) DO UPDATE SET revision=EXCLUDED.revision, "
                "updated_by=EXCLUDED.updated_by, updated_at=now()",
                (uuid.uuid4().hex, asset_type, str(revision), actor, actor),
            )
    for asset_type in revisions:
        cache.bump_generation(f"integrity:{asset_type}")
    cache.publish("integrity-change", json.dumps({"revisions": revisions}, ensure_ascii=False))


def get_integrity_revision(asset_type: str) -> str:
    """读取资产族完整性版本号。"""
    row = fetch_one(
        f"SELECT revision FROM {_SCHEMA}.integrity_revision WHERE asset_type=%s",
        (asset_type,),
    )
    return str(row["revision"]) if row else "0"


def get_latest_integrity_report() -> dict | None:
    """读取最新完整性报告。"""
    row = fetch_one(
        f"SELECT report FROM {_SCHEMA}.integrity_report ORDER BY checked_at DESC LIMIT 1"
    )
    return dict(row["report"]) if row and isinstance(row["report"], dict) else None


def save_integrity_report(report: dict[str, Any], *, actor: str = "system") -> None:
    """UPSERT 当前报告并写 CK 历史。"""
    report_id = str(report.get("id") or uuid.uuid4().hex)
    checked_at = report.get("checked_at") or datetime.now().isoformat(timespec="seconds")
    execute(
        f"INSERT INTO {_SCHEMA}.integrity_report "
        "(id, report_id, checked_at, status, report, created_by, updated_by) "
        "VALUES (%s, %s, %s::timestamptz, %s, %s::jsonb, %s, %s) "
        "ON CONFLICT (report_id) DO UPDATE SET checked_at=EXCLUDED.checked_at, "
        "status=EXCLUDED.status, report=EXCLUDED.report, updated_by=EXCLUDED.updated_by, updated_at=now()",
        (
            uuid.uuid4().hex,
            report_id,
            checked_at,
            str(report.get("status", "issues")),
            json.dumps(report, ensure_ascii=False),
            actor,
            actor,
        ),
    )
    from app.state.audit import write_config_audit, write_integrity_report_history

    write_integrity_report_history(report, actor=actor)
    write_config_audit("integrity", "report", report_id, actor)


def repair_backoff_remaining_s(start_date: date) -> float:
    """读取修补冷却剩余秒数。"""
    key = f"repair:backoff:{start_date.isoformat()}"
    cached = cache.get_json(key)
    if isinstance(cached, dict):
        return max(0.0, float(cached.get("until", 0)) - _now())
    row = fetch_one(
        f"SELECT last_attempt_at FROM {_SCHEMA}.repair_backoff WHERE start_date=%s",
        (start_date,),
    )
    if row is None:
        return 0.0
    last = row["last_attempt_at"]
    if isinstance(last, datetime):
        return max(0.0, 6 * 3600 - (datetime.now() - last).total_seconds())
    return 0.0


def record_repair_attempt(start_date: date) -> None:
    """记录修补尝试并写 6h Redis 冷却。"""
    now = datetime.now()
    execute(
        f"INSERT INTO {_SCHEMA}.repair_backoff "
        "(id, start_date, last_attempt_at, created_by, updated_by) VALUES (%s, %s, %s, %s, %s) "
        "ON CONFLICT (start_date) DO UPDATE SET last_attempt_at=EXCLUDED.last_attempt_at, "
        "updated_by=EXCLUDED.updated_by, updated_at=now()",
        (uuid.uuid4().hex, start_date, now, "system", "system"),
    )
    cache.set_json(
        f"repair:backoff:{start_date.isoformat()}",
        {"until": now.timestamp() + 6 * 3600},
        6 * 3600,
    )


def _now() -> float:
    """返回当前 Unix 时间，便于测试注入。"""
    import time

    return time.time()
