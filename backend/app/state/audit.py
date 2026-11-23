"""用户配置变更审计门面（DES-006）。"""

from __future__ import annotations

import logging
import uuid

from app.config import settings
from app.state.pg import execute

logger = logging.getLogger(__name__)


def write_config_audit(domain: str, action: str, detail: str, actor: str = "system") -> None:
    """写 CK 配置变更审计；失败只记录日志，不阻断主流程。"""
    event_id = uuid.uuid4().hex
    try:
        from app.storage import require_storage

        runtime = require_storage()
        from datetime import datetime

        now = datetime.now()
        runtime.client.insert_rows(
            runtime.client.table("ck_config_change_audit"),
            [
                [
                    event_id,
                    domain,
                    action,
                    actor,
                    detail,
                    actor,
                    now,
                    actor,
                    now,
                ]
            ],
            column_names=[
                "event_id",
                "domain",
                "action",
                "actor",
                "detail",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )
    except Exception as exc:  # noqa: BLE001 - 审计失败不能阻断配置写入
        logger.warning("config audit write failed: %s", exc)


def write_integrity_report_history(report: dict, *, actor: str = "system") -> None:
    """把自检报告写入 CK 历史；失败只记录日志，不阻断主流程。"""
    report_id = str(report.get("id") or "")
    if not report_id:
        return
    try:
        import json
        from datetime import datetime

        from app.storage import require_storage

        runtime = require_storage()
        checked_at = report.get("checked_at") or datetime.now().isoformat(timespec="seconds")
        now = datetime.now()
        runtime.client.insert_rows(
            runtime.client.table("ck_integrity_report_history"),
            [
                [
                    report_id,
                    checked_at,
                    str(report.get("status", "issues")),
                    json.dumps(report, ensure_ascii=False, default=str),
                    actor,
                    now,
                    actor,
                    now,
                ]
            ],
            column_names=[
                "report_id",
                "checked_at",
                "status",
                "report_json",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )
    except Exception as exc:  # noqa: BLE001 - 审计失败不能阻断报告写入
        logger.warning("integrity report history write failed: %s", exc)


def record_migration_audit(
    migration_id: str,
    source_path: str,
    target_ref: str,
    status: str,
    checksum: str = "",
    detail: str = "",
) -> None:
    """记录迁移审计到 PG；重复执行幂等。"""
    execute(
        f"INSERT INTO {settings.user_state_pg_schema}.state_migration_audit "
        "(id, migration_id, source_path, target_ref, status, checksum, detail, created_by, updated_by) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (migration_id) DO UPDATE SET status=EXCLUDED.status, "
        "checksum=EXCLUDED.checksum, detail=EXCLUDED.detail, updated_at=now()",
        (
            uuid.uuid4().hex,
            migration_id,
            source_path,
            target_ref,
            status,
            checksum,
            detail,
            "migration",
            "migration",
        ),
    )
