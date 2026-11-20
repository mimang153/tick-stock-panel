"""mining 产物元数据 CK 落表（产物字节不入 CK）。"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.services.mining_audit import enabled

logger = logging.getLogger(__name__)

_TABLE = "ck_mining_artifact"
_ACTOR = "mining_artifact"


def record_artifact(
    namespace: str,
    run_id: str,
    name: str,
    content: dict[str, Any],
) -> None:
    """幂等登记一份产物；CK 写入失败只告警，不阻断 Redis/文件权威态。"""
    if not enabled():
        return
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return
    now = datetime.now()
    client = runtime.client
    try:
        client.insert_rows(
            client.table(_TABLE),
            [
                [
                    namespace,
                    run_id,
                    name,
                    content["content_addr"],
                    content["uri"],
                    int(content.get("row_count", 0)),
                    int(content["size_bytes"]),
                    int(content.get("schema_version", 1)),
                    now,
                    _ACTOR,
                    now,
                    _ACTOR,
                ]
            ],
            column_names=[
                "namespace",
                "run_id",
                "artifact_name",
                "content_addr",
                "uri",
                "row_count",
                "size_bytes",
                "schema_version",
                "created_at",
                "created_by",
                "updated_at",
                "updated_by",
            ],
        )
    except Exception as exc:  # noqa: BLE001 - CK 元数据延迟落表不阻断产物注册
        logger.warning("mining 产物元数据写 CK 失败: %s", exc)
