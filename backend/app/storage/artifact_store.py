"""挖掘产物共享存储适配器（PG blob / 共享卷 / S3 兼容对象存储）。"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from urllib.parse import quote, unquote

from app.config import settings
from app.state import put_blob, resolve_blob_uri

logger = logging.getLogger(__name__)

_SHARED_ROOT = "shared-root"
_S3 = "s3"
_PGBLOB = "pgblob"


def store_artifact_uri(
    local_path: Path,
    namespace: str,
    run_id: str,
    name: str,
) -> str:
    """把已写出的 Parquet 登记为跨实例可解析的 URI。"""
    if settings.mining_artifact_store == _SHARED_ROOT:
        return local_path.resolve().as_uri()
    if settings.mining_artifact_store == _S3:
        return _store_s3(local_path, namespace, run_id, name)
    if settings.mining_artifact_store == _PGBLOB:
        # 内容寻址去重由 user_blob 门面完成；超限由门面显式报错，不静默截断。
        return f"pgblob://{put_blob(local_path.read_bytes())}"
    raise ValueError(f"unsupported mining artifact store: {settings.mining_artifact_store!r}")


def resolve_artifact_uri(uri: str) -> bytes | None:
    """解析产物 URI 为字节；未知协议返回 None 由调用方决定降级。"""
    if uri.startswith(("pgblob://", "file://")):
        return resolve_blob_uri(uri)
    if uri.startswith("s3://"):
        return _resolve_s3(uri)
    return None


def _store_s3(local_path: Path, namespace: str, run_id: str, name: str) -> str:
    """上传到 S3 兼容存储并返回 s3:// URI。"""
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("MINING_ARTIFACT_STORE=s3 需要安装 boto3") from exc
    bucket = settings.mining_artifact_s3_bucket
    prefix = settings.mining_artifact_s3_prefix.strip("/")
    key = (
        f"{prefix}/{namespace}/{run_id}/{name}.parquet"
        if prefix
        else f"{namespace}/{run_id}/{name}.parquet"
    )
    client = boto3.client(
        "s3",
        endpoint_url=settings.mining_artifact_s3_endpoint_url or None,
    )
    client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{quote(key, safe='/')}"


def _resolve_s3(uri: str) -> bytes | None:
    """从 s3:// URI 下载产物；不可达时记录告警并返回 None。"""
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("MINING_ARTIFACT_STORE=s3 需要安装 boto3") from exc
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        return None
    buffer = io.BytesIO()
    try:
        client = boto3.client(
            "s3",
            endpoint_url=settings.mining_artifact_s3_endpoint_url or None,
        )
        client.download_fileobj(bucket, unquote(key), buffer)
    except Exception as exc:  # noqa: BLE001 - S3 不可达按产物缺失处理，日志留痕
        logger.warning("resolve s3 artifact failed: %s", exc)
        return None
    return buffer.getvalue()
