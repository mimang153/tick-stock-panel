"""挖掘产物共享存储适配器（共享卷 / S3 兼容对象存储）。"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from app.config import settings

_SHARED_ROOT = "shared-root"
_S3 = "s3"


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
    raise ValueError(f"unsupported mining artifact store: {settings.mining_artifact_store!r}")


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
