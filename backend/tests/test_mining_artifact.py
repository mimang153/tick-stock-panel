"""mining 产物共享 URI 与 CK 元数据登记契约。"""

from __future__ import annotations

import sys
import types
from pathlib import Path

from app.config import settings
from app.services.mining_artifact import record_artifact
from app.storage.artifact_store import store_artifact_uri


def test_shared_root_returns_file_uri(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "mining_artifact_store", "shared-root")
    path = tmp_path / "factors.parquet"
    path.write_bytes(b"artifact")

    uri = store_artifact_uri(path, "mining", "run-1", "factors")

    assert uri.startswith("file://")
    assert uri.endswith("/factors.parquet")


def test_s3_adapter_uploads_and_returns_s3_uri(tmp_path: Path, monkeypatch) -> None:
    calls: list[tuple[str, str, str]] = []

    class FakeS3Client:
        def upload_file(self, local_path: str, bucket: str, key: str) -> None:
            calls.append((local_path, bucket, key))

    fake_boto3 = types.SimpleNamespace(
        client=lambda service, **kwargs: FakeS3Client() if service == "s3" else None
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setattr(settings, "mining_artifact_store", "s3")
    monkeypatch.setattr(settings, "mining_artifact_s3_bucket", "tsp-artifacts")
    monkeypatch.setattr(settings, "mining_artifact_s3_prefix", "mining/artifacts")
    monkeypatch.setattr(settings, "mining_artifact_s3_endpoint_url", None)
    path = tmp_path / "factors.parquet"
    path.write_bytes(b"artifact")

    uri = store_artifact_uri(path, "mining", "run-1", "factors")

    assert uri == "s3://tsp-artifacts/mining/artifacts/mining/run-1/factors.parquet"
    assert calls == [(str(path), "tsp-artifacts", "mining/artifacts/mining/run-1/factors.parquet")]


def test_artifact_metadata_written_to_ck(ck_runtime, monkeypatch) -> None:
    monkeypatch.setattr(settings, "mining_store_backend", "redis")
    record_artifact(
        "mining",
        "run-1",
        "factors",
        {
            "content_addr": "sha256:abc",
            "uri": "file:///shared/run-1/factors.parquet",
            "size_bytes": 12,
            "schema_version": 1,
        },
    )

    from app.storage.runtime import get_storage

    client = get_storage().client  # type: ignore[union-attr]
    rows = client.query(
        f"SELECT content_addr, uri, size_bytes FROM {client.table('ck_mining_artifact')} "
        "WHERE namespace = 'mining' AND run_id = 'run-1'"
    )
    assert rows.height == 1
    row = rows.row(0, named=True)
    assert row["content_addr"] == "sha256:abc"
    assert row["uri"] == "file:///shared/run-1/factors.parquet"
    assert row["size_bytes"] == 12
