"""mining run CK 审计与重建（DES-004 方案 C）：状态转换落表 + Redis 重建。

连真实 CK（ck_runtime）与真实 Redis；验证 mining 状态机旁边的审计事实
可被记录，并能从审计快照把 run 重建回 Redis。
"""

from __future__ import annotations

import uuid

import pytest

from app.services import mining_audit
from app.services.mining_run_store_redis import RedisMiningRunStore


@pytest.fixture()
def redis_installed(monkeypatch):
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url("redis://:123456@localhost:6379/0")
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"真实 Redis 不可达: {exc}")
    monkeypatch.setattr("app.config.settings.mining_store_backend", "redis")
    yield client


def test_record_transition_writes_audit(ck_runtime, redis_installed, monkeypatch) -> None:
    namespace = "t" + uuid.uuid4().hex[:8]
    manifest = {"run_id": "r1", "status": "running", "schema_version": 1}

    mining_audit.record_transition(namespace, "r1", "queued", "running", manifest)

    from app.storage.runtime import get_storage

    client = get_storage().client  # type: ignore[union-attr]
    rows = client.query(
        f"SELECT run_id, from_status, to_status FROM {client.table('ck_mining_run_audit')} "
        f"WHERE namespace = '{namespace}'"
    )
    assert rows.height == 1
    assert rows.row(0, named=True)["to_status"] == "running"


def test_rebuild_mining_runs_from_audit(ck_runtime, redis_installed) -> None:
    namespace = "t" + uuid.uuid4().hex[:8]
    rid = "r" + uuid.uuid4().hex[:8]
    manifest = {
        "run_id": rid,
        "status": "succeeded",
        "schema_version": 1,
        "run_signature": "sig",
        "artifacts": {},
    }
    mining_audit.record_transition(namespace, rid, "running", "succeeded", manifest)

    counts = mining_audit.rebuild_mining_runs()

    assert counts.get(namespace) == 1
    store = RedisMiningRunStore(namespace=namespace)
    rebuilt = store.get(rid)
    assert rebuilt is not None and rebuilt["status"] == "succeeded"

    client = redis_installed
    for kind in ("runs", "summary", "runindex", "signature"):
        client.delete(f"tickflow:mining:{kind}:{namespace}")
    client.delete(mining_audit.outbox_key(namespace))
    client.srem(mining_audit._OUTBOX_INDEX, namespace)


def test_transition_outbox_survives_ck_failure_and_barrier_flushes(
    ck_runtime, redis_installed, monkeypatch
) -> None:
    namespace = "t" + uuid.uuid4().hex[:8]
    store = RedisMiningRunStore(namespace=namespace)
    original_flush = mining_audit.flush
    monkeypatch.setattr(mining_audit, "flush", lambda namespace=None: 0)
    manifest = store.create({"factor": "f1"}, "fp")
    store.transition_status(manifest["run_id"], "running")

    assert mining_audit.pending(namespace) == 1
    monkeypatch.setattr(mining_audit, "flush", original_flush)
    mining_audit.barrier(namespace)

    assert mining_audit.pending(namespace) == 0
    from app.storage.runtime import get_storage

    client = get_storage().client  # type: ignore[union-attr]
    rows = client.query(
        f"SELECT run_id, to_status FROM {client.table('ck_mining_run_audit')} "
        f"WHERE namespace = '{namespace}'"
    )
    assert rows.height == 1
    assert rows.row(0, named=True)["to_status"] == "running"


def test_enabled_guard(ck_runtime, monkeypatch) -> None:
    monkeypatch.setattr("app.config.settings.mining_store_backend", "memory")
    assert mining_audit.enabled() is False
    monkeypatch.setattr("app.config.settings.mining_store_backend", "redis")
    assert mining_audit.enabled() is True
