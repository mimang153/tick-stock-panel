"""mining Redis 状态机 store（DES-004 方案 C）：状态机 / 事件 / 并发原子性。

连真实 Redis（fakeredis 无 EVAL），不可达时跳过；每用例用独立 namespace 前缀隔离，
结束时清理自己的键，不污染其它用例。
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.services.mining_jobs import InvalidMiningStatusTransitionError, MiningRunStore
from app.services.mining_run_store_redis import RedisMiningRunStore


@pytest.fixture()
def store():
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url("redis://:123456@localhost:6379/0")
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"真实 Redis 不可达: {exc}")
    namespace = "t" + uuid.uuid4().hex[:8]
    s = RedisMiningRunStore(Path("/tmp"), namespace=namespace)
    yield s
    for kind in ("runs", "summary", "runindex", "signature"):
        client.delete(f"tickflow:mining:{kind}:{namespace}")
    client.delete(f"tickflow:mining:audit:outbox:{namespace}")
    client.srem("tickflow:mining:audit:outbox:index", namespace)


def _rid() -> str:
    return "run" + uuid.uuid4().hex[:8]


def test_read_falls_back_to_legacy_file_runs(tmp_path: Path) -> None:
    """Redis 后端只读回退到迁移前写入本地文件的历史 run。"""
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url("redis://:123456@localhost:6379/0")
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"真实 Redis 不可达: {exc}")
    namespace = "t" + uuid.uuid4().hex[:8]
    store = RedisMiningRunStore(tmp_path, namespace=namespace)
    file_store = MiningRunStore(tmp_path, namespace=namespace)
    run_id = "legacy-file-run"

    manifest = file_store.create({"factor": "f1"}, "fp", run_id=run_id)
    file_store.write_summary(run_id, {"candidate_count": 7})
    file_store.append_event(run_id, "queued", {"source": "legacy"})

    fetched = store.get(run_id)
    assert fetched is not None and fetched["status"] == "queued"
    assert store.read_summary(run_id) == {"candidate_count": 7}
    assert [event["type"] for event in store.read_events(run_id)] == ["queued"]
    assert any(run["run_id"] == run_id for run in store.list_runs())
    found = store.find_by_signature(manifest["run_signature"])
    assert found is not None and found["run_id"] == run_id

    # 写路径不回退：历史文件 run 在 Redis 后端保持只读。
    with pytest.raises(KeyError):
        store.transition_status(run_id, "running")

    for kind in ("runs", "summary", "runindex", "signature"):
        client.delete(f"tickflow:mining:{kind}:{namespace}")


def test_create_get_status_machine(store: RedisMiningRunStore) -> None:
    rid = _rid()
    m = store.create({"factor": "f1"}, "fp", run_id=rid)
    assert m["status"] == "queued"
    fetched = store.get(rid)
    assert fetched is not None and fetched["run_id"] == rid

    running = store.transition_status(rid, "running")
    assert running["status"] == "running" and running["started_at"]

    done = store.transition_status(rid, "succeeded")
    assert done["status"] == "succeeded" and done["finished_at"]


def test_same_status_is_noop(store: RedisMiningRunStore) -> None:
    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    store.transition_status(rid, "running")
    before = store.get(rid)
    after = store.transition_status(rid, "running")
    assert before == after


def test_illegal_transition_rejected(store: RedisMiningRunStore) -> None:
    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    with pytest.raises(InvalidMiningStatusTransitionError):
        store.transition_status(rid, "succeeded")  # queued 不能直接 succeeded
    store.transition_status(rid, "running")
    store.transition_status(rid, "succeeded")
    with pytest.raises(InvalidMiningStatusTransitionError):
        store.transition_status(rid, "failed")  # 终态不可转出


def test_event_sequence_and_trim(store: RedisMiningRunStore) -> None:
    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    ids = [store.append_event(rid, "progress", {"i": i})["id"] for i in range(5)]
    assert ids == [1, 2, 3, 4, 5]
    assert [e["id"] for e in store.read_events(rid)] == ids
    assert [e["id"] for e in store.read_events(rid, after_id=2)] == [3, 4, 5]


def test_summary_and_artifact(store: RedisMiningRunStore) -> None:
    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    store.write_summary(rid, {"selected": 5})
    assert store.read_summary(rid) == {"selected": 5}
    store.artifact_path(rid, "factors").write_bytes(b"artifact")
    store.register_artifact(rid, "factors", "factors.parquet")
    after = store.get(rid)
    assert after is not None and after["artifacts"]["factors"] == "factors.parquet"
    content = after["artifact_content"]["factors"]
    # DES-006: artifact 二进制入 PG blob（MINING_ARTIFACT_STORE=pgblob）。
    assert content["uri"].startswith("pgblob://")
    assert content["content_addr"].startswith("sha256:")
    assert content["size_bytes"] == len(b"artifact")


def test_artifact_path_uses_shared_root_when_configured(
    store: RedisMiningRunStore,
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.config import settings

    monkeypatch.setattr(settings, "mining_artifact_root", tmp_path)
    rid = _rid()
    path = store.artifact_path(rid, "candidates")

    assert path.parent == (tmp_path / store.namespace / rid).resolve()


def test_concurrent_artifact_registration_keeps_all_names(store: RedisMiningRunStore) -> None:
    import threading

    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    names = ("factors", "candidates")
    for name in names:
        store.artifact_path(rid, name).write_bytes(name.encode())

    errors: list[BaseException] = []

    def register(name: str) -> None:
        try:
            store.register_artifact(rid, name)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=register, args=(name,)) for name in names]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    manifest = store.get(rid)
    assert manifest is not None
    assert set(manifest["artifacts"]) == set(names)
    assert set(manifest["artifact_content"]) == set(names)


def test_list_and_find_by_signature(store: RedisMiningRunStore) -> None:
    rid = _rid()
    m = store.create({"factor": "f1"}, "fp", run_id=rid)
    assert any(r["run_id"] == rid for r in store.list_runs())
    found = store.find_by_signature(m["run_signature"])
    assert found is not None and found["run_id"] == rid


def test_signature_claim_is_atomic_and_replaces_failed_run(store: RedisMiningRunStore) -> None:
    signature = "dedup-signature"
    first = _rid()
    second = _rid()

    assert store.claim_signature(signature, first) == first
    assert store.claim_signature(signature, second) == first

    store.create({"factor": "f1"}, "fp", run_id=first)
    store.transition_status(first, "running")
    store.transition_status(first, "failed")
    assert store.claim_signature(signature, second) == second


def test_dangling_signature_is_cleaned_on_recovery(store: RedisMiningRunStore) -> None:
    """DIST-07：claim 后崩溃（run 从未创建）的悬挂签名在启动恢复时清理。"""
    signature = "dangling-signature"
    crashed = _rid()
    recovering = _rid()

    assert store.claim_signature(signature, crashed) == crashed
    assert store.get(crashed) is None  # 悬挂：signature 指向不存在的 run
    assert store.clear_dangling_signatures() >= 1
    assert store.claim_signature(signature, recovering) == recovering


def test_concurrent_transitions_only_one_wins(store: RedisMiningRunStore) -> None:
    import threading

    rid = _rid()
    store.create({"factor": "f1"}, "fp", run_id=rid)
    store.transition_status(rid, "running")

    errors: list[BaseException] = []

    def transition(status: str) -> None:
        try:
            store.transition_status(rid, status)
        except InvalidMiningStatusTransitionError:
            pass  # 竞争失败方非法转换被拒，符合预期
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=transition, args=(status,))
        for status in ("succeeded", "failed", "cancelled")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    final = store.get(rid)
    assert final is not None
    assert final["status"] in {"succeeded", "failed", "cancelled"}
    assert final["finished_at"]
