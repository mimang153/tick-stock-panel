"""mining Redis 状态机 store（DES-004 方案 C）：状态机 / 事件 / 并发原子性。

连真实 Redis（fakeredis 无 EVAL），不可达时跳过；每用例用独立 namespace 前缀隔离，
结束时清理自己的键，不污染其它用例。
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.services.mining_jobs import InvalidMiningStatusTransitionError
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
    for kind in ("runs", "summary", "runindex"):
        client.delete(f"tickflow:mining:{kind}:{namespace}")


def _rid() -> str:
    return "run" + uuid.uuid4().hex[:8]


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
    store.register_artifact(rid, "factors", "factors.parquet")
    after = store.get(rid)
    assert after is not None and after["artifacts"]["factors"] == "factors.parquet"


def test_list_and_find_by_signature(store: RedisMiningRunStore) -> None:
    rid = _rid()
    m = store.create({"factor": "f1"}, "fp", run_id=rid)
    assert any(r["run_id"] == rid for r in store.list_runs())
    found = store.find_by_signature(m["run_signature"])
    assert found is not None and found["run_id"] == rid


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
