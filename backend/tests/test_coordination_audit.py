"""协调态 CK 审计与重建（DES-004 §2.3 / 方案 B）。

连真实 CK（ck_runtime fixture 建全部表），coordination 用可记录的假后端，
不依赖真实 Redis 协调层；验证审计落表、按 FINAL 取最新、重建回填三套后端。
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services import coordination_audit
from app.services.coordination_audit import (
    rebuild_coordination,
    record_cancel,
    record_job_state,
    record_lease_acquire,
    record_lease_release,
)


class _FakeFlags:
    def __init__(self):
        self.set_ids: list[str] = []

    def set(self, job_id: str) -> bool:
        self.set_ids.append(job_id)
        return True

    def is_set(self, job_id: str) -> bool:
        return job_id in self.set_ids

    def register(self, job_id: str) -> None:
        return None

    def clear(self, job_id: str) -> None:
        return None


class _FakeCounter:
    def __init__(self):
        self.restored: int | None = None

    def restore(self, used: int) -> None:
        self.restored = used


class _FakeJobState:
    def __init__(self):
        self.items: dict[str, dict] = {}

    def put(self, job_id: str, job: dict) -> None:
        self.items[job_id] = job


@pytest.fixture()
def fake_coordination(monkeypatch):
    flags, counter, jobs = _FakeFlags(), _FakeCounter(), _FakeJobState()
    fake = SimpleNamespace(cancel_flags=flags, limiter_counter=counter, job_state=jobs)
    monkeypatch.setattr("app.services.coordination.coordination", fake, raising=False)
    monkeypatch.setattr("app.config.settings.coordination_backend", "redis")
    return fake


def test_job_state_audit_and_rebuild(ck_runtime, fake_coordination) -> None:
    job = {"id": "j1", "status": "running", "progress": 0.3, "result": {"n": 1}}
    record_job_state(job)

    counts = rebuild_coordination()

    assert counts["job_state"] == 1
    assert fake_coordination.job_state.items["j1"]["status"] == "running"
    assert fake_coordination.job_state.items["j1"]["progress"] == 0.3


def test_job_state_latest_wins(ck_runtime, fake_coordination) -> None:
    record_job_state({"id": "j2", "status": "running", "progress": 0.1})
    record_job_state({"id": "j2", "status": "succeeded", "progress": 1.0})

    rebuild_coordination()

    assert fake_coordination.job_state.items["j2"]["status"] == "succeeded"
    assert len(fake_coordination.job_state.items) == 1


def test_cancel_audit_rebuild(ck_runtime, fake_coordination) -> None:
    record_cancel("job-cancel-1")

    counts = rebuild_coordination()

    assert counts["cancel"] == 1
    assert "job-cancel-1" in fake_coordination.cancel_flags.set_ids


def test_lease_audit_rebuild_counts_unreleased(ck_runtime, fake_coordination) -> None:
    record_lease_acquire("lease-a", "host:1", "normal", 1)
    record_lease_acquire("lease-b", "host:2", "mining", 2)
    record_lease_release("lease-b")

    counts = rebuild_coordination()

    assert counts["lease"] == 1  # 仅 lease-a 未释放
    assert fake_coordination.limiter_counter.restored == 1


def test_enabled_guard(ck_runtime, monkeypatch) -> None:
    monkeypatch.setattr("app.config.settings.coordination_backend", "memory")
    assert coordination_audit.enabled() is False
    monkeypatch.setattr("app.config.settings.coordination_backend", "redis")
    assert coordination_audit.enabled() is True


def test_rebuild_noop_when_not_redis(ck_runtime, monkeypatch) -> None:
    monkeypatch.setattr("app.config.settings.coordination_backend", "memory")
    assert rebuild_coordination() == {"job_state": 0, "cancel": 0, "lease": 0}
