"""DIST-03: mining 取消信号的跨实例传播与 flock 门控出局。"""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import app.main as main_module
import app.services.mining_manager as mining_manager_module
from app.services.heavy_job_limiter import HeavyJobLimiter
from app.services.mining_manager import MiningJobManager, _CoordinatedCancelEvent


class SharedCancelFlags:
    """进程内共享的取消标志，模拟多个 manager 实例共用同一协调层。"""

    def __init__(self) -> None:
        self._flags: set[str] = set()

    def register(self, job_id: str) -> None:
        return None

    def set(self, job_id: str) -> bool:
        self._flags.add(job_id)
        return True

    def is_set(self, job_id: str) -> bool:
        return job_id in self._flags

    def clear(self, job_id: str) -> None:
        self._flags.discard(job_id)


@pytest.fixture()
def shared_flags(monkeypatch: pytest.MonkeyPatch) -> SharedCancelFlags:
    flags = SharedCancelFlags()
    monkeypatch.setattr(mining_manager_module.coordination, "cancel_flags", flags)
    return flags


def _task_factory(kind: str, data_dir: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {"kind": kind, "payload": payload}


def _isolated_limiter(monkeypatch: pytest.MonkeyPatch) -> HeavyJobLimiter:
    limiter = HeavyJobLimiter(capacity=2, cancel_poll_interval=0.005)
    monkeypatch.setattr(mining_manager_module, "shared_heavy_job_limiter", limiter)
    return limiter


def _wait_for_status(manager: MiningJobManager, run_id: str, status: str, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        manifest = manager.store.get(run_id)
        if manifest is not None and manifest["status"] == status:
            return manifest
        time.sleep(0.005)
    pytest.fail(f"run {run_id} did not reach {status}")


def test_coordinated_event_observes_flag_from_other_instance(
    shared_flags: SharedCancelFlags,
) -> None:
    """一个实例的复合 Event 能观察到另一实例设置的取消标志。"""
    local = _CoordinatedCancelEvent("mining:mining:run-1")

    assert not local.is_set()
    shared_flags.set("mining:mining:run-1")
    assert local.is_set()
    shared_flags.clear("mining:mining:run-1")
    local.clear()
    assert not local.is_set()


def test_cancel_writes_coordination_flag_even_without_local_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shared_flags: SharedCancelFlags
) -> None:
    """本 worker 没有线程时，cancel 仍写跨实例标志，供持有者感知。"""
    _isolated_limiter(monkeypatch)
    manager = MiningJobManager(
        tmp_path, worker_runner=lambda *a: {"status": "succeeded"}, task_factory=_task_factory
    )
    created = manager.start({"factor_names": ["quality"]}, "v1")
    run_id = created["run_id"]
    key = manager._cancel_key(run_id)

    shared_flags.clear(key)
    assert not shared_flags.is_set(key)

    # 模拟请求落到另一个 worker：本实例没有该 run 的本地线程。
    manager._cancel_events.pop(run_id, None)
    cancelled = manager.cancel(run_id)
    assert cancelled["status"] == "cancelled"
    assert shared_flags.is_set(key)


def test_remote_cancel_flag_stops_running_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shared_flags: SharedCancelFlags
) -> None:
    """另一 worker 置标志并推进 store 后，运行中的 worker 感知并收尾为取消。"""
    _isolated_limiter(monkeypatch)
    runner_started = threading.Event()

    def runner(task, progress_cb, cancel_event):
        runner_started.set()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not cancel_event.is_set():
            time.sleep(0.01)
        return {"status": "succeeded"}

    manager = MiningJobManager(tmp_path, worker_runner=runner, task_factory=_task_factory)
    created = manager.start({"factor_names": ["growth"]}, "v1")
    run_id = created["run_id"]
    assert runner_started.wait(1)

    # 外部 worker：写协调标志 + 把共享 store 状态推进到 cancelling。
    shared_flags.set(manager._cancel_key(run_id))
    manifest = manager.store.get(run_id)
    assert manifest is not None and manifest["status"] == "running"
    manager.store.transition_status(run_id, "cancelling")

    _wait_for_status(manager, run_id, "cancelled")


def test_redis_mode_ensure_mining_lock_bypasses_flock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """方案 C 下 ensure_mining_lock 直接放行，不触碰 flock。"""
    from app.config import settings
    from app.services import mining_lock_admin

    monkeypatch.setattr(settings, "mining_store_backend", "redis")

    class BoomLock:
        acquired = False

        def acquire(self):
            raise AssertionError("redis 模式不应尝试获取 flock")

    state = SimpleNamespace(
        mining_process_lock=BoomLock(),
        mining_lock_acquired=False,
        repo=SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path)),
    )
    assert mining_lock_admin.ensure_mining_lock(state) is True
    assert state.mining_lock_acquired is False  # 交由 lifespan 的分布式语义决定


def test_redis_mode_lifespan_skips_flock_acquire(monkeypatch: pytest.MonkeyPatch) -> None:
    """方案 C 下 lifespan 不调用 flock.acquire。"""
    from app.config import settings

    calls: list[str] = []
    monkeypatch.setattr(settings, "mining_store_backend", "redis")

    class LockStub:
        def __init__(self, data_dir) -> None:
            del data_dir
            self.acquired = False

        def acquire(self) -> None:
            calls.append("acquire")
            raise AssertionError("redis 模式不应获取 flock")

        def release(self) -> None:
            calls.append("release")

    @asynccontextmanager
    async def application_lifespan(_app):
        yield

    monkeypatch.setattr(main_module, "MiningProcessLock", LockStub)
    monkeypatch.setattr(main_module, "_application_lifespan", application_lifespan)

    async def exercise() -> None:
        async with main_module.lifespan(SimpleNamespace(state=SimpleNamespace())):  # type: ignore[arg-type]
            pass

    asyncio.run(exercise())
    assert calls == []
