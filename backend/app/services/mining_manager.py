"""Threaded orchestration for persistent mining jobs."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from app.backtest.worker import make_worker_task, run_worker_task
from app.services.coordination import coordination
from app.services.heavy_job_limiter import (
    HeavyJobCancelledError,
    shared_heavy_job_limiter,
)
from app.services.mining_jobs import (
    TERMINAL_RUN_STATUSES,
    MiningRunStore,
    MiningRunValidationError,
    compute_run_signature,
    get_run_store,
)

WorkerRunner = Callable[
    [dict[str, Any], Callable[[dict[str, Any]], None], threading.Event],
    dict[str, Any],
]
TaskFactory = Callable[[str, Path, dict[str, Any]], dict[str, Any]]

_SUCCESS_STATUSES = {"succeeded", "succeeded_with_budget_exhausted"}
_SHUTDOWN_JOIN_SECONDS = 1.0


class _CoordinatedCancelEvent(threading.Event):
    """本地 Event 即时唤醒 + 协调层取消标志的跨实例可见性。

    worker 父进程每 100ms 轮询 ``is_set``，这里叠加协调层查询后即可把另一个
    worker 的取消请求传播到本进程的 worker 子进程；memory 模式下协调层查询是
    noop，语义与单进程 ``threading.Event`` 完全一致。
    """

    def __init__(self, job_key: str) -> None:
        super().__init__()
        self._job_key = job_key

    def is_set(self) -> bool:
        return super().is_set() or coordination.cancel_flags.is_set(self._job_key)


class MiningJobManager:
    """Coordinate mining persistence, capacity, cancellation, and worker threads."""

    def __init__(
        self,
        data_dir: Path | str,
        worker_runner: WorkerRunner = run_worker_task,
        task_factory: TaskFactory = make_worker_task,
        *,
        kind: str = "mining",
        store: MiningRunStore | None = None,
    ) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._kind = kind
        self._store = store if store is not None else get_run_store(self._data_dir)
        self._worker_runner = worker_runner
        self._task_factory = task_factory
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._shutdown = False

    @property
    def store(self) -> MiningRunStore:
        return self._store

    def start(
        self,
        request: dict[str, Any],
        data_fingerprint: Any,
        force: bool = False,
        source: str = "manual",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        signature = compute_run_signature(request, data_fingerprint)
        proposed_run_id = run_id or uuid.uuid4().hex
        with self._lock:
            if self._shutdown:
                raise RuntimeError("mining job manager is shut down")

        # claim → create 必须跨实例串行化：否则 A claim 后未 create 时，B 观察到
        # 悬挂 signature 会另建 run，产生同签名双 run（DIST-07）。拿不到锁说明
        # 另一 worker 正在创建同签名 run，读取它的结果即可。
        with coordination.lock(f"mining_claim:{signature}") as acquired:
            if not acquired:
                existing = self._store.find_by_signature(signature)
                if existing is not None:
                    return existing
                raise RuntimeError("同一签名的 mining run 正在被其他实例创建, 请稍后重试")
            return self._start_locked(
                request, data_fingerprint, signature, proposed_run_id, force, source
            )

    def _start_locked(
        self,
        request: dict[str, Any],
        data_fingerprint: Any,
        signature: str,
        proposed_run_id: str,
        force: bool,
        source: str,
    ) -> dict[str, Any]:
        """在持有 signature 跨实例租约的前提下 claim 并创建 run。"""
        with self._lock:
            if not force:
                claimed = self._store.claim_signature(signature, proposed_run_id)
                if claimed != proposed_run_id:
                    existing = self._store.get(claimed)
                    if existing is not None:
                        return existing
                    # 悬挂签名由 claim 的 Lua 自愈（重指到 proposed_run_id），
                    # 正常情况下不会走到这里；保守再取一次，仍无则继续创建（DIST-07）。
                    existing = self._store.get(proposed_run_id)
                    if existing is not None:
                        return existing

            try:
                manifest = self._store.create(
                    request,
                    data_fingerprint,
                    run_id=proposed_run_id,
                )
            except MiningRunValidationError:
                existing = self._store.get(proposed_run_id)
                if existing is None:
                    raise
                return existing
            run_id = manifest["run_id"]
            self._store.append_event(
                run_id,
                "queued",
                {"status": "queued", "source": source},
            )
            self._start_thread_locked(run_id, source)
            return manifest

    def _cancel_key(self, run_id: str) -> str:
        """跨实例取消标志键: 带 kind 前缀, 避免 mining/guardian 同名 run 串号。"""
        return f"mining:{self._kind}:{run_id}"

    def cancel(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            manifest = self._store.get(run_id)
            if manifest is None:
                raise KeyError(run_id)
            if manifest["status"] in TERMINAL_RUN_STATUSES:
                return manifest

            # 先落跨实例取消标志: 即便本 worker 没有该 run 的线程, 持有它的
            # worker 也能在 100ms 轮询内感知。memory 模式下这是 noop。
            coordination.cancel_flags.set(self._cancel_key(run_id))

            cancel_event = self._cancel_events.get(run_id)
            if cancel_event is None:
                cancelled = self._store.transition_status(run_id, "cancelled")
                self._store.append_event(run_id, "cancelled", {"status": "cancelled"})
                return cancelled

            cancel_event.set()
            if manifest["status"] != "cancelling":
                manifest = self._store.transition_status(run_id, "cancelling")
                self._store.append_event(run_id, "cancelling", {"status": "cancelling"})
            return manifest

    def shutdown(self) -> None:
        with self._lock:
            self._shutdown = True
            run_ids = list(self._threads)
        for run_id in run_ids:
            self.cancel(run_id)

        deadline = time.monotonic() + _SHUTDOWN_JOIN_SECONDS
        current = threading.current_thread()
        for run_id in run_ids:
            with self._lock:
                thread = self._threads.get(run_id)
            if thread is None or thread is current:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

    def recover_interrupted(self) -> int:
        return self._store.recover_interrupted()

    def _start_thread_locked(self, run_id: str, source: str) -> None:
        if run_id in self._threads:
            return
        cancel_event = _CoordinatedCancelEvent(self._cancel_key(run_id))
        thread = threading.Thread(
            target=self._run_job,
            args=(run_id, source, cancel_event),
            name=f"{self._kind}-{run_id}",
            daemon=True,
        )
        self._cancel_events[run_id] = cancel_event
        self._threads[run_id] = thread
        thread.start()

    def _run_job(
        self,
        run_id: str,
        source: str,
        cancel_event: threading.Event,
    ) -> None:
        try:
            # 守护复验与挖掘共用 "mining" heavy 槽: 两者都是全市场级内存任务,
            # 串行化避免双份大矩阵并发 (设计见 docs/auto-strategy-lab-plan.md §3.4)。
            with shared_heavy_job_limiter.slot("mining", cancel_event=cancel_event):
                if not self._mark_running(run_id, cancel_event):
                    return
                manifest = self._store.get(run_id)
                if manifest is None:
                    raise KeyError(run_id)
                payload = {
                    "run_id": run_id,
                    "request": manifest["request"],
                    "data_fingerprint": manifest["data_fingerprint"],
                    "source": source,
                }
                task = self._task_factory(self._kind, self._data_dir, payload)
                result = self._worker_runner(
                    task,
                    lambda progress: self._record_progress(run_id, progress, cancel_event),
                    cancel_event,
                )
                if not isinstance(result, dict):
                    raise TypeError("mining worker result must be a compact dict")
                self._finish_success(run_id, result, cancel_event)
        except HeavyJobCancelledError:
            self._finish_cancelled(run_id)
        except Exception as exc:  # noqa: BLE001 - worker 任意失败都要回填到 run 状态,避免悬挂
            if cancel_event.is_set():
                self._finish_cancelled(run_id)
            else:
                self._finish_failed(run_id, exc)
        finally:
            with self._lock:
                self._threads.pop(run_id, None)
                self._cancel_events.pop(run_id, None)

    def _mark_running(self, run_id: str, cancel_event: threading.Event) -> bool:
        with self._lock:
            if cancel_event.is_set():
                self._finish_cancelled_locked(run_id)
                return False
            self._store.transition_status(run_id, "running")
            # 记录 owner 供启动恢复判定存活（DIST-12）；文件版无跨实例恢复需求，
            # 仅 Redis 后端提供 set_run_owner。
            set_owner = getattr(self._store, "set_run_owner", None)
            if set_owner is not None:
                import os
                import socket

                set_owner(run_id, os.getpid(), socket.gethostname())
            self._store.append_event(run_id, "running", {"status": "running"})
            return True

    def _record_progress(
        self,
        run_id: str,
        progress: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        if not isinstance(progress, dict):
            raise TypeError("mining progress must be a compact dict")
        with self._lock:
            if cancel_event.is_set():
                return
            self._store.append_event(run_id, "progress", progress)
            self._store.write_summary(run_id, {"progress": progress})

    def _finish_success(
        self,
        run_id: str,
        result: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        status = result.get("status", "succeeded")
        if status not in _SUCCESS_STATUSES:
            raise ValueError(f"unsupported mining worker status: {status!r}")
        with self._lock:
            if cancel_event.is_set():
                self._finish_cancelled_locked(run_id)
                return
            self._store.write_summary(run_id, result)
            self._store.transition_status(run_id, status)
            self._store.append_event(run_id, status, {"status": status})

    def _finish_cancelled(self, run_id: str) -> None:
        with self._lock:
            self._finish_cancelled_locked(run_id)

    def _finish_cancelled_locked(self, run_id: str) -> None:
        manifest = self._store.get(run_id)
        if manifest is None or manifest["status"] in TERMINAL_RUN_STATUSES:
            return
        self._store.transition_status(run_id, "cancelled")
        self._store.append_event(run_id, "cancelled", {"status": "cancelled"})

    def _finish_failed(self, run_id: str, exc: Exception) -> None:
        message = str(exc)[:2000]
        with self._lock:
            manifest = self._store.get(run_id)
            if manifest is None or manifest["status"] in TERMINAL_RUN_STATUSES:
                return
            self._store.transition_status(run_id, "failed", error=message)
            self._store.append_event(
                run_id,
                "error",
                {"status": "failed", "message": message},
            )
