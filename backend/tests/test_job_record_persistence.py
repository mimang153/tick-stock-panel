"""回归测试: job 记录跨进程死亡可恢复(「数据在、记录丢」补丁)。

DES-006 后的契约:
  - create()/start()/终态经 app.state 门面写入 Redis live state;
  - 启动时 recover_interrupted_jobs() 把遗留 pending/running 补标为 failed,
    finished_at 取最后已知存活时刻, 不虚增时长;
  - 终态记录不受补录影响, 且不再产生 job_store/*.json 文件。
"""

from __future__ import annotations

from app.services.pipeline_jobs import JobStore, recover_interrupted_jobs
from app.state import get_job_state


def test_create_writes_pending_snapshot_to_live_state():
    store = JobStore()
    job_id, _ = store.create(timeout_s=60)

    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "pending"
    assert job["stage"] == "init"


def test_start_updates_live_snapshot_to_running():
    store = JobStore()
    job_id, _ = store.create(timeout_s=60)
    store.start(job_id)

    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "running"
    assert job["started_at"] is not None


# ── 进程死亡 → 启动恢复补录 ──────────────────────────────────────────


def test_orphan_running_record_is_reaped_on_next_boot():
    """核心场景: 进程死在 running(甚至工作已做完但未终态), 记录必须可见。"""
    dead = JobStore()
    job_id, _ = dead.create(timeout_s=60)
    dead.start(job_id)
    dead.progress(job_id, "sync", 50, "halfway")

    # 新进程 = 新实例(内存为空, 只有 live state) + 启动恢复入口
    revived = JobStore()
    assert recover_interrupted_jobs() >= 1
    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert "中断" in job["error"]
    assert job["finished_at"] is not None
    # finished_at 来自最后已知存活时刻, 时长不得虚增为负或巨大
    assert job["duration_s"] is not None
    assert 0 <= job["duration_s"] <= 60
    assert any(item["id"] == job_id for item in revived.list_recent())


def test_orphan_pending_record_is_reaped():
    """进程死在 create() 与 start() 之间: 记录同样可见, 时长为 None。"""
    dead = JobStore()
    job_id, _ = dead.create(timeout_s=60)
    # 未 start 即死亡

    recover_interrupted_jobs()
    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert job["duration_s"] is None


def test_reap_does_not_touch_terminal_records():
    store = JobStore()
    job_id, _ = store.create(timeout_s=60)
    store.start(job_id)
    store.succeed(job_id, {"daily_rows": 100})

    recover_interrupted_jobs()
    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["result"] == {"daily_rows": 100}


def test_reap_allows_new_job_after_dead_orphan():
    """补录后旧 job 已 failed: 新进程 create() 不被死孤儿阻塞(单飞只看内存)。"""
    dead = JobStore()
    old_job_id, _ = dead.create(timeout_s=60)
    dead.start(old_job_id)

    recover_interrupted_jobs()
    revived = JobStore()
    new_job_id, is_new = revived.create(timeout_s=60)
    assert is_new is True
    assert new_job_id != old_job_id


# ── 终态覆盖 live 快照 ───────────────────────────────────────────────


def test_terminal_write_replaces_running_snapshot():
    store = JobStore()
    job_id, _ = store.create(timeout_s=60)
    store.start(job_id)
    store.fail(job_id, "boom")

    job = get_job_state(job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert job["error"] == "boom"
