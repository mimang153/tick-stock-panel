"""多实例协调层：Redis 取消标志与全局限流计数的跨实例语义。"""

from __future__ import annotations

import pytest

from app.services.coordination import (
    CoordinationUnavailableError,
    RedisCancelFlags,
    RedisJobState,
    RedisLimiterCounter,
)
from app.services.heavy_job_limiter import HeavyJobLimiter


@pytest.fixture()
def redis_client():
    fakeredis = pytest.importorskip("fakeredis")
    return fakeredis.FakeRedis(decode_responses=False)


def test_cancel_flag_visible_across_instances(redis_client) -> None:
    """一个实例注册并置取消，另一个实例可立即观察到。"""
    flags_a = RedisCancelFlags(redis_client, "test:coord:")
    flags_b = RedisCancelFlags(redis_client, "test:coord:")
    flags_a.register("job-1")
    assert not flags_b.is_set("job-1")
    assert flags_b.set("job-1")
    assert flags_a.is_set("job-1")
    flags_a.clear("job-1")
    assert not flags_b.is_set("job-1")


def test_limiter_counter_enforces_global_capacity(redis_client) -> None:
    """两个计数器实例共享容量上限，第三个获取被拒绝。"""
    counter_a = RedisLimiterCounter(redis_client, "test:limiter:shared")
    counter_b = RedisLimiterCounter(redis_client, "test:limiter:shared")
    assert counter_a.try_acquire(1, 2)
    assert counter_b.try_acquire(1, 2)
    assert not counter_a.try_acquire(1, 2)
    assert counter_b.used() == 2
    counter_a.release(1)
    assert counter_b.try_acquire(1, 2)


def test_heavy_limiters_share_global_capacity(redis_client) -> None:
    """两个 HeavyJobLimiter 共用 Redis 计数时不会各自超发。"""
    counter = RedisLimiterCounter(redis_client, "test:limiter:shared")
    limiter_a = HeavyJobLimiter(capacity=2, counter=counter)
    limiter_b = HeavyJobLimiter(capacity=2, counter=counter)
    assert limiter_a.acquire(timeout=0)
    assert limiter_b.acquire(timeout=0)
    assert not limiter_a.acquire(timeout=0)
    limiter_a.release()
    assert limiter_b.acquire(timeout=0)
    limiter_b.release()
    limiter_b.release()


def test_job_state_visible_across_instances(redis_client) -> None:
    """一个实例写入的任务终态可被另一实例 get / list 查询。"""
    state_a = RedisJobState(redis_client, "test:jobs:")
    state_b = RedisJobState(redis_client, "test:jobs:")
    state_a.put("job-1", {"id": "job-1", "status": "succeeded", "kind": "pipeline"})
    remote = state_b.get("job-1")
    assert remote is not None and remote["status"] == "succeeded"
    recent = state_b.recent(10)
    assert [j["id"] for j in recent] == ["job-1"]
    state_b.clear()
    assert state_a.get("job-1") is None


class _FailingClient:
    """所有 Redis 操作都抛连接错误，用于验证禁止降级。"""

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise ConnectionError("redis down")

        return fail


def test_redis_failure_raises_instead_of_degrading() -> None:
    """Redis 不可用时三类协调状态都抛明确异常，不返回默认语义。"""
    flags = RedisCancelFlags(_FailingClient(), "test:down:")
    counter = RedisLimiterCounter(_FailingClient(), "test:down:limiter")
    state = RedisJobState(_FailingClient(), "test:down:jobs")
    for target, method, args in (
        (flags, "register", ("job-x",)),
        (flags, "is_set", ("job-x",)),
        (flags, "set", ("job-x",)),
        (flags, "clear", ("job-x",)),
        (counter, "try_acquire", (1, 2)),
        (counter, "release", (1,)),
        (counter, "used", ()),
        (state, "put", ("job-x", {"id": "job-x"})),
        (state, "get", ("job-x",)),
        (state, "recent", (10,)),
        (state, "clear", ()),
    ):
        with pytest.raises(CoordinationUnavailableError, match="Redis 协调层不可用"):
            getattr(target, method)(*args)
