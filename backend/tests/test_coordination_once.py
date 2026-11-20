"""启动钩子跨 worker 单飞原语 `Coordination.once()` 契约测试。

S4 前置：memory 模式进程内去重（单 worker）；redis 模式用 SET NX 跨实例去重。
"""

from __future__ import annotations

import uuid

import pytest

from app.config import settings


def test_once_memory_single_shot(monkeypatch) -> None:
    """memory（单 worker）：同 key 只放行一次，不同 key 独立。"""
    monkeypatch.setattr(settings, "coordination_backend", "memory")
    from app.services.coordination import Coordination

    coord = Coordination()
    key = f"startup:test_{uuid.uuid4().hex}"
    assert coord.once(key) is True
    assert coord.once(key) is False
    assert coord.once(f"{key}_other") is True


def test_lock_memory_can_be_released(monkeypatch) -> None:
    """memory 模式的租约释放后可再次获取；lock 上下文 yield 持有 token。"""
    monkeypatch.setattr(settings, "coordination_backend", "memory")
    from app.services.coordination import Coordination

    coord = Coordination()
    key = f"repair:stock:{uuid.uuid4().hex}"
    token = coord.acquire_lock(key)
    assert token is not None
    assert coord.acquire_lock(key) is None
    coord.release_lock(key, token)
    token2 = coord.acquire_lock(key)
    assert token2 is not None
    # fence 在 memory 后端恒定通过（无跨实例），且 lock 上下文 yield token 真值。
    coord.fence(key, token2)
    coord.release_lock(key, token2)
    with coord.lock(key) as acquired:
        assert acquired  # yield 的是 token（bytes），非 bool
        coord.fence(key, acquired)


def test_fence_rejects_stale_token_redis(monkeypatch) -> None:
    """redis 模式：租约被接管后，旧 token 的 fence 必须拒绝写入（DIST-13）。"""
    pytest.importorskip("redis")
    monkeypatch.setattr(settings, "coordination_backend", "redis")
    monkeypatch.setattr(settings, "coordination_redis_url", settings.cache_redis_url)
    from app.services.coordination import Coordination

    try:
        first = Coordination()
    except Exception as exc:  # noqa: BLE001 - Redis 不可用时 skip，不误判失败
        pytest.skip(f"Redis 不可用: {exc}")
    second = Coordination()
    key = f"repair:stock:{uuid.uuid4().hex}"
    lock_key = f"{first._once_prefix}lock:{key}"
    try:
        token = first.acquire_lock(key)
        assert token is not None
        first.fence(key, token)  # 仍持有 → 通过
        # 模拟租约过期被接管：删除旧锁，second 拿新 token。
        first.release_lock(key, token)
        token2 = second.acquire_lock(key)
        assert token2 is not None
        with pytest.raises(PermissionError, match="租约已失效或被接管"):
            first.fence(key, token)  # 旧 token → 拒绝
        second.fence(key, token2)  # 新 token → 通过
        second.release_lock(key, token2)
    finally:
        assert first._once_client is not None
        first._once_client.delete(lock_key)


def test_once_redis_cross_worker(monkeypatch) -> None:
    """redis（多 worker）：两个独立实例的协调层只有一个能拿到执行权。"""
    pytest.importorskip("redis")
    monkeypatch.setattr(settings, "coordination_backend", "redis")
    # 四后端复用同一宿主机 Redis；协调 URL 在开发 .env 未必单独配置，
    # 用已配置的 cache URL（含密码）保证连到同一实例。
    monkeypatch.setattr(settings, "coordination_redis_url", settings.cache_redis_url)
    from app.services.coordination import Coordination

    try:
        first = Coordination()
    except Exception as exc:  # noqa: BLE001 - Redis 不可用时 skip，不误判失败
        pytest.skip(f"Redis 不可用: {exc}")
    second = Coordination()
    key = f"startup:test_{uuid.uuid4().hex}"
    try:
        assert first.once(key) is True
        assert second.once(key) is False
        assert first.once(key) is False
    finally:
        assert first._once_client is not None
        first._once_client.delete(first._once_prefix + key)


def test_lock_redis_cross_worker_release(monkeypatch) -> None:
    """redis 模式租约可跨实例互斥，并由持有者安全释放。"""
    pytest.importorskip("redis")
    monkeypatch.setattr(settings, "coordination_backend", "redis")
    monkeypatch.setattr(settings, "coordination_redis_url", settings.cache_redis_url)
    from app.services.coordination import Coordination

    try:
        first = Coordination()
    except Exception as exc:  # noqa: BLE001 - Redis 不可用时 skip，不误判失败
        pytest.skip(f"Redis 不可用: {exc}")
    second = Coordination()
    key = f"repair:stock:{uuid.uuid4().hex}"
    lock_key = f"{first._once_prefix}lock:{key}"
    try:
        token = first.acquire_lock(key)
        assert token is not None
        assert second.acquire_lock(key) is None
        second.release_lock(key, token)
        token2 = second.acquire_lock(key)
        assert token2 is not None
        first.release_lock(key, token)
        assert second.acquire_lock(key) is None
        second.release_lock(key, token2)
        assert second.acquire_lock(key) is not None
    finally:
        assert first._once_client is not None
        first._once_client.delete(lock_key)
