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
