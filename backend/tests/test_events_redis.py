"""EventBus Redis Pub/Sub 后端：跨实例广播与退订语义。"""

from __future__ import annotations

import queue
import time

import pytest

from app.services.events import EventBus, EventBusUnavailableError, RedisEventBusBackend


@pytest.fixture()
def bus_factory(monkeypatch):
    """两个 EventBus 实例共享同一 fakeredis server，模拟多进程订阅同一频道。"""
    fakeredis = pytest.importorskip("fakeredis")
    server = fakeredis.FakeServer()
    buses: list[EventBus] = []

    def make_bus(channel: str) -> EventBus:
        backend = RedisEventBusBackend("redis://unused", channel, socket_timeout_s=1.0)
        monkeypatch.setattr(
            backend,
            "_new_client",
            lambda: fakeredis.FakeRedis(server=server, decode_responses=False),
        )
        bus = EventBus(backend)
        buses.append(bus)
        return bus

    yield make_bus
    for bus in buses:
        bus.close()


def test_redis_bus_broadcasts_across_instances(bus_factory) -> None:
    """两个独立发布/订阅实例均能收到同一事件，事件结构与内存后端一致。"""
    bus_a = bus_factory("tickflow:test:events:a")
    bus_b = bus_factory("tickflow:test:events:a")
    qa = bus_a.subscribe()
    qb = bus_b.subscribe()
    try:
        time.sleep(0.2)  # 等后台订阅线程建立连接
        bus_a.publish("alert", {"message": "hello"}, "read:analysis")
        for q in (qa, qb):
            event = q.get(timeout=3)
            assert event["type"] == "alert"
            assert event["data"] == {"message": "hello"}
            assert event["scope"] == "read:analysis"
            assert isinstance(event["id"], int)
    finally:
        bus_a.unsubscribe(qa)
        bus_b.unsubscribe(qb)


def test_redis_bus_unsubscribe_stops_delivery(bus_factory) -> None:
    """退订后不再向该队列投递事件。"""
    bus = bus_factory("tickflow:test:events:b")
    q = bus.subscribe()
    time.sleep(0.2)
    bus.unsubscribe(q)
    time.sleep(0.3)  # 等后台订阅线程退出，避免退订与在途消息竞态
    bus.publish("alert", {"message": "after"}, "*")
    with pytest.raises(queue.Empty):
        q.get(timeout=0.5)


def test_redis_bus_scoped_event_reaches_subscriber(bus_factory) -> None:
    """scope 随事件原样下发，过滤仍由 SSE 票据层负责。"""
    bus = bus_factory("tickflow:test:events:c")
    q = bus.subscribe()
    try:
        time.sleep(0.2)
        bus.publish("alert", {"message": "scoped"}, "read:analysis")
        assert q.get(timeout=3)["scope"] == "read:analysis"
    finally:
        bus.unsubscribe(q)


def test_redis_failure_raises_instead_of_dropping_event(monkeypatch) -> None:
    """Redis 不可用时 publish/subscribe 直接抛错，不静默丢事件或降级内存广播。"""
    backend = RedisEventBusBackend("redis://down:6379/0", "channel")

    def fail(*_args, **_kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(backend, "_new_client", fail)
    with pytest.raises(EventBusUnavailableError, match="Redis 事件总线不可用"):
        backend.publish("alert", {"message": "x"}, "*")
    with pytest.raises(EventBusUnavailableError, match="Redis 事件总线不可用"):
        backend.subscribe()
