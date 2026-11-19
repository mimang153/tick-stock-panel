"""事件总线 — SSE /api/events 的发布/订阅底座。

设计约束:
  - 默认进程内广播 (单进程 uvicorn)；配置 EVENT_BUS_BACKEND=redis 后跨实例广播。
  - Redis 后端用独立客户端：订阅连接进入 subscribe 模式后不复用缓存命令连接。
  - 订阅队列有界 (默认 200): 慢消费者丢最旧事件而不是拖垮发布方;
    SSE 断线由客户端换新票据重连补拉 (告警等持久数据另有 REST 出口)。
  - 事件带 required_scope: 流端点按票据 scope 过滤, 票据不放大权限。
"""

from __future__ import annotations

import contextlib
import json
import logging
import queue
import threading
import time
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_QUEUE_MAX = 200
_RECONNECT_BACKOFF_S = 0.5


class EventBusUnavailableError(RuntimeError):
    """Redis 事件总线不可用时直接抛出，禁止静默丢事件或降级内存广播。"""

    def __init__(self, operation: str, cause: BaseException, url: str) -> None:
        super().__init__(
            f"Redis 事件总线不可用（EVENT_BUS_BACKEND=redis, "
            f"EVENT_BUS_REDIS_URL={url}）：{operation} 失败：{cause}。"
            "请确认 Redis 可达后再重试。"
        )
        self.cause = cause


def _serialize(event_type: str, payload: dict[str, Any], scope: str) -> dict[str, Any]:
    return {
        "type": event_type,
        "scope": scope,
        "data": payload,
        "ts": time.time(),
    }


class EventBusBackend(Protocol):
    """事件总线后端契约: 与 EventBus 公开接口一一对应。"""

    def subscribe(self) -> queue.Queue[dict[str, Any]]: ...

    def unsubscribe(self, q: queue.Queue[dict[str, Any]]) -> None: ...

    def publish(self, event_type: str, payload: dict[str, Any], scope: str) -> None: ...

    def close(self) -> None: ...


def _push_event(q: queue.Queue[dict[str, Any]], event: dict[str, Any]) -> None:
    """有界队列投递: 满时丢最旧保最新 (监控类事件新比全重要)。"""
    try:
        q.put_nowait(event)
    except queue.Full:
        with contextlib.suppress(queue.Empty, queue.Full):
            q.get_nowait()
            q.put_nowait(event)


class MemoryEventBusBackend:
    """进程内广播后端 — 保持原有语义不变。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: list[queue.Queue[dict[str, Any]]] = []
        self._last_id = 0

    def subscribe(self) -> queue.Queue[dict[str, Any]]:
        q: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=_QUEUE_MAX)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue[dict[str, Any]]) -> None:
        with self._lock, contextlib.suppress(ValueError):
            self._subscribers.remove(q)

    def publish(self, event_type: str, payload: dict[str, Any], scope: str) -> None:
        event = _serialize(event_type, payload, scope)
        with self._lock:
            # SSE id 取毫秒时间戳但严格递增: 同毫秒多告警各得不同 id, 时钟回拨不倒退。
            self._last_id = max(int(event["ts"] * 1000), self._last_id + 1)
            event["id"] = self._last_id
            subscribers = list(self._subscribers)
        for q in subscribers:
            _push_event(q, event)

    def close(self) -> None:
        with self._lock:
            self._subscribers.clear()


class RedisEventBusBackend:
    """Redis Pub/Sub 后端: 每个订阅者独立连接，断线自动重连。"""

    def __init__(self, url: str, channel: str, *, socket_timeout_s: float = 1.0) -> None:
        self._url = url
        self._channel = channel
        self._socket_timeout_s = socket_timeout_s
        self._publish_client: Any = None
        self._client_lock = threading.Lock()
        self._subscribers: dict[
            queue.Queue[dict[str, Any]], tuple[threading.Event, threading.Thread]
        ] = {}

    def _new_client(self) -> Any:
        import redis

        return redis.Redis.from_url(
            self._url, decode_responses=False, socket_timeout=self._socket_timeout_s
        )

    def _get_publish_client(self) -> Any:
        with self._client_lock:
            if self._publish_client is None:
                self._publish_client = self._new_client()
            return self._publish_client

    def publish(self, event_type: str, payload: dict[str, Any], scope: str) -> None:
        event = _serialize(event_type, payload, scope)
        # 纳秒时间戳跨进程近似单调，避免毫秒 id 在多实例下碰撞。
        event["id"] = time.time_ns()
        try:
            client = self._get_publish_client()
            client.publish(self._channel, json.dumps(event, ensure_ascii=False))
        except Exception as exc:
            raise EventBusUnavailableError("publish", exc, self._url) from exc

    def subscribe(self) -> queue.Queue[dict[str, Any]]:
        # 启动即校验连接：不可用时直接抛错，不让后台线程静默重连伪装成订阅成功。
        probe = None
        try:
            probe = self._new_client()
            probe.ping()
        except Exception as exc:
            raise EventBusUnavailableError("subscribe 连接检查", exc, self._url) from exc
        finally:
            if probe is not None:
                with contextlib.suppress(Exception):
                    probe.close()
        q: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=_QUEUE_MAX)
        stop = threading.Event()
        thread = threading.Thread(
            target=self._listen, args=(q, stop), daemon=True, name="eventbus-redis-sub"
        )
        with self._client_lock:
            self._subscribers[q] = (stop, thread)
        thread.start()
        return q

    def _listen(self, q: queue.Queue[dict[str, Any]], stop: threading.Event) -> None:
        """订阅循环: 断线重连, 停止时由 unsubscribe 置位退出。"""
        while not stop.is_set():
            try:
                client = self._new_client()
                pubsub = client.pubsub()
                pubsub.subscribe(self._channel)
                while not stop.is_set():
                    message = pubsub.get_message(timeout=0.2, ignore_subscribe_messages=True)
                    if message and message.get("type") == "message" and not stop.is_set():
                        with contextlib.suppress(ValueError, TypeError, UnicodeDecodeError):
                            _push_event(q, json.loads(message["data"]))
                pubsub.close()
                client.close()
            except Exception:
                logger.exception(
                    "Redis 事件总线不可用（EVENT_BUS_REDIS_URL=%s），订阅中断；正在重试",
                    self._url,
                )
                stop.wait(_RECONNECT_BACKOFF_S)

    def unsubscribe(self, q: queue.Queue[dict[str, Any]]) -> None:
        with self._client_lock:
            entry = self._subscribers.pop(q, None)
        if entry is not None:
            entry[0].set()

    def close(self) -> None:
        for q in list(self._subscribers):
            self.unsubscribe(q)
        with self._client_lock:
            client = self._publish_client
            self._publish_client = None
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()


def _default_backend() -> EventBusBackend:
    from app.config import settings

    if settings.event_bus_backend == "redis":
        return RedisEventBusBackend(
            settings.event_bus_redis_url,
            settings.event_bus_redis_channel,
            socket_timeout_s=settings.event_bus_redis_socket_timeout_s,
        )
    return MemoryEventBusBackend()


class EventBus:
    """事件总线门面 — 公开接口与旧版一致，后端按配置注入。"""

    def __init__(self, backend: EventBusBackend | None = None) -> None:
        self._backend = backend if backend is not None else _default_backend()

    def subscribe(self) -> queue.Queue[dict[str, Any]]:
        return self._backend.subscribe()

    def unsubscribe(self, q: queue.Queue[dict[str, Any]]) -> None:
        self._backend.unsubscribe(q)

    def publish(self, event_type: str, payload: dict[str, Any], scope: str) -> None:
        """广播事件; scope = 订阅方票据需持有的最小 scope。"""
        self._backend.publish(event_type, payload, scope)

    def close(self) -> None:
        self._backend.close()


def sse_format(event: dict[str, Any]) -> str:
    """事件 → SSE 帧 (id 用总线分配的单调时间戳, EventSource lastEventId 可用于去重)。"""
    event_id = event.get("id", int(event["ts"] * 1000))
    return f"id: {event_id}\nevent: {event['type']}\ndata: {json.dumps(event['data'], ensure_ascii=False)}\n\n"


# 进程级单例 — 域模块与 API 层共用同一总线；后端由 EVENT_BUS_BACKEND 决定
bus = EventBus()
