"""用户状态 Redis 门面（DES-006）。

业务层只依赖本模块，不直接持有 Redis 客户端；JSON 序列化只承载可缓存短态。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import redis

from app.config import settings

logger = logging.getLogger(__name__)
_LOCK = threading.Lock()
# redis 6.x 类型存根把同步方法返回推断为 Awaitable|Any，按项目惯例标 Any（见 services/events.py）。
_CLIENT: Any | None = None
_LISTENERS: dict[str, list[Callable[[str], None]]] = {}
_LISTENER_STARTED = False


def _client() -> Any:
    """返回进程内 Redis 客户端；首次访问时创建。"""
    global _CLIENT
    with _LOCK:
        if _CLIENT is None:
            _CLIENT = redis.Redis.from_url(settings.user_state_redis_url, decode_responses=True)
        return _CLIENT


def _key(name: str) -> str:
    """拼接带前缀和 generation 的 Redis key。"""
    return f"{settings.user_state_redis_prefix}{name}"


def ping() -> None:
    """校验 Redis 连通性；不可达时抛 RedisError 让启动 fail-fast。"""
    _client().ping()


def check_persistence() -> None:
    """提示性检查 AOF/noeviction（DES-006 §3.2）：不满足仅告警，不阻塞启动。"""
    try:
        info = _client().config_get("appendonly", "maxmemory-policy", "appendfsync")
    except redis.RedisError as exc:
        logger.warning("user state Redis 持久化检查失败: %s", exc)
        return
    if info.get("appendonly") not in ("yes", True, 1, "1"):
        logger.warning("user state Redis 未开启 AOF (appendonly)，会话/任务热态重启后丢失")
    if str(info.get("maxmemory-policy", "")).lower() != "noeviction":
        logger.warning(
            "user state Redis maxmemory-policy=%s，期望 noeviction；会话/任务态可能被驱逐",
            info.get("maxmemory-policy"),
        )


def get_json(name: str) -> Any | None:
    """读取 JSON 值；不存在或解析失败返回 None。"""
    raw = _client().get(_key(name))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("invalid cached json: %s", name)
        return None


def set_json(name: str, value: Any, ttl_s: int | None = None) -> None:
    """写入 JSON 值，可选 TTL 秒。"""
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if ttl_s is None:
        _client().set(_key(name), raw)
    else:
        _client().set(_key(name), raw, ex=max(1, int(ttl_s)))


def delete_key(name: str) -> None:
    """删除一个状态 key。"""
    _client().delete(_key(name))


def delete_prefix(prefix: str) -> int:
    """按前缀删除 key；返回删除数量。"""
    keys = list(_client().scan_iter(match=_key(prefix) + "*"))
    return int(_client().delete(*keys)) if keys else 0


def generation(name: str) -> str:
    """读取命名空间的 generation，不存在返回 0。"""
    return str(_client().get(_key(f"generation:{name}")) or "0")


def bump_generation(name: str) -> str:
    """递增命名空间 generation 并返回新值。"""
    value = _client().incr(_key(f"generation:{name}"))
    return str(value)


def publish(channel: str, message: str) -> None:
    """发布配置变更消息；失败只记录日志，不阻断主流程。"""
    try:
        _client().publish(_key(f"pubsub:{channel}"), message)
    except redis.RedisError as exc:
        logger.warning("pubsub publish failed: %s", exc)


def subscribe(channel: str, callback: Callable[[str], None]) -> None:
    """注册进程内回调；真实订阅由 start_listener 驱动。"""
    _LISTENERS.setdefault(channel, []).append(callback)


def start_listener() -> None:
    """启动后台订阅线程；重复调用安全。"""
    global _LISTENER_STARTED
    with _LOCK:
        if _LISTENER_STARTED:
            return
        _LISTENER_STARTED = True
    threading.Thread(target=_listen, name="user-state-events", daemon=True).start()


def _listen() -> None:
    """订阅所有已注册频道并分发消息；断线指数退避。"""
    backoff = 1
    while True:
        try:
            pubsub = _client().pubsub()
            channels = [_key(f"pubsub:{name}") for name in _LISTENERS]
            if channels:
                pubsub.subscribe(*channels)
            for message in pubsub.listen():
                if message.get("type") == "message":
                    _dispatch(str(message.get("channel", "")), str(message.get("data", "")))
            backoff = 1
        except redis.RedisError as exc:
            logger.warning("user state listener retry in %ss: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _dispatch(channel: str, message: str) -> None:
    """按频道后缀分发消息给本地回调。"""
    suffix = channel.removeprefix(settings.user_state_redis_prefix + "pubsub:")
    for callback in list(_LISTENERS.get(suffix, [])):
        try:
            callback(message)
        except Exception as exc:  # noqa: BLE001 - 回调异常不能中断事件线程
            logger.warning("user state event callback failed: %s", exc)


def session_key(token: str) -> str:
    """生成会话 token 的哈希 key。"""
    return "session:" + hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_session(token: str) -> dict | None:
    """读取会话 payload。"""
    value = get_json(session_key(token))
    return value if isinstance(value, dict) else None


def set_session(token: str, payload: dict, ttl_s: int) -> None:
    """写入带 TTL 的会话 payload。"""
    set_json(session_key(token), payload, ttl_s)


def delete_session(token: str) -> None:
    """删除指定会话。"""
    delete_key(session_key(token))
