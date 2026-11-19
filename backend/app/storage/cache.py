"""缓存抽象（设计文档 §4.4）。

业务代码只依赖 ``CacheBackend``：默认实现是进程内存，配置
``CACHE_BACKEND=redis`` 后切换为 Redis，业务代码无需改动。
``generation`` 语义取代了文件型 ``EnrichedPublication``：``bump(name)`` 之后，
所有在此之前写入该命名空间的条目一律视为失效（§8.3）。

键名约定 ``命名空间:标识``（如 ``enriched:latest``、``pool:CSS300``），命名空间
就是 generation 的作用域。Redis 实现禁用 pickle：DataFrame 用 Arrow IPC，
dict 用 JSON。
"""

from __future__ import annotations

import base64
import io
import json
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable

import polars as pl


@runtime_checkable
class CacheBackend(Protocol):
    def get(self, key: str) -> pl.DataFrame | None: ...
    def set(self, key: str, df: pl.DataFrame, *, ttl_s: int | None = None) -> None: ...
    def get_json(self, key: str) -> dict | None: ...
    def set_json(self, key: str, value: dict, *, ttl_s: int | None = None) -> None: ...
    def invalidate(self, prefix: str) -> None: ...
    def generation(self, name: str) -> int: ...
    def bump(self, name: str) -> int: ...
    def clear(self) -> None: ...
    def ping(self) -> bool: ...


@dataclass(frozen=True)
class _Entry:
    value: object
    generation: int
    expires_at: float | None


def _namespace(key: str) -> str:
    return key.partition(":")[0]


class MemoryCacheBackend:
    """进程内存实现：直接持有 Polars DataFrame / dict，零序列化。

    线程安全：实时线程与请求线程会并发读写同一命名空间（§6.2）。
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._items: dict[str, _Entry] = {}
        self._generations: dict[str, int] = {}
        self._lock = threading.Lock()

    # ── DataFrame ──────────────────────────────────────────────
    def get(self, key: str) -> pl.DataFrame | None:
        entry = self._live(key)
        if entry is None or not isinstance(entry.value, pl.DataFrame):
            return None
        return entry.value

    def set(self, key: str, df: pl.DataFrame, *, ttl_s: int | None = None) -> None:
        self._put(key, df, ttl_s)

    # ── JSON ───────────────────────────────────────────────────
    def get_json(self, key: str) -> dict | None:
        entry = self._live(key)
        if entry is None or not isinstance(entry.value, dict):
            return None
        return entry.value

    def set_json(self, key: str, value: dict, *, ttl_s: int | None = None) -> None:
        self._put(key, value, ttl_s)

    # ── 失效 ───────────────────────────────────────────────────
    def invalidate(self, prefix: str) -> None:
        with self._lock:
            for key in [k for k in self._items if k.startswith(prefix)]:
                del self._items[key]

    def generation(self, name: str) -> int:
        with self._lock:
            return self._generations.get(name, 0)

    def bump(self, name: str) -> int:
        """递增命名空间版本；旧版本条目立即清掉，避免内存滞留。"""
        with self._lock:
            current = self._generations.get(name, 0) + 1
            self._generations[name] = current
            for key in [
                k
                for k, entry in self._items.items()
                if _namespace(k) == name and entry.generation < current
            ]:
                del self._items[key]
            return current

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def ping(self) -> bool:
        return True

    # ── 内部 ───────────────────────────────────────────────────
    def _put(self, key: str, value: object, ttl_s: int | None) -> None:
        with self._lock:
            namespace = _namespace(key)
            expires_at = None if ttl_s is None else self._clock() + ttl_s
            self._items[key] = _Entry(
                value=value,
                generation=self._generations.get(namespace, 0),
                expires_at=expires_at,
            )

    def _live(self, key: str) -> _Entry | None:
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                return None
            if entry.expires_at is not None and self._clock() >= entry.expires_at:
                del self._items[key]
                return None
            if entry.generation < self._generations.get(_namespace(key), 0):
                del self._items[key]
                return None
            return entry


@dataclass(frozen=True)
class _RedisEntry:
    kind: str
    generation: int
    expires_at: float | None
    payload: bytes


class RedisCacheBackend:
    """Redis 实现：Arrow IPC / JSON 序列化，generation 由 Redis 计数器维护。

    显式配置 Redis 时连接失败直接抛错，不静默降级到内存，避免多实例之间
    出现不一致的失效语义。
    """

    def __init__(
        self,
        url: str,
        *,
        prefix: str = "tickflow:cache:",
        clock: Callable[[], float] = time.time,
        client: Any | None = None,
        socket_timeout_s: float = 1.0,
    ) -> None:
        if client is None:
            try:
                import redis
            except ImportError as exc:  # pragma: no cover - 依赖通过 extra 安装
                raise RuntimeError(
                    "RedisCacheBackend 需要 redis 包：uv sync --extra cache-redis"
                ) from exc
            client = redis.Redis.from_url(
                url,
                decode_responses=False,
                socket_timeout=socket_timeout_s,
                socket_connect_timeout=socket_timeout_s,
            )
        self._client = client
        self._prefix = prefix
        self._clock = clock
        self._client.ping()

    # ── DataFrame ──────────────────────────────────────────────
    def get(self, key: str) -> pl.DataFrame | None:
        entry = self._live_entry(key)
        if entry is None or entry.kind != "df":
            return None
        return pl.read_ipc(io.BytesIO(entry.payload))

    def set(self, key: str, df: pl.DataFrame, *, ttl_s: int | None = None) -> None:
        buffer = io.BytesIO()
        df.write_ipc(buffer)
        self._put(key, "df", buffer.getvalue(), ttl_s)

    # ── JSON ───────────────────────────────────────────────────
    def get_json(self, key: str) -> dict | None:
        entry = self._live_entry(key)
        if entry is None or entry.kind != "json":
            return None
        value = json.loads(entry.payload.decode("utf-8"))
        return value if isinstance(value, dict) else None

    def set_json(self, key: str, value: dict, *, ttl_s: int | None = None) -> None:
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self._put(key, "json", payload, ttl_s)

    # ── 失效 ───────────────────────────────────────────────────
    def invalidate(self, prefix: str) -> None:
        self._delete_matching(f"{self._prefix}entry:{prefix}*")

    def generation(self, name: str) -> int:
        raw = cast(bytes | None, self._client.get(self._gen_key(name)))
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def bump(self, name: str) -> int:
        current = cast(int, self._client.incr(self._gen_key(name)))
        self.invalidate(f"{name}:")
        return current

    def clear(self) -> None:
        """只清缓存条目，保留 generation 计数器（与内存实现一致）。"""
        self._delete_matching(f"{self._prefix}entry:*")

    def ping(self) -> bool:
        return bool(self._client.ping())

    def client(self) -> Any:
        """返回底层 Redis 客户端，供需要原子脚本的专用缓存使用（如 strategy 缓存的 Hash+Lua）。"""
        return self._client

    def entry_prefix(self) -> str:
        """返回本实例使用的键前缀，便于专用缓存落在同一命名空间内。"""
        return self._prefix

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if close is not None:
            close()

    # ── 内部 ───────────────────────────────────────────────────
    def _entry_key(self, key: str) -> str:
        return f"{self._prefix}entry:{key}"

    def _gen_key(self, name: str) -> str:
        return f"{self._prefix}gen:{name}"

    def _delete_matching(self, pattern: str) -> None:
        keys = list(self._client.scan_iter(match=pattern, count=500))
        if keys:
            self._client.delete(*keys)

    @staticmethod
    def _encode_entry(entry: _RedisEntry) -> bytes:
        payload = {
            "kind": entry.kind,
            "generation": entry.generation,
            "expires_at": entry.expires_at,
            "payload": base64.b64encode(entry.payload).decode("ascii"),
        }
        return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _decode_entry(raw: bytes) -> _RedisEntry | None:
        try:
            payload = json.loads(raw)
            return _RedisEntry(
                kind=payload["kind"],
                generation=int(payload["generation"]),
                expires_at=payload.get("expires_at"),
                payload=base64.b64decode(payload["payload"]),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    def _live_entry(self, key: str) -> _RedisEntry | None:
        entry_key = self._entry_key(key)
        raw = cast(bytes | None, self._client.get(entry_key))
        if raw is None:
            return None
        entry = self._decode_entry(raw)
        if entry is None:
            return None
        stale = entry.generation < self.generation(_namespace(key))
        expired = entry.expires_at is not None and self._clock() >= entry.expires_at
        if stale or expired:
            self._client.delete(entry_key)
            return None
        return entry

    def _put(self, key: str, kind: str, payload: bytes, ttl_s: int | None) -> None:
        expires_at = None if ttl_s is None else self._clock() + ttl_s
        entry = _RedisEntry(kind, self.generation(_namespace(key)), expires_at, payload)
        encoded = self._encode_entry(entry)
        entry_key = self._entry_key(key)
        if ttl_s is None:
            self._client.set(entry_key, encoded)
        else:
            self._client.setex(entry_key, max(1, math.ceil(ttl_s)), encoded)
