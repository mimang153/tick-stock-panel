"""缓存抽象（设计文档 §4.4）。

业务代码只依赖 ``CacheBackend``：当前实现是进程内存，将来换 Redis 时只换实现。
``generation`` 语义取代了文件型 ``EnrichedPublication``：``bump(name)`` 之后，
所有在此之前写入该命名空间的条目一律视为失效（§8.3）。

键名约定 ``命名空间:标识``（如 ``enriched:latest``、``pool:CSS300``），命名空间
就是 generation 的作用域。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

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
