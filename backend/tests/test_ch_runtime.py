"""运行期存储单例与健康检查（L1）：未接线时行为不变，接线后 fail-closed。"""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from app.storage import runtime as storage_runtime
from app.storage.cache import MemoryCacheBackend

_SYNC_STATE_SCHEMA: dict[str, Any] = {
    "task": pl.Utf8,
    "status": pl.Utf8,
    "last_ok_at": pl.Datetime("us"),
    "last_error": pl.Utf8,
}


@pytest.fixture(autouse=True)
def _isolate_runtime():
    """每个用例前后清空全局 runtime，避免假客户端泄漏到后续 CK 测试。"""
    storage_runtime.shutdown_storage()
    yield
    storage_runtime.shutdown_storage()


class _FakeClient:
    """够用的假客户端：建表 DDL 照单全收，查询只回答版本号与空 sync_state。"""

    table_prefix = "t_"
    url = "http://fake:8123"
    database = "tsp"

    def __init__(self, *, alive: bool = True) -> None:
        self.alive = alive
        self.connected = False
        self.closed = False
        self.inserted: list[tuple[str, pl.DataFrame]] = []

    def connect(self, **kwargs) -> None:
        self.connected = True

    def execute(self, sql, parameters=None, *, settings=None) -> None:
        return None

    def query(self, sql, parameters=None, *, settings=None) -> pl.DataFrame:
        if "max(version)" in sql:
            return pl.DataFrame({"version": pl.Series([0], dtype=pl.UInt32)})
        if "sync_state" in sql:
            return pl.DataFrame(schema=_SYNC_STATE_SCHEMA)
        return pl.DataFrame()

    def insert(self, table, df) -> None:
        self.inserted.append((table, df))

    def table(self, name: str) -> str:
        return f"{self.table_prefix}{name}"

    def ping(self) -> bool:
        return self.alive

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_runtime():
    storage_runtime.shutdown_storage()
    yield
    storage_runtime.shutdown_storage()


def test_health_reports_not_initialized_without_connection() -> None:
    assert storage_runtime.storage_health() == {"status": "not_initialized"}
    assert storage_runtime.get_storage() is None
    with pytest.raises(RuntimeError, match="尚未初始化"):
        storage_runtime.require_storage()


def test_cache_health_reports_memory_backend() -> None:
    client = _FakeClient()
    storage_runtime.init_storage(client=client)  # type: ignore[arg-type]

    health = storage_runtime.cache_health()

    assert health["backend"] == "memory"
    assert health["status"] == "ok"
    assert health["reachable"] is True


def test_cache_health_reports_degraded_ping() -> None:
    class DownCache(MemoryCacheBackend):
        def ping(self) -> bool:
            return False

    client = _FakeClient()
    storage_runtime.init_storage(client=client, cache=DownCache())  # type: ignore[arg-type]

    health = storage_runtime.cache_health()

    assert health["backend"] == "memory"
    assert health["status"] == "degraded"
    assert health["reachable"] is False


def test_init_builds_runtime_and_is_idempotent() -> None:
    client = _FakeClient()

    first = storage_runtime.init_storage(client=client)  # type: ignore[arg-type]
    second = storage_runtime.init_storage(client=_FakeClient())  # type: ignore[arg-type]

    assert first is second  # 重复初始化返回同一实例
    assert client.connected is True
    assert first.store is not None
    assert first.cache is not None
    assert first.sync_state is not None
    assert storage_runtime.require_storage() is first


def test_health_reports_buffered_state() -> None:
    client = _FakeClient()
    runtime = storage_runtime.init_storage(client=client)  # type: ignore[arg-type]

    runtime.writer.write("kline_daily", pl.DataFrame({"symbol": ["A"]}))
    health = storage_runtime.storage_health()

    assert health["status"] == "ok"
    assert health["reachable"] is True
    assert health["pending_rows"] == 1
    assert health["failed_batches"] == 0


def test_health_degrades_when_ping_fails() -> None:
    storage_runtime.init_storage(client=_FakeClient(alive=False))  # type: ignore[arg-type]

    assert storage_runtime.storage_health()["status"] == "degraded"


def test_shutdown_flushes_and_closes() -> None:
    client = _FakeClient()
    runtime = storage_runtime.init_storage(client=client)  # type: ignore[arg-type]
    runtime.writer.write("kline_daily", pl.DataFrame({"symbol": ["A"]}))

    storage_runtime.shutdown_storage()

    assert client.closed is True
    assert client.inserted, "退出前必须把缓冲写完"
    assert storage_runtime.get_storage() is None
    storage_runtime.shutdown_storage()  # 可重复调用


def test_api_health_includes_clickhouse_section() -> None:
    from app.api import routes

    assert routes.health()["clickhouse"] == {"status": "not_initialized"}
