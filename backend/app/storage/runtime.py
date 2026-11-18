"""运行期存储单例（设计文档 §4.12 启动序列）。

启动顺序：连接 CK（重试）→ 幂等建表 → 把残留 ``running`` 标为中断 → 建写缓冲。
不可达时 fail-closed：抛 ``ClickHouseUnavailableError``，不进入半可用状态。

``get_storage()`` 在未初始化时返回 None，``/api/health`` 据此报 ``not_initialized``；
业务代码用 ``require_storage()``，未初始化直接报错，避免静默读到空数据。
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from app.storage.cache import CacheBackend, MemoryCacheBackend
from app.storage.clickhouse import ClickHouseClient
from app.storage.schema import ensure_schema
from app.storage.store import ClickHouseStore
from app.storage.sync_state import SyncStateStore
from app.storage.writer import BufferedWriter

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_runtime: StorageRuntime | None = None


class StorageRuntime:
    """一次进程内的存储句柄集合。"""

    def __init__(
        self,
        *,
        client: ClickHouseClient,
        store: ClickHouseStore,
        writer: BufferedWriter,
        cache: CacheBackend,
        sync_state: SyncStateStore,
    ) -> None:
        self.client = client
        self.store = store
        self.writer = writer
        self.cache = cache
        self.sync_state = sync_state

    def close(self) -> None:
        """先落库再断连：退出时不能丢已接受的写入（§4.2）。"""
        try:
            self.writer.close()
        finally:
            self.client.close()

    def health(self) -> dict:
        started = time.monotonic()
        alive = self.client.ping()
        return {
            "status": "ok" if alive else "degraded",
            "reachable": alive,
            "pending_rows": self.writer.pending_rows,
            "failed_batches": self.writer.failed_batches,
            "ping_ms": round((time.monotonic() - started) * 1000, 1),
        }


def init_storage(*, client: ClickHouseClient | None = None) -> StorageRuntime:
    """初始化全局存储；重复调用返回同一实例。"""
    global _runtime
    with _lock:
        if _runtime is not None:
            return _runtime
        if client is None:
            client = ClickHouseClient.from_settings()
        client.connect()
        ensure_schema(client)
        _warn_if_local_data_not_backfilled(client)
        sync_state = SyncStateStore(client)
        recovered = sync_state.recover_interrupted()
        if recovered:
            logger.warning("已把中断的同步任务标记为失败：%s", sorted(recovered))
        store = ClickHouseStore(client)
        _runtime = StorageRuntime(
            client=client,
            store=store,
            writer=BufferedWriter.from_settings(store),
            cache=MemoryCacheBackend(),
            sync_state=sync_state,
        )
        logger.info("存储层就绪（%s/%s）", client.url, client.database)
        return _runtime


def get_storage() -> StorageRuntime | None:
    return _runtime


def _warn_if_local_data_not_backfilled(client: ClickHouseClient) -> None:
    """CK 为空但本地仍有 Parquet 历史时给出明确提示。

    读路径已切 CK（设计文档 §8.4），未回填时页面会"看起来没有行情"，
    而数据其实还在 Parquet 里 —— 这种静默降级最容易误判，启动期必须告警。
    """
    try:
        frame = client.query(f"SELECT count() AS n FROM {client.table('kline_daily')}")
        if frame.is_empty() or int(frame["n"][0] or 0) > 0:
            return
        from app.config import settings

        root = Path(settings.data_dir) / "kline_daily"
        if root.exists() and any(root.glob("date=*")):
            logger.warning(
                "ClickHouse 的 kline_daily 为空，但本地仍有 Parquet 历史；读路径已切 CK，"
                "页面将读不到历史数据。请先执行 `python scripts/backfill_ck.py`。"
            )
    except Exception as e:  # noqa: BLE001 - 启动期提示失败不影响主流程
        logger.debug("CK 空表检查跳过: %s", e)


def require_storage() -> StorageRuntime:
    if _runtime is None:
        raise RuntimeError("存储层尚未初始化；启动流程应先调用 init_storage()")
    return _runtime


def shutdown_storage() -> None:
    """flush 写缓冲并断开连接；可重复调用。"""
    global _runtime
    with _lock:
        runtime, _runtime = _runtime, None
    if runtime is not None:
        runtime.close()


def storage_health() -> dict:
    """供 /api/health 使用；未初始化时不做任何连接尝试。"""
    runtime = _runtime
    if runtime is None:
        return {"status": "not_initialized"}
    return runtime.health()
