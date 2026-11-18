"""写入缓冲（设计文档 §4.2）。

所有写请求先进本缓冲，满足 **2s 或 5000 行** 任一条件即按表批量 INSERT：

- 同表的待写帧在 flush 时合并成**一条** INSERT —— CK 的语句级原子性靠这一点承接
  "一次发布 = 一条 INSERT"的硬约束（§4.2），不再需要文件发布标记
- 失败重试 3 次后丢弃该批并记 error（F3），``failed_batches`` 供 /api/health 展示 degraded
- ``close()`` 必须先 flush，进程退出不丢已接受的数据

不在这里做去重/合并：ReplacingMergeTree 按 ``(symbol, date)+updated_at`` 收敛，
后写覆盖先写（§4.2）。
"""

from __future__ import annotations

import logging
import threading

import polars as pl

from app.storage.protocol import StorageProtocol

logger = logging.getLogger(__name__)


class BufferedWriter:
    """按表分组的批量写入缓冲。线程安全，可被实时线程与后台同步共用。"""

    def __init__(
        self,
        store: StorageProtocol,
        *,
        flush_interval_s: float = 2.0,
        flush_rows: int = 5000,
        max_retries: int = 3,
    ) -> None:
        if flush_interval_s <= 0 or flush_rows <= 0 or max_retries < 1:
            raise ValueError("写缓冲参数必须为正：flush_interval_s / flush_rows / max_retries")
        self._store = store
        self._flush_interval_s = flush_interval_s
        self._flush_rows = flush_rows
        self._max_retries = max_retries
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._pending: dict[str, list[pl.DataFrame]] = {}
        self._pending_rows = 0
        self._failed_batches = 0
        self._closed = False
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="ch-write-buffer",
            daemon=True,
        )
        self._thread.start()

    @classmethod
    def from_settings(cls, store: StorageProtocol) -> BufferedWriter:
        from app.config import settings

        return cls(
            store,
            flush_interval_s=settings.ch_write_flush_interval_ms / 1000.0,
            flush_rows=settings.ch_write_flush_rows,
        )

    # ── 状态 ────────────────────────────────────────────────────
    @property
    def pending_rows(self) -> int:
        with self._lock:
            return self._pending_rows

    @property
    def failed_batches(self) -> int:
        with self._lock:
            return self._failed_batches

    # ── 写入 ────────────────────────────────────────────────────
    def write(self, table: str, df: pl.DataFrame) -> None:
        """接收一批行；达到行数阈值时立即 flush（在调用线程内完成）。"""
        if df.is_empty():
            return
        with self._lock:
            if self._closed:
                raise RuntimeError("写缓冲已关闭，拒绝新的写入")
            self._pending.setdefault(table, []).append(df)
            self._pending_rows += df.height
            due = self._pending_rows >= self._flush_rows
        if due:
            self.flush()

    def flush(self) -> None:
        """把当前缓冲全部落库；空缓冲直接返回。"""
        with self._flush_lock:
            with self._lock:
                batches = self._pending
                self._pending = {}
                self._pending_rows = 0
            for table, frames in batches.items():
                self._insert(table, frames)

    def close(self) -> None:
        """停止后台线程并落库剩余数据。"""
        self._stop.set()
        self._thread.join(timeout=self._flush_interval_s * 2 + 1)
        self.flush()
        with self._lock:
            self._closed = True

    # ── 内部 ────────────────────────────────────────────────────
    def _run(self) -> None:
        while not self._stop.wait(self._flush_interval_s):
            if self.pending_rows:
                self.flush()

    def _insert(self, table: str, frames: list[pl.DataFrame]) -> None:
        # diagonal：同一个窗口里可能混入不同列集的帧（历史日K 没有 quote_ts，
        # 实时快照帧有），按列并集对齐，缺失列落 NULL，其余交给 CK 的 Nullable。
        payload = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")
        for attempt in range(1, self._max_retries + 1):
            try:
                self._store.write(table, payload)
                return
            except Exception as exc:  # noqa: BLE001 - 重试耗尽后必须记账而非崩溃
                if attempt == self._max_retries:
                    with self._lock:
                        self._failed_batches += 1
                    logger.error(
                        "写入 %s 失败（%d 行，已重试 %d 次），丢弃该批：%s",
                        table,
                        payload.height,
                        attempt,
                        exc,
                    )
                    return
                logger.warning("写入 %s 失败（第 %d 次），准备重试：%s", table, attempt, exc)
