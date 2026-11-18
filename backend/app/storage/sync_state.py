"""同步状态表（设计文档 §3.4）。

取代两个旧机制：``preferences.json`` 里的 ``last_sync`` 时间、以及 enriched 的
``EnrichedPublication`` 发布标记文件（追溯见 §8.3）。

约定：

- 一种任务一行；状态变化用"插新版本行"表达，不做 UPDATE/mutation
- 只记当前状态，不记运行历史、不自动重试
- ``updated_at`` 在进程内保证严格递增：同毫秒内连续 mark 时 ``FINAL`` 收敛结果才是确定的
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

from app.storage.clickhouse import ClickHouseClient

STATUS_OK = "ok"
STATUS_RUNNING = "running"
STATUS_FAILED = "failed"

INTERRUPTED_ERROR = "上次同步中断"

_COLUMNS = ("task", "status", "last_ok_at", "last_error")


@dataclass(frozen=True)
class SyncState:
    task: str
    status: str
    last_ok_at: datetime | None
    last_error: str | None


class SyncStateStore:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client
        self._lock = threading.Lock()
        self._last_updated_at: datetime | None = None

    @property
    def table(self) -> str:
        return self._client.table("sync_state")

    # ── 写入 ────────────────────────────────────────────────────
    def mark_running(self, task: str) -> None:
        self._write(task, STATUS_RUNNING, self._last_ok_at(task), None)

    def mark_ok(self, task: str) -> None:
        self._write(task, STATUS_OK, self._next_updated_at(), None)

    def mark_failed(self, task: str, error: str) -> None:
        # 失败不清空上次成功时间：页面展示"上次成功"仍然有意义
        self._write(task, STATUS_FAILED, self._last_ok_at(task), error)

    def recover_interrupted(self) -> list[str]:
        """启动时把残留 ``running`` 标为中断；不自动重跑（§3.4）。"""
        df = self.all()
        if df.is_empty():
            return []
        stuck = df.filter(pl.col("status") == STATUS_RUNNING)
        for row in stuck.iter_rows(named=True):
            self._write(row["task"], STATUS_FAILED, row["last_ok_at"], INTERRUPTED_ERROR)
        return stuck["task"].to_list()

    # ── 读取 ────────────────────────────────────────────────────
    def get(self, task: str) -> SyncState | None:
        df = self._select(" WHERE task = {task:String}", {"task": task})
        if df.is_empty():
            return None
        row = df.row(0, named=True)
        return SyncState(**row)

    def all(self) -> pl.DataFrame:
        return self._select("", {})

    # ── 内部 ────────────────────────────────────────────────────
    def _select(self, where: str, params: dict) -> pl.DataFrame:
        columns = ", ".join(_COLUMNS)
        return self._client.query(
            f"SELECT {columns} FROM {self.table} FINAL{where} ORDER BY task",
            params,
        )

    def _last_ok_at(self, task: str) -> datetime | None:
        state = self.get(task)
        return state.last_ok_at if state is not None else None

    def _write(
        self, task: str, status: str, last_ok_at: datetime | None, error: str | None
    ) -> None:
        df = pl.DataFrame(
            {
                "task": [task],
                "status": [status],
                "last_ok_at": [last_ok_at],
                "last_error": [error],
                "updated_at": [self._next_updated_at()],
            },
            schema={
                "task": pl.Utf8,
                "status": pl.Utf8,
                "last_ok_at": pl.Datetime("us"),
                "last_error": pl.Utf8,
                "updated_at": pl.Datetime("us"),
            },
        )
        self._client.insert(self.table, df)

    def _next_updated_at(self) -> datetime:
        """保证进程内严格递增，避免同毫秒多次 mark 时 FINAL 结果不确定。"""
        with self._lock:
            now = datetime.now()
            if self._last_updated_at is not None and now <= self._last_updated_at:
                now = self._last_updated_at + timedelta(milliseconds=1)
            self._last_updated_at = now
            return now
