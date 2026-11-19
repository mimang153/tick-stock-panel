"""polars collect 并发闸。

共享执行器并发 collect 存在死锁风险 (#24448 等), 用进程级信号量限制在飞数量。
车道: background 最多占 background_permits 个, 其余留给 interactive; 顺序恒为
background → 总闸, 无环。worker 子进程单任务串行执行, 不经过本闸。
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

import polars as pl

from app.config import settings

CollectPriority = Literal["interactive", "background"]

_TOTAL_GATE = threading.BoundedSemaphore(settings.polars_collect_permits)
_BACKGROUND_LANE = threading.BoundedSemaphore(settings.polars_collect_background_permits)


@contextmanager
def collect_slot(priority: CollectPriority = "interactive") -> Iterator[None]:
    """占用一个 collect 闸位; background 需同时占用车道位与总闸位。"""
    if priority == "background":
        with _BACKGROUND_LANE, _TOTAL_GATE:
            yield
        return
    with _TOTAL_GATE:
        yield


def guarded_collect(
    lf: pl.LazyFrame,
    *,
    priority: CollectPriority = "interactive",
    **kwargs: object,
) -> pl.DataFrame:
    """在并发闸内执行 LazyFrame.collect; 语义不变, 仅串行化调度。"""
    with collect_slot(priority):
        return lf.collect(**kwargs)
