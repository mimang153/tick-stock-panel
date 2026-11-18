"""写入缓冲（L1，用假 store，不需要真 CK）。

守的是 §4.2 的三条硬约束：阈值触发、退出前 flush、失败重试后丢弃并记账。
"""

from __future__ import annotations

import logging
import time

import polars as pl
import pytest

from app.storage.writer import BufferedWriter


class _FakeStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[str, pl.DataFrame]] = []
        self.fail = fail

    def write(self, table: str, df: pl.DataFrame) -> None:
        if self.fail:
            raise RuntimeError("ck down")
        self.calls.append((table, df.clone()))


def _frame(n: int) -> pl.DataFrame:
    return pl.DataFrame({"symbol": [f"S{i}" for i in range(n)], "close": [1.0] * n})


def _wait_until(predicate, *, timeout: float = 2.0) -> None:  # predicate: 无返回值的判定函数
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("等待条件超时")


def test_threshold_flush_happens_in_caller_thread() -> None:
    store = _FakeStore()
    writer = BufferedWriter(store, flush_interval_s=60, flush_rows=3)  # type: ignore[arg-type]
    try:
        writer.write("kline_daily", _frame(2))
        assert store.calls == []  # 未达阈值不落库
        assert writer.pending_rows == 2

        writer.write("kline_daily", _frame(1))

        assert len(store.calls) == 1  # 达到 3 行立即 flush
        assert store.calls[0][0] == "kline_daily"
        assert store.calls[0][1].height == 3
        assert writer.pending_rows == 0
    finally:
        writer.close()


def test_timer_flush_merges_same_table_into_one_insert() -> None:
    store = _FakeStore()
    writer = BufferedWriter(store, flush_interval_s=0.05, flush_rows=1000)  # type: ignore[arg-type]
    try:
        writer.write("kline_daily", _frame(1))
        writer.write("kline_daily", _frame(2))

        _wait_until(lambda: bool(store.calls))

        assert len(store.calls) == 1  # 同表的待写帧合并成一条 INSERT
        assert store.calls[0][1].height == 3
    finally:
        writer.close()


def test_frames_with_different_columns_are_merged_by_union() -> None:
    """同一窗口里历史帧(无 quote_ts) 与实时帧(有 quote_ts) 混写时不能丢列/报错。"""
    store = _FakeStore()
    writer = BufferedWriter(store, flush_interval_s=0.05, flush_rows=1000)  # type: ignore[arg-type]
    try:
        writer.write("kline_daily", _frame(1))
        writer.write(
            "kline_daily", _frame(1).with_columns(pl.lit(1_700_000_000_000).alias("quote_ts"))
        )

        _wait_until(lambda: bool(store.calls))

        payload = store.calls[0][1]
        assert payload.height == 2
        assert payload.columns == ["symbol", "close", "quote_ts"]
        assert payload["quote_ts"].null_count() == 1
    finally:
        writer.close()


def test_close_flushes_pending_rows() -> None:
    store = _FakeStore()
    writer = BufferedWriter(store, flush_interval_s=60, flush_rows=1000)  # type: ignore[arg-type]

    writer.write("adj_factor", _frame(2))
    writer.close()

    assert len(store.calls) == 1
    assert store.calls[0][1].height == 2
    assert writer.pending_rows == 0


def test_retries_then_drops_batch_and_counts_failure(caplog: pytest.LogCaptureFixture) -> None:
    store = _FakeStore(fail=True)
    writer = BufferedWriter(store, flush_interval_s=60, flush_rows=2, max_retries=2)  # type: ignore[arg-type]
    try:
        with caplog.at_level(logging.WARNING, logger="app.storage.writer"):
            writer.write("kline_daily", _frame(2))

        assert writer.failed_batches == 1
        assert writer.pending_rows == 0
        assert "丢弃该批" in caplog.text
        assert "第 1 次" in caplog.text  # 重试确实发生过
    finally:
        writer.close()


def test_write_after_close_is_rejected() -> None:
    writer = BufferedWriter(_FakeStore(), flush_interval_s=60, flush_rows=1000)  # type: ignore[arg-type]
    writer.close()

    with pytest.raises(RuntimeError, match="已关闭"):
        writer.write("kline_daily", _frame(1))


def test_empty_frame_is_ignored() -> None:
    store = _FakeStore()
    writer = BufferedWriter(store, flush_interval_s=60, flush_rows=1)  # type: ignore[arg-type]
    try:
        writer.write("kline_daily", pl.DataFrame())

        assert writer.pending_rows == 0
        assert store.calls == []
    finally:
        writer.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"flush_interval_s": 0},
        {"flush_rows": 0},
        {"max_retries": 0},
    ],
)
def test_invalid_parameters_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        BufferedWriter(_FakeStore(), **kwargs)  # type: ignore[arg-type]
