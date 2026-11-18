"""收盘价过期分区回归（CK 版）：盘后管道必须重算"行数完整但价格停留在竞价前"的日期。

实时 flush 可能在收盘集合竞价结果发布前写入当日 enriched 行 (实测 2026-09-04: TickFlow
实时端点收盘后仍返回旧价, 3392/5554 只股票 enriched 收盘价与官方日线不符), 行数与
daily 相同, #223 的行数校验识别不到, 增量路径不会重算, 当日偏离值/动量等全部
enriched 消费方都会用旧价。``_prune_stale_price_partitions`` 按官方日线做
raw_close vs close 值级比对, 不一致即删除该日, 让增量重算按官方日线全市场重建。
"""

from __future__ import annotations

from datetime import date

import polars as pl

from app.jobs.daily_pipeline import _prune_stale_price_partitions
from app.storage.store import ClickHouseStore

DAILY = "kline_daily"
ENRICHED = "kline_daily_enriched"


def _write_day(
    store: ClickHouseStore,
    table: str,
    day: date,
    symbols: list[str],
    closes: list[float],
    col: str,
) -> None:
    store.write_daily(
        table,
        pl.DataFrame(
            {
                "symbol": symbols,
                "date": [day] * len(symbols),
                col: closes,
            }
        ),
    )


def _rows_on(store: ClickHouseStore, table: str, day: date) -> int:
    return store.read_daily(table, start=day, end=day, columns=["symbol"]).height


def test_stale_close_day_is_pruned(store: ClickHouseStore) -> None:
    day = date(2026, 9, 4)
    syms = ["002084.SZ", "600519.SH", "000001.SZ"]
    # 官方日线收盘 7.10, 实时写入的 enriched 停留在竞价前 7.07; 两侧覆盖一致
    _write_day(store, DAILY, day, syms, [7.10, 1330.10, 11.89], "close")
    _write_day(store, ENRICHED, day, syms, [7.07, 1330.10, 11.89], "raw_close")

    pruned = _prune_stale_price_partitions(store, DAILY, ENRICHED)

    assert pruned == ["2026-09-04"]
    assert _rows_on(store, ENRICHED, day) == 0
    assert _rows_on(store, DAILY, day) == 3


def test_matching_close_untouched(store: ClickHouseStore) -> None:
    day = date(2026, 9, 4)
    syms = ["002084.SZ", "600519.SH"]
    _write_day(store, DAILY, day, syms, [7.10, 1330.10], "close")
    _write_day(store, ENRICHED, day, syms, [7.10, 1330.10], "raw_close")

    assert _prune_stale_price_partitions(store, DAILY, ENRICHED) == []
    assert _rows_on(store, ENRICHED, day) == 2


def test_multiple_stale_days_all_pruned(store: ClickHouseStore) -> None:
    # 管道连续数日未触发重建时, 最近多个交易日的过期数据一并修复
    fresh: list[tuple[date, float]] = [
        (date(2026, 9, 4), 7.07),
        (date(2026, 9, 3), 6.88),
        (date(2026, 9, 2), 6.80),
    ]
    for day, stale in fresh:
        _write_day(store, DAILY, day, ["002084.SZ"], [stale + 0.03], "close")
        _write_day(store, ENRICHED, day, ["002084.SZ"], [stale], "raw_close")

    pruned = _prune_stale_price_partitions(store, DAILY, ENRICHED)

    assert sorted(pruned) == ["2026-09-02", "2026-09-03", "2026-09-04"]
    for day, _ in fresh:
        assert _rows_on(store, ENRICHED, day) == 0


def test_daily_day_missing_left_alone(store: ClickHouseStore) -> None:
    # 官方日线尚未同步的日期不比对 (留给当日正常流程)
    day = date(2026, 9, 4)
    _write_day(store, ENRICHED, day, ["002084.SZ"], [7.07], "raw_close")

    assert _prune_stale_price_partitions(store, DAILY, ENRICHED) == []
    assert _rows_on(store, ENRICHED, day) == 1
