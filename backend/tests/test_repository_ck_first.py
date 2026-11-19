"""KlineRepository 的 CK 优先读路径回归：写 CK 后必须读到 CK 的值。"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from app.tickflow.repository import DataStore, KlineRepository


def _daily_frame(day: date, close: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "date": [day],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [100.0],
            "amount": [1000.0],
            "updated_at": [datetime(2025, 1, 2, 16)],
        }
    )


def _minute_frame(ts: datetime, close: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "datetime": [ts],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [100.0],
            "amount": [1000.0],
            "updated_at": [datetime(2025, 1, 2, 9, 36)],
        }
    )


def test_scan_daily_symbol_reads_ck_before_local(ck_runtime, tmp_path):
    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    day = date(2025, 1, 2)
    ck_runtime.write_daily("kline_daily_enriched", _daily_frame(day, 11.0))

    df = repo._scan_daily_symbol("600519.SH", day, day, ["close"])

    assert df["close"].to_list() == [11.0]


def test_get_minute_reads_ck_before_local(ck_runtime, tmp_path):
    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    ts = datetime(2025, 1, 2, 9, 35)
    ck_runtime.write_minute("kline_minute", _minute_frame(ts, 10.5))

    df = repo.get_minute("600519.SH", ts.date())

    assert df["close"].to_list() == [10.5]
