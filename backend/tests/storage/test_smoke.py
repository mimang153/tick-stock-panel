"""存储层契约（L2，真实 CK，库内带前缀表隔离）。

覆盖验收文档 §4.1（基础读写/去重）、§4.2（Nullable 与类型）、§4.6（时区）。
表由 ``ch`` fixture 建，结束时只 drop 本前缀的表。
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from app.storage import SCHEMA_VERSION, ClickHouseClient

_DAILY_SCHEMA = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
    "updated_at": pl.Datetime("us"),
}

_MINUTE_SCHEMA = {
    "symbol": pl.Utf8,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
    "updated_at": pl.Datetime("us"),
}

_INSTRUMENT_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "code": pl.Utf8,
    "exchange": pl.Utf8,
    "region": pl.Utf8,
    "type": pl.Utf8,
    "listing_date": pl.Utf8,
    "total_shares": pl.Float64,
    "float_shares": pl.Float64,
    "tick_size": pl.Float64,
    "limit_up": pl.Float64,
    "limit_down": pl.Float64,
    "as_of": pl.Date,
    "updated_at": pl.Datetime("us"),
}


def _daily_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH", "000001.SZ"],
            "date": [
                datetime(2024, 1, 2).date(),
                datetime(2023, 12, 29).date(),
                datetime(2024, 1, 2).date(),
            ],
            "open": [10.0, 9.0, 11.0],
            "high": [10.5, 9.5, 11.5],
            "low": [9.5, 8.5, 10.5],
            "close": [10.0, 9.0, 11.0],
            "volume": [1000.0, 900.0, 1100.0],
            "amount": [10000.0, 8100.0, 12100.0],
            "updated_at": [
                datetime(2024, 1, 2, 16, 0),
                datetime(2023, 12, 29, 16, 0),
                datetime(2024, 1, 2, 16, 0),
            ],
        },
        schema=_DAILY_SCHEMA,
    )


def test_schema_version_is_recorded(ch: ClickHouseClient) -> None:
    df = ch.query(f"SELECT version FROM {ch.table('schema_version')}")

    assert df["version"].to_list() == [SCHEMA_VERSION]


def test_daily_write_reads_back_with_declared_types(ch: ClickHouseClient) -> None:
    ch.insert(ch.table("kline_daily"), _daily_rows())

    df = ch.query(
        f"SELECT symbol, date, close FROM {ch.table('kline_daily')} ORDER BY symbol, date"
    )

    assert df.schema["date"] == pl.Date
    assert df["symbol"].to_list() == ["000001.SZ", "600519.SH", "600519.SH"]
    assert df["date"].to_list()[1:] == [
        datetime(2023, 12, 29).date(),
        datetime(2024, 1, 2).date(),
    ]


def test_repeated_key_keeps_latest_version(ch: ClickHouseClient) -> None:
    """ReplacingMergeTree(updated_at) 语义：同 key 后写覆盖先写，读侧 FINAL 收敛。"""
    table = ch.table("kline_daily")
    ch.insert(table, _daily_rows())
    ch.insert(
        table,
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "date": [datetime(2024, 1, 2).date()],
                "open": [10.0],
                "high": [12.0],
                "low": [10.0],
                "close": [12.0],
                "volume": [2000.0],
                "amount": [24000.0],
                "updated_at": [datetime(2024, 1, 3, 16, 0)],
            },
            schema=_DAILY_SCHEMA,
        ),
    )

    df = ch.query(
        f"SELECT symbol, date, close FROM {table} FINAL WHERE symbol = '600519.SH' "
        "AND date = toDate('2024-01-02')"
    )

    assert df.height == 1
    assert df["close"][0] == 12.0


def test_minute_datetime_keeps_beijing_wall_clock(ch: ClickHouseClient) -> None:
    """写入 naive 北京墙钟 → 读回同样的 naive 墙钟（不出现 ±8h 漂移）。"""
    table = ch.table("kline_minute")
    ch.insert(
        table,
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH"],
                "datetime": [datetime(2024, 1, 2, 9, 35), datetime(2024, 1, 2, 15, 0)],
                "open": [10.0, 10.2],
                "high": [10.1, 10.3],
                "low": [9.9, 10.1],
                "close": [10.2, 10.3],
                "volume": [100.0, 200.0],
                "amount": [1000.0, 2000.0],
                "updated_at": [datetime(2024, 1, 2, 16, 0)] * 2,
            },
            schema=_MINUTE_SCHEMA,
        ),
    )

    df = ch.query(f"SELECT datetime FROM {table} ORDER BY datetime")

    assert df.schema["datetime"] == pl.Datetime("us")
    assert df["datetime"].to_list() == [
        datetime(2024, 1, 2, 9, 35),
        datetime(2024, 1, 2, 15, 0),
    ]


def test_nullable_price_limit_stays_null(ch: ClickHouseClient) -> None:
    """缺少涨跌停价时必须是 NULL，不能退化成 0（否则前端会当"永远跌停"）。"""
    table = ch.table("instruments")
    ch.insert(
        table,
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "name": ["贵州茅台"],
                "code": ["600519"],
                "exchange": ["SH"],
                "region": ["CN"],
                "type": ["stock"],
                "listing_date": ["2001-08-27"],
                "total_shares": [1.256e9],
                "float_shares": [1.256e9],
                "tick_size": [0.01],
                "limit_up": [None],
                "limit_down": [None],
                "as_of": [datetime(2024, 1, 2).date()],
                "updated_at": [datetime(2024, 1, 2, 16, 0)],
            },
            schema=_INSTRUMENT_SCHEMA,
        ),
    )

    df = ch.query(
        f"SELECT symbol, limit_up, limit_down FROM {table} FINAL WHERE symbol = '600519.SH'"
    )

    assert df["limit_up"].to_list() == [None]
    assert df["limit_down"].to_list() == [None]


def test_date_range_is_closed_on_both_ends(ch: ClickHouseClient) -> None:
    table = ch.table("kline_daily")
    ch.insert(table, _daily_rows())

    df = ch.query(
        f"SELECT date FROM {table} FINAL WHERE symbol = '600519.SH' "
        "AND date BETWEEN toDate('2023-12-29') AND toDate('2024-01-02') ORDER BY date"
    )

    assert df["date"].to_list() == [
        datetime(2023, 12, 29).date(),
        datetime(2024, 1, 2).date(),
    ]


def test_empty_insert_is_noop(ch: ClickHouseClient) -> None:
    table = ch.table("kline_daily")

    ch.insert(table, pl.DataFrame(schema=_DAILY_SCHEMA))

    assert ch.query(f"SELECT count() AS n FROM {table}")["n"][0] == 0
