"""仓储门面契约（L2，真实 CK）：验收文档 §4.1~§4.4 的主干数据集部分。

只用 ``store`` 门面，不拼 SQL —— 这是"业务代码看不到表名/SQL"的回归防线。
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import polars as pl
import pytest

from app.storage.schema import TABLE_READ_COLUMNS
from app.storage.store import ClickHouseStore
from app.storage.writer import BufferedWriter

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

_ADJ_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "ex_factor": pl.Float64,
    "dividend": pl.Float64,
    "bonus": pl.Float64,
    "allot": pl.Float64,
    "allot_price": pl.Float64,
    "prev_close": pl.Float64,
    "updated_at": pl.Datetime("us"),
}


def _daily_frame(rows: list[tuple[str, date, float]], *, updated_at: datetime) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "date": [r[1] for r in rows],
            "open": [r[2] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[2] for r in rows],
            "close": [r[2] for r in rows],
            "volume": [1.0] * len(rows),
            "amount": [1.0] * len(rows),
            "updated_at": [updated_at] * len(rows),
        },
        schema=_DAILY_SCHEMA,
    )


THREE_ROWS = [
    ("600519.SH", date(2024, 1, 2), 10.0),
    ("600519.SH", date(2023, 12, 29), 9.0),
    ("000001.SZ", date(2024, 1, 2), 11.0),
]


def test_read_daily_returns_parquet_projection_sorted(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    df = store.read_daily("kline_daily")

    assert df.columns == list(TABLE_READ_COLUMNS["kline_daily"])  # 不多不少，顺序稳定
    assert df["symbol"].to_list() == ["000001.SZ", "600519.SH", "600519.SH"]
    assert df["date"].to_list()[1:] == [date(2023, 12, 29), date(2024, 1, 2)]


def test_repeated_key_keeps_latest_and_both_dedupe_paths_agree(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))
    store.write_daily(
        "kline_daily",
        _daily_frame([("600519.SH", date(2024, 1, 2), 12.0)], updated_at=datetime(2024, 1, 3, 16)),
    )

    final = store.read_daily("kline_daily", dedupe="final")
    argmax = store.read_daily("kline_daily", dedupe="argmax")

    assert final.equals(argmax)  # 验收 §4.3：两条去重路径结果一致
    assert final.height == 3
    latest = final.filter((pl.col("symbol") == "600519.SH") & (pl.col("date") == date(2024, 1, 2)))
    assert latest["close"].to_list() == [12.0]


def test_filters_are_closed_on_both_ends(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    df = store.read_daily(
        "kline_daily",
        symbols=["600519.SH"],
        start=date(2023, 12, 29),
        end=date(2024, 1, 2),
    )

    assert df["date"].to_list() == [date(2023, 12, 29), date(2024, 1, 2)]


def test_empty_symbol_list_returns_no_rows(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    assert store.read_daily("kline_daily", symbols=[]).is_empty()


def test_column_pruning_and_unknown_column(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    df = store.read_daily("kline_daily", columns=["close"])

    assert df.columns == ["close"]
    with pytest.raises(ValueError, match="不存在列"):
        store.read_daily("kline_daily", columns=["not_a_column"])


@pytest.mark.parametrize("table", ["instruments", "kline_minute", "nope"])
def test_read_daily_rejects_non_daily_tables(store: ClickHouseStore, table: str) -> None:
    with pytest.raises(ValueError):
        store.read_daily(table)


def test_delete_daily_is_immediately_visible(store: ClickHouseStore) -> None:
    day = date(2024, 1, 2)
    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    store.delete_daily("kline_daily", day=day, symbols=["000001.SZ"])

    remaining = store.read_daily("kline_daily")
    assert "000001.SZ" not in remaining["symbol"].to_list()
    # 删除不存在的行不报错、无副作用
    store.delete_daily("kline_daily", day=date(2020, 1, 2), symbols=["000001.SZ"])
    assert store.read_daily("kline_daily").height == remaining.height


def test_latest_daily_date_is_none_when_empty(store: ClickHouseStore) -> None:
    assert store.latest_daily_date("kline_daily") is None

    store.write_daily("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))

    assert store.latest_daily_date("kline_daily") == date(2024, 1, 2)


def test_write_drops_unknown_columns_with_warning(
    store: ClickHouseStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)).with_columns(
        pl.lit("x").alias("snapshot_note"),
    )

    with caplog.at_level(logging.WARNING, logger="app.storage.store"):
        store.write_daily("kline_daily", df)

    assert "丢弃未知列" in caplog.text
    assert store.read_daily("kline_daily").height == 3


def test_empty_write_is_noop(store: ClickHouseStore) -> None:
    store.write_daily("kline_daily", pl.DataFrame(schema=_DAILY_SCHEMA))

    assert store.read_daily("kline_daily").is_empty()


def test_instruments_roundtrip_keeps_null_and_latest(store: ClickHouseStore) -> None:
    df = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "name": ["贵州茅台", "平安银行"],
            "code": ["600519", "000001"],
            "exchange": ["SH", "SZ"],
            "region": ["CN", "CN"],
            "type": ["stock", "stock"],
            "listing_date": ["2001-08-27", "1991-04-03"],
            "total_shares": [1.256e9, 1.94e10],
            "float_shares": [1.256e9, 1.94e10],
            "tick_size": [0.01, 0.01],
            "limit_up": [None, 12.0],
            "limit_down": [None, None],
            "as_of": [date(2024, 1, 2)] * 2,
            "updated_at": [datetime(2024, 1, 2, 16)] * 2,
        },
        schema=_INSTRUMENT_SCHEMA,
    )
    store.write_instruments("stock", df)
    store.write_instruments(
        "stock",
        df.filter(pl.col("symbol") == "000001.SZ").with_columns(
            pl.lit("平安银行A").alias("name"),
            pl.lit(datetime(2024, 1, 3, 16)).alias("updated_at"),
        ),
    )

    read = store.read_instruments("stock")

    assert read.columns == list(TABLE_READ_COLUMNS["instruments"])
    assert read["symbol"].to_list() == ["000001.SZ", "600519.SH"]  # 按 symbol 升序
    assert read.filter(pl.col("symbol") == "000001.SZ")["name"].to_list() == ["平安银行A"]
    assert read.filter(pl.col("symbol") == "600519.SH")["limit_up"].to_list() == [None]


def test_adj_factor_uses_trade_date(store: ClickHouseStore) -> None:
    rows = [
        ("600519.SH", date(2023, 6, 1), 2.0),
        ("600519.SH", date(2024, 6, 3), 2.1),
        ("000001.SZ", date(2024, 6, 3), 1.5),
    ]
    store.write_adj_factor(
        pl.DataFrame(
            {
                "symbol": [r[0] for r in rows],
                "trade_date": [r[1] for r in rows],
                "ex_factor": [r[2] for r in rows],
                "dividend": [0.0] * 3,
                "bonus": [0.0] * 3,
                "allot": [0.0] * 3,
                "allot_price": [0.0] * 3,
                "prev_close": [0.0] * 3,
                "updated_at": [datetime(2024, 7, 1, 16)] * 3,
            },
            schema=_ADJ_SCHEMA,
        ),
    )

    df = store.read_adj_factor(
        symbols=["600519.SH"], start=date(2024, 1, 1), end=date(2024, 12, 31)
    )

    assert df.columns == list(TABLE_READ_COLUMNS["adj_factor"])
    assert df["trade_date"].to_list() == [date(2024, 6, 3)]
    assert store.read_adj_factor(etf=True).is_empty()


def test_unknown_asset_type_rejected(store: ClickHouseStore) -> None:
    with pytest.raises(ValueError, match="未知资产类型"):
        store.read_instruments("bond")


def test_buffered_write_lands_in_clickhouse(store: ClickHouseStore) -> None:
    """三层串联（writer → store → client）：close 时的 flush 必须真的落库。"""
    writer = BufferedWriter(store, flush_interval_s=60, flush_rows=1000)
    try:
        writer.write("kline_daily", _daily_frame(THREE_ROWS, updated_at=datetime(2024, 1, 2, 16)))
        assert writer.pending_rows == 3
    finally:
        writer.close()

    assert store.read_daily("kline_daily").height == 3
