"""ClickHouse 客户端类型/时区边界（L1，纯函数，不需要真 CK）。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pyarrow as pa
import pytest

from app.storage.clickhouse import (
    BEIJING_TZ,
    ClickHouseClient,
    from_arrow,
    parse_endpoint,
    to_arrow,
)


def test_parse_endpoint_reads_http_defaults() -> None:
    assert parse_endpoint("http://host.docker.internal:8123") == (
        "host.docker.internal",
        8123,
        False,
    )
    assert parse_endpoint("https://ck.internal") == ("ck.internal", 8443, True)
    assert parse_endpoint("http://localhost") == ("localhost", 8123, False)


@pytest.mark.parametrize("url", ["localhost:8123", "", "tcp://localhost:9000"])
def test_parse_endpoint_rejects_non_http(url: str) -> None:
    with pytest.raises(ValueError, match="CLICKHOUSE_URL"):
        parse_endpoint(url)


def test_to_arrow_marks_naive_datetime_as_beijing_wall_clock() -> None:
    """naive datetime 原样解释为北京墙钟，不能当成 UTC（否则整体偏移 8 小时）。"""
    df = pl.DataFrame({"datetime": [datetime(2024, 1, 2, 9, 35)]})

    table = to_arrow(df)

    field = table.schema.field("datetime")
    assert field.type.tz == BEIJING_TZ
    value = table.column("datetime")[0].as_py()
    assert value.utcoffset() == timedelta(hours=8)
    assert (value.hour, value.minute) == (9, 35)


def test_to_arrow_converts_aware_datetime_by_instant() -> None:
    """已带时区的值按瞬时换算，不按墙钟重贴时区。"""
    df = pl.DataFrame({"datetime": [datetime(2024, 1, 2, 1, 35, tzinfo=UTC)]})

    value = to_arrow(df).column("datetime")[0].as_py()

    assert (value.hour, value.minute) == (9, 35)
    assert value.utcoffset() == timedelta(hours=8)


def test_arrow_roundtrip_keeps_beijing_wall_clock() -> None:
    df = pl.DataFrame({"datetime": [datetime(2024, 1, 2, 9, 35), datetime(2024, 1, 2, 15, 0)]})

    restored = from_arrow(to_arrow(df))

    assert restored.equals(df)
    assert restored.schema["datetime"] == pl.Datetime("us")


def test_from_arrow_converts_utc_column_to_beijing_wall_clock() -> None:
    """CK 的 DateTime64(x, 'Asia/Shanghai') 读回来必须还原成 naive 墙钟。"""
    table = pa.table(
        {
            "datetime": pa.array(
                [datetime(2024, 1, 2, 1, 35, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    df = from_arrow(table)

    assert df.schema["datetime"] == pl.Datetime("us")
    assert df["datetime"][0] == datetime(2024, 1, 2, 9, 35)


def test_from_arrow_leaves_date_and_naive_columns_untouched() -> None:
    table = pa.table(
        {
            "date": pa.array([datetime(2024, 1, 2).date()], type=pa.date32()),
            "quote_ts": pa.array([1704164100000], type=pa.int64()),
        }
    )

    df = from_arrow(table)

    assert df.schema["date"] == pl.Date
    assert df.schema["quote_ts"] == pl.Int64


def test_table_applies_prefix_for_test_isolation() -> None:
    client = ClickHouseClient(
        url="http://localhost:8123",
        database="tsp_test",
        user="tsp",
        table_prefix="t_ab12cd34_",
    )

    assert client.table("kline_daily") == "t_ab12cd34_kline_daily"
    assert (
        ClickHouseClient(
            url="http://localhost:8123",
            database="tsp",
            user="tsp",
        ).table("kline_daily")
        == "kline_daily"
    )


def test_insert_skips_empty_frame_without_connecting() -> None:
    """空写入不应发起请求（也不需要可达的 CK）。"""
    client = ClickHouseClient(url="http://127.0.0.1:1", database="tsp", user="tsp")

    client.insert("kline_daily", pl.DataFrame())

    assert client._client is None  # 没有触发连接
