"""S3 复权链路验收：真实 CK + TestClient 的三响应逐行对账。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import app.indicators.pipeline as pipeline

SYMBOL = "000001.SZ"
DATES = [date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)]
EVENT_DAY = date(2024, 6, 2)


def _raw_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [SYMBOL] * len(DATES),
            "date": DATES,
            "open": [10.0] * len(DATES),
            "high": [11.0] * len(DATES),
            "low": [9.0] * len(DATES),
            "close": [10.0] * len(DATES),
            "volume": [100.0] * len(DATES),
            "amount": [1000.0] * len(DATES),
        }
    )


def _factor_frame() -> pl.DataFrame:
    return pl.DataFrame({"symbol": [SYMBOL], "trade_date": [EVENT_DAY], "ex_factor": [1.25]})


def _factor_for(day: date, ex_factor: float) -> float:
    """D 之后的事件乘积；事件当天按 join_asof backward 计入前复权。"""
    return ex_factor if day < EVENT_DAY else 1.0


def test_s3_adjustment_chain(scenario_client) -> None:
    client, repo, runtime = scenario_client
    raw = _raw_frame()
    factors = _factor_frame()
    forward = pipeline._apply_adj_factor(raw, factors)

    runtime.write_daily("kline_daily", raw)
    runtime.write_daily("kline_daily_enriched", forward)
    runtime.write_adj_factor(factors)
    repo.refresh_cache()

    none_resp = client.get(
        "/api/kline/daily",
        params={
            "symbol": SYMBOL,
            "start_date": "2024-06-01",
            "end_date": "2024-06-03",
            "adjust": "none",
        },
    )
    forward_resp = client.get(
        "/api/kline/daily",
        params={
            "symbol": SYMBOL,
            "start_date": "2024-06-01",
            "end_date": "2024-06-03",
            "adjust": "forward",
        },
    )
    factor_resp = client.get(
        "/api/adj-factor",
        params={"symbol": SYMBOL, "start_date": "2024-06-01", "end_date": "2024-06-03"},
    )
    assert none_resp.status_code == 200
    assert forward_resp.status_code == 200
    assert factor_resp.status_code == 200

    none_rows = none_resp.json()["rows"]
    forward_rows = forward_resp.json()["rows"]
    assert (
        [row["date"] for row in none_rows]
        == [row["date"] for row in forward_rows]
        == [
            "2024-06-01",
            "2024-06-02",
            "2024-06-03",
        ]
    )
    assert [row["close"] for row in none_rows] == [10.0, 10.0, 10.0]

    factor_rows = factor_resp.json()["rows"]
    assert len(factor_rows) == 1
    ex_factor = factor_rows[0]["ex_factor"]
    for raw_row, adjusted in zip(none_rows, forward_rows, strict=False):
        day = date.fromisoformat(raw_row["date"])
        expected = raw_row["close"] / _factor_for(day, ex_factor)
        assert adjusted["close"] == pytest.approx(expected)
