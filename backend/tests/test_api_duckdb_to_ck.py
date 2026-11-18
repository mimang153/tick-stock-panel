"""B5.1 前置：数据状态 API 的 DuckDB 读点切 CK 后，统计口径保持一致。

覆盖新增的 store 统计方法与 api/data 聚合函数的 CK 读路径；
无 runtime 时按空处理（与原 DuckDB 视图缺失同语义），不抛异常。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from app.api import data as data_api


def _repo(store, data_dir=None):
    return SimpleNamespace(
        storage=store,
        store=SimpleNamespace(data_dir=Path(data_dir) if data_dir else Path("/nonexistent")),
    )


def test_table_stats_empty_and_with_rows(ck_runtime):
    """空表返空 dict；有行时返回行数/日期范围/标的数/交易日数。"""
    assert ck_runtime.table_stats("kline_etf_daily") == {}

    df = pl.DataFrame(
        {
            "symbol": ["600000.SH", "600000.SH", "000001.SZ"],
            "date": [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 6)],
            "open": [10.0] * 3,
            "high": [11.0] * 3,
            "low": [9.0] * 3,
            "close": [10.5] * 3,
            "volume": [100.0] * 3,
            "amount": [1000.0] * 3,
        }
    )
    ck_runtime.write_daily("kline_daily", df)
    stats = ck_runtime.table_stats("kline_daily")
    assert stats["rows"] == 3
    assert stats["earliest_date"] == "2026-01-05"
    assert stats["latest_date"] == "2026-01-06"
    assert stats["symbols_covered"] == 2
    assert stats["trading_days"] == 2


def test_table_stats_trade_date_axis(ck_runtime):
    """adj_factor 用 trade_date 作为日期轴。"""
    ck_runtime.write_adj_factor(
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [date(2026, 3, 2)],
                "ex_factor": [1.0],
            }
        )
    )
    stats = ck_runtime.table_stats("adj_factor", date_column="trade_date")
    assert stats["rows"] == 1
    assert stats["earliest_date"] == "2026-03-02"
    assert stats["trading_days"] == 1


def test_instrument_stats(ck_runtime):
    ck_runtime.write_instruments(
        "stock",
        pl.DataFrame(
            {
                "symbol": ["600000.SH", "000001.SZ"],
                "name": ["浦发银行", ""],
            }
        ),
    )
    stats = ck_runtime.instrument_stats("stock")
    assert stats["rows"] == 2
    assert stats["symbols_covered"] == 2
    assert stats["named"] == 1  # 空名不计入


def test_table_columns_typed(ck_runtime):
    cols = ck_runtime.table_columns_typed("kline_daily")
    names = {c[0] for c in cols}
    assert "symbol" in names and "date" in names
    assert all(isinstance(c[1], str) and c[1] for c in cols)


def test_safe_aggregate_uses_ck(ck_runtime, tmp_path):
    """api/data 的聚合函数从 CK 取数，不再查 DuckDB 视图。"""
    repo = _repo(ck_runtime, tmp_path)
    ck_runtime.write_daily(
        "kline_daily",
        pl.DataFrame(
            {
                "symbol": ["600001.SH", "600002.SH"],
                "date": [date(2026, 5, 6), date(2026, 5, 6)],
                "open": [10.0] * 2,
                "high": [11.0] * 2,
                "low": [9.0] * 2,
                "close": [10.5] * 2,
                "volume": [100.0] * 2,
                "amount": [1000.0] * 2,
            }
        ),
    )
    stats = data_api._safe_aggregate(repo, "kline_daily")
    assert stats is not None
    assert stats["rows"] == 2
    assert stats is not None
    assert stats["symbols_covered"] == 2
    assert stats is not None
    assert stats["trading_days"] == 1


def test_safe_aggregate_instruments_uses_ck(ck_runtime, tmp_path):
    repo = _repo(ck_runtime, tmp_path)
    ck_runtime.write_instruments(
        "stock",
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "name": ["浦发银行"],
            }
        ),
    )
    stats = data_api._safe_aggregate_instruments(repo)
    assert stats is not None
    assert stats["rows"] == 1
    assert stats is not None
    assert stats["named"] == 1


def test_table_schema_uses_ck_columns(ck_runtime):
    """/schema 端点从真实 CK DESCRIBE 取精确类型（不再用 DuckDB DESCRIBE 视图）。"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(data_api.router)
    app.state.repo = _repo(ck_runtime)
    with TestClient(app) as client:
        resp = client.get("/api/data/schema/daily")
    assert resp.status_code == 200
    fields = {f["name"]: f["type"] for f in resp.json()}
    assert "symbol" in fields and "date" in fields
    assert fields["date"]  # CK 返回精确类型（非空字符串）
    assert fields["symbol"]
