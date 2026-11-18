"""B3 数据集存储契约（L2，真实 CK）：分钟/财务/盘口/扩展/矩阵缓存/回测/同步状态。"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore
from app.storage.sync_state import (
    INTERRUPTED_ERROR,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_RUNNING,
    SyncStateStore,
)

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


def _minute_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "datetime": [datetime(2024, 1, 2, 9, 35), datetime(2024, 1, 2, 15, 0)],
            "open": [10.0, 10.2],
            "high": [10.1, 10.3],
            "low": [9.9, 10.1],
            "close": [10.2, 10.3],
            "volume": [100.0, 200.0],
            "amount": [1000.0, 2000.0],
            "updated_at": [datetime(2024, 1, 2, 16)] * 2,
        },
        schema=_MINUTE_SCHEMA,
    )


# ── 分钟K（T3.1）──────────────────────────────────────────────
def test_minute_roundtrip_and_latest(store: ClickHouseStore) -> None:
    store.write_minute("kline_minute", _minute_rows())

    df = store.read_minute("kline_minute", symbols=["600519.SH"])

    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df["datetime"].to_list() == [datetime(2024, 1, 2, 9, 35), datetime(2024, 1, 2, 15, 0)]
    assert df.schema["datetime"] == pl.Datetime("us")  # 读回是北京墙钟 naive
    assert store.latest_minute_datetime("kline_minute") == datetime(2024, 1, 2, 15, 0)


def test_minute_datetime_range_is_closed(store: ClickHouseStore) -> None:
    store.write_minute("kline_minute", _minute_rows())

    df = store.read_minute(
        "kline_minute",
        start=datetime(2024, 1, 2, 9, 35),
        end=datetime(2024, 1, 2, 9, 35),
    )

    assert df.height == 1
    assert store.latest_minute_datetime("kline_etf_minute") is None


# ── 财务（T3.2）────────────────────────────────────────────────
def test_financials_expand_and_union_fields(store: ClickHouseStore) -> None:
    store.write_financials(
        "financials_metrics",
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "period_end": [date(2024, 3, 31), date(2024, 3, 31)],
                "roe": [0.31, 0.12],
                "eps": [12.5, 0.5],
                "announce_date": ["2024-04-25", "2024-04-20"],
            },
        ),
    )
    store.write_financials(
        "financials_metrics",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "period_end": [date(2024, 6, 30)],
                "roe": [0.35],
                "debt_ratio": [0.2],  # 新字段：加字段零 DDL
                "announce_date": ["2024-08-02"],
            },
        ),
    )

    df = store.read_financials("financials_metrics")

    assert df.columns[0] == "symbol" and df.columns[1] == "period_end"
    assert set(df.columns) == {"symbol", "period_end", "roe", "eps", "debt_ratio", "announce_date"}
    first = df.filter(pl.col("period_end") == date(2024, 3, 31)).sort("symbol")
    assert first["roe"].to_list() == [0.12, 0.31]
    # 未提供的字段是 NULL，不是 0
    assert first["debt_ratio"].to_list() == [None, None]
    assert df.filter(pl.col("symbol") == "600519.SH")["debt_ratio"].to_list() == [None, 0.2]


def test_financials_missing_period_key_rejected(store: ClickHouseStore) -> None:
    with pytest.raises(ValueError, match="缺少键列"):
        store.write_financials("financials_metrics", pl.DataFrame({"symbol": ["600519.SH"]}))


def test_financials_rejects_non_financial_table(store: ClickHouseStore) -> None:
    with pytest.raises(ValueError):
        store.read_financials("kline_daily")


# ── 五档盘口（T3.3）────────────────────────────────────────────
def test_depth5_keeps_three_state_sealed(store: ClickHouseStore) -> None:
    store.write_depth5(
        pl.DataFrame(
            {
                "symbol": ["A", "B", "C"],
                "date": [date(2024, 1, 2)] * 3,
                "sealed_up": [True, False, None],
                "sealed_down": [None, True, None],
                "ask1_vol": [0, 100, None],
                "bid1_vol": [500, None, None],
                "status": ["limit_up", "limit_down", "limit_up"],
                "fetched_at": [1.0, 2.0, 3.0],
                "updated_at": [datetime(2024, 1, 2, 16)] * 3,
            },
        ),
    )

    df = store.read_depth5(symbols=["A", "B", "C"]).sort("symbol")

    assert df["sealed_up"].to_list() == [True, False, None]
    assert df["sealed_down"].to_list() == [None, True, None]
    assert df["bid1_vol"].to_list() == [500, None, None]


# ── 扩展数据（T3.4）────────────────────────────────────────────
def test_ext_timeseries_is_isolated_by_ext_id_and_expands(store: ClickHouseStore) -> None:
    store.write_ext_timeseries(
        pl.DataFrame(
            {
                "ext_id": ["north_flow", "north_flow", "sector"],
                "symbol": ["600519.SH", "600519.SH", "600519.SH"],
                "date": [date(2024, 1, 2)] * 3,
                "time_value": ["", "10:00", ""],
                "net_inflow": [1.5, 2.5, 9.9],
                "board": ["白酒", "白酒", "白酒"],
            },
        ),
    )

    df = store.read_ext_timeseries("north_flow", symbols=["600519.SH"])

    assert df["net_inflow"].to_list() == [1.5, 2.5]  # 日内多盘按 time_value 升序
    assert df["board"].to_list() == ["白酒", "白酒"]
    assert store.read_ext_timeseries("sector")["net_inflow"].to_list() == [9.9]
    assert store.read_ext_timeseries("other").is_empty()


def test_ext_instruments_roundtrip(store: ClickHouseStore) -> None:
    store.write_ext_instruments(
        pl.DataFrame(
            {
                "ext_id": ["sector"],
                "symbol": ["600519.SH"],
                "code": ["BK0477"],
                "weight": [3.21],
                "board": ["白酒"],
            },
        ),
    )

    df = store.read_ext_instruments("sector")

    assert df["code"].to_list() == ["BK0477"]
    assert df["weight"].to_list() == [3.21]
    assert df["board"].to_list() == ["白酒"]


# ── 矩阵缓存（T3.5）────────────────────────────────────────────
def test_matrix_cache_roundtrip_is_byte_exact(store: ClickHouseStore) -> None:
    payload = b"\x00\x01\x02ARROW-IPC\xff"

    store.put_matrix_cache("abc123", payload)

    assert store.get_matrix_cache("abc123") == payload
    assert store.get_matrix_cache("missing") is None
    assert store.matrix_cache_bytes() == len(payload)


def test_matrix_cache_touch_keeps_payload_and_moves_accessed_at(store: ClickHouseStore) -> None:
    store.put_matrix_cache("abc123", b"payload")
    before = store.client.query(
        f"SELECT created_at, accessed_at FROM {store.client.table('matrix_cache')} FINAL",
    ).row(0, named=True)

    store.touch_matrix_cache("abc123")

    after = store.client.query(
        f"SELECT created_at, accessed_at FROM {store.client.table('matrix_cache')} FINAL",
    ).row(0, named=True)
    assert store.get_matrix_cache("abc123") == b"payload"
    assert after["created_at"] == before["created_at"]
    assert after["accessed_at"] >= before["accessed_at"]


def test_matrix_cache_delete(store: ClickHouseStore) -> None:
    store.put_matrix_cache("k1", b"a")
    store.put_matrix_cache("k2", b"bb")

    store.delete_matrix_cache(["k1", "not-there"])

    assert store.get_matrix_cache("k1") is None
    assert store.get_matrix_cache("k2") == b"bb"


# ── 回测结果（T3.6）────────────────────────────────────────────
def test_backtest_run_and_trades_roundtrip(store: ClickHouseStore) -> None:
    store.write_backtest_run(
        {
            "run_id": "run-1",
            "created_at": datetime(2024, 1, 2, 16),
            "strategy_id": "high_turnover",
            "config_json": '{"start":"2024-01-02"}',
            "stats_json": '{"total_return":0.12}',
            "equity_curve": "[]",
            "extra_json": "{}",
            "n_trades": 2,
            "elapsed_ms": 123.4,
            "error": None,
        },
    )
    store.write_backtest_trades(
        pl.DataFrame(
            {
                "run_id": ["run-1", "run-1"],
                "seq": [2, 1],
                "symbol": ["000001.SZ", "600519.SH"],
                "entry_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "exit_date": [date(2024, 1, 5), date(2024, 1, 4)],
                "pnl_pct": [0.05, -0.01],
                "updated_at": [datetime(2024, 1, 6)] * 2,
            },
        ),
    )

    run = store.read_backtest_run("run-1")
    trades = store.read_backtest_trades("run-1")

    assert run is not None
    assert run["strategy_id"] == "high_turnover"
    assert run["n_trades"] == 2
    assert run["error"] is None
    assert trades["seq"].to_list() == [1, 2]
    assert store.read_backtest_run("missing") is None
    assert store.list_backtest_runs()["run_id"].to_list() == ["run-1"]


def test_backtest_run_pagination_and_order(store: ClickHouseStore) -> None:
    for index in range(3):
        store.write_backtest_run(
            {
                "run_id": f"run-{index}",
                "created_at": datetime(2024, 1, 2 + index, 16),
                "strategy_id": "s",
                "n_trades": index,
                "elapsed_ms": 1.0,
            },
        )

    page = store.list_backtest_runs(limit=2, offset=0)

    assert page["run_id"].to_list() == ["run-2", "run-1"]  # 最新优先
    assert store.list_backtest_runs(limit=2, offset=2)["run_id"].to_list() == ["run-0"]
    with pytest.raises(ValueError):
        store.list_backtest_runs(limit=0)


# ── 同步状态（T2.5）────────────────────────────────────────────
def test_sync_state_transitions_keep_last_ok(ch: ClickHouseClient) -> None:
    state = SyncStateStore(ch)

    state.mark_running("kline_sync")
    assert state.get("kline_sync").status == STATUS_RUNNING

    state.mark_ok("kline_sync")
    ok = state.get("kline_sync")
    assert ok.status == STATUS_OK
    assert ok.last_ok_at is not None
    assert ok.last_error is None

    state.mark_failed("kline_sync", "源超时")
    failed = state.get("kline_sync")
    assert failed.status == STATUS_FAILED
    assert failed.last_error == "源超时"
    assert failed.last_ok_at == ok.last_ok_at  # 失败不清空上次成功时间


def test_sync_state_recovers_interrupted(store: ClickHouseStore, ch: ClickHouseClient) -> None:
    state = SyncStateStore(ch)
    state.mark_ok("financial_sync")
    state.mark_running("financial_sync")
    state.mark_running("minute_sync")

    interrupted = state.recover_interrupted()

    assert sorted(interrupted) == ["financial_sync", "minute_sync"]
    row = state.all().filter(pl.col("task") == "financial_sync").row(0, named=True)
    assert row["status"] == STATUS_FAILED
    assert row["last_error"] == INTERRUPTED_ERROR
    assert row["last_ok_at"] is not None
    assert state.get("unknown_task") is None
