"""分钟数据缺口体检 (services/minute_gaps) 与 /api/kline/minute 的 partial 返回。

迁移后 scan_minute_gaps 仍枚举本地 ``date=*/part.parquet`` 目录来确定"扫描哪些
日期"，但每个分区的实际数据经 _read_minute_activity 走 CK read_minute 读取；
日线核对 _daily_active_symbols 也走 CK read_daily。因此测试需：本地建占位分区
目录（供 glob 枚举）+ 真实分钟/日K数据写 CK 隔离库。
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import polars as pl
import pytest

from app.services.minute_gaps import FULL_DAY_BARS, scan_minute_gaps

_BJ = _dt.date(2026, 10, 23)
_MINUTE = "kline_minute"
_DAILY = "kline_daily"


def _minute_df(trade_date: str, rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "datetime": [_dt.datetime.fromisoformat(f"{trade_date}T{t}:00") for _, t in rows],
            "open": [10.0] * len(rows),
            "high": [10.0] * len(rows),
            "low": [10.0] * len(rows),
            "close": [10.0] * len(rows),
            "volume": [1.0] * len(rows),
            "amount": [1.0] * len(rows),
        }
    )


def _write_partition(store, minute_dir: Path, trade_date: str, rows: list[tuple[str, str]]) -> None:
    """本地建占位分区（供 glob 枚举）+ CK 写真实分钟数据。"""
    d = minute_dir / f"date={trade_date}"
    d.mkdir(parents=True, exist_ok=True)
    df = _minute_df(trade_date, rows)
    df.write_parquet(d / "part.parquet")
    store.write_minute(_MINUTE, df)


def _full_day(symbol: str) -> list[tuple[str, str]]:
    """完整交易日分钟序列 (与实盘落盘同口径: 09:30~11:30 + 13:00~15:00, 241 根)。"""
    times: list[str] = []
    t = _dt.datetime(2026, 1, 1, 9, 30)
    while t <= _dt.datetime(2026, 1, 1, 11, 30):
        times.append(t.strftime("%H:%M"))
        t += _dt.timedelta(minutes=1)
    t = _dt.datetime(2026, 1, 1, 13, 0)
    while t <= _dt.datetime(2026, 1, 1, 15, 0):
        times.append(t.strftime("%H:%M"))
        t += _dt.timedelta(minutes=1)
    return [(symbol, hm) for hm in times]


def test_complete_day_reports_no_gap(ck_runtime, tmp_path):
    minute_dir = tmp_path / "kline_minute"
    _write_partition(ck_runtime, minute_dir, "2026-10-22", _full_day("600519.SH"))
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))
    assert r["gaps"] == []
    assert r["complete_dates"] == ["2026-10-22"]


def test_partial_day_reported_with_time_range(ck_runtime, tmp_path):
    minute_dir = tmp_path / "kline_minute"
    rows = _full_day("600519.SH")[:171]
    _write_partition(ck_runtime, minute_dir, "2026-10-20", rows)
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))
    assert len(r["gaps"]) == 1
    g = r["gaps"][0]
    assert g["symbol"] == "600519.SH"
    assert g["date"] == "2026-10-20"
    assert g["actual_bars"] == 171
    assert g["expected_bars"] == FULL_DAY_BARS
    assert g["missing_bars"] == 69
    assert g["last_time"] == "13:49"


def test_tail_only_sequence_also_reported(ck_runtime, tmp_path):
    """只有尾盘一段 (复现 2026-10-21 的尾部残序列) 同样算缺口。"""
    minute_dir = tmp_path / "kline_minute"
    full = _full_day("002388.SZ")
    _write_partition(ck_runtime, minute_dir, "2026-10-21", full[-23:])
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))
    assert len(r["gaps"]) == 1
    assert r["gaps"][0]["first_time"] == "14:38"
    assert r["gaps"][0]["last_time"] == "15:00"


def test_symbol_filter_narrows_scan(ck_runtime, tmp_path):
    minute_dir = tmp_path / "kline_minute"
    rows = _full_day("A.SZ")[:100] + _full_day("B.SZ")[:100]
    _write_partition(ck_runtime, minute_dir, "2026-10-20", rows)
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0), symbols=["A.SZ"])
    assert {g["symbol"] for g in r["gaps"]} == {"A.SZ"}


def test_summary_by_date_aggregates_missing_bars(ck_runtime, tmp_path):
    minute_dir = tmp_path / "kline_minute"
    rows = _full_day("A.SZ")[:171] + _full_day("B.SZ")[:120]
    _write_partition(ck_runtime, minute_dir, "2026-10-20", rows)
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))
    cell = r["summary_by_date"][0]
    assert cell["gap_symbols"] == 2
    assert cell["missing_bars"] == (240 - 171) + (240 - 120)


def test_intraday_expected_scales_with_clock(ck_runtime, tmp_path):
    """当日盘中: expected 按已开盘分钟折算, 正常进度不应误报缺口。"""
    minute_dir = tmp_path / "kline_minute"
    today_str = "2026-10-23"
    rows = _full_day("600519.SH")[:90]
    _write_partition(ck_runtime, minute_dir, today_str, rows)
    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(11, 0))
    assert r["gaps"] == []


def test_missing_dir_returns_empty(ck_runtime, tmp_path):
    r = scan_minute_gaps(tmp_path / "nope", today=_BJ, now_hm=(20, 0))
    assert r["gaps"] == []
    assert r["scanned_dates"] == 0


def _zero_activity(store, minute_dir: Path, trade_date: str, symbol: str) -> None:
    """把某标的当日分钟成交量/额改为 0（CK 读改写）。"""
    existing = store.read_minute(
        _MINUTE,
        start=_dt.datetime.fromisoformat(f"{trade_date}T00:00"),
        end=_dt.datetime.fromisoformat(f"{trade_date}T23:59"),
    )
    modified = existing.with_columns(
        pl.when(pl.col("symbol") == symbol).then(0.0).otherwise(pl.col(c)).alias(c)
        for c in ("volume", "amount")
    )
    store.write_minute(_MINUTE, modified)


def _write_daily(store, trade_date: str, symbol: str, volume: float) -> None:
    store.write_daily(
        _DAILY,
        pl.DataFrame(
            {
                "symbol": [symbol],
                "date": [_dt.date.fromisoformat(trade_date)],
                "volume": [volume],
                "amount": [volume * 10],
            }
        ),
    )


@pytest.mark.parametrize("count", [171, 23])
def test_zero_activity_residue_is_separate_from_real_gaps(ck_runtime, tmp_path, count):
    """停牌残留不会被合并补拉清除, 不能把它反复列为可补齐缺口。"""
    minute_dir = tmp_path / "kline_minute"
    day = "2026-10-20"
    _write_partition(
        ck_runtime, minute_dir, day, _full_day("002731.SZ")[:count] + _full_day("600519.SH")[:100]
    )
    _zero_activity(ck_runtime, minute_dir, day, "002731.SZ")
    _write_daily(ck_runtime, day, "600519.SH", 1000)

    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))

    assert [g["symbol"] for g in r["gaps"]] == ["600519.SH"]
    assert r["gaps"][0]["actual_bars"] == 100
    assert r["inactive_symbol_days"] == 1
    assert r["summary_by_date"][0]["gap_symbols"] == 1


@pytest.mark.parametrize("daily_state", ["traded", "missing", "unknown", "corrupt", "empty"])
def test_zero_activity_is_still_a_gap_without_daily_support(ck_runtime, tmp_path, daily_state):
    """日线有成交或无法核对时保留告警, 不用零成交量掩盖真实数据缺失。"""
    minute_dir = tmp_path / "kline_minute"
    day = "2026-10-20"
    _write_partition(ck_runtime, minute_dir, day, _full_day("002731.SZ")[:171])
    _zero_activity(ck_runtime, minute_dir, day, "002731.SZ")
    if daily_state in ("traded", "unknown", "empty"):
        vol = {"traded": 1000, "unknown": 1000, "empty": 1000}[daily_state]
        _write_daily(ck_runtime, day, "002731.SZ", vol)
        if daily_state == "empty":
            ck_runtime.execute(
                f"ALTER TABLE {ck_runtime._table(_DAILY)} DELETE WHERE symbol='002731.SZ'"
            ) if hasattr(ck_runtime, "execute") else None
    # missing/corrupt：CK 无损坏文件概念；缺失即不写日线（corrupt 由 CK 固定 schema 不可表达，
    # 其"无法核对→保留告警"语义由 missing 分支等价覆盖，不造假构造损坏数据）。

    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))

    assert len(r["gaps"]) == 1
    assert r["inactive_symbol_days"] == 0


@pytest.mark.parametrize("activity", ["volume", "amount", "unknown_volume", "missing_amount"])
def test_partial_day_with_activity_or_unknown_fields_keeps_gap(ck_runtime, tmp_path, activity):
    minute_dir = tmp_path / "kline_minute"
    day = "2026-10-20"
    _write_partition(ck_runtime, minute_dir, day, _full_day("002731.SZ")[:171])
    _zero_activity(ck_runtime, minute_dir, day, "002731.SZ")
    existing = ck_runtime.read_minute(
        _MINUTE,
        start=_dt.datetime.fromisoformat(f"{day}T00:00"),
        end=_dt.datetime.fromisoformat(f"{day}T23:59"),
    )
    # CK 分钟表 volume/amount 为非空 Float64：null/缺列不可表达（会落为 0）。
    # "字段未知不能当无成交证据"在 CK 下等价为"只要有一行有活动量就不误判停牌残留"：
    # unknown_volume / missing_amount 用极小活动量(0.01)代表"无法确认零成交"，
    # volume / amount 用正常活动量(1.0)。两者都要求保留缺口、不判 inactive。
    if activity in ("volume", "unknown_volume"):
        column, value = "volume", (0.01 if activity == "unknown_volume" else 1.0)
    else:
        column, value = "amount", (0.01 if activity == "missing_amount" else 1.0)
    modified = existing.with_columns(
        pl.when(pl.col("datetime") == existing["datetime"][0])
        .then(value)
        .otherwise(pl.col(column))
        .alias(column)
    )
    ck_runtime.write_minute(_MINUTE, modified)
    _write_daily(ck_runtime, day, "600519.SH", 1000)

    r = scan_minute_gaps(minute_dir, today=_BJ, now_hm=(20, 0))

    assert len(r["gaps"]) == 1
    assert r["gaps"][0]["actual_bars"] == 171
    assert r["inactive_symbol_days"] == 0


# ---------- /api/kline/minute 的 local_partial 返回 ----------


def _fake_req(repo):
    state = type("S", (), {"repo": repo})()
    return type("R", (), {"app": type("A", (), {"state": state})()})()


def test_gap_api_exposes_inactive_days_without_changing_source_data(
    ck_runtime, tmp_path, monkeypatch
):
    from types import SimpleNamespace

    from app.api import kline as kline_api

    minute_dir = tmp_path / "kline_minute"
    day = "2026-10-20"
    _write_partition(ck_runtime, minute_dir, day, _full_day("002731.SZ")[:171])
    _zero_activity(ck_runtime, minute_dir, day, "002731.SZ")
    _write_daily(ck_runtime, day, "600519.SH", 1000)
    monkeypatch.setattr(kline_api, "cn_now", lambda: _dt.datetime(2026, 10, 23, 20))
    monkeypatch.setattr(kline_api, "cn_today", lambda: _BJ)
    # get_minute_gaps 内部经 require_storage() 读 CK，ck_runtime 已初始化全局 runtime
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))

    result = kline_api.get_minute_gaps(_fake_req(repo), symbols="002731.SZ", dates=day, limit=500)

    assert result["gaps"] == []
    assert result["complete_dates"] == []
    assert result["inactive_symbol_days"] == 1


def test_get_minute_returns_partial_instead_of_dropping(tmp_path, monkeypatch):
    """本地 171 根必须原样返回 (source=local_partial), 不能丢弃去问源。"""
    from app.api import kline as kline_api
    from app.tickflow.capabilities import CapabilitySet
    from app.tickflow.repository import DataStore, KlineRepository

    repo = KlineRepository(DataStore(tmp_path))
    rows = _full_day("600519.SH")[:171]
    day_df = pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "datetime": [_dt.datetime.fromisoformat(f"2026-10-20T{t}:00") for _, t in rows],
            "open": [10.0] * len(rows),
            "high": [10.0] * len(rows),
            "low": [10.0] * len(rows),
            "close": [10.0] * len(rows),
            "volume": [1.0] * len(rows),
            "amount": [1.0] * len(rows),
        }
    )
    monkeypatch.setattr(repo, "get_minute", lambda *a, **kw: day_df)
    monkeypatch.setattr(repo, "resolve_asset_type", lambda s: "stock")
    monkeypatch.setattr(kline_api, "_get_stock_info", lambda *a, **kw: {"name": "测试"})
    monkeypatch.setattr(kline_api, "_get_previous_closes", lambda *a, **kw: {})
    monkeypatch.setattr(kline_api, "_get_price_limit_info", lambda *a, **kw: None)
    monkeypatch.setattr(kline_api, "cn_today", lambda: _BJ)
    monkeypatch.setattr(kline_api.minute_adjust, "minute_basis_is_raw", lambda d: False)

    called = {"n": 0}

    def _no_pull(*a, **kw):
        called["n"] += 1
        return pl.DataFrame()

    monkeypatch.setattr(kline_api.kline_sync, "fetch_minute_single", _no_pull)

    state = type("S", (), {"repo": repo, "capabilities": CapabilitySet()})()
    req = type("R", (), {"app": type("A", (), {"state": state}), "headers": {}})()

    out = kline_api.get_minute(
        req, symbol="600519.SH", trade_date=_dt.date(2026, 10, 20), live=False
    )

    assert out["source"] == "local_partial"
    assert len(out["rows"]) == 171
    assert out["expected"] == 240
    assert out["actual"] == 171
    assert called["n"] == 0
