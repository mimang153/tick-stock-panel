"""B2 第一步：日K/除权 过渡期双写 CK（设计文档 §8.4）。

parquet 仍是页面读取源，CK 侧只做镜像写入：
- 6 张日K表（stock/index/etf × daily/enriched）按 (symbol, date) 由 ReplacingMergeTree 收敛
- 未注入 CK（进程级 runtime 也没初始化）时降级为只写 parquet，不打断既有链路
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository

DAY = date(2026, 9, 10)


@pytest.fixture()
def repo(tmp_path, store):
    return KlineRepository(DataStore(tmp_path), storage=store)


def _daily(symbol: str, close: float, day: date = DAY) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "date": [day],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [100.0],
            "amount": [close * 100],
        }
    )


def test_append_daily_writes_both_parquet_and_ck(repo, store):
    repo.append_daily(_daily("600000.SH", 10.0))

    # parquet 仍是唯一读取源（页面不受影响）
    part = repo.store.data_dir / "kline_daily" / f"date={DAY}" / "part.parquet"
    assert part.exists()

    # CK 镜像写入生效
    got = store.read_daily("kline_daily", symbols=["600000.SH"])
    assert got.height == 1
    assert got["close"][0] == 10.0
    assert got["date"][0] == DAY


def test_repeat_append_converges_to_latest_in_ck(repo, store):
    repo.append_daily(_daily("600000.SH", 10.0))
    repo.append_daily(_daily("600000.SH", 11.0))

    got = store.read_daily("kline_daily", symbols=["600000.SH"])
    assert got.height == 1, "ReplacingMergeTree 按 (symbol, date) 收敛"
    assert got["close"][0] == 11.0


def test_append_enriched_mirrors_storage_columns_only(repo, store):
    df = _daily("600000.SH", 10.0).with_columns(
        pl.lit(9.5).alias("raw_close"),
        pl.lit(0.01).alias("turnover_rate"),
        pl.lit("浦发银行").alias("name"),  # 非存储列, 不应进入 CK
    )

    repo.append_enriched(df)

    got = store.read_daily("kline_daily_enriched", symbols=["600000.SH"])
    assert got.height == 1
    assert "name" not in got.columns
    assert got["raw_close"][0] == 9.5
    part = repo.store.data_dir / "kline_daily_enriched" / f"date={DAY}" / "part.parquet"
    assert "name" not in pl.read_parquet(part).columns


def test_flush_live_daily_asset_mirrors(repo, store):
    repo.flush_live_daily_asset("etf", _daily("510300.SH", 4.0))

    got = store.read_daily("kline_etf_daily", symbols=["510300.SH"])
    assert got.height == 1
    assert got["close"][0] == 4.0


def test_index_daily_mirrors_to_own_table(repo, store):
    repo.append_index_daily(_daily("000001.SH", 3200.0))

    assert store.read_daily("kline_index_daily").height == 1
    # 不污染股票表
    assert store.read_daily("kline_daily").is_empty()


def test_replace_daily_rows_mirrors_delete_and_write(repo, store, tmp_path):
    # 待清理标的必须是"明确非交易"的空记录（零成交/零额），否则修补会被业务校验拒绝
    empty_row = _daily("000001.SZ", 0.0).with_columns(
        pl.lit(0.0).alias("volume"),
        pl.lit(0.0).alias("amount"),
    )
    repo.append_daily(pl.concat([_daily("600000.SH", 10.0), empty_row]))

    incoming = _daily("600000.SH", 12.0)
    repo.replace_daily_rows(
        "stock",
        DAY,
        incoming,
        symbols=["600000.SH", "000001.SZ"],
        backup_dir=tmp_path / "backup",
        remove_symbols=["000001.SZ"],
    )

    got = store.read_daily("kline_daily", symbols=["600000.SH", "000001.SZ"])
    assert got.height == 1, "被清理的 symbol 在 CK 中同步删除"
    assert got["symbol"][0] == "600000.SH"
    assert got["close"][0] == 12.0


def test_mirror_degrades_without_storage(tmp_path, monkeypatch):
    """未注入 CK 且进程级 runtime 未初始化时，parquet 链路照常（保护既有 68 处用例）。"""
    from app.storage import runtime

    monkeypatch.setattr(runtime, "get_storage", lambda: None)
    repo = KlineRepository(DataStore(tmp_path), storage=None)

    repo.append_daily(_daily("600000.SH", 10.0))

    part = repo.store.data_dir / "kline_daily" / f"date={DAY}" / "part.parquet"
    assert part.exists()
