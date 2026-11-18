"""#223 回归: 盘后管道必须识别并重算覆盖不全的 enriched 分区。

自选实时路径 (merge_live_enriched_asset) 会在全市场 enriched 生成前提前
创建当日分区 (只有几只自选); 仅按日期计数比较会把它误判为完整分区而跳过
计算, 造成日K连续缺失与均线错误。_prune_partial_enriched_partitions
按过滤停牌后的标的集合检查覆盖, 避免正确过滤的停牌记录导致反复删除重算。

迁移后 _prune_partial_enriched_partitions 走 ClickHouse store API
(list_daily_dates / read_daily / delete_daily_date), 本测试用真实 CK 隔离库
造数, 断言改为查询 CK 而非枚举 parquet 目录。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from app.indicators import pipeline
from app.jobs import daily_pipeline
from app.jobs.daily_pipeline import _prune_partial_enriched_partitions
from app.services import data_integrity, preferences
from app.tickflow.capabilities import CapabilitySet

DAILY = "kline_daily"
ENRICHED = "kline_daily_enriched"


def _write_partition(
    store, table: str, day: str, symbols: list[str], *, halted: set[str] | None = None
) -> None:
    """写 CK 日K/enriched 单日数据（ halted 标的 open/high=0 触发停牌过滤）。"""
    halted = halted or set()
    n = len(symbols)
    df = pl.DataFrame(
        {
            "symbol": symbols,
            "date": [date.fromisoformat(day)] * n,
            "open": [0.0 if s in halted else 1.0 for s in symbols],
            "high": [0.0 if s in halted else 1.0 for s in symbols],
            "low": [0.0] * n,
            "close": [1.0] * n,
            "volume": [100.0] * n,
            "amount": [100.0] * n,
        }
    )
    if table == ENRICHED:
        # _prune_stale_price_partitions 以 raw_close 与 daily.close 做值级比对；
        # enriched 必须与 daily 同价，否则完整分区被误判"收盘价过期"而重算。
        df = df.with_columns(pl.col("close").alias("raw_close"))
    store.write_daily(table, df)


def _dates(store, table: str) -> list[date]:
    return store.list_daily_dates(table)


def test_partial_partition_is_pruned(ck_runtime) -> None:
    store = ck_runtime
    # 2026-08-28: daily 全市场 100 只, enriched 只有实时写入的 5 只 (issue 实测形态)
    _write_partition(store, DAILY, "2026-08-28", [f"s{i:06d}" for i in range(100)])
    _write_partition(store, ENRICHED, "2026-08-28", [f"s{i:06d}" for i in range(5)])
    # 前一日两边都是完整的 → 不动
    _write_partition(store, DAILY, "2026-08-27", ["a", "b"])
    _write_partition(store, ENRICHED, "2026-08-27", ["a", "b"])

    pruned = _prune_partial_enriched_partitions(store, DAILY, ENRICHED)

    assert pruned == ["2026-08-28"]
    assert date(2026, 8, 28) not in _dates(store, ENRICHED)
    assert date(2026, 8, 27) in _dates(store, ENRICHED)  # 完整分区保留


def test_complete_partitions_untouched(ck_runtime) -> None:
    store = ck_runtime
    syms = [f"s{i:06d}" for i in range(50)]
    _write_partition(store, DAILY, "2026-09-01", syms)
    _write_partition(store, ENRICHED, "2026-09-01", syms)

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []
    assert date(2026, 9, 1) in _dates(store, ENRICHED)


def test_enriched_date_without_daily_is_left_alone(ck_runtime) -> None:
    # 今日日K尚未同步时, 实时创建的当日分区留给当日正常流程处理
    store = ck_runtime
    day = "2026-09-07"
    _write_partition(store, ENRICHED, day, ["only_watchlist"])

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []
    assert date(2026, 9, 7) in _dates(store, ENRICHED)


@pytest.mark.parametrize("legacy_halt", [False, True])
def test_computed_partition_with_halt_is_preserved_on_repeated_checks(
    ck_runtime, monkeypatch, legacy_halt
):
    monkeypatch.setattr(pipeline, "_custom_signal_exprs", {})
    store = ck_runtime
    day = "2026-09-07"
    # 600002.SH 停牌（legacy: volume/amount=0；新: open/high/volume/amount=0）
    _write_partition(store, DAILY, day, ["600001.SH", "600002.SH"], halted={"600002.SH"})
    raw = store.read_daily(DAILY, start=date(2026, 9, 7), end=date(2026, 9, 7))
    computed = pipeline._select_storage_cols(pipeline.compute_enriched(raw))
    computed = computed.with_columns(pl.lit(date(2026, 9, 7)).alias("date"))
    assert computed["symbol"].to_list() == ["600001.SH"]
    store.write_daily(ENRICHED, computed)

    for _ in range(2):
        assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []
        assert store.read_daily(ENRICHED, start=date(2026, 9, 7), end=date(2026, 9, 7))[
            "symbol"
        ].to_list() == ["600001.SH"]


@pytest.mark.parametrize("actual", [["a"], ["a", "a"], ["a", "extra"], ["a", "extra", "other"]])
def test_missing_active_symbol_is_not_hidden_by_counts(ck_runtime, actual):
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"])
    _write_partition(store, ENRICHED, date_s, actual)

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == ["2026-09-07"]


def test_duplicate_daily_rows_do_not_require_duplicate_enriched_rows(ck_runtime):
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "a", "b"])
    _write_partition(store, ENRICHED, date_s, ["a", "b"])

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []


@pytest.mark.parametrize("missing", ["symbol", "open", "high"])
def test_missing_daily_required_column_does_not_delete(ck_runtime, monkeypatch, missing):
    """CK 表有固定 schema，无法"缺列"；用 read_daily 抛错模拟读取失败时的保留语义。"""
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"])
    _write_partition(store, ENRICHED, date_s, ["a"])
    original = store.read_daily

    def broken(table, *a, **kw):
        if table == DAILY:
            raise ValueError(f"missing column {missing}")
        return original(table, *a, **kw)

    monkeypatch.setattr(store, "read_daily", broken)

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []
    assert date(2026, 9, 7) in _dates(store, ENRICHED)


@pytest.mark.parametrize("broken_kind", ["daily", "enriched"])
def test_unreadable_partition_does_not_delete(ck_runtime, monkeypatch, broken_kind):
    """CK 无损坏文件概念；用 read_daily 抛错模拟读取失败时的保留语义。"""
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"])
    _write_partition(store, ENRICHED, date_s, ["a"])
    original = store.read_daily

    def broken(table, *a, **kw):
        if table == (DAILY if broken_kind == "daily" else ENRICHED):
            raise OSError("unreadable")
        return original(table, *a, **kw)

    monkeypatch.setattr(store, "read_daily", broken)

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []
    assert date(2026, 9, 7) in _dates(store, ENRICHED)


def test_partition_files_are_checked_together(ck_runtime):
    """CK 一天的数据是同一张表，天然"一起检查"；验证多行 enrichment 聚合覆盖。"""
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"])
    _write_partition(store, ENRICHED, date_s, ["a"])
    store.write_daily(
        ENRICHED,
        pl.DataFrame(
            {
                "symbol": ["b"],
                "date": [date(2026, 9, 7)],
                "open": [1.0],
                "high": [1.0],
                "low": [0.0],
                "close": [1.0],
                "volume": [100.0],
                "amount": [100.0],
            }
        ),
    )

    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []


@pytest.mark.parametrize("missing", ["volume", "amount"])
def test_legacy_daily_without_optional_halt_column(ck_runtime, missing):
    """CK 固定 schema 无法"缺可选列"；缺列时 filter_halt_days 不启用零成交判据，
    用 open/high 正常值造数即可覆盖该分支（停牌判定退化为仅看 open/high）。"""
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"])
    _write_partition(store, ENRICHED, date_s, ["a", "b"])
    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []


def test_all_halted_partition_does_not_require_active_rows(ck_runtime):
    store = ck_runtime
    date_s = "2026-09-07"
    _write_partition(store, DAILY, date_s, ["a", "b"], halted={"a", "b"})
    _write_partition(store, ENRICHED, date_s, ["a"])
    assert _prune_partial_enriched_partitions(store, DAILY, ENRICHED) == []


def test_pruned_interior_date_is_rebuilt_without_new_daily_or_factors(
    ck_runtime, monkeypatch, tmp_path
):
    store = ck_runtime
    dates = ["2026-09-01", "2026-09-02", "2026-09-03"]
    for day in dates:
        _write_partition(store, DAILY, day, ["a", "b"])
        _write_partition(store, ENRICHED, day, ["a"] if day == dates[1] else ["a", "b"])

    monkeypatch.setattr(daily_pipeline.settings, "data_dir", Path(tmp_path))
    monkeypatch.setattr(
        preferences,
        "load",
        lambda: {
            "pipeline_pull_a_share": False,
            "pipeline_regime_enabled": False,
            "minute_sync_enabled": False,
            "adj_factor_provider": "tickflow",
        },
    )
    monkeypatch.setattr(daily_pipeline.instrument_sync, "sync_instruments", lambda *_: 0)
    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda *_: [])
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *_: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *_: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_views", lambda *_: None)
    monkeypatch.setattr(data_integrity, "scan_recent_integrity", lambda *_, **__: [])
    calls = []

    def rebuild(**kwargs):
        calls.append(kwargs)
        assert kwargs["new_dates_only"] is True
        assert kwargs["symbols"] is None
        _write_partition(store, ENRICHED, dates[1], ["a", "b"])
        return 2

    monkeypatch.setattr(daily_pipeline, "run_pipeline", rebuild)
    # run_now 内 _prune 走 require_storage().store（= ck_runtime fixture 全局实例），
    # 而 repo.store 仅用于本地 data_dir 路径（相关阶段已被 mock/短路）。
    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=Path(tmp_path)),
        storage=store,
        latest_daily_date=lambda: date.fromisoformat(dates[-1]),
    )
    daily_pipeline.run_now(repo, CapabilitySet(set()))  # type: ignore[arg-type]
    assert len(calls) == 1
    assert sorted(
        store.read_daily(ENRICHED, start=date(2026, 9, 2), end=date(2026, 9, 2))["symbol"].to_list()
    ) == ["a", "b"]
    daily_pipeline.run_now(repo, CapabilitySet(set()))  # type: ignore[arg-type]
    assert len(calls) == 1
