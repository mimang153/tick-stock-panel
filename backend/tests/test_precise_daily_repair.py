"""日K修补的业务边界: 精确取数、先校验、完整历史计算、隔离其他股票。"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from app.market_time import CN_TZ
from app.services import data_integrity, integrity_check, kline_sync
from app.tickflow.repository import DataStore, KlineRepository

A, B = "600001.SH", "600002.SH"
TODAY = date(2026, 10, 30)
BAD_DAY = date(2026, 10, 27)
REAL_ITER_DAILY_FRAMES = kline_sync.iter_daily_frames


def frame(day, symbols=(A, B), prices=(10.0, 20.0)):
    return pl.DataFrame(
        {
            "symbol": list(symbols),
            "date": [day] * len(symbols),
            "open": list(prices),
            "high": [x + 1 for x in prices],
            "low": [x - 1 for x in prices],
            "close": list(prices),
            "volume": [100.0] * len(symbols),
            "amount": [1000.0] * len(symbols),
        }
    )


DAILY_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]


def repair_snapshot(storage, table, day):
    audit = storage.client.query(
        f"SELECT DISTINCT repair_id FROM {storage.client.table('ck_repair_audit')} "
        "WHERE dataset = {table:String} AND day = {day:Date} "
        "ORDER BY repair_id",
        {"table": table, "day": day},
    )
    assert not audit.is_empty()
    return storage.read_repair_snapshot(audit["repair_id"].to_list()[-1])


@pytest.fixture
def env(tmp_path, monkeypatch, ck_runtime):
    from app.jobs import daily_pipeline
    from app.services import preferences

    monkeypatch.setattr(integrity_check, "cn_today", lambda: TODAY)
    monkeypatch.setattr(
        data_integrity,
        "_trading_calendar",
        lambda: {date(2026, 10, d) for d in (26, 27, 28, 29, 30)},
    )
    monkeypatch.setattr(preferences, "load", lambda: {})

    def full_pipeline(*a, **kw):
        raise AssertionError("精准修补不应调用整条盘后管道")

    monkeypatch.setattr(daily_pipeline, "run_now", full_pipeline)
    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    for n in range(4):
        day = BAD_DAY + timedelta(days=n)
        repo.append_daily(frame(day))
        repo.append_enriched(frame(day).with_columns(pl.col("close").alias("raw_close")))
    corrupted = ck_runtime.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        pl.when(pl.col("symbol") == A)
        .then(pl.lit(999.0))
        .otherwise(pl.col("close"))
        .alias("close"),
    )
    ck_runtime.write_daily("kline_daily", corrupted)
    calls = []

    def fetch(symbols, capset, *, start_time, end_time, asset_type="stock", on_chunk_done=None):
        calls.append((list(symbols), start_time.date(), end_time.date(), asset_type))
        # 源即使多返回股票/日期, 也只能改请求范围里的异常记录。
        yield pl.concat([frame(BAD_DAY), frame(BAD_DAY + timedelta(days=1), prices=(50.0, 60.0))])

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch, raising=False)
    return SimpleNamespace(
        root=tmp_path, repo=repo, caps=SimpleNamespace(has=lambda _: True), calls=calls
    )


def test_self_check_identifies_only_the_bad_symbol(env):
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    assert report["issues"][0]["symbols"] == [A]
    assert report["issues"][0]["symbol_count"] == 1


def test_repair_fetches_only_bad_symbol_and_day_and_preserves_other_rows(env):
    generation_before = env.repo.get_matrix_data_generation("stock")
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    next_day = BAD_DAY + timedelta(days=1)
    next_before = env.repo.storage.read_daily("kline_daily", start=next_day, end=next_day)
    other_before = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).filter(
        pl.col("symbol") == B
    )
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert env.calls == [([A], BAD_DAY, BAD_DAY, "stock")]
    assert env.repo.storage.read_daily("kline_daily", start=next_day, end=next_day).equals(
        next_before
    )
    repaired = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert repaired.filter(pl.col("symbol") == B).equals(other_before)
    assert repaired.filter(pl.col("symbol") == A)["close"].item() == 10.0
    assert result["status"] == "clean"
    generation = env.repo.storage.current_generation("stock")
    assert generation["state"] == "ready"
    assert generation["changed_from"] == BAD_DAY
    assert env.repo.get_matrix_data_generation("stock") != generation_before


@pytest.mark.parametrize("empty", [False, True])
def test_failed_source_validation_never_changes_raw_or_enriched(env, monkeypatch, empty):
    from app.indicators import pipeline

    report = integrity_check.run_check(env.root, asset_types=["stock"])
    before = {
        table: env.repo.storage.read_daily(
            table, start=BAD_DAY - timedelta(days=1), end=BAD_DAY + timedelta(days=4)
        )
        for table in ("kline_daily", "kline_daily_enriched")
    }
    computed = []
    monkeypatch.setattr(
        pipeline, "recompute_daily_symbols", lambda *a, **kw: computed.append(True), raising=False
    )

    def fetch(*a, **kw):
        yield pl.DataFrame() if empty else frame(BAD_DAY, (A,), (float("nan"),))

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch)
    with pytest.raises((ValueError, RuntimeError)):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert not computed
    assert all(
        env.repo.storage.read_daily(
            table, start=BAD_DAY - timedelta(days=1), end=BAD_DAY + timedelta(days=4)
        ).equals(frame_before)
        for table, frame_before in before.items()
    )


@pytest.mark.parametrize("evidence", ["zero_activity", "before_listing", "stale_quote"])
def test_empty_source_removes_only_confirmed_nontrading_residue_with_snapshot(
    env, monkeypatch, evidence
):
    original = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    if evidence == "zero_activity":
        original = original.with_columns(
            *(
                pl.when(pl.col("symbol") == A).then(0.0).otherwise(pl.col(c)).alias(c)
                for c in ("volume", "amount")
            ),
        )
    elif evidence == "before_listing":
        monkeypatch.setattr(
            env.repo,
            "get_instruments_asset",
            lambda _: pl.DataFrame(
                {
                    "symbol": [A, B],
                    "listing_date": [BAD_DAY + timedelta(days=1), date(2000, 1, 1)],
                }
            ),
        )
    else:
        ts = int(
            datetime.combine(BAD_DAY - timedelta(days=1), time(15), tzinfo=CN_TZ).timestamp() * 1000
        )
        original = original.with_columns(
            pl.when(pl.col("symbol") == A)
            .then(ts)
            .otherwise(None)
            .cast(pl.Int64)
            .alias("quote_ts"),
        )
    env.repo.storage.write_daily("kline_daily", original)
    monkeypatch.setattr(kline_sync, "iter_daily_frames", lambda *a, **kw: iter(()))

    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])

    assert result["status"] == "clean"
    repaired = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert repaired.equals(original.filter(pl.col("symbol") == B).select(DAILY_COLS))
    snapshot = repair_snapshot(env.repo.storage, "kline_daily", BAD_DAY)
    assert snapshot["symbol"].to_list() == [A]
    assert not (env.root / "integrity_backups").exists()
    enriched = env.repo.storage.read_daily("kline_daily_enriched", start=BAD_DAY, end=BAD_DAY)
    assert enriched["symbol"].to_list() == [B]
    later = env.repo.storage.read_daily(
        "kline_daily_enriched", start=BAD_DAY + timedelta(days=1), end=BAD_DAY + timedelta(days=1)
    )
    assert A in later["symbol"].to_list()


@pytest.mark.parametrize(
    "column,value",
    [("volume", float("nan")), ("amount", float("nan")), ("amount", 1.0), ("volume", -1.0)],
)
def test_empty_source_cannot_remove_unconfirmed_activity(env, monkeypatch, column, value):
    original = (
        env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
        .with_columns(
            *(
                pl.when(pl.col("symbol") == A).then(0.0).otherwise(pl.col(c)).alias(c)
                for c in ("volume", "amount")
            ),
        )
        .with_columns(
            pl.when(pl.col("symbol") == A)
            .then(pl.lit(value))
            .otherwise(pl.col(column))
            .alias(column)
        )
    )
    env.repo.storage.write_daily("kline_daily", original)
    monkeypatch.setattr(kline_sync, "iter_daily_frames", lambda *a, **kw: iter(()))
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    with pytest.raises(ValueError, match="数据源未返回"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    unchanged = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert unchanged.equals(original.select(DAILY_COLS))
    assert not (env.root / "integrity_backups").exists()


def test_mixed_real_repair_and_nontrading_cleanup_preserve_healthy_rows(env, monkeypatch):
    placeholder = "600003.SH"
    base = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    residue = frame(BAD_DAY, (placeholder,), (float("nan"),)).with_columns(
        pl.lit(0.0).alias("volume"),
        pl.lit(0.0).alias("amount"),
    )
    polluted = pl.concat([base, residue])
    env.repo.storage.write_daily("kline_daily", polluted)
    monkeypatch.setattr(
        kline_sync, "iter_daily_frames", lambda *a, **kw: iter([frame(BAD_DAY, (A,), (10.0,))])
    )
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert result["status"] == "clean"
    repaired = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert set(repaired["symbol"]) == {A, B}
    assert repaired.filter(pl.col("symbol") == A)["close"].item() == 10.0
    assert repaired.filter(pl.col("symbol") == B).equals(
        base.filter(pl.col("symbol") == B).select(DAILY_COLS)
    )
    snapshot = repair_snapshot(env.repo.storage, "kline_daily", BAD_DAY)
    assert placeholder in snapshot["symbol"].to_list()
    assert not (env.root / "integrity_backups").exists()


def test_nontrading_cleanup_waits_for_all_required_source_rows(env, monkeypatch):
    original = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        *(
            pl.when(pl.col("symbol") == B).then(0.0).otherwise(pl.col(c)).alias(c)
            for c in ("volume", "amount")
        ),
        pl.when(pl.col("symbol") == B).then(float("nan")).otherwise(pl.col("close")).alias("close"),
    )
    env.repo.storage.write_daily("kline_daily", original)
    monkeypatch.setattr(kline_sync, "iter_daily_frames", lambda *a, **kw: iter(()))
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    with pytest.raises(ValueError, match="数据源未返回"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    unchanged = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert unchanged.equals(original.select(DAILY_COLS))


def test_repair_requests_full_beijing_day_through_tickflow_adapter(env, monkeypatch):
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    requested = []

    def batch(symbols, **kwargs):
        requested.append(kwargs)
        ts = int(datetime.combine(BAD_DAY, time(15), tzinfo=CN_TZ).timestamp() * 1000)
        if not kwargs["start_time"] <= ts <= kwargs["end_time"]:
            return {}
        return {
            A: {
                "timestamp": [ts],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.0],
                "volume": [100.0],
                "amount": [1000.0],
            }
        }

    monkeypatch.setattr(kline_sync, "iter_daily_frames", REAL_ITER_DAILY_FRAMES)
    monkeypatch.setattr(
        kline_sync, "get_client", lambda: SimpleNamespace(klines=SimpleNamespace(batch=batch))
    )
    monkeypatch.setattr(
        kline_sync, "resolve_limit", lambda *a, **kw: SimpleNamespace(batch=100, rpm=None)
    )
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    caps = CapabilitySet({Cap.KLINE_DAILY_BATCH: CapabilityLimits()})
    assert integrity_check.run_repair(env.repo, caps, report["id"])["status"] == "clean"
    start = datetime.fromtimestamp(requested[0]["start_time"] / 1000, CN_TZ)
    end = datetime.fromtimestamp(requested[0]["end_time"] / 1000, CN_TZ)
    assert start == datetime.combine(BAD_DAY, time.min, tzinfo=CN_TZ)
    assert end.date() == BAD_DAY and end.hour == 23 and end.minute == 59 and end.second == 59


def test_nontrading_cleanup_does_not_discard_an_active_row(env, monkeypatch):
    original = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        *(
            pl.when(pl.col("symbol") == A).then(value).otherwise(pl.col(column)).alias(column)
            for column, value in (("volume", 100.0), ("amount", 1000.0))
        ),
    )
    env.repo.storage.write_daily("kline_daily", original)
    monkeypatch.setattr(kline_sync, "iter_daily_frames", lambda *a, **kw: iter(()))
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    with pytest.raises(ValueError, match="数据源未返回"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    unchanged = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert unchanged.equals(original.select(DAILY_COLS))


def test_confirmed_cleanup_does_not_bypass_source_failure(env, monkeypatch):
    original = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        *(
            pl.when(pl.col("symbol") == A).then(0.0).otherwise(pl.col(c)).alias(c)
            for c in ("volume", "amount")
        ),
    )
    env.repo.storage.write_daily("kline_daily", original)

    def fail(*a, **kw):
        raise RuntimeError("source unavailable")

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fail)
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    with pytest.raises(RuntimeError, match="source unavailable"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    unchanged = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert unchanged.equals(original.select(DAILY_COLS))


def test_repository_rejects_unconfirmed_or_whole_day_cleanup(env):
    empty = pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date})
    before = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    backup = env.root / "integrity_backups" / "test"
    with pytest.raises(ValueError, match="无法确认"):
        env.repo.replace_daily_rows(
            "stock", BAD_DAY, empty, symbols=[A], backup_dir=backup, remove_symbols=[A]
        )
    assert env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).equals(before)
    assert not backup.exists()
    zeroed = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        pl.lit(0.0).alias("volume"),
        pl.lit(0.0).alias("amount"),
    )
    env.repo.storage.write_daily("kline_daily", zeroed)
    with pytest.raises(ValueError, match="整日日K为空"):
        env.repo.replace_daily_rows(
            "stock", BAD_DAY, empty, symbols=[A, B], backup_dir=backup, remove_symbols=[A, B]
        )
    unchanged = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert set(unchanged["symbol"]) == {A, B}
    assert not backup.exists()


def test_repository_cleanup_rechecks_concurrent_daily_changes(env, monkeypatch):
    zeroed = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        *(
            pl.when(pl.col("symbol") == A).then(0.0).otherwise(pl.col(c)).alias(c)
            for c in ("volume", "amount")
        ),
    )
    env.repo.storage.write_daily("kline_daily", zeroed)
    concurrent = frame(BAD_DAY)
    original_read = env.repo.storage.read_daily
    injected = False

    def changed(*args, **kwargs):
        nonlocal injected
        result = original_read(*args, **kwargs)
        if not injected and kwargs.get("columns") is None and kwargs.get("start") == BAD_DAY:
            injected = True
            env.repo.storage.write_daily("kline_daily", concurrent)
        return result

    monkeypatch.setattr(env.repo.storage, "read_daily", changed)
    backup = env.root / "integrity_backups" / "test"
    empty = pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date})
    with pytest.raises(ValueError, match="修补期间发生变化"):
        env.repo.replace_daily_rows(
            "stock", BAD_DAY, empty, symbols=[A], backup_dir=backup, remove_symbols=[A]
        )
    assert env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).equals(
        concurrent.select(DAILY_COLS)
    )
    assert not backup.exists()


def test_index_zero_activity_cannot_be_pruned_as_stock_residue(env):
    original = frame(BAD_DAY).with_columns(pl.lit(0.0).alias("volume"), pl.lit(0.0).alias("amount"))
    env.repo.storage.write_daily("kline_index_daily", original)
    backup = env.root / "integrity_backups" / "test"
    empty = pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date})
    with pytest.raises(ValueError, match="无法确认"):
        env.repo.replace_daily_rows(
            "index", BAD_DAY, empty, symbols=[A], backup_dir=backup, remove_symbols=[A]
        )
    unchanged = env.repo.storage.read_daily("kline_index_daily", start=BAD_DAY, end=BAD_DAY)
    assert unchanged.equals(original.select(DAILY_COLS))
    assert not backup.exists()


def test_empty_custom_stock_daily_does_not_silently_switch_to_tickflow(env, monkeypatch, caplog):
    from app.data_providers import custom
    from app.services import preferences

    calls = []

    def fetch(symbols, **kwargs):
        calls.append((symbols, kwargs))
        return pl.DataFrame()

    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: "test_source")
    monkeypatch.setattr(custom, "provider_has_dataset", lambda *a: True)
    monkeypatch.setattr(custom, "get_provider", lambda _: SimpleNamespace(get_daily=fetch))
    monkeypatch.setattr(kline_sync, "iter_daily_frames", REAL_ITER_DAILY_FRAMES)

    def wrong_source():
        raise AssertionError("must not switch the selected stock daily source")

    monkeypatch.setattr(kline_sync, "get_client", wrong_source)
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    with caplog.at_level("INFO"), pytest.raises(ValueError, match="数据源未返回"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert calls[0][0] == [A]
    assert calls[0][1]["start_time"].tzinfo == CN_TZ
    assert calls[0][1]["end_time"].hour == 23
    assert "provider=test_source" in caplog.text


def test_one_corrupt_file_falls_back_to_only_its_day(env):
    corrupted = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY).with_columns(
        pl.lit(-1.0).alias("close")
    )
    env.repo.storage.write_daily("kline_daily", corrupted)
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert env.calls == [([A, B], BAD_DAY, BAD_DAY, "stock")]
    assert result["status"] == "clean"
    repaired = env.repo.storage.read_daily("kline_daily", start=BAD_DAY, end=BAD_DAY)
    assert set(repaired["symbol"]) == {A, B}
    assert repaired.filter(pl.col("symbol") == A)["close"].to_list() == [10.0]
    snapshot = repair_snapshot(env.repo.storage, "kline_daily", BAD_DAY)
    assert set(snapshot["symbol"]) == {A, B}


@pytest.mark.parametrize("cancel", [False, True])
def test_retry_after_recompute_failure_does_not_fetch_day_k_again(env, monkeypatch, cancel):
    from app.indicators import pipeline
    from app.services.daily_health import DailyIntegrityError, assert_usable
    from app.services.pipeline_jobs import JobCancelledError

    original = pipeline.recompute_daily_symbols
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    failure = JobCancelledError("cancelled") if cancel else RuntimeError("compute failed")

    def fail(*a, **kw):
        raise failure

    monkeypatch.setattr(pipeline, "recompute_daily_symbols", fail)
    with pytest.raises(type(failure)):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    pending = integrity_check.latest_report(env.root)
    assert pending is not None
    assert pending["issues"][0]["kind"] == "recompute"
    assert pending is not None
    assert pending["repairable_count"] == 1
    assert len(env.calls) == 1
    with pytest.raises(DailyIntegrityError):
        assert_usable(env.root, symbols=[A])
    monkeypatch.setattr(pipeline, "recompute_daily_symbols", original)
    assert pending is not None
    result = integrity_check.run_repair(env.repo, env.caps, pending["id"])
    assert result["status"] == "clean" and len(env.calls) == 1
    assert_usable(env.root)


def test_known_bad_data_blocks_dependents_but_not_other_stock_or_etf(env, monkeypatch):
    from app.backtest.engine import BacktestEngine
    from app.services import daily_health
    from app.services.screener import ScreenerService

    integrity_check.run_check(env.root, asset_types=["stock"])
    with pytest.raises(daily_health.DailyIntegrityError):
        env.repo.get_daily(A, BAD_DAY, TODAY, columns=["close"])
    other = env.repo.get_daily(B, BAD_DAY, TODAY, columns=["symbol", "date", "close"])
    assert other.height == 4 and set(other["close"].to_list()) == {20.0}
    daily_health.assert_usable(env.root, "etf")
    with pytest.raises(daily_health.DailyIntegrityError):
        BacktestEngine(env.repo).data_generation_await(timeout_s=0.01)
    with pytest.raises(daily_health.DailyIntegrityError):
        ScreenerService(env.repo).run(TODAY, ["close > 0"])


def test_scanning_another_asset_cannot_erase_known_pending_stock(env):
    from app.services import daily_health

    integrity_check.run_check(env.root, asset_types=["stock"])
    integrity_check.run_check(env.root, lookback_days=1, asset_types=["etf"])
    with pytest.raises(daily_health.DailyIntegrityError):
        daily_health.assert_usable(env.root)
    report = integrity_check.run_check(env.root, lookback_days=1, asset_types=["stock"])
    assert report["repairable_count"] == 1
    assert report["issues"][0]["date"] == BAD_DAY.isoformat()


def test_targeted_repair_keeps_unhandled_issue_and_does_not_recompute_its_stock(env, monkeypatch):
    from app.indicators import pipeline
    from app.services import daily_health, repair_daily

    issue = data_integrity.IntegrityIssue(BAD_DAY, "kline_daily", "invalid", symbols=(A,))
    other_day = BAD_DAY + timedelta(days=1)
    other = data_integrity.IntegrityIssue(
        other_day, "kline_daily", "corrupt", symbols=(B,), reason="文件位于数据目录以外"
    )
    daily_health.record_scan(
        env.root, [issue, other], assets=["stock"], start=BAD_DAY, end=other_day
    )
    computed = []
    monkeypatch.setattr(
        pipeline,
        "recompute_daily_symbols",
        lambda repo, asset, **kw: computed.append((asset, kw["start"], kw["symbols"])),
    )
    monkeypatch.setattr(repair_daily, "_refresh_repair_dependents", lambda *args: None)
    repair_daily.repair_integrity_issues(
        env.repo, env.caps, [issue], backup_dir=env.root / "integrity_backups" / "targeted"
    )
    assert computed == [("stock", BAD_DAY, [A])]
    remaining = daily_health.snapshot(env.root)["entries"]
    assert len(remaining) == 1
    assert remaining[0]["symbol"] == B and remaining[0]["needs_raw"]
    assert remaining[0]["needs_enriched"]


@pytest.mark.parametrize("table", ["kline_daily", "kline_daily_enriched"])
def test_repair_rejects_linked_history_and_preserves_outside_file(env, table):
    from app.services import daily_health
    from app.tickflow.repository import DataStore, KlineRepository

    linked_day = BAD_DAY + timedelta(days=1) if table == "kline_daily" else BAD_DAY
    path = env.root / table / f"date={linked_day}/part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame(linked_day).write_parquet(path)
    outside = env.root.parent / f"{env.root.name}-{table}-outside.parquet"
    original = path.read_bytes()
    outside.write_bytes(original)
    path.unlink()
    path.symlink_to(outside)

    # CK 在线路径不读本地分区；离线兼容路径仍须拒绝链接。
    offline_repo = KlineRepository(DataStore(env.root))
    with pytest.raises(ValueError, match=r"链接|目录以外"):
        offline_repo._validate_repair_path(path)

    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert result["status"] == "clean"
    assert path.is_symlink() and outside.read_bytes() == original
    daily_health.assert_usable(env.root)


@pytest.mark.parametrize("asset,symbol", [("etf", "510300.SH"), ("index", "000001.SH")])
def test_etf_and_index_repairs_never_write_stock_data(env, monkeypatch, asset, symbol):
    from app.tickflow.repository import daily_dirname

    stock_before = env.repo.storage.read_daily(
        "kline_daily", start=BAD_DAY - timedelta(days=1), end=BAD_DAY + timedelta(days=4)
    )
    for n in range(3):
        env.repo.append_daily_asset(asset, frame(BAD_DAY + timedelta(days=n), (symbol,), (10.0,)))
    corrupted = env.repo.storage.read_daily(
        daily_dirname(asset), start=BAD_DAY, end=BAD_DAY
    ).with_columns(pl.lit(-1.0).alias("close"))
    env.repo.storage.write_daily(daily_dirname(asset), corrupted)
    calls = []

    def fetch(symbols, capset, **kwargs):
        calls.append((symbols, kwargs["asset_type"]))
        yield frame(BAD_DAY, (symbol,), (10.0,))

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch)
    report = integrity_check.run_check(env.root, asset_types=[asset])
    assert integrity_check.run_repair(env.repo, env.caps, report["id"])["status"] == "clean"
    assert calls == [([symbol], asset)]
    stock_after = env.repo.storage.read_daily(
        "kline_daily", start=BAD_DAY - timedelta(days=1), end=BAD_DAY + timedelta(days=4)
    )
    assert stock_after.equals(stock_before)


def _seed_full_raw_history(env, monkeypatch):
    from app.indicators import pipeline

    days = []
    cursor = BAD_DAY - timedelta(days=150)
    while cursor < BAD_DAY:
        if cursor.weekday() < 5:
            days.append(cursor)
            env.repo.append_daily(frame(cursor, prices=(10 + len(days) % 11 / 10, 20.0)))
        cursor += timedelta(days=1)
    instruments = pl.DataFrame({"symbol": [A, B], "name": ["A", "B"], "float_shares": [1e6, 2e6]})
    instruments.write_parquet(env.root / "instruments/all.parquet")
    factors = pl.DataFrame({"symbol": [A], "trade_date": [days[30]], "ex_factor": [1.1]})
    factors.write_parquet(env.root / "adj_factor/all.parquet")
    # 除权因子与历史股本是 CK 权威读源，重算路径从 CK 读，必须同步写入
    env.repo.storage.write_adj_factor(factors)
    shares = pl.DataFrame(
        {
            "symbol": [A],
            "period_end": [days[0]],
            "announce_date": [days[20]],
            "float_shares": [800000.0],
        }
    )
    shares.write_parquet(env.root / "financials/shares/part.parquet")
    env.repo.storage.write_financials("financials_shares", shares)
    monkeypatch.setattr(env.repo, "get_instruments_asset", lambda asset: instruments)
    monkeypatch.setattr(
        data_integrity,
        "_trading_calendar",
        lambda: set(days) | {BAD_DAY + timedelta(days=n) for n in range(4)},
    )
    raw = env.repo.storage.read_daily(
        "kline_daily", start=days[0] - timedelta(days=1), end=BAD_DAY + timedelta(days=4)
    ).sort("symbol", "date")
    correct = raw.with_columns(
        pl.when((pl.col("symbol") == A) & (pl.col("date") == BAD_DAY))
        .then(pl.lit(10.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )
    expected = pipeline._select_storage_cols(
        pipeline.compute_enriched(
            correct, factors=factors, instruments=instruments, historical_shares=shares
        )
    )
    env.repo.append_enriched(expected)
    return expected, days[-1], expected.filter(pl.col("symbol") == B).sort("date")


def test_recompute_uses_full_raw_history_and_preserves_prior_dates_and_other_stock(
    env, monkeypatch
):
    from polars.testing import assert_frame_equal

    from app.indicators import pipeline

    expected, prior_day, other_before = _seed_full_raw_history(env, monkeypatch)
    prior_before = env.repo.storage.read_daily(
        "kline_daily_enriched", start=prior_day, end=prior_day
    )
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert result["status"] == "clean"
    actual = (
        env.repo.storage.read_daily(
            "kline_daily_enriched",
            start=BAD_DAY - timedelta(days=160),
            end=BAD_DAY + timedelta(days=4),
        )
        .select(expected.columns)
        .sort("symbol", "date")
    )
    assert_frame_equal(
        actual.filter(pl.col("symbol") == B).sort("date"), other_before, check_exact=True
    )
    assert_frame_equal(
        actual.filter(pl.col("symbol") == A),
        expected.filter(pl.col("symbol") == A).sort("date"),
        check_exact=True,
    )
    prior_after = env.repo.storage.read_daily(
        "kline_daily_enriched", start=prior_day, end=prior_day
    )
    assert prior_after.equals(prior_before)
    indicators = pipeline.compute_indicators(actual.filter(pl.col("symbol") == A))
    full_expected = pipeline.compute_indicators(expected.filter(pl.col("symbol") == A))
    assert_frame_equal(
        indicators.select("date", "ma20", "macd_dif"),
        full_expected.select("date", "ma20", "macd_dif"),
        check_exact=True,
    )


def test_interrupted_publication_retries_without_fetching_or_widening_scope(env, monkeypatch):
    from app.services.daily_health import DailyIntegrityError, assert_usable

    write_daily = env.repo.storage.write_daily
    failed = False

    def fail_once(table, df, *args, **kwargs):
        nonlocal failed
        if table == "kline_daily_enriched" and not failed:
            failed = True
            raise OSError("write interrupted")
        return write_daily(table, df, *args, **kwargs)

    report = integrity_check.run_check(env.root, asset_types=["stock"])
    monkeypatch.setattr(env.repo.storage, "write_daily", fail_once)
    with pytest.raises(OSError, match="interrupted"):
        integrity_check.run_repair(env.repo, env.caps, report["id"])
    with pytest.raises(DailyIntegrityError):
        assert_usable(env.root)
    pending = integrity_check.latest_report(env.root)
    assert pending is not None
    assert integrity_check.run_repair(env.repo, env.caps, pending["id"])["status"] == "clean"
    assert len(env.calls) == 1
    assert_usable(env.root)


def test_local_publication_marker_does_not_block_ck_repair(env):
    from app.enriched_generation import EnrichedPublication

    previous = EnrichedPublication(env.root, recover=True)
    previous.write_parquet(
        frame(BAD_DAY, (B,), (20.0,)),
        env.root / f"kline_daily_enriched/date={BAD_DAY}/part.parquet",
    )
    previous.abandon()
    report = integrity_check.run_check(env.root, asset_types=["stock"])
    result = integrity_check.run_repair(env.repo, env.caps, report["id"])
    assert result["status"] == "clean"
    generation = env.repo.storage.current_generation("stock")
    assert generation["state"] == "ready"


def test_empty_recomputed_market_result_removes_old_rows_only_in_repaired_range(env, monkeypatch):
    from app.services import market_mainline, regime_builder

    # 本测试验证本地 parquet fallback 路径（tmp_path 造数），禁用 CK 门面
    # 避免 replace_mainline_range 改走 CK 而不动本地文件。
    monkeypatch.setattr(market_mainline, "_mainline_store", lambda: None)

    before = BAD_DAY - timedelta(days=1)
    path = market_mainline.mainline_path(env.root)
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [before, BAD_DAY, BAD_DAY],
            "kind": ["concept", "concept", "industry"],
            "rank": [1, 1, 1],
            "name": ["keep prior", "stale", "keep other kind"],
        }
    ).write_parquet(path)
    market_mainline.replace_mainline_range(env.root, BAD_DAY, TODAY, "concept", pl.DataFrame())
    assert pl.read_parquet(path)["name"].to_list() == ["keep prior", "keep other kind"]
    # CK-only 下 regime 权威在 CK：构造 CK 行并验证坏日被清理。
    from app.storage.runtime import get_storage

    runtime = get_storage()
    assert runtime is not None
    store = runtime.store
    store.upsert_regime(
        pl.DataFrame(
            {
                "date": [before, BAD_DAY],
                "payload_json": [
                    json.dumps({"date": before.isoformat(), "score": 50.0}),
                    json.dumps({"date": BAD_DAY.isoformat(), "score": 99.0}),
                ],
            }
        )
    )
    regime_builder.replace_regime_range(env.root, BAD_DAY, TODAY, pl.DataFrame())
    remaining = regime_builder.load_regime_history(env.root)
    assert remaining["date"].to_list() == [before]


def test_completed_backtest_reconnect_cache_expires_after_daily_correction(env, monkeypatch):
    import time

    from app.api import backtest
    from app.config import settings

    monkeypatch.setattr(settings, "data_dir", env.root)
    job = backtest._BacktestJob("repair-test")
    job.done = True
    job.finish_ts = time.time()
    monkeypatch.setattr(backtest, "_running_jobs", {job.key: job})
    integrity_check.run_check(env.root, asset_types=["stock"])
    backtest._cleanup_stale_jobs()
    assert job.key not in backtest._running_jobs


def test_integrity_notifications_reach_every_subscriber_without_dropping_repair_event():
    from app.services.quote_service import QuoteService

    service = QuoteService()
    first, second = service.subscribe(), service.subscribe()
    service.notify_daily_integrity_updated(["stock"], data_changed=False)
    service.notify_daily_integrity_updated(["stock"], data_changed=True)
    for sub in (first, second):
        assert sub.wait(timeout=0.01)
        assert sub.pop()["daily_integrity_updated"] is True
        assert not sub.wait(timeout=0.01)


def test_realtime_matrix_rebuilds_history_after_daily_repair_revision(monkeypatch):
    import numpy as np

    from app.strategy.engine import StrategyDataContext, StrategyEngine

    engine = StrategyEngine(strategy_dirs=[])
    strategy = SimpleNamespace(
        meta={},
        execution_backend="matrix_native",
        matrix_strategy=SimpleNamespace(required_warmup_bars=lambda params: 2),
    )
    monkeypatch.setattr(engine, "get", lambda sid: strategy)
    monkeypatch.setattr(engine, "resolve_params", lambda *args: {})
    monkeypatch.setattr(engine, "_matrix_field_columns", lambda *args: {"close"})
    current = frame(TODAY, (A,), (30.0,))
    old_history = pl.concat(
        [frame(BAD_DAY, (A,), (10.0,)), frame(BAD_DAY + timedelta(days=1), (A,), (20.0,))]
    )
    corrected = pl.concat(
        [frame(BAD_DAY, (A,), (5.0,)), frame(BAD_DAY + timedelta(days=1), (A,), (7.0,))]
    )
    first = engine.prepare_realtime_matrix(
        StrategyDataContext(
            asset_type="stock",
            timeframe="1d",
            as_of=TODAY,
            current=current,
            history=old_history,
            cache_key="monitor:stock",
            data_generation="before-repair",
        ),
        ["test"],
    )
    second = engine.prepare_realtime_matrix(
        StrategyDataContext(
            asset_type="stock",
            timeframe="1d",
            as_of=TODAY,
            current=current,
            history=corrected,
            cache_key="monitor:stock",
            data_generation="after-repair",
        ),
        ["test"],
    )
    np.testing.assert_allclose(first.close[:, 0], [10.0, 20.0, 30.0])
    np.testing.assert_allclose(second.close[:, 0], [5.0, 7.0, 30.0])
