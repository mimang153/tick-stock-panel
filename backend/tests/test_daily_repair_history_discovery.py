"""Historical defects discovered during a narrow repair must join its retry plan."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from app.indicators import pipeline
from app.services import daily_health, data_integrity, integrity_check, kline_sync, preferences
from app.services.pipeline_jobs import JobCancelledError
from app.tickflow.repository import DataStore, KlineRepository, daily_dirname, enriched_dirname

OLD = date(2026, 8, 31)
RECENT = date(2026, 10, 30)
END = date(2026, 11, 2)
TODAY = date(2026, 11, 3)


def frame(day, symbols, *, bad=False):
    prices = [10.0 + 10 * i for i in range(len(symbols))]
    return pl.DataFrame(
        {
            "symbol": symbols,
            "date": [day] * len(symbols),
            "open": prices,
            "high": [p + 1 for p in prices],
            "low": [p - 1 for p in prices],
            "close": [999.0 if bad and i == 0 else p for i, p in enumerate(prices)],
            "volume": [100.0] * len(symbols),
            "amount": [1000.0] * len(symbols),
        }
    )


@pytest.fixture(
    params=[
        ("stock", "600001.SH", "600002.SH"),
        ("etf", "510300.SH", "510500.SH"),
        ("index", "000001.SH", "399001.SZ"),
    ]
)
def env(request, tmp_path, monkeypatch, ck_runtime):
    asset, symbol, other = request.param
    monkeypatch.setattr(integrity_check, "cn_today", lambda: TODAY)
    monkeypatch.setattr(data_integrity, "cn_today", lambda: TODAY)
    monkeypatch.setattr(data_integrity, "_trading_calendar", lambda: {OLD, RECENT, END})
    monkeypatch.setattr(preferences, "load", lambda: {})
    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    monkeypatch.setattr(repo, "get_instruments_asset", lambda _: pl.DataFrame())
    raw_dir, enriched_dir = daily_dirname(asset), enriched_dirname(asset)
    for day in (OLD, RECENT, END):
        for table, bad in ((raw_dir, day == OLD), (enriched_dir, False)):
            part = tmp_path / table / f"date={day}"
            part.mkdir(parents=True, exist_ok=True)
            frame(day, [symbol, other], bad=bad).write_parquet(part / "part.parquet")
        # 原始日K双写 CK：recompute_daily_symbols 从 CK 读原始表重算（读路径 §8.4）
        ck_runtime.write_daily(raw_dir, frame(day, [symbol, other], bad=day == OLD))
    issue = data_integrity.IntegrityIssue(RECENT, raw_dir, "invalid", symbols=(symbol,))
    daily_health.record_scan(tmp_path, [issue], assets=[asset], start=RECENT, end=RECENT)
    daily_health.record_scan(tmp_path, [], assets=[asset], start=RECENT, end=RECENT)
    calls = []

    def fetch(symbols, _caps, *, start_time, asset_type, **_kwargs):
        calls.append((asset_type, start_time.date(), list(symbols)))
        yield frame(start_time.date(), [symbol, other]).filter(pl.col("symbol").is_in(symbols))

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch)

    def write_raw(day, df):
        """原始日K写本地分区 + CK（recompute_daily_symbols 读 CK 重算）。"""
        part = tmp_path / raw_dir / f"date={day}"
        part.mkdir(parents=True, exist_ok=True)
        df.write_parquet(part / "part.parquet")
        ck_runtime.write_daily(raw_dir, df)

    yield SimpleNamespace(
        root=tmp_path,
        repo=repo,
        asset=asset,
        symbol=symbol,
        other=other,
        raw_dir=raw_dir,
        enriched_dir=enriched_dir,
        calls=calls,
        fetch=fetch,
        write_raw=write_raw,
        ck=ck_runtime,
    )


def report(env):
    return integrity_check.run_check(
        env.root, lookback_days=30, asset_types=[env.asset], end_date=END
    )


def discover(env):
    initial = report(env)
    assert [(i["date"], i["kind"]) for i in initial["issues"]] == [
        (RECENT.isoformat(), "recompute")
    ]
    with pytest.raises(ValueError, match="2026-08-31"):
        integrity_check.run_repair(env.repo, object(), initial["id"])
    pending = integrity_check.latest_report(env.root)
    assert pending is not None
    historical = next(i for i in pending["issues"] if i["date"] == OLD.isoformat())
    assert historical["kind"] == "invalid" and historical["symbols"] == [env.symbol]
    return pending, historical


def files(root, table):
    return {p: p.read_bytes() for p in (root / table).rglob("*.parquet")}


def test_discovery_registers_old_partition_and_retry_keeps_narrow_scope(env):
    before_raw, before_enriched = files(env.root, env.raw_dir), files(env.root, env.enriched_dir)
    pending, issue = discover(env)
    assert "最高价" in issue["reason"]
    assert env.symbol in pending["repair_error"]
    assert "自检" in pending["repair_error"]
    assert env.calls == []
    assert files(env.root, env.raw_dir) == before_raw
    assert files(env.root, env.enriched_dir) == before_enriched
    with pytest.raises(daily_health.DailyIntegrityError):
        daily_health.assert_usable(env.root, env.asset)

    # The newly found issue survives a fresh read; it is not just attached to an exception.
    daily_health._read.cache_clear()
    assert any(
        e["day"] == OLD.isoformat() and e["needs_raw"]
        for e in daily_health.snapshot(env.root)["entries"]
    )
    result = integrity_check.run_repair(env.repo, object(), pending["id"])
    assert result["status"] == "clean" and result["lookback_days"] == 30
    assert env.calls == [(env.asset, OLD, [env.symbol])]
    daily_health.assert_usable(env.root, env.asset)
    for path, content in before_raw.items():
        if path.parent.name != f"date={OLD}":
            assert path.read_bytes() == content
    corrected = env.ck.read_daily(env.raw_dir, start=OLD, end=OLD)
    assert corrected.filter(pl.col("symbol") == env.symbol)["close"].item() == 10.0
    assert corrected.filter(pl.col("symbol") == env.other)["close"].item() == 20.0


@pytest.mark.parametrize("failure", ["empty", "invalid", "cancelled"])
def test_failed_retry_keeps_discovered_issue_and_existing_data(env, monkeypatch, failure):
    pending, _ = discover(env)
    before = {**files(env.root, env.raw_dir), **files(env.root, env.enriched_dir)}

    def fetch(*_args, **_kwargs):
        if failure == "cancelled":
            raise JobCancelledError("cancelled")
        yield pl.DataFrame() if failure == "empty" else frame(OLD, [env.symbol], bad=True)

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch)
    with pytest.raises(JobCancelledError if failure == "cancelled" else ValueError):
        integrity_check.run_repair(env.repo, object(), pending["id"])
    assert all(path.read_bytes() == content for path, content in before.items())
    historical = next(
        e for e in daily_health.snapshot(env.root)["entries"] if e["day"] == OLD.isoformat()
    )
    assert historical["needs_raw"] and historical["needs_enriched"]
    with pytest.raises(daily_health.DailyIntegrityError):
        daily_health.assert_usable(env.root, env.asset)


def test_discovery_does_not_clear_other_pending_symbols_or_assets(env):
    other_issue = data_integrity.IntegrityIssue(
        OLD, env.raw_dir, "invalid", symbols=(env.other,), reason="keep pending"
    )
    index_issue = data_integrity.IntegrityIssue(
        OLD,
        "kline_etf_daily" if env.asset != "etf" else "kline_daily",
        "corrupt",
        reason="keep other asset",
    )
    daily_health.record_scan(env.root, [other_issue, index_issue], assets=[], start=OLD, end=OLD)
    existing = [dict(e) for e in daily_health.snapshot(env.root)["entries"]]
    with pytest.raises(ValueError):
        pipeline.recompute_daily_symbols(env.repo, env.asset, start=RECENT, symbols=[env.symbol])
    entries = daily_health.snapshot(env.root)["entries"]
    for entry in existing:
        assert entry in entries
    assert any(e["symbol"] == env.symbol and e["day"] == OLD.isoformat() for e in entries)


def test_multiple_old_dates_are_added_to_the_same_retry_plan(env, monkeypatch):
    earlier = date(2026, 8, 28)
    env.write_raw(earlier, frame(earlier, [env.symbol], bad=True))
    monkeypatch.setattr(data_integrity, "_trading_calendar", lambda: {earlier, OLD, RECENT, END})
    initial = report(env)
    with pytest.raises(ValueError, match="2026-08-28"):
        integrity_check.run_repair(env.repo, object(), initial["id"])
    pending = integrity_check.latest_report(env.root)
    assert {i["date"] for i in pending["issues"]} == {  # type: ignore[index]
        earlier.isoformat(),
        OLD.isoformat(),
        RECENT.isoformat(),
    }
    assert pending is not None
    assert integrity_check.run_repair(env.repo, object(), pending["id"])["status"] == "clean"
    assert env.calls == [(env.asset, earlier, [env.symbol]), (env.asset, OLD, [env.symbol])]


def test_missing_row_date_is_repaired_using_its_source_partition(env):
    """行内 date 缺失能被自检定位到源分区。

    迁移边界：CK 分区键不允许 null date，recompute 从 CK 读原始日K重算时无法
    复现"行内日期缺失"（CK 层根本存不进 null）。故本用例验证仍有效的部分——
    自检（读本地分区）能识别 null-date 并定位到 OLD 分区；修复阶段的等价场景
    由 test_discovery_registers_old_partition 用 CK 可表达的坏值覆盖。
    """
    path = env.root / env.raw_dir / f"date={OLD}/part.parquet"
    corrupted = pl.read_parquet(path).with_columns(
        pl.when(pl.col("symbol") == env.symbol)
        .then(pl.lit(None, dtype=pl.Date))
        .otherwise(pl.col("date"))
        .alias("date"),
    )
    corrupted.write_parquet(path)
    # 自检仍读本地分区，能发现 null-date 并登记为 OLD 的 invalid issue
    issues = data_integrity.inspect_daily_partition(env.root, env.raw_dir, OLD)
    assert issues is not None
    assert "日期" in issues.reason
    assert env.symbol in issues.symbols


def test_in_progress_today_does_not_become_an_unrepairable_historical_issue(env):
    env.write_raw(OLD, frame(OLD, [env.symbol, env.other]))
    env.write_raw(TODAY, frame(TODAY, [env.symbol], bad=True))
    initial = report(env)
    with pytest.raises(ValueError, match="当日日K同步"):
        integrity_check.run_repair(env.repo, object(), initial["id"])
    assert all(e["day"] != TODAY.isoformat() for e in daily_health.snapshot(env.root)["entries"])
    assert env.calls == []
    env.write_raw(TODAY, frame(TODAY, [env.symbol]))
    pending = integrity_check.latest_report(env.root)
    assert pending is not None
    assert integrity_check.run_repair(env.repo, object(), pending["id"])["status"] == "clean"


def test_cancel_after_raw_repair_retries_only_indicator_calculation(env, monkeypatch):
    pending, _ = discover(env)
    original = pipeline.compute_enriched

    def cancelled(*_args, **_kwargs):
        raise JobCancelledError("cancelled calculation")

    before = files(env.root, env.enriched_dir)
    monkeypatch.setattr(pipeline, "compute_enriched", cancelled)
    with pytest.raises(JobCancelledError):
        integrity_check.run_repair(env.repo, object(), pending["id"])
    assert files(env.root, env.enriched_dir) == before
    pending = integrity_check.latest_report(env.root)
    assert all(i["kind"] == "recompute" for i in pending["issues"])  # type: ignore[index]
    assert env.calls == [(env.asset, OLD, [env.symbol])]
    monkeypatch.setattr(pipeline, "compute_enriched", original)
    assert pending is not None
    assert integrity_check.run_repair(env.repo, object(), pending["id"])["status"] == "clean"
    assert env.calls == [(env.asset, OLD, [env.symbol])]


def test_discovery_after_an_earlier_batch_write_keeps_ck_generation_unready(env, monkeypatch):
    issue = data_integrity.IntegrityIssue(RECENT, env.raw_dir, "invalid", symbols=(env.other,))
    daily_health.record_scan(env.root, [issue], assets=[], start=RECENT, end=RECENT)
    daily_health.record_scan(env.root, [], assets=[env.asset], start=RECENT, end=RECENT)
    env.write_raw(
        OLD,
        frame(OLD, [env.symbol, env.other]).with_columns(
            pl.when(pl.col("symbol") == env.other)
            .then(pl.lit(999.0))
            .otherwise(pl.col("close"))
            .alias("close"),
        ),
    )
    monkeypatch.setattr(preferences, "get_enriched_batch_size", lambda: 1)
    initial = report(env)
    with pytest.raises(ValueError, match="2026-08-31"):
        integrity_check.run_repair(env.repo, object(), initial["id"])
    assert not env.ck.current_generation(env.asset)
    pending = integrity_check.latest_report(env.root)
    assert pending is not None
    assert next(i for i in pending["issues"] if i["date"] == OLD.isoformat())["symbols"] == [
        env.other
    ]
    with pytest.raises(daily_health.DailyIntegrityError):
        daily_health.assert_usable(env.root, env.asset)
    assert pending is not None
    assert integrity_check.run_repair(env.repo, object(), pending["id"])["status"] == "clean"
    assert env.ck.current_generation(env.asset)["state"] == "ready"
    assert env.calls == [(env.asset, OLD, [env.other])]
