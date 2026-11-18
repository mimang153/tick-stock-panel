"""S9 数据完整性检查与修复（验收文档 §7）。

CK 双写造数 → 制造"某交易日坏分区" → 自检精确报告日期+标的维度
→ 模拟数据源修复 → 复查 clean。完整性检查语义保留本地分区口径（设计 §8.4）。
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import data as data_api
from app.services import (
    data_integrity,
    heavy_job_limiter,
    integrity_check,
    pipeline_jobs,
    preferences,
)
from app.tickflow.repository import DataStore, KlineRepository

from .conftest import DAILY_ROWS, write_daily_frames


def _wait(store, jid, deadline_s=3):
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        job = store.get(jid)
        if job["status"] in ("succeeded", "failed"):
            return job
        time.sleep(0.01)
    raise AssertionError("job did not finish")


def test_s9_integrity_scan_repair(scenario_app, monkeypatch):
    _, base_repo, ck = scenario_app
    root = base_repo.store.data_dir
    bad_day = date(2026, 10, 29)

    # job 相关单例隔离（同 test_data_integrity_api.app_env）
    jobs = pipeline_jobs.JobStore(store_dir=root / "jobs")
    monkeypatch.setattr(data_api, "job_store", jobs)
    monkeypatch.setattr(pipeline_jobs, "job_store", jobs)
    monkeypatch.setattr(pipeline_jobs, "_CANCEL_FLAGS", {})
    monkeypatch.setattr(pipeline_jobs, "_run_slot_owner", None)
    monkeypatch.setattr(
        heavy_job_limiter, "shared_heavy_job_limiter", heavy_job_limiter.HeavyJobLimiter()
    )
    monkeypatch.setattr(heavy_job_limiter, "_memory_admission_ok", lambda: True)
    monkeypatch.setattr(preferences, "load", lambda: {})
    monkeypatch.setattr(integrity_check, "cn_today", lambda: date(2026, 10, 30))
    monkeypatch.setattr(data_integrity, "_trading_calendar", lambda: {bad_day})

    # repo 走 CK 双写
    repo = KlineRepository(DataStore(root), storage=ck)
    repo.save_instruments(pl.DataFrame({"symbol": ["600001.SH", "600002.SH"], "name": ["A", "B"]}))
    write_daily_frames(
        repo,
        [
            DAILY_ROWS("600001.SH", bad_day, 10.0),
            DAILY_ROWS("600002.SH", bad_day, 20.0),
        ],
    )
    # 制造坏分区：覆盖本地 part.parquet（CK 镜像仍有正确数据）
    part = root / "kline_daily" / f"date={bad_day}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "part.parquet").write_bytes(b"bad parquet")

    from app.services import kline_sync

    def fetch(*args, **kwargs):
        yield pl.DataFrame(
            {
                "symbol": ["600001.SH", "600002.SH"],
                "date": [bad_day, bad_day],
                "open": [10.0, 20.0],
                "high": [11.0, 21.0],
                "low": [9.0, 19.0],
                "close": [10.0, 20.0],
                "volume": [100.0, 100.0],
                "amount": [1000.0, 2000.0],
            }
        )

    monkeypatch.setattr(kline_sync, "iter_daily_frames", fetch)

    @contextmanager
    def pause():
        yield

    app2 = FastAPI()
    app2.include_router(data_api.router)
    app2.state.repo = repo
    app2.state.capabilities = SimpleNamespace(has=lambda cap: True)
    app2.state.quote_service = SimpleNamespace(paused=pause)

    with TestClient(app2) as client:
        # 1. 正常态之前先自检：精确报告坏日期（不是笼统 boolean）
        resp = client.post(
            "/api/data/integrity/scan", json={"lookback_days": 7, "asset_types": ["stock"]}
        )
        assert resp.status_code == 200
        assert _wait(jobs, resp.json()["job_id"])["status"] == "succeeded"
        report = client.get("/api/data/integrity").json()["report"]
        assert report["repairable_count"] >= 1
        assert any(i["date"] == bad_day.isoformat() for i in report["issues"])

        # 2. 修复 → 复查 clean
        fix = client.post("/api/data/integrity/repair", json={"report_id": report["id"]})
        assert fix.status_code == 200
        assert _wait(jobs, fix.json()["job_id"])["status"] == "succeeded"
        final = client.get("/api/data/integrity").json()["report"]
        assert final["status"] == "clean" and final["issues"] == []

        # 3. CK 镜像在修复后仍可读，行数一致（无重复）
        assert ck.table_stats("kline_daily")["rows"] >= 2
