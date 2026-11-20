"""S4 同步中断恢复验收：子进程硬退出 + 启动恢复 + 重跑完整性。"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import router as core_router
from app.storage.sync_state import INTERRUPTED_ERROR, SyncStateStore
from app.storage.writer import BufferedWriter

TASK = "kline_sync"
SYMBOL = "000001.SZ"
DATES = [date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)]


def _batch() -> pl.DataFrame:
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


def _kill_child_code() -> str:
    return """
import os
import polars as pl
from app.config import settings
from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore
from app.storage.sync_state import SyncStateStore
from app.storage.writer import BufferedWriter
client = ClickHouseClient(
    url=settings.clickhouse_url,
    database=settings.clickhouse_database,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
    table_prefix=os.environ['TSP_TEST_PREFIX'],
)
store = ClickHouseStore(client)
state = SyncStateStore(client)
state.mark_ok('kline_sync')
state.mark_running('kline_sync')
writer = BufferedWriter(store, flush_interval_s=60, flush_rows=999)
writer.write('kline_daily', pl.DataFrame({
    'symbol': ['000001.SZ'],
    'date': [date(2024, 7, 1)],
    'open': [10.0], 'high': [11.0], 'low': [9.0], 'close': [10.0],
    'volume': [100.0], 'amount': [1000.0],
}))
os._exit(9)
""".replace("date(2024, 7, 1)", "__import__('datetime').date(2024, 7, 1)")


def _count(client, table: str, *, unique: bool = False) -> int:
    expression = "uniqExact((symbol, date))" if unique else "count()"
    df = client.query(f"SELECT {expression} AS n FROM {client.table(table)}")
    return int(df["n"][0])


def test_s4_sync_interruption(ck_runtime, tmp_path: Path) -> None:
    env = os.environ.copy()
    env["TSP_TEST_PREFIX"] = ck_runtime._client.table_prefix
    proc = subprocess.run(
        [sys.executable, "-c", _kill_child_code()],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0

    state = SyncStateStore(ck_runtime._client)
    running = state.get(TASK)
    assert running is not None
    assert running.status == "running"
    assert TASK in state.recover_interrupted()
    recovered = state.get(TASK)
    assert recovered is not None
    assert recovered.status == "failed"
    assert recovered.last_error == INTERRUPTED_ERROR
    assert "中断" in (recovered.last_error or "")

    app = FastAPI()
    app.include_router(core_router)
    with TestClient(app) as client:
        response = client.get("/api/sync/status", params={"task": TASK})
    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert "中断" in (response.json()["error"] or "")

    assert _count(ck_runtime._client, "kline_daily") == 0

    writer = BufferedWriter(ck_runtime, flush_interval_s=60, flush_rows=999)
    writer.write("kline_daily", _batch())
    writer.close()
    assert _count(ck_runtime._client, "kline_daily") == len(DATES)
    assert _count(ck_runtime._client, "kline_daily", unique=True) == len(DATES)
