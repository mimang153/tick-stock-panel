"""T6.1 场景测试共享 fixture（验收文档 §7，真实 CK 隔离库）。

所有场景跑在 ``ck_runtime`` 的随机前缀隔离表上，HTTP 层用 TestClient 注入
真实 ``KlineRepository``（双写本地 tmp_path + CK），不启认证中间件（场景关注
数据链路，认证由 L3.5 契约层覆盖）。造数统一经 repository 写方法，保证本地
与 CK 镜像一致。
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.tickflow.repository import DataStore, KlineRepository

__all__ = ["DAILY_ROWS", "scenario_app", "scenario_client", "write_daily_frames"]


def _daily_frame(symbol: str, day: date, close: float, *, volume: float = 100.0) -> dict:
    return {
        "symbol": symbol,
        "date": day,
        "open": close - 0.5,
        "high": close + 0.5,
        "low": close - 1.0,
        "close": close,
        "volume": volume,
        "amount": close * volume,
    }


DAILY_ROWS = _daily_frame


def write_daily_frames(
    repo: KlineRepository, rows: list[dict], *, asset_type: str = "stock"
) -> None:
    """经 repository 写日K + enriched，本地 parquet 与 CK 双写。"""
    df = pl.DataFrame(rows)
    repo.append_daily_asset(asset_type, df)
    repo.append_enriched_asset(asset_type, df)
    repo.refresh_cache()


@pytest.fixture()
def scenario_app(ck_runtime, tmp_path, monkeypatch):
    """真实 CK + 本地 repo 的 HTTP app（仅注入场景所需 state，无认证）。"""
    monkeypatch.setattr("app.config.settings.data_dir", tmp_path)
    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    repo.save_instruments(
        pl.DataFrame(
            {
                "symbol": ["000001.SZ", "600000.SH"],
                "name": ["平安银行", "浦发银行"],
            }
        )
    )

    from app.api import data as data_api
    from app.api import kline as kline_api
    from app.api import watchlist as watchlist_api

    app = FastAPI()
    app.include_router(data_api.router)
    app.include_router(kline_api.router)
    app.include_router(watchlist_api.router)
    app.state.repo = repo
    app.state.capabilities = SimpleNamespace()
    return app, repo, ck_runtime


@pytest.fixture()
def scenario_client(scenario_app):
    app, repo, store = scenario_app
    with TestClient(app) as client:
        yield client, repo, store
