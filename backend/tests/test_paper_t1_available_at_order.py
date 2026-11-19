"""模拟盘 T+1 可卖数量在下单时按当前交易日现算, 不读物化持仓里过期的 available_qty。

复现: 当日买入成交后持仓里的 available_qty 是成交当日的 T+1 口径 (0);
跨日后没有任何路径重建它 (只有再次成交 / 除权 / 手动 rebuild 才会), create_order
的卖出预检却一直读这个 0, 而 overview 按当前交易日现算显示可卖 100 ——
持仓表写着可卖, 下单却被「可卖数量不足 (T+1): 可卖 0」拒绝。

DES-005: 持仓/台账/账户在 PG 中按 account_id 隔离，唯一 id 由 conftest 提供。
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.paper import router
from app.strategy import paper
from app.tickflow.repository import DataStore, KlineRepository

SYM = "600519.SH"
DAY = date(2026, 9, 24)  # 周四, 次日仍是交易日


def _write_daily(tmp_path: Path, rows: list[tuple[date, float, float]]) -> None:
    """写 kline_daily 分区: [(day, open, close)] (不复权 raw 价)。"""
    repo = KlineRepository(DataStore(tmp_path))
    df = pl.DataFrame(
        {
            "symbol": [SYM] * len(rows),
            "date": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [max(r[1], r[2]) for r in rows],
            "low": [min(r[1], r[2]) for r in rows],
            "close": [r[2] for r in rows],
            "volume": [10000.0] * len(rows),
            "amount": [r[2] * 10000.0 for r in rows],
        }
    )
    repo.append_daily(df)


def _buy_intraday(tmp_path: Path, monkeypatch, qty: int, account_id: str) -> None:
    """DAY 盘中即时买入成交; 当日 T+1 不可卖 (两种口径此时一致, 均为 0)。"""
    monkeypatch.setattr(paper, "cn_today", lambda: DAY)
    _write_daily(tmp_path, [(DAY - timedelta(days=1), 10.0, 10.0)])
    paper.create_account(tmp_path, 1_000_000, account_id=account_id)
    _, err = paper.create_order(
        tmp_path, SYM, "buy", qty=qty, ref_price=10.0, account_id=account_id
    )
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=account_id)) == 1
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=account_id)
    assert err is not None and "可卖数量不足" in err


def test_sell_next_day_without_rematerialize(tmp_path, monkeypatch, paper_account_id: str):
    """次日可卖: 中间不重建物化持仓 (生产环境跨日后没有任何路径会重建)。"""
    _buy_intraday(tmp_path, monkeypatch, 100, paper_account_id)
    monkeypatch.setattr(paper, "cn_today", lambda: DAY + timedelta(days=1))
    # 持仓表 (overview) 按当前交易日现算: 可卖 100
    assert (
        paper.overview(tmp_path, account_id=paper_account_id)["holdings"][0]["available_qty"] == 100
    )
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=paper_account_id)
    assert sell is not None
    assert err is None and sell["status"] == "pending"


def test_sell_next_day_after_settle_fill(tmp_path, monkeypatch, paper_account_id: str):
    """盘后结算成交 (close 单) 的买入, 次日同样可卖。"""
    monkeypatch.setattr(paper, "cn_today", lambda: DAY)
    _write_daily(tmp_path, [(DAY - timedelta(days=1), 10.0, 10.0), (DAY, 10.2, 10.8)])
    paper.create_account(tmp_path, 1_000_000, account_id=paper_account_id)
    _, err = paper.create_order(
        tmp_path,
        SYM,
        "buy",
        qty=100,
        order_type="close",
        ref_price=10.0,
        account_id=paper_account_id,
    )
    assert err is None
    assert paper.settle_day(tmp_path, DAY.isoformat(), account_id=paper_account_id)["filled"] == 1
    monkeypatch.setattr(paper, "cn_today", lambda: DAY + timedelta(days=1))
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=paper_account_id)
    assert sell is not None
    assert err is None and sell["status"] == "pending"


def test_pending_sell_quota_uses_current_day_availability(
    tmp_path, monkeypatch, paper_account_id: str
):
    """超卖防护同样按当前交易日可卖数计: 可卖 200, 两张 100 股卖出单都能挂, 第三张拒。"""
    _buy_intraday(tmp_path, monkeypatch, 200, paper_account_id)
    monkeypatch.setattr(paper, "cn_today", lambda: DAY + timedelta(days=1))
    s1, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=paper_account_id)
    assert s1 is not None
    assert err is None and s1["status"] == "pending"
    s2, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=paper_account_id)
    assert s2 is not None
    assert err is None and s2["status"] == "pending"
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=paper_account_id)
    assert err is not None and "可卖 200" in err and "待成交卖出 200" in err


def test_api_sell_next_day_returns_200(tmp_path, monkeypatch, paper_account_id: str):
    """HTTP 层: 持仓接口显示可卖 100, 卖出下单应 200 而非 400「可卖 0」。"""
    _buy_intraday(tmp_path, monkeypatch, 100, paper_account_id)
    monkeypatch.setattr(paper, "cn_today", lambda: DAY + timedelta(days=1))
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        resolve_asset_type=lambda s: "stock",
    )
    client = TestClient(app)
    q = f"?account={paper_account_id}"
    holdings = client.get(f"/api/paper/positions{q}").json()["holdings"]
    assert holdings[0]["available_qty"] == 100
    r = client.post(f"/api/paper/orders{q}", json={"symbol": SYM, "side": "sell", "qty": 100})
    assert r.status_code == 200, r.text
    assert r.json()["order"]["status"] == "pending"
