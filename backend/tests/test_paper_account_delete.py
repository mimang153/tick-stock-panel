"""模拟盘账户删除 / 回合明细 / 对比盘中估算 契约测试。

删除是破坏性操作 (per CONTRIBUTING §8): 必须覆盖 fail-closed 拒绝、force 强删、
不存在账户、路径穿越防护; 强删后 PG 中不得残留该账户的任何数据。

DES-005: 账户状态在 PG 中按 account_id 隔离，测试用 conftest 唯一 id。
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.paper import router
from app.market_time import cn_today
from app.strategy import paper


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        resolve_asset_type=lambda s: "stock",
    )
    return TestClient(app)


def _mk_account(client: TestClient, acc: str, cash: float = 1000000) -> None:
    r = client.post("/api/paper/account", json={"initial_cash": cash, "account_id": acc})
    assert r.status_code == 200


def test_delete_empty_account(client: TestClient, paper_account_id: str, paper_pg):
    _mk_account(client, paper_account_id)
    r = client.delete(f"/api/paper/account?account={paper_account_id}")
    assert r.status_code == 200 and r.json()["ok"] is True
    assert paper_pg.get_account(paper_account_id) is None
    ids = {a["id"] for a in client.get("/api/paper/accounts").json()["accounts"]}
    assert paper_account_id not in ids


def test_delete_missing_account_404(client: TestClient, paper_account_id: str):
    r = client.delete(f"/api/paper/account?account={paper_account_id}")
    assert r.status_code == 404


def test_delete_rejects_invalid_id(client: TestClient):
    assert client.delete("/api/paper/account?account=../escape").status_code == 400


def test_delete_with_pending_order_requires_force(
    client: TestClient, paper_account_id: str, paper_pg
):
    _mk_account(client, paper_account_id)
    assert (
        client.post(
            f"/api/paper/orders?account={paper_account_id}",
            json={"symbol": "600519.SH", "side": "buy", "qty": 100, "ref_price": 1500.0},
        ).status_code
        == 200
    )
    r = client.delete(f"/api/paper/account?account={paper_account_id}")
    assert r.status_code == 400 and "force" in r.json()["detail"]
    assert paper_pg.get_account(paper_account_id) is not None  # 拒绝时不动数据

    r = client.delete(f"/api/paper/account?account={paper_account_id}&force=true")
    assert r.status_code == 200
    assert paper_pg.get_account(paper_account_id) is None
    assert paper_pg.load_orders(paper_account_id) == {}


def test_delete_with_holding_requires_force(
    client: TestClient, tmp_path: Path, paper_account_id: str, paper_pg
):
    """持仓存在时同样 fail-closed; 直接写 PG 台账构造持仓 (绕过撮合, 聚焦删除语义)。"""
    _mk_account(client, paper_account_id)
    paper_pg.append_fill(
        paper_account_id,
        {
            "ts": "2026-10-14T10:00:00+08:00",
            "date": "2026-10-13",
            "order_id": "o1",
            "symbol": "600519.SH",
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 1500.0,
            "fee": 5.0,
            "kind": "fill",
        },
    )
    paper.rebuild_positions(tmp_path, paper_account_id)

    r = client.delete(f"/api/paper/account?account={paper_account_id}")
    assert r.status_code == 400 and "持仓" in r.json()["detail"]
    assert paper_pg.get_account(paper_account_id) is not None  # 拒绝时不动数据
    assert (
        client.delete(f"/api/paper/account?account={paper_account_id}&force=true").status_code
        == 200
    )
    assert paper_pg.get_account(paper_account_id) is None
    assert paper_pg.load_fills(paper_account_id) == []


def test_rounds_endpoint_empty_and_filled(client: TestClient, paper_account_id: str, paper_pg):
    _mk_account(client, paper_account_id)
    assert client.get(f"/api/paper/rounds?account={paper_account_id}").json() == {"rounds": []}

    # 一买一卖 → 一个完整回合（直接写 PG 台账）
    for row in (
        {
            "ts": "2026-10-10T10:00:00+08:00",
            "date": "2026-10-10",
            "order_id": "o1",
            "symbol": "600519.SH",
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 100.0,
            "fee": 5.0,
            "kind": "fill",
        },
        {
            "ts": "2026-10-13T10:00:00+08:00",
            "date": "2026-10-13",
            "order_id": "o2",
            "symbol": "600519.SH",
            "asset_type": "stock",
            "side": "sell",
            "qty": 100,
            "price": 110.0,
            "fee": 6.0,
            "kind": "fill",
        },
    ):
        paper_pg.append_fill(paper_account_id, row)

    rounds = client.get(f"/api/paper/rounds?account={paper_account_id}").json()["rounds"]
    assert len(rounds) == 1
    rt = rounds[0]
    assert rt["symbol"] == "600519.SH"
    assert rt["open_date"] == "2026-10-10" and rt["close_date"] == "2026-10-13"
    assert rt["qty"] == 100
    # 盈亏 = 卖出净额 (110*100-6) - 买入成本 (100*100+5) = 10994 - 10005 = 989
    assert rt["pnl"] == 989.0
    assert rt["holding_days"] == 3


def test_compare_without_quote_service_uses_settled(client: TestClient, paper_account_id: str):
    """无 quote_service (或快照失败) 时 live 字段为空, 定版口径不受影响。"""
    _mk_account(client, paper_account_id)
    rows = {r["account"]: r for r in client.get("/api/paper/compare").json()["accounts"]}
    row = rows[paper_account_id]
    assert row["live_total"] is None and row["estimating"] is False


def test_compare_with_live_snapshot(
    client: TestClient, tmp_path: Path, paper_account_id: str, paper_pg
):
    """当日快照就绪 → live_total 按快照价估算 (盘中口径), 定版字段并存。"""
    _mk_account(client, paper_account_id)
    paper_pg.append_fill(
        paper_account_id,
        {
            "ts": "2026-10-13T10:00:00+08:00",
            "date": "2026-10-13",
            "order_id": "o1",
            "symbol": "600519.SH",
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 100.0,
            "fee": 5.0,
            "kind": "fill",
        },
    )
    paper.rebuild_positions(tmp_path, paper_account_id)

    # mock quote_service: 当日 enriched 快照, 现价 120 → 估算总资产 = 现金 + 100*120
    enriched = pl.DataFrame({"symbol": ["600519.SH"], "raw_close": [120.0]})
    cast(Any, client.app).state.quote_service = SimpleNamespace(
        get_enriched_today=lambda: (enriched, cn_today())
    )

    rows = {r["account"]: r for r in client.get("/api/paper/compare").json()["accounts"]}
    row = rows[paper_account_id]
    assert row["estimating"] is True
    # 现金 1000000 - 10005 = 989995; 市值 100*120 = 12000 → live_total 1001995
    assert row["live_total"] == pytest.approx(1001995.0, abs=0.01)
    assert row["live_pnl_pct"] == pytest.approx(0.1995, abs=0.001)
