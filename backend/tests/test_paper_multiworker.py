"""S4 前置验证：多个 API 实例（模拟多 worker）并发写同一 PG 模拟盘账户。

同进程内两个独立 FastAPI app 各有自己的 `_repo` / 状态，但共享 `PAPER_PG_URL`，
与多 worker 的关键差异（存储层并发）由 test_paper_pg.py 的真实多进程测试覆盖。
本文件验证 API 路由层并发下单/规则写入不留丢失、状态最终一致。

DES-005: 账户事实源在 PG，唯一 account_id 隔离，用例结束清理。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.paper import router
from app.storage import paper_pg as pg


@pytest.fixture()
def two_clients(tmp_path: Path, paper_account_id: str):
    def make_client() -> TestClient:
        app = FastAPI()
        app.include_router(router)
        app.state.repo = SimpleNamespace(
            store=SimpleNamespace(data_dir=tmp_path),
            resolve_asset_type=lambda s: "stock",
        )
        return TestClient(app)

    pg.create_account(
        paper_account_id,
        {
            "id": paper_account_id,
            "name": "并发账户",
            "initial_cash": 1_000_000.0,
            "cash": 1_000_000.0,
            "commission_pct": 0.00025,
            "stamp_tax_pct": 0.001,
            "slippage_bps": 5.0,
            "queue_limit_orders": False,
            "status": "active",
            "created_at": "2026-11-19T00:00:00+00:00",
        },
    )
    return make_client(), make_client()


def test_two_instances_concurrent_orders_no_loss(tmp_path, paper_account_id, two_clients):
    c1, c2 = two_clients
    q = f"?account={paper_account_id}"

    def place(index: int):
        client = c1 if index % 2 == 0 else c2
        return client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "buy",
                "qty": 100 * (index + 1),
                "ref_price": 10.0,
                "client_order_id": f"conc-{index}",
            },
        ).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = list(pool.map(place, range(8)))
    assert codes == [200] * 8
    orders = pg.load_orders(paper_account_id)
    assert len(orders) == 8
    assert {o["qty"] for o in orders.values()} == {100 * (i + 1) for i in range(8)}


def test_two_instances_concurrent_rules_no_loss(tmp_path, paper_account_id, two_clients):
    c1, c2 = two_clients

    def create(index: int):
        client = c1 if index % 2 == 0 else c2
        return client.post(
            f"/api/paper/auto_rules?account={paper_account_id}",
            json={
                "name": f"规则{index}",
                "match_kind": "strategy",
                "match_id": f"s{index}",
                "side": "buy",
                "size_mode": "fixed_amount",
                "size_value": 1000,
            },
        ).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = list(pool.map(create, range(6)))
    assert codes == [200] * 6
    assert len(pg.load_auto_rules(paper_account_id)) == 6


def test_two_instances_settings_last_write_wins(tmp_path, paper_account_id, two_clients):
    c1, c2 = two_clients
    q = f"?account={paper_account_id}"
    r1 = c1.post(f"/api/paper/settings{q}", json={"slippage_bps": 3})
    r2 = c2.post(f"/api/paper/settings{q}", json={"slippage_bps": 9})
    assert r1.status_code == 200 and r2.status_code == 200
    acc = pg.get_account(paper_account_id)
    assert acc is not None
    assert acc["slippage_bps"] in (3, 9)  # 无丢失/无半写入，后写覆盖先写
