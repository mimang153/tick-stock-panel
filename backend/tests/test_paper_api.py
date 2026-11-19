"""虚拟账户 API 契约测试 — 成功/无数据/错误响应 (per CONTRIBUTING API 契约矩阵)。

镜像 test_alerts_query_bounds.py: 只挂本路由的裸 FastAPI app, 不经过主应用
全局中间件 (访问密码门是部署层关注点, 不属于本契约)。

DES-005: 账户状态在 PG 中按 account_id 隔离, 用例通过 conftest 的
paper_account_id(s) fixture 取唯一 id, 避免共享库跨用例碰撞。
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.paper import router


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        resolve_asset_type=lambda s: "etf"
        if s.split(".")[0].startswith(("51", "56", "58", "15"))
        else "stock",
    )
    return TestClient(app)


def test_overview_before_init(client: TestClient, paper_account_id: str):
    r = client.get(f"/api/paper/overview?account={paper_account_id}")
    assert r.status_code == 200
    assert r.json() == {"initialized": False, "account_id": paper_account_id}


def test_multi_account_isolation_and_settings(client: TestClient, paper_account_ids):
    """双账户隔离: 订单/概览互不可见; settings 端点切换涨跌停排队。"""
    main_id, other_id = paper_account_ids(2)
    client.post("/api/paper/account", json={"initial_cash": 1000000, "account_id": main_id})
    r = client.post(
        "/api/paper/account", json={"initial_cash": 500000, "account_id": other_id, "name": "策略A"}
    )
    assert r.status_code == 200 and r.json()["account"]["name"] == "策略A"

    # 账户列表 (PG 为共享库: 只断言本用例的两个账户都在)
    ids = {a["id"] for a in client.get("/api/paper/accounts").json()["accounts"]}
    assert {main_id, other_id} <= ids

    # 订单隔离: 同一笔下单只出现在对应账户
    assert (
        client.post(
            f"/api/paper/orders?account={other_id}",
            json={"symbol": "600519.SH", "side": "buy", "qty": 100, "ref_price": 1500.0},
        ).status_code
        == 200
    )
    assert len(client.get(f"/api/paper/orders?account={other_id}").json()["orders"]) == 1
    assert len(client.get(f"/api/paper/orders?account={main_id}").json()["orders"]) == 0

    # 概览隔离: other_id 有 pending 单不影响 main_id 的现金展示
    assert client.get(f"/api/paper/overview?account={other_id}").json()["initial_cash"] == 500000
    assert client.get(f"/api/paper/overview?account={main_id}").json()["initial_cash"] == 1000000

    # settings: 开启涨跌停排队 → 账户字段更新
    r = client.post(f"/api/paper/settings?account={other_id}", json={"queue_limit_orders": True})
    assert r.status_code == 200 and r.json()["account"]["queue_limit_orders"] is True
    assert (
        client.get(f"/api/paper/overview?account={other_id}").json()["queue_limit_orders"] is True
    )
    assert (
        client.get(f"/api/paper/overview?account={main_id}").json()["queue_limit_orders"] is False
    )

    # 非法账户 id → 400 (路径穿越防护)
    assert client.get("/api/paper/overview?account=../x").status_code == 400


def test_account_create_idempotent(client: TestClient, paper_account_id: str):
    body = {"initial_cash": 500000, "account_id": paper_account_id}
    r1 = client.post("/api/paper/account", json=body)
    assert r1.status_code == 200
    r2 = client.post("/api/paper/account", json={**body, "initial_cash": 900000})
    assert r2.status_code == 200
    assert r2.json()["account"]["cash"] == 500000  # 幂等: 不覆盖


def test_account_rejects_non_positive_cash(client: TestClient, paper_account_id: str):
    assert (
        client.post(
            "/api/paper/account",
            json={
                "initial_cash": 0,
                "account_id": paper_account_id,
            },
        ).status_code
        == 400
    )
    assert (
        client.post(
            "/api/paper/account",
            json={
                "initial_cash": -1,
                "account_id": paper_account_id,
            },
        ).status_code
        == 400
    )


def test_order_create_list_cancel_flow(client: TestClient, paper_account_id: str):
    q = f"?account={paper_account_id}"
    client.post(
        "/api/paper/account", json={"initial_cash": 1000000, "account_id": paper_account_id}
    )
    r = client.post(
        f"/api/paper/orders{q}",
        json={
            "symbol": "600519.SH",
            "side": "buy",
            "qty": 100,
            "ref_price": 1500.0,
        },
    )
    assert r.status_code == 200
    order = r.json()["order"]
    assert order["status"] == "pending"

    # 非百股整数倍 → 400
    assert (
        client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "buy",
                "qty": 150,
            },
        ).status_code
        == 400
    )
    # 超资金 → 400
    assert (
        client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "buy",
                "qty": 100000,
                "ref_price": 1500.0,
            },
        ).status_code
        == 400
    )
    # 卖出无持仓 → 400
    assert (
        client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "sell",
                "qty": 100,
            },
        ).status_code
        == 400
    )

    # 列表 + 撤单
    assert len(client.get(f"/api/paper/orders{q}&status=pending").json()["orders"]) == 1
    r = client.delete(f"/api/paper/orders/{order['id']}{q}")
    assert r.status_code == 200 and r.json()["order"]["status"] == "cancelled"
    # 已撤不可再撤
    assert client.delete(f"/api/paper/orders/{order['id']}{q}").status_code == 400


def test_order_amount_mode_auto_converts(client: TestClient, paper_account_id: str):
    q = f"?account={paper_account_id}"
    client.post(
        "/api/paper/account", json={"initial_cash": 1000000, "account_id": paper_account_id}
    )
    r = client.post(
        f"/api/paper/orders{q}",
        json={
            "symbol": "600519.SH",
            "side": "buy",
            "amount": 200000,
            "ref_price": 1500.0,
        },
    )
    assert r.status_code == 200
    assert r.json()["order"]["qty"] == 100  # 20万 / 1500 = 133 股 → 向下取整百 = 100
    # 无参考价的金额单 → 400
    assert (
        client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "buy",
                "amount": 100,
            },
        ).status_code
        == 400
    )


def test_freeze_blocks_new_orders(client: TestClient, paper_account_id: str):
    q = f"?account={paper_account_id}"
    client.post("/api/paper/account", json={"initial_cash": 100000, "account_id": paper_account_id})
    assert client.post(f"/api/paper/freeze{q}&frozen=true").status_code == 200
    r = client.post(
        f"/api/paper/orders{q}",
        json={
            "symbol": "600519.SH",
            "side": "buy",
            "qty": 100,
            "ref_price": 10.0,
        },
    )
    assert r.status_code == 400 and "冻结" in r.json()["detail"]
    # 解冻恢复
    assert client.post(f"/api/paper/freeze{q}&frozen=false").status_code == 200
    assert (
        client.post(
            f"/api/paper/orders{q}",
            json={
                "symbol": "600519.SH",
                "side": "buy",
                "qty": 100,
                "ref_price": 10.0,
            },
        ).status_code
        == 200
    )


def test_freeze_before_init_rejected(client: TestClient, paper_account_id: str):
    assert (
        client.post(f"/api/paper/freeze?account={paper_account_id}&frozen=true").status_code == 400
    )


def test_trades_nav_stats_empty(client: TestClient, paper_account_id: str):
    q = f"?account={paper_account_id}"
    client.post("/api/paper/account", json={"initial_cash": 100000, "account_id": paper_account_id})
    assert client.get(f"/api/paper/trades{q}").json() == {"fills": []}
    assert client.get(f"/api/paper/nav{q}").json() == {"nav": []}
    stats = client.get(f"/api/paper/stats{q}").json()
    assert stats["rounds"] == 0 and stats["win_rate"] == 0.0


def test_settings_updates_fees(client: TestClient, paper_account_id: str):
    """费用三参数可经 settings 端点调整; 超范围 → 400; 未知字段忽略。"""
    q = f"?account={paper_account_id}"
    client.post(
        "/api/paper/account", json={"initial_cash": 1000000, "account_id": paper_account_id}
    )
    r = client.post(
        f"/api/paper/settings{q}",
        json={
            "commission_pct": 0.0003,
            "stamp_tax_pct": 0.0005,
            "slippage_bps": 8,
        },
    )
    assert r.status_code == 200
    acc = r.json()["account"]
    assert acc["commission_pct"] == 0.0003
    assert acc["stamp_tax_pct"] == 0.0005
    assert acc["slippage_bps"] == 8
    # 超范围
    r = client.post(f"/api/paper/settings{q}", json={"commission_pct": 0.5})
    assert r.status_code == 400 and "超出合理范围" in r.json()["detail"]
    # 部分更新: 只改滑点, 佣金不动
    r = client.post(f"/api/paper/settings{q}", json={"slippage_bps": 3})
    acc = r.json()["account"]
    assert acc["slippage_bps"] == 3 and acc["commission_pct"] == 0.0003


def test_compare_accounts(client: TestClient, paper_account_ids):
    """/compare: 逐账户概览+统计+净值; 未初始化的空壳账户不出现。"""
    aid, other = paper_account_ids(2)
    # PG 共享库: 断言本用例账户初始不在 compare 列表
    listed = {r["account"] for r in client.get("/api/paper/compare").json()["accounts"]}
    assert not ({aid, other} & listed)

    client.post(
        "/api/paper/account", json={"initial_cash": 1000000, "account_id": aid, "name": "甲"}
    )
    client.post(
        "/api/paper/account", json={"initial_cash": 500000, "account_id": other, "name": "乙"}
    )
    rows = {r["account"]: r for r in client.get("/api/paper/compare").json()["accounts"]}
    assert {aid, other} <= set(rows)
    row = rows[aid]
    assert row["name"] == "甲" and row["initial_cash"] == 1000000
    assert row["total"] == pytest.approx(1000000)
    assert row["pnl_pct"] == 0.0
    assert row["fees"]["commission_pct"] == pytest.approx(0.00025)
    assert row["rounds"] == 0 and row["win_rate"] == 0.0
    assert row["nav"] == []  # 尚无定版净值
    # 字段完整性 (对比表依赖)
    for key in (
        "cash",
        "market_value",
        "total_pnl",
        "profit_loss_ratio",
        "max_drawdown",
        "avg_holding_days",
        "realized_pnl",
    ):
        assert key in row
