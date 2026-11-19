"""虚拟账户(模拟盘)域模块测试 — 撮合口径 / 账务一致性 / 管道幂等。

设计文档: docs/paper-trading-plan.md。价格全程不复权 raw 价;
台账 append-only, 持仓/现金由重放推导, 修复以追加记录表达。

DES-005: 账户/订单/台账/净值/规则按 account_id 存 PG，所有账户态用例
通过 conftest 的 paper_account_id(s) fixture 取唯一 id 隔离。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import polars as pl
import pytest

from app.market_time import CN_TZ
from app.strategy import paper
from app.tickflow.repository import DataStore, KlineRepository

SYM = "600519.SH"


def _cap_account(tmp_path, account_id: str, cash: float = 1_000_000.0) -> dict:
    return paper.create_account(tmp_path, cash, account_id=account_id)


def _write_daily(tmp_path, rows: list[tuple[date, float, float]]) -> None:
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


def _write_factor(
    tmp_path, day: date, factor: float, asset_type: str = "stock", ck_runtime=None
) -> None:
    sub = "adj_factor_etf" if asset_type == "etf" else "adj_factor"
    out = tmp_path / sub / "all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {"symbol": [SYM], "trade_date": [day], "ex_factor": [factor]},
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64},
    )
    if ck_runtime is not None:
        # 因子读路径已切 CK: 同步写入带前缀的 CK 表
        ck_runtime.write_adj_factor(df, etf=asset_type == "etf")
    df.write_parquet(out)


# ── 纯函数口径 ──────────────────────────────────────────
def test_fees_min_commission_and_stamp_tax_sell_only():
    # 佣金最低 5 元: 100 股 x 10 元 x 万2.5 = 0.25 → 取 5
    assert paper.buy_fee(100, 10.0, 0.00025) == 5.0
    # 大额按比例: 10000 股 x 10 元 = 10万 x 万2.5 = 25
    assert paper.buy_fee(10000, 10.0, 0.00025) == 25.0
    # 印花税仅卖出: 25 + 10万 x 千1 = 125
    assert paper.sell_fee(10000, 10.0, 0.00025, 0.001) == 25.0 + 100.0
    assert paper.buy_fee(10000, 10.0, 0.001) == 100.0


def test_slippage_direction_adverse():
    assert paper.apply_slippage(10.0, "buy", 5) > 10.0
    assert paper.apply_slippage(10.0, "sell", 5) < 10.0
    assert paper.apply_slippage(10.0, "buy", 5) == pytest.approx(10.005)


def test_limit_prices_by_board():
    assert paper.limit_pct("600519.SH", "stock") == 0.10
    assert paper.limit_pct("000001.SZ", "stock") == 0.10
    assert paper.limit_pct("300750.SZ", "stock") == 0.20
    assert paper.limit_pct("688981.SH", "stock") == 0.20
    assert paper.limit_pct("832000.BJ", "stock") == 0.30
    assert paper.limit_pct("510300.SH", "etf") == 0.10
    up, down = paper.limit_prices(10.0, "600519.SH", "stock")
    assert (up, down) == (11.0, 9.0)
    up3, down3 = paper.limit_prices(1.0, "510300.SH", "etf")
    assert (up3, down3) == (1.1, 0.9)  # 3 位小数 round 不改变该值


def test_qty_from_amount_floors_to_lot():
    assert paper.qty_from_amount(9_999.0, 100.0) == 0  # 不足一手
    assert paper.qty_from_amount(10_500.0, 100.0) == 100
    assert paper.qty_from_amount(99_000.0, 100.0) == 900


# ── 下单校验 ────────────────────────────────────────────
def test_create_order_requires_account(tmp_path, paper_account_id: str):
    with pytest.raises(ValueError, match="尚未创建"):
        paper.create_order(tmp_path, SYM, "buy", qty=100, account_id=paper_account_id)


def test_create_order_rejects_bad_qty_and_insufficient_cash(tmp_path, paper_account_id: str):
    aid = paper_account_id
    _cap_account(tmp_path, aid, 100_000.0)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=150, account_id=aid)  # 非百股整数倍
    assert err is not None
    assert "100 的整数倍" in err
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=2000, ref_price=100.0, account_id=aid)
    assert err is not None
    assert "资金不足" in err


def test_create_order_etf_market_converts_to_next_open(tmp_path, paper_account_id: str):
    _cap_account(tmp_path, paper_account_id)
    order, err = paper.create_order(
        tmp_path,
        "510300.SH",
        "buy",
        qty=1000,
        order_type="market",
        asset_type="etf",
        ref_price=4.0,
        account_id=paper_account_id,
    )
    assert err is None
    assert order is not None
    assert order["order_type"] == "next_open"


def test_sell_requires_holding_and_t1(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path, aid)
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert err is not None
    assert "不能卖出" in err

    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    buy_order, err = paper.create_order(
        tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid
    )
    assert err is None
    assert buy_order is not None
    assert buy_order["status"] == "pending"
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=aid)) == 1
    # T+1: 当日买入当日不可卖
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert err is not None
    assert "可卖数量不足" in err

    # 次日可卖
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path, account_id=aid)
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert err is None and sell["status"] == "pending"  # type: ignore[index]


def test_cancel_pending_only(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path, aid)
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid)
    assert order is not None
    cancelled, err = paper.cancel_order(tmp_path, order["id"], account_id=aid)
    assert cancelled is not None
    assert err is None and cancelled["status"] == "cancelled"
    _, err = paper.cancel_order(tmp_path, order["id"], account_id=aid)
    assert err is not None
    assert "不可撤销" in err


# ── 撮合流程 ────────────────────────────────────────────
def test_immediate_fill_cash_and_positions_consistent(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)

    order, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0, account_id=aid)
    assert err is None
    events = paper.evaluate_intraday(tmp_path, {SYM: 10.0, "000002.SZ": 5.0}, account_id=aid)
    assert len(events) == 1
    # V3 成交事件: 字段对齐监控告警 (source=paper), ts 为毫秒, 文案中文可读
    ev = events[0]
    assert ev["source"] == "paper" and ev["type"] == "fill" and ev["severity"] == "info"
    assert ev["symbol"] == SYM and ev["side"] == "buy" and ev["qty"] == 1000
    assert ev["price"] == pytest.approx(10.005)
    assert isinstance(ev["ts"], int) and ev["ts"] > 0
    assert "买入成交" in ev["message"] and "1000股" in ev["message"]
    assert order is not None
    filled = paper.get_order(tmp_path, order["id"], account_id=aid)
    # 滑点向上: 10 * 1.0005
    assert filled is not None
    assert filled["fill_price"] == pytest.approx(10.005)
    fee = paper.buy_fee(1000, 10.005, paper.DEFAULT_COMMISSION_PCT)
    assert filled["fees"] == pytest.approx(fee)

    positions, cash = paper.replay_positions(tmp_path, account_id=aid)
    assert cash == pytest.approx(1_000_000 - 1000 * 10.005 - fee)
    assert positions[SYM]["qty"] == 1000
    assert positions[SYM]["avg_cost"] == pytest.approx((1000 * 10.005 + fee) / 1000)
    # 台账重放 == 物化持仓 (账务一致性)
    mat = paper.load_positions(tmp_path, account_id=aid)[SYM]
    assert mat["qty"] == positions[SYM]["qty"]
    assert mat["avg_cost"] == pytest.approx(positions[SYM]["avg_cost"])


def test_limit_up_buy_rejected(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid)
    # 主板涨停 11.0; 快照 11 + 滑点 ≥ 涨停 → 拒单
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}, account_id=aid) == []
    assert order is not None
    got = paper.get_order(tmp_path, order["id"], account_id=aid)
    assert got is not None
    assert got["status"] == "expired"
    assert "涨停" in got["reason"]


def test_limit_down_sell_rejected(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid)
    assert err is None
    paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=aid)
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path, account_id=aid)
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert err is None
    # 跌停 9.0; 快照 9 减滑点后不高于跌停价 -> 拒单
    assert paper.evaluate_intraday(tmp_path, {SYM: 9.0}, account_id=aid) == []
    got = paper.get_order(tmp_path, sell["id"], account_id=aid)  # type: ignore[index]
    assert got is not None
    assert got["status"] == "expired" and "跌停" in got["reason"]


def test_settle_next_open_close_and_postpone_expire(tmp_path, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    _cap_account(tmp_path, aid)
    _write_daily(
        tmp_path,
        [
            (day - timedelta(days=1), 10.0, 10.0),
            (day, 10.2, 10.8),  # 涨跌幅内 (±10%): 开 10.2 收 10.8
        ],
    )
    no_open, _ = paper.create_order(
        tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=10.0, account_id=aid
    )
    close_ord, _ = paper.create_order(
        tmp_path, SYM, "buy", qty=100, order_type="close", ref_price=10.0, account_id=aid
    )
    summary = paper.settle_day(tmp_path, day.isoformat(), account_id=aid)
    assert summary["filled"] == 2
    assert no_open is not None
    no_open_order = paper.get_order(tmp_path, no_open["id"], account_id=aid)
    assert no_open_order is not None
    assert no_open_order["fill_price"] == pytest.approx(10.2 * 1.0005)  # type: ignore[index]
    assert close_ord is not None
    assert paper.get_order(tmp_path, close_ord["id"], account_id=aid)[
        "fill_price"
    ] == pytest.approx(10.8 * 1.0005)  # type: ignore[index]

    # 停牌顺延 → 连续无行情 4 日过期 (MAX_POSTPONE_DAYS=3)
    stuck, _ = paper.create_order(
        tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=11.0, account_id=aid
    )
    for i in range(4):
        s = paper.settle_day(tmp_path, (day + timedelta(days=i + 1)).isoformat(), account_id=aid)
    assert stuck is not None
    got = paper.get_order(tmp_path, stuck["id"], account_id=aid)
    assert got is not None
    assert got["status"] == "expired" and "自动过期" in got["reason"]
    assert s["expired"] >= 1


def test_pending_sell_occupies_available_quota(tmp_path, monkeypatch, paper_account_id: str):
    """超卖防护: pending 卖出单占用可卖额度, 第二张超额卖出单在下单时被拒。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=200, ref_price=10.0, account_id=aid)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=aid)) == 1
    # 次日: 可卖 200
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path, account_id=aid)
    s1, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert s1 is not None
    assert err is None and s1["status"] == "pending"
    # 已 pending 100, 再挂 200 超出可卖 200 → 拒 (基础可卖校验或占用检查, 双闸任一)
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=200, account_id=aid)
    assert err is not None and ("可卖" in err or "占用" in err)
    # 撤单后额度释放, 可以再挂
    paper.cancel_order(tmp_path, s1["id"], account_id=aid)
    s2, err = paper.create_order(tmp_path, SYM, "sell", qty=100, account_id=aid)
    assert s2 is not None
    assert err is None and s2["status"] == "pending"


def test_pending_buy_occupies_cash_at_fill(tmp_path, monkeypatch, paper_account_id: str):
    """资金占用兜底: 两张 pending 买入, 第一张成交后现金不够第二张 → 第二张拒单留痕。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(
        tmp_path,
        [
            (day - timedelta(days=1), 10.0, 10.0),
            (day, 10.0, 10.0),  # 当日有行情, close 单才会在 settle 撮合
        ],
    )
    _cap_account(tmp_path, aid, 10_000.0)  # 只够一张 500 股 x 10 元 + 费用
    a, _ = paper.create_order(
        tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0, account_id=aid
    )
    b, err = paper.create_order(
        tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0, account_id=aid
    )
    assert err is None  # 预检按单张各自通过, 由撮合侧 (资金/占用双闸) 兜底
    paper.settle_day(tmp_path, day.isoformat(), account_id=aid)
    assert a is not None
    assert paper.get_order(tmp_path, a["id"], account_id=aid)["status"] == "filled"  # type: ignore[index]
    assert b is not None
    got_b = paper.get_order(tmp_path, b["id"], account_id=aid)
    # b 必须被拒且留痕: 现金已被 a 消耗, 基础资金校验或占用检查任一拦下都算防线生效
    assert got_b["status"] == "expired"  # type: ignore[index]
    assert got_b["reason"] is not None  # type: ignore[index]
    # 台账只有一张成交, 现金不透支
    fills = [
        f for f in paper.load_fills(tmp_path, account_id=aid) if f.get("kind", "fill") == "fill"
    ]
    assert len(fills) == 1
    _, cash = paper.replay_positions(tmp_path, account_id=aid)
    assert cash >= 0


def test_position_symbol_cap(tmp_path, monkeypatch, paper_account_id: str):
    """持仓标的数上限: 第 51 只新开仓买入被拒 (加仓已有持仓不受限)。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid, 100_000_000.0)
    # 人为塞满 50 只持仓 (直接写台账, 绕过 100 股 x 价格的资金约束)
    for i in range(50):
        paper._append_fill(
            tmp_path,
            {
                "seq": i,
                "ts": "",
                "date": (day - timedelta(days=1)).isoformat(),
                "order_id": f"seed{i}",
                "symbol": f"{600000 + i:06d}.SH",
                "asset_type": "stock",
                "side": "buy",
                "qty": 100,
                "price": 1.0,
                "fee": 1.0,
                "kind": "fill",
            },
            account_id=aid,
        )
    paper._materialize(tmp_path, account_id=aid)
    # 新开仓第 51 只 → 拒
    _, err = paper.create_order(
        tmp_path, "601999.SH", "buy", qty=100, ref_price=10.0, account_id=aid
    )
    assert err is not None
    assert "上限" in err
    # 加仓已有持仓 → 放行
    ok, err = paper.create_order(
        tmp_path, "600000.SH", "buy", qty=100, ref_price=10.0, account_id=aid
    )
    assert ok is not None
    assert err is None and ok["status"] == "pending"


def test_settle_idempotent_no_double_fill(tmp_path, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    _cap_account(tmp_path, aid)
    _write_daily(tmp_path, [(day, 10.0, 10.5)])
    paper.create_order(
        tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=10.0, account_id=aid
    )
    paper.settle_day(tmp_path, day.isoformat(), account_id=aid)
    paper.settle_day(tmp_path, day.isoformat(), account_id=aid)  # 重跑同日
    fills = [
        f for f in paper.load_fills(tmp_path, account_id=aid) if f.get("kind", "fill") == "fill"
    ]
    assert len(fills) == 1
    _, cash = paper.replay_positions(tmp_path, account_id=aid)
    assert cash == pytest.approx(1_000_000 - fills[0]["qty"] * fills[0]["price"] - fills[0]["fee"])


def test_corporate_action_adjusts_position_and_idempotent(
    tmp_path, ck_runtime, paper_account_id: str
):
    day = date(2026, 9, 24)
    aid = paper_account_id
    _cap_account(tmp_path, aid)
    _write_daily(
        tmp_path,
        [
            (day - timedelta(days=1), 10.0, 10.0),
            (day, 8.0, 8.0),  # 除权后价格
        ],
    )
    order, _ = paper.create_order(
        tmp_path, SYM, "buy", qty=1000, order_type="close", ref_price=10.0, account_id=aid
    )
    paper.settle_day(tmp_path, (day - timedelta(days=1)).isoformat(), account_id=aid)
    assert paper.load_positions(tmp_path, account_id=aid)[SYM]["qty"] == 1000

    _write_factor(tmp_path, day, 1.25, ck_runtime=ck_runtime)
    paper.settle_day(tmp_path, day.isoformat(), account_id=aid)
    pos = paper.load_positions(tmp_path, account_id=aid)[SYM]
    assert pos["qty"] == pytest.approx(1250)
    # 成本被因子摊薄: 总成本不变 (买入费已摊入), 数量乘 1.25 后 avg_cost 除以 1.25
    assert order is not None
    order_after = paper.get_order(tmp_path, order["id"], account_id=aid)
    assert order_after is not None
    total_cost = 1000 * order_after["fill_price"] + order_after["fees"]
    replayed = paper.replay_positions(tmp_path, account_id=aid)[0][SYM]
    assert replayed["avg_cost"] * 1250 == pytest.approx(total_cost, rel=1e-4)

    # 重跑同日: 不二次乘因子
    paper.settle_day(tmp_path, day.isoformat(), account_id=aid)
    assert paper.load_positions(tmp_path, account_id=aid)[SYM]["qty"] == pytest.approx(1250)
    corp = [f for f in paper.load_fills(tmp_path, account_id=aid) if f.get("kind") == "corp_action"]
    assert len(corp) == 1


def test_nav_and_overview_math(tmp_path, monkeypatch, paper_account_id: str):
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path, aid, 100_000.0)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0, account_id=aid)
    assert err is None
    paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=aid)
    fill = next(
        f for f in paper.load_fills(tmp_path, account_id=aid) if f.get("kind", "fill") == "fill"
    )

    # 盘中估算: 价格 11 → 市值 11000
    ov = paper.overview(tmp_path, {SYM: 11.0}, account_id=aid)
    assert ov["estimating"] is True
    assert ov["market_value"] == pytest.approx(11000.0)
    assert ov["cash"] == pytest.approx(100_000 - fill["qty"] * fill["price"] - fill["fee"])
    assert ov["total"] == pytest.approx(ov["cash"] + ov["market_value"])

    # 定版净值: 写入 PG 当日行, 同日重写幂等
    nav = paper.daily_nav(tmp_path, day.isoformat(), {SYM: 10.5}, account_id=aid)
    assert nav is not None
    paper._write_nav_line(tmp_path, day.isoformat(), nav, account_id=aid)
    paper._write_nav_line(tmp_path, day.isoformat(), nav, account_id=aid)
    lines = paper.load_nav(tmp_path, account_id=aid)
    assert len(lines) == 1 and lines[0]["nav"] == pytest.approx(nav["nav"])


def test_round_trips_fifo_stats(tmp_path, paper_account_id: str):
    day = date(2026, 9, 1)
    aid = paper_account_id
    _cap_account(tmp_path, aid)
    _write_daily(tmp_path, [(day, 10.0, 10.0), (day + timedelta(days=1), 12.0, 12.0)])
    # 手工构造台账 (纯统计口径测试, 绕过撮合): 两次买入一次卖出
    for f in (
        {
            "seq": 1,
            "ts": "",
            "date": day.isoformat(),
            "order_id": "o1",
            "symbol": SYM,
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 10.0,
            "fee": 5.0,
            "kind": "fill",
        },
        {
            "seq": 2,
            "ts": "",
            "date": day.isoformat(),
            "order_id": "o2",
            "symbol": SYM,
            "asset_type": "stock",
            "side": "buy",
            "qty": 100,
            "price": 11.0,
            "fee": 5.0,
            "kind": "fill",
        },
        {
            "seq": 3,
            "ts": "",
            "date": (day + timedelta(days=1)).isoformat(),
            "order_id": "o3",
            "symbol": SYM,
            "asset_type": "stock",
            "side": "sell",
            "qty": 150,
            "price": 12.0,
            "fee": 5.0 + 18.0,
            "kind": "fill",
        },
    ):
        paper._append_fill(tmp_path, f, account_id=aid)
    rounds = paper.round_trips(tmp_path, account_id=aid)
    assert len(rounds) == 2
    # FIFO: 先平 10 元批次 100 股, 再平 11 元批次 50 股
    assert rounds[0]["qty"] == 100 and rounds[0]["open_date"] == day.isoformat()
    assert rounds[0]["pnl"] == pytest.approx(
        12.0 * 100 - (10.0 * 100 + 5.0) - (5.0 + 18.0) * 100 / 150, rel=1e-3
    )
    assert rounds[1]["qty"] == 50
    st = paper.stats(tmp_path, account_id=aid)
    assert st["rounds"] == 2 and st["win_rate"] == 100.0


# ── 自动跟单 (V2) ───────────────────────────────────────
def _auto_rule(**overrides) -> dict:
    base = {
        "name": "跟单测试",
        "match_kind": "strategy",
        "match_id": "strat_1",
        "side": "buy",
        "size_mode": "fixed_amount",
        "size_value": 5000.0,
        "order_type": "next_open",
        "cooldown_days": 5,
        "enabled": True,
    }
    base.update(overrides)
    return base


def test_auto_rule_crud_and_validation(tmp_path, paper_account_id: str):
    from app.strategy import paper_auto

    aid = paper_account_id
    with pytest.raises(ValueError, match="不能为空"):
        paper_auto.create_auto_rule(tmp_path, _auto_rule(name=""), account_id=aid)
    with pytest.raises(ValueError, match="match_kind"):
        paper_auto.create_auto_rule(tmp_path, _auto_rule(match_kind="xxx"), account_id=aid)
    with pytest.raises(ValueError, match="不能超过 100"):
        paper_auto.create_auto_rule(
            tmp_path, _auto_rule(size_mode="pct_equity", size_value=150), account_id=aid
        )
    rule = paper_auto.create_auto_rule(tmp_path, _auto_rule(), account_id=aid)
    assert rule["id"].startswith("arule_")
    assert len(paper_auto.load_auto_rules(tmp_path, account_id=aid)) == 1
    paper_auto.set_enabled(tmp_path, rule["id"], False, account_id=aid)
    assert paper_auto.load_auto_rules(tmp_path, account_id=aid, enabled_only=True) == []
    assert paper_auto.delete_auto_rule(tmp_path, rule["id"], account_id=aid) is True
    assert paper_auto.delete_auto_rule(tmp_path, rule["id"], account_id=aid) is False


def test_auto_trigger_matches_strategy_event(tmp_path, monkeypatch, paper_account_id: str):
    from app.strategy import paper_auto

    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)
    paper_auto.create_auto_rule(tmp_path, _auto_rule(), account_id=aid)

    ev = {
        "source": "strategy",
        "strategy_id": "strat_1",
        "rule_id": "r1",
        "symbol": SYM,
        "price": 10.0,
    }
    orders = paper_auto.on_rule_events(tmp_path, [ev], account_id=aid)
    assert len(orders) == 1
    assert orders[0]["qty"] == 500  # 5000 元 / 10 元 → 500 股
    assert orders[0]["source"].startswith("auto:")
    assert orders[0]["order_type"] == "next_open"

    # 冷却期内同 symbol 不再触发
    assert paper_auto.on_rule_events(tmp_path, [ev], account_id=aid) == []
    # 冷却外 (6 天后) 恢复触发: 直接改订单 created_at 模拟
    orders_loaded = paper.load_orders(tmp_path, account_id=aid)
    for o in orders_loaded:
        o["created_at"] = "2026-09-18T10:00:00+08:00"
        paper.save_order(tmp_path, o, account_id=aid)
    orders2 = paper_auto.on_rule_events(tmp_path, [ev], account_id=aid)
    assert len(orders2) == 1


def test_auto_trigger_non_matching_events_ignored(tmp_path, paper_account_id: str):
    from app.strategy import paper_auto

    aid = paper_account_id
    _cap_account(tmp_path, aid)
    paper_auto.create_auto_rule(tmp_path, _auto_rule(), account_id=aid)
    # strategy_id 不匹配
    ev1 = {"source": "strategy", "strategy_id": "other", "symbol": SYM, "price": 10.0}
    # 无 symbol (批量事件)
    ev2 = {"source": "strategy", "strategy_id": "strat_1", "symbol": "", "price": 10.0}
    # 无价格
    ev3 = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM}
    assert paper_auto.on_rule_events(tmp_path, [ev1, ev2, ev3], account_id=aid) == []


def test_auto_rule_disabled_not_triggered(tmp_path, paper_account_id: str):
    from app.strategy import paper_auto

    aid = paper_account_id
    _cap_account(tmp_path, aid)
    rule = paper_auto.create_auto_rule(tmp_path, _auto_rule(enabled=False), account_id=aid)
    ev = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM, "price": 10.0}
    assert paper_auto.on_rule_events(tmp_path, [ev], account_id=aid) == []
    paper_auto.set_enabled(tmp_path, rule["id"], True, account_id=aid)
    assert len(paper_auto.on_rule_events(tmp_path, [ev], account_id=aid)) == 1


def test_auto_frozen_account_rejects(tmp_path, monkeypatch, paper_account_id: str):
    from app.strategy import paper_auto

    aid = paper_account_id
    acc = _cap_account(tmp_path, aid)
    acc["status"] = "frozen"
    paper.save_account(tmp_path, acc, account_id=aid)
    paper_auto.create_auto_rule(tmp_path, _auto_rule(), account_id=aid)
    ev = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM, "price": 10.0}
    assert paper_auto.on_rule_events(tmp_path, [ev], account_id=aid) == []


def test_max_drawdown_pure():
    assert paper.max_drawdown([]) is None
    assert paper.max_drawdown([100]) == 0.0
    assert paper.max_drawdown([100, 110, 99, 105]) == pytest.approx(0.1)
    assert paper.max_drawdown([100, 120, 60, 90]) == pytest.approx(0.5)
    assert paper.max_drawdown([100, 100, 100]) == 0.0


# ── 涨跌停排队 (V2: queue_limit_orders) ─────────────────
def test_limit_queue_default_off_rejects_immediately(tmp_path, monkeypatch, paper_account_id: str):
    """默认关闭: 触及涨停直接过期 (与 V1 行为一致, 由 test_limit_up_buy_rejected 锁定)。"""
    acc = _cap_account(tmp_path, paper_account_id)
    assert acc["queue_limit_orders"] is False


def test_limit_queue_retries_next_open_then_expires(tmp_path, monkeypatch, paper_account_id: str):
    """排队开启: 涨停买不进 → 转 next_open 次日重试, 连续涨停计顺延, 超限过期。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    # 盘中 10:00 下单并排队: 当日开盘价在排队前已打印, 当日结算不成交, 从次日开盘起重试
    monkeypatch.setattr(paper, "cn_now", lambda: datetime.combine(day, time(10, 0), tzinfo=CN_TZ))
    paper.create_account(tmp_path, 1_000_000, queue_limit_orders=True, account_id=aid)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid)

    # day1 盘中触及涨停 11.0 → 不再过期, 转 next_open 排队
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}, account_id=aid) == []
    assert order is not None
    got = paper.get_order(tmp_path, order["id"], account_id=aid)
    assert got is not None
    assert got["status"] == "pending" and got["order_type"] == "next_open"
    assert got["postponed"] == 1 and "排队" in got["reason"]

    # day2..day4 开盘连续一字涨停 (open == 涨停价) → 顺延到 3, 第 4 次超限过期
    _write_daily(
        tmp_path,
        [
            (day, 12.10, 12.10),  # day1 收盘 (排队当日, 结算跳过)
            (day + timedelta(days=1), 13.31, 13.31),  # vs 12.10 涨停 13.31
            (day + timedelta(days=2), 14.64, 14.64),  # vs 13.31 涨停 14.64
            (day + timedelta(days=3), 16.10, 16.10),  # vs 14.64 涨停 16.10 → 第 4 次
        ],
    )
    for i in range(4):
        paper.settle_day(tmp_path, (day + timedelta(days=i)).isoformat(), account_id=aid)
    got = paper.get_order(tmp_path, order["id"], account_id=aid)
    assert got is not None
    assert got["status"] == "expired" and "排队" in got["reason"]
    assert got["postponed"] == paper.MAX_POSTPONE_DAYS


def test_limit_queue_fills_when_open_below_limit(tmp_path, monkeypatch, paper_account_id: str):
    """排队开启后次日开盘回落 → 按 next_open 正常成交。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    monkeypatch.setattr(paper, "cn_now", lambda: datetime.combine(day, time(10, 0), tzinfo=CN_TZ))
    paper.create_account(tmp_path, 1_000_000, queue_limit_orders=True, account_id=aid)
    _write_daily(
        tmp_path,
        [
            (day - timedelta(days=1), 10.0, 10.0),
            (day, 10.6, 11.0),  # 排队当日 (盘中触及涨停 11.0)
            (day + timedelta(days=1), 10.5, 10.9),  # 次日开盘 10.5 未涨停
        ],
    )
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0, account_id=aid)
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}, account_id=aid) == []  # 涨停排队
    assert order is not None
    assert paper.get_order(tmp_path, order["id"], account_id=aid)["status"] == "pending"  # type: ignore[index]
    assert (
        paper.settle_day(tmp_path, day.isoformat(), account_id=aid)["filled"] == 0
    )  # 当日开盘价在排队前已打印
    summary = paper.settle_day(tmp_path, (day + timedelta(days=1)).isoformat(), account_id=aid)
    assert summary["filled"] == 1
    got = paper.get_order(tmp_path, order["id"], account_id=aid)
    assert got is not None
    assert got["status"] == "filled" and got["fill_price"] == pytest.approx(
        paper.apply_slippage(10.5, "buy", 5.0)
    )


# ── 多账户 (V2) ─────────────────────────────────────────
def test_multi_account_isolation(tmp_path, monkeypatch, paper_account_ids):
    """两账户完全隔离: 订单/持仓/现金/统计互不可见。"""
    day = date(2026, 9, 24)
    main_id, other_id = paper_account_ids(2)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    paper.create_account(tmp_path, 1_000_000, account_id=main_id, name="主账户")
    paper.create_account(tmp_path, 500_000, account_id=other_id, name="策略A")

    # 同一标的分别在两个账户各买一笔
    assert paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=main_id) == []  # 无订单
    o1, err = paper.create_order(tmp_path, SYM, "buy", qty=100, account_id=main_id)
    o2, err = paper.create_order(tmp_path, SYM, "buy", qty=200, account_id=other_id)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=main_id)) == 1
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=other_id)) == 1

    # 订单互不可见
    assert [o["id"] for o in paper.load_orders(tmp_path, main_id)] == [o1["id"]]  # type: ignore[index]
    assert [o["id"] for o in paper.load_orders(tmp_path, other_id)] == [o2["id"]]  # type: ignore[index]
    # 持仓互不可见
    assert paper.load_positions(tmp_path, main_id)[SYM]["qty"] == 100
    assert paper.load_positions(tmp_path, other_id)[SYM]["qty"] == 200
    # 现金互不可见 (含费用扣减不同)
    ov1 = paper.overview(tmp_path, account_id=main_id)
    ov2 = paper.overview(tmp_path, account_id=other_id)
    assert ov1["cash"] > ov2["cash"]
    assert ov1["account_name"] == "主账户" and ov2["account_name"] == "策略A"

    # 账户列表与遍历 (PG 共享库: 只断言本用例的两个账户都在)
    assert {main_id, other_id} <= set(paper.list_account_ids(tmp_path))
    infos = {a["id"]: a for a in paper.list_accounts(tmp_path)}
    assert {main_id, other_id} <= set(infos) and infos[other_id]["name"] == "策略A"

    # settings 只影响目标账户
    paper.update_settings(tmp_path, other_id, queue_limit_orders=True)
    assert paper.get_account(tmp_path, main_id)["queue_limit_orders"] is False  # type: ignore[index]
    assert paper.get_account(tmp_path, other_id)["queue_limit_orders"] is True  # type: ignore[index]

    # 净值隔离
    paper.settle_day(tmp_path, day.isoformat(), account_id=main_id)
    paper.settle_day(tmp_path, day.isoformat(), account_id=other_id)
    nav1 = paper.load_nav(tmp_path, main_id)
    nav2 = paper.load_nav(tmp_path, other_id)
    assert len(nav1) == 1 and len(nav2) == 1 and nav1[0]["nav"] != nav2[0]["nav"]


def test_account_id_validation_blocks_traversal(tmp_path):
    import pytest as _pytest

    for bad in ("../evil", "a/b", "", "x" * 33, "a b"):
        with _pytest.raises(ValueError):
            paper.get_account(tmp_path, bad)
        with _pytest.raises(ValueError):
            paper.create_account(tmp_path, 100.0, account_id=bad)


# ── V3 补全: 结算成交留痕 / 自动跟单下单事件 ──────────
def test_day_fill_events_returns_todays_fills(tmp_path, monkeypatch, paper_account_id: str):
    """day_fill_events: 只取指定交易日、kind=fill 的台账行, 转 AlertEvent 同构事件。"""
    day = date(2026, 9, 24)
    aid = paper_account_id
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, aid)

    # 当日无成交 → 空列表
    assert paper.day_fill_events(tmp_path, day.isoformat(), account_id=aid) == []

    _, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0, account_id=aid)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id=aid)) == 1

    events = paper.day_fill_events(tmp_path, day.isoformat(), account_id=aid)
    assert len(events) == 1
    ev = events[0]
    assert ev["source"] == "paper" and ev["type"] == "fill" and ev["severity"] == "info"
    assert ev["symbol"] == SYM and ev["side"] == "buy" and ev["qty"] == 1000
    assert isinstance(ev["ts"], int) and ev["ts"] > 0
    assert "买入成交" in ev["message"]
    # 幂等 (结算重跑同日不重复追加事件)
    assert paper.day_fill_events(tmp_path, day.isoformat(), account_id=aid) == events


def test_auto_order_events_shape(tmp_path):
    """auto_order_events: 订单 → AlertEvent 同构事件, rule_id 从 source 还原。"""
    from app.strategy import paper_auto

    orders = [
        {
            "symbol": "600000.SH",
            "side": "buy",
            "qty": 500,
            "order_type": "next_open",
            "source": "auto:rule_abc",
        },
        {
            "symbol": "000001.SZ",
            "side": "sell",
            "qty": 200,
            "order_type": "market",
            "source": "manual",
        },
    ]
    events = paper_auto.auto_order_events(orders, account_id="acc1")
    assert len(events) == 2
    buy_ev, sell_ev = events
    assert buy_ev["source"] == "paper" and buy_ev["type"] == "auto_order"
    assert buy_ev["rule_id"] == "rule_abc" and buy_ev["account_id"] == "acc1"
    assert (
        "买入" in buy_ev["message"]
        and "次日开盘" in buy_ev["message"]
        and "500股" in buy_ev["message"]
    )
    assert isinstance(buy_ev["ts"], int) and buy_ev["ts"] > 0
    assert (
        sell_ev["rule_id"] == "" and "卖出" in sell_ev["message"] and "即时" in sell_ev["message"]
    )
    # 空列表 → 空
    assert paper_auto.auto_order_events([]) == []


def test_limit_pct_chinext_302():
    # 创业板 302 号段 (2025-02-17 起的 302132.SZ 中航成飞) 为 ±20%
    assert paper.limit_pct("302132.SZ", "stock") == 0.20


def test_limit_pct_star_689():
    # 科创板 689 号段 (存托凭证, 如 689009.SH 九号公司) 为 ±20%,
    # 与 price_limits.board_limit_pct 一致
    assert paper.limit_pct("689009.SH", "stock") == 0.20
