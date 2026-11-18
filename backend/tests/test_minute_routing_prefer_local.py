"""优先本地缓存与健康检查场景。

从原模块复用辅助函数，断言与用例逻辑保持原样。"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from tests.minute_routing_helpers import (
    _healthy_svc,
    _mock_minute_df,
    _mock_minute_rows,
    _mock_tail_rows,
)


def test_sync_minute_single_rejects_index_symbol():
    """指数分钟K无本地存储, 落库会污染股票分钟表; 端点应显式 400 而非 500。"""
    import asyncio

    from fastapi import HTTPException

    from app.api import kline as kline_api

    mock_repo = MagicMock()
    mock_repo.resolve_asset_type.return_value = "index"
    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(kline_api.sync_minute_single(mock_request, {"symbol": "000001.SH"}))
    assert exc_info.value.status_code == 400
    assert "指数" in str(exc_info.value.detail)


# ---------- 测试: prefer_local (自选分时 × 全量分钟健康) ----------


def test_get_minute_batch_prefer_local_healthy_skips_stock_refetch(monkeypatch):
    """全量分钟服务健康 + prefer_local: 股票 incomplete 不再批量补拉,
    本地现有数据(哪怕 <90%)直接返回; ETF 不在服务 universe, 维持补拉。"""
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_df(symbol="510300.SH"))
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = {"510300.SH"}
    # 股票本地 100 根 (expected=240 → <90% 判 incomplete), ETF 本地空
    mock_repo.get_minute_batch.side_effect = lambda syms, d, asset_type="stock": (
        _mock_minute_rows("600519.SH", 100) if asset_type == "stock" else pl.DataFrame()
    )

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, True)

    body = {"symbols": ["600519.SH", "510300.SH"], "date": "2026-01-15", "prefer_local": True}
    result = json.loads(kline_api.get_minute_batch(mock_request, body).body)

    # 股票未被补拉: sync_minute_batch 只为 ETF 调了一次
    assert sync_spy.call_count == 1
    assert sync_spy.call_args.kwargs.get("asset_type") == "etf"
    assert sync_spy.call_args.args[0] == ["510300.SH"]
    # 股票返回的是本地 100 根 (部分数据, 不因 incomplete 而缺失)
    assert len(result["data"]["600519.SH"]) == 100
    assert result["full_minute_local"] is True


def test_get_minute_batch_prefer_local_unhealthy_falls_back(monkeypatch):
    """服务不健康 (挂了/停了) + prefer_local: 回落现状补拉兜底, 行为与不带标志一致。"""
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_df())
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    mock_repo.get_minute_batch.return_value = _mock_minute_rows("600519.SH", 100)

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, False)

    body = {"symbols": ["600519.SH"], "date": "2026-01-15", "prefer_local": True}
    result = json.loads(kline_api.get_minute_batch(mock_request, body).body)

    assert sync_spy.call_count == 1  # 股票照常补拉
    assert sync_spy.call_args.kwargs.get("asset_type") == "stock"
    assert result["full_minute_local"] is False


def test_get_minute_batch_no_flag_unaffected_even_if_healthy(monkeypatch):
    """不带 prefer_local (策略页等) 即使服务健康也维持现状补拉 — 本轮只辐射自选场景。"""
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_df())
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    mock_repo.get_minute_batch.return_value = _mock_minute_rows("600519.SH", 100)

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, True)

    body = {"symbols": ["600519.SH"], "date": "2026-01-15"}
    result = json.loads(kline_api.get_minute_batch(mock_request, body).body)

    assert sync_spy.call_count == 1
    assert result["full_minute_local"] is False


# ---------- 测试: 前部洞 (盘中重启/停机跨开盘的残留) ----------


def test_get_minute_batch_leading_hole_triggers_full_day_refetch(monkeypatch):
    """前部洞: 首根显著晚于开盘的连续尾部K → 全天重拉, 而非"最后一根+1min"增量。

    场景: 盘中重启/停机跨开盘后, 本地只剩 11:20 起的连续尾巴 (11 根)。
    旧逻辑判"仅尾部落后"走增量, 上午的洞永远不会被回看; 新逻辑判洞 → 全天拉。
    """
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_rows("600519.SH", 121))
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    mock_repo.get_minute_batch.return_value = _mock_tail_rows(
        "600519.SH", 11, datetime(2026, 1, 15, 11, 20)
    )

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, False)

    body = {"symbols": ["600519.SH"], "date": "2026-01-15", "prefer_local": True}
    result = json.loads(kline_api.get_minute_batch(mock_request, body).body)

    # 全天拉: start_time = 当日开盘窗口 (09:25), 不是"最后一根 + 1min" (11:31)
    assert sync_spy.call_count == 1
    assert sync_spy.call_args.kwargs.get("start_time") == datetime(2026, 1, 15, 9, 25)
    # 合并结果包含上午: 首根回到开盘附近, 根数覆盖全天
    rows = result["data"]["600519.SH"]
    assert rows[0]["datetime"] == "2026-01-15T09:31:00"
    assert len(rows) >= 121


def test_get_minute_batch_healthy_does_not_suppress_leading_hole_refetch(monkeypatch):
    """服务健康 + prefer_local: 前部洞的股票仍全天补拉 (服务增量锚定本地最新,
    补不了洞); 纯尾部落后的股票维持不补拉 (服务下一轮会补尾巴)。"""
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_rows("600519.SH", 121))
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    hole_local = _mock_tail_rows("600519.SH", 11, datetime(2026, 1, 15, 11, 20))
    stale_local = _mock_minute_rows("000001.SZ", 100)  # 09:31 开头的连续序列, 仅根数不足

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    mock_repo.get_minute_batch.return_value = pl.concat([hole_local, stale_local])

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, True)

    body = {"symbols": ["600519.SH", "000001.SZ"], "date": "2026-01-15", "prefer_local": True}
    result = json.loads(kline_api.get_minute_batch(mock_request, body).body)

    # 洞票被全天补拉; 尾部票未被拉 (只调了一次, 只为 600519)
    assert sync_spy.call_count == 1
    assert sync_spy.call_args.args[0] == ["600519.SH"]
    assert sync_spy.call_args.kwargs.get("start_time") == datetime(2026, 1, 15, 9, 25)
    # 尾部票返回本地 100 根 (健康压制原样生效)
    assert len(result["data"]["000001.SZ"]) == 100
    # 洞票拿到全天
    assert result["data"]["600519.SH"][0]["datetime"] == "2026-01-15T09:31:00"
    assert result["full_minute_local"] is True


def test_get_minute_batch_normal_open_not_treated_as_leading_hole(monkeypatch):
    """无集合竞价K的源首根 09:31/09:35 → 不算前部洞, 维持增量语义 (不全天重拉)。"""
    from app.api import kline as kline_api

    sync_spy = MagicMock(return_value=_mock_minute_df())
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    # 首根 09:31, 仅 5 根 (历史日 expected=240, 根数不足但非洞)
    mock_repo.get_minute_batch.return_value = _mock_minute_rows("600519.SH", 5)

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    mock_request.app.state.minute_refresh = _healthy_svc(monkeypatch, False)

    body = {"symbols": ["600519.SH"], "date": "2026-01-15", "prefer_local": True}
    kline_api.get_minute_batch(mock_request, body)

    # 增量拉: start_time = 最后一根本身 (09:35), 不是 09:25 全天窗口
    assert sync_spy.call_count == 1
    assert sync_spy.call_args.kwargs.get("start_time") == datetime(2026, 1, 15, 9, 35)


def test_minute_refresh_is_healthy_requires_recent_round(monkeypatch):
    """is_healthy 三条件: 偏好开 + 线程活 + 最近一轮距现在 ≤ max(2×间隔, 30s)。"""
    import time as time_mod

    from app.services import minute_refresh as mr

    svc = mr.MinuteRefreshService(MagicMock())  # is_healthy 不触达 repo

    monkeypatch.setattr(mr.preferences, "get_minute_refresh_enabled", lambda: True)
    monkeypatch.setattr(mr.preferences, "get_minute_refresh_interval", lambda: 6)
    # 线程未启动 → False
    assert svc.is_healthy() is False

    svc._thread = MagicMock()
    svc._thread.is_alive.return_value = True
    # 无轮次记录 → False
    assert svc.is_healthy() is False

    # 最近一轮在 10s 前 (≤ max(12, 30)) → True
    svc._state.last_round_at = time_mod.time() - 10
    assert svc.is_healthy() is True

    # 最近一轮在 120s 前 (> 30) → False (连续失败不更新 last_round_at, 自动超时)
    svc._state.last_round_at = time_mod.time() - 120
    assert svc.is_healthy() is False

    # 偏好关闭 → False
    monkeypatch.setattr(mr.preferences, "get_minute_refresh_enabled", lambda: False)
    svc._state.last_round_at = time_mod.time() - 10
    assert svc.is_healthy() is False
