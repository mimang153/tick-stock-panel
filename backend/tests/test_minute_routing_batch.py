"""分钟 K 批量拉取/落盘/写锁场景。

从原模块复用辅助函数，断言与用例逻辑保持原样。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from threading import Lock
from unittest.mock import MagicMock

import httpx
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from app.services import kline_sync
from tests.minute_routing_helpers import (
    _bars,
    _endpoint_mocks,
    _mock_minute_df,
    _setup_custom_provider,
)


def test_minute_batch_tail_stale_pulls_incremental_and_persists(monkeypatch):
    """尾部落后 (本地连续但根数不足) → 从最后一根本身增量拉 (动态K重拉覆盖);
    拉取结果落盘, 响应为本地+增量合并去重。"""
    from app.api import kline as kline_api

    local = _bars(
        "600519.SH",
        [
            datetime(2026, 1, 15, 9, 31),
            datetime(2026, 1, 15, 9, 32),
            datetime(2026, 1, 15, 9, 33),
        ],
    )
    inc = _bars("600519.SH", [datetime(2026, 1, 15, 9, 34), datetime(2026, 1, 15, 9, 35)])
    captured, writes, req = _endpoint_mocks(monkeypatch, local, sync_ret=inc)

    result = json.loads(
        kline_api.get_minute_batch(req, {"symbols": ["600519.SH"], "date": "2026-01-15"}).body
    )

    assert len(captured) == 1
    assert captured[0]["start"] == datetime(2026, 1, 15, 9, 33)  # 从最后一根本身重拉 (动态K覆盖)
    assert captured[0]["asset"] == "stock"
    assert writes and writes[0].height == 2  # 取到即落盘
    rows = result["data"]["600519.SH"]
    assert len(rows) == 5  # 3 本地 + 2 增量
    assert rows[-1]["datetime"] == "2026-01-15T09:35:00"


def test_minute_batch_middle_hole_falls_back_to_full_day(monkeypatch):
    """中间缺K (间距 2min) → 增量窗口永远回看不到洞, 必须退回全天重拉。"""
    from app.api import kline as kline_api

    local = _bars("600519.SH", [datetime(2026, 1, 15, 9, 31), datetime(2026, 1, 15, 9, 33)])
    captured, _, req = _endpoint_mocks(monkeypatch, local, sync_ret=pl.DataFrame())

    kline_api.get_minute_batch(req, {"symbols": ["600519.SH"], "date": "2026-01-15"})

    assert len(captured) == 1
    assert captured[0]["start"] == datetime(2026, 1, 15, 9, 25)  # 全天窗口


def test_minute_batch_fresh_local_skips_pull(monkeypatch):
    """本地完整 (240 根, 含午休 91min 间距) → 不发任何拉取请求, 直读本地。"""
    from app.api import kline as kline_api

    dts = [datetime(2026, 1, 15, 9, 31) + timedelta(minutes=i) for i in range(120)] + [
        datetime(2026, 1, 15, 13, 1) + timedelta(minutes=i) for i in range(120)
    ]
    local = _bars("600519.SH", dts)
    captured, writes, req = _endpoint_mocks(monkeypatch, local, sync_ret=pl.DataFrame())

    result = json.loads(
        kline_api.get_minute_batch(req, {"symbols": ["600519.SH"], "date": "2026-01-15"}).body
    )

    assert captured == []  # 零请求
    assert writes == []
    assert len(result["data"]["600519.SH"]) == 240


def test_minute_batch_since_returns_only_new_bars(monkeypatch):
    """since 增量响应: 只回 >= since 的K (含 since 本身 — 形成中的动态K需覆盖),
    无新增的 symbol 不出现在 data; incremental 标志为 True。"""
    from app.api import kline as kline_api

    dts = [datetime(2026, 1, 15, 9, 31) + timedelta(minutes=i) for i in range(120)] + [
        datetime(2026, 1, 15, 13, 1) + timedelta(minutes=i) for i in range(120)
    ]
    local = _bars("600519.SH", dts)  # fresh: 不触发补拉, 直读本地后做 since 过滤
    _, _, req = _endpoint_mocks(monkeypatch, local, sync_ret=pl.DataFrame())

    result = json.loads(
        kline_api.get_minute_batch(
            req,
            {
                "symbols": ["600519.SH"],
                "date": "2026-01-15",
                "since": "2026-01-15T15:00:00",
            },
        ).body
    )
    rows = result["data"]["600519.SH"]
    assert [r["datetime"] for r in rows] == ["2026-01-15T15:00:00"]  # 含 since 当根
    assert result["incremental"] is True

    # since 早于全部K → 全量返回; 无 since → incremental False
    full = json.loads(
        kline_api.get_minute_batch(
            req,
            {
                "symbols": ["600519.SH"],
                "date": "2026-01-15",
                "since": "2026-01-15T09:00:00",
            },
        ).body
    )
    assert len(full["data"]["600519.SH"]) == 240
    plain = json.loads(
        kline_api.get_minute_batch(req, {"symbols": ["600519.SH"], "date": "2026-01-15"}).body
    )
    assert plain["incremental"] is False
    assert len(plain["data"]["600519.SH"]) == 240

    # 防御: 带 Z 的 UTC aware 输入 (toISOString 客户端) → 归一为北京 naive 再比,
    # 不抛 TypeError 且语义正确 (15:00 北京 = 07:00 UTC)
    utc = json.loads(
        kline_api.get_minute_batch(
            req,
            {
                "symbols": ["600519.SH"],
                "date": "2026-01-15",
                "since": "2026-01-15T07:00:00Z",
            },
        ).body
    )
    assert [r["datetime"] for r in utc["data"]["600519.SH"]] == ["2026-01-15T15:00:00"]


# ---------- 测试 10: sync_minute_batch 自定义源成功时调 on_segment (Issue 1) ----------


def test_sync_minute_batch_custom_calls_on_segment(monkeypatch):
    """Issue 1: sync_minute_batch 自定义源成功 + 传了 on_segment →
    调 on_segment(df), 返回空 df (数据已落盘)。
    """
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    on_segment_spy = MagicMock(name="on_segment_spy")
    df = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        on_segment=on_segment_spy,
        asset_type="stock",
    )

    on_segment_spy.assert_called_once()
    assert_frame_equal(on_segment_spy.call_args.args[0], expected_df)
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()
    get_client_spy.assert_not_called()


# ---------- 测试 11: 自定义源返回空 df 时不调 on_segment (Issue 1 边界) ----------


def test_sync_minute_batch_custom_empty_df_skips_on_segment(monkeypatch):
    """Issue 1 边界: 自定义源返回空 df → 不调 on_segment (与 TickFlow `if seg_out:` 对称)。"""
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = pl.DataFrame()
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    on_segment_spy = MagicMock(name="on_segment_spy")
    df = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        on_segment=on_segment_spy,
        asset_type="stock",
    )

    on_segment_spy.assert_not_called()
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()


# ---------- 测试 12: 同步落盘的自定义源失败路径 ----------


def test_sync_and_persist_minute_custom_failure_does_not_call_tickflow(monkeypatch, tmp_path):
    """自定义源失败时，免费档增补的分钟能力不能触发 TickFlow 回退。"""
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    provider = MagicMock()
    provider.get_minute.side_effect = httpx.TimeoutException("timeout")
    _setup_custom_provider(monkeypatch, provider, has_dataset=True)
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda repo: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda repo: None)
    monkeypatch.setattr(
        kline_sync, "_latest_minute_datetime", lambda repo, asset_type="stock": None
    )
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *a, **kw: MagicMock(batch=100, rpm=30))
    write_spy = MagicMock()
    monkeypatch.setattr(kline_sync, "_write_minute_partition", write_spy)
    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    repo = MagicMock()
    repo.store.data_dir = tmp_path
    repo._write_lock = Lock()
    capset = CapabilitySet({Cap.KLINE_MINUTE_BATCH: CapabilityLimits()})

    from app.data_providers.base import ProviderRequestError

    with pytest.raises(ProviderRequestError, match="取数失败"):
        kline_sync.sync_and_persist_minute(["600519.SH"], repo, capset)
    write_spy.assert_not_called()
    get_client_spy.assert_not_called()


def test_sync_and_persist_minute_custom_persists(monkeypatch, tmp_path):
    """Issue 1 端到端: sync_and_persist_minute + 自定义源 →
    _write_minute_partition 被调, written > 0。
    """
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    # mock sync_and_persist_minute 内部依赖 (通过 monkeypatch kline_sync 模块属性)
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda repo: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda repo: None)
    monkeypatch.setattr(
        kline_sync, "_latest_minute_datetime", lambda repo, asset_type="stock": None
    )
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *a, **kw: MagicMock(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)

    # _write_minute_partition spy: 记录调用, 返回行数
    write_spy = MagicMock(return_value=expected_df.height)
    monkeypatch.setattr(kline_sync, "_write_minute_partition", write_spy)

    # get_client spy: 自定义源成功时不应走 TickFlow
    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    # mock repo
    mock_repo = MagicMock()
    mock_repo.store.data_dir = tmp_path
    mock_repo.db.execute = MagicMock()

    # mock capset (minute_is_custom=True 绕过 has() 检查, resolve_limit 已 mock)
    mock_capset = MagicMock()

    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"],
        mock_repo,
        mock_capset,
    )

    assert write_spy.called
    assert written == expected_df.height
    assert written > 0
    get_client_spy.assert_not_called()


def test_sync_and_persist_minute_holds_repository_write_lock(monkeypatch, tmp_path):
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda repo: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda repo: None)
    monkeypatch.setattr(
        kline_sync, "_latest_minute_datetime", lambda repo, asset_type="stock": None
    )
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *a, **kw: MagicMock(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)

    write_lock = Lock()

    def assert_locked(df, minute_dir):
        assert not write_lock.acquire(blocking=False)
        return df.height

    monkeypatch.setattr(kline_sync, "_write_minute_partition", assert_locked)

    mock_repo = MagicMock()
    mock_repo.store.data_dir = tmp_path
    mock_repo.db.execute = MagicMock()
    mock_repo._write_lock = write_lock

    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"],
        mock_repo,
        MagicMock(),
    )

    assert written == expected_df.height
