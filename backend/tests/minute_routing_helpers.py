"""分钟路由测试共享辅助函数。

由拆分后的 test_minute_routing*.py 复用，保持用例逻辑不变。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl

from app.data_providers.custom.config import CustomSourceConfig, DatasetConfig
from app.services import kline_sync


def _mock_minute_df(symbol: str = "600519.SH") -> pl.DataFrame:
    """构造非空分钟 K df, 用于 mock provider.get_minute 返回值。"""
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "datetime": [datetime(2026, 1, 15, 9, 35, 0)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.5],
            "close": [100.5],
            "volume": [1000.0],
            "amount": [100500.0],
        }
    )


def _empty_capset():
    from app.tickflow.capabilities import CapabilitySet

    return CapabilitySet()


def _tickflow_minute_capset():
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    return CapabilitySet({Cap.KLINE_MINUTE_BY_SYMBOL: CapabilityLimits()})


def _setup_custom_provider(monkeypatch, provider: object, has_dataset: bool = True) -> None:
    """统一 mock 自定义分钟源路由前置: preferences + provider_has_dataset + get_provider。

    - preferences.get_minute_data_provider → "mock_src"
    - custom.provider_has_dataset → has_dataset
    - custom.get_provider → provider
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    monkeypatch.setattr("app.data_providers.custom.names", lambda: {"mock_src"})
    provider.config.datasets = {"minute": object()} if has_dataset else {}  # type: ignore[attr-defined]
    # 旧接口用例不声明可选流式能力,避免 MagicMock 自动虚构 iter_minute。
    if isinstance(provider, MagicMock):
        provider.iter_minute = None
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: has_dataset,
    )
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: provider,
    )


def _bars(symbol: str, dts: list) -> pl.DataFrame:
    """构造 canonical 8 列分钟K帧 (dts 为 datetime 列表)。"""
    n = len(dts)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "datetime": dts,
            "open": [10.0] * n,
            "high": [10.1] * n,
            "low": [9.9] * n,
            "close": [10.0] * n,
            "volume": [100.0] * n,
            "amount": [1000.0] * n,
        }
    )


def _endpoint_mocks(monkeypatch, local_df: pl.DataFrame, sync_ret: pl.DataFrame | None):
    """get_minute_batch 的最小 mock: repo/capset + sync/落盘 spy。返回 (捕获, 落盘, request)。"""
    from app.api import kline as kline_api

    captured: list[dict] = []

    def fake_sync(symbols, *, start_time, end_time, batch_size, rpm, asset_type, raw_basis=False):
        captured.append({"symbols": list(symbols), "start": start_time, "asset": asset_type})
        return sync_ret if sync_ret is not None else pl.DataFrame()

    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", fake_sync)
    writes: list[pl.DataFrame] = []

    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = set()
    mock_repo.get_minute_batch.return_value = local_df
    mock_repo._write_lock = Lock()
    mock_repo.storage = SimpleNamespace(
        write_minute=lambda table, df: writes.append(df),
        latest_minute_datetime=lambda table: None,
    )
    mock_repo.store.data_dir = Path("data")

    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset
    return captured, writes, mock_request


def _make_minute_config(**extra) -> CustomSourceConfig:
    """构造带 minute dataset 的最小 CustomSourceConfig, extra 传给 DatasetConfig。"""
    field_map = {
        f: f for f in ("symbol", "datetime", "open", "high", "low", "close", "volume", "amount")
    }
    return CustomSourceConfig(
        name="test_src",
        display_name="Test Source",
        datasets={
            "minute": DatasetConfig(
                url="http://example.com/minute",
                field_map=field_map,
                **extra,
            )
        },
    )


def _capture_request_rows(provider):
    """替换 _request_rows 为捕获 spy, 返回 captured dict。"""
    captured: dict = {}

    def fake_request_rows(
        cfg,
        *,
        symbols=None,
        start_time=None,
        end_time=None,
        override_params=None,
        override_body=None,
    ):
        captured["override_params"] = override_params
        captured["override_body"] = override_body
        return []  # 空行 → 空 df

    provider._request_rows = fake_request_rows
    return captured


def _mock_minute_rows(symbol: str, n: int) -> pl.DataFrame:
    """n 根分钟K (同日递增分钟), 用于构造'部分完整'的本地分区数据。"""
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "datetime": [datetime(2026, 1, 15, 9, 31, 0) + timedelta(minutes=i) for i in range(n)],
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.5] * n,
            "close": [100.5] * n,
            "volume": [1000.0] * n,
            "amount": [100500.0] * n,
        }
    )


def _healthy_svc(monkeypatch, healthy: bool):
    """mock app.state.minute_refresh (is_healthy 可控)。"""
    svc = MagicMock()
    svc.is_healthy.return_value = healthy
    return svc


def _mock_tail_rows(symbol: str, n: int, first: datetime) -> pl.DataFrame:
    """n 根从 first 开始的连续分钟K — 模拟重启后实时写入的尾部序列。"""
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "datetime": [first + timedelta(minutes=i) for i in range(n)],
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.5] * n,
            "close": [100.5] * n,
            "volume": [1000.0] * n,
            "amount": [100500.0] * n,
        }
    )


def _compress_mock_env(monkeypatch, *, compress_on, accept="gzip, deflate"):
    """构造 get_minute_batch 压缩路径的最小 mock 环境, 返回 (mock_request, sync_spy)。"""
    from app.api import kline as kline_api
    from app.services import preferences as prefs

    monkeypatch.setattr(prefs, "get_minute_batch_compress", lambda: compress_on)

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
    mock_request.headers = {"accept-encoding": accept} if accept else {}
    return mock_request, sync_spy


def _daily_mock_env(monkeypatch, *, compress_on, accept="gzip, deflate"):
    """构造 get_daily_batch 压缩路径的最小 mock。"""
    from app.services import preferences as prefs

    monkeypatch.setattr(prefs, "get_daily_batch_compress", lambda: compress_on)

    # 20 根日K (date 列), 足以过 1KB 阈值
    n = 20
    daily_df = pl.DataFrame(
        {
            "symbol": ["600519.SH"] * n,
            "date": [date(2026, 1, 1) + timedelta(days=i) for i in range(n)],
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.0] * n,
            "close": [100.5] * n,
            "volume": [1000.0] * n,
        }
    )
    mock_repo = MagicMock()
    mock_repo.resolve_asset_type.return_value = "stock"
    mock_repo.get_daily_batch.return_value = daily_df

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.headers = {"accept-encoding": accept} if accept else {}
    return mock_request
