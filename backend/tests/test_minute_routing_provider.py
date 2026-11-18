"""provider 异常、解析器与自定义源配置场景。

从原模块复用辅助函数，断言与用例逻辑保持原样。"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import httpx

from app.services import kline_sync
from tests.minute_routing_helpers import (
    _capture_request_rows,
    _make_minute_config,
    _setup_custom_provider,
)


def test_get_provider_exception_does_not_fall_back_to_tickflow(monkeypatch):
    """选中的自定义源无法解析时 fail-closed,不向其他数据源回退。"""
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    monkeypatch.setattr("app.data_providers.custom.names", lambda: {"mock_src"})
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: True,  # provider 存在, 但 get_provider 会抛
    )

    def _raising_get_provider(name):
        raise ValueError("not found")

    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        _raising_get_provider,
    )

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"],
        None,
        None,
        asset_type="stock",
    )

    assert fallback is False
    assert df is not None and df.is_empty()


# ---------- 测试 14: provider_has_dataset 异常时 fail-closed ----------


def test_provider_has_dataset_exception_does_not_fall_back(monkeypatch):
    """自定义源注册信息异常时 fail-closed,不向 TickFlow 请求数据。"""
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    monkeypatch.setattr("app.data_providers.custom.names", lambda: {"mock_src"})
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: MagicMock(config=MagicMock(datasets={"minute": {}})),
    )

    def _raising_has_dataset(name, ds):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        _raising_has_dataset,
    )

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"],
        None,
        None,
        asset_type="stock",
    )

    assert fallback is False
    assert df is not None and df.is_empty()


# ---------- 测试 15-17: GenericHTTPProvider opt-in 参数传递 (Issue 3) ----------
from app.data_providers.custom.provider import (  # noqa: E402 - 测试分节内导入, 紧邻本组 fixture
    GenericHTTPProvider,
)


def test_generic_http_get_minute_passes_asset_type_when_configured():
    """Issue 3: 配了 asset_type_param="asset" → override 含 {"asset": "etf"}。"""
    config = _make_minute_config(asset_type_param="asset")
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="etf", freq="1m")

    assert captured["override_params"] == {"asset": "etf"}
    assert captured["override_body"] == {"asset": "etf"}


def test_generic_http_get_minute_passes_freq_when_configured():
    """Issue 3: 配了 freq_param="period" → override 含 {"period": "1m"}。"""
    config = _make_minute_config(freq_param="period")
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="stock", freq="1m")

    assert captured["override_params"] == {"period": "1m"}
    assert captured["override_body"] == {"period": "1m"}


def test_generic_http_get_minute_omits_params_when_not_configured():
    """Issue 3 向后兼容: 未配 asset_type_param/freq_param → override 为 None, 不传上游。"""
    config = _make_minute_config()  # 无 asset_type_param / freq_param
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="etf", freq="1m")

    # override 为 None (空 dict → `override or None`), 不传上游
    assert captured["override_params"] is None
    assert captured["override_body"] is None


# ---------- 测试 18: sync_and_persist_minute resolver 异常时优雅返回 0 (观察项加固) ----------


def test_sync_and_persist_minute_resolver_exception_returns_zero(monkeypatch, tmp_path):
    """观察项加固: sync_and_persist_minute 开头 _resolve_minute_provider 异常 →
    不向接口抛 500, 优雅降级 (minute_is_custom=False → 走 capset 检查 → 无权限 return 0)。
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )

    # provider_has_dataset 抛异常 (模拟 registry 损坏)
    def _raising_has_dataset(name, ds):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        _raising_has_dataset,
    )

    # 无 KLINE_MINUTE_BATCH 权限 → resolver 异常视为非 custom → capset 检查失败 → return 0
    mock_capset = MagicMock()
    mock_capset.has.return_value = False

    mock_repo = MagicMock()
    mock_repo.store.data_dir = tmp_path

    # 不应抛异常, 优雅降级到 0
    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"],
        mock_repo,
        mock_capset,
    )

    assert written == 0


# ---------- 测试 19: _resolve_minute_provider 单元测试 ----------


def test_resolve_minute_provider_tickflow_returns_silent_fallback():
    """provider_name == "tickflow" 时使用原生数据源。"""
    provider, fallback, err = kline_sync._resolve_minute_provider("tickflow")
    assert provider is None
    assert fallback is True
    assert err is None


def test_resolve_minute_provider_no_dataset_fails_closed(monkeypatch):
    """选中的源没有 minute dataset 时明确停用，不静默切换来源。"""
    monkeypatch.setattr("app.data_providers.custom.names", lambda: {"mock_src"})
    provider = MagicMock()
    provider.config.datasets = {}
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: False,  # 已注册但未配 minute
    )
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")  # type: ignore[assignment]
    assert provider is None
    assert fallback is False
    assert err is not None
    assert "does not configure minute" in err


def test_resolve_minute_provider_unloaded_during_resolution_returns_err(monkeypatch):
    """数据源重载期间源消失时，不把它误判为未配置 minute 数据集。"""
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: (_ for _ in ()).throw(
            ValueError("Custom data source not found or invalid: mock_src")
        ),
    )
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda name, ds: False)
    monkeypatch.setattr("app.data_providers.custom.names", lambda: set())

    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")

    assert provider is None
    assert fallback is False
    assert err is not None
    assert "not found or invalid" in err


def test_resolve_minute_provider_config_exception_returns_err(monkeypatch):
    """自定义源配置无法读取时记录解析错误。"""

    class _BrokenProvider:
        @property
        def config(self):
            raise RuntimeError("registry corrupted")

    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: _BrokenProvider())
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is None
    assert fallback is False
    assert err is not None
    assert "registry corrupted" in err


def test_resolve_minute_provider_get_provider_exception_returns_err(monkeypatch):
    """provider_has_dataset 返回 True 但 get_provider 抛错时记录解析错误。"""

    def _raising_get(name):
        raise ValueError("not found")

    monkeypatch.setattr("app.data_providers.custom.get_provider", _raising_get)
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is None
    assert fallback is False
    assert err is not None
    assert "not found" in err


def test_resolve_minute_provider_success_returns_provider(monkeypatch):
    """观察项加固: 正常路径 → (provider, False, None)。"""
    mock_provider = MagicMock()
    mock_provider.config.datasets = {"minute": {}}
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: mock_provider,
    )
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: True,
    )
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is mock_provider
    assert fallback is False
    assert err is None


def test_minute_allowed_resolver_exception_returns_false(monkeypatch):
    """权限入口复用安全 resolver, 插件注册异常不再穿透为 500。"""
    from app.api import kline as kline_api
    from app.tickflow.capabilities import CapabilitySet

    monkeypatch.setattr(
        "app.services.preferences.get_minute_data_provider",
        lambda: "broken",
    )

    def _raising(name, dataset):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", _raising)

    assert kline_api._minute_allowed(CapabilitySet()) is False


def test_intraday_monitor_support_resolver_exception_fails_closed(monkeypatch):
    """所选自定义源无法解析时，分时监控不误报为 TickFlow 可用。"""
    from app.tickflow.capabilities import Cap, CapabilitySet

    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "broken",
    )

    def _raising(name, dataset):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", _raising)
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH)

    support = kline_sync.intraday_monitor_support(capset)

    assert support["available"] is False
    assert support["source"] is None


def test_intraday_monitor_custom_failure_does_not_request_tickflow(monkeypatch):
    """分时监控批量取数遇到自定义源故障时，不尝试请求 TickFlow。"""
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    provider = MagicMock()
    provider.get_minute.side_effect = httpx.TimeoutException("timeout")
    _setup_custom_provider(monkeypatch, provider, has_dataset=True)
    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)
    capset = CapabilitySet({Cap.KLINE_MINUTE_BATCH: CapabilityLimits()})

    result = kline_sync.fetch_intraday_monitor_batch(
        ["600519.SH"],
        capset,
        now=datetime(2026, 1, 15, 10, 0),
    )

    assert result.is_empty()
    get_client_spy.assert_not_called()
