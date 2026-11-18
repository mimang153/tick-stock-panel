"""TdxQuantProvider 数据解包/试拉/客户端生命周期场景。

从原模块复用辅助函数，断言与用例逻辑保持原样。"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from app.plugins.tdxquant import provider as tp
from tests.test_tdxquant_provider import (
    _FakeClient,
    _FixedDT,
    _his_raw,
    _minute_raw,
    _provider,
    _snap_row,
)


def test_minute_and_daily_unwrap_value_envelope():
    """#509: 数据整体包在 "Value" 键的返回不得解析为 0 行 (FakeClient 绕过
    client 层, 这里验证 provider 解析层的双保险)。"""
    fake = _FakeClient()
    fake.his = {
        "600519.SH": {
            "Value": _minute_raw(
                [
                    ("20261009", 93100, 1255.0, 1256.0, 1254.9, 1255.5, 123_400.0, 15_500.0),
                ]
            )
        }
    }
    df = _provider(fake).get_minute(
        ["600519.SH"], datetime(2026, 10, 8), datetime(2026, 10, 9, 23, 59)
    )
    assert df.height == 1
    assert df.to_dicts()[0]["datetime"] == datetime(2026, 10, 9, 9, 31, 0)

    fake2 = _FakeClient()
    fake2.his = {"600519.SH": {"Value": _his_raw()}}
    assert (
        _provider(fake2)
        .get_daily(["600519.SH"], datetime(2025, 12, 1), datetime(2026, 1, 31))
        .height
        == 2
    )


def test_minute_request_end_keeps_intraday_time():
    """#509: 分钟请求 end 不得截到 00:00:00, 必须传完整时刻。"""
    fake = _FakeClient()
    fake.his["600519.SH"] = _minute_raw(
        [
            ("20261009", 93100, 1255.0, 1256.0, 1254.9, 1255.5, 123_400.0, 15_500.0),
        ]
    )
    _provider(fake).get_minute(["600519.SH"], None, datetime(2026, 10, 9, 14, 30))
    end = fake.his_calls[0]["end"]
    assert isinstance(end, datetime) and (end.hour, end.minute, end.second) == (14, 30, 0)
    _provider(fake).get_intraday_batch(["600519.SH"])
    assert isinstance(fake.his_calls[1]["end"], datetime)
    _provider(fake).get_intraday_latest(["600519.SH"], count=2)
    assert isinstance(fake.his_calls[2]["end"], datetime)


def test_minute_zero_parse_diagnostic_warns_with_sample(caplog):
    """#509: 返回非空但解析 0 行必须留下带样本的 WARNING, 不再静默成功。"""
    tp._UNPARSED_MINUTE_SAMPLES = 0  # 模块级限流计数, 测试前复位
    fake = _FakeClient()
    fake.his = {
        "600519.SH": _minute_raw(
            [
                ("20261009", "not-a-time", 10.0, 10.1, 9.9, 10.0, 12_300.0, 1_500.0),
            ]
        )
    }
    with caplog.at_level(logging.WARNING, logger="app.plugins.tdxquant.provider"):
        df = _provider(fake).get_minute(
            ["600519.SH"], datetime(2026, 10, 8), datetime(2026, 10, 9, 23, 59)
        )
    assert df.is_empty()
    assert "解析为 0 行" in caplog.text
    assert "600519.SH" in caplog.text and "not-a-time" in caplog.text  # 带样本可定位


def test_minute_zero_parse_diagnostic_rate_limited(caplog):
    tp._UNPARSED_MINUTE_SAMPLES = 0
    fake = _FakeClient()
    bad = _minute_raw([("20261009", "bad", 10.0, 10.1, 9.9, 10.0, 12_300.0, 1_500.0)])
    symbols = [f"60000{i}.SH" for i in range(5)]
    for sym in symbols:
        fake.his[sym] = bad
    with caplog.at_level(logging.WARNING, logger="app.plugins.tdxquant.provider"):
        _provider(fake).get_minute(symbols, None, datetime(2026, 10, 9, 23, 59))
    assert caplog.text.count("解析为 0 行") == 3  # 全市场轮不刷屏


def test_get_intraday_batch_today_only():
    fake = _FakeClient()
    today = date.today().strftime("%Y%m%d")
    yday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    fake.his = {
        "600519.SH": _minute_raw(
            [
                (
                    yday,
                    "150000",
                    1250.0,
                    1250.0,
                    1250.0,
                    1250.0,
                    100_000.0,
                    12_500.0,
                ),  # 前日余量 → 过滤
                (today, "093100", 1255.0, 1256.0, 1254.9, 1255.5, 123_400.0, 15_500.0),
                (today, "093200", 1255.5, 1257.0, 1255.0, 1256.0, 88_800.0, 11_160.0),
            ]
        )
    }
    df = _provider(fake).get_intraday_batch(["600519.SH"])
    assert df.height == 2  # 只保留今日行
    assert fake.his_calls[0]["period"] == "1m"


def test_get_intraday_latest_universe_default():
    fake = _FakeClient()
    fake.universe = ["600000.SH", "000001.SZ"]
    today = date.today().strftime("%Y%m%d")
    for sym in fake.universe:
        fake.his[sym] = _minute_raw(
            [
                (today, "093100", 10.0, 10.1, 9.9, 10.0, 50_000.0, 500.0),
                (today, "093200", 10.0, 10.1, 9.9, 10.0, 50_000.0, 500.0),
            ]
        )
    df = _provider(fake).get_intraday_latest(count=3)
    assert df.height == 4  # 每只 2 根
    assert set(df["symbol"].to_list()) == {"600000.SH", "000001.SZ"}
    assert all(c["count"] <= 30 for c in fake.his_calls)


def test_test_dataset_minute(monkeypatch):
    # 试拉窗口 [now-3d, now] 依赖真实时钟, 冻结到 _FixedDT 保证任意时区/时刻可复现
    monkeypatch.setattr(tp, "datetime", _FixedDT)
    fake = _FakeClient()
    fake.his = {
        "600519.SH": _minute_raw(
            [
                ("20261011", "093100", 1255.0, 1256.0, 1254.9, 1255.5, 123_400.0, 15_500.0),
            ]
        )
    }
    out = _provider(fake).test_dataset("minute", ["600519.SH"])
    assert out["rows"] == 1
    assert "datetime" in out["columns"]


# ---------------------------------------------------------------- provider: 试拉
def test_test_dataset_realtime(monkeypatch):
    monkeypatch.setattr(tp, "datetime", _FixedDT)
    fake = _FakeClient()
    fake.snapshot_results = [{"600519.SH": _snap_row()}]
    out = _provider(fake).test_dataset("realtime")
    assert out["rows"] == 1 and out["preview"][0]["symbol"] == "600519.SH"


def test_test_dataset_unsupported_dataset():
    out = _provider(_FakeClient()).test_dataset("financial")
    assert "error" in out and "回退" in out["error"]


# ---------------------------------------------------------------- loader 集成
def test_loader_manifest_parses_and_entry_importable():
    from importlib import import_module

    from app.data_providers.custom import loader

    manifest = loader.plugin_manifest("tdxquant")
    assert manifest is not None
    assert manifest["name"] == "tdxquant"
    assert manifest["display_name"] == "TdxQuant (TdxAiData)"
    assert set(manifest["datasets"]) == {
        "realtime",
        "depth5",
        "daily",
        "adj_factor",
        "minute",
        "full_minute",
    }
    # 计费标识与 Key/积分管理页 (设置页积分角标 + 商城入口的契约来源)
    assert manifest["billing"] == "points"
    assert manifest["key_manage_url"].startswith("https://vip.tdx.com.cn/")
    # 计费提示与使用要点 (设置页警示框 + 详情列表的契约来源)
    assert "积分" in manifest["billing_note"] and "间隔" in manifest["billing_note"]
    assert len(manifest["usage_notes"]) >= 4
    assert any("快照" in n for n in manifest["usage_notes"])
    module_path, _, class_name = manifest["entry"].partition(":")
    provider_cls = getattr(import_module(module_path), class_name)
    assert provider_cls.name == "tdxquant"


def test_loader_probe_plugin_key_dispatch(monkeypatch):
    from app.data_providers.custom import loader

    monkeypatch.setattr(
        tp, "probe_api_key", lambda key: (key == "good", "ok" if key == "good" else "bad")
    )
    assert loader.probe_plugin_key("tdxquant", "good") == (True, "ok")
    assert loader.probe_plugin_key("tdxquant", "bad") == (False, "bad")


def test_close_resets_client():
    fake = _FakeClient()
    p = _provider(fake)
    p.close()
    assert p._client is None
