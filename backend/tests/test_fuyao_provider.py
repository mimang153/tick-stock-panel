"""FuyaoProvider 契约与单位标准化测试。

不依赖真实网络: 用假 FuyaoClient 返回样例快照页/历史K线/dump, 验证字段映射、
单位口径 (CONTRIBUTING §3.1: 百分数→小数、volume 股→手)、日K取数分档
(10d dump / 单标的接口)、除权因子推导 (交易所公式 + half-up 舍入 +
同日合并 + 涨跌停自检)、能力声明 (未声明数据集回退 tickflow) 与设置页试拉。
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from app.plugins.fuyao import client as fc
from app.plugins.fuyao import provider as fp
from app.plugins.fuyao.provider import FuyaoProvider


class _FakeClient:
    """按调用次数返回预置页, 记录调用供分页断言。snapshot_all 同真实客户端语义。"""

    def __init__(
        self,
        pages: list[list[dict]],
        count: int,
        error: Exception | None = None,
        server_ts: int = 0,
    ):
        self.pages = pages
        self.count = count
        self.error = error
        self.server_ts = server_ts
        self.calls: list[dict] = []
        self.last_server_ts = server_ts

    def snapshot_page(self, limit=500, offset=0):
        self.calls.append({"limit": limit, "offset": offset})
        if self.error:
            raise self.error
        if not self.pages:
            return [], self.count
        # 按调用次数取页(provider 每轮 offset += len(rows), 页序与调用序一致)
        idx = min(len(self.calls) - 1, len(self.pages) - 1)
        return list(self.pages[idx]), self.count

    def snapshot_all(self):
        """与真实客户端同语义的分页循环 (无页间隔, 测试用)。返回 (rows, server_ts)。"""
        out: list[dict] = []
        offset = 0
        for _ in range(50):
            rows, count = self.snapshot_page(offset=offset)
            if not rows:
                break
            out.extend(rows)
            if count and len(out) >= count:
                break
            offset += len(rows)
        if not out:
            raise fc.FuyaoError("全市场快照为空")
        return out, self.server_ts

    def close(self):
        pass


def _row(thscode: str = "600519.SH", **over):
    """实测快照行结构(2026-08): high_price/low_price/prev_price 命名。"""
    row = {
        "thscode": thscode,
        "last_price": 1480.0,
        "price_change": 25.0,
        "price_change_ratio_pct": 1.72,
        "open_price": 1460.0,
        "high_price": 1490.5,
        "low_price": 1455.0,
        "prev_price": 1455.0,
        "volume": 1234500,
        "turnover": 1.83e9,
    }
    row.update(over)
    return row


def _provider_with(monkeypatch, pages, count=None, error=None, **fake_kwargs):
    fake = _FakeClient(
        pages, count if count is not None else sum(len(p) for p in pages), error, **fake_kwargs
    )
    monkeypatch.setattr(fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: fake}))
    monkeypatch.setattr(fp, "get_api_key", lambda: "test-key")
    return FuyaoProvider(), fake


# ---- 单位与字段映射 ----


def test_snapshot_units_and_field_mapping(monkeypatch):
    """核心口径: price_change_ratio_pct 百分数 → change_pct 小数制 (1.72 → 0.0172)。"""
    provider, _ = _provider_with(monkeypatch, [[_row()]])
    records = provider.get_realtime()
    assert len(records) == 1
    r = records[0]
    assert r["symbol"] == "600519.SH"
    assert r["change_pct"] == pytest.approx(0.0172)
    assert r["change_amount"] == pytest.approx(25.0)
    assert r["prev_close"] == 1455.0
    assert r["open"] == 1460.0
    assert r["high"] == 1490.5
    assert r["low"] == 1455.0
    # volume 单位股(1234500) → 内部契约手: floor(1234500/100) = 12345
    assert r["volume"] == 12345
    assert r["amount"] == 1.83e9
    assert r["timestamp"] > 0
    # 快照不提供的字段必须为 None, 不启发式伪造
    assert r["name"] is None
    assert r["amplitude"] is None
    assert r["turnover_rate"] is None


def test_missing_pct_derives_decimal_from_change_amount(monkeypatch):
    """涨跌幅缺失时按 change_amount/prev_close 推导, 仍为小数制。"""
    provider, _ = _provider_with(monkeypatch, [[_row(price_change_ratio_pct=None)]])
    r = provider.get_realtime()[0]
    assert r["change_pct"] == pytest.approx(25.0 / 1455.0)
    assert r["change_amount"] == pytest.approx(25.0)


def test_missing_change_amount_derived_from_prices(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row(price_change=None)]])
    r = provider.get_realtime()[0]
    assert r["change_amount"] == pytest.approx(1480.0 - 1455.0)


def test_row_without_thscode_dropped(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row(), {"last_price": 1.0}, _row("000001.SZ")]])
    records = provider.get_realtime()
    assert [r["symbol"] for r in records] == ["600519.SH", "000001.SZ"]


def test_all_rows_unrecognized_returns_empty_with_no_fake_data(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[{"foo": "bar"}, {"baz": 1}]])
    assert provider.get_realtime() == []


# ---- 客户端信封解析 (实测结构 vs 文档示例) ----


def _patch_http(monkeypatch, payload, status_code=200):
    class _Resp:
        def json(self):
            return payload

    cast(Any, _Resp).status_code = status_code

    class _Http:
        def get(self, path, params=None):
            return _Resp()

        def close(self):
            pass

    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: _Http())


def test_client_parses_real_world_envelope(monkeypatch):
    """实测信封(2026-08): data={timestamp, total, item}。"""
    _patch_http(
        monkeypatch,
        {
            "code": 0,
            "message": "success",
            "data": {"timestamp": 1787542612000, "total": 2, "item": [_row(), _row("000001.SZ")]},
        },
    )
    c = fc.FuyaoClient(api_key="k")
    rows, total = c.snapshot_page()
    assert total == 2 and len(rows) == 2
    assert c.last_server_ts == 1787542612000


def test_client_parses_documented_envelope(monkeypatch):
    """官方文档示例信封: data={count, data}。"""
    _patch_http(
        monkeypatch,
        {
            "code": 0,
            "message": "OK",
            "data": {"count": 3, "data": [_row()]},
        },
    )
    c = fc.FuyaoClient(api_key="k")
    rows, total = c.snapshot_page()
    assert total == 3 and len(rows) == 1


def test_client_raises_on_error_code(monkeypatch):
    _patch_http(monkeypatch, {"code": 4001, "message": "rate limited", "data": None})
    c = fc.FuyaoClient(api_key="k")
    with pytest.raises(fc.FuyaoError, match="4001"):
        c.snapshot_page()


# ---- 字段名兼容与服务端时间戳 ----


def test_doc_style_field_names_fallback(monkeypatch):
    """文档示例字段名 (highest_price/lowest_price/prev_close_price) 也能映射。"""
    row = {
        "thscode": "600519.SH",
        "last_price": 1480.0,
        "price_change": 25.0,
        "price_change_ratio_pct": 1.72,
        "open_price": 1460.0,
        "highest_price": 1490.5,
        "lowest_price": 1455.0,
        "prev_close_price": 1455.0,
        "volume": 1234500,
        "turnover": 1.83e9,
    }
    provider, _ = _provider_with(monkeypatch, [[row]])
    r = provider.get_realtime()[0]
    assert r["high"] == 1490.5
    assert r["low"] == 1455.0
    assert r["prev_close"] == 1455.0


def test_realtime_uses_server_timestamp(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row()]], server_ts=1787542612000)
    assert provider.get_realtime()[0]["timestamp"] == 1787542612000


def test_realtime_falls_back_to_local_time_without_server_ts(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row()]], server_ts=0)
    assert provider.get_realtime()[0]["timestamp"] > 0


# ---- 分页 ----


def test_snapshot_pagination_merges_pages(monkeypatch):
    page1 = [_row(f"{600000 + i}.SH") for i in range(2)]
    page2 = [_row(f"{688000 + i}.SH") for i in range(1)]
    provider, fake = _provider_with(monkeypatch, [page1, page2], count=3)
    records = provider.get_realtime()
    assert len(records) == 3
    assert len(fake.calls) == 2
    assert fake.calls[1]["offset"] == 2


def test_snapshot_stops_when_page_empty(monkeypatch):
    provider, fake = _provider_with(monkeypatch, [[_row()], []], count=0)
    assert len(provider.get_realtime()) == 1
    assert len(fake.calls) == 2


# ---- 软失败 ----


def test_realtime_error_returns_empty_list(monkeypatch):
    provider, _ = _provider_with(
        monkeypatch, [[]], error=fc.FuyaoError("扶摇接口错误 code=4001: 频率超限")
    )
    assert provider.get_realtime() == []


# ---- 指数快照 (可选插件协议 get_realtime_indices) ----


class _FakeIndexClient:
    def __init__(self, rows=None, server_ts=0, error=None):
        self.rows = rows or []
        self.server_ts = server_ts
        self.error = error
        self.calls: list[list[str]] = []

    def index_snapshot(self, thscodes):
        self.calls.append(list(thscodes))
        if self.error:
            raise self.error
        return list(self.rows), self.server_ts

    def close(self):
        pass


def _index_provider_with(monkeypatch, **kwargs):
    fake = _FakeIndexClient(**kwargs)
    monkeypatch.setattr(fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: fake}))
    monkeypatch.setattr(fp, "get_api_key", lambda: "test-key")
    return FuyaoProvider(), fake


def test_realtime_indices_maps_and_keeps_volume_unit(monkeypatch):
    """指数快照 → realtime record; volume 无股→手口径, 直接透传。"""
    provider, fake = _index_provider_with(
        monkeypatch,
        rows=[_row("000001.SH", volume=576656606, price_change_ratio_pct=0.86)],
        server_ts=1787542612000,
    )
    records = provider.get_realtime_indices(["000001.SH", "399001.SZ"])
    assert fake.calls == [["000001.SH", "399001.SZ"]]
    assert len(records) == 1
    r = records[0]
    assert r["symbol"] == "000001.SH"
    assert r["change_pct"] == pytest.approx(0.0086)
    assert r["timestamp"] == 1787542612000
    assert r["volume"] == 576656606  # 不做 /100


def test_realtime_indices_skips_bj_symbols(monkeypatch):
    """北交所指数扶摇不支持, 未知代码会整批 1002 连坐 → .BJ 直接跳过不进请求。"""
    provider, fake = _index_provider_with(monkeypatch, rows=[])
    assert provider.get_realtime_indices(["899050.BJ", "000001.SH"]) == []
    assert fake.calls == [["000001.SH"]]
    assert provider.get_realtime_indices(["899050.BJ"]) == []
    assert fake.calls == [["000001.SH"]]  # 全 .BJ 时根本不发请求


def test_realtime_indices_error_returns_none(monkeypatch):
    provider, _ = _index_provider_with(
        monkeypatch, error=fc.FuyaoError("扶摇接口错误 code=1002: Unknown thscode")
    )
    assert provider.get_realtime_indices(["000001.SH"]) is None


def test_client_requires_api_key():
    with pytest.raises(fc.FuyaoError):
        fc.FuyaoClient(api_key="")


# ---- 能力声明与注册 ----


def test_datasets_declaration():
    """声明 realtime/daily/adj_factor/financial; minute 未声明 (回退 tickflow)。"""
    config = FuyaoProvider().config
    assert "realtime" in config.datasets
    assert "daily" in config.datasets
    assert "adj_factor" in config.datasets
    assert "financial" in config.datasets
    assert "minute" not in config.datasets


# ---- API Key 解析 (secrets.json > .env, 对齐 tickflow 语义) ----


def test_get_api_key_secrets_store_takes_priority(monkeypatch):
    from app import secrets_store

    monkeypatch.delenv(fp.API_KEY_ENV, raising=False)
    monkeypatch.setattr(secrets_store, "load", lambda: {fp.SECRETS_FIELD: "sk-from-ui"})
    assert fp.get_api_key() == "sk-from-ui"


def test_get_api_key_falls_back_to_env(monkeypatch):
    from app import secrets_store

    monkeypatch.setenv(fp.API_KEY_ENV, "sk-from-env")
    monkeypatch.setattr(secrets_store, "load", lambda: {})
    assert fp.get_api_key() == "sk-from-env"


def test_availability_accepts_secrets_store_key(monkeypatch):
    from app import secrets_store

    monkeypatch.delenv(fp.API_KEY_ENV, raising=False)
    monkeypatch.setattr(secrets_store, "load", lambda: {fp.SECRETS_FIELD: "sk-from-ui"})
    assert fp.availability() == (True, "ok")


def test_availability_requires_env_key(monkeypatch):
    from app import secrets_store

    monkeypatch.delenv(fp.API_KEY_ENV, raising=False)
    monkeypatch.setattr(secrets_store, "load", lambda: {})
    ok, reason = fp.availability()
    assert ok is False and fp.API_KEY_ENV in reason


# ---- 先探后存 (probe_api_key) ----


def _patch_client_cls(monkeypatch, fake):
    monkeypatch.setattr(fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: fake}))


def test_probe_api_key_ok(monkeypatch):
    _patch_client_cls(monkeypatch, _FakeClient([[_row()]], 1))
    ok, reason = fp.probe_api_key("sk-candidate")
    assert ok is True and reason == "ok"


def test_probe_api_key_invalid_key(monkeypatch):
    _patch_client_cls(
        monkeypatch,
        _FakeClient([[]], 0, error=fc.FuyaoError("扶摇接口错误 code=1001: 无效 api key")),
    )
    ok, reason = fp.probe_api_key("sk-bad")
    assert ok is False and "无效" in reason


def test_loader_probe_plugin_key_dispatch(monkeypatch):
    import app.plugins.fuyao.provider as provider_mod
    from app.data_providers.custom import loader

    monkeypatch.setattr(
        provider_mod, "probe_api_key", lambda key: (True, "ok") if key == "good" else (False, "bad")
    )
    assert loader.probe_plugin_key("fuyao", "good") == (True, "ok")
    assert loader.probe_plugin_key("fuyao", "bad") == (False, "bad")


def test_loader_probe_plugin_key_unsupported_plugin(monkeypatch):
    from app.data_providers.custom import loader

    # 未声明 api_key_env 的插件 → 不支持界面配 Key (桩 manifest 驱动, 不依赖具体内置插件)
    monkeypatch.setattr(
        loader,
        "plugin_manifest",
        lambda name: {"name": name} if name == "no_key_plugin" else None,
    )
    ok, reason = loader.probe_plugin_key("no_key_plugin", "x")
    assert ok is False and "不支持" in reason
    ok, reason = loader.probe_plugin_key("no_such_plugin", "x")
    assert ok is False and "不存在" in reason


# ---- 保存/清除端点 (直接调用 handler, 先探后存语义) ----


def test_save_plugin_key_invalid_key_not_persisted(monkeypatch):
    from app.api import settings as settings_api
    from app.data_providers import custom as custom_sources

    saved: dict = {}
    monkeypatch.setattr(custom_sources, "probe_plugin_key", lambda n, k: (False, "Key 无效"))
    monkeypatch.setattr(
        settings_api.secrets_store, "save", lambda updates: saved.update(updates) or updates
    )
    out = settings_api.save_plugin_key(settings_api.PluginKeyIn(plugin="fuyao", api_key="bad"))
    assert out["ok"] is False and out["reason"] == "invalid"
    assert saved == {}  # 无效 Key 不落盘


def test_save_plugin_key_valid_persists_and_rescans(monkeypatch):
    from app.api import settings as settings_api
    from app.data_providers import custom as custom_sources

    saved: dict = {}
    reloaded = []
    monkeypatch.setattr(custom_sources, "probe_plugin_key", lambda n, k: (True, "ok"))
    monkeypatch.setattr(
        settings_api.secrets_store, "save", lambda updates: saved.update(updates) or updates
    )
    monkeypatch.setattr(
        settings_api.secrets_store, "mask", lambda key, prefix=4, suffix=4: "abcd••••wxyz"
    )
    monkeypatch.setattr(custom_sources, "refresh_plugin", lambda name: reloaded.append(name))
    monkeypatch.setattr(
        custom_sources, "list_plugins", lambda: [{"name": "fuyao", "available": True}]
    )
    out = settings_api.save_plugin_key(settings_api.PluginKeyIn(plugin="fuyao", api_key="good-key"))
    assert out["ok"] is True
    assert saved == {"fuyao_api_key": "good-key"}  # 字段名与 provider.SECRETS_FIELD 一致
    assert out["plugin_available"] is True
    assert reloaded == ["fuyao"]  # 只刷新当前插件，不关闭其他数据源


def test_clear_plugin_key(monkeypatch):
    from app.api import settings as settings_api
    from app.data_providers import custom as custom_sources

    cleared: list = []
    monkeypatch.setattr(custom_sources, "is_builtin", lambda n: n == "fuyao")
    monkeypatch.setattr(settings_api.secrets_store, "clear", lambda *keys: cleared.extend(keys))
    monkeypatch.setattr(custom_sources, "refresh_plugin", lambda name: None)
    monkeypatch.setattr(
        custom_sources, "list_plugins", lambda: [{"name": "fuyao", "available": False}]
    )
    out = settings_api.clear_plugin_key("fuyao")
    assert out["ok"] is True and out["plugin_available"] is False
    assert cleared == ["fuyao_api_key"]


def test_manifest_declares_datasets():
    from app.data_providers.custom import loader

    manifest = loader.plugin_manifest("fuyao")
    assert manifest is not None
    assert manifest["entry"] == "app.plugins.fuyao.provider:FuyaoProvider"
    assert {"realtime", "daily", "adj_factor"} <= set(manifest.get("datasets") or [])
    assert manifest.get("runtime") == "none"
    assert manifest.get("api_key_env") == fp.API_KEY_ENV


def test_hidden_plugin_not_registered():
    """fuyao 已取消隐藏 (plugin.yaml 不再声明 hidden); hidden 机制本身仍生效。

    用合成清单验证: hidden: true 的插件不注册、不在数据源页展示。
    """
    from app.data_providers.custom import loader

    manifest = loader.plugin_manifest("fuyao")
    assert manifest is not None and not manifest.get("hidden"), (
        "fuyao 应保持可见; 如需重新隐藏请在 plugin.yaml 声明 hidden 并更新本测试"
    )
    loader._register_one_plugin(manifest)
    assert "fuyao" in loader._PLUGIN_STATUS

    hidden_manifest = dict(manifest, name="hidden_probe", hidden=True)
    loader._register_one_plugin(hidden_manifest)
    assert "hidden_probe" not in loader._PLUGIN_STATUS
    assert "hidden_probe" not in loader._PROVIDERS


# ---- 插件 Key 脱敏展示 (与 TickFlow Key 契约一致) ----


def test_plugin_key_masked_from_secrets_then_env(monkeypatch):
    """api_key_masked 随插件状态返回: secrets.json 优先, .env 兜底, 未配置为空。

    完整 Key 不出后端, 只出 mask() 结果 — 与 settings API 的
    tickflow_api_key_masked 同一展示契约。
    """
    from app import secrets_store
    from app.data_providers.custom import loader

    # 未声明 api_key_env / 未配置 Key → 空
    assert loader._plugin_key_masked("x", "") == ""
    monkeypatch.setattr(secrets_store, "load", lambda: {})
    monkeypatch.delenv(fp.API_KEY_ENV, raising=False)
    assert loader._plugin_key_masked("fuyao", fp.API_KEY_ENV) == ""

    # .env 兜底
    monkeypatch.setenv(fp.API_KEY_ENV, "env-secret-key-123456")
    assert loader._plugin_key_masked("fuyao", fp.API_KEY_ENV) == "env-••••••3456"

    # secrets.json 优先于 .env
    monkeypatch.setattr(secrets_store, "load", lambda: {"fuyao_api_key": "stored-secret-key-999"})
    assert loader._plugin_key_masked("fuyao", fp.API_KEY_ENV) == "stor••••••-999"

    # 注册进插件状态: 数据源列表接口据此常驻展示 (而非仅保存后瞬时显示)
    loader._register_one_plugin(cast(Any, loader.plugin_manifest("fuyao")))
    assert loader._PLUGIN_STATUS["fuyao"]["api_key_masked"] == "stor••••••-999"


# ---- 设置页试拉 ----


def test_test_dataset_realtime_preview(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row(), _row("000001.SZ")]], count=5400)
    out = provider.test_dataset("realtime")
    assert out["provider"] == "fuyao"
    assert out["rows"] == 5400
    assert out["preview"][0]["symbol"] == "600519.SH"
    assert out["preview"][0]["change_pct"] == pytest.approx(0.0172)


def test_test_dataset_unsupported_dataset_reports_fallback(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[]])
    out = provider.test_dataset("minute")
    assert "error" in out and "回退" in out["error"]


def test_close_is_idempotent(monkeypatch):
    provider, _ = _provider_with(monkeypatch, [[_row()]])
    provider.close()
    provider.close()
