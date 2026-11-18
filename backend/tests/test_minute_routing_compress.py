"""gzip 压缩开关与偏好并发保存场景。

从原模块复用辅助函数，断言与用例逻辑保持原样。"""

from __future__ import annotations

import json

from tests.minute_routing_helpers import (
    _compress_mock_env,
    _daily_mock_env,
)


def test_minute_batch_compress_preference_default_and_toggle(monkeypatch):
    """偏好默认开启; 关闭后 getter 立即反映 (逐请求读取, 无缓存)。"""
    from app.services import preferences as prefs

    monkeypatch.setattr(prefs, "load", lambda: {})
    assert prefs.get_minute_batch_compress() is True
    monkeypatch.setattr(prefs, "load", lambda: {"minute_batch_compress": False})
    assert prefs.get_minute_batch_compress() is False


def test_get_minute_batch_gzip_response_when_enabled(monkeypatch):
    """开关开 + 客户端接受 gzip + 响应超阈值 → 返回 gzip Response, 解压后 JSON 完整。"""
    import gzip as gzip_mod

    from fastapi import Response

    from app.api import kline as kline_api

    mock_request, _ = _compress_mock_env(monkeypatch, compress_on=True)
    result = kline_api.get_minute_batch(
        mock_request, {"symbols": ["600519.SH"], "date": "2026-01-15"}
    )
    assert isinstance(result, Response)
    assert result.headers["content-encoding"] == "gzip"
    import json as json_mod

    payload = json_mod.loads(gzip_mod.decompress(result.body))
    assert payload["full_minute_local"] is False
    assert len(payload["data"]["600519.SH"]) > 0


def test_get_minute_batch_plain_when_disabled(monkeypatch):
    """开关关 → 不压缩, 返回 JSON bytes 的未压缩 Response (无 Content-Encoding)。"""
    from fastapi import Response

    from app.api import kline as kline_api

    mock_request, _ = _compress_mock_env(monkeypatch, compress_on=False)
    result = kline_api.get_minute_batch(
        mock_request, {"symbols": ["600519.SH"], "date": "2026-01-15"}
    )
    assert isinstance(result, Response)
    assert "content-encoding" not in result.headers
    assert "600519.SH" in json.loads(result.body)["data"]  # type: ignore[arg-type]


def test_get_minute_batch_plain_without_accept_encoding(monkeypatch):
    """开关开但客户端未声明 gzip (如裸 curl) → 尊重协商, 原样返回。"""
    from app.api import kline as kline_api

    mock_request, _ = _compress_mock_env(monkeypatch, compress_on=True, accept=None)
    result = kline_api.get_minute_batch(
        mock_request, {"symbols": ["600519.SH"], "date": "2026-01-15"}
    )
    from fastapi import Response

    assert isinstance(result, Response)
    assert "content-encoding" not in result.headers
    assert "600519.SH" in json.loads(result.body)["data"]  # type: ignore[arg-type]


# ---------- 测试: 日K批量传输压缩 (与分时独立开关) ----------


def test_daily_batch_gzip_when_enabled(monkeypatch):
    """日K压缩开 + 接受 gzip → 压缩 Response, 解压 JSON 完整。"""
    import gzip as gzip_mod
    import json as json_mod

    from fastapi import Response

    from app.api import kline as kline_api

    req = _daily_mock_env(monkeypatch, compress_on=True)
    result = kline_api.get_daily_batch(req, {"symbols": ["600519.SH"], "days": 20})
    assert isinstance(result, Response)
    assert result.headers["content-encoding"] == "gzip"
    payload = json_mod.loads(gzip_mod.decompress(result.body))
    assert len(payload["data"]["600519.SH"]) == 20


def test_daily_batch_plain_when_disabled(monkeypatch):
    from app.api import kline as kline_api

    req = _daily_mock_env(monkeypatch, compress_on=False)
    result = kline_api.get_daily_batch(req, {"symbols": ["600519.SH"], "days": 20})
    from fastapi import Response

    assert isinstance(result, Response)
    assert "content-encoding" not in result.headers
    assert "600519.SH" in json.loads(result.body)["data"]  # type: ignore[arg-type]


def test_daily_batch_independent_from_minute_switch(monkeypatch):
    """日K与分时独立: 分时关、日K开 → 日K仍压缩 (helper 按 pref_key 走各自 getter)。"""
    import gzip as gzip_mod

    from fastapi import Response

    from app.api import kline as kline_api
    from app.services import preferences as prefs

    monkeypatch.setattr(prefs, "get_minute_batch_compress", lambda: False)
    req = _daily_mock_env(monkeypatch, compress_on=True)
    result = kline_api.get_daily_batch(req, {"symbols": ["600519.SH"], "days": 20})
    assert isinstance(result, Response) and gzip_mod.decompress(result.body)


def test_preferences_parallel_saves_do_not_lose_each_other(tmp_path, monkeypatch):
    """回归: 并行 save 不同键不得互相覆盖 (压缩总开关并行 PUT 两键的竞态)。

    save 是 read-modify-write, 无锁时两线程同时基于旧快照写盘,
    后写者会把先写者的更新覆盖掉。
    """
    import threading

    from app.services import preferences as prefs

    monkeypatch.setattr(prefs, "_path", lambda: tmp_path / "preferences.json")
    prefs._invalidate_cache()
    prefs.save({"minute_batch_compress": True})

    barrier = threading.Barrier(2)

    def write_key(key: str) -> None:
        barrier.wait()  # 尽量同时进入 save
        prefs.save({key: False})

    t1 = threading.Thread(target=write_key, args=("minute_batch_compress",))
    t2 = threading.Thread(target=write_key, args=("daily_batch_compress",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    final = prefs.load()
    assert final["minute_batch_compress"] is False
    assert final["daily_batch_compress"] is False
