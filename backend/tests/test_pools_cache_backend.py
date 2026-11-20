"""标的池缓存接入 CacheBackend 的契约测试。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.storage.cache import MemoryCacheBackend
from app.tickflow import pools


@pytest.fixture()
def cache_env(tmp_path, monkeypatch):
    cache = MemoryCacheBackend()
    monkeypatch.setattr(pools.settings, "data_dir", tmp_path)
    monkeypatch.setattr(
        "app.storage.runtime.get_storage",
        lambda: SimpleNamespace(cache=cache),
    )
    return cache


def test_pool_uses_cache_and_refetch_on_refresh(cache_env, monkeypatch):
    calls: list[bool] = []

    def fetch(pool_id):
        calls.append(True)
        return ["600000.SH"] if len(calls) == 1 else ["600036.SH"]

    monkeypatch.setattr(pools, "_fetch_pool", fetch)

    assert pools.get_pool("CSI300") == ["600000.SH"]
    assert pools.get_pool("CSI300") == ["600000.SH"]
    assert len(calls) == 1

    assert pools.get_pool("CSI300", refresh=True) == ["600036.SH"]
    assert pools.get_pool("CSI300") == ["600036.SH"]
    assert len(calls) == 2


def test_pool_bump_invalidates_namespace(cache_env, monkeypatch):
    calls: list[bool] = []

    def fetch(pool_id):
        calls.append(True)
        return ["600000.SH"] if len(calls) == 1 else ["600036.SH"]

    monkeypatch.setattr(pools, "_fetch_pool", fetch)

    assert pools.get_pool("CSI300") == ["600000.SH"]
    cache_env.bump("pool")

    assert pools.get_pool("CSI300") == ["600036.SH"]
    assert len(calls) == 2


def test_pool_no_longer_double_writes_parquet(cache_env, monkeypatch, tmp_path):
    """CacheBackend 可用时本地 parquet 保持纯离线回退，不再被无条件写入。"""
    monkeypatch.setattr(pools, "_fetch_pool", lambda pool_id: ["600000.SH"])

    assert pools.get_pool("CSI300") == ["600000.SH"]
    assert not (tmp_path / "pools" / "CSI300.parquet").exists()


def test_pool_cache_failure_falls_back_to_local_file(cache_env, monkeypatch, tmp_path):
    class BrokenCache:
        def get_json(self, key):
            raise OSError("redis down")

        def set_json(self, key, value, *, ttl_s=None):
            raise OSError("redis down")

    monkeypatch.setattr(
        "app.storage.runtime.get_storage",
        lambda: SimpleNamespace(cache=BrokenCache()),
    )
    monkeypatch.setattr(pools, "_fetch_pool", lambda pool_id: [])
    path = tmp_path / "pools" / "CSI300.parquet"
    path.parent.mkdir(parents=True)
    import polars as pl

    pl.DataFrame({"symbol": ["600000.SH"], "as_of": ["2026-11-18"]}).write_parquet(path)

    assert pools.get_pool("CSI300") == ["600000.SH"]
