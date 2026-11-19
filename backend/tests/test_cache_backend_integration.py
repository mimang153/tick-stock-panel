"""CacheBackend 与业务缓存/ generation 的集成测试。"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl

from app.enriched_generation import bump_enriched_generation
from app.indicators import pipeline, pipeline_enriched
from app.storage.cache import MemoryCacheBackend


def _benchmark_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["000002.SH", "000002.SH"],
            "date": [date(2026, 11, 17), date(2026, 11, 18)],
            "close": [10.0, 11.0],
        }
    )


def test_benchmark_uses_cache_backend(monkeypatch, tmp_path) -> None:
    cache = MemoryCacheBackend()
    calls: list[bool] = []

    class FakeStore:
        def read_daily(self, table, **kwargs):
            calls.append(True)
            return _benchmark_frame()

    monkeypatch.setattr("app.storage.runtime.get_storage", lambda: SimpleNamespace(cache=cache))
    monkeypatch.setattr(pipeline, "_daily_store", lambda: FakeStore())
    pipeline._benchmark_cache.clear()

    first = pipeline_enriched.load_benchmark_momentum(tmp_path)
    # L1 与 L2 都会命中；清掉 L1 后仍能从 CacheBackend 读到同一结果。
    pipeline._benchmark_cache.clear()
    second = pipeline_enriched.load_benchmark_momentum(tmp_path)

    assert first is not None and second is not None
    assert first.equals(second)
    assert len(calls) == 1
    assert cache.get("benchmark:momentum:v1") is not None
    pipeline._benchmark_cache.clear()


def test_enriched_bump_advances_cache_generation(monkeypatch, tmp_path) -> None:
    cache = MemoryCacheBackend()
    monkeypatch.setattr("app.storage.runtime.get_storage", lambda: SimpleNamespace(cache=cache))

    bump_enriched_generation(tmp_path)

    assert cache.generation("enriched") == 1
