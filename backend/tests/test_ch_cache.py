"""缓存抽象（L1）：TTL 与 generation 语义（设计文档 §4.4）。"""

from __future__ import annotations

import polars as pl

from app.storage.cache import MemoryCacheBackend


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _frame(value: float = 1.0) -> pl.DataFrame:
    return pl.DataFrame({"symbol": ["600519.SH"], "close": [value]})


def test_set_get_dataframe_and_json() -> None:
    cache = MemoryCacheBackend()
    cache.set("enriched:latest", _frame(2.0))
    cache.set_json("pool:CSS300", {"symbols": ["600519.SH"]})

    assert cache.get("enriched:latest")["close"].to_list() == [2.0]  # type: ignore[index]
    assert cache.get_json("pool:CSS300") == {"symbols": ["600519.SH"]}
    # 类型不匹配视为未命中，而不是把 DataFrame 当 dict 返回
    assert cache.get_json("enriched:latest") is None
    assert cache.get("pool:CSS300") is None


def test_ttl_expires() -> None:
    clock = _Clock()
    cache = MemoryCacheBackend(clock=clock)
    cache.set_json("pool:CSS300", {"a": 1}, ttl_s=10)

    clock.now += 9
    assert cache.get_json("pool:CSS300") == {"a": 1}
    clock.now += 2
    assert cache.get_json("pool:CSS300") is None


def test_bump_invalidates_older_entries_in_same_namespace() -> None:
    cache = MemoryCacheBackend()
    cache.set("enriched:latest", _frame(1.0))
    cache.set("instruments:all", _frame(2.0))

    generation = cache.bump("enriched")

    assert generation == 1
    assert cache.get("enriched:latest") is None  # 旧 generation 视为失效
    assert cache.get("instruments:all") is not None  # 其它命名空间不受影响

    cache.set("enriched:latest", _frame(3.0))
    assert cache.get("enriched:latest")["close"].to_list() == [3.0]  # type: ignore[index]

    assert cache.bump("enriched") == 2
    assert cache.generation("enriched") == 2


def test_bump_only_affects_its_own_namespace() -> None:
    cache = MemoryCacheBackend()
    cache.set_json("live:agg", {"n": 1})
    cache.bump("enriched")

    assert cache.get_json("live:agg") == {"n": 1}


def test_invalidate_by_prefix() -> None:
    cache = MemoryCacheBackend()
    cache.set_json("pool:CSS300", {"a": 1})
    cache.set_json("pool:CSS500", {"b": 2})
    cache.set_json("ai:chat", {"c": 3})

    cache.invalidate("pool:")

    assert cache.get_json("pool:CSS300") is None
    assert cache.get_json("pool:CSS500") is None
    assert cache.get_json("ai:chat") == {"c": 3}


def test_clear_drops_everything_but_keeps_generations() -> None:
    cache = MemoryCacheBackend()
    cache.bump("enriched")
    cache.set("enriched:latest", _frame())

    cache.clear()

    assert cache.get("enriched:latest") is None
    assert cache.generation("enriched") == 1
