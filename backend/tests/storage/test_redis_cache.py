"""RedisCacheBackend 契约测试：与内存实现保持相同的 TTL / generation 语义。"""

from __future__ import annotations

import pytest

from app.storage.cache import RedisCacheBackend


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture()
def cache():
    fakeredis = pytest.importorskip("fakeredis")
    client = fakeredis.FakeRedis(decode_responses=False)
    backend = RedisCacheBackend("redis://unused", client=client, prefix="test:cache:")
    yield backend
    backend.close()


def test_set_get_dataframe_and_json(cache) -> None:
    frame = pytest.importorskip("polars").DataFrame({"symbol": ["600519.SH"], "close": [2.0]})
    cache.set("enriched:latest", frame)
    cache.set_json("pool:CSS300", {"symbols": ["600519.SH"]})

    assert cache.get("enriched:latest")["close"].to_list() == [2.0]
    assert cache.get_json("pool:CSS300") == {"symbols": ["600519.SH"]}
    assert cache.get_json("enriched:latest") is None
    assert cache.get("pool:CSS300") is None


def test_ttl_expires() -> None:
    fakeredis = pytest.importorskip("fakeredis")
    clock = _Clock()
    cache = RedisCacheBackend(
        "redis://unused",
        client=fakeredis.FakeRedis(decode_responses=False),
        prefix="test:ttl:",
        clock=clock,
    )
    cache.set_json("pool:CSS300", {"a": 1}, ttl_s=10)

    clock.now += 9
    assert cache.get_json("pool:CSS300") == {"a": 1}
    clock.now += 2
    assert cache.get_json("pool:CSS300") is None
    cache.close()


def test_bump_invalidates_only_own_namespace(cache) -> None:
    pl = pytest.importorskip("polars")
    cache.set("enriched:latest", pl.DataFrame({"close": [1.0]}))
    cache.set("instruments:all", pl.DataFrame({"close": [2.0]}))

    assert cache.bump("enriched") == 1
    assert cache.get("enriched:latest") is None
    assert cache.get("instruments:all") is not None

    cache.set("enriched:latest", pl.DataFrame({"close": [3.0]}))
    assert cache.get("enriched:latest")["close"].to_list() == [3.0]
    assert cache.generation("enriched") == 1
    assert cache.bump("enriched") == 2


def test_invalidate_by_prefix_and_clear_keep_generation(cache) -> None:
    cache.set_json("pool:CSS300", {"a": 1})
    cache.set_json("pool:CSS500", {"b": 2})
    cache.set_json("ai:chat", {"c": 3})
    cache.bump("enriched")

    cache.invalidate("pool:")
    assert cache.get_json("pool:CSS300") is None
    assert cache.get_json("pool:CSS500") is None
    assert cache.get_json("ai:chat") == {"c": 3}

    cache.clear()
    assert cache.get_json("ai:chat") is None
    assert cache.generation("enriched") == 1
