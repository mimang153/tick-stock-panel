"""RC-05 策略缓存 Redis Hash + Lua 原子合并：跨实例语义与镜像失效。

fakeredis 不支持 EVAL，本文件连真实 Redis；不可达时整体跳过，memory 模式全量
不受影响。每个用例用随机 key 前缀隔离，结束清理自己的键。
"""

from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services import strategy_cache, strategy_cache_redis
from app.storage.cache import RedisCacheBackend


@pytest.fixture()
def redis_backend(monkeypatch):
    redis = pytest.importorskip("redis")
    url = "redis://:123456@localhost:6379/0"
    client = redis.Redis.from_url(url)
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001 - 连不上就跳过，不污染 memory 基线
        pytest.skip(f"真实 Redis 不可达: {exc}")
    prefix = f"test:sc:{uuid.uuid4().hex}:"
    backend = RedisCacheBackend(url, client=client, prefix=prefix)
    monkeypatch.setattr("app.storage.runtime.get_storage", lambda: SimpleNamespace(cache=backend))
    strategy_cache_redis._MIRROR.clear()
    yield backend
    keys = list(client.scan_iter(match=f"{prefix}*", count=500))
    if keys:
        client.delete(*keys)
    strategy_cache_redis._MIRROR.clear()
    backend.close()


def _result(*symbols: str) -> dict:
    return {
        "total": len(symbols),
        "as_of": "2026-07-20",
        "rows": [{"symbol": symbol, "close": index + 1.0} for index, symbol in enumerate(symbols)],
    }


def test_same_day_writes_merge_strategies_and_ever_rows(redis_backend, tmp_path: Path) -> None:
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000001.SZ")})
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s2": _result("600000.SH")})
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000002.SZ")})

    cached = strategy_cache.read_cache(tmp_path)
    assert cached is not None
    assert set(cached["results"]) == {"s1", "s2"}
    assert cached["results"]["s1"]["rows"][0]["symbol"] == "000002.SZ"
    assert cached["results"]["s2"]["rows"][0]["symbol"] == "600000.SH"
    assert set(cached["today_ever_rows"]["s1"]) == {"000001.SZ", "000002.SZ"}
    assert cached["today_ever_matched"]["s1"] == ["000001.SZ", "000002.SZ"]


def test_new_day_resets_ever_rows_and_results(redis_backend, tmp_path: Path) -> None:
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000001.SZ")})
    strategy_cache.write_cache(tmp_path, "2026-07-21", {"s2": _result("600000.SH")})

    cached = strategy_cache.read_cache(tmp_path)
    assert cached is not None
    assert cached["as_of"] == "2026-07-21"
    assert set(cached["results"]) == {"s2"}
    assert set(cached["today_ever_rows"]) == {"s2"}


def test_concurrent_writers_do_not_lose_results(redis_backend, tmp_path: Path) -> None:
    import threading

    def writer(sid: str, symbol: str) -> None:
        strategy_cache.write_cache(tmp_path, "2026-07-20", {sid: _result(symbol)})

    threads = [threading.Thread(target=writer, args=(f"s{i}", f"00000{i}.SZ")) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    cached = strategy_cache.read_cache(tmp_path)
    assert cached is not None
    assert len(cached["results"]) == 8
    assert all(len(cached["today_ever_rows"][f"s{i}"]) == 1 for i in range(8))


def test_mirror_invalidated_by_other_instance_write(redis_backend, tmp_path: Path) -> None:
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000001.SZ")})
    first = strategy_cache.read_cache(tmp_path)
    assert first is not None

    # 模拟另一实例：清空本进程镜像后写入，再由本进程读回。
    strategy_cache_redis._MIRROR.clear()
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000002.SZ")})
    second = strategy_cache.read_cache(tmp_path)

    assert first is not second
    assert second is not None
    assert second["results"]["s1"]["rows"][0]["symbol"] == "000002.SZ"


def test_clear_removes_hash_and_mirror(redis_backend, tmp_path: Path) -> None:
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000001.SZ")})
    assert strategy_cache.read_cache(tmp_path) is not None

    strategy_cache.clear_cache(tmp_path)

    assert strategy_cache.read_cache(tmp_path) is None
    assert strategy_cache_redis._MIRROR == {}


def test_shared_mirror_hit_skips_redis_read(redis_backend, tmp_path: Path) -> None:
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"s1": _result("000001.SZ")})
    client = redis_backend.client()
    hgetall_calls = {"n": 0}
    real_hgetall = client.hgetall

    def counting(key):
        hgetall_calls["n"] += 1
        return real_hgetall(key)

    client.hgetall = counting
    first = strategy_cache.read_cache(tmp_path)
    second = strategy_cache.read_cache(tmp_path)
    third = strategy_cache.read_cache(tmp_path)
    client.hgetall = real_hgetall

    # 写入后镜像被清，首次读从 Redis 重建一次；后续命中同一对象，不再拉取。
    assert first is second is third
    assert hgetall_calls["n"] == 1
