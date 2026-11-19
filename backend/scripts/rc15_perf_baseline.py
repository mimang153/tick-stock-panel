"""RC-15 性能标定（ISS-003 §3 验收项）——memory / redis 双模式。

对缓存读写入口与 CK 读写入口按 --samples 次采样，输出 P50/P95/P99（毫秒），
并采集进程峰值 RSS、Redis used_memory / QPS。CK 写延迟用带随机前缀的临时
Memory 表测量，脚本结束即 drop，不污染任何业务表。

输出 JSON 到 --out（默认 data/rc15_perf_baseline.json）。

用法：
    uv run --extra cache-redis python scripts/rc15_perf_baseline.py \\
        --mode both --samples 50 --out data/rc15_perf_baseline.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import resource
import sys
import time
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from app.config import settings
from app.storage.cache import MemoryCacheBackend, RedisCacheBackend
from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore


def _percentiles(samples: list[float]) -> dict:
    """按最近秩法计算 P50/P95/P99，样本为空返回全 None。"""
    if not samples:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    ordered = sorted(samples)

    def pick(q: float) -> float:
        idx = min(len(ordered) - 1, max(0, int(q * len(ordered)) - 1))
        return round(ordered[idx], 3)

    return {"n": len(ordered), "p50_ms": pick(0.50), "p95_ms": pick(0.95), "p99_ms": pick(0.99)}


def _sample(fn: Callable[[], Any], n: int) -> list[float]:
    """采集 n 次调用的耗时毫秒数，异常记为一次快速失败样本并重抛。"""
    out: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t0) * 1000)
    return out


def _redis_info(url: str) -> dict:
    """独立连接读 Redis INFO，返回内存字节与已处理命令数。"""
    import redis

    client = redis.Redis.from_url(url, decode_responses=False, socket_timeout=2.0)
    try:
        mem = client.info("memory")
        stats = client.info("stats")
        return {
            "used_memory_bytes": int(mem.get("used_memory", 0)),
            "total_commands_processed": int(stats.get("total_commands_processed", 0)),
        }
    finally:
        client.close()


def _run_mode(mode: str, store: ClickHouseStore, samples: int) -> dict:
    """跑单模式标定：cache 读写/generation + CK 读写入口。"""
    if mode == "redis":
        cache: Any = RedisCacheBackend(settings.cache_redis_url, prefix=settings.cache_redis_prefix)
    else:
        cache = MemoryCacheBackend()
    result: dict = {"mode": mode, "scenarios": {}}

    # 缓存读写/generation：每次写独立键避免命中干扰，读刚写的键。
    df = pl.DataFrame({"x": list(range(64))})
    counter = itertools.count()
    result["scenarios"]["cache_set_json"] = _percentiles(
        _sample(lambda: cache.set_json(f"perf:{next(counter)}:j", {"v": 1}, ttl_s=60), samples)
    )
    result["scenarios"]["cache_set_df"] = _percentiles(
        _sample(lambda: cache.set(f"perf:{next(counter)}:df", df, ttl_s=60), samples)
    )
    result["scenarios"]["cache_get_json"] = _percentiles(
        _sample(lambda: cache.get_json("perf:0:j"), samples)
    )
    result["scenarios"]["cache_get_df"] = _percentiles(
        _sample(lambda: cache.get("perf:0:df"), samples)
    )
    result["scenarios"]["generation_bump"] = _percentiles(
        _sample(lambda: cache.bump("perf"), samples)
    )

    # CK 读：单票日K（小结果集，反映点查延迟）。
    result["scenarios"]["ck_read_daily"] = _percentiles(
        _sample(
            lambda: store.read_daily(
                "kline_daily", symbols=["000001.SZ"], start=date(2026, 1, 1), end=date(2026, 3, 31)
            ),
            samples,
        )
    )

    # CK 写：随机前缀临时 Memory 表，写后即删，不污染业务表。
    client = store._client
    tmp = f"t_perf_{uuid.uuid4().hex[:10]}"
    row = pl.DataFrame({"date": [date(2026, 1, 1)], "v": [1.0]})
    try:
        client.execute(f"CREATE TABLE {tmp} (date Date, v Float64) ENGINE = Memory")
        result["scenarios"]["ck_write_row"] = _percentiles(
            _sample(lambda: client.insert(tmp, row), samples)
        )
    finally:
        client.execute(f"DROP TABLE IF EXISTS {tmp}")

    # 进程峰值 RSS（两种模式都记录，便于对比缓存后内存变化）。
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    result["peak_rss_mb"] = round(rss_mb, 1)

    if mode == "redis":
        before = _redis_info(settings.cache_redis_url)
        # 用一段真实缓存负载估算 QPS Redis 侧命令增量。
        t0 = time.perf_counter()
        for i in range(samples):
            key = f"perf:qps:{i % 16}"
            cache.set_json(key, {"i": i}, ttl_s=30)
            cache.get_json(key)
        elapsed = max(1e-6, time.perf_counter() - t0)
        after = _redis_info(settings.cache_redis_url)
        cmds = after["total_commands_processed"] - before["total_commands_processed"]
        result["redis"] = {
            "used_memory_bytes": after["used_memory_bytes"],
            "commands_during_qps_probe": cmds,
            "probe_seconds": round(elapsed, 3),
            "approx_qps": round(cmds / elapsed, 1),
        }
        cache.invalidate("perf:")
    else:
        result["redis"] = None

    close = getattr(cache, "close", None)
    if close is not None:
        close()
    return result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="/app/data/rc15_perf_baseline.json")
    p.add_argument("--samples", type=int, default=50)
    p.add_argument("--mode", choices=["memory", "redis", "both"], default="both")
    args = p.parse_args()

    client = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    client.connect()
    store = ClickHouseStore(client)
    modes = ["memory", "redis"] if args.mode == "both" else [args.mode]
    out: dict = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "samples_per_scenario": args.samples,
        "modes": {},
    }
    for mode in modes:
        out["modes"][mode] = _run_mode(mode, store, args.samples)

    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
