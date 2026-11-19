"""RC-14 应用层 Redis 冒烟（ISS-003 §3 验收项）。

在进程内以 ``CACHE_BACKEND=redis`` 初始化存储 runtime，逐项验证：

1. 健康检查 ``cache_health()`` 报告 redis / ok；
2. 池缓存读：第一次取数回填 Redis，第二次命中缓存（本地 parquet 改名后仍读到）；
3. generation bump：bump 后命名空间内旧条目失效、generation 递增；
4. flush（cache.clear）后重新取数正常，且 generation 计数器保留。

输出 JSON 报告到 --out（默认 data/rc14_redis_smoke.json）。任一断言失败以非零码退出。

用法：
    CACHE_BACKEND=redis CACHE_REDIS_URL=redis://:123456@localhost:6379/0 \\
        uv run --extra cache-redis python scripts/rc14_redis_smoke.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# 与 backfill_ck.py 一致：脚本直跑时把 backend/ 放进 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from app.config import settings
from app.storage import runtime as storage_runtime
from app.storage.cache import RedisCacheBackend


def _check(results: dict, name: str, ok: bool, detail: str) -> None:
    results["checks"][name] = {"ok": bool(ok), "detail": detail}
    if not ok:
        results["failed"].append(name)


def _check_generation(cache: RedisCacheBackend, results: dict) -> None:
    """generation bump 后旧条目失效、计数递增。"""
    cache.set_json("smoke:payload", {"v": 1}, ttl_s=60)
    before = cache.generation("smoke")
    after = cache.bump("smoke")
    miss = cache.get_json("smoke:payload") is None
    _check(
        results,
        "generation_bump",
        after == before + 1 and miss,
        f"gen {before}->{after}, old_entry_miss={miss}",
    )


def _check_pool(cache: RedisCacheBackend, results: dict, pool_id: str) -> None:
    """池缓存命中：重命名本地 parquet 后仍从 Redis 读到预置列表。"""
    from app.tickflow import pools

    key = pools._pool_cache_key(pool_id)
    cache.set_json(
        key, {"symbols": ["000001.SZ", "600000.SH"], "as_of": "2026-01-01", "schema": 1}, ttl_s=300
    )
    parquet = pools._pool_cache_path(pool_id)
    renamed = parquet.with_suffix(".parquet.bak")
    had_local = parquet.exists()
    if had_local:
        parquet.rename(renamed)
    try:
        got = pools.get_pool(pool_id)
        _check(results, "pool_cache_read", got == ["000001.SZ", "600000.SH"], f"pool={got}")
    finally:
        if had_local:
            renamed.rename(parquet)
        cache.invalidate(key)


def _check_flush(cache: RedisCacheBackend, results: dict) -> None:
    """flush（clear）后条目消失、generation 保留，重新写入可读回。"""
    cache.set("smoke:df", pl.DataFrame({"x": [1, 2, 3]}), ttl_s=60)
    before = cache.generation("smoke")
    cache.clear()
    miss = cache.get("smoke:df") is None
    after = cache.generation("smoke")
    cache.set("smoke:df2", pl.DataFrame({"x": [4]}), ttl_s=60)
    reget = cache.get("smoke:df2")
    ok = miss and after == before and reget is not None and len(reget) == 1
    _check(
        results,
        "flush_and_reread",
        ok,
        f"df_miss={miss}, gen {before}->{after}, reget_rows={0 if reget is None else len(reget)}",
    )
    cache.invalidate("smoke:")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="/app/data/rc14_redis_smoke.json")
    p.add_argument("--pool-id", default="hs300", help="用哪个标的池做缓存读验证")
    args = p.parse_args()

    url = settings.cache_redis_url
    prefix = settings.cache_redis_prefix
    results: dict = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "redis_url": url,
        "redis_prefix": prefix,
        "checks": {},
        "failed": [],
    }

    # 装配 Redis 缓存 + CK runtime（CK 只用于 runtime 完整性，池数据用本地 parquet 回退）
    cache = RedisCacheBackend(url, prefix=prefix)
    storage_runtime.shutdown_storage()
    storage_runtime.init_storage(cache=cache)

    health = storage_runtime.cache_health()
    _check(
        results,
        "health",
        health.get("backend") == "redis" and health.get("status") == "ok",
        json.dumps(health, ensure_ascii=False),
    )
    _check_generation(cache, results)
    _check_pool(cache, results, args.pool_id)
    _check_flush(cache, results)

    storage_runtime.shutdown_storage()
    results["passed"] = not results["failed"]
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if results["failed"]:
        raise SystemExit(f"RC-14 冒烟失败: {results['failed']}")


if __name__ == "__main__":
    main()
