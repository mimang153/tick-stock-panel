"""策略结果缓存的 Redis 共享实现（RC-05）。

只在 ``CACHE_BACKEND=redis`` 且存储 runtime 已装配 RedisCacheBackend 时启用；
其余情况仍走 ``strategy_cache.py`` 原有本地文件逻辑，保证默认部署零变化。

数据结构是一个 Hash（键 ``{prefix}strategy:{digest}``，不含绝对路径）：
- ``as_of`` / ``updated_at`` / ``enriched_mtime``：快照元信息；
- ``result:{sid}``：该策略当前结果 JSON；
- ``ever:{sid}``：该策略今日曾命中 ``{symbol: row}`` JSON；
- ``version``：每次写入递增，供进程内镜像判失效。

写入用 Lua 在 Redis 端一次完成“读旧值 → 同日并集合并 / 跨日重置 → 写新值 → bump”，
多实例并发不会丢其它策略或今日曾命中的并集；单次只改当前策略的两个 field，
不搬运整包 13MB。读侧进程内镜像以 version 判失效，镜像命中时只做一次 HGET。
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MIRROR: dict[str, tuple[int, dict]] = {}

# KEYS[1]=hash key; ARGV[1]=as_of; ARGV[2]=updated_at; ARGV[3]=enriched_mtime or '';
# ARGV[4]=results JSON。返回值是递增后的 version。
_WRITE_LUA = """
local key = KEYS[1]
local as_of = ARGV[1]
local results = cjson.decode(ARGV[4])
local old_as_of = redis.call('HGET', key, 'as_of')
local same_day = (old_as_of == as_of)
if not same_day then
  local fields = redis.call('HKEYS', key)
  for _, field in ipairs(fields) do
    if string.sub(field, 1, 7) == 'result:' or string.sub(field, 1, 5) == 'ever:' then
      redis.call('HDEL', key, field)
    end
  end
  redis.call('HSET', key, 'as_of', as_of)
end
for sid, result in pairs(results) do
  redis.call('HSET', key, 'result:' .. sid, cjson.encode(result))
  local ever = {}
  if same_day then
    local previous = redis.call('HGET', key, 'ever:' .. sid)
    if previous then
      local decoded = cjson.decode(previous)
      if type(decoded) == 'table' then ever = decoded end
    end
  end
  local rows = result['rows']
  if type(rows) == 'table' then
    for _, row in ipairs(rows) do
      local symbol = row['symbol']
      if symbol then ever[symbol] = row end
    end
  end
  redis.call('HSET', key, 'ever:' .. sid, cjson.encode(ever))
end
redis.call('HSET', key, 'updated_at', ARGV[2])
if ARGV[3] ~= '' then
  redis.call('HSET', key, 'enriched_mtime', ARGV[3])
else
  redis.call('HDEL', key, 'enriched_mtime')
end
return redis.call('HINCRBY', key, 'version', 1)
"""


def _store() -> Any | None:
    """返回装配好的 RedisCacheBackend；未装配或非 Redis 后端返回 None。"""
    from app.storage.cache import RedisCacheBackend
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None or not isinstance(runtime.cache, RedisCacheBackend):
        return None
    return runtime.cache


def _key(backend: Any, data_dir: Path) -> str:
    digest = hashlib.sha256(str(data_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"{backend.entry_prefix()}strategy:{digest}"


def _text(raw: bytes | None) -> str | None:
    return None if raw is None else raw.decode("utf-8")


def _assemble(fields: dict[bytes, bytes]) -> dict:
    """把 Hash 的全量字段还原为 ``read_cache`` 约定的 payload 结构。"""
    payload: dict[str, Any] = {
        "as_of": None,
        "results": {},
        "today_ever_matched": {},
        "today_ever_rows": {},
        "enriched_mtime": None,
        "updated_at": None,
    }
    for raw_key, raw_value in fields.items():
        key = raw_key.decode("utf-8")
        if key.startswith("result:"):
            payload["results"][key[len("result:") :]] = json.loads(raw_value.decode("utf-8"))
        elif key.startswith("ever:"):
            sid = key[len("ever:") :]
            decoded = json.loads(raw_value.decode("utf-8"))
            rows = decoded if isinstance(decoded, dict) else {}
            payload["today_ever_rows"][sid] = rows
            payload["today_ever_matched"][sid] = sorted(rows)
        elif key == "as_of":
            payload["as_of"] = raw_value.decode("utf-8")
        elif key == "updated_at":
            payload["updated_at"] = int(raw_value)
        elif key == "enriched_mtime":
            payload["enriched_mtime"] = float(raw_value)
    return payload


def read(data_dir: Path) -> dict | None:
    """读缓存；镜像命中返回共享对象（只读），失效时从 Redis 重建。"""
    backend = _store()
    if backend is None:
        return None
    key = _key(backend, data_dir)
    client = backend.client()
    raw_version = client.hget(key, "version")
    if raw_version is None:
        _MIRROR.pop(key, None)
        return None
    version = int(raw_version)
    cached = _MIRROR.get(key)
    if cached is not None and cached[0] == version:
        return cached[1]
    payload = _assemble(client.hgetall(key))
    _MIRROR[key] = (version, payload)
    return payload


def write(data_dir: Path, as_of: str, results: dict[str, Any], mtime: float | None) -> None:
    """原子写入：同日按策略合并并集，跨日重置，失败直接抛出不让多实例读到分叉。"""
    from app.services.strategy_cache import _json_default

    backend = _store()
    if backend is None:
        raise RuntimeError("Redis 策略缓存需要已装配 RedisCacheBackend")
    key = _key(backend, data_dir)
    payload = json.dumps(results, ensure_ascii=False, default=_json_default)
    version = backend.client().eval(
        _WRITE_LUA,
        1,
        key,
        as_of,
        str(int(time.time() * 1000)),
        "" if mtime is None else str(mtime),
        payload,
    )
    _MIRROR.pop(key, None)
    logger.info("Redis 策略缓存已写入: %s, %d 策略, version=%s", as_of, len(results), version)


def clear(data_dir: Path) -> None:
    """清空该数据目录的缓存 Hash 与本地镜像。"""
    backend = _store()
    if backend is None:
        return
    key = _key(backend, data_dir)
    backend.client().delete(key)
    _MIRROR.pop(key, None)
