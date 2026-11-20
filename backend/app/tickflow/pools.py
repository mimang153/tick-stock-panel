"""标的池(Universe)定义(§6.3)。

Phase 1 实现:
  - 常用指数成份(沪深 300 / 中证 500 / 上证 50)用 TickFlow `quote.pool` 端点拉取并缓存
  - 全 A 通过 instruments.batch 获取
  - 自选池 = 用户的 watchlist
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date
from pathlib import Path
from typing import Any, Literal

import polars as pl

from app.config import settings
from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

PoolId = Literal["CSI300", "CSI500", "SSE50", "CN_Equity_A", "CN_Index", "watchlist"]

# TickFlow universe id 是它内部命名(见 tf.universes.list())。
# 没有官方对照表,启动时按名称模糊匹配从 universes.list() 里找。
# 常见名:沪深300 / 中证500 / 上证50 / 全 A
_POOL_NAME_HINTS = {
    "CSI300": ["沪深300", "HS300", "CSI300"],
    "CSI500": ["中证500", "ZZ500", "CSI500"],
    "SSE50": ["上证50", "SH50", "SSE50"],
}


def _find_universe_id(hints: list[str]) -> str | None:
    """从 universes.list() 里按 name/id 子串匹配找一个 universe id。"""
    try:
        tf = get_client()
        unis = tf.universes.list()
    except Exception as e:  # noqa: BLE001
        logger.warning("universes.list failed: %s", e)
        return None
    for u in unis or []:
        item = (
            u
            if isinstance(u, dict)
            else {"id": getattr(u, "id", ""), "name": getattr(u, "name", "")}
        )
        haystack = (item.get("id", "") + " " + item.get("name", "")).lower()
        for h in hints:
            if h.lower() in haystack:
                return item["id"]
    return None


_POOL_TTL_S = 24 * 3600


def _pool_cache_path(pool_id: str) -> Path:
    return settings.data_dir / "pools" / f"{pool_id}.parquet"


def _pool_cache_key(pool_id: str) -> str:
    digest = hashlib.sha256(str(settings.data_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"pool:{pool_id}:{digest}"


def _cache_backend() -> Any | None:
    from app.storage.runtime import get_storage

    runtime = get_storage()
    return runtime.cache if runtime is not None else None


def _read_pool_from_cache(cache: Any, key: str) -> list[str] | None:
    payload = cache.get_json(key)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        return None
    return [str(symbol) for symbol in payload["symbols"]]


def _write_pool_to_cache(cache: Any, key: str, symbols: list[str]) -> None:
    payload = {
        "symbols": symbols,
        "as_of": date.today().isoformat(),
        "schema": 1,
    }
    try:
        cache.set_json(key, payload, ttl_s=_POOL_TTL_S)
    except Exception as exc:  # noqa: BLE001 - 缓存写入失败不影响本地原子落盘
        logger.warning("写入标的池缓存失败(%s): %s", key, exc)


def get_pool(pool_id: PoolId, refresh: bool = False) -> list[str]:
    """返回标的池里的 symbol 列表。"""
    if pool_id == "watchlist":
        return _load_watchlist()

    backend = _cache_backend()
    key = _pool_cache_key(pool_id)
    cache_ok = backend is not None
    if cache_ok and not refresh:
        try:
            symbols = _read_pool_from_cache(backend, key)
        except Exception as exc:  # noqa: BLE001 - 缓存故障时降级到本地 parquet 离线回退
            logger.warning("读取标的池缓存失败(%s): %s", key, exc)
            cache_ok = False
        else:
            if symbols is not None:
                return symbols

    if cache_ok:
        symbols = _fetch_pool(pool_id)
        if symbols:
            _write_pool_to_cache(backend, key, symbols)
        return symbols

    # 本地 parquet 仅作 CacheBackend 不可用时的离线回退，不再无条件双写。
    cache = _pool_cache_path(pool_id)
    if cache.exists() and not refresh:
        df = pl.read_parquet(cache)
        return df["symbol"].to_list()

    symbols = _fetch_pool(pool_id)
    if symbols:
        from app.services.fs_utils import atomic_write_parquet

        cache.parent.mkdir(parents=True, exist_ok=True)
        # 原子写: 半截缓存会让上面的 read_parquet 每次都抛错, 不会自愈
        atomic_write_parquet(
            pl.DataFrame({"symbol": symbols, "as_of": [date.today()] * len(symbols)}),
            cache,
        )
    return symbols


def _fetch_pool(pool_id: PoolId) -> list[str]:  # noqa: C901 - 既有 TickFlow 多分支池拉取逻辑，本次仅接入缓存
    """从 TickFlow 拉取池成份。

    实现:先用 universes.list 找到 universe id,再 quotes.get_by_universes 拉成份。
    """
    tf = get_client()

    if pool_id in _POOL_NAME_HINTS:
        uid = _find_universe_id(_POOL_NAME_HINTS[pool_id])
        if not uid:
            logger.warning("无法在 TickFlow universes 列表里匹配到 %s", pool_id)
            return []
        try:
            df = tf.quotes.get_by_universes([uid], as_dataframe=True)
            if df is not None and len(df) > 0 and "symbol" in df.columns:
                return df["symbol"].astype(str).tolist()
        except Exception as e:  # noqa: BLE001
            logger.warning("fetch pool %s via universe %s failed: %s", pool_id, uid, e)

    if pool_id == "CN_Equity_A":
        # 全 A — 优先直接用 CN_Equity_A universe (包含沪深京三市)
        uid = _find_universe_id(["CN_Equity_A", "沪深京A股", "全A"])
        if uid:
            try:
                df = tf.quotes.get_by_universes([uid], as_dataframe=True)
                if df is not None and len(df) > 0 and "symbol" in df.columns:
                    return sorted(set(df["symbol"].astype(str).tolist()))
            except Exception as e:  # noqa: BLE001
                logger.warning("fetch CN_Equity_A via universe %s failed: %s", uid, e)

        # fallback: 聚合申万一级行业 (覆盖度较低, 缺北交所/新股)
        try:
            unis = tf.universes.list()
        except Exception as e:  # noqa: BLE001
            logger.warning("universes.list failed: %s", e)
            unis = []
        sw1_ids = []
        for u in unis or []:
            item = u if isinstance(u, dict) else {"id": getattr(u, "id", "")}
            uid = item.get("id", "")
            if "SW1_" in uid:
                sw1_ids.append(uid)
        if sw1_ids:
            try:
                df = tf.quotes.get_by_universes(sw1_ids, as_dataframe=True)
                if df is not None and "symbol" in df.columns:
                    return sorted(set(df["symbol"].astype(str).tolist()))
            except Exception as e:  # noqa: BLE001
                logger.warning("aggregate SW1 fetch failed: %s", e)

    if pool_id == "CN_Index":
        uid = _find_universe_id(["CN_Index", "沪深指数", "指数"])
        ids = [uid] if uid else ["CN_Index"]
        try:
            df = tf.quotes.get_by_universes(ids, as_dataframe=True)
            if df is not None and len(df) > 0 and "symbol" in df.columns:
                return sorted(set(df["symbol"].astype(str).tolist()))
        except Exception as e:  # noqa: BLE001
            logger.warning("fetch CN_Index via universe %s failed: %s", ids, e)

    return []


def _load_watchlist() -> list[str]:
    """读取用户自选(由 watchlist service 维护)。"""
    path = settings.data_dir / "user_data" / "watchlist.parquet"
    if not path.exists():
        return []
    df = pl.read_parquet(path)
    if df.is_empty() or "symbol" not in df.columns:
        return []
    return df["symbol"].to_list()
