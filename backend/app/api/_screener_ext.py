"""筛选器扩展列投影：value_map 缓存与实时行情展示字段覆盖。"""

from __future__ import annotations

import glob as _glob
import math
import os
from typing import Any

from fastapi import HTTPException, Request

from app.api._root_ref import bind as _bind

_root = _bind("app.api.screener")
logger = _root.logger


def _safe(result_dict: dict) -> dict:
    """sanitize for JSON(NaN / Inf → None)."""
    rows = result_dict.get("rows", [])
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, float) and not math.isfinite(v):
                r[k] = None
    return result_dict


# ── 扩展列 value_map 缓存 ────────────────────────────────────────────
# 每次请求 _load_ext_value_maps 都会重新从磁盘读 ext parquet 并重建 {symbol: value}。
# 用底层 parquet 文件的 (路径, mtime) 签名做 memoize: 文件未变则复用上次的 map,
# parquet 被重写 (mtime 变化) 时自动失效重算。仅缓存基于 config 的快照/时序路径,
# 无 config 的 DuckDB view 回退路径不缓存 (少见)。
# 键含目标日期: 同一扩展列的「结果日分区」与「最新分区」是两个独立映射, 不可混用。
_ext_value_map_cache: dict[tuple[str, str, str], tuple[Any, dict[str, Any]]] = {}


def _ext_parquet_signature(cfg, data_dir) -> tuple | None:
    """该扩展配置底层 parquet 文件的 (路径, mtime) 签名; 出错返回 None (禁用缓存)。"""
    try:
        from app.api.ext_data import _parquet_glob

        pattern = _parquet_glob(cfg, data_dir)
        files = sorted(_glob.glob(pattern, recursive=True))
        if not files:
            return None
        return tuple((f, os.path.getmtime(f)) for f in files)
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return None


def _ext_maps_for_dates(
    repo, ext_columns: str | None, dates
) -> dict[str | None, dict[str, dict[str, Any]]]:
    """按日期集合构建多份 ext 投影映射 (缓存 payload 的结果/今日曾命中可能跨日)。

    无 ext_columns 时返回 {} (调用方视为"无投影")。None 键 = 无日期结果, 取最新分区。
    """
    if not ext_columns:
        return {}
    keys = {str(d) if d else None for d in dates}
    return {k: _root._load_ext_value_maps(repo, ext_columns, as_of=k) for k in keys}


def _maps_for_date(
    maps_by_date: dict[str | None, dict[str, dict[str, Any]]], date
) -> dict[str, dict[str, Any]]:
    key = str(date) if date else None
    return maps_by_date.get(key) or maps_by_date.get(None) or {}


def _row_with_ext(
    row: dict, ext_values: dict[str, dict[str, Any]], symbol: str | None = None
) -> dict:
    next_row = dict(row)
    sym = symbol or next_row.get("symbol")
    for out_col, value_map in ext_values.items():
        next_row[out_col] = value_map.get(str(sym)) if sym else None
    return next_row


def _rows_with_ext(rows: list[dict], ext_values: dict[str, dict[str, Any]]) -> list[dict]:
    if not ext_values:
        return rows
    return [_row_with_ext(r, ext_values) for r in rows]


def _result_with_ext(result_dict: dict, ext_values: dict[str, dict[str, Any]]) -> dict:
    if not ext_values:
        return result_dict
    return {**result_dict, "rows": _rows_with_ext(result_dict.get("rows", []), ext_values)}


def _results_with_ext(
    results: dict[str, dict], ext_values: dict[str, dict[str, Any]]
) -> dict[str, dict]:
    if not ext_values:
        return results
    return {sid: _result_with_ext(r, ext_values) for sid, r in results.items()}


def _cache_payload_with_ext(
    cached: dict, maps_by_date: dict[str | None, dict[str, dict[str, Any]]]
) -> dict:
    """把 ext 投影按各自日期合并进缓存 payload (结果行与今日曾命中行各用其 date)。"""
    if not maps_by_date:
        return cached

    payload = dict(cached)
    results = cached.get("results") or {}
    payload["results"] = {
        sid: _result_with_ext(r, _maps_for_date(maps_by_date, r.get("as_of")))
        if isinstance(r, dict)
        else r
        for sid, r in results.items()
    }

    ever_rows = cached.get("today_ever_rows")
    if isinstance(ever_rows, dict):
        enriched_ever: dict[str, dict[str, dict]] = {}
        for sid, sym_map in ever_rows.items():
            if not isinstance(sym_map, dict):
                continue
            enriched_ever[sid] = {
                sym: _row_with_ext(row, _maps_for_date(maps_by_date, row.get("date")), symbol=sym)
                for sym, row in sym_map.items()
                if isinstance(row, dict)
            }
        payload["today_ever_rows"] = enriched_ever

    return payload


# 被实时行情覆盖的展示字段。
#
# 策略结果缓存只记录「计算那一刻」的价格: 盘中运行 run_all 后, 列表里的
# close/change_pct 会一直停在那时, 而点开详情走的是实时 enriched 缓存 ——
# 同一个票在两个地方显示不同涨幅 (实测 13:27 的扫描结果到收盘差了 2 个点)。
# 这里在返回前用当前实时快照覆盖这些纯展示字段; 指标列 (ma20/score/signal_*)
# 保持计算时点值不动, 否则等于用新价格重算了一个过期策略结果。
_LIVE_QUOTE_FIELDS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "volume",
    "amount",
    "change_pct",
    "change_amount",
    "turnover_rate",
    "raw_close",
    "raw_high",
    "raw_low",
    "quote_ts",
)


def _live_quote_overlay(repo) -> tuple[dict[str, dict[str, Any]], str | None]:
    """当前实时 enriched 快照的展示字段: ({symbol: row}, 日期字符串)。

    缓存为空 (启动预热中) 或 repo 不支持该接口时返回空映射, 调用方原样返回
    策略结果 —— 拿不到实时数据时不能伪造新价格。
    """
    loader = getattr(repo, "get_enriched_latest", None)
    if not callable(loader):
        return {}, None
    try:
        frame, cache_date = loader()
    except Exception:  # noqa: BLE001 - 实时行情叠加失败时回退缓存数据
        logger.debug("live quote overlay unavailable", exc_info=True)
        return {}, None
    if frame is None or frame.is_empty() or cache_date is None:
        return {}, None
    columns = [c for c in ("symbol", *_LIVE_QUOTE_FIELDS) if c in frame.columns]
    if "symbol" not in columns:
        return {}, None
    return {str(row["symbol"]): row for row in frame.select(columns).to_dicts()}, str(cache_date)


def _quote_date_matches(row: dict, live_date: str | None, result_as_of: Any = None) -> bool:
    """结果日期与实时快照同日才可覆盖。

    历史日期结果 (例如回看昨天/上周) 必须保留当时价格, 不能用今天的行情覆盖。
    优先用结果级 as_of (策略结果整体属于哪一天), 行级 date 作兜底 —— 个别
    策略输出行不带 date 字段, 只看行日期会误覆盖历史结果。
    """
    if live_date is None:
        return False
    for candidate in (result_as_of, row.get("date")):
        if candidate is None:
            continue
        return str(candidate)[:10] == live_date
    # 结果与行都没有日期: 视为「最新」(与 as_of=None 的缓存语义一致)
    return True


def _rows_with_live_quotes(
    rows: list[dict],
    overlay: dict[str, dict[str, Any]],
    live_date: str | None,
    result_as_of: Any = None,
) -> list[dict]:
    if not overlay:
        return rows
    merged: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            merged.append(row)
            continue
        live = overlay.get(str(row.get("symbol") or ""))
        if live is None or not _quote_date_matches(row, live_date, result_as_of):
            merged.append(row)
            continue
        next_row = dict(row)
        for key, value in live.items():
            if key == "symbol":
                continue
            next_row[key] = value
        merged.append(next_row)
    return merged


def _cached_with_live_quotes(request: Request) -> dict:
    """盘后缓存 + 监控实时结果 + 当前行情展示字段覆盖。

    与 `_cached_with_realtime` 的区别: 后者只覆盖「加入监控的策略」(每轮行情
    重算结果), 未加监控的策略仍显示计算时点价格。这里对所有策略只覆盖价格类
    展示字段, 行情轮询期间列表与详情保持一致。
    """
    cached = _root._cached_with_realtime(request)
    results = cached.get("results") or {}
    ever_rows = cached.get("today_ever_rows")
    if not results and not ever_rows:
        return cached

    overlay, live_date = _live_quote_overlay(request.app.state.repo)
    if not overlay:
        return cached

    payload = dict(cached)
    payload["results"] = {
        sid: (
            {
                **result,
                "rows": _rows_with_live_quotes(
                    result.get("rows") or [],
                    overlay,
                    live_date,
                    result.get("as_of"),
                ),
            }
            if isinstance(result, dict)
            else result
        )
        for sid, result in results.items()
    }

    if isinstance(ever_rows, dict):
        # 「今日曾命中」行是逐行的历史快照 (各自带 date), 一律按行日期判断
        payload["today_ever_rows"] = {
            sid: (
                {
                    symbol: _rows_with_live_quotes([row], overlay, live_date)[0]
                    for symbol, row in sym_map.items()
                    if isinstance(row, dict)
                }
                if isinstance(sym_map, dict)
                else sym_map
            )
            for sid, sym_map in ever_rows.items()
        }
    return payload


def _parse_ext_columns(ext_columns: str) -> list[tuple[str, str, str | None]]:
    """解析并去重扩展列；同一输出列存在歧义时返回 400。"""
    from app.services.ext_data import parse_ext_columns

    try:
        return parse_ext_columns(ext_columns)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
