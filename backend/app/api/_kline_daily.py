"""日 K 辅助：标的信息、涨跌停、前收、enriched 组装与实时蜡烛。"""

from __future__ import annotations

import math
from datetime import date, timedelta

from fastapi import HTTPException, Request

from app.api._root_ref import bind as _bind
from app.db_safe import is_valid_ext_ident
from app.price_limits import (
    is_no_limit_day,
    is_risk_warning_name,
    parse_listing_date,
    price_limit_pct,
)

from ._kline_http import _gzip_payload

_root = _bind("app.api.kline")
logger = _root.logger


def _get_stock_info(repo, symbol: str) -> dict:
    """从 instruments 内存缓存查标的名称 + 股本。

    该接口在个股弹窗打开时每秒被调用 (SSE invalidate 触发重拉), 走
    repo.get_instruments() 的 Polars 内存缓存按 symbol 过滤, 不再每请求
    DuckDB 扫 instruments parquet。列缺失时返回空 dict, 与旧 SQL 报错路径一致。
    """
    import polars as pl

    try:
        df = repo.get_instruments()
        needed = ("symbol", "name", "total_shares", "float_shares")
        if df.is_empty() or not all(c in df.columns for c in needed):
            return {}
        hit = df.filter(pl.col("symbol") == symbol).head(1)
        if hit.is_empty():
            return {}
        return {
            "name": hit["name"][0],
            "total_shares": hit["total_shares"][0],
            "float_shares": hit["float_shares"][0],
        }
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return {}


def _get_asset_info(repo, symbol: str, asset_type: str) -> dict:
    """非股票标的 (ETF / 指数) 的名称信息 — 从对应 instruments 缓存查, 无股本概念。"""
    import polars as pl

    try:
        df = repo.get_instruments_asset(asset_type)
        if df.is_empty() or "symbol" not in df.columns or "name" not in df.columns:
            return {}
        hit = df.filter(pl.col("symbol") == symbol).head(1)
        if hit.is_empty():
            return {}
        return {"name": hit["name"][0]}
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return {}


def _get_price_limit_info(  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
    repo,
    symbol: str,
    trade_date: date,
    asset_type: str,
    instrument_name: str | None,
) -> dict | None:
    """Return the date-aware limit rule and today's authoritative prices."""
    if asset_type == "index":
        return None

    info: dict = {
        "rate": price_limit_pct(
            symbol,
            trade_date,
            is_risk_warning=(asset_type == "stock" and is_risk_warning_name(instrument_name)),
        ),
        "limit_up": None,
        "limit_down": None,
        "no_limit": False,
        "source": "rule",
    }

    # instrument 行一次取出: 今日权威涨跌停价 + listing_date 窗口判定共用
    row: dict | None = None
    try:
        import polars as pl

        instruments = repo.get_instruments_asset(asset_type)
        available = [
            column
            for column in ("symbol", "limit_up", "limit_down", "listing_date")
            if column in instruments.columns
        ]
        if "symbol" in available and len(available) > 1:
            hit = instruments.filter(pl.col("symbol") == symbol).select(available).head(1)
            if not hit.is_empty():
                row = hit.to_dicts()[0]
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        row = None

    # 注册制新股上市初期无涨跌幅: listing_date 命中窗口时 no_limit=True,
    # 压过 rate 与维表值 (前端不再画涨跌停带, y 轴按实际数据自适应)
    if row is not None:
        listing = parse_listing_date(row.get("listing_date"))
        if listing is not None and is_no_limit_day(symbol, listing, trade_date):
            info["no_limit"] = True
            return info

    if trade_date != _root.cn_today():
        return info

    if row is None:
        return info

    has_authoritative_price = False
    for field in ("limit_up", "limit_down"):
        value = row.get(field)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric) and 0 < numeric < 10_000:
            info[field] = numeric
            has_authoritative_price = True
    if has_authoritative_price:
        info["source"] = "instrument"
    return info


def _get_previous_closes(
    repo,
    symbol: str,
    trade_dates: list[date],
    asset_type: str,
) -> dict[date, float | None]:
    """Return the previous trading day's adjusted close for each session."""
    if not trade_dates:
        return {}
    start = min(trade_dates) - timedelta(days=45)
    end = max(trade_dates)
    try:
        daily = repo.get_daily_asset(
            asset_type,
            symbol,
            start,
            end,
            columns=["date", "close"],
        ).sort("date")
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        daily = None
    if daily is None or daily.is_empty():
        return {trade_date: None for trade_date in trade_dates}

    closes: list[tuple[date, float]] = []
    for daily_date, close in daily.select(["date", "close"]).iter_rows():
        if close is None:
            continue
        numeric = float(close)
        if math.isfinite(numeric) and numeric > 0:
            closes.append((daily_date, numeric))

    result: dict[date, float | None] = {}
    for trade_date in trade_dates:
        result[trade_date] = next(
            (close for daily_date, close in reversed(closes) if daily_date < trade_date),
            None,
        )
    return result


def _ensure_daily_history(request, repo, symbol, asset_type, start, end):
    """enriched 为空时补齐历史；繁忙/失败按稳定 HTTP 错误返回。"""
    from app.services import daily_bootstrap
    from app.tickflow.capabilities import CapabilitySet

    capset = getattr(request.app.state, "capabilities", None) or CapabilitySet()
    try:
        qs = getattr(request.app.state, "quote_service", None)
        return daily_bootstrap.ensure_history(
            repo,
            capset,
            symbol,
            asset_type,
            start,
            end,
            on_updated=qs.notify_data_updated if qs else None,
        )
    except Exception as e:
        from app.services.heavy_job_limiter import HeavyJobLimitTimeoutError
        from app.services.shared_work import WorkBusyError

        if isinstance(e, WorkBusyError | HeavyJobLimitTimeoutError):
            raise HTTPException(503, "日K补齐繁忙，请稍后重试", headers={"Retry-After": "2"}) from e
        raise HTTPException(
            status_code=502, detail="日K数据源获取失败，请检查数据源配置或稍后重试"
        ) from e


def _daily_response(
    request, repo, symbol, asset_type, stock_info, stock_name, rows, end, ext_columns, source
):
    """组装单只日K响应并应用压缩偏好。"""
    resp = {
        "symbol": symbol,
        "asset_type": asset_type,
        "name": stock_name,
        "stock_info": stock_info,
        "rows": rows,
        "source": source,
    }
    resp["quote_freshness"] = _daily_freshness(request, rows, end)
    return _gzip_payload(
        request,
        _attach_ext(resp, repo, symbol, ext_columns),
        pref_key="daily_batch_compress",
    )


def _empty_daily_response(request, symbol, asset_type, stock_info, stock_name, end):
    payload = {
        "symbol": symbol,
        "asset_type": asset_type,
        "name": stock_name,
        "stock_info": stock_info,
        "rows": [],
        "quote_freshness": _daily_freshness(request, [], end),
    }
    return _gzip_payload(request, payload, pref_key="daily_batch_compress")


def _enriched_rows(enriched, symbol: str, start, end) -> list[dict]:
    import polars as pl

    return (
        enriched.filter((pl.col("symbol") == symbol) & pl.col("date").is_between(start, end))
        .sort("date")
        .to_dicts()
    )


def _daily_freshness(request: Request, rows: list[dict], end: date) -> dict:
    from app.services.quote_freshness import QuoteFreshness

    return QuoteFreshness.for_request(request).describe(
        rows[-1] if rows else None, historical=end < _root.cn_today()
    )


def _attach_ext(resp: dict, repo, symbol: str, ext_columns: str | None) -> dict:
    """按 ext_columns 规格为单只股票 LEFT JOIN 扩展数据，平铺到 stock_info['ext']。

    key 形如 "{config_id}__{field_name}"，与自选列表 enriched 接口保持一致。
    委托 screener._load_ext_value_maps 取值: 复用其 (路径,mtime) 签名缓存,
    个股弹窗每秒重拉时不再重复读 ext parquet; 任何 ext 表/字段缺失都静默跳过。
    """
    if not ext_columns or not ext_columns.strip():
        return resp

    specs: list[tuple[str, str]] = []
    for part in ext_columns.split(","):
        part = part.strip()
        if "." not in part:
            continue
        config_id, field_name = part.split(".", 1)
        config_id, field_name = config_id.strip(), field_name.strip()
        if config_id and field_name and is_valid_ext_ident(config_id):
            specs.append((config_id, field_name))
    if not specs:
        return resp

    try:
        from app.api.screener import _load_ext_value_maps

        value_maps = _load_ext_value_maps(repo, ext_columns)
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        value_maps = {}

    ext_values: dict = {}
    for config_id, field_name in specs:
        ext_col_name = f"{config_id}__{field_name}"
        vmap = value_maps.get(ext_col_name) or {}
        ext_values[ext_col_name] = vmap.get(symbol)

    stock_info = dict(resp.get("stock_info") or {})
    stock_info["ext"] = ext_values
    resp["stock_info"] = stock_info
    return resp


_LIVE_INDICATOR_KEYS = (
    "ma5",
    "ma10",
    "ma20",
    "ma30",
    "ma60",
    "macd_dif",
    "macd_dea",
    "macd_hist",
    "kdj_k",
    "kdj_d",
    "kdj_j",
    "boll_upper",
    "boll_lower",
    "rsi_6",
    "rsi_14",
    "rsi_24",
    "atr_14",
    "vol_ratio_5d",
)
_LIVE_LIMIT_KEYS = (
    "raw_low",
    "raw_close",
    "limit_down_price",
    "signal_limit_down",
    "signal_limit_down_recovery",
    "signal_limit_up",
    "signal_broken_limit_up",
    "consecutive_limit_ups",
)


def _live_candle_row(symbol: str, enriched_date, q: dict) -> dict:
    """按完整日K接口字段契约投影单条实时 enriched 行。"""
    close_price = q.get("close")
    raw_open, raw_high, raw_low = q.get("open"), q.get("high"), q.get("low")
    live_row = {
        "date": str(enriched_date),
        "symbol": symbol,
        "open": raw_open if raw_open and raw_open > 0 else close_price,
        "high": raw_high if raw_high and raw_high > 0 else close_price,
        "low": raw_low if raw_low and raw_low > 0 else close_price,
        "close": close_price,
        "volume": q.get("volume"),
        "amount": q.get("amount"),
        "change_pct": q.get("change_pct"),
        "is_live": True,
    }
    if q.get("quote_ts") is not None:
        live_row["quote_ts"] = q["quote_ts"]
    for key in _LIVE_INDICATOR_KEYS:
        if key in q and q[key] is not None:
            live_row[key] = q[key]
    for key in _LIVE_LIMIT_KEYS:
        live_row[key] = q.get(key)
    return live_row


def _latest_live_candle(
    request: Request,
    symbol: str,
    asset_type: str = "stock",
    *,
    refresh_asset: bool = True,
) -> dict | None:
    """从内存缓存读取单只标的的当日实时 enriched 行。"""

    if asset_type == "stock":
        qs = getattr(request.app.state, "quote_service", None)
        if not qs:
            return None
        df_today, enriched_date = qs.get_enriched_today()
    elif asset_type in {"etf", "index"}:
        df_today, enriched_date = request.app.state.repo.get_enriched_latest_asset(
            asset_type,
            refresh=refresh_asset,
        )
    else:
        return None
    if df_today.is_empty():
        return None

    # 非交易日(周末/假日)缓存日期 != 北京今天, 跳过注入避免产生重复蜡烛。
    # 必须用 _root.cn_today(): 美洲时区主机整个 A 股交易时段本地日期落后北京一天,
    # 旧代码盘中直接丢K。UTC 主机盘中(UTC 1:30-7:00)本地日期与北京相同, 并不丢K;
    # UTC 的旧症状是北京 00:00-08:00 把昨日残留快照误当实时K注入。
    if not enriched_date or enriched_date != _root.cn_today():
        return None

    # 查找该 symbol 的实时 enriched 行
    import polars as pl

    try:
        q = df_today.filter(pl.col("symbol") == symbol).to_dicts()
        if not q:
            return None
        q = q[0]
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return None

    close_price = q.get("close")
    if not close_price or close_price <= 0:
        return None
    return _live_candle_row(symbol, enriched_date, q)


def _maybe_inject_live_candle(
    request: Request, symbol: str, rows: list[dict], asset_type: str = "stock"
) -> list[dict]:
    """如果有当日实时 enriched 数据, 用实时数据生成今日蜡烛并追加/覆盖。"""
    live_row = _latest_live_candle(request, symbol, asset_type)
    if live_row is None:
        return rows

    # 如果已有今天的 enriched 行, 覆盖; 否则追加
    found = False
    for r in rows:
        if str(r.get("date")) == live_row["date"]:
            r.update(live_row)
            found = True
            break

    if not found:
        rows.append(live_row)

    return rows


class DailyBatchRequest:
    """批量日K请求。"""

    symbols: list[str]
    days: int = 12


def _read_daily_batch_range(repo, sym_type: dict[str, str], syms: list[str], start, end, cols):
    """按资产类型批量读取日K窗口。"""
    import polars as pl

    stock_syms = [x for x in syms if sym_type[x] not in ("etf", "index")]
    frames: list[pl.DataFrame] = []
    if stock_syms:
        df_stock = repo.get_daily_batch(stock_syms, start, end, columns=cols)
        if not df_stock.is_empty():
            frames.append(df_stock)
    for symbol in syms:
        if sym_type[symbol] == "etf":
            sub = repo.get_etf_daily(symbol, start, end, columns=cols)
        elif sym_type[symbol] == "index":
            sub = repo.get_index_daily(symbol, start, end, columns=cols)
        else:
            continue
        if not sub.is_empty():
            frames.append(sub)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _tail_daily_frames(df, days: int):
    """按 symbol 取最近 N 条，全程保持 DataFrame 不转 dict。"""
    out: dict = {}
    if df.is_empty():
        return out
    for part in df.partition_by("symbol", maintain_order=True):
        sub = part.sort("date").tail(days)
        if not sub.is_empty():
            out[sub["symbol"][0]] = sub
    return out
