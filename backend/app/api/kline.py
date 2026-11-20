"""K 线 / 同步 API。"""

from __future__ import annotations

import importlib
import logging
from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from app.market_time import cn_now as cn_now
from app.market_time import cn_today, in_continuous_session
from app.services import (
    kline_sync as kline_sync,
)
from app.services import (
    minute_adjust,
    minute_gaps,
)
from app.services import (
    trading_day as trading_day,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kline", tags=["kline"])

_RAW_PRICE_COLUMNS = ("open", "high", "low", "close", "volume", "amount")


def _merge_raw_prices(rows: list[dict], raw_rows: list[dict]) -> list[dict]:
    """Overlay raw OHLCV onto enriched rows while keeping indicator columns."""
    raw_by_date = {row["date"]: row for row in raw_rows}
    merged: list[dict] = []
    for row in rows:
        raw = raw_by_date.get(row.get("date"))
        if raw is None:
            merged.append(row)
            continue
        updated = dict(row)
        for column in _RAW_PRICE_COLUMNS:
            if column in raw:
                updated[column] = raw[column]
        merged.append(updated)
    return merged


def _rows_with_adjust(
    rows: list[dict],
    *,
    adjust: Literal["forward", "none"],
    symbol: str,
    asset_type: str,
    start: date,
    end: date,
) -> list[dict]:
    """Project enriched rows to raw prices when ``adjust=none`` is requested."""
    if adjust == "forward":
        return rows
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return rows
    table = "kline_etf_daily" if asset_type == "etf" else "kline_daily"
    raw = runtime.store.read_daily(table, symbols=[symbol], start=start, end=end)
    return _merge_raw_prices(rows, raw.to_dicts())


@router.get("/instruments/search")
def search_instruments(
    request: Request,
    q: str = Query("", min_length=0, max_length=50, description="搜索关键词"),
    limit: int = Query(20, ge=1, le=50),
    asset_types: str = Query("stock", description="逗号分隔的资产类型: stock,etf"),
):
    """模糊搜索标的 (代码 / 名称)。从内存 instruments 缓存中查。

    默认只搜股票, 保持既有调用方行为不变; 自选等场景传 asset_types=stock,etf
    可一并搜出 ETF, 结果附带 asset_type 字段供前端区分。
    """
    if not q.strip():
        return {"results": []}

    repo = request.app.state.repo
    import polars as pl

    types = [t.strip() for t in asset_types.split(",") if t.strip()]
    parts = _instrument_frames(repo, types)
    if not parts:
        return {"results": []}
    df = pl.concat(parts, how="vertical")

    keyword = q.strip().upper()

    # code/symbol 前缀优先，再 name 包含匹配
    prefix_mask = pl.col("code").str.starts_with(keyword) | pl.col(
        "symbol"
    ).str.to_uppercase().str.starts_with(keyword)
    contains_mask = (
        pl.col("code").str.contains(keyword, literal=True)
        | pl.col("symbol").str.to_uppercase().str.contains(keyword, literal=True)
        | pl.col("name").str.contains(keyword, literal=True)
    )

    # 分层匹配: ① code/symbol 前缀 → ② 拼音首字母前缀(纯字母输入) → ③ 包含匹配
    prefix_hits = df.filter(prefix_mask).head(limit)
    if prefix_hits.height >= limit:
        matched = prefix_hits
    else:
        collected = [prefix_hits] if prefix_hits.height else []
        seen = set(prefix_hits["symbol"].to_list()) if prefix_hits.height else set()
        remaining = limit - prefix_hits.height

        # ② 拼音首字母前缀: 仅纯字母输入触发 (如 payh → 平安银行); 中文/代码输入零开销跳过
        pinyin_rows = _pinyin_prefix_hits(df, keyword, seen, remaining)
        if pinyin_rows:
            collected.append(pl.DataFrame(pinyin_rows))
            seen.update(r["symbol"] for r in pinyin_rows)
            remaining -= len(pinyin_rows)

        # ③ 包含匹配补充
        if remaining > 0:
            contain_hits = df.filter(contains_mask & ~pl.col("symbol").is_in(seen)).head(remaining)
            if contain_hits.height:
                collected.append(contain_hits)

        matched = (
            pl.concat(collected, how="vertical")
            if len(collected) > 1
            else (collected[0] if collected else df.head(0))
        )
    rows = matched.select(["symbol", "name", "code", "asset_type"]).to_dicts()
    return {"results": rows}


@router.post("/instruments/names")
def instruments_names(request: Request, symbols: list[str]):
    """批量查标的名称 (股票 + ETF + 指数)。传入 symbol 列表, 返回 {symbol: name}。"""
    if not symbols:
        return {"names": {}}
    repo = request.app.state.repo
    return {"names": repo.get_name_map(symbols)}


@router.get("/daily")
def get_daily(
    request: Request,
    symbol: str = Query(..., description="标的代码,如 000001.SZ"),
    days: int = Query(120, ge=10, le=2000),
    start_date: str | None = Query(None, description="起始日期 YYYY-MM-DD, 优先于 days"),
    end_date: str | None = Query(None, description="截止日期 YYYY-MM-DD, 默认今天"),
    ext_columns: str | None = Query(None, description="逗号分隔的 ext 列: config_id.field_name"),
    adjust: Literal["forward", "none"] = Query("forward", description="复权口径: forward/none"),
):
    """读取本地 enriched 表中某只股票的日 K。

    - 若 QuoteService 有实时行情, 追加/覆盖今日实时蜡烛
    - Free 用户: 若 enriched 表里没有该股票, 实时拉取 + 本地算 enriched 返回
    - ext_columns: 可选，动态 LEFT JOIN 扩展数据表，结果平铺到 stock_info.ext 下
      (key 为 "{config_id}__{field_name}")，供日K信息条等场景展示自定义字段
    """
    repo = request.app.state.repo
    # 未传 end_date 时用北京今天: 实时注入只在内存缓存命中时补当日 K,
    # 缓存冷时 parquet 当日行能否进结果取决于这个窗口右端。
    end = date.fromisoformat(end_date) if end_date else cn_today()
    start = date.fromisoformat(start_date) if start_date else end - timedelta(days=days)

    asset_type = repo.resolve_asset_type(symbol)
    stock_info = (
        _get_stock_info(repo, symbol)
        if asset_type == "stock"
        else _get_asset_info(repo, symbol, asset_type)
    )
    stock_name = stock_info.get("name")

    # 从 enriched 表读取 (已含前复权 OHLCV + 技术指标 + 信号); ETF/指数走独立存储
    df = repo.get_daily_asset(asset_type, symbol, start, end)

    if df.is_empty():
        enriched = _ensure_daily_history(request, repo, symbol, asset_type, start, end)
        if enriched.is_empty():
            return _empty_daily_response(request, symbol, asset_type, stock_info, stock_name, end)
        rows = _enriched_rows(enriched, symbol, start, end)
        rows = _rows_with_adjust(
            rows, adjust=adjust, symbol=symbol, asset_type=asset_type, start=start, end=end
        )
        # 即使 live 模式也尝试追加实时蜡烛；none 口径不混入前复权实时价
        if adjust == "forward" and end >= cn_today():
            rows = _maybe_inject_live_candle(request, symbol, rows, asset_type)
        return _daily_response(
            request,
            repo,
            symbol,
            asset_type,
            stock_info,
            stock_name,
            rows,
            end,
            ext_columns,
            "live",
        )

    rows = df.to_dicts()
    rows = _rows_with_adjust(
        rows, adjust=adjust, symbol=symbol, asset_type=asset_type, start=start, end=end
    )
    if adjust == "forward" and end >= cn_today():
        rows = _maybe_inject_live_candle(request, symbol, rows, asset_type)
    return _daily_response(
        request,
        repo,
        symbol,
        asset_type,
        stock_info,
        stock_name,
        rows,
        end,
        ext_columns,
        "enriched",
    )


@router.get("/daily/latest")
def get_daily_latest(
    request: Request,
    symbol: str = Query(..., description="标的代码,如 000001.SZ"),
):
    """返回内存中的当日单行 K 线, 供详情页实时增量更新。"""
    repo = request.app.state.repo
    asset_type = repo.resolve_asset_type(symbol)
    row = _latest_live_candle(request, symbol, asset_type, refresh_asset=False)
    from app.services.quote_freshness import QuoteFreshness

    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "row": row,
        "source": "live" if row is not None else "none",
        **({"quote_freshness": QuoteFreshness.for_request(request).describe(row)} if row else {}),
    }


@router.post("/daily-batch")
def get_daily_batch(request: Request, body: dict):
    """批量获取多只股票最近 N 天日K (OHLCV)。

    用于自选列表迷你蜡烛图等场景，只返回基础列，不返回全部 enriched 指标。
    按交易日语义取「最近 N 根」: 停牌/断档股在窗口内凑不满时向更早窗口回补,
    跳过停牌空档取复牌前的 K, 而不是把迷你K截成只剩复牌后几根。
    """
    symbols = body.get("symbols", [])
    days = body.get("days", 12)
    if not symbols:
        return {"data": {}}
    # 大自选整表直发会构造千只级响应 (百万行 dict 峰值); 前端已按视口/分块
    # 收敛, 上限挡住旧客户端或异常调用的一次性全量请求 (值可配置)。
    _reject_over_limit(symbols, "daily")
    days = max(5, min(60, days))

    repo = request.app.state.repo
    from datetime import timedelta

    # 窗口右端必须是北京今天: QuoteService 当日 flush 的分区日期是北京交易日。
    # 美西主机整个 A 股交易时段、UTC 主机北京 00:00-08:00, date.today() 比北京早一天,
    # 迷你蜡烛会把当日实时 K 排除在窗口外。
    end = cn_today()
    start = end - timedelta(days=days * 2)  # 多取一些确保交易日够

    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]

    # 资产类型解析一次, 回补轮次复用: stock 走批量缓存; etf/index 逐只查独立存储
    # (数量少, 成本可忽略)
    sym_type = {s: repo.resolve_asset_type(s) for s in symbols}

    result = _tail_daily_frames(
        _read_daily_batch_range(repo, sym_type, list(symbols), start, end, cols), days
    )

    # 停牌回补: 首段窗口是「自然日×2 ≈ 交易日」启发式, 默认窗口内每个交易日都有行情;
    # 停牌股在窗口内交易日不足, tail(days) 只封顶不回补, 迷你K会只剩复牌后几根。
    # 只对凑不满 days 根的股票 (含整窗无数据的长期停牌股) 分轮向更早窗口重取:
    # 第一轮 +120 自然日通常落在 repo 的 enriched 300 天内存缓存内 (近零成本),
    # 第二轮 +480 自然日 (约回看一年) 兜更长停牌, 兜不着的极端停牌按可得分量返回。
    backlog: set[str] = {s for s, sub in result.items() if sub.height < days}
    backlog |= {s for s in symbols if s not in result}
    for pad in (120, 480):
        if not backlog:
            break
        wider = _tail_daily_frames(
            _read_daily_batch_range(
                repo, sym_type, sorted(backlog), start - timedelta(days=pad), end, cols
            ),
            days,
        )
        still: set[str] = set()
        for sym in backlog:
            w = wider.get(sym)
            if w is not None and w.height:  # 宽窗读到的必然 ⊇ 窄窗行, 直接覆盖
                result[sym] = w
            if w is None or w.height < days:
                still.add(sym)
        backlog = still

    # 日K批量同为大响应端点 (千只自选 MB 级), 与分时各自独立压缩开关
    return _gzip_raw(
        request,
        _batch_frames_json(result, "date", "%Y-%m-%d"),
        pref_key="daily_batch_compress",
    )


@router.post("/minute-batch")
def get_minute_batch(request: Request, body: dict):
    """批量获取多只股票某天的分钟K (分时图用)。

    - 本地优先: 先从 kline_minute parquet 读, 完整的直接用
    - 缺失补拉: 本地不完整的 symbol 用 sync_minute_batch 批量实时拉 (不落库)
    - 需 Pro+ 权限 (kline.minute.batch)
    """
    from app.tickflow.capabilities import Cap

    symbols: list[str] = body.get("symbols", [])
    trade_date_str: str | None = body.get("date")
    # 增量响应: since (ISO datetime) 之前的K不回传, 客户端本地缓存合并。
    # since 应传客户端已持有的最后一根时间 — 形成中的动态K >= since, 每轮覆盖。
    since_dt = _minute_batch_since(body.get("since"))
    # 自选分时本地优先标志: 全量分钟服务健康时, 股票缺口不再批量补拉
    # (本地分区由服务按间隔持续写入, 下一轮自然补全; 停牌/临停票补拉也是空, 无损)。
    # ETF 不在全量分钟 universe 内, 恒走补拉。服务不健康时回落现状补拉兜底。
    prefer_local = bool(body.get("prefer_local", False))
    if not symbols:
        return {"data": {}}
    # 同 daily-batch: 前端分块/视口化后单请求远低于此, 上限只挡异常规模 (可配置)
    _reject_over_limit(symbols, "minute")

    repo = request.app.state.repo
    capset = request.app.state.capabilities

    # 权限守卫: 分钟K批量补拉是 Pro+ 能力; 但本地读不消耗 provider 配额 ——
    # 全量分钟服务健康 (正持续写本地分区) 时放行, 避免"数据在盘上却 403"。
    # is_healthy() 纯内存 O(1), 不加热路径成本。
    if not capset.has(Cap.KLINE_MINUTE_BATCH):
        svc = getattr(request.app.state, "minute_refresh", None)
        if svc is None or not svc.is_healthy():
            raise HTTPException(status_code=403, detail="需要 Pro+ 权限 (kline.minute.batch)")

    trade_date = _minute_batch_trade_date(repo, trade_date_str)

    # Step 1: 本地优先 — 一次 scan 读全部 symbol 当日分钟K (股票 / ETF 分钟数据分开存储)
    etf_set = repo.get_etf_symbol_set()
    stock_syms = [s for s in symbols if s not in etf_set]
    etf_syms = [s for s in symbols if s in etf_set]
    df_local = _minute_batch_local(repo, stock_syms, etf_syms, trade_date)
    expected = _minute_batch_expected(trade_date)

    result, full_pull, stale_last, local_parts = _minute_batch_classify(
        symbols, df_local, expected, trade_date
    )

    full_minute_healthy = False
    if prefer_local:
        svc = getattr(request.app.state, "minute_refresh", None)
        full_minute_healthy = bool(svc is not None and svc.is_healthy())
    if full_minute_healthy:
        stale_last = _minute_batch_suppress_stale(result, stale_last, local_parts, etf_set)

    # Step 2: 补拉并落盘 (取到即写, upsert 语义; 下一轮命中本地, 请求量骤降)。
    # 落盘失败只降级 (log 后继续返回本轮数据), 不影响响应 —— 持久化是优化而非正确性前提。
    # 契约: 本端点只接受 stock/ETF (指数分钟K走 /api/index/minute 独立路径),
    # 按 asset_type 拆分调用 (自定义源 / TickFlow 路由均依赖 asset_type 正确传递)。
    live_map = _minute_batch_pull(repo, request, capset, full_pull, stale_last, etf_set, trade_date)

    result, truncated = _minute_batch_merge(result, local_parts, live_map)
    return _minute_batch_response(request, result, truncated, since_dt, full_minute_healthy)


@router.get("/minute-range")
def get_minute_range(
    request: Request,
    symbol: str = Query(..., description="标的代码"),
    days: int = Query(10, ge=1, le=20, description="最近交易日数量"),
):
    """读取单只标的最近 N 个已落库交易日的分钟 K。"""
    import polars as pl

    repo = request.app.state.repo
    asset_type = repo.resolve_asset_type(symbol)
    stock_info = (
        _get_stock_info(repo, symbol)
        if asset_type == "stock"
        else _get_asset_info(repo, symbol, asset_type)
    )
    base_response = {
        "symbol": symbol,
        "name": stock_info.get("name"),
        "asset_type": asset_type,
        "requested_days": days,
    }

    # 指数分钟 K 不落本地仓库, 最新分时仍由 /api/index/minute 实时读取。
    if asset_type == "index":
        return _gzip_payload(
            request,
            {**base_response, "sessions": [], "source": "none"},
            pref_key="minute_batch_compress",
        )

    end = cn_today()
    start = end - timedelta(days=days * 3 + 20)
    minute = repo.get_minute_range([symbol], start, end, asset_type=asset_type)
    if minute.is_empty() or "datetime" not in minute.columns:
        return _gzip_payload(
            request,
            {**base_response, "sessions": [], "source": "none"},
            pref_key="minute_batch_compress",
        )

    minute = minute.with_columns(
        pl.col("datetime").dt.date().alias("_trade_date"),
    )
    trade_dates = sorted(minute["_trade_date"].unique().to_list())[-days:]
    previous_closes = _get_previous_closes(repo, symbol, trade_dates, asset_type)
    row_columns = [
        column
        for column in ("datetime", "open", "high", "low", "close", "volume", "amount")
        if column in minute.columns
    ]
    sessions = []
    daily_dir = repo.store.data_dir / ("kline_etf_daily" if asset_type == "etf" else "kline_daily")
    for trade_date in trade_dates:
        day = minute.filter(pl.col("_trade_date") == trade_date).sort("datetime")
        partial = False
        if trade_date < end:
            bad, _ = minute_gaps.incomplete_minute_groups(
                day, daily_dir, trade_date.isoformat(), minute_gaps.FULL_DAY_BARS
            )
            partial = not bad.is_empty()
        sessions.append(
            {
                "date": trade_date.isoformat(),
                "prev_close": previous_closes.get(trade_date),
                "rows": day.select(row_columns).to_dicts(),
                "partial": partial,
            }
        )

    return _gzip_payload(
        request,
        {
            **base_response,
            "sessions": sessions,
            "source": "local" if sessions else "none",
        },
        pref_key="minute_batch_compress",
    )


@router.get("/minute")
def get_minute(
    request: Request,
    symbol: str = Query(..., description="标的代码"),
    trade_date: date | None = Query(None, alias="date", description="交易日期, 默认最新"),  # noqa: B008 - B008: FastAPI Depends 默认值为框架惯例
    live: bool = Query(False, description="当日盘中跳过本地优先, 直接实时拉取(个股详情分时轮询用)"),
):
    """读取某只股票某天的分钟 K 线。

    - 本地有完整数据(240条) → 直接返回
    - 本地无数据或不完整 → 从有效分钟数据源实时拉取返回(不写入)
    - 自定义源失败时, 仅具备 TickFlow 单股分钟能力才回退 TickFlow
    - live=true 且当日连续竞价时段 → 跳过本地优先直接实时拉取:
      盘中分钟增量落盘的本地分区按 ≥60s 轮次更新, 90% 完整度启发式会让
      详情分时图停在上一增量轮, 与行情列表的节奏脱节
    """
    repo = request.app.state.repo
    capset = request.app.state.capabilities
    raw_basis = minute_adjust.minute_basis_is_raw(repo.store.data_dir)
    asset_type = repo.resolve_asset_type(symbol)
    stock_info = (
        _get_stock_info(repo, symbol)
        if asset_type == "stock"
        else _get_asset_info(repo, symbol, asset_type)
    )
    stock_name = stock_info.get("name")

    trade_date = _minute_resolve_trade_date(repo, trade_date, asset_type, symbol)
    if trade_date is None:
        return _minute_first_live_response(
            request, repo, capset, raw_basis, symbol, asset_type, stock_name, stock_info
        )

    prev_close = _get_previous_closes(repo, symbol, [trade_date], asset_type).get(trade_date)
    price_limit = _get_price_limit_info(repo, symbol, trade_date, asset_type, stock_name)
    ctx = _MinuteCtx(request, symbol, stock_name, stock_info, asset_type, price_limit, prev_close)

    if live and trade_date == cn_today() and in_continuous_session():
        # 详情分时轮询: 当日盘中实时拉取最新一根K, 不落盘; 拉空则落回本地优先路径。
        live_df = _minute_fetch_adjust(
            symbol, trade_date, asset_type, capset, raw_basis, repo.store.data_dir
        )
        if not live_df.is_empty():
            return _minute_payload(ctx, trade_date, live_df.to_dicts(), "live")

    df = repo.get_minute(symbol, trade_date, asset_type=asset_type)
    expected = _minute_expected(trade_date)
    is_complete = not df.is_empty() and len(df) >= expected * 0.9  # 允许 10% 容差

    if is_complete:
        return _minute_payload(ctx, trade_date, df.to_dicts(), "local")

    # 历史日期本地有数据但不完整: 优先返回已有行, 不再丢弃去问一个给不出
    # 历史分钟的源。当日盘中不适用: 盘中本地落后于实时源是常态, 仍应走下方补拉。
    if not df.is_empty() and trade_date != cn_today():
        return _minute_payload(ctx, trade_date, df.to_dicts(), "local_partial", expected, len(df))

    # 本地无数据 (或当日盘中数据落后) → 从当前有效分钟源实时拉取
    live_df = _minute_fetch_adjust(
        symbol, trade_date, asset_type, capset, raw_basis, repo.store.data_dir, skip_if_empty=True
    )
    return _minute_payload(
        ctx, trade_date, live_df.to_dicts(), "live" if not live_df.is_empty() else "none"
    )


# ---------------------------------------------------------------------------
# 子模块 re-export（保持原公开 API 与 monkeypatch 可观察性）
# ---------------------------------------------------------------------------
# 用赋值式 re-export：子模块的路由注册在 importlib.import_module 时完成；
# 赋值目标进入本模块 globals，函数体与测试 monkeypatch 仍按原模块名解析。

__kline_http_mod = importlib.import_module("._kline_http", __package__)
_batch_frames_json = __kline_http_mod._batch_frames_json
_batch_symbols_limit = __kline_http_mod._batch_symbols_limit
_client_accepts_gzip = __kline_http_mod._client_accepts_gzip
_compress_on = __kline_http_mod._compress_on
_gzip_payload = __kline_http_mod._gzip_payload
_gzip_raw = __kline_http_mod._gzip_raw
_init_pinyin_dict = __kline_http_mod._init_pinyin_dict
_instrument_frames = __kline_http_mod._instrument_frames
_json_safe = __kline_http_mod._json_safe
_match_pinyin = __kline_http_mod._match_pinyin
_minute_allowed = __kline_http_mod._minute_allowed
_name_pinyin_keys = __kline_http_mod._name_pinyin_keys
_pinyin_prefix_hits = __kline_http_mod._pinyin_prefix_hits
_reject_over_limit = __kline_http_mod._reject_over_limit
del __kline_http_mod

__kline_daily_mod = importlib.import_module("._kline_daily", __package__)
DailyBatchRequest = __kline_daily_mod.DailyBatchRequest
_LIVE_INDICATOR_KEYS = __kline_daily_mod._LIVE_INDICATOR_KEYS
_LIVE_LIMIT_KEYS = __kline_daily_mod._LIVE_LIMIT_KEYS
_attach_ext = __kline_daily_mod._attach_ext
_daily_freshness = __kline_daily_mod._daily_freshness
_daily_response = __kline_daily_mod._daily_response
_empty_daily_response = __kline_daily_mod._empty_daily_response
_enriched_rows = __kline_daily_mod._enriched_rows
_ensure_daily_history = __kline_daily_mod._ensure_daily_history
_get_asset_info = __kline_daily_mod._get_asset_info
_get_previous_closes = __kline_daily_mod._get_previous_closes
_get_price_limit_info = __kline_daily_mod._get_price_limit_info
_get_stock_info = __kline_daily_mod._get_stock_info
_latest_live_candle = __kline_daily_mod._latest_live_candle
_live_candle_row = __kline_daily_mod._live_candle_row
_maybe_inject_live_candle = __kline_daily_mod._maybe_inject_live_candle
_read_daily_batch_range = __kline_daily_mod._read_daily_batch_range
_tail_daily_frames = __kline_daily_mod._tail_daily_frames
del __kline_daily_mod

__kline_minute_mod = importlib.import_module("._kline_minute", __package__)
_MinuteCtx = __kline_minute_mod._MinuteCtx
_minute_batch_classify = __kline_minute_mod._minute_batch_classify
_minute_batch_expected = __kline_minute_mod._minute_batch_expected
_minute_batch_local = __kline_minute_mod._minute_batch_local
_minute_batch_merge = __kline_minute_mod._minute_batch_merge
_minute_batch_pull = __kline_minute_mod._minute_batch_pull
_minute_batch_pull_one = __kline_minute_mod._minute_batch_pull_one
_minute_batch_response = __kline_minute_mod._minute_batch_response
_minute_batch_since = __kline_minute_mod._minute_batch_since
_minute_batch_suppress_stale = __kline_minute_mod._minute_batch_suppress_stale
_minute_batch_trade_date = __kline_minute_mod._minute_batch_trade_date
_minute_expected = __kline_minute_mod._minute_expected
_minute_fetch_adjust = __kline_minute_mod._minute_fetch_adjust
_minute_first_live_response = __kline_minute_mod._minute_first_live_response
_minute_has_holes = __kline_minute_mod._minute_has_holes
_minute_payload = __kline_minute_mod._minute_payload
_minute_resolve_trade_date = __kline_minute_mod._minute_resolve_trade_date
del __kline_minute_mod

__kline_jobs_mod = importlib.import_module("._kline_jobs", __package__)
_long_task_executor = __kline_jobs_mod._long_task_executor
_minute_sync_message = __kline_jobs_mod._minute_sync_message
_minute_sync_universe = __kline_jobs_mod._minute_sync_universe
_run_minute_sync_fetch = __kline_jobs_mod._run_minute_sync_fetch
_run_minute_sync_job = __kline_jobs_mod._run_minute_sync_job
_run_rebuild_enriched_job = __kline_jobs_mod._run_rebuild_enriched_job
_run_repair_daily_job = __kline_jobs_mod._run_repair_daily_job
trigger_minute_sync = __kline_jobs_mod.trigger_minute_sync
del __kline_jobs_mod

__kline_admin_mod = importlib.import_module("._kline_admin", __package__)
clear_minute = __kline_admin_mod.clear_minute
extend_history = __kline_admin_mod.extend_history
get_minute_gaps = __kline_admin_mod.get_minute_gaps
minute_migrate = __kline_admin_mod.minute_migrate
rebuild_enriched = __kline_admin_mod.rebuild_enriched
refresh_views = __kline_admin_mod.refresh_views
repair_daily = __kline_admin_mod.repair_daily
repair_minute_gaps = __kline_admin_mod.repair_minute_gaps
sync_batch = __kline_admin_mod.sync_batch
sync_minute = __kline_admin_mod.sync_minute
sync_minute_single = __kline_admin_mod.sync_minute_single
sync_symbol = __kline_admin_mod.sync_symbol
del __kline_admin_mod
