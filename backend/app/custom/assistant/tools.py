"""AI 助手工具层 — 核心工具目录透传 + 面向各页面能力的本地查询/动作工具。

设计原则(为项目服务、有界):
- 查询工具对应页面核心查询能力(行情/自选/看板/指数/板块轮动/市场
  环境/异动/持仓提醒/信号库/财务/个股分析/因子值/跑策略), 全部只读。
- 动作工具(create_signal_strategy/add_to_watchlist, 及核心目录的
  run_backtest)有写副作用或重计算, 由 chat_service 的确认闸门把守
  (actions.py), 未经用户确认不会执行。
- 返回体有界: rows 全部带 limit 钳制, 大表(enriched/财务)先 filter 再取
  少量行; 数值统一四舍五入 4 位, 日期转 ISO 字符串, 保证 JSON 可序列化。
- 未知的工具名回退到核心 services.tool_catalog(因子/策略/数据能力/回测)。

本模块还提供:
- QUICK_SUGGESTS: 空会话快捷指令。
- summarize_tool_result: 工具结果的足迹卡一行摘要(不回传原始大 JSON)。
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.market_time import cn_today
from app.services import tool_catalog


# ----------------------------------------------------------------
# 执行上下文 — 由路由从 app.state 注入, 测试可替换为桩对象。
# ----------------------------------------------------------------
@dataclass
class ToolContext:
    repo: Any = None
    quote_service: Any = None
    depth_service: Any = None
    engine: Any = None
    data_dir: Path | None = None
    capabilities: Any = None
    financial_scheduler: Any = None

    @classmethod
    def build(
        cls,
        *,
        repo: Any = None,
        quote_service: Any = None,
        depth_service: Any = None,
        engine: Any = None,
        data_dir: str | Path | None = None,
        capabilities: Any = None,
        financial_scheduler: Any = None,
    ) -> ToolContext:
        return cls(
            repo=repo,
            quote_service=quote_service,
            depth_service=depth_service,
            engine=engine,
            data_dir=Path(data_dir) if data_dir else None,
            capabilities=capabilities,
            financial_scheduler=financial_scheduler,
        )


_SYMBOL_RE = re.compile(r"^[0-9A-Za-z._-]{3,16}$")


def _clamp(value: Any, low: int, high: int, default: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def _clean_value(value: Any) -> Any:
    """JSON 序列化清洗: 日期转 ISO, 浮点四舍五入并剔除 nan/inf。"""
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 4)
    return value


def _clean_obj(value: Any) -> Any:
    """递归清洗 dict/list 结构(run_strategy 行等动态载荷)。"""
    if isinstance(value, dict):
        return {str(k): _clean_obj(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_clean_obj(v) for v in value]
    return _clean_value(value)


def _rows(
    df: pl.DataFrame | None,
    limit: int,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """DataFrame → 有界的 JSON 行列表; 空/缺列安全。"""
    if df is None or df.is_empty():
        return []
    if columns:
        keep = [c for c in columns if c in df.columns]
        if keep:
            df = df.select(keep)
    if df.width > 60:
        df = df.select(df.columns[:60])
    rows: list[dict[str, Any]] = []
    for row in df.head(limit).to_dicts():
        rows.append({key: _clean_value(value) for key, value in row.items()})  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性
    return rows


_QUOTE_COLUMNS = (
    "symbol",
    "close",
    "open",
    "high",
    "low",
    "volume",
    "amount",
    "prev_close",
    "change_pct",
    "change_amount",
    "amplitude",
    "turnover_rate",
)


def _asset_quote_frame(ctx: ToolContext, symbols: list[str]) -> pl.DataFrame:
    """symbols 的行情快照: 股票取 get_quotes_compat, ETF / 指数取各自的 enriched 缓存。

    get_quotes_compat 只含股票 enriched 缓存; ETF 与指数是独立缓存, 与自选页
    /api/watchlist/enriched 一样须按资产类型分流, 否则 ETF / 指数永远查不到价格。
    只对股票缓存里没有的代码判定资产类型, 全是股票时不加载 ETF / 指数缓存。
    """
    frames: list[pl.DataFrame] = []
    df = ctx.quote_service.get_quotes_compat()
    if df is not None and not df.is_empty() and "symbol" in df.columns:
        frames.append(df.filter(pl.col("symbol").is_in(symbols)))
    found = set(frames[0]["symbol"].to_list()) if frames else set()
    missing = [s for s in symbols if s not in found]
    if missing and ctx.repo is not None:
        by_asset: dict[str, list[str]] = {}
        for symbol in missing:
            asset_type = ctx.repo.resolve_asset_type(symbol)
            if asset_type != "stock":
                by_asset.setdefault(asset_type, []).append(symbol)
        for asset_type, members in by_asset.items():
            asset_df, _ = ctx.repo.get_enriched_latest_asset(asset_type)
            if asset_df is None or asset_df.is_empty() or "symbol" not in asset_df.columns:
                continue
            keep = [c for c in _QUOTE_COLUMNS if c in asset_df.columns]
            frames.append(asset_df.filter(pl.col("symbol").is_in(members)).select(keep))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _quotes_map(ctx: ToolContext, symbols: list[str]) -> dict[str, dict[str, Any]]:
    """symbol → {close, change_pct} 轻量行情映射(自选/持仓等合并展示用)。"""
    if ctx.quote_service is None or not symbols:
        return {}
    try:
        df = _asset_quote_frame(ctx, symbols)
    except Exception:  # 行情服务未就绪时静默降级为无价格列  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return {}
    if df is None or df.is_empty() or "symbol" not in df.columns:
        return {}
    keep = [c for c in ("symbol", "close", "change_pct") if c in df.columns]
    out: dict[str, dict[str, Any]] = {}
    for row in df.filter(pl.col("symbol").is_in(symbols)).select(keep).to_dicts():
        out[row["symbol"]] = {k: _clean_value(v) for k, v in row.items() if k != "symbol"}
    return out


def _require_repo(ctx: ToolContext) -> Any:
    if ctx.repo is None:
        raise ValueError("行情数据仓库未就绪, 请确认后端已完成启动。")
    return ctx.repo


def _validate_symbol(value: Any) -> str:
    symbol = str(value or "").strip()
    if not _SYMBOL_RE.fullmatch(symbol):
        raise ValueError(f"无效的证券代码: {symbol!r} (示例: 600519.SH / 300750.SZ)")
    return symbol


# ----------------------------------------------------------------
# 本地查询工具实现 (全部同步, 由 execute_assistant_tool 挪出事件循环)。
# ----------------------------------------------------------------
def _get_stock_quote(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    if ctx.quote_service is None:
        raise ValueError("实时行情服务未启用, 无法查询个股快照。")
    raw = args.get("symbols") or []
    if not isinstance(raw, list) or not raw:
        raise ValueError("symbols 不能为空, 请传入证券代码数组 (如 ['600519.SH'])。")
    symbols = list(dict.fromkeys(_validate_symbol(s) for s in raw[:50]))
    sub = _asset_quote_frame(ctx, symbols)
    names = ctx.repo.get_name_map(symbols) if ctx.repo is not None else {}
    rows = _rows(sub, 50)
    for row in rows:
        row.setdefault("name", names.get(row.get("symbol"), ""))
    if not rows:
        return {"count": 0, "rows": [], "note": "未匹配到行情, 请确认代码格式 (如 600519.SH)。"}
    result: dict[str, Any] = {"count": len(rows), "rows": rows}
    # 单只查询时附当日分时小图; 批量查询 N 只附 N 张图会淹没回答, 不附。
    if len(rows) == 1:
        row = rows[0]
        prev_close = row.get("prev_close")
        if (
            prev_close is None
            and row.get("close") is not None
            and row.get("change_pct") is not None
        ):
            # 行情快照无昨收列时按 close/(1+pct) 反推, 供分时图画昨收基准线
            prev_close = round(float(row["close"]) / (1.0 + float(row["change_pct"])), 3)
        chart = _intraday_chart_payload(ctx, symbols[0], str(row.get("name") or ""), prev_close)
        if chart is not None:
            result["charts"] = [chart]
    return result


def _finite_float(value: Any) -> float | None:
    """转 float; None / 非数字 / nan / inf 返回 None, 避免写入非法 JSON。"""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _num(v: Any) -> float | None:
    number = _finite_float(v)
    return None if number is None else round(number, 3)


def _intraday_chart_payload(
    ctx: ToolContext,
    symbol: str,
    name: str,
    prev_close: float | None = None,
) -> dict[str, Any] | None:
    """当日分时小图 payload — 本地分钟K收盘序列, 非模型生成(结果可核对)。

    点数封顶 240(一个交易日); 本地无分钟数据时返回 None, 不阻断行情回答。
    """
    if ctx.repo is None:
        return None
    try:
        asset_type = ctx.repo.resolve_asset_type(symbol)
        # 取行情快照所在交易日 (昨收基准线也来自快照行): 周末/节假日/开盘前快照停在
        # 最近交易日, 按服务器本地 date.today() 查分钟分区会永远为空; 无快照日期时按北京日期。
        _, trade_date = ctx.repo.get_enriched_latest_asset(asset_type, refresh=False)
        df = ctx.repo.get_minute(symbol, trade_date or cn_today(), asset_type)
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return None
    if df is None or df.is_empty() or "close" not in df.columns or "datetime" not in df.columns:
        return None
    points: list[list[Any]] = []
    for row in df.sort("datetime").to_dicts():
        dt = row.get("datetime")
        close = _finite_float(row.get("close"))
        if dt is None or close is None:
            continue
        t = dt.strftime("%H:%M") if hasattr(dt, "strftime") else str(dt)[-8:-3]
        volume = _finite_float(row.get("volume"))
        points.append([t, round(close, 3), 0.0 if volume is None else round(volume, 2)])
    if len(points) < 2:
        return None
    payload: dict[str, Any] = {
        "kind": "intraday",
        "symbol": symbol,
        "name": name,
        "points": points[-240:],
    }
    if prev_close is not None:
        payload["prev_close"] = prev_close
    return payload


def _daily_kline_chart_payload(
    symbol: str, name: str, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """日K 小图 payload — [date, open, high, low, close, volume], 前端渲染蜡烛图。

    点数封顶 120, 避免长区间把 NDJSON 流式事件撑大; 不足 2 根不附图。
    """
    points = [
        [
            str(row["date"]),
            _num(row.get("open")),
            _num(row.get("high")),
            _num(row.get("low")),
            _num(row.get("close")),
            _num(row.get("volume")),
        ]
        for row in rows
        if row.get("date") is not None
        and row.get("close") is not None
        and row.get("open") is not None
    ]
    if len(points) < 2:
        return None
    return {"kind": "daily_kline", "symbol": symbol, "name": name, "points": points[-120:]}


def _get_stock_daily(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    repo = _require_repo(ctx)
    symbol = _validate_symbol(args.get("symbol"))
    days = _clamp(args.get("days"), 10, 500, 60)
    end = cn_today()
    start = end - timedelta(days=int(days * 1.9) + 20)
    df = repo.get_daily_asset(repo.resolve_asset_type(symbol), symbol, start, end)
    if df is None or df.is_empty():
        return {"symbol": symbol, "rows": [], "note": "本地没有该标的的日线数据。"}
    keep = [
        c
        for c in (
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "change_pct",
            "turnover_rate",
            "amplitude",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
        )
        if c in df.columns
    ]
    rows = _rows(df.select(keep).tail(days), days)
    names = repo.get_name_map([symbol])
    name = names.get(symbol, "")
    result: dict[str, Any] = {
        "symbol": symbol,
        "name": name,
        "count": len(rows),
        "rows": rows,
    }
    kline = _daily_kline_chart_payload(symbol, name, rows)
    if kline is not None:
        result["charts"] = [kline]
    return result


def _get_stock_analysis(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.indicators.levels import compute_levels, summarize_levels

    repo = _require_repo(ctx)
    symbol = _validate_symbol(args.get("symbol"))
    end = cn_today()
    df = repo.get_daily_asset(
        repo.resolve_asset_type(symbol), symbol, end - timedelta(days=500), end
    )
    if df is None or df.is_empty():
        return {"symbol": symbol, "note": "本地没有该标的的日线数据, 无法分析。"}
    recent = df.tail(120)
    close = float(recent["close"][-1]) if recent.height else None
    levels = compute_levels(recent)
    keep = [
        c
        for c in (
            "date",
            "close",
            "change_pct",
            "amount",
            "turnover_rate",
            "amplitude",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "high_60d",
            "low_60d",
        )
        if c in recent.columns
    ]
    latest_rows = _rows(recent.select(keep).tail(1), 1)
    names = repo.get_name_map([symbol])
    return {
        "symbol": symbol,
        "name": names.get(symbol, ""),
        "close": _clean_value(close),
        "summary": summarize_levels(levels, close),
        "latest": latest_rows[0] if latest_rows else {},
        "levels": _clean_obj(levels),
    }


def _get_financials(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.financial_sync import FINANCIAL_TABLES, get_financial_df

    symbol = _validate_symbol(args.get("symbol"))
    table = str(args.get("table") or "metrics")
    if table not in FINANCIAL_TABLES:
        raise ValueError(f"table 必须是 {list(FINANCIAL_TABLES)} 之一。")
    periods = _clamp(args.get("periods"), 1, 8, 4)
    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    df = get_financial_df(ctx.data_dir, table)
    if df is None or df.is_empty():
        return {
            "symbol": symbol,
            "table": table,
            "rows": [],
            "note": "本地暂无财务数据, 可在数据页同步。",
        }
    sub = df.filter(pl.col("symbol") == symbol)
    if sub.is_empty():
        return {"symbol": symbol, "table": table, "rows": [], "note": "本地财务数据中没有该标的。"}
    # 本地财务表的报告期列是 period_end; 物理行序取决于同步路径, 须显式排序后再取最近 N 期
    sort_col = next(
        (
            c
            for c in ("period_end", "report_date", "end_date", "ann_date", "date")
            if c in sub.columns
        ),
        None,
    )
    if sort_col:
        sub = sub.sort(sort_col)
    sub = sub.tail(periods)
    if sub.height:
        nulls = dict(zip(sub.columns, sub.null_count().row(0), strict=True))
        keep = [c for c in sub.columns if nulls.get(c, 0) < sub.height]
        if keep:
            sub = sub.select(keep)
    rows = _rows(sub, periods)
    return {"symbol": symbol, "table": table, "count": len(rows), "rows": rows}


def _get_watchlist(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services import watchlist as watchlist_service

    items = watchlist_service.list_symbols()
    if not items:
        return {"count": 0, "rows": [], "note": "自选列表为空。"}
    symbols = [str(i.get("symbol") or "") for i in items if i.get("symbol")]
    quotes = _quotes_map(ctx, symbols)
    names = ctx.repo.get_name_map(symbols) if ctx.repo is not None else {}
    rows = []
    for item in items[:300]:
        symbol = str(item.get("symbol") or "")
        quote = quotes.get(symbol, {})
        rows.append(
            {
                "symbol": symbol,
                "name": names.get(symbol, ""),
                "note": item.get("note") or "",
                "close": quote.get("close"),
                "change_pct": quote.get("change_pct"),
            }
        )
    return {"count": len(rows), "rows": rows}


# ----------------------------------------------------------------
# 动作工具实现 (写操作; 由 chat_service 的确认闸门把守, 未经确认不会到达这里)。
# ----------------------------------------------------------------
def _invalidate_custom_signals(data_dir: Path, repo: Any) -> None:
    """与 api/signals.py 的 _invalidate 同一条失效链, 保证创建后立即可回测。"""
    from app.indicators.pipeline import invalidate_custom_signals
    from app.services import strategy_cache
    from app.strategy import custom_signals

    custom_signals.invalidate_intraday_cache()
    invalidate_custom_signals()
    strategy_cache.clear_cache(data_dir)
    if repo is not None and hasattr(repo, "clear_cache"):
        repo.clear_cache()


_SIGNAL_FIELD_HINT = (
    "常用字段: close/open/high/low/volume/amount/change_pct/turnover_rate/"
    "ma5/ma10/ma20/ma30/ma60/ema5-ema60/macd_dif/macd_dea/macd_hist/"
    "boll_upper/boll_lower/kdj_k/kdj_d/kdj_j/rsi_6/rsi_14/rsi_24/atr_14/"
    "vol_ma5/vol_ma10/vol_ratio_5d/high_60d/low_60d/momentum_5d-60d/"
    "annual_vol_20d/consecutive_limit_ups; 右值数字直接写, 字段引用写 'field:字段名'"
)

# 写操作工具: 结果里补统一的 action_ok / action 字段。
# 历史上分别返回 created / added / started, 模型要逐工具记住哪个字段代表"已生效";
# 统一后只需认 action_ok (原字段保留, 不破坏既有消费方)。
ACTION_RESULT_TOOLS: frozenset[str] = frozenset(
    {
        "create_signal_strategy",
        "create_strategy",
        "add_to_watchlist",
        "sync_data",
    }
)

# 策略源码上限: 正常策略 200~400 行, 60KB 足够; 防止模型一次灌入超大文本
__all__ = [
    "ACTION_RESULT_TOOLS",
    "QUICK_SUGGESTS",
    "_LOCAL_TOOLS",
    "_MAX_STRATEGY_CODE_CHARS",
    "_QUOTE_COLUMNS",
    "_SIGNAL_FIELD_HINT",
    "_SYMBOL_RE",
    "Any",
    "Callable",
    "Path",
    "ToolContext",
    "_add_to_watchlist",
    "_asset_quote_frame",
    "_biz_days_behind",
    "_check_data_coverage",
    "_clamp",
    "_clean_obj",
    "_clean_value",
    "_coverage_section",
    "_create_signal_strategy",
    "_create_strategy",
    "_daily_kline_chart_payload",
    "_ext_table_freshness",
    "_financial_coverage",
    "_finite_float",
    "_get_abnormal",
    "_get_factor_values",
    "_get_financials",
    "_get_indices",
    "_get_lots",
    "_get_market_overview",
    "_get_regime",
    "_get_sector_rotation",
    "_get_stock_analysis",
    "_get_stock_daily",
    "_get_stock_quote",
    "_get_sync_status",
    "_get_watchlist",
    "_intraday_chart_payload",
    "_invalidate_custom_signals",
    "_list_ext_tables",
    "_list_signals",
    "_listing_dates",
    "_local_tool_schemas",
    "_local_tool_schemas_data",
    "_local_tool_schemas_market",
    "_local_tool_schemas_quotes",
    "_local_tool_schemas_signal_strategy",
    "_local_tool_schemas_strategy_actions",
    "_local_tool_schemas_strategy_query",
    "_mark_action_result",
    "_num",
    "_partition_dates",
    "_query_ext_table",
    "_quotes_map",
    "_require_repo",
    "_rows",
    "_run_strategy",
    "_schema",
    "_strategy_guide_text",
    "_summarize_data_tool",
    "_symbol_coverage",
    "_sync_data",
    "_validate_symbol",
    "asyncio",
    "build_tool_schemas",
    "cn_today",
    "contextlib",
    "dataclass",
    "date",
    "datetime",
    "execute_assistant_tool",
    "math",
    "pl",
    "re",
    "summarize_tool_result",
    "timedelta",
    "tool_catalog",
    "uuid",
]


_MAX_STRATEGY_CODE_CHARS = 60_000


# ----------------------------------------------------------------
# 分组实现模块: 由根模块统一 re-export, 保持公开导入路径与 patch 目标不变。
# ----------------------------------------------------------------
from app.custom.assistant._tools_data import (  # noqa: E402
    _biz_days_behind,
    _check_data_coverage,
    _coverage_section,
    _ext_table_freshness,
    _financial_coverage,
    _get_sync_status,
    _list_ext_tables,
    _partition_dates,
    _query_ext_table,
    _symbol_coverage,
    _sync_data,
)
from app.custom.assistant._tools_market import (  # noqa: E402
    _get_abnormal,
    _get_indices,
    _get_lots,
    _get_market_overview,
    _get_regime,
    _get_sector_rotation,
)
from app.custom.assistant._tools_schemas import (  # noqa: E402
    _local_tool_schemas,
    _local_tool_schemas_data,
    _local_tool_schemas_market,
    _local_tool_schemas_quotes,
    _local_tool_schemas_signal_strategy,
    _local_tool_schemas_strategy_actions,
    _local_tool_schemas_strategy_query,
    _schema,
)
from app.custom.assistant._tools_signal import (  # noqa: E402
    _add_to_watchlist,
    _create_signal_strategy,
    _invalidate_custom_signals,
    _listing_dates,
)
from app.custom.assistant._tools_strategy import (  # noqa: E402
    _create_strategy,
    _get_factor_values,
    _list_signals,
    _run_strategy,
    _strategy_guide_text,
)
from app.custom.assistant._tools_suggests import QUICK_SUGGESTS  # noqa: E402

_LOCAL_TOOLS: dict[str, Callable[[dict[str, Any], ToolContext], Any]] = {
    "get_stock_quote": _get_stock_quote,
    "get_stock_daily": _get_stock_daily,
    "get_stock_analysis": _get_stock_analysis,
    "get_financials": _get_financials,
    "get_watchlist": _get_watchlist,
    "get_market_overview": _get_market_overview,
    "get_indices": _get_indices,
    "get_sector_rotation": _get_sector_rotation,
    "get_regime": _get_regime,
    "get_abnormal": _get_abnormal,
    "get_lots": _get_lots,
    "list_signals": _list_signals,
    "run_strategy": _run_strategy,
    "get_factor_values": _get_factor_values,
    "create_signal_strategy": _create_signal_strategy,
    "create_strategy": _create_strategy,
    "add_to_watchlist": _add_to_watchlist,
    "check_data_coverage": _check_data_coverage,
    "list_ext_tables": _list_ext_tables,
    "query_ext_table": _query_ext_table,
    "get_sync_status": _get_sync_status,
    "sync_data": _sync_data,
}


def build_tool_schemas() -> list[dict[str, Any]]:
    """核心工具目录(因子/策略/能力/回测) + 本模块的页面查询工具。"""
    return [*tool_catalog.build_tool_schemas(), *_local_tool_schemas()]


async def execute_assistant_tool(
    name: str, args: dict[str, Any], ctx: ToolContext
) -> dict[str, Any]:
    """统一执行入口: 本地工具(to_thread 防阻塞) → 核心工具目录回退。

    契约与核心 tool_catalog.execute_tool 一致: {"ok": bool, "result"|"error"}。
    写工具的结果再补一个 action_ok / action 标记: 各工具历史上分别返回
    created / added / started, 模型要逐工具记住哪个字段表示"已生效"。统一补
    action_ok 后, 模型只需认一个字段 (原字段保留, 不破坏既有消费方)。
    """
    handler = _LOCAL_TOOLS.get(name)
    if handler is not None:
        try:
            if asyncio.iscoroutinefunction(handler):
                # async 工具(sync_data)需要在事件循环上调度后台任务, 不挪线程
                result = await handler(dict(args or {}), ctx)
            else:
                result = await asyncio.to_thread(handler, dict(args or {}), ctx)
            return {"ok": True, "result": _mark_action_result(name, args, result)}
        except Exception as exc:  # 工具失败按契约回填给模型, 不打断对话流  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return {"ok": False, "error": str(exc) or exc.__class__.__name__}
    return await tool_catalog.execute_tool(
        name,
        args,
        engine=ctx.engine,
        repo=ctx.repo,
        data_dir=str(ctx.data_dir) if ctx.data_dir else None,
    )


def _mark_action_result(name: str, args: dict[str, Any] | None, result: Any) -> Any:
    """给写操作结果补统一的 action_ok / action 字段 (幂等, 不改原字段)。

    只读调用不标记: create_strategy(show_guide=true) 只返回编写规范、不落盘,
    标成 action_ok 会让模型以为策略已创建。
    """
    if not isinstance(result, dict) or name not in ACTION_RESULT_TOOLS:
        return result
    if "action_ok" in result:
        return result
    if name == "create_strategy" and (args or {}).get("show_guide"):
        return result
    marked = dict(result)
    marked["action"] = name
    marked["action_ok"] = True
    return marked


def _summarize_data_tool(  # noqa: C901 - 工具名到摘要文案为平铺映射, 保持原分支顺序便于核对
    name: str, result: dict[str, Any], count: int
) -> str | None:
    """数据类工具的一行摘要；未命中返回 None。"""
    if name == "get_stock_quote":
        return f"查询 {count} 只个股实时行情"
    if name == "get_stock_daily":
        return f"返回 {count} 个交易日日线"
    if name == "get_stock_analysis":
        return "完成个股关键价位分析"
    if name == "get_financials":
        return f"返回 {result.get('table', '财务')} 近 {count} 期"
    if name == "get_watchlist":
        return f"返回自选 {count} 只"
    if name == "get_market_overview":
        breadth = result.get("breadth") or {}
        limit = (result.get("limit") or {}).get("limit_up", "-")
        return f"总览: 涨 {breadth.get('up', '-')} / 跌 {breadth.get('down', '-')} · 涨停 {limit}"
    if name == "get_indices":
        return f"返回 {count} 条指数行情"
    if name == "get_sector_rotation":
        return "完成板块轮动查询"
    if name == "get_regime":
        rows = result.get("rows") or []
        latest = rows[0].get("state_label") if rows else "-"
        return f"返回 {count} 日 regime, 最新 {latest}"
    if name == "get_abnormal":
        return f"返回异动 {count} 行"
    if name == "get_lots":
        return f"返回持仓提醒 {count} 条"
    if name == "list_signals":
        return f"返回 {count} 个信号定义"
    if name == "run_strategy":
        return f"命中 {result.get('total', 0)} 只 · 耗时 {result.get('elapsed_ms', '-')}ms"
    if name == "get_factor_values":
        return f"返回因子 {result.get('factor', '-')} Top{count}"
    if name == "create_signal_strategy":
        signal = result.get("signal") or {}
        return f"创建信号 {signal.get('name', '-')} ({signal.get('id', '-')})"
    if name == "add_to_watchlist":
        return result.get("note") or "已加入自选"
    if name == "check_data_coverage":
        issue_count = len(result.get("issues") or [])
        return f"发现 {issue_count} 项数据缺口" if issue_count else "数据完整, 无缺口"
    if name == "list_ext_tables":
        ext_count = result.get("count", 0)
        return f"返回 {ext_count} 张扩展表" if ext_count else "暂无扩展数据表"
    if name == "query_ext_table":
        return f"读取扩展表 {result.get('label', '-')} {result.get('returned', 0)}/{result.get('total', 0)} 行"
    return None


def summarize_tool_result(  # noqa: C901 - 先分流目录类工具再委托数据摘要 helper, 保持输出顺序稳定
    name: str, payload: dict[str, Any]
) -> str:
    """把工具返回压缩成一行人类可读摘要, 用于前端足迹卡。"""
    if not payload.get("ok"):
        error = str(payload.get("error") or "执行失败")
        return f"失败: {error[:80]}"

    result = payload.get("result")
    if not isinstance(result, dict):
        return "完成"

    def _count(key: str = "rows") -> int:
        rows = result.get(key)
        return len(rows) if isinstance(rows, list) else int(result.get("count") or 0)

    if name == tool_catalog.LIST_FACTORS:
        factors = result.get("factors")
        return f"返回 {len(factors) if isinstance(factors, list) else 0} 个因子"
    if name == tool_catalog.LIST_STRATEGIES:
        strategies = result.get("strategies")
        return f"返回 {len(strategies) if isinstance(strategies, list) else 0} 个策略"
    if name == tool_catalog.LIST_DATA_CAPABILITIES:
        caps = result.get("capabilities")
        return f"返回 {len(caps) if isinstance(caps, list) else 0} 项能力"
    if name == tool_catalog.RUN_BACKTEST:
        stats = result.get("stats")
        if not isinstance(stats, dict) or not stats:
            return "回测完成(无指标)"
        return " · ".join(
            str(p)
            for p in (
                f"总收益 {stats.get('total_return', '-')}",
                f"年化 {stats.get('annual_return', '-')}",
                f"回撤 {stats.get('max_drawdown', '-')}",
                f"夏普 {stats.get('sharpe', '-')}",
            )
        )
    data_summary = _summarize_data_tool(name, result, _count())
    if data_summary is not None:
        return data_summary
    if name == "get_sync_status":
        active = result.get("active_job")
        if active:
            return f"进行中: {active.get('stage', '-')} {active.get('progress', 0)}%"
        if result.get("financial_syncing"):
            return "财务表同步中"
        return "当前无同步任务"
    if name == "sync_data":
        job_id = result.get("job_id")
        reused = result.get("reused")
        label = {
            "pipeline": "盘后管道",
            "financials": "财务表同步",
            "minute_extend": "分钟K扩展",
        }.get(result.get("kind"), "同步")  # type: ignore[arg-type]
        if reused:
            return f"复用进行中的{label}任务 {job_id or '-'}"
        return f"已启动{label}任务 {job_id or '-'}"

    return f"返回 {_count()} 行" if _count() else "完成"
