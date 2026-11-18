"""assistant tools — market 分组实现。

由 app.custom.assistant.tools 组合并 re-export；公开导入路径不变。

集中处理行情域的助手工具，每个 ``_get_*`` 接收归一化参数 ``args`` 与工具上下文
``ctx``，从仓储/服务读取数据并返回可 JSON 序列化的字典：

- ``_get_market_overview`` / ``_get_indices``: 大盘概览与指数行情；
- ``_get_sector_rotation`` / ``_get_regime``: 板块轮动与市场状态；
- ``_get_abnormal`` / ``_get_lots``: 异常波动与标的份额信息。

仓储获取统一走 ``_require_repo``，缺数据时降级为空结果，不抛给调用方。
"""

from __future__ import annotations

from app.custom.assistant.tools import (
    Any,
    ToolContext,
    _clamp,
    _clean_value,
    _quotes_map,
    _require_repo,
    _rows,
)


def _get_market_overview(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.market_overview_builder import build_market_overview

    repo = _require_repo(ctx)
    return build_market_overview(
        repo, quote_service=ctx.quote_service, depth_service=ctx.depth_service
    )


def _get_indices(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    if ctx.quote_service is None:
        raise ValueError("实时行情服务未启用, 无法查询指数。")
    df = ctx.quote_service.get_index_quotes()
    rows = _rows(df, 60)
    if not rows:
        return {"count": 0, "rows": [], "note": "指数实时缓存为空。"}
    return {"count": len(rows), "rows": rows}


def _get_sector_rotation(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.sector_rotation import build_sector_rotation

    repo = _require_repo(ctx)
    kind = str(args.get("kind") or "concept")
    if kind not in {"concept", "industry"}:
        raise ValueError("kind 必须是 concept 或 industry。")
    top = _clamp(args.get("top"), 10, 50, 20)
    data = build_sector_rotation(repo, kind=kind, top=top)
    status = data.get("status")
    if status in {"no_data", "empty"}:
        return {"status": status, "reason": data.get("reason", "")}
    sectors = data.get("sectors")
    cross = data.get("cross_events")
    return {
        "kind": kind,
        "status": status,
        "sectors": sectors[:top] if isinstance(sectors, list) else sectors,
        "cross_events": cross[:20] if isinstance(cross, list) else cross,
        "note": "已省略时间序列矩阵, 只保留板块榜与切换事件。",
    }


def _get_regime(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.regime_builder import STATE_LABELS, load_regime_history

    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    days = _clamp(args.get("days"), 1, 120, 30)
    df = load_regime_history(ctx.data_dir)
    if df is None or df.is_empty():
        return {"count": 0, "rows": [], "note": "本地暂无市场环境(regime)数据。"}
    keep = [
        c
        for c in (
            "date",
            "state",
            "score",
            "phase",
            "max_consecutive",
            "first_board",
            "ge2_count",
            "promo_rate",
            "seal_rate",
        )
        if c in df.columns
    ]
    sub = df.select(keep).sort("date", descending=True).head(days)
    rows = _rows(sub, days)
    for row in rows:
        state = row.get("state")
        if state:
            row["state_label"] = STATE_LABELS.get(str(state), str(state))
    return {"count": len(rows), "rows": rows}


def _get_abnormal(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.abnormal_moves import build_intraday, build_overview

    repo = _require_repo(ctx)
    kind = str(args.get("kind") or "intraday")
    if kind not in {"intraday", "overview"}:
        raise ValueError("kind 必须是 intraday 或 overview。")
    limit = _clamp(args.get("limit"), 10, 200, 50)
    if kind == "intraday":
        data = build_intraday(repo, limit=limit)
        rows = data.get("rows") or []
        return {
            "kind": kind,
            "cache_date": data.get("cache_date"),
            "counts": data.get("counts"),
            "rows": rows[:limit],
        }
    data = build_overview(repo, ctx.quote_service, limit=limit)
    rows = data.get("rows") or []
    return {"kind": kind, "rows": rows[:limit]}


def _get_lots(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.strategy.lots import load_all

    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    lots = load_all(ctx.data_dir)
    if not lots:
        return {"count": 0, "rows": [], "note": "暂无持仓提醒记录。"}
    symbols = [str(lot.get("symbol") or "") for lot in lots if lot.get("symbol")]
    quotes = _quotes_map(ctx, symbols)
    names = ctx.repo.get_name_map(symbols) if ctx.repo is not None else {}
    rows = []
    for lot in lots[:100]:
        symbol = str(lot.get("symbol") or "")
        quote = quotes.get(symbol, {})
        rows.append(
            {
                "symbol": symbol,
                "name": names.get(symbol, ""),
                "qty": lot.get("qty"),
                "cost_price": lot.get("cost_price"),
                "buy_date": _clean_value(lot.get("buy_date")),
                "target_pct": lot.get("target_pct"),
                "stop_pct": lot.get("stop_pct"),
                "close": quote.get("close"),
                "change_pct": quote.get("change_pct"),
            }
        )
    return {"count": len(rows), "rows": rows}
