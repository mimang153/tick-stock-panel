"""assistant tools — strategy 分组实现。

由 app.custom.assistant.tools 组合并 re-export；公开导入路径不变。
"""

from __future__ import annotations

from app.custom.assistant._tools_signal import _listing_dates
from app.custom.assistant.tools import (
    _MAX_STRATEGY_CODE_CHARS,
    Any,
    Path,
    ToolContext,
    _clamp,
    _clean_obj,
    _clean_value,
    _require_repo,
    _rows,
    contextlib,
    date,
    pl,
    timedelta,
    tool_catalog,
    uuid,
)


def _list_signals(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.strategy.custom_signals import load_all

    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    rows = [_clean_obj(r) for r in load_all(ctx.data_dir)[:100]]
    return {"count": len(rows), "rows": rows}


def _run_strategy(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.screener import ScreenerService
    from app.strategy.config import load_override

    repo = _require_repo(ctx)
    if ctx.engine is None:
        raise ValueError("策略引擎未就绪。")
    strategy_id = str(args.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("strategy_id 不能为空, 可先用 list_strategies 查看。")
    if not ctx.engine.has(strategy_id):
        # 与 run_backtest 同口径: 附带可用 id, 模型一次即可自行修正
        try:
            available = ", ".join(
                str(s.get("id")) for s in ctx.engine.list_strategies()[:12] if s.get("id")
            )
        except Exception:  # noqa: BLE001 - 列举策略失败不影响原始错误提示
            available = ""
        hint = f"; 可用策略: {available}" if available else "; 可先用 list_strategies 查询"
        raise ValueError(f"策略 {strategy_id} 不存在{hint}")
    strategy = ctx.engine.get(strategy_id)
    asset_type, timeframe = tool_catalog.resolve_strategy_target(
        strategy,
        asset_type=args.get("asset_type"),
        timeframe=args.get("timeframe"),
    )
    if timeframe == "1m" and asset_type != "stock":
        raise ValueError("分钟策略当前仅支持 A 股 (stock)")
    svc = ScreenerService(repo, asset_type=asset_type)
    as_of = svc.latest_date()
    if as_of is None:
        raise ValueError("本地暂无行情数据, 无法执行策略。")
    overrides = load_override(ctx.data_dir, strategy_id) or {} if ctx.data_dir else {}
    current = None
    if timeframe == "1m":
        minute_day = repo.latest_minute_date_global()
        if minute_day is None:
            raise ValueError("本地无分钟K数据，请先同步分钟K或开启盘中分钟刷新")
        # 日线可能尚未收盘，基础信息取已存在的日线快照，分钟窗口取最新分钟分区。
        current = svc._load_enriched_for_date(min(as_of, minute_day))
        as_of = minute_day
    context = svc.build_strategy_context(
        ctx.engine,
        as_of,
        [strategy_id],
        overrides_map={strategy_id: overrides},
        timeframe=timeframe,
        current=current,
        params_map={strategy_id: args.get("params") or {}},
    )
    result = ctx.engine.run(
        strategy_id, context, overrides=overrides or None, params=args.get("params")
    )
    rows = [_clean_obj(r) for r in (getattr(result, "rows", None) or [])][:30]
    return {
        "strategy_id": strategy_id,
        "asset_type": asset_type,
        "timeframe": timeframe,
        "as_of": _clean_value(getattr(result, "as_of", as_of)),
        "total": int(getattr(result, "total", 0) or 0),
        "elapsed_ms": round(float(getattr(result, "elapsed_ms", 0.0) or 0.0)),
        "rows": rows,
    }


def _get_factor_values(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    repo = _require_repo(ctx)
    factor = str(args.get("factor") or "").strip()
    if not factor:
        raise ValueError("factor 不能为空, 可先用 list_factors 查看可用因子 id。")
    top = _clamp(args.get("top"), 5, 50, 30)
    descending = str(args.get("direction") or "desc") != "asc"
    # 次新股过滤: 上市不足 N 个自然日的标的历史序列太短, 递推类指标 (RSI/MACD/均线)
    # 会给 0 或极端值, 直接按值排序会把整个榜单挤成新股 —— 用户要的是「全市场 RSI 最低」,
    # 不是「刚上市还没数据」。默认 60 天; 传 0 可关闭过滤。
    min_listed_days = _clamp(args.get("min_listed_days"), 0, 3650, 60)
    df, as_of = repo.get_enriched_latest()
    if df is None or df.is_empty():
        raise ValueError("本地暂无行情数据, 无法读取因子值。")
    if factor not in df.columns:
        raise ValueError(f"因子 {factor} 不在物化列中, 请先用 list_factors 确认因子 id。")
    keep = [c for c in ("symbol", factor, "change_pct") if c in df.columns]
    sub = df.select(keep).drop_nulls(subset=[factor])
    excluded_new = 0
    if min_listed_days > 0:
        listing = _listing_dates(repo)
        if listing:
            cutoff = as_of - timedelta(days=min_listed_days) if isinstance(as_of, date) else None
            if cutoff is not None:
                too_new = {s for s, d in listing.items() if d is not None and d > cutoff}
                if too_new:
                    before = sub.height
                    sub = sub.filter(~pl.col("symbol").is_in(list(too_new)))
                    excluded_new = before - sub.height
    sub = sub.sort(factor, descending=descending).head(top)
    names = repo.get_name_map(sub["symbol"].to_list()) if sub.height else {}
    rows = _rows(sub, top)
    for row in rows:
        row.setdefault("name", names.get(row.get("symbol"), ""))
    out: dict[str, Any] = {
        "factor": factor,
        "as_of": _clean_value(as_of),
        "count": len(rows),
        "rows": rows,
    }
    if excluded_new > 0:
        out["excluded_new_listings"] = excluded_new
        out["note"] = (
            f"已排除 {excluded_new} 只上市不足 {min_listed_days} 天的次新股 "
            "(历史序列不足会让递推指标失真, 例如新股 RSI 恒为 0)。"
            "要包含它们请传 min_listed_days=0。"
        )
    return out


def _strategy_guide_text() -> str:
    """读取策略编写规范 (内置精简指南)。

    助手没有文件读取工具, 让模型「去读 prompts/xxx.md」是够不着的指令 ——
    它只会凭印象编格式, 然后被保存校验拒掉。这里把规范作为工具返回值直接给它。
    """
    prompts = Path(__file__).resolve().parents[2] / "strategy" / "prompts"
    for name in ("strategy-guide-compact.md", "strategy-example.md"):
        p = prompts / name
        if p.exists():
            try:
                return p.read_text(encoding="utf-8")
            except OSError:
                continue
    return ""


def _create_strategy(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    """把完整 Python 策略代码落盘为 ai_ 草稿。

    复用 api/strategy.py 的 save_strategy_code_core —— 与「策略页保存」同一条
    校验链 (安全校验 / META 语义 / engine 加载核对 / 自定义信号存在性), 失败
    自动回滚。本工具不自己写文件, 也不放宽任何校验。
    新建策略固定为 research_only 草稿态, 需用户在策略页显式发布后才进入公开列表。

    args.show_guide=true 时只返回编写规范 (不落盘): 助手没有读文件工具, 模型
    必须先拿到 META 字段与信号函数契约, 再写代码, 否则必然被校验拒。
    """
    if args.get("show_guide"):
        guide = _strategy_guide_text()
        if not guide:
            raise ValueError("策略编写规范文件缺失, 无法生成策略。")
        return {
            "guide": guide,
            "next": "请严格按此规范编写完整策略文件, 再调用 create_strategy(不带 show_guide) 保存。",
        }

    from app.api.strategy import StrategyCodeSaveRequest, save_strategy_code_core

    if ctx.data_dir is None or ctx.engine is None:
        raise ValueError("数据目录或策略引擎未就绪。")

    code = str(args.get("code") or "").strip()
    if not code:
        raise ValueError("code 不能为空, 需提供完整策略 Python 源码 (含 META 字典)。")
    if len(code) > _MAX_STRATEGY_CODE_CHARS:
        raise ValueError(f"策略代码过长 ({len(code)} 字符), 上限 {_MAX_STRATEGY_CODE_CHARS}。")
    name = str(args.get("name") or "").strip()
    if not name:
        raise ValueError("name 不能为空 (策略中文名)。")
    description = str(args.get("description") or "").strip()

    # strategy_id 由助手生成: 不会覆盖用户已有文件, 也避免模型造出非法 id
    sid = f"ai_{uuid.uuid4().hex[:10]}"
    save_req = StrategyCodeSaveRequest(
        code=code,
        strategy_id=sid,
        target_source="ai",
        mode="create",
        name=name,
        description=description,
    )
    try:
        saved = save_strategy_code_core(save_req, ctx.engine, ctx.data_dir)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    # 与端点一致: 落盘后清策略缓存 + 让监控引擎重载规则状态
    from app.services import strategy_cache

    strategy_cache.clear_cache(ctx.data_dir)
    if ctx.repo is not None and hasattr(ctx.repo, "clear_cache"):
        # 清缓存失败不影响已成功的落盘
        with contextlib.suppress(Exception):
            ctx.repo.clear_cache()

    return {
        "created": True,
        "strategy_id": saved["strategy_id"],
        "name": name,
        "source": saved.get("source"),
        "research_only": bool(saved.get("research_only", True)),
        "path": saved.get("path"),
        "params": [
            p.get("id") for p in (saved.get("meta", {}).get("params") or []) if isinstance(p, dict)
        ],
        "note": (
            "已保存为 AI 策略草稿 (research_only), 不会进入公开列表、也不会被监控使用。"
            "请让用户到「策略」页确认并点「发布」后才正式生效; "
            "回测验证可用 run_backtest (需策略已发布或传入该 id)。"
        ),
    }
