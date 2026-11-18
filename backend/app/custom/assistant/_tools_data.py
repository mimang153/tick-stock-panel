"""assistant tools — data 分组实现。

由 app.custom.assistant.tools 组合并 re-export；公开导入路径不变。
"""

from __future__ import annotations

from app.custom.assistant.tools import (
    Any,
    Path,
    ToolContext,
    _clamp,
    _clean_obj,
    _clean_value,
    _validate_symbol,
    asyncio,
    date,
    pl,
    timedelta,
)


# ----------------------------------------------------------------
# 数据完整性与补全 — 检查只读, 补全是动作工具(确认闸门把守)。
# 同步触发复用数据页同一条路径: 盘后管道 / 分钟K扩展 / 财务表调度器,
# 全部后台执行、单飞去重, 工具立即返回, 进度用 get_sync_status 轮询。
# ----------------------------------------------------------------
def _partition_dates(data_dir: Path, subdir: str) -> list[str]:
    """date=YYYY-MM-DD 分区目录扫描 — 与 /api/data/status 同款轻量口径, 不读 parquet。"""
    root = data_dir / subdir
    if not root.exists():
        return []
    dates = [d.name[5:] for d in root.iterdir() if d.is_dir() and d.name.startswith("date=")]
    return sorted(dates)


def _biz_days_behind(latest_iso: str | None, today: date) -> int:
    """数据最新日落后今天约多少个工作日 — 无交易日历, 按周一~周五近似, 节假日会高估。

    数的是最新日之后、截至今天 (含) 的工作日: 周五的数据在周六/周日不算落后。
    """
    if not latest_iso:
        return 0
    try:
        latest = date.fromisoformat(str(latest_iso)[:10])
    except ValueError:
        return 0
    count = 0
    cur = latest
    while cur < today:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            count += 1
    return count


def _coverage_section(dates: list[str]) -> dict[str, Any]:
    if not dates:
        return {"present": False}
    return {
        "present": True,
        "earliest_date": dates[0],
        "latest_date": dates[-1],
        "trading_days": len(dates),
    }


def _symbol_coverage(
    ctx: ToolContext,
    symbol: str,
    global_daily_latest: str | None,
    today: date,
) -> dict[str, Any]:
    """单标的覆盖: 近 30 日日K行数/最新日 + 财务报告期, 用于「为什么这只没数据」。"""
    out: dict[str, Any] = {"symbol": symbol}
    if ctx.repo is not None:
        try:
            asset_type = ctx.repo.resolve_asset_type(symbol)
            df = ctx.repo.get_daily_asset(asset_type, symbol, today - timedelta(days=30), today)
            if df is None or df.is_empty():
                out["daily_missing"] = True
                out["latest_daily"] = None
            else:
                last = df.sort("date").tail(1).to_dicts()[0] if "date" in df.columns else {}
                latest = _clean_value(last.get("date"))
                out["latest_daily"] = latest
                out["daily_rows_30d"] = df.height
                out["daily_missing"] = bool(
                    global_daily_latest and latest and str(latest)[:10] < global_daily_latest
                )
        except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            out["daily_error"] = str(exc)[:80]
    if ctx.data_dir is not None:
        try:
            from app.services.financial_sync import get_financial_df

            df = get_financial_df(ctx.data_dir, "metrics")
            if df is None or df.is_empty() or "symbol" not in df.columns:
                out["has_financials"] = False
            else:
                sub = df.filter(pl.col("symbol") == symbol)
                out["has_financials"] = not sub.is_empty()
                period = next(
                    (c for c in ("period_end", "report_date", "end_date") if c in sub.columns), None
                )
                if period and sub.height:
                    out["latest_financial_period"] = _clean_value(sub[period].max())
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            pass
    return out


def _financial_coverage(data_dir: Path, issues: list[str]) -> dict[str, Any]:
    """汇总财务表覆盖状态，并把无财务数据加入问题列表。"""
    from app.services.financial_sync import FINANCIAL_TABLES, get_financial_df

    financials: dict[str, Any] = {}
    any_financial = False
    for table in FINANCIAL_TABLES:
        try:
            df = get_financial_df(data_dir, table)
        except Exception:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
            df = None
        if df is None or df.is_empty():
            financials[table] = {"present": False}
            continue
        any_financial = True
        period = next(
            (c for c in ("period_end", "report_date", "end_date") if c in df.columns), None
        )
        financials[table] = {
            "present": True,
            "rows": df.height,
            "latest_period": _clean_value(df[period].max()) if period else None,
        }
    if not any_financial:
        issues.append("本地没有任何财务数据, 财务问答需先同步财务表。")
    return financials


def _check_data_coverage(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services.pipeline_jobs import job_store

    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    today = cn_today()
    daily = _partition_dates(ctx.data_dir, "kline_daily")
    enriched = _partition_dates(ctx.data_dir, "kline_daily_enriched")
    minute = _partition_dates(ctx.data_dir, "kline_minute")
    daily_latest = daily[-1] if daily else None
    enriched_latest = enriched[-1] if enriched else None

    issues: list[str] = []
    if not daily:
        issues.append("本地没有任何股票日线数据, 请先执行数据同步(盘后管道)。")
    elif _biz_days_behind(daily_latest, today) >= 1:
        behind = _biz_days_behind(daily_latest, today)
        issues.append(
            f"日线停留在 {daily_latest}, 落后最近工作日约 {behind} 天(节假日可能高估), 建议补全。"
        )
    if daily and not enriched:
        issues.append("指标数据(enriched)尚未生成, 多数分析页与策略依赖它, 建议运行盘后管道。")
    elif daily and enriched and enriched_latest < daily_latest:  # type: ignore[operator]
        issues.append(
            f"指标(enriched)停留在 {enriched_latest}, 落后日线 {daily_latest}, 建议重跑盘后管道。"
        )
    if not minute:
        issues.append("本地无分钟K数据(分时图/盘中信号依赖; 未开启分钟同步可忽略)。")

    financials = _financial_coverage(ctx.data_dir, issues)

    result: dict[str, Any] = {
        "today": today.isoformat(),
        "daily": _coverage_section(daily),
        "enriched": _coverage_section(enriched),
        "minute": _coverage_section(minute),
        "financials": financials,
        "issues": issues,
    }

    try:
        job_store.reap_stale()
        active_id = job_store.active_id()
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        active_id = None
    if active_id:
        job = job_store.get(active_id) or {}
        result["active_job"] = {
            "job_id": active_id,
            "stage": job.get("stage"),
            "progress": job.get("progress"),
            "status": job.get("status"),
        }

    symbol = str(args.get("symbol") or "").strip()
    if symbol:
        symbol = _validate_symbol(symbol)
        info = _symbol_coverage(ctx, symbol, daily_latest, today)
        result["symbol"] = info
        if info.get("daily_missing"):
            issues.append(
                f"{symbol} 本地日线缺失或停更(最新 {info.get('latest_daily') or '无数据'}), "
                "可先补全数据再分析。"
            )
    return result


# ── 扩展数据表只读问答: 复用数据页 ext_data 的同一条读取链 ──────────


def _ext_table_freshness(data_dir: Path, table_id: str, mode: str) -> dict[str, Any]:
    """扫描分区目录给出数据新鲜度(轻量, 不读 parquet 内容)。

    timeseries: 最新分区日期 + 分区数; snapshot: part.parquet 是否存在。
    """
    if mode == "snapshot":
        exists = (data_dir / "ext_data" / table_id / "part.parquet").exists()
        return {"has_data": exists}
    base = data_dir / "ext_data" / table_id / "timeseries"
    if not base.exists():
        return {"has_data": False}
    dates = sorted(
        d.name[5:]
        for d in base.iterdir()
        if d.is_dir() and d.name.startswith("date=") and (d / "part.parquet").exists()
    )
    if not dates:
        return {"has_data": False}
    return {
        "has_data": True,
        "latest_date": dates[-1],
        "earliest_date": dates[0],
        "days": len(dates),
    }


def _list_ext_tables(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    """列出用户配置的扩展数据表: 模式、字段、说明与数据日期。"""
    if ctx.data_dir is None:
        raise ValueError("数据目录不可用")
    from app.services.ext_data import ExtConfigStore

    configs = ExtConfigStore(Path(ctx.data_dir)).load_all()
    tables: list[dict[str, Any]] = []
    for c in configs:
        item: dict[str, Any] = {
            "id": c.id,
            "label": c.label,
            "mode": c.mode,
            "market_level": c.market_level,
            "description": c.description,
            "fields": [{"name": f.name, "dtype": f.dtype, "label": f.label} for f in c.fields],
        }
        item.update(_ext_table_freshness(Path(ctx.data_dir), c.id, c.mode))
        tables.append(item)
    tables.sort(key=lambda t: (not t.get("has_data"), t["id"]))
    result: dict[str, Any] = {"tables": tables, "count": len(tables)}
    if not tables:
        result["note"] = "还没有配置任何扩展数据表; 可在数据页的扩展数据里创建或上传。"
    return result


def _query_ext_table(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    """读取一张扩展数据表的行: 支持等值过滤、排序、日期范围, limit 封顶 100。"""
    if ctx.data_dir is None:
        raise ValueError("数据目录不可用")
    from fastapi import HTTPException

    from app.api.ext_data import _apply_row_filters, _apply_row_sort, _read_ext_dataframe
    from app.services.ext_data import ExtConfigStore

    table = str(args.get("table") or "").strip()
    if not table:
        raise ValueError("缺少参数 table (扩展表 id, 可先用 list_ext_tables 查询)")
    config = ExtConfigStore(Path(ctx.data_dir)).get(table)
    if config is None:
        raise ValueError(f"扩展表不存在: {table} (可先用 list_ext_tables 查看全部表)")

    try:
        df, active_date = _read_ext_dataframe(
            config,
            Path(ctx.data_dir),
            str(args["date"]) if args.get("date") else None,
            str(args["start_date"]) if args.get("start_date") else None,
            str(args["end_date"]) if args.get("end_date") else None,
        )
        if args.get("filter"):
            df = _apply_row_filters(df, [str(args["filter"])])
        if args.get("sort"):
            df = _apply_row_sort(df, str(args["sort"]))
    except HTTPException as exc:  # 复用的过滤/排序/日期校验按契约转错误文案
        raise ValueError(str(exc.detail)) from exc

    total = df.height
    limit = max(1, min(100, int(args.get("limit") or 20)))
    rows = [{k: _clean_value(v) for k, v in row.items()} for row in df.head(limit).to_dicts()]
    result: dict[str, Any] = {
        "id": config.id,
        "label": config.label,
        "mode": config.mode,
        "date": active_date,
        "total": total,
        "returned": len(rows),
        "limit": limit,
        "columns": df.columns,
        "rows": rows,
    }
    if total > len(rows):
        result["note"] = (
            f"共 {total} 行, 只返回前 {len(rows)} 行; 可用 filter=字段:值1|值2 过滤、"
            "sort=字段:desc 排序后再查。"
        )
    return result


def _get_sync_status(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    import contextlib

    from app.services.pipeline_jobs import job_store

    keys = ("id", "status", "stage", "progress", "error", "started_at", "finished_at", "duration_s")
    with contextlib.suppress(Exception):
        job_store.reap_stale()
    out: dict[str, Any] = {"recent_jobs": []}
    active_id = None
    with contextlib.suppress(Exception):
        active_id = job_store.active_id()
    if active_id:
        job = job_store.get(active_id) or {}
        out["active_job"] = {k: _clean_value(job.get(k)) for k in keys if job.get(k) is not None}
    with contextlib.suppress(Exception):
        out["recent_jobs"] = [
            {k: _clean_value(j.get(k)) for k in keys if j.get(k) is not None}
            for j in job_store.list_recent(limit=5)
        ]
    fs = ctx.financial_scheduler
    if fs is not None:
        out["financial_syncing"] = bool(getattr(fs, "is_syncing", False))
        out["financial_last_sync"] = _clean_obj(getattr(fs, "last_sync", None) or {})
    if not active_id and not out.get("financial_syncing"):
        out["note"] = "当前没有正在运行的数据任务。"
    return out


async def _sync_data(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    """数据补全(动作工具): 触发后台同步任务并立即返回, 不等待完成。

    kind=pipeline 复用 POST /api/pipeline/run 的触发路径(单飞),
    kind=minute_extend 复用 POST /api/kline/sync_minute 的向前扩展模式,
    kind=financials 复用 POST /api/financials/sync/{table} 的调度器。
    """
    from fastapi import HTTPException

    kind = str(args.get("kind") or "pipeline")
    if kind not in {"pipeline", "financials", "minute_extend"}:
        raise ValueError(
            "kind 必须是 pipeline(盘后管道) / financials(财务表) / minute_extend(扩展分钟K历史) 之一。"
        )
    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")

    if kind == "pipeline":
        if ctx.repo is None or ctx.capabilities is None:
            raise ValueError("数据仓库或数据源能力未就绪, 无法启动同步。")
        from app.api.pipeline import trigger_pipeline_job

        trigger = await trigger_pipeline_job(ctx.repo, ctx.capabilities, ctx.quote_service)
        return {
            "started": True,
            "kind": kind,
            "reused": bool(trigger.get("reused")),
            "job_id": trigger.get("job_id"),
            "note": "盘后管道已在后台执行(日K/指标/复权因子/名单, 含已启用的分钟K), "
            "可用 get_sync_status 查进度; 完成前部分数据会停留在旧交易日。",
        }

    if kind == "minute_extend":
        if ctx.repo is None or ctx.capabilities is None:
            raise ValueError("数据仓库或数据源能力未就绪, 无法启动同步。")
        from app.api.kline import trigger_minute_sync

        days = _clamp(args.get("days"), 30, 1095, 365)
        try:
            trigger = await trigger_minute_sync(
                ctx.repo,
                ctx.capabilities,
                override_days=days,
                extend_flag=True,
            )
        except HTTPException as exc:  # 权限不足等 4xx → 工具错误契约
            raise ValueError(str(getattr(exc, "detail", "") or exc)) from exc
        return {
            "started": True,
            "kind": kind,
            "days": days,
            "reused": trigger.get("status") == "reused",
            "job_id": trigger.get("job_id"),
            "note": f"分钟K向前扩展已启动(目标 {days} 天), 全市场拉取耗时较长, 可用 get_sync_status 轮询进度。",
        }

    fs = ctx.financial_scheduler
    if fs is None:
        raise ValueError("财务同步调度器未就绪(需要数据源的财务数据能力)。")
    from app.services.financial_sync import FINANCIAL_TABLES

    table = str(args.get("table") or "all")
    if table not in {*FINANCIAL_TABLES, "all"}:
        raise ValueError(f"table 必须是 {sorted(FINANCIAL_TABLES)} 之一或 all。")
    target = None if table == "all" else table
    detail = await asyncio.to_thread(fs.trigger, target)
    return {
        "started": True,
        "kind": kind,
        "table": table,
        "detail": _clean_obj(detail),
        "note": "财务表同步已在后台执行(全量需数分钟), 可用 get_sync_status 查看进度。",
    }


def cn_today():
    """动态取根模块 cn_today，保留 monkeypatch 可观察性。"""
    from app.custom.assistant import tools as _tools_root

    return _tools_root.cn_today()
