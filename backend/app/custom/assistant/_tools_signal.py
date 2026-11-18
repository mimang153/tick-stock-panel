"""assistant tools — signal 分组实现。

由 app.custom.assistant.tools 组合并 re-export；公开导入路径不变。
"""

from __future__ import annotations

from app.custom.assistant.tools import (
    _SIGNAL_FIELD_HINT,
    Any,
    Path,
    ToolContext,
    _validate_symbol,
    date,
    datetime,
    uuid,
)


def _listing_dates(repo: Any) -> dict[str, date]:
    """symbol → 上市日期 (来自 instruments 维表); 读不到时返回空 dict。

    repo 是 KlineRepository, 拿 instruments 要么走它的缓存要么直读 parquet。
    这里只做一次轻量读, 失败不影响主流程 (过滤退化为不过滤)。
    """
    try:
        from app.services.instrument_sync import load_instruments

        df = load_instruments(getattr(repo, "storage", None))
        if df.is_empty():
            return {}
        df = df.select(["symbol", "listing_date"])
        out: dict[str, date] = {}
        for row in df.drop_nulls(subset=["listing_date"]).iter_rows(named=True):
            val = row.get("listing_date")
            parsed: date | None = None
            if isinstance(val, datetime):
                parsed = val.date()
            elif isinstance(val, date):
                parsed = val
            elif isinstance(val, str) and len(val) >= 10:
                try:
                    parsed = date.fromisoformat(val[:10])
                except ValueError:
                    parsed = None
            if parsed is not None:
                out[str(row["symbol"])] = parsed
        return out
    except Exception:  # noqa: BLE001 — 维表不可读时退化为不过滤
        return {}


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


def _create_signal_strategy(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.strategy import custom_signals

    if ctx.data_dir is None:
        raise ValueError("数据目录未就绪。")
    name = str(args.get("name") or "").strip()
    kind = str(args.get("kind") or "entry")
    raw_conditions = args.get("conditions")
    if not isinstance(raw_conditions, list) or not raw_conditions:
        raise ValueError("conditions 不能为空, 每条形如 {left, op, right, leftDays?, rightDays?}。")
    conditions: list[dict[str, Any]] = []
    for i, cond in enumerate(raw_conditions[:8]):
        if not isinstance(cond, dict):
            raise ValueError(f"第 {i + 1} 个条件必须是 {{left, op, right}} 对象。")
        right = cond.get("right")
        if isinstance(right, bool) or right is None or str(right).strip() == "":
            raise ValueError(f"第 {i + 1} 个条件: right 不能为空 (数字或 'field:字段名')。")
        try:
            left_days = int(cond.get("leftDays") or 0)
            right_days = int(cond.get("rightDays") or 0)
        except (TypeError, ValueError):
            raise ValueError(f"第 {i + 1} 个条件: leftDays/rightDays 必须是整数。") from None
        conditions.append(
            {
                "left": str(cond.get("left") or "").strip(),
                "op": str(cond.get("op") or ""),
                "right": str(right),
                "leftDays": left_days,
                "rightDays": right_days,
            }
        )
    # id 由助手生成而非模型给定: 不会覆盖用户已有的同名/同 id 信号
    sig = {
        "id": f"csg_a{uuid.uuid4().hex[:10]}",
        "name": name,
        "kind": kind,
        "conditions": conditions,
        "enabled": True,
        "timeframe": str(args.get("timeframe") or "daily"),
        "min_bars": int(args.get("min_bars") or 0),
    }
    try:
        custom_signals.validate(sig)
    except ValueError as exc:
        from app.strategy.intraday_features import INTRADAY_FEATURES

        hint = (
            "盘中可用字段: " + ", ".join(INTRADAY_FEATURES)
            if sig["timeframe"] == "intraday"
            else _SIGNAL_FIELD_HINT
        )
        raise ValueError(f"{exc}。{hint}") from exc
    custom_signals.save_one(ctx.data_dir, sig)
    _invalidate_custom_signals(ctx.data_dir, ctx.repo)
    return {
        "created": True,
        "signal": {
            "id": sig["id"],
            "name": name,
            "kind": kind,
            "conditions": conditions,
            "timeframe": sig["timeframe"],
            "min_bars": sig["min_bars"],
        },
        "note": "已写入信号库并默认启用, 可在「信号库」页面查看/编辑/删除; 创建成功后可让用户在策略里引用。",
    }


def _add_to_watchlist(args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
    from app.services import watchlist as watchlist_service

    symbol = _validate_symbol(args.get("symbol"))
    note = str(args.get("note") or "").strip()[:80]
    current = watchlist_service.list_symbols()
    if symbol in {str(r.get("symbol")) for r in current}:
        return {
            "added": False,
            "symbol": symbol,
            "total": len(current),
            "note": f"{symbol} 已在自选列表中, 未重复添加。",
        }
    rows = watchlist_service.add(symbol, note=note)
    return {
        "added": True,
        "symbol": symbol,
        "total": len(rows),
        "note": f"已把 {symbol} 加入自选列表(当前共 {len(rows)} 只)。",
    }
