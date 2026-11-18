"""板块分时辅助：成分股分钟 K 聚合与全市场对照线。"""

from __future__ import annotations

import math
from datetime import date, datetime
from datetime import time as dt_time
from pathlib import Path

import polars as pl

from app.api._root_ref import bind as _bind
from app.services.ext_data import (
    ExtConfig,
)
from app.services.ext_data_schema import PullAuthReq as PullAuthReq

from ._ext_read import _filter_dimension_member_rows

_root = _bind("app.api.ext_data")
logger = _root.logger


# ---------------------------------------------------------------------------
# 板块分时 (dimension intraday)
# ---------------------------------------------------------------------------

# 点击触发 + 60s 进程内缓存: 分钟分区是滚动底座 (minute_refresh / 盘后分钟同步),
# 不做后台预计算 — 板块基数大而单次聚合仅几十毫秒。
_DIMENSION_INTRADAY_CACHE: dict[tuple[str, str, str, str | None], tuple[float, dict]] = {}
_DIMENSION_INTRADAY_CACHE_TTL_S = 60.0
# 成分股网格化 ffill 上限: 超大板块退化为逐时间戳可得均值 (内存保护)。
_DIMENSION_INTRADAY_FFILL_CAP = 2000
# 分钟 pct 物理边界 (与 services.sector_rotation._PCT_PHYSICAL_BOUND 同值,
# A 股日涨跌停最大 ±30%): 超出视为脏点剔除 (GH #473 同族)。
_DIMENSION_INTRADAY_PCT_BOUND = 0.30
_DIMENSION_INTRADAY_PCT_EPS = 1e-9  # 浮点容差: 130/100-1 略大于 0.30, 真涨停勿滤


def _bare_symbol_expr(col: str = "symbol") -> pl.Expr:
    """'000001.SZ' → '000001'; 已是裸代码则原样。"""
    return pl.col(col).cast(pl.String).str.strip_chars().str.split(".").list.first()


def _dimension_member_bares(matched: pl.DataFrame, config: ExtConfig) -> list[str]:
    """成分股裸代码集合 (symbol 列优先级与 dimension-members 端点一致)。"""
    if matched.is_empty():
        return []
    symbol_columns = ["symbol", "code", "股票代码", "代码"]
    for mapping in (config.symbol_map, config.code_map):
        if isinstance(mapping, dict) and mapping.get("type") == "mapped" and mapping.get("col"):
            symbol_columns.append(str(mapping["col"]))  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性
    cols = [c for c in dict.fromkeys(symbol_columns) if c in matched.columns]
    if not cols:
        return []
    coalesced = pl.coalesce(
        [pl.col(c).cast(pl.String).str.strip_chars().str.split(".").list.first() for c in cols]
    )
    series = matched.select(coalesced.alias("_bare")).to_series()
    return sorted({s for s in series.to_list() if s})


def _prev_daily_close(data_dir: Path, target_date: str) -> pl.DataFrame | None:
    """目标日前最近一个交易日的收盘价 → (_bare, prev_close); 无则 None。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return None
    try:
        dates = [d.isoformat() for d in runtime.store.list_daily_dates("kline_daily")]
    except Exception as exc:  # noqa: BLE001 - 前收不可得时调用方退化到首根分钟
        logger.warning("ext_data: 交易日枚举失败: %s", exc)
        return None
    prevs = [d for d in dates if d < target_date]
    if not prevs:
        return None
    prev = date.fromisoformat(prevs[-1])
    try:
        df = runtime.store.read_daily(
            "kline_daily",
            start=prev,
            end=prev,
            columns=["symbol", "close"],
        )
    except Exception:  # noqa: BLE001 - 前一交易日读失败返回 None, 调用方降级
        return None
    if df.is_empty():
        return None
    return (
        df.with_columns(_bare_symbol_expr().alias("_bare"))
        .select([pl.col("_bare"), pl.col("close").cast(pl.Float64).alias("prev_close")])
        # 前收 <= 0 / 非有限视为缺失 (否则 close/ref 为 inf, 整个响应 JSON 渲染 500),
        # 缺失时由调用方退化为当日首根有效分钟 close
        .filter(pl.col("prev_close").is_finite() & (pl.col("prev_close") > 0))
        .unique(subset=["_bare"], keep="last")
    )


def _dimension_intraday_compute(
    config: ExtConfig,
    data_dir: Path,
    field: str,
    value: str,
    snapshot_date: str | None,
) -> dict:
    """板块等权分时: 成分股当日分钟K逐分钟平均涨跌幅 + 全市场对照线。

    口径: pct = 分钟close / ref − 1 (小数制, 与快照涨跌幅契约一致, 前端 ×100 显示),
    ref 优先前一交易日日K收盘 (prev_close, 开盘跳空体现在曲线起点);
    日K缺失的标的退化为当日首根分钟close (混合基准)。
    停牌/无成交分钟按成分股 forward-fill 后再平均, 全市场线取逐时间戳可得均值。
    """
    target = _dimension_intraday_target(data_dir, snapshot_date)
    if not target:
        return {
            "status": "no_data",
            "reason": "minute_missing",
            "date": snapshot_date,
            "points": [],
        }

    ext_df, _active = _root._read_ext_dataframe(config, data_dir)
    if ext_df.is_empty() or field not in ext_df.columns:
        return {"status": "empty", "reason": "no_members", "date": target, "points": []}
    member_bares = _dimension_member_bares(
        _filter_dimension_member_rows(ext_df, field, value), config
    )
    if not member_bares:
        return {"status": "empty", "reason": "no_members", "date": target, "points": []}

    bars = _dimension_intraday_bars(target)
    if bars is None:
        return {"status": "no_data", "reason": "minute_schema", "date": target, "points": []}
    # close <= 0 / 非有限的分钟行无效: 作基准时 pct 为 inf, 作分子时是 -100% 假跌幅
    bars = bars.drop_nulls(subset=["datetime", "close"]).filter(
        pl.col("close").cast(pl.Float64).is_finite() & (pl.col("close") > 0)
    )
    if bars.is_empty():
        return {"status": "no_data", "reason": "minute_empty", "date": target, "points": []}

    joined, basis = _dimension_intraday_refs(bars, data_dir, target)

    member_bars = joined.filter(pl.col("_bare").is_in(member_bares))
    members_with_minute = member_bars["_bare"].n_unique() if not member_bars.is_empty() else 0
    if members_with_minute == 0:
        return {
            "status": "empty",
            "reason": "no_member_bars",
            "date": target,
            "member_count": len(member_bares),
            "members_with_minute": 0,
            "points": [],
        }
    combined = _dimension_intraday_combine(joined, member_bars, members_with_minute)
    points = _dimension_intraday_points(combined)
    return {
        "status": "ok",
        "date": target,
        "basis": basis,
        "member_count": len(member_bares),
        "members_with_minute": members_with_minute,
        "points": points,
    }


def _dimension_intraday_target(data_dir: Path, snapshot_date: str | None) -> str | None:
    minute_dir = data_dir / "kline_minute"
    partitions: list[str] = []
    if minute_dir.exists():
        partitions = sorted(
            d.name[5:]
            for d in minute_dir.iterdir()
            if d.is_dir() and d.name.startswith("date=") and (d / "part.parquet").exists()
        )
    if snapshot_date:
        return snapshot_date if snapshot_date in partitions else None
    return partitions[-1] if partitions else None


def _dimension_intraday_bars(target: str):
    try:
        from app.storage.runtime import require_storage

        day = date.fromisoformat(target)
        return require_storage().store.read_minute(
            "kline_minute",
            start=datetime.combine(day, dt_time.min),
            end=datetime.combine(day, dt_time.max),
            columns=["symbol", "datetime", "close"],
        )
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("dimension-intraday read minute data failed: %s", exc)
        return None


def _dimension_intraday_refs(bars, data_dir: Path, target: str):
    bars = bars.with_columns(_bare_symbol_expr().alias("_bare"))
    prev = _prev_daily_close(data_dir, target)
    joined = (
        bars.join(prev, on="_bare", how="left")
        if prev is not None
        else bars.with_columns(pl.lit(None, dtype=pl.Float64).alias("prev_close"))
    )
    refs = (
        joined.group_by("_bare")
        .agg(
            pl.col("prev_close").first().alias("_prev"),
            pl.col("close").sort_by("datetime").first().alias("_first"),
        )
        .with_columns(pl.coalesce(["_prev", "_first"]).alias("_ref"))
    )
    n_prev = refs["_prev"].is_not_null().sum()
    basis = "prev_close" if n_prev == refs.height else ("first_close" if n_prev == 0 else "mixed")
    joined = (
        joined.join(refs.select(["_bare", "_ref"]), on="_bare", how="left")
        .with_columns((pl.col("close") / pl.col("_ref") - 1.0).alias("_pct"))
        # 物理边界外视为脏点剔除 (与 sector_rotation 同口径, GH #473 同族)
        .filter(pl.col("_pct").abs() <= _DIMENSION_INTRADAY_PCT_BOUND + _DIMENSION_INTRADAY_PCT_EPS)
    )
    return joined, basis


def _dimension_intraday_combine(joined, member_bars, members_with_minute):
    if members_with_minute <= _DIMENSION_INTRADAY_FFILL_CAP:
        # 网格化 (成分股 × 全时间轴) + 逐股 ffill: 停牌分钟冻结在最后价而非退出均值
        grid = member_bars.select(pl.col("_bare").unique()).join(
            joined.select(pl.col("datetime").unique()), how="cross"
        )
        member_bars = (
            grid.join(
                member_bars.select(["_bare", "datetime", "_pct"]),
                on=["_bare", "datetime"],
                how="left",
            )
            .sort(["_bare", "datetime"])
            .with_columns(pl.col("_pct").forward_fill().over("_bare"))
        )
    sector = member_bars.group_by("datetime").agg(pl.col("_pct").mean().alias("_sector"))
    market = joined.group_by("datetime").agg(pl.col("_pct").mean().alias("_market"))
    return market.join(sector, on="datetime", how="left").sort("datetime")


def _r4(v) -> float | None:
    return (
        round(float(v), 4)
        if v is not None and not (isinstance(v, float) and math.isnan(v))
        else None
    )


def _dimension_intraday_points(combined):
    return [
        {
            "time": row["datetime"].strftime("%H:%M"),
            "sector": _r4(row["_sector"]),
            "market": _r4(row["_market"]),
        }
        for row in combined.iter_rows(named=True)
    ]
