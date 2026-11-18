"""扩展数据读取辅助：路径、分区、列裁剪、CK/本地读取与统计。"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from pathlib import Path

import polars as pl
from fastapi import HTTPException

from app.api._root_ref import bind as _bind
from app.market_time import CN_TZ
from app.polars_guard import guarded_collect
from app.services.ext_data import (
    ExtConfig,
    apply_config_mapping,
)
from app.services.ext_data_schema import PullAuthReq as PullAuthReq

_root = _bind("app.api.ext_data")
logger = _root.logger


def _apply_mapping(df: pl.DataFrame, config: ExtConfig, data_dir: Path) -> pl.DataFrame:
    return apply_config_mapping(df, config, data_dir)


def _clean_col_names(df: pl.DataFrame) -> pl.DataFrame:
    """清洗列名：去掉所有 (...) 及其内容，避免时间戳导致列名不稳定。"""
    import re

    renames = {col: re.sub(r"\([^)]*\)", "", col).strip() for col in df.columns}
    # 去重：如果清洗后重名，加序号后缀
    seen: dict[str, int] = {}
    final = {}
    for old, new in renames.items():
        if new in seen:
            seen[new] += 1
            final[old] = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
            final[old] = new
    return df.rename(final)


_DIMENSION_SEPARATOR_CLASS = r"、,，;；|/\s-"


def _filter_dimension_member_rows(df: pl.DataFrame, field: str, value: str) -> pl.DataFrame:
    """按分隔后的完整标签匹配成员，避免“人工智能”误命中“人工智能体”。"""
    if field not in df.columns:
        raise HTTPException(400, f"字段 '{field}' 不存在")
    normalized = value.strip()
    if not normalized:
        raise HTTPException(400, "标签值不能为空")
    pattern = rf"(^|[{_DIMENSION_SEPARATOR_CLASS}]){re.escape(normalized)}($|[{_DIMENSION_SEPARATOR_CLASS}])"
    return df.filter(
        pl.col(field).cast(pl.String, strict=False).fill_null("").str.contains(pattern)
    )


def _ext_data_dir(config: ExtConfig, data_dir: Path) -> Path:
    """返回扩展数据的数据目录。

    - snapshot: data/ext_data/{id}/（part.parquet 与 config.json 同级）
    - timeseries: data/ext_data/{id}/timeseries/
    """
    cfg_dir = data_dir / "ext_data" / config.id
    if config.mode == "timeseries":
        return cfg_dir / "timeseries"
    return cfg_dir


def _parquet_glob(config: ExtConfig, data_dir: Path) -> str:
    """返回该扩展配置下所有 parquet 文件的 glob 模式。

    snapshot: 'data/ext_data/{id}/*.parquet'（只有 part.parquet）
    timeseries: 'data/ext_data/{id}/timeseries/**/*.parquet'
    """
    cfg_dir = data_dir / "ext_data" / config.id
    if config.mode == "snapshot":
        return str(cfg_dir / "*.parquet")
    return str(cfg_dir / "timeseries" / "**" / "*.parquet")


def _safe_json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def _partition_date(raw: str) -> str:
    """把 `date` 入参规范成 `YYYY-MM-DD` 分区名。

    这个值直接拼进分区目录名 (`timeseries/date=<value>`), 所以非法值不只是格式问题:
    `date=x/../../../../kline_daily` 会让读取路径离开 `ext_data/<id>/timeseries/`。
    同一文件的 `/sync`、`/ingest`、`/backfill` 都先 `date.fromisoformat` 再用, 只有
    `/rows` 和 `/dimension-members` 走的这条路把原始字符串直接拼进了路径。
    """
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as e:
        raise HTTPException(400, f"日期格式错误: {raw}") from e


def _normalize_ext_dates(df: pl.DataFrame) -> pl.DataFrame:
    """CK 行还原成对外口径：去掉 ext_id、时间列回落 ``ts``、date 转字符串并补 data_date。"""
    if df.is_empty():
        return df
    out = df.drop("ext_id") if "ext_id" in df.columns else df
    if "time_value" in out.columns and "ts" not in out.columns:
        out = out.rename({"time_value": "ts"})
    if "date" in out.columns:
        out = out.with_columns(pl.col("date").cast(pl.Utf8))
        if "data_date" not in out.columns:
            out = out.with_columns(pl.col("date").alias("data_date"))
    return out


def _market_level_partition_days(base: Path) -> list[str]:
    """本地时序分区里可用的日期 (升序)。"""
    days: list[str] = []
    if not base.exists():
        return days
    for part in sorted(base.glob("date=*")):
        if not part.is_dir() or not (part / "part.parquet").exists():
            continue
        try:
            days.append(date.fromisoformat(part.name[5:]).isoformat())
        except ValueError:
            continue
    return days


def _select_ext_partitions(
    partitions: list[str],
    snapshot_day: str | None,
    lo: str | None,
    hi: str | None,
) -> tuple[list[str], str | None]:
    """按快照日 / 区间 / 默认最新挑分区; 返回 (选中日期, 对外日期标签)。"""
    if snapshot_day:
        if snapshot_day not in partitions:
            return [], snapshot_day
        return [snapshot_day], snapshot_day
    if lo or hi:
        selected = [d for d in partitions if (not lo or d >= lo) and (not hi or d <= hi)]
        if not selected:
            return [], None
        return selected, f"{selected[0]}..{selected[-1]}"
    if not partitions:
        return [], None
    return [partitions[-1]], partitions[-1]


def _read_market_level_timeseries(
    config: ExtConfig,
    data_dir: Path,
    *,
    snapshot_day: str | None = None,
    lo: str | None = None,
    hi: str | None = None,
    columns: list[str] | None = None,
    required_columns: list[str] | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """市场级时序表读本地 Parquet 分区。

    CK ``ext_timeseries`` 主键含 symbol, 市场级行 (无 symbol) 无法无损入库,
    写入侧不镜像 → 读侧同样以本地分区为准, 口径与 CK 分支一致:
    date 取分区名, data_date 同步给出, 损坏分区跳过不阻断整表。
    """
    base = data_dir / "ext_data" / config.id / "timeseries"
    selected, label = _select_ext_partitions(
        _market_level_partition_days(base),
        snapshot_day,
        lo,
        hi,
    )
    if not selected:
        return pl.DataFrame(), label

    frames: list[pl.DataFrame] = []
    for day in selected:
        path = base / f"date={day}" / "part.parquet"
        try:
            frame = pl.read_parquet(path)
        except Exception as exc:  # noqa: BLE001 - 损坏分区跳过, 不阻断整表
            logger.warning("ext_data: 市场级扩展表 %s 分区 %s 读取失败: %s", config.id, day, exc)
            continue
        frames.append(frame.with_columns(pl.lit(day).alias("date")))
    if not frames:
        return pl.DataFrame(), label
    df = pl.concat(frames, how="diagonal_relaxed")
    if "date" in df.columns:
        df = df.with_columns(pl.col("date").cast(pl.Utf8))
        if "data_date" not in df.columns:
            df = df.with_columns(pl.col("date").alias("data_date"))
    if columns:
        keep = [c for c in columns if c in df.columns]
        required = required_columns or []
        if keep and all(c in df.columns for c in required):
            df = df.select(list(dict.fromkeys([*keep, *required])))
    return df, label


def _resolve_ext_range(
    snapshot_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str | None, str | None, str | None]:
    """规范化查询区间: 返回 (快照日, 起始日, 结束日); 非法区间抛 400。"""
    snapshot_day = _partition_date(snapshot_date) if snapshot_date else None
    lo = _partition_date(start_date) if start_date else None
    hi = _partition_date(end_date) if end_date else None
    if lo and hi and lo > hi:
        raise HTTPException(400, f"start_date {lo} 晚于 end_date {hi}")
    return snapshot_day, lo, hi


def _select_ext_columns(
    df: pl.DataFrame,
    columns: list[str] | None,
    required_columns: list[str] | None,
) -> pl.DataFrame:
    """按请求列裁剪; 请求列缺失时保留全行 (沿用既有降级口径)。"""
    if not columns or df.is_empty():
        return df
    keep = [c for c in columns if c in df.columns]
    required = required_columns or []
    if keep and all(c in df.columns for c in required):
        return df.select(list(dict.fromkeys([*keep, *required])))
    return df


def _read_ext_timeseries_ck(
    config: ExtConfig,
    store,
    snapshot_day: str | None,
    lo: str | None,
    hi: str | None,
    columns: list[str] | None,
    required_columns: list[str] | None,
) -> tuple[pl.DataFrame, str | None]:
    """CK 时序表读取: 快照日 / 区间 / 默认最新分区。"""
    try:
        dates = store.list_ext_dates(config.id)
    except Exception as exc:  # noqa: BLE001 - 扩展表不可读时按空处理
        logger.warning("ext_data: 扩展表 %s 日期枚举失败: %s", config.id, exc)
        return pl.DataFrame(), None
    if not dates:
        return pl.DataFrame(), None

    if snapshot_day:
        target = date.fromisoformat(snapshot_day)
        if target not in set(dates):
            return pl.DataFrame(), snapshot_day
        df = store.read_ext_timeseries(config.id, start=target, end=target)
        return _select_ext_columns(
            _normalize_ext_dates(df), columns, required_columns
        ), snapshot_day

    if lo or hi:
        selected = [
            d for d in dates if (not lo or d.isoformat() >= lo) and (not hi or d.isoformat() <= hi)
        ]
        if not selected:
            return pl.DataFrame(), None
        # 行级日期列由 CK 的 date 列统一提供 (原分区名补列逻辑不再需要)
        df = store.read_ext_timeseries(config.id, start=selected[0], end=selected[-1])
        return (
            _select_ext_columns(_normalize_ext_dates(df), columns, required_columns),
            f"{selected[0].isoformat()}..{selected[-1].isoformat()}",
        )

    latest = dates[-1]
    df = store.read_ext_timeseries(config.id, start=latest, end=latest)
    return _select_ext_columns(
        _normalize_ext_dates(df), columns, required_columns
    ), latest.isoformat()


def _read_ext_snapshot_ck(
    config: ExtConfig,
    store,
    data_dir: Path,
    columns: list[str] | None,
    required_columns: list[str] | None,
) -> tuple[pl.DataFrame, str | None] | None:
    """CK 快照表读取; CK 无数据时返回 None (调用方回退本地分区)。"""
    try:
        dates = store.list_ext_dates(config.id)
    except Exception as exc:  # noqa: BLE001 - 扩展表不可读时按空处理
        logger.warning("ext_data: 快照表 %s 日期枚举失败: %s", config.id, exc)
        return None
    if not dates:
        logger.warning(
            "ext_data: 快照表 %s 在 CK 无数据, 回退本地分区（请检查镜像写入）", config.id
        )
        return None
    latest = dates[-1]
    df = store.read_ext_timeseries(config.id, start=latest, end=latest)
    return (
        _select_ext_columns(_normalize_ext_dates(df), columns, required_columns),
        _latest_sync_date(config, data_dir),
    )


def _collect_ext_frame(
    lf: pl.LazyFrame,
    columns: list[str] | None,
    required_columns: list[str] | None,
) -> pl.DataFrame:
    """按显示列/必需列裁剪扩展表，保留未知输出字段的原行回退语义。"""
    if columns:
        available = lf.collect_schema().names()
        keep = [c for c in columns if c in available]
        required = required_columns or []
        if keep and all(c in available for c in required):
            lf = lf.select(list(dict.fromkeys([*keep, *required])))
    return guarded_collect(lf, priority="interactive")


def _latest_sync_date(config: ExtConfig, data_dir: Path) -> str | None:
    """扫描数据文件，返回该扩展配置的最新同步时间（北京墙钟, 含时分秒）。

    - snapshot: 直接取 ext_data/{id}/part.parquet 的 mtime
    - timeseries: 扫描 ext_data/{id}/timeseries/date=xxx 分区目录

    用北京时间而非宿主机时钟: 前端 ExtDataStatCard 原样展示这串裸时间,
    容器默认 UTC 时会比同一页拉取面板里的 pull.last_run(带时区 ISO,
    浏览器按本地时区渲染)整整差一个时区。
    """
    from datetime import datetime

    if config.mode == "snapshot":
        # 快照: part.parquet 与 config.json 同级
        p = data_dir / "ext_data" / config.id / "part.parquet"
        if p.exists():
            ts = datetime.fromtimestamp(p.stat().st_mtime, tz=CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            return ts
        # 兼容旧路径
        old = data_dir / "instruments_ext"
        if old.exists():
            return _latest_sync_from_partitions(old)
        return None

    # 时序: 扫描 timeseries/date=xxx
    base = _ext_data_dir(config, data_dir)
    if not base.exists():
        # 兼容旧路径
        base = data_dir / "kline_ext"
    if not base.exists():
        return None
    return _latest_sync_from_partitions(base)


def _latest_sync_from_partitions(base: Path) -> str | None:
    """从 date=xxx 分区目录中找到最新分区的修改时间 (北京墙钟)。"""
    from datetime import datetime

    latest_ts: float = 0
    latest_date: str | None = None
    for d in base.iterdir():
        if d.is_dir() and d.name.startswith("date="):
            for f in d.glob("*.parquet"):
                mtime = f.stat().st_mtime
                if mtime > latest_ts:
                    latest_ts = mtime
                    latest_date = d.name[5:]
    if latest_date and latest_ts > 0:
        ts = datetime.fromtimestamp(latest_ts, tz=CN_TZ).strftime("%H:%M:%S")
        return f"{latest_date} {ts}"
    return latest_date


def _date_range(config: ExtConfig, data_dir: Path) -> list[str] | None:
    """返回时序型扩展数据的日期范围 [最早, 最新]（CK 为唯一来源）。"""
    if config.mode != "timeseries":
        return None
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return None
    try:
        dates = runtime.store.list_ext_dates(config.id)
    except Exception as exc:  # noqa: BLE001 - 范围不可得时前端退化为无范围
        logger.warning("ext_data: 日期范围枚举失败 %s: %s", config.id, exc)
        return None
    if not dates:
        return None
    return [dates[0].isoformat(), dates[-1].isoformat()]


def _plate_stats(config: ExtConfig, data_dir: Path) -> dict | None:
    """日内多盘表最新分区的盘次概览 (源卡片展示用)。

    只读最新分区的时间列: 盘数 = 时刻值去重计数, from/to = 时刻范围 (HH:MM:SS,
    与 collapse_ext_rows_at 的时刻切片同口径, 兼容 ISO 前缀/纯时刻两种列)。
    非多盘表 (非 timeseries 或未配 time_field)、无分区、列缺失或文件损坏 → None。
    """
    if config.mode != "timeseries":
        return None
    tf = config.pull.time_field if config.pull else None
    if not tf:
        return None
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return None
    try:
        dates = runtime.store.list_ext_dates(config.id)
        if not dates:
            return None
        latest = dates[-1]
        frame = runtime.store.read_ext_timeseries(config.id, start=latest, end=latest)
        if tf in frame.columns:
            col = frame[tf].cast(pl.Utf8)
        elif "time_value" in frame.columns:
            col = frame["time_value"].cast(pl.Utf8)
        else:
            return None
    except Exception:  # noqa: BLE001 — 列缺失/读取失败 → 无多盘信息, 不阻断列表
        return None
    if col.is_empty():
        return None
    tod = (
        pl.DataFrame({tf: col})
        .select(
            pl.when(pl.col(tf).str.len_chars() >= 11)
            .then(pl.col(tf).str.slice(11, 8))
            .otherwise(pl.col(tf))
            .alias("_tod")
        )
        .select(
            plates=pl.col("_tod").n_unique(),
            first=pl.col("_tod").min(),
            last=pl.col("_tod").max(),
            # 粒度判定 (值形态, 比盘数稳): 存在「含冒号且非 00:00:00」的时刻值
            # → 日内多盘; 全为日期形态 (无冒号) 或 00:00:00 → 日级别。
            # 盘后只拉过一两次的日内表不会因此误判 (非零时刻仍在)。
            intraday=(pl.col("_tod").str.contains(":") & (pl.col("_tod") != "00:00:00")).any(),
        )
        .row(0, named=True)
    )
    return {
        "date": latest.isoformat(),
        "plates": tod["plates"],
        "from": tod["first"],
        "to": tod["last"],
        "granularity": "intraday" if tod["intraday"] else "daily",
    }


def _granularity_effective(config: ExtConfig, data_dir: Path) -> str | None:
    """序列粒度生效值: 显式声明 > 数据形态自动判断; snapshot 表 → None。

    判断规则 (auto): 未配 time_field → daily (落盘契约「留空 = 每日快照表」);
    配了 time_field 但尚无可读分区 → intraday (以配置意图为准, 与历史 UI 行为
    一致); 有数据 → _plate_stats 的值形态判定。
    """
    if config.mode != "timeseries":
        return None
    declared = config.pull.granularity if config.pull else "auto"
    if declared in ("intraday", "daily"):
        return declared
    tf = config.pull.time_field if config.pull else None
    if not tf:
        return "daily"
    stats = _plate_stats(config, data_dir)
    if stats is None:
        return "intraday"
    return stats["granularity"]
