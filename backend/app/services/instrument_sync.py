"""标的维表同步服务。

盘前 9:10 调用 tf.exchanges.get_instruments("SH"/"SZ"/"BJ", type="stock")
获取全量标的元数据，flatten ext 字段，写入 instruments.parquet。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from app.market_time import cn_today
from app.tickflow import client as tf_client
from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

_EXCHANGES = ["SH", "SZ", "BJ"]


def _flatten_instruments(items: list[dict], *, current_limits: bool = True) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up") if current_limits else None
        row["limit_down"] = ext.get("limit_down") if current_limits else None
        rows.append(row)
    return rows


def _fetch_instruments_via_provider() -> list[dict] | None:
    """若当前日K数据源不是 tickflow 且该 provider 提供 get_instruments, 用它拉标的维表。

    返回 flatten 行列表; 未命中(仍应走 tickflow)时返回 None。
    标的维表跟随日K数据源(二者天然耦合, 无独立偏好项)。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return None
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_custom_provider(provider_name):
        return None
    provider = custom_sources.get_provider(provider_name)
    if not hasattr(provider, "get_instruments"):
        return None
    try:
        items = provider.get_instruments("stock") or []
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("provider %s get_instruments 失败: %s", provider_name, e)
        return None
    rows = _flatten_instruments(items)
    logger.info("instruments via %s: %d stocks", provider_name, len(rows))
    return rows


def load_instruments(storage=None) -> pl.DataFrame:
    """统一读取标的维表（ClickHouse 为唯一数据源）。

    供原先直接 `read_parquet(data/instruments/instruments.parquet)` 的调用方替换。
    """
    if storage is None:
        from app.storage.runtime import require_storage

        storage = require_storage().store
    return storage.read_instruments("stock")


def sync_instruments(data_dir: Path, storage=None) -> int:
    """全量同步标的维表 → ClickHouse。

    返回写入的行数。
    """
    all_rows = _fetch_instruments_via_provider()
    if all_rows is None:
        # 未命中非 tickflow provider → 走 tickflow 直连
        tf = get_client()
        # 免费接口的限价来自历史行情，不能盖上同步日期当作当日权威价。
        current_limits = tf_client.current_mode() == "api_key"
        all_rows = []
        for ex in _EXCHANGES:
            try:
                items = tf.exchanges.get_instruments(ex, instrument_type="stock")
                if items:
                    all_rows.extend(_flatten_instruments(items, current_limits=current_limits))
                    logger.info("instruments %s: %d stocks", ex, len(items))
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("get_instruments(%s) failed: %s", ex, e)

    if not all_rows:
        return 0

    df = pl.DataFrame(all_rows)
    if storage is None:
        from app.storage.runtime import require_storage

        storage = require_storage().store
    existing = load_instruments(storage)
    df = _preserve_instrument_fields(df, existing)
    df = df.with_columns(pl.lit(cn_today()).cast(pl.Date).alias("as_of"))

    storage.write_instruments("stock", df)

    logger.info("instruments synced: %d rows → clickhouse", df.height)
    return df.height


# 股本与静态交易规则字段: 部分行情源的标的维表只有代码/名称/市场/类型。
# 这些字段缺失会让 enriched 的 turnover_rate / 市值为空, 进而使
# 依赖换手率与流通市值的策略筛不出任何标的 —— 因此绝不能用 null 覆盖已有值。
# 涨跌停价随交易日变化，不从历史维表继承。
_SHARE_FIELDS = (
    "listing_date",
    "total_shares",
    "float_shares",
    "tick_size",
)


def _preserve_instrument_fields(df: pl.DataFrame, existing: pl.DataFrame) -> pl.DataFrame:
    """用现有维表补全 provider 未提供的名称、股本和交易规则字段。

    新增标的仍保留 null (没有历史值可用, 不伪造); 只在同一 symbol 上把
    provider 的空值替换为既有值, 并在日志里给出补全规模。
    """
    if "name" in df.columns:
        df = df.with_columns(pl.col("name").cast(pl.Utf8).str.strip_chars().replace("", None))
    available = [c for c in ("name", *_SHARE_FIELDS) if c in df.columns]
    if not available:
        return df
    if all(df[c].null_count() == 0 for c in available):
        return df  # provider 已提供全部字段, 无需补全

    if existing.is_empty():
        logger.warning(
            "instruments provider 缺少标的信息且无历史维表可补全: %s",
            "/".join(c for c in available if df[c].null_count()),
        )
        return df
    if "symbol" not in existing.columns:
        return df

    keep = ["symbol", *[c for c in available if c in existing.columns]]
    if len(keep) <= 1:
        return df
    merged = df.join(existing.select(keep), on="symbol", how="left", suffix="_prev")
    filled: list[str] = []
    for col in keep[1:]:
        prev = f"{col}_prev"
        if prev not in merged.columns:
            continue
        # 逐行 coalesce: provider 有值用 provider, 为空才回落历史值。这样
        # 「部分标的缺字段」也能补齐, 而不只处理整列为空的源。
        if merged[col].null_count() and merged[prev].is_not_null().any():
            merged = merged.with_columns(pl.coalesce(pl.col(col), pl.col(prev)).alias(col))
            filled.append(col)
    merged = merged.drop([f"{c}_prev" for c in keep[1:] if f"{c}_prev" in merged.columns])
    if filled:
        restored = int(merged.select([pl.col(c).is_not_null().sum() for c in filled]).row(0)[0])
        logger.info(
            "instruments provider 缺 %s, 已逐行从历史维表补全 (首列非空 %d/%d)",
            "/".join(filled),
            restored,
            merged.height,
        )
    return merged


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    df = load_instruments()
    if df.is_empty():
        return 0

    # 只更新空 name 的行
    updates = pl.DataFrame(
        {
            "symbol": list(name_map.keys()),
            "_new_name": list(name_map.values()),
        }
    )
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    from app.storage.runtime import require_storage

    require_storage().store.write_instruments("stock", df)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)
