"""把本地 Parquet 日K/除权数据回填到 ClickHouse（B2 读路径切换的先决步骤）。

设计文档 §8.4 的过渡期双写只从启用时刻开始积累数据；要让"读切 CK"之后页面仍有
历史数据，必须先做一次离线回填。回填是幂等的（ReplacingMergeTree 按主键收敛），
可以反复执行、也可以中途中断后重跑。

用法：

    uv run python scripts/backfill_ck.py                    # 日K + 分钟K + 除权 + 标维表
    uv run python scripts/backfill_ck.py --tables kline_daily kline_daily_enriched
    uv run python scripts/backfill_ck.py --dry-run           # 只统计不写入

退出码：0 = 成功；2 = CK 连不上；3 = 有分区读失败（其余分区已尽力写入）。
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings
from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore

logger = logging.getLogger("backfill_ck")

DAILY_TABLES = (
    "kline_daily",
    "kline_daily_enriched",
    "kline_index_daily",
    "kline_index_enriched",
    "kline_etf_daily",
    "kline_etf_enriched",
)
ADJ_TABLES = ("adj_factor", "adj_factor_etf")

#: 分钟K：按 date= 分区，单分区可能是全市场一天（百万级行），按 symbol 分块写入
MINUTE_TABLES = ("kline_minute", "kline_etf_minute")
MINUTE_SYMBOL_BLOCK = 500

#: 标的维表：Parquet 为整表快照文件 `<table>/<table>.parquet`（不是按日分区）
INSTRUMENT_TABLES: dict[str, str] = {
    "instruments": "stock",
    "instruments_index": "index",
    "instruments_etf": "etf",
}

#: 财务宽表：CK 表名 → Parquet 目录（financials/<kind>/part.parquet）
FINANCIAL_TABLES: dict[str, str] = {
    "financials_metrics": "metrics",
    "financials_income": "income",
    "financials_balance_sheet": "balance_sheet",
    "financials_cash_flow": "cash_flow",
    "financials_shares": "shares",
}


def _iter_daily_parts(root: Path):
    yield from sorted(root.glob("date=*/part.parquet"))


def _backfill_daily(
    store: ClickHouseStore, data_dir: Path, table: str, *, dry_run: bool
) -> tuple[int, int, int]:
    """返回 (分区数, 写入行数, 失败分区数)。"""
    root = data_dir / table
    if not root.exists():
        logger.info("%s: 目录不存在, 跳过", table)
        return 0, 0, 0
    parts = 0
    rows = 0
    failed = 0
    for part in _iter_daily_parts(root):
        parts += 1
        try:
            df = pl.read_parquet(part)
        except Exception as exc:  # noqa: BLE001 - 单个坏分区不阻断整体回填
            logger.warning("%s: 读取失败 %s: %s", table, part, exc)
            failed += 1
            continue
        if df.is_empty():
            continue
        rows += df.height
        if not dry_run:
            try:
                store.write_daily(table, df)
            except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("%s: 写入失败 %s: %s", table, part, exc)
                failed += 1
        if parts % 50 == 0:
            logger.info("%s: 已处理 %d 个分区 / %d 行", table, parts, rows)
    return parts, rows, failed


def _backfill_adj(
    store: ClickHouseStore, data_dir: Path, table: str, *, dry_run: bool
) -> tuple[int, int]:
    path = data_dir / table / "all.parquet"
    if not path.exists():
        logger.info("%s: 无 all.parquet, 跳过", table)
        return 0, 0
    df = pl.read_parquet(path)
    if df.is_empty():
        return 0, 0
    if not dry_run:
        store.write_adj_factor(df, etf=(table == "adj_factor_etf"))
    return 1, df.height


def _backfill_instruments(
    store: ClickHouseStore, data_dir: Path, table: str, *, dry_run: bool
) -> tuple[int, int]:
    """返回 (文件数, 写入行数)。维表是整表快照，不按日分区。"""
    path = data_dir / table / f"{table}.parquet"
    if not path.exists():
        logger.info("%s: 无 %s, 跳过", table, path.name)
        return 0, 0
    df = pl.read_parquet(path)
    if df.is_empty():
        return 0, 0
    if not dry_run:
        store.write_instruments(INSTRUMENT_TABLES[table], df)
    return 1, df.height


def _backfill_ext(store: ClickHouseStore, data_dir: Path, *, dry_run: bool) -> tuple[int, int, int]:  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
    """回填扩展数据到 ext_timeseries（跳过市场级/无 symbol 的扩展）。"""
    from app.services.ext_data import ExtConfigStore, _mirror_ext_to_ck

    configs = ExtConfigStore(data_dir).load_all()
    if not configs:
        logger.info("ext_timeseries: 无扩展配置, 跳过")
        return 0, 0, 0
    parts = 0
    rows = 0
    failed = 0
    for config in configs:
        cfg_dir = data_dir / "ext_data" / config.id
        files: list[tuple[Path, date | None]] = []
        if config.mode == "snapshot":
            path = cfg_dir / "part.parquet"
            if path.exists():
                files.append((path, None))
        else:
            ts_dir = cfg_dir / "timeseries"
            if ts_dir.exists():
                for part_dir in sorted(ts_dir.glob("date=*")):
                    path = part_dir / "part.parquet"
                    if path.exists():
                        try:
                            files.append((path, date.fromisoformat(part_dir.name[5:])))
                        except ValueError:
                            logger.warning("扩展 %s 分区名非法: %s", config.id, part_dir.name)
        for path, day in files:
            parts += 1
            try:
                df = pl.read_parquet(path)
            except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("扩展 %s 读取失败 %s: %s", config.id, path, exc)
                failed += 1
                continue
            if df.is_empty():
                continue
            rows += df.height
            if dry_run or config.market_level:
                continue
            real_day = day or date.fromtimestamp(path.stat().st_mtime)
            try:
                _mirror_ext_to_ck(config, df, real_day, store=store)
            except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("扩展 %s 写入失败 %s: %s", config.id, path, exc)
                failed += 1
    return parts, rows, failed


def _backfill_depth5(
    store: ClickHouseStore, data_dir: Path, *, dry_run: bool
) -> tuple[int, int, int]:
    """返回 (分区数, 写入行数, 失败分区数)。Parquet 无 date 列，由分区名补。"""
    root = data_dir / "depth5"
    if not root.exists():
        logger.info("depth5: 目录不存在, 跳过")
        return 0, 0, 0
    parts = 0
    rows = 0
    failed = 0
    for part in sorted(root.glob("date=*/part.parquet")):
        parts += 1
        try:
            day = date.fromisoformat(part.parent.name.split("=", 1)[1])
            df = pl.read_parquet(part)
        except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("depth5: 读取失败 %s: %s", part, exc)
            failed += 1
            continue
        if df.is_empty():
            continue
        rows += df.height
        if dry_run:
            continue
        if "date" not in df.columns:
            df = df.with_columns(pl.lit(day).alias("date"))
        try:
            store.write_depth5(df)
        except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("depth5: 写入失败 %s: %s", part, exc)
            failed += 1
    return parts, rows, failed


def _backfill_financial(
    store: ClickHouseStore, data_dir: Path, table: str, *, dry_run: bool
) -> tuple[int, int]:
    """返回 (文件数, 写入行数)。财务是整表快照，主键 (symbol, period_end)。"""
    kind = FINANCIAL_TABLES[table]
    path = data_dir / "financials" / kind / "part.parquet"
    if not path.exists():
        logger.info("%s: 无 %s, 跳过", table, path)
        return 0, 0
    df = pl.read_parquet(path)
    if df.is_empty():
        return 0, 0
    if not {"symbol", "period_end"} <= set(df.columns):
        logger.warning("%s: 缺 symbol/period_end 主键, 跳过", table)
        return 0, 0
    if not dry_run:
        store.write_financials(table, df)
    return 1, df.height


def _backfill_minute(
    store: ClickHouseStore, data_dir: Path, table: str, *, dry_run: bool
) -> tuple[int, int, int]:
    """返回 (分区数, 写入行数, 失败分区数)。单分区按 symbol 分块写入，控制单请求体积。"""
    root = data_dir / table
    if not root.exists():
        logger.info("%s: 目录不存在, 跳过", table)
        return 0, 0, 0
    parts = 0
    rows = 0
    failed = 0
    for part in _iter_daily_parts(root):
        parts += 1
        try:
            df = pl.read_parquet(part)
        except Exception as exc:  # noqa: BLE001 - 单个坏分区不阻断整体回填
            logger.warning("%s: 读取失败 %s: %s", table, part, exc)
            failed += 1
            continue
        if df.is_empty():
            continue
        rows += df.height
        if dry_run:
            continue
        symbols = df["symbol"].unique().to_list()
        for i in range(0, len(symbols), MINUTE_SYMBOL_BLOCK):
            block = symbols[i : i + MINUTE_SYMBOL_BLOCK]
            try:
                store.write_minute(table, df.filter(pl.col("symbol").is_in(block)))
            except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("%s: 写入失败 %s (symbol 块 %d): %s", table, part, i, exc)
                failed += 1
        if parts % 20 == 0:
            logger.info("%s: 已处理 %d 个分区 / %d 行", table, parts, rows)
    return parts, rows, failed


def _parse_args(argv: list[str] | None):
    parser = argparse.ArgumentParser(
        description="回填 Parquet 日K/分钟K/除权/标的维表到 ClickHouse"
    )
    parser.add_argument("--data-dir", default=str(settings.data_dir))
    parser.add_argument(
        "--tables",
        nargs="*",
        default=(
            list(DAILY_TABLES)
            + list(ADJ_TABLES)
            + list(INSTRUMENT_TABLES)
            + list(MINUTE_TABLES)
            + list(FINANCIAL_TABLES)
            + ["depth5", "ext_timeseries"]
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args(argv)

    known = (
        DAILY_TABLES
        + ADJ_TABLES
        + tuple(INSTRUMENT_TABLES)
        + MINUTE_TABLES
        + tuple(FINANCIAL_TABLES)
        + ("depth5", "ext_timeseries")
    )
    unknown = [t for t in args.tables if t not in known]
    if unknown:
        logger.error("未知数据集: %s", ", ".join(unknown))
        return 1

    client = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    try:
        client.connect()
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.error("ClickHouse 连不上(%s): %s", settings.clickhouse_url, exc)  # noqa: TRY400 - CLI 连接失败不打印栈
        return 2
    store = ClickHouseStore(client)
    data_dir = Path(args.data_dir)
    logger.info(
        "数据目录: %s → CK %s/%s", data_dir, settings.clickhouse_url, settings.clickhouse_database
    )

    failed_total = 0
    for table in args.tables:
        if table in ADJ_TABLES:
            parts, rows = _backfill_adj(store, data_dir, table, dry_run=args.dry_run)
            logger.info("%s: %d 文件 / %d 行", table, parts, rows)
        elif table in INSTRUMENT_TABLES:
            parts, rows = _backfill_instruments(store, data_dir, table, dry_run=args.dry_run)
            logger.info("%s: %d 文件 / %d 行", table, parts, rows)
        elif table in MINUTE_TABLES:
            parts, rows, failed = _backfill_minute(store, data_dir, table, dry_run=args.dry_run)
            failed_total += failed
            logger.info("%s: %d 分区 / %d 行 / 失败 %d", table, parts, rows, failed)
        elif table in FINANCIAL_TABLES:
            parts, rows = _backfill_financial(store, data_dir, table, dry_run=args.dry_run)
            logger.info("%s: %d 文件 / %d 行", table, parts, rows)
        elif table == "depth5":
            parts, rows, failed = _backfill_depth5(store, data_dir, dry_run=args.dry_run)
            failed_total += failed
            logger.info("depth5: %d 分区 / %d 行 / 失败 %d", parts, rows, failed)
        elif table == "ext_timeseries":
            parts, rows, failed = _backfill_ext(store, data_dir, dry_run=args.dry_run)
            failed_total += failed
            logger.info("ext_timeseries: %d 分区 / %d 行 / 失败 %d", parts, rows, failed)
        else:
            parts, rows, failed = _backfill_daily(store, data_dir, table, dry_run=args.dry_run)
            failed_total += failed
            logger.info("%s: %d 分区 / %d 行 / 失败 %d", table, parts, rows, failed)

    if failed_total:
        logger.error("有 %d 个分区失败, 可重跑本脚本补齐", failed_total)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
