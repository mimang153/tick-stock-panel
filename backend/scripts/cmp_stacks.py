"""双栈对照工具（WBS T2.9 / 验收文档 §16）。

把 Parquet（旧栈、本地 data 目录）与 ClickHouse（新栈）逐数据集比对，产出 JSON 报告。
存在的意义：硬切 + 不迁数据的方案里，只有"逐值对齐"能证明切换没改语义。

用法：

    uv run python scripts/cmp_stacks.py --dataset kline_daily --start 2024-01-02 --end 2024-01-31
    uv run python scripts/cmp_stacks.py --dataset instruments --out reports/cmp_b2.json

退出码：0 = 无可解释差异；1 = 存在差异；2 = CK 连不上。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings
from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore

logger = logging.getLogger("cmp_stacks")

DEFAULT_RTOL = 1e-9
DEFAULT_ATOL = 1e-9
MAX_SAMPLES = 3


@dataclass(frozen=True)
class DatasetSpec:
    """一个待对照的数据集：Parquet 布局 + CK 读法。"""

    name: str
    keys: tuple[str, ...]
    columns: tuple[str, ...]
    date_column: str
    parquet: str  # 分区目录 / 单文件名
    read: Callable[..., pl.DataFrame] = field(compare=False)


def _daily(table: str) -> Callable[..., pl.DataFrame]:
    return lambda store, symbols, start, end: store.read_daily(
        table,
        symbols=symbols,
        start=start,
        end=end,
    )


def _adj(etf: bool) -> Callable[..., pl.DataFrame]:
    return lambda store, symbols, start, end: store.read_adj_factor(
        symbols=symbols,
        start=start,
        end=end,
        etf=etf,
    )


def _instruments(asset_type: str) -> Callable[..., pl.DataFrame]:
    return lambda store, symbols, start, end: store.read_instruments(asset_type)


_DAILY_COLUMNS = ("symbol", "date", "open", "high", "low", "close", "volume", "amount")
_ENRICHED_COLUMNS = (
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "raw_close",
    "raw_high",
    "raw_low",
    "turnover_rate",
    "consecutive_limit_ups",
    "consecutive_limit_downs",
    "quote_ts",
)
_NARROW_ENRICHED_COLUMNS = (
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "raw_close",
    "raw_high",
    "raw_low",
)
_INSTRUMENT_COLUMNS = (
    "symbol",
    "name",
    "code",
    "exchange",
    "region",
    "type",
    "listing_date",
    "total_shares",
    "float_shares",
    "tick_size",
    "limit_up",
    "limit_down",
    "as_of",
)
_ADJ_COLUMNS = (
    "symbol",
    "trade_date",
    "ex_factor",
    "dividend",
    "bonus",
    "allot",
    "allot_price",
    "prev_close",
)

DATASETS: dict[str, DatasetSpec] = {
    spec.name: spec
    for spec in (
        DatasetSpec(
            "kline_daily",
            ("symbol", "date"),
            _DAILY_COLUMNS,
            "date",
            "date=*",
            _daily("kline_daily"),
        ),
        DatasetSpec(
            "kline_index_daily",
            ("symbol", "date"),
            _DAILY_COLUMNS,
            "date",
            "date=*",
            _daily("kline_index_daily"),
        ),
        DatasetSpec(
            "kline_etf_daily",
            ("symbol", "date"),
            _DAILY_COLUMNS,
            "date",
            "date=*",
            _daily("kline_etf_daily"),
        ),
        DatasetSpec(
            "kline_daily_enriched",
            ("symbol", "date"),
            _ENRICHED_COLUMNS,
            "date",
            "date=*",
            _daily("kline_daily_enriched"),
        ),
        DatasetSpec(
            "kline_index_enriched",
            ("symbol", "date"),
            _NARROW_ENRICHED_COLUMNS,
            "date",
            "date=*",
            _daily("kline_index_enriched"),
        ),
        DatasetSpec(
            "kline_etf_enriched",
            ("symbol", "date"),
            _NARROW_ENRICHED_COLUMNS,
            "date",
            "date=*",
            _daily("kline_etf_enriched"),
        ),
        DatasetSpec(
            "instruments",
            ("symbol",),
            _INSTRUMENT_COLUMNS,
            "as_of",
            "instruments.parquet",
            _instruments("stock"),
        ),
        DatasetSpec(
            "instruments_index",
            ("symbol",),
            _INSTRUMENT_COLUMNS,
            "as_of",
            "instruments_index.parquet",
            _instruments("index"),
        ),
        DatasetSpec(
            "instruments_etf",
            ("symbol",),
            (*_INSTRUMENT_COLUMNS, "asset_type"),
            "as_of",
            "instruments_etf.parquet",
            _instruments("etf"),
        ),
        DatasetSpec(
            "adj_factor",
            ("symbol", "trade_date"),
            _ADJ_COLUMNS,
            "trade_date",
            "all.parquet",
            _adj(etf=False),
        ),
        DatasetSpec(
            "adj_factor_etf",
            ("symbol", "trade_date"),
            _ADJ_COLUMNS,
            "trade_date",
            "all.parquet",
            _adj(etf=True),
        ),
    )
}


def load_parquet(  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
    name: str,
    data_dir: Path,
    *,
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame | None:
    """读旧栈 Parquet；数据缺失返回 None（报告里标 ``no_parquet``，不算差异）。"""
    spec = DATASETS[name]
    root = data_dir / name
    if not root.exists():
        return None

    if spec.parquet == "date=*":
        # 只读区间内的分区，避免为了对一天而扫 241 个分区
        files: list[Path] = []
        for part in sorted(root.glob("date=*")):
            try:
                day = date.fromisoformat(part.name.removeprefix("date="))
            except ValueError:
                continue
            if start is not None and day < start:
                continue
            if end is not None and day > end:
                continue
            files.extend(sorted(part.glob("*.parquet")))
        if not files:
            return None
        df = pl.read_parquet(files)
    else:
        file = root / spec.parquet
        if not file.exists():
            return None
        df = pl.read_parquet(file)

    if symbols is not None:
        df = df.filter(pl.col("symbol").is_in(list(symbols)))
    column = spec.date_column
    if column in df.columns:
        if start is not None:
            df = df.filter(pl.col(column) >= start)
        if end is not None:
            df = df.filter(pl.col(column) <= end)
    return df


def _is_numeric(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def compare_frames(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    keys: Sequence[str],
    value_columns: Sequence[str],
    rtol: float = DEFAULT_RTOL,
    atol: float = DEFAULT_ATOL,
) -> dict:
    """按主键对齐两边，统计缺行/多行/逐列差异。"""
    required = [*keys, *value_columns]
    for side, frame in (("parquet", left), ("clickhouse", right)):
        missing = [c for c in required if c not in frame.columns]
        if missing:
            raise ValueError(f"{side} 缺少列 {missing}")

    left_frame = left.select(required).with_columns(pl.lit(True).alias("_in_left"))
    right_frame = right.select(required).with_columns(pl.lit(True).alias("_in_right"))
    merged = left_frame.join(
        right_frame,
        on=list(keys),
        how="full",
        coalesce=True,
        suffix="_right",
    )

    only_left = merged.filter(pl.col("_in_right").is_null())
    only_right = merged.filter(pl.col("_in_left").is_null())
    both = merged.filter(
        pl.col("_in_left").is_not_null() & pl.col("_in_right").is_not_null(),
    )

    column_diffs: dict[str, int] = {}
    samples: dict[str, list[dict]] = {}
    for column in value_columns:
        lhs = pl.col(column)
        rhs = pl.col(f"{column}_right")
        both_null = lhs.is_null() & rhs.is_null()
        one_null = lhs.is_null() != rhs.is_null()
        if _is_numeric(left.schema[column]):
            close = ((lhs - rhs).abs() <= atol + rtol * rhs.abs()).fill_null(False)
            mismatch = one_null | (~both_null & ~close)
        else:
            same = (lhs == rhs).fill_null(False)
            mismatch = one_null | (~both_null & ~same)
        bad = both.filter(mismatch)
        if bad.is_empty():
            continue
        column_diffs[column] = bad.height
        samples[column] = (
            bad.select(
                [*keys, column, f"{column}_right"],
            )
            .head(MAX_SAMPLES)
            .to_dicts()
        )

    return {
        "parquet_rows": left.height,
        "clickhouse_rows": right.height,
        "only_in_parquet": only_left.height,
        "only_in_clickhouse": only_right.height,
        "column_diffs": column_diffs,
        "samples": samples,
        "ok": only_left.height == 0 and only_right.height == 0 and not column_diffs,
    }


def compare_dataset(
    name: str,
    store: ClickHouseStore,
    data_dir: Path,
    *,
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    rtol: float = DEFAULT_RTOL,
    atol: float = DEFAULT_ATOL,
) -> dict:
    spec = DATASETS[name]
    parquet = load_parquet(name, data_dir, symbols=symbols, start=start, end=end)
    if parquet is None:
        return {"status": "no_parquet", "ok": True}

    clickhouse = spec.read(store, symbols, start, end)
    if clickhouse.is_empty():
        return {
            "status": "ck_empty",
            "ok": False,
            "parquet_rows": parquet.height,
            "note": "CK 里没有数据，通常是该数据集还没接入写入侧",
        }

    result = compare_frames(
        parquet.select(list(spec.columns)),
        clickhouse.select(list(spec.columns)),
        keys=spec.keys,
        value_columns=[c for c in spec.columns if c not in spec.keys],
        rtol=rtol,
        atol=atol,
    )
    result["status"] = "ok" if result["ok"] else "diff"
    return result


def _parse_args(argv: Sequence[str] | None):
    parser = argparse.ArgumentParser(description="Parquet(旧栈) vs ClickHouse(新栈) 对照")
    parser.add_argument(
        "--dataset", action="append", choices=sorted(DATASETS), help="可重复；缺省对照全部数据集"
    )
    parser.add_argument("--symbols", help="逗号分隔的标的过滤")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--url", default=settings.clickhouse_url)
    parser.add_argument("--database", default=settings.clickhouse_database)
    parser.add_argument("--user", default=settings.clickhouse_user)
    parser.add_argument("--password", default=settings.clickhouse_password)
    parser.add_argument("--out", type=Path, default=Path("reports/cmp_b2.json"))
    parser.add_argument(
        "--ensure-schema", action="store_true", help="对照前先幂等建表（目标库还没有 schema 时用）"
    )
    parser.add_argument("--rtol", type=float, default=DEFAULT_RTOL)
    parser.add_argument("--atol", type=float, default=DEFAULT_ATOL)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else None

    client = ClickHouseClient(
        url=args.url,
        database=args.database,
        user=args.user,
        password=args.password,
    )
    try:
        client.connect(retry_total_s=5)
    except Exception as exc:  # noqa: BLE001 - CLI 需要给出可读的失败原因
        logger.error("连不上 ClickHouse（%s/%s）：%s", args.url, args.database, exc)  # noqa: TRY400 - CLI 连接失败不打印栈
        return 2

    if args.ensure_schema:
        from app.storage.schema import ensure_schema

        ensure_schema(client)

    store = ClickHouseStore(client)
    names = args.dataset or list(DATASETS)
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "url": args.url,
        "database": args.database,
        "data_dir": str(args.data_dir),
        "filters": {
            "symbols": symbols,
            "start": args.start.isoformat() if args.start else None,
            "end": args.end.isoformat() if args.end else None,
        },
        "datasets": {},
    }
    try:
        for name in names:
            result = compare_dataset(
                name,
                store,
                args.data_dir,
                symbols=symbols,
                start=args.start,
                end=args.end,
                rtol=args.rtol,
                atol=args.atol,
            )
            report["datasets"][name] = result
            logger.info(
                "%-22s %-11s parquet=%-6s ck=%-6s 仅旧栈=%-4s 仅新栈=%-4s 差异列=%s",
                name,
                result["status"],
                result.get("parquet_rows", "-"),
                result.get("clickhouse_rows", "-"),
                result.get("only_in_parquet", "-"),
                result.get("only_in_clickhouse", "-"),
                result.get("column_diffs", {}),
            )
    finally:
        client.close()

    report["ok"] = all(item["ok"] for item in report["datasets"].values())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    logger.info("报告已写入 %s（ok=%s）", args.out, report["ok"])
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
