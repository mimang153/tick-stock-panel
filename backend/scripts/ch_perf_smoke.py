"""T6.2 性能冒烟标定（验收文档 §9.1）——只读，实测各场景耗时。

输出 JSON 到 --out（默认 data/ch_perf_smoke.json），供回填阈值表。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

from app.config import settings
from app.storage.clickhouse import ClickHouseClient
from app.storage.store import ClickHouseStore


def timed(fn) -> tuple[float, object]:
    t0 = time.perf_counter()
    result = fn()
    return round((time.perf_counter() - t0) * 1000, 1), result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="/app/data/ch_perf_smoke.json")
    args = p.parse_args()

    c = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    c.connect()
    s = ClickHouseStore(c)
    out: dict = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "database": c.database,
        "scenarios": {},
    }

    # 单票日K 10 年（2500 行）：< 300ms
    ms, df = timed(
        lambda: s.read_daily(
            "kline_daily", symbols=["000001.SZ"], start=date(2016, 1, 1), end=date(2026, 11, 16)
        )
    )
    out["scenarios"]["单票日K10年"] = {"ms": ms, "rows": len(df), "target_ms": 300}  # type: ignore[arg-type]

    # 全市场单日横截面（5500×1）：< 5s
    ms, df = timed(
        lambda: s.read_daily("kline_daily", start=date(2026, 11, 13), end=date(2026, 11, 13))
    )
    out["scenarios"]["全市场单日横截面"] = {"ms": ms, "rows": len(df), "target_ms": 5000}  # type: ignore[arg-type]

    # 全市场日K 1 年（回测面板，133 万行）：< 15s，内存 < 2G（内存用 resource 粗测）
    ms, df = timed(
        lambda: s.read_daily("kline_daily", start=date(2025, 11, 1), end=date(2026, 11, 16))
    )
    import resource

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    out["scenarios"]["全市场日K1年"] = {
        "ms": ms,
        "rows": len(df),  # type: ignore[arg-type]
        "target_ms": 15000,
        "peak_rss_mb": round(rss_mb, 1),
    }

    # 单票分钟查 1 个月：< 2s（当前分钟表仅 1103 行，按现有规模测，标注非满量）
    ms, df = timed(
        lambda: s.read_minute(
            "kline_minute",
            symbols=["600000.SH"],
            start=__import__("datetime").datetime(2026, 10, 1),
            end=__import__("datetime").datetime(2026, 11, 10),
        )
    )
    out["scenarios"]["单票分钟1月"] = {
        "ms": ms,
        "rows": len(df),  # type: ignore[arg-type]
        "target_ms": 2000,
        "note": "分钟表现存 ~1103 行，非 5000 万满量",
    }

    # 财务宽表（动态列展开）：< 3s
    try:
        ms, df = timed(
            lambda: s.read_financials("financials_metrics", symbols=["000001.SZ", "600000.SH"])
        )
        out["scenarios"]["财务宽表"] = {"ms": ms, "rows": len(df), "target_ms": 3000}  # type: ignore[arg-type]
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        out["scenarios"]["财务宽表"] = {"error": f"{type(e).__name__}: {e}", "target_ms": 3000}

    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
