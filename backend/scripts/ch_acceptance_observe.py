"""T6.5 连续观察采集器（验收文档 §11：连续 3 天无数据异常，跨交易日/跨重启）。

每 ``--interval`` 秒采集一行 JSONL，追加到 ``--out``：
- CK 健康（ping / pending_rows / failed_batches）
- 核心表行数与最新日期
- schema_version
- 本地日K完整性条目数
- 进程 uptime（用于识别"跨重启"：uptime 回落即发生重启）

长期后台运行；不做任何写操作，只读。异常只记录不抛出，保证观察不中断。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

TABLES_DATE = {
    "kline_daily": "date",
    "kline_daily_enriched": "date",
    "kline_index_daily": "date",
    "kline_etf_daily": "date",
    "kline_minute": "datetime",
    "ext_timeseries": "date",
}
BOOT = time.monotonic()


def _safe(fn, default=None):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return {"error": f"{type(exc).__name__}: {exc}"} if default is None else default


def collect() -> dict:
    from app.config import settings
    from app.services.daily_health import snapshot
    from app.storage import runtime

    row = {
        "ts": datetime.now().astimezone().isoformat(),
        "uptime_s": int(time.monotonic() - BOOT),
    }
    rt = runtime.get_storage()
    if rt is None:
        # 独立采集进程：首次采集前自行初始化，之后复用进程内单例。
        rt = _safe(runtime.init_storage)
    if rt is None:
        row["storage"] = "not_initialized"
        return row

    row["health"] = _safe(rt.health)
    client = rt.client

    tables = {}
    for table, date_col in TABLES_DATE.items():
        tables[table] = _safe(
            lambda t=table, d=date_col: client.query(
                f"SELECT count() AS n, max({d}) AS mx FROM {client.table(t)}"
            ).to_dicts()[0]
        )
    row["tables"] = tables

    row["schema_version"] = _safe(
        lambda: client.query(
            f"SELECT max(version) AS v FROM {client.table('schema_version')}"
        ).to_dicts()[0]
    )

    try:
        entries = snapshot(Path(settings.data_dir))["entries"]
        row["integrity_entries"] = len(entries)
        row["integrity_days"] = sorted({e["day"] for e in entries})
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        row["integrity_entries"] = f"error: {type(exc).__name__}: {exc}"

    return row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/app/data/ch_acceptance_observe.jsonl")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--duration", type=int, default=3 * 24 * 3600)
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration
    n = 0
    with out.open("a", encoding="utf-8") as fh:
        while time.monotonic() < deadline:
            row = collect()
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            fh.flush()
            n += 1
            bad = []
            if isinstance(row.get("health"), dict) and row["health"].get("failed_batches"):
                bad.append(f"failed_batches={row['health']['failed_batches']}")
            if isinstance(row.get("integrity_entries"), int) and row["integrity_entries"]:
                bad.append(f"integrity={row['integrity_entries']}")
            print(
                f"[{n}] {row['ts']} uptime={row['uptime_s']}s "
                f"{'WARN ' + ','.join(bad) if bad else 'ok'}",
                flush=True,
            )
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
