"""S8 并发与一致性（验收文档 §7）。

多线程并发打真实 CK 读路径（kline/daily + table_stats），断言无 500、无异常。
这是唯一能暴露"同一 CK client 并发复用 session"问题的场景（clickhouse_connect
在同一 session 内并发查询会抛 ProgrammingError）。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from .conftest import DAILY_ROWS, write_daily_frames


def test_s8_concurrent_ck_reads(scenario_client):
    client, repo, _ = scenario_client
    from datetime import date

    # 造数：3 天 × 2 标的，经 repository 双写 CK
    rows = []
    for i in range(3):
        for j, sym in enumerate(["000001.SZ", "600000.SH"]):
            rows.append(DAILY_ROWS(sym, date(2026, 11, 14 + i), 10.0 + i + j))
    write_daily_frames(repo, rows)
    repo.refresh_cache()

    # 8 线程并发：4 读 kline/daily + 4 读 data/status（后者触发 CK 聚合）
    errors: list[str] = []
    counts: list[int] = []

    def read_daily(n: int) -> str:
        try:
            r = client.get(
                "/api/kline/daily",
                params={"symbol": "000001.SZ", "days": 10, "end_date": "2026-11-16"},
            )
            if r.status_code != 200:
                errors.append(f"daily-{n}: {r.status_code} {r.text[:120]}")
            else:
                counts.append(len(r.json()["rows"]))
            return "ok"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"daily-{n}: {type(exc).__name__}: {exc}")
            return "err"

    def read_stats(n: int) -> str:
        try:
            r = client.get("/api/data/status")
            if r.status_code != 200:
                errors.append(f"status-{n}: {r.status_code}")
            return "ok"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"status-{n}: {type(exc).__name__}: {exc}")
            return "err"

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(read_daily, i) for i in range(4)]
        futures += [pool.submit(read_stats, i) for i in range(4)]
        for f in as_completed(futures):
            f.result()

    # 无 500 / 无并发异常；已返回的行数稳定一致（无脏读/半行）
    assert not errors, "并发读出现错误: " + "; ".join(errors[:5])
    assert counts and all(c == counts[0] for c in counts), f"行数不一致: {counts}"


def test_s8_concurrent_store_queries(scenario_client):
    """直接并发打同一 CK store（绕过 HTTP/anyio 串行化），验证 client 线程安全。

    clickhouse_connect 同 session 并发查询会抛 ProgrammingError；若 CM 未串行化，
    这里会稳定复现。这是读路径并发一致性的底座。
    """
    _, _, store = scenario_client
    errors: list[str] = []

    def query(n: int) -> int:
        try:
            return store.table_stats("kline_daily").get("rows", 0)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"q-{n}: {type(exc).__name__}: {exc}")
            return -1

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(query, range(16)))

    assert not errors, "并发查询 CK 出错: " + "; ".join(errors[:5])
    assert all(r == results[0] for r in results), f"并发读结果不一致: {results}"
