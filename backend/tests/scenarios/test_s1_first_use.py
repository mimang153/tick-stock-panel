"""S1 首次使用全流程（空库起步）— 验收文档 §7。

空库 status → 造数（模拟同步标的+日K）→ status 有数 → instruments 搜索
→ kline/daily 行数=造数 → watchlist/enriched 可用。全程无 500，前置可观测。
"""

from __future__ import annotations

from datetime import date, timedelta

from .conftest import DAILY_ROWS, write_daily_frames


def test_s1_first_use_flow(scenario_client):
    client, repo, store = scenario_client
    symbols = ["000001.SZ", "600000.SH"]

    # 1. 空库起手：直查 CK 确认 enriched 为空（不触发 status 后台刷新线程，
    #    避免与随后造数的 CK 镜像写入并发复用同一 session）
    assert store.table_stats("kline_daily_enriched") == {}

    # 2. 模拟"同步标的 + 日K"（固定 3 个交易日）
    today = date(2026, 11, 16)
    rows = []
    for i in range(3):
        day = today - timedelta(days=i)
        for j, symbol in enumerate(symbols):
            rows.append(DAILY_ROWS(symbol, day, 10.0 + i + j))
    write_daily_frames(repo, rows)
    repo.refresh_cache()
    # 模拟真实同步完成后的缓存失效（data/status 是异步聚合缓存）
    from app.api.data import invalidate_storage_cache

    invalidate_storage_cache()

    # 3. 标的搜索可见（repo.save_instruments 已在 fixture 写入并双写 CK）
    found = client.get("/api/kline/instruments/search", params={"q": "000001"})
    assert found.status_code == 200
    assert any(x["symbol"] == "000001.SZ" for x in found.json()["results"])

    # 4. 日K行数 = 造数（3 天）
    detail = client.get(
        "/api/kline/daily",
        params={"symbol": "000001.SZ", "days": 10, "end_date": today.isoformat()},
    )
    assert detail.status_code == 200
    assert len(detail.json()["rows"]) == 3

    # 5. status 二次刷新后统计可观测（异步缓存，等待收敛）。
    #    注意 daily/enriched 的轻量统计口径：rows 恒为 0，真实可观测的是
    #    日期范围 + 交易日数 + 标的数（见 _safe_aggregate_daily 契约）。
    import time

    for _ in range(30):
        body = client.get("/api/data/status").json()
        if not body.get("stats_pending") and (body.get("daily") or {}).get("trading_days"):
            break
        time.sleep(0.2)
    daily = body["daily"]
    assert daily["trading_days"] == 3
    assert daily["earliest_date"] == "2026-11-14"
    assert daily["latest_date"] == "2026-11-16"
    assert daily["symbols_covered"] >= len(symbols)
    assert body["instruments"]["rows"] >= len(symbols)

    # 6. watchlist/enriched 可用（空自选返回结构完整，无 500）
    wl = client.get("/api/watchlist/enriched")
    assert wl.status_code == 200
    assert "rows" in wl.json() and "dates" in wl.json()

    # 7. enriched 轻量统计同样反映造数后的交易日范围
    enriched = body["enriched"]
    assert enriched["trading_days"] == 3

    # 8. 全程读路径落 CK：enriched 表经 repository 双写已有数据
    assert store.table_stats("kline_daily_enriched")["rows"] >= len(rows)
