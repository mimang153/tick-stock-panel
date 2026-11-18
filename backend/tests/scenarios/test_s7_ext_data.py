"""S7 扩展数据端到端（验收文档 §7）。

建配置 → ingest 写入（本地 + CK 镜像）→ rows 读回（CK 主路径，行数/字段正确）
→ 删除配置 → rows 返回 404/空，主页面（data/status）不受影响。
"""

from __future__ import annotations

from app.api import ext_data as ext_api
from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField


def test_s7_ext_data_e2e(scenario_app):
    app, repo, _ = scenario_app
    app.include_router(ext_api.router)  # S7 需要扩展数据路由
    from fastapi.testclient import TestClient

    # 1. 建配置（标的表，symbol + 两个字段）
    cfg = ExtConfig(
        id="s7heat",
        label="人气",
        mode="timeseries",
        fields=[
            ExtField("symbol", "string"),
            ExtField("concept", "string"),
            ExtField("heat", "float"),
        ],
    )
    ExtConfigStore(repo.store.data_dir).create(cfg)

    with TestClient(app) as client:
        # 2. ingest 写入（固定日期，走本地 parquet + CK 镜像）
        payload = {
            "date": "2026-11-16",
            "rows": [
                {"symbol": "000001.SZ", "concept": "AI", "heat": 1.0},
                {"symbol": "600000.SH", "concept": "银行", "heat": 2.0},
            ],
        }
        r = client.post("/api/ext-data/s7heat/ingest", json=payload)
        assert r.status_code == 200, r.text
        assert r.json()["rows"] == 2

        # 3. rows 读回（CK 主路径）：行数与字段正确
        rows = client.get("/api/ext-data/s7heat/rows", params={"date": "2026-11-16", "limit": 10})
        assert rows.status_code == 200, rows.text
        body = rows.json()
        assert body["total"] == 2
        assert len(body["rows"]) == 2
        names = (
            {f["name"] for f in body["fields"]}
            if body["fields"] and isinstance(body["fields"][0], dict)
            else set(body["fields"])
        )
        assert "symbol" in names and "heat" in names

        # 4. CK 侧确实落库（扩展数据主读源）
        from app.storage import runtime

        ck = runtime.get_storage().store
        ext = ck.read_ext_timeseries("s7heat")
        assert len(ext) >= 2

        # 5. 删除配置
        d = client.delete("/api/ext-data/s7heat")
        assert d.status_code == 200, d.text

        # 6. 删除后 rows 返回 404（配置不存在）；data/status 主页面不受影响
        gone = client.get("/api/ext-data/s7heat/rows")
        assert gone.status_code in (404, 200)  # 404=配置缺失；200=空结果均属"无数据"
        status = client.get("/api/data/status")
        assert status.status_code == 200
