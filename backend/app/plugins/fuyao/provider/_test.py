"""FuyaoProvider 设置页试拉 mixin。"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

from app.data_providers.base import ProviderRequestError
from app.plugins.fuyao.client import FuyaoError

from ._common import _map_snapshot_row


class FuyaoProviderTest:
    """设置页 test_dataset 预览, 复用各数据集的既有取数路径。"""

    # 下列成员由 FuyaoProvider 组装类的其它 mixin 提供（分包后 mypy 不可见）。
    name: Any  # provider 名称, Base 类属性
    _get_client: Any  # 客户端工厂
    get_daily: Any  # 日K取数, Daily mixin
    get_adj_factors: Any  # 复权因子, Adj mixin
    get_financials: Any  # 财务取数, Financial mixin

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        if dataset in ("daily", "adj_factor"):
            syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
            try:
                if dataset == "daily":
                    df = self.get_daily(syms, datetime.now() - timedelta(days=30), datetime.now())
                else:
                    df = self.get_adj_factors(
                        syms, datetime.now() - timedelta(days=365), datetime.now()
                    )
            except (FuyaoError, ProviderRequestError) as e:
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            head = df.head(5).to_dicts()
            for row in head:  # date/datetime → ISO 字符串, 保证 JSON 可序列化
                for k, v in list(row.items()):
                    if isinstance(v, date | datetime):
                        row[k] = v.isoformat()
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": df.height,
                "columns": df.columns,
                "preview": head,
            }
        if dataset == "financial":
            syms = [s for s in (symbols or [])][:1] or ["600519.SH"]
            try:
                df = self.get_financials("metrics", syms, latest_only=True)
            except FuyaoError as e:
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            head = df.head(5).to_dicts()
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": df.height,
                "columns": df.columns,
                "preview": head,
            }
        if dataset != "realtime":
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "error": f"扶摇插件未接入 {dataset} 数据集(自动回退 TickFlow)",
            }
        try:
            rows, count = self._get_client().snapshot_page(limit=5)
        except FuyaoError as e:
            return {"provider": self.name, "dataset": "realtime", "rows": 0, "error": str(e)}
        fetched_ms = int(time.time() * 1000)
        head = [r for r in (_map_snapshot_row(row, fetched_ms) for row in rows) if r][:5]
        return {
            "provider": self.name,
            "dataset": "realtime",
            "rows": count or len(head),
            "columns": list(head[0].keys()) if head else [],
            "preview": head,
        }
