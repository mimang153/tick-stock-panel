"""TdxQuantProvider 设置页试拉 mixin。"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

from app.plugins.tdxquant.client import SNAPSHOT_FIELDS, TdxQuantError

from ._common import _map_snapshot_row

_fp = sys.modules[__package__]


class TdxQuantProviderTest:
    """设置页 test_dataset 预览, 复用各数据集的既有取数路径。"""

    # 由兄弟 mixin / 组装类提供: mixin 拆分后 mypy 不跨兄弟类解析属性
    name: Any
    get_daily: Any
    get_adj_factors: Any
    get_intraday_batch: Any
    get_minute: Any
    _depth_entry: Any
    _get_client: Any
    _auth_cooldown_active: Any
    _auth_failure_reason: Any

    def _test_dataset_frame(self, dataset: str, symbols: list[str] | None) -> dict:
        """日线/分钟类数据集预览: 复用取数 mixin, 统一日期与列字段的 JSON 序列化。"""
        syms = [s for s in (symbols or [])][:3]
        try:
            if dataset in ("daily", "adj_factor"):
                syms = syms or ["000001.SZ"]
                if dataset == "daily":
                    df = self.get_daily(
                        syms, _fp.datetime.now() - timedelta(days=30), _fp.datetime.now()
                    )
                else:
                    df = self.get_adj_factors(
                        syms, _fp.datetime.now() - timedelta(days=365), _fp.datetime.now()
                    )
            else:
                syms = syms or ["600519.SH"]
                end = _fp.datetime.now()
                df = (
                    self.get_intraday_batch(syms)
                    if dataset == "full_minute"
                    else self.get_minute(syms, end - timedelta(days=3), end)
                )
        except TdxQuantError as e:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
        if dataset == "full_minute" and self._auth_cooldown_active():
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "error": self._auth_failure_reason
                or "TdxQuant 凭据或额度不可用，请核对后重新保存 Key",
            }
        head = df.head(5).to_dicts()
        for row in head:
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

    def _test_dataset_snapshot(self, dataset: str, symbols: list[str] | None) -> dict:
        """实时/五档数据集预览: 失败按降级口径返回 0 行与错误原因, 不阻断设置页。"""
        syms = [s for s in (symbols or [])][:5] or ["600519.SH", "000001.SZ"]
        try:
            rows = self._get_client().snapshot_batch(syms)
        except TdxQuantError as e:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
        fallback_ms = int(time.time() * 1000)
        if dataset == "realtime":
            head = [
                r for r in (_map_snapshot_row(c, row, fallback_ms) for c, row in rows.items()) if r
            ]
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": len(rows),
                "columns": list(head[0].keys()) if head else list(SNAPSHOT_FIELDS),
                "preview": head[:5],
            }
        depths = {
            c: d
            for c, d in ((c, self._depth_entry(row, fallback_ms)) for c, row in rows.items())
            if d
        }
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": len(depths),
            "columns": [
                "symbol",
                "bid_prices",
                "bid_volumes",
                "ask_prices",
                "ask_volumes",
                "timestamp",
            ],
            "preview": [{"symbol": c, **d} for c, d in list(depths.items())[:5]],
        }

    def _test_dataset_impl(self, dataset: str, symbols: list[str] | None) -> dict:
        """按数据集类别分流到帧预览或快照预览; 未接入的数据集明确提示回退 TickFlow。"""
        if dataset in ("daily", "adj_factor", "minute", "full_minute"):
            return self._test_dataset_frame(dataset, symbols)
        if dataset in ("realtime", "depth5"):
            return self._test_dataset_snapshot(dataset, symbols)
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": 0,
            "error": f"tdx 插件未接入 {dataset} 数据集(自动回退 TickFlow)",
        }

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页试拉入口: 对外部暴露的统一预览签名。"""
        return self._test_dataset_impl(dataset, symbols)
