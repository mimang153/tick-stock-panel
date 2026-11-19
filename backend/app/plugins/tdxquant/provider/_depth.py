"""TdxQuantProvider 五档盘口 mixin。"""

from __future__ import annotations

import math
import time
from typing import Any, cast

from app.plugins.tdxquant.client import TdxQuantError

from ._common import _SNAPSHOT_BATCH, _snapshot_ms, _to_float, logger


class TdxQuantProviderDepth:
    """五档盘口: 数量单位手(透传), 价格转 float。"""

    # 由 TdxQuantProviderBase 提供: mixin 拆分后 mypy 不跨兄弟类解析属性
    _get_client: Any

    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """五档盘口 → {symbol: 标准盘口字典}。数量单位手(透传), 价格转 float。"""
        client = self._get_client()
        out: dict[str, dict] = {}
        fallback_ms = int(time.time() * 1000)
        for i in range(0, len(symbols), _SNAPSHOT_BATCH):
            batch = symbols[i : i + _SNAPSHOT_BATCH]
            try:
                rows = client.snapshot_batch(batch)
            except TdxQuantError as e:
                logger.warning("TdxQuant 五档批次失败(第 %d 批): %s", i // _SNAPSHOT_BATCH + 1, e)
                continue
            for code, row in rows.items():
                entry = self._depth_entry(row, fallback_ms)
                if entry is not None:
                    out[code] = entry
        return out

    def _depth_entry(self, row: dict, fallback_ms: int) -> dict | None:
        def _prices(name: str) -> list[float] | None:
            values = row.get(name)
            if not values:
                return None
            return [cast(float, _to_float(v)) for v in values]

        def _volumes(name: str) -> list[int] | None:
            values = row.get(name)
            if not values:
                return None
            out: list[int] = []
            for v in values:
                f = _to_float(v)
                out.append(math.floor(f) if f is not None else 0)
            return out

        bid_prices = _prices("Buyp")
        ask_prices = _prices("Sellp")
        if not bid_prices or not ask_prices:
            return None
        return {
            "bid_prices": bid_prices,
            "bid_volumes": _volumes("Buyv") or [],
            "ask_prices": ask_prices,
            "ask_volumes": _volumes("Sellv") or [],
            "timestamp": _snapshot_ms(row.get("RefreshTime"), fallback_ms),
        }
