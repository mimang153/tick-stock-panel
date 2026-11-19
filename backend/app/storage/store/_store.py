"""ClickHouseStore 组装：以 mixin 组合保持原 ClickHouseStore 同一类语义。"""

from __future__ import annotations

from ._core import ClickHouseStoreCoreMixin
from ._extended import ClickHouseExtendedDataMixin
from ._generation import ClickHouseGenerationMixin
from ._market import ClickHouseMarketDataMixin
from ._metadata import ClickHouseMetadataMixin
from ._repair import ClickHouseRepairMixin


class ClickHouseStore(
    ClickHouseStoreCoreMixin,
    ClickHouseMarketDataMixin,
    ClickHouseExtendedDataMixin,
    ClickHouseMetadataMixin,
    ClickHouseRepairMixin,
    ClickHouseGenerationMixin,
):
    """``StorageProtocol`` 的 ClickHouse 实现（行为与原单类实现一致）。"""
