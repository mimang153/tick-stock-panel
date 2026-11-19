"""策略回测服务组装：以 mixin 组合保持原 StrategyBacktestService 同一类语义。"""

from __future__ import annotations

from ._service_prepare import StrategyBacktestPrepareMixin
from ._service_run import StrategyBacktestRunMixin
from ._service_run_tail import StrategyBacktestRunTailMixin
from ._service_sim import StrategyBacktestSimMixin
from ._service_support import StrategyBacktestSupportMixin


class StrategyBacktestService(
    StrategyBacktestPrepareMixin,
    StrategyBacktestRunMixin,
    StrategyBacktestRunTailMixin,
    StrategyBacktestSimMixin,
    StrategyBacktestSupportMixin,
):
    """全周期策略回测服务（行为与原单类实现一致）。"""
