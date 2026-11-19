"""Internal split module for app.backtest.factor: service facade."""

from __future__ import annotations

from app.backtest.engine import BacktestEngine

from ._core import FactorCoreMixin
from ._stats import FactorStatsMixin


class FactorBacktestService(FactorCoreMixin, FactorStatsMixin):
    def __init__(self, engine: BacktestEngine) -> None:
        self.engine = engine

    # 盘中实时刷新 (默认 ~6s 一轮) 每轮换新 generation, 快照式读取撞上发布属常态
    # (issue #517): 有界重读新世代整体重跑, 而不是 attempts=1 直接失败。
    _SNAPSHOT_MAX_ATTEMPTS = 3
    _SNAPSHOT_RETRY_SLEEP_S = 1.0
