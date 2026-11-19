"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import contextlib
import datetime as _dt
import logging
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from app.market_time import cn_today
from app.strategy import config as _strategy_config
from app.strategy.custom_signals import _OP_BUILDERS
from app.strategy.custom_signals import signal_names as _custom_signal_names
from app.strategy.intraday_signals import INTRADAY_SIGNAL_LABELS, uses_intraday_signals
from app.strategy.monitor_rules import date_rule_in_window

"""策略实时监控 — 订阅行情更新，检查策略买卖信号。

职责: 接收实时行情 DataFrame → 检查监控中策略的信号 → 推送告警。
不知道: 策略加载逻辑、AI、API、配置持久化、回测。
依赖: 外部调用 on_quote_update() 传入实时数据。

本模块含两个评估器:
  1. StrategyMonitorService — 旧的策略监控 (type=strategy),第二步迁移到 MonitorRuleEngine
  2. MonitorRuleEngine — 通用规则引擎,覆盖 signal/price/market/strategy 四类,
     支持 scope (symbols/all/sector) + 多条件 AND/OR + cooldown 去重
"""


logger = logging.getLogger("app.strategy.monitor")


_SIGNAL_CN: dict[str, str] = {
    **INTRADAY_SIGNAL_LABELS,
    # 涨跌停判定价 (运行时列)
    "limit_up_price": "涨停判定价",
    "limit_down_price": "跌停判定价",
    # 行情字段
    "close": "收盘价",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "change_pct": "涨跌幅",
    "change_amount": "涨跌额",
    "amplitude": "振幅",
    "turnover_rate": "换手率",
    "volume": "成交量",
    "amount": "成交额",
    "_volume_delta": "轮询成交量差值(手)",
    "_sealed_vol": "封单量(手)",
    # 均线
    "ma5": "MA5",
    "ma10": "MA10",
    "ma20": "MA20",
    "ma30": "MA30",
    "ma60": "MA60",
    "ema5": "EMA5",
    "ema10": "EMA10",
    "ema20": "EMA20",
    # MACD / BOLL / KDJ / RSI
    "macd_dif": "MACD-DIF",
    "macd_dea": "MACD-DEA",
    "macd_hist": "MACD柱",
    "boll_upper": "布林上轨",
    "boll_lower": "布林下轨",
    "kdj_k": "KDJ-K",
    "kdj_d": "KDJ-D",
    "kdj_j": "KDJ-J",
    "rsi_6": "RSI6",
    "rsi_14": "RSI14",
    "rsi_24": "RSI24",
    # 量能 / 动量 / 波动
    "vol_ratio_5d": "5日量比",
    "vol_ratio_20d": "20日量比",
    "vol_ma5": "5日均量",
    "vol_ma10": "10日均量",
    "high_60d": "60日最高",
    "low_60d": "60日最低",
    "momentum_5d": "5日动量",
    "momentum_20d": "20日动量",
    "momentum_60d": "60日动量",
    "atr_14": "ATR14",
    "annual_vol_20d": "20日年化波动",
    "consecutive_limit_ups": "连板数",
    "consecutive_limit_downs": "跌停连板",
}


def _signal_cn_name(name: str) -> str:
    """返回信号/字段的中文名, 找不到原样返回 (与前端 cnSignal 对齐)。"""
    return _SIGNAL_CN.get(name, name)


def format_alert_quote(price, change_pct) -> str:
    """告警正文尾部: '现价 1650.0 · +10.0%'。price/pct 均可缺; pct 为小数制。"""
    parts = []
    if price is not None:
        parts.append(f"现价 {price}")
    if change_pct is not None:
        sign = "+" if change_pct >= 0 else ""
        parts.append(f"{sign}{change_pct * 100:.1f}%")
    return " · ".join(parts)


@dataclass
class StrategyAlert:
    """策略告警"""

    type: str  # "entry" | "exit"
    strategy_id: str
    symbol: str
    name: str | None
    message: str
    price: float | None = None
    change_pct: float | None = None
    signals: list[str] = field(default_factory=list)


class StrategyMonitorService:
    """策略实时监控服务"""

    def __init__(self, alert_handler: Callable[[StrategyAlert], None] | None = None):
        """
        Args:
            alert_handler: 告警回调 (如推 SSE)
        """
        self._alert_handler = alert_handler
        # strategy_id → 监控配置
        self._watching: dict[str, dict] = {}
        # _watching 跨线程锁: on_quote_update 跑在行情轮询线程迭代 _watching,
        # API 线程同时 start/stop 增删会抛 "dict changed size during iteration"。
        # 增删与迭代前的快照都持此锁 (镜像 MonitorRuleEngine.evaluate 的 list 快照)。
        self._watching_lock = threading.Lock()

    def start(self, strategy_id: str, config: dict) -> None:
        """开始监控一个策略

        config: {
            "entry_signals": ["signal_n_day_high", ...],
            "exit_signals": ["signal_ma20_breakdown", ...],
        }
        """
        with self._watching_lock:
            self._watching[strategy_id] = config
        logger.info("strategy monitor started: %s", strategy_id)

    def stop(self, strategy_id: str) -> None:
        with self._watching_lock:
            self._watching.pop(strategy_id, None)
        logger.info("strategy monitor stopped: %s", strategy_id)

    def stop_all(self) -> None:
        with self._watching_lock:
            self._watching.clear()

    @property
    def watching(self) -> dict[str, dict]:
        with self._watching_lock:
            return dict(self._watching)

    def on_quote_update(self, df: pl.DataFrame) -> list[StrategyAlert]:
        """行情更新后调用。向量化检查所有监控策略。

        Args:
            df: 实时 enriched 数据 (~5500行)
        Returns:
            触发的告警列表
        """
        if not self._watching or df.is_empty():
            return []

        all_alerts: list[StrategyAlert] = []

        # 迭代前持锁快照, 避免行情线程迭代时 API 线程 start/stop 改变字典大小
        with self._watching_lock:
            watching_items = list(self._watching.items())

        for strategy_id, cfg in watching_items:
            # 买入信号
            entry_sigs = cfg.get("entry_signals", [])
            if entry_sigs:
                for sym, name, price, pct, hit_sigs in self._check_signals(df, entry_sigs):
                    alert = StrategyAlert(
                        type="entry",
                        strategy_id=strategy_id,
                        symbol=sym,
                        name=name,
                        message="入场信号触发",
                        price=price,
                        change_pct=pct,
                        signals=hit_sigs,
                    )
                    all_alerts.append(alert)
                    self._emit(alert)

            # 卖出信号
            exit_sigs = cfg.get("exit_signals", [])
            if exit_sigs:
                for sym, name, price, pct, hit_sigs in self._check_signals(df, exit_sigs):
                    alert = StrategyAlert(
                        type="exit",
                        strategy_id=strategy_id,
                        symbol=sym,
                        name=name,
                        message="出场信号触发",
                        price=price,
                        change_pct=pct,
                        signals=hit_sigs,
                    )
                    all_alerts.append(alert)
                    self._emit(alert)

        return all_alerts

    def _emit(self, alert: StrategyAlert) -> None:
        if self._alert_handler:
            try:
                self._alert_handler(alert)
            except Exception as e:  # noqa: BLE001 - 告警回调由外部注入, 失败隔离以免影响监控主循环
                logger.warning("alert handler failed: %s", e)

    @staticmethod
    def _check_signals(
        df: pl.DataFrame,
        signals: list[str],
    ) -> list[tuple[str, str | None, float | None, float | None, list[str]]]:
        """检查信号列，返回 [(symbol, name, price, change_pct, [hit_signals])]。
        支持内置 signal_ 与自定义 csg_ 前缀。"""
        cols = set(df.columns)
        resolved: list[tuple[str, str]] = []  # (原值, 列名)
        for s in signals:
            col = s if (s.startswith("signal_") or s.startswith("csg_")) else f"signal_{s}"
            if col in cols:
                resolved.append((s, col))
        if not resolved:
            return []

        mask = pl.any_horizontal(pl.col(c).fill_null(False) for _, c in resolved)
        hit_df = df.filter(mask)

        results = []
        for row in hit_df.iter_rows(named=True):
            sym = row.get("symbol", "")
            name = row.get("name")
            price = row.get("close")
            pct = row.get("change_pct")
            hit_sigs = [orig for orig, col in resolved if row.get(col)]
            results.append((sym, name, price, pct, hit_sigs))
        return results


_SIGNAL_PREFIXES = ("signal_", "csg_")


_group_cache_lock = threading.Lock()


_group_cache: dict[str, Any] = {}


_warned_missing_groups: set[tuple[str, str]] = set()


def _watchlist_groups_snapshot() -> dict[str, frozenset[str]]:
    """返回 {group_id: 成员symbol集}。读前后版本一致才写缓存, 避免缓存住写竞态下的旧数据。"""
    from app.services import watchlist

    rev_before = watchlist.revision()
    with _group_cache_lock:
        cached = _group_cache.get("groups")
        if cached is not None and _group_cache.get("_rev") == rev_before:
            return cached
    groups: dict[str, set[str]] = {g["id"]: set() for g in watchlist.list_groups()}
    for row in watchlist.list_symbols():
        for gid in row.get("group_ids") or []:
            members = groups.get(gid)
            if members is not None:
                members.add(str(row["symbol"]))
    frozen = {gid: frozenset(syms) for gid, syms in groups.items()}
    if watchlist.revision() == rev_before:
        with _group_cache_lock:
            _group_cache["_rev"] = rev_before
            _group_cache["groups"] = frozen
    return frozen


def _group_members_or_none(rule: dict) -> frozenset[str] | None:
    """解析规则绑定的分组成员; 分组已删除返回 None, 解析异常返回 None 并记日志。"""
    group_id = str(rule.get("group_id") or "")
    try:
        groups = _watchlist_groups_snapshot()
    except Exception as exc:  # noqa: BLE001
        logger.warning("自选分组数据读取失败, 规则 %s 本轮跳过: %s", rule.get("id"), exc)
        return None
    members = groups.get(group_id)
    if members is None:
        key = (str(rule.get("id") or ""), group_id)
        if key not in _warned_missing_groups:
            _warned_missing_groups.add(key)
            logger.warning(
                "监控规则 %s 绑定的自选分组 %s 已删除, 本轮跳过 (fail-closed, 恢复分组后自动生效)",
                rule.get("id"),
                group_id,
            )
    return members


def _is_signal_field(field: str) -> bool:
    return any(field.startswith(p) for p in _SIGNAL_PREFIXES)


def _build_condition_mask(df: pl.DataFrame, conditions: list[dict], logic: str) -> pl.DataFrame:
    """根据 conditions + logic 构建过滤后的命中 DataFrame。

    conditions: [{"field","op","value"?}] — op=truth 为布尔信号, 否则阈值比较
    logic: "and" | "or"
    返回命中行 (含 symbol/name/close/change_pct + 各信号列)
    """
    cols = set(df.columns)
    parts: list[pl.Expr] = []
    for c in conditions:
        field = c["field"]
        if field not in cols:
            return df.head(0)  # 字段缺失,无法判定 → 空结果
        op = c["op"]
        if op == "truth":
            parts.append(pl.col(field).fill_null(False))
        elif op in _OP_BUILDERS:
            parts.append(_OP_BUILDERS[op](pl.col(field), c["value"]))
        else:
            return df.head(0)
    if not parts:
        return df.head(0)
    mask = pl.any_horizontal(parts) if logic == "or" else pl.all_horizontal(parts)
    return df.filter(mask)


__all__ = [
    "INTRADAY_SIGNAL_LABELS",
    "_OP_BUILDERS",
    "_SIGNAL_CN",
    "_SIGNAL_PREFIXES",
    "Any",
    "Callable",
    "StrategyAlert",
    "StrategyMonitorService",
    "_build_condition_mask",
    "_custom_signal_names",
    "_dt",
    "_group_cache",
    "_group_cache_lock",
    "_group_members_or_none",
    "_is_signal_field",
    "_signal_cn_name",
    "_strategy_config",
    "_warned_missing_groups",
    "_watchlist_groups_snapshot",
    "cn_today",
    "contextlib",
    "dataclass",
    "date_rule_in_window",
    "field",
    "format_alert_quote",
    "logger",
    "logging",
    "math",
    "pl",
    "threading",
    "time",
    "uses_intraday_signals",
]
