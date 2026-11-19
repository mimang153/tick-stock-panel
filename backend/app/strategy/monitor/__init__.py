"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from ._common import (
    _OP_BUILDERS,
    _SIGNAL_CN,
    _SIGNAL_PREFIXES,
    INTRADAY_SIGNAL_LABELS,
    Any,
    Callable,
    StrategyAlert,
    StrategyMonitorService,
    _build_condition_mask,
    _custom_signal_names,
    _dt,
    _group_cache,
    _group_cache_lock,
    _group_members_or_none,
    _is_signal_field,
    _signal_cn_name,
    _strategy_config,
    _warned_missing_groups,
    _watchlist_groups_snapshot,
    cn_today,
    contextlib,
    dataclass,
    date_rule_in_window,
    field,
    format_alert_quote,
    logger,
    logging,
    math,
    pl,
    threading,
    time,
    uses_intraday_signals,
)
from ._part1 import _MonitorRuleEngineMixin1
from ._part2 import _MonitorRuleEngineMixin2
from ._part3 import _MonitorRuleEngineMixin3
from ._part4 import _MonitorRuleEngineMixin4


class MonitorRuleEngine(
    _MonitorRuleEngineMixin1,
    _MonitorRuleEngineMixin2,
    _MonitorRuleEngineMixin3,
    _MonitorRuleEngineMixin4,
):
    """通用监控规则引擎 — 接收实时行情 DataFrame,评估所有规则,返回 AlertEvent。

    与 StrategyMonitorService 的区别:
      - 规则来自 monitor_rules 存储 (用户可配), 而非写死的 strategy config
      - 支持 scope (symbols/all/sector) 过滤作用域
      - 支持 conditions + logic (AND/OR) 任意组合
      - ★ cooldown 去重: 同一 (rule_id, symbol, event_type) 在冷却期内不重复触发
    """

    pass


__all__ = [
    "INTRADAY_SIGNAL_LABELS",
    "_OP_BUILDERS",
    "_SIGNAL_CN",
    "_SIGNAL_PREFIXES",
    "Any",
    "Callable",
    "MonitorRuleEngine",
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
