"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。

组装类 StrategyEngine 只继承四个 mixin；运行时属性在 mixin1 的 __init__ 初始化，
其它分片通过类体注解声明跨 mixin 成员，保证 mypy 可独立解析每个分片。
"""

from ._common import (
    DEFAULT_BASIC_FILTER,
    MAX_COMPOSITE_CHILDREN,
    SCORING_DIRECTION_LOW,
    Any,
    Callable,
    CompositeChild,
    CompositeSpec,
    Mapping,
    Path,
    StrategyDataContext,
    StrategyDef,
    StrategyResult,
    ThreadPoolExecutor,
    _normalize_param_defs,
    _normalize_param_item,
    _parse_composite_children,
    _RealtimeMatrixEntry,
    _resolve_required_features,
    _sanitize,
    contextlib,
    dataclass,
    date,
    effective_scoring,
    effective_scoring_directions,
    field,
    importlib,
    logger,
    logging,
    materialize_scoring_columns,
    np,
    pl,
    replace,
    scoring_dependencies,
    scoring_value_expr,
    scoring_warmup_bars,
    settings,
    sys,
    threading,
    time,
)
from ._part1 import _StrategyEngineMixin1
from ._part2 import _StrategyEngineMixin2
from ._part3 import _StrategyEngineMixin3
from ._part4 import _StrategyEngineMixin4


class StrategyEngine(
    _StrategyEngineMixin1,
    _StrategyEngineMixin2,
    _StrategyEngineMixin3,
    _StrategyEngineMixin4,
):
    """策略引擎 — 策略加载 + 执行 + 评分"""

    pass


__all__ = [
    "DEFAULT_BASIC_FILTER",
    "MAX_COMPOSITE_CHILDREN",
    "SCORING_DIRECTION_LOW",
    "Any",
    "Callable",
    "CompositeChild",
    "CompositeSpec",
    "Mapping",
    "Path",
    "StrategyDataContext",
    "StrategyDef",
    "StrategyEngine",
    "StrategyResult",
    "ThreadPoolExecutor",
    "_RealtimeMatrixEntry",
    "_normalize_param_defs",
    "_normalize_param_item",
    "_parse_composite_children",
    "_resolve_required_features",
    "_sanitize",
    "contextlib",
    "dataclass",
    "date",
    "effective_scoring",
    "effective_scoring_directions",
    "field",
    "importlib",
    "logger",
    "logging",
    "materialize_scoring_columns",
    "np",
    "pl",
    "replace",
    "scoring_dependencies",
    "scoring_value_expr",
    "scoring_warmup_bars",
    "settings",
    "sys",
    "threading",
    "time",
]
