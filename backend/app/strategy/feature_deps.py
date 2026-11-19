"""filter_history 列依赖的 AST 推导 — REQUIRED_FEATURES 缺失时的自动补全。

背景: 声明了 filter_history (动态 Python 过滤) 但没有声明 REQUIRED_FEATURES 的
策略, 回测无法静态推断需要哪些列, 只能退回"计算全部指标"(backtest/strategy.py
的 full_fallback, 结果正确但白白多算)。本模块对策略文件及其 `_*.py` 依赖文件
做 AST 静态追踪, 收集列引用常量 (含模块级辅助函数), 让绝大多数此类策略拿到
精确依赖, 消灭全量兜底。

fail-closed 契约: 任何无法证明的形态 — 动态列名、pl.all()/polars.selectors、
完全收集不到列引用 — 一律返回 None, 调用方保持原有全量兜底。宁可慢,
绝不漏列 (漏列会静默算错, 是金融数据的 P0 事故形态)。
"""

from __future__ import annotations

import ast
import inspect
import logging
import textwrap

logger = logging.getLogger(__name__)

# 列引用形态: <...>.col("x") / 裸 col("x") / pl.cols("a", "b")。
_COL_FORMS = ("pl.col", "col")
_COLS_FORMS = ("pl.cols", "cols")
# 列名动态函数 (polars 表达式里按正则/前后缀选列): 出现即不可推导。
_DYNAMIC_NAME_FUNCS = frozenset({"contains", "starts_with", "ends_with", "matches"})


def _dotted(func: ast.expr) -> str | None:
    """AST 调用目标 → 点链名 ("pl.col" / "col" / "cs.numeric" / None)。"""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        base = _dotted(func.value)
        return f"{base}.{func.attr}" if base else func.attr
    return None


def _dynamic_col_param_functions(tree: ast.Module) -> dict[str, dict[str, ast.expr | None]]:
    """形参被 col/cols 动态引用的模块级函数: {函数名: {形参名: 默认值}}。

    典型形态: `def _ema(col, alpha): return pl.col(col).ewm_mean(...)` —
    列名经形参传入。是否可证明取决于调用点实参是否全为常量 (见
    _collect_from_source 的常量流)。"""
    dynamic: dict[str, dict[str, ast.expr | None]] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        param_order = [a.arg for a in node.args.args]
        defaults: dict[str, ast.expr | None] = {name: None for name in param_order}
        # defaults 与形参列表尾部对齐 (python 语义)
        for offset, default in enumerate(node.args.defaults):
            defaults[param_order[len(param_order) - len(node.args.defaults) + offset]] = default
        hit: set[str] = set()
        for call in ast.walk(node):
            if not isinstance(call, ast.Call):
                continue
            dotted = _dotted(call.func)
            if dotted not in _COL_FORMS and dotted not in _COLS_FORMS:
                continue
            for arg in (*call.args, *(kw.value for kw in call.keywords)):
                if isinstance(arg, ast.Name) and arg.id in defaults:
                    hit.add(arg.id)
        if hit:
            dynamic[node.name] = {name: defaults[name] for name in hit}
    return dynamic


def _parse_and_index_sources(
    sources,
) -> tuple[
    list[ast.Module],
    dict[str, dict[str, ast.expr | None]],
    dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
]:
    """解析源码并汇总动态形参函数与函数定义。"""
    trees: list[ast.Module] = []
    dyn_funcs: dict[str, dict[str, ast.expr | None]] = {}
    func_defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    for raw in sources:
        tree = ast.parse(textwrap.dedent(raw))
        trees.append(tree)
        for fname, params in _dynamic_col_param_functions(tree).items():
            dyn_funcs.setdefault(fname, {}).update(params)
        for node in tree.body:
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                func_defs[node.name] = node
    return trees, dyn_funcs, func_defs


def _call_callee(node: ast.Call) -> str:
    """取调用点的被调函数名（裸名调用或跨模块属性调用）。"""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _resolve_argument(
    node: ast.Call, callee: str, param: str, default, func_defs, names: set[str]
) -> bool:
    """解析调用点上形参 param 的实参；返回是否可证明为字符串常量。"""
    func = func_defs.get(callee)
    value: ast.expr | None = None
    for kw in node.keywords:
        if kw.arg == param:
            value = kw.value
            break
    if value is None and func is not None:
        param_order = [a.arg for a in func.args.args]
        if param in param_order:
            pos = param_order.index(param)
            if pos < len(node.args):
                value = node.args[pos]
    if value is None:
        value = default
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        names.add(value.value)
        return True
    return False


def _collect_col_forms(node: ast.Call, dotted: str, dyn_funcs, names: set[str]) -> bool:
    """处理 col/cols 系列调用；返回是否遇到不可证明形态。"""
    if dotted in _COL_FORMS:
        if not node.args:
            return False
        return not _classify_column_argument(node.args[0], dyn_funcs, names)
    if dotted in _COLS_FORMS:
        for arg in node.args:
            if not _classify_column_argument(arg, dyn_funcs, names):
                return True
    return False


def _collect_from_sources(sources) -> tuple[set[str], bool]:
    """对一组源码整体收集; 返回 (列名, 是否存在不可证明形态)。

    常量流 (P1a+): 列名经辅助函数形参传递时 (`def _e(col): ... pl.col(col)`),
    该函数**全部调用点**的对应实参 (或形参默认值) 都是字符串常量 → 常量并入
    收集; 任一调用点非常量 (变量/循环元素) → 整体不可证明。模块级自由变量
    的 col 引用同样不可证明 — 宁可回退全量兜底, 不冒漏列风险。

    动态形参函数注册表与调用点解析是**跨文件**的: `_dep.py` 里定义
    `momentum_ok(col_name)`、主文件里 `_dep.momentum_ok("rsi_6")` 属于同一
    条常量流。同名函数跨文件并集 — 误并入只多收集列或整体断裂, 不漏列。"""
    trees, dyn_funcs, func_defs = _parse_and_index_sources(sources)
    names: set[str] = set()
    uninferable = False

    for tree in trees:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            dotted = _dotted(node.func)
            if dotted is None:
                continue
            # 辅助函数调用点: 裸名 + 跨模块属性 (`_dep.helper(...)`, 同名即并入解析)。
            callee = _call_callee(node)
            if callee in dyn_funcs:
                for param, default in dyn_funcs[callee].items():
                    if not _resolve_argument(node, callee, param, default, func_defs, names):
                        uninferable = True
                continue
            # pl.all() 是列选择器; polars.selectors (cs.*) 全族不可推导。
            if dotted == "pl.all" or dotted.startswith("cs."):
                uninferable = True
                continue
            if _collect_col_forms(node, dotted, dyn_funcs, names):
                uninferable = True
    return names, uninferable


def _collect_from_source(source: str) -> tuple[set[str], bool]:
    """单文件收集 (家族路径请用 _collect_from_sources 跨文件解析)。"""
    return _collect_from_sources([source])


def _classify_column_argument(arg: ast.expr, dyn_funcs, names: set[str]) -> bool:
    """col/cols 实参分类: 常量 → 收集; 动态形参 → True (交由调用点解析);
    其他 (自由变量/表达式) → False (调用方据此置 uninferable)。"""
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        names.add(arg.value)
        return True
    if isinstance(arg, ast.Name):
        # 与某个动态形参同名: 该引用由函数调用点的常量流覆盖;
        # 模块级自由变量恰与形参同名的场景, 常量流同样覆盖了该形参
        # 的全部取值, 不漏列。其余 Name → 不可证明。
        return any(arg.id in params for params in dyn_funcs.values())
    return False


def infer_filter_history_features(fn) -> frozenset[str] | None:
    """推导单个 filter_history 函数引用的输入列; None = 不可推导。

    只看函数自身源码 — 覆盖不了"同模块辅助函数/`_*.py` 依赖文件里硬编码列"
    的形态, 引擎路径请用 infer_features_from_sources (整家族文件一起推导)。
    """
    try:
        source = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError) as e:
        logger.debug("feature inference: no source for %r: %s", getattr(fn, "__name__", fn), e)
        return None
    return infer_features_from_sources([source])


def infer_features_from_sources(sources) -> frozenset[str] | None:
    """对一组源码 (策略主文件 + `_*.py` 依赖) 整体推导输入列。

    家族内任一文件出现不可证明形态 (动态列名/pl.all/cs.*) 即整体返回 None —
    辅助函数漏列会让回测帧缺列, 宁可整体回退全量兜底。
    其余 fail-closed 契约与单函数版一致。
    """
    from app.indicators.pipeline import ENRICHED_STORAGE_COLS, INDICATOR_COLUMNS

    try:
        names, uninferable = _collect_from_sources(sources)
    except SyntaxError:
        return None  # 家族文件解析失败按不可证明处理
    if uninferable:
        return None
    if not names:
        # 收集不到列引用 (如 df["x"] 下标风格) 同样不可证明。
        return None
    known = (
        set(ENRICHED_STORAGE_COLS)
        | set(INDICATOR_COLUMNS)
        | {"name", "total_shares", "float_shares"}
    )
    features = frozenset(n for n in names if n in known and not n.startswith("_"))
    return features or None
