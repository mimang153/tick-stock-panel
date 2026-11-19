"""策略、信号和因子的传输服务（分包实现）。"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any

from app import definitions
from app.factors import store
from app.services import config_backup

from ._common import (
    BundleSummary,
    StrategyBundleError,
    _factor_refs,
    _match_signal,
    _ResourceCollection,
    _signal_lookup,
    _strategy_refs_from_code,
    _strategy_rel_path,
)


def collect_dependencies(
    data_dir: Path, engine: Any, strategy_id: str
) -> tuple[list[tuple[str, bytes]], BundleSummary]:
    """策略 → (条目列表, 摘要)。条目 = [(包内相对路径, 文件字节)]。

    闭包范围: 策略 .py + 其引用的自定义信号/因子 (递归) + 用户调参 override
    + composite 子策略 (递归)。内置因子列/内置信号名不打包 (任何安装自带);
    找不到的自定义引用 (csg_/uf_) 记 warnings, 不阻断导出 — 如实告知。
    """
    return collect_resources(data_dir, engine, [{"kind": "strategy", "id": strategy_id}])


def _collect_factor(state: _ResourceCollection, factor_id: str) -> None:
    if factor_id in state.included_factors:
        return
    definition = state.factor_defs.get(factor_id)
    if definition is None:
        return
    state.included_factors.add(factor_id)
    state.entries[f"user_data/custom_factors/{factor_id}.json"] = json.dumps(
        definition, ensure_ascii=False, indent=2
    ).encode("utf-8")
    try:
        deps = _factor_refs(definition)
    except ValueError as e:
        state.summary.warnings.append(f"因子 {factor_id} 定义编译失败, 依赖未展开: {e}")
        return
    for dep in sorted(deps):
        _collect_factor_ref(state, dep)


def _collect_factor_ref(state: _ResourceCollection, ref: str) -> None:
    if ref in state.factor_defs:
        _collect_factor(state, ref)
        return
    if state.known_builtin_cols is None:
        state.known_builtin_cols = definitions._known_input_columns()
    if ref not in state.known_builtin_cols:
        state.summary.warnings.append(f"引用因子列 {ref} 不在因子库 (可能已删除或为未知列), 未打包")


def _collect_signal(state: _ResourceCollection, sig: dict) -> None:
    sid = str(sig.get("id"))
    if sid in state.included_signals:
        return
    state.included_signals.add(sid)
    state.entries[f"user_data/custom_signals/{sid}.json"] = json.dumps(
        sig, ensure_ascii=False, indent=2
    ).encode("utf-8")
    deps = definitions.definition_dependencies("signal", sig)
    if sig.get("timeframe") == "intraday":
        from app.strategy.intraday_features import INTRADAY_FEATURES

        deps -= frozenset(INTRADAY_FEATURES)
    for dep in sorted(deps):
        _collect_factor_ref(state, dep)


def _load_strategy(
    state: _ResourceCollection, sid: str, root: bool
) -> tuple[Any, str | None] | None:
    try:
        child = state.engine.get(sid)
    except ValueError as exc:
        if root:
            raise StrategyBundleError(str(exc)) from exc
        state.summary.warnings.append(f"composite 子策略 {sid} 不存在, 未打包")
        return None
    rel = _strategy_rel_path(state.data_dir, child)
    if rel is None:
        if root:
            raise StrategyBundleError(f"策略 {sid} 是内置策略, 无需打包导出")
        state.summary.warnings.append(f"composite 子策略 {sid} 是内置策略 (任何安装自带), 未打包")
    elif rel not in state.entries:
        path = state.data_dir / rel
        state.entries[rel] = path.read_bytes()
    else:
        return None
    if root and sid == state.strategy_id:
        state.summary.label = str(child.meta.get("name") or sid)

    # 同目录辅助模块由引擎白名单加载, 必须随策略一起迁移。
    if rel is not None:
        for helper in (state.data_dir / rel).parent.glob("_*.py"):
            state.entries[helper.relative_to(state.data_dir).as_posix()] = helper.read_bytes()
    return child, rel


def _collect_strategy_refs(
    state: _ResourceCollection, sid: str, child: Any, rel: str | None
) -> tuple[set[str], list[str], dict]:
    # 因子引用: META.scoring + 模块 SCORING + override 里的 scoring
    factor_refs = {str(k) for k in (child.meta.get("scoring") or {})}

    factor_refs.update(child.scoring_columns)
    factor_refs.update(
        ref for ref in child.required_features if ref.startswith(("uf_", "cf_", "ext_"))
    )
    code = state.entries.get(rel, b"").decode("utf-8") if rel else ""
    native_signals = child.execution_backend == "matrix_native"
    code_factors, code_signals = _strategy_refs_from_code(
        code, include_signal_outputs=not native_signals
    )
    factor_refs.update(code_factors)
    helper_signals: list[str] = []
    if rel is not None:
        for helper in (state.data_dir / rel).parent.glob("_*.py"):
            helper_factors, helper_signal_refs = _strategy_refs_from_code(
                helper.read_text(encoding="utf-8")
            )
            factor_refs.update(helper_factors)
            helper_signals.extend(helper_signal_refs)

    # 信号引用: 模块常量 + override 增改
    # 矩阵策略的进出场名称是 compute_signals 输出标签, 不是信号库输入。
    signal_refs = code_signals + helper_signals
    if not native_signals:
        signal_refs += list(child.entry_signals) + list(child.exit_signals)
    signal_refs.extend(
        ref for ref in child.required_features if ref.startswith(("csg_", "csgi_", "signal_"))
    )
    override = state.strategy_config.load_override(state.data_dir, sid) or {}
    if not native_signals:
        signal_refs += [str(r) for r in (override.get("entry_signals") or [])]
        signal_refs += [str(r) for r in (override.get("exit_signals") or [])]
    ov_scoring = override.get("scoring")
    if isinstance(ov_scoring, dict):
        factor_refs.update(str(k) for k in ov_scoring)
    for condition in override.get("overlay_filter") or []:
        if isinstance(condition, dict):
            factor_refs.add(str(condition.get("left", "")))
            right = condition.get("right")
            if isinstance(right, str) and right.startswith("field:"):
                factor_refs.add(right[6:])
    factor_refs.discard("")
    return factor_refs, signal_refs, override


def _resolve_strategy_refs(
    state: _ResourceCollection, factor_refs: set[str], signal_refs: list[str]
) -> None:
    for ref in sorted(factor_refs):
        _collect_factor_ref(state, ref)
    for ref in signal_refs:
        hit = _match_signal(ref, state.signal_lookup)
        if hit is not None:
            _collect_signal(state, hit)
        elif ref.startswith(("csg_", "csgi_")):
            state.summary.warnings.append(f"引用自定义信号 {ref} 不在信号库 (可能已删除), 未打包")


def _collect_strategy_override(state: _ResourceCollection, sid: str) -> None:
    # 用户调参 (换机/分享场景里比默认值重要)
    ov_path = state.data_dir / "user_data" / "strategy_overrides" / f"{sid}.json"
    if ov_path.is_file():
        state.entries[f"user_data/strategy_overrides/{sid}.json"] = ov_path.read_bytes()


def _collect_composite_children(
    state: _ResourceCollection,
    child: Any,
    override: dict,
    add_strategy: Callable[[str, bool], None],
) -> None:
    # composite 子策略递归 (只允许一层, 与引擎非嵌套约束一致)
    if child.composite is not None:
        children = override.get("children")
        child_ids = (
            [str(c["strategy_id"]) for c in children]
            if isinstance(children, list) and children
            else [c.strategy_id for c in child.composite.children]
        )
        for child_id in child_ids:
            add_strategy(child_id, False)


def _collect_roots(
    state: _ResourceCollection,
    roots: list[dict[str, str]],
    add_strategy: Callable[[str, bool], None],
    add_factor: Callable[[str], None],
    add_signal: Callable[[dict], None],
) -> None:
    for ref in roots:
        kind, rid = ref["kind"], ref["id"]
        if kind == "strategy":
            add_strategy(rid, True)
        elif kind == "factor" and rid in state.factor_defs:
            add_factor(rid)
        elif kind == "signal" and rid in state.signal_lookup:
            add_signal(state.signal_lookup[rid])
        else:
            raise StrategyBundleError(f"资源不存在: {kind}:{rid}")


def _entry_counts(entries: dict[str, bytes], data_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rel in entries:
        category = config_backup._classify(PurePosixPath(rel), data_dir)
        if category is not None:
            counts[category[0]] = counts.get(category[0], 0) + 1
    return counts


def collect_resources(
    data_dir: Path, engine: Any, roots: list[dict[str, str]]
) -> tuple[list[tuple[str, bytes]], BundleSummary]:
    """多策略/信号/因子共享同一闭包收集器; 文件和共享依赖只收一次。"""
    from app.strategy import config as strategy_config

    if not roots:
        raise StrategyBundleError("请至少选择一个资源")
    strategy_id = roots[0]["id"]
    summary = BundleSummary(strategy_id=strategy_id, label=strategy_id)
    entries: dict[str, bytes] = {}

    state = _ResourceCollection(
        data_dir=data_dir,
        engine=engine,
        strategy_id=strategy_id,
        summary=summary,
        entries=entries,
        factor_defs={str(d.get("id")): d for d in store.load_all(data_dir)},
        signal_lookup=_signal_lookup(data_dir),
        strategy_config=strategy_config,
    )

    def _add_strategy(sid: str, root: bool) -> None:
        loaded = _load_strategy(state, sid, root)
        if loaded is None:
            return
        child, rel = loaded
        factor_refs, signal_refs, override = _collect_strategy_refs(state, sid, child, rel)
        _resolve_strategy_refs(state, factor_refs, signal_refs)
        _collect_strategy_override(state, sid)
        _collect_composite_children(state, child, override, _add_strategy)

    _collect_roots(
        state,
        roots,
        _add_strategy,
        lambda factor_id: _collect_factor(state, factor_id),
        lambda sig: _collect_signal(state, sig),
    )
    summary.counts = _entry_counts(state.entries, data_dir)
    return sorted(entries.items()), summary
