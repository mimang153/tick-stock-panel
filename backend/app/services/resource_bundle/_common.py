"""策略、信号和因子的传输服务（分包实现）— 共享契约与工具。

资源包把策略/信号/因子定义及其依赖打成一个 ZIP，导入时按类别落到用户目录。
本模块集中定义下列契约，其它分包不得各自定义副本：

类别与错误：
  - BUNDLE_CATEGORIES/BUNDLE_FORMAT/RESOURCE_FORMAT：manifest 与 ZIP 条目白名单；
  - ResourceBundleError/StrategyBundleError：API 层据此区分 400 与引擎异常；
  - BundleSummary/ParsedBundle：预览与落盘的传递结构，进 manifest 供 UI 展示。

依赖解析：
  - _DependencyResolver 按列名/引用名解析 策略→信号→因子 的依赖闭包；
  - _closure/_changed_factor_specs 用包内+本机合并快照判依赖是否已存在。

源码引用提取（只读，绝不执行策略源码）：
  - _explicit_column_values collected from AST 常量与 col()/matrix_feature() 调用；
  - _strategy_refs_from_code 提取 META.scoring/SCORING/ENTRY/EXIT_SIGNALS 字面量，
    并保留 REQUIRED_FEATURES 与显式列输入，矩阵策略可排除进出场输出标签。

归档与安全：
  - _archive/_hash 保证同内容幂等；_target/_safe_rel 先校验路径再由调用方写盘；
  - _zip_entries/_literals 只读取 ZIP 内白名单条目与字面量赋值。

关键不变量：预览与导入共用同一份快照和同一套引用提取；本机依赖变化进入预览
指纹，避免预览后修改因子而导入另一种语义。
"""

from __future__ import annotations

import ast
import hashlib
import io
import json
import logging
import re
import stat
import threading
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast
from zoneinfo import ZoneInfo

from app import definitions
from app.factors import registry, store
from app.services import config_backup
from app.strategy import custom_signals
from app.strategy.ai_generator import AIStrategyGenerator, find_meta_assignment

logger = logging.getLogger("app.services.resource_bundle")


RESOURCE_FORMAT = "tick-resource-bundle"


RESOURCE_VERSION = 1


_IMPORT_LOCK = threading.RLock()


_KIND_BY_CATEGORY = {
    "strategies": "strategy",
    "custom_signals": "signal",
    "custom_factors": "factor",
    "strategy_overrides": "settings",
}


class ResourceBundleError(ValueError):
    """用户可修正的资源包、冲突或依赖错误。"""


BUNDLE_FORMAT = "tick-strategy-bundle"


BUNDLE_VERSION = 1


BUNDLE_CATEGORIES: tuple[str, ...] = (
    "strategies",
    "strategy_overrides",
    "custom_signals",
    "custom_factors",
)


class StrategyBundleError(ValueError):
    """包形状/依赖错误 — API 层转 400, 不与引擎异常混淆。"""


@dataclass
class BundleSummary:
    """导出闭包的结果摘要 (进 manifest, 供 UI 展示)。"""

    strategy_id: str
    label: str
    counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ParsedBundle:
    manifest: dict[str, Any]
    entries: list[tuple[str, str, Path]]  # (zip 内路径, 类别, 本机目标绝对路径)


@dataclass
class _ResourceCollection:
    data_dir: Path
    engine: Any
    strategy_id: str
    summary: BundleSummary
    entries: dict[str, bytes]
    factor_defs: dict[str, dict]
    signal_lookup: dict[str, dict]
    strategy_config: Any
    known_builtin_cols: frozenset[str] | None = None
    included_factors: set[str] = field(default_factory=set)
    included_signals: set[str] = field(default_factory=set)


class _DependencyResolver:
    def __init__(
        self,
        resources: dict[str, dict],
        factors: dict[str, dict],
        checked: set[str],
        signal_keys: dict[str, dict],
        known: set[str],
    ) -> None:
        self.resources = resources
        self.factors = factors
        self.checked = checked
        self.signal_keys = signal_keys
        self.known = known
        self.resolving_local: set[tuple[str, str]] = set()

    def field(self, item: dict, ref: str, *, is_signal: bool = False) -> None:
        signal = (
            _match_signal(ref, self.signal_keys)
            if is_signal or ref.startswith(("csg_", "csgi_", "signal_"))
            else None
        )
        if signal is not None:
            key = f"signal:{signal['id']}"
        elif ref in self.factors:
            key = f"factor:{ref}"
        elif ref in self.known:
            return
        else:
            item["issues"].append(f"缺少依赖: {ref}")
            return
        if key in self.resources:
            item["dependencies"].append(key)
        else:
            item["local_dependencies"].append(key)
            marker = (item["key"], key)
            if marker in self.resolving_local:
                return
            self.resolving_local.add(marker)
            try:
                if signal is not None:
                    doc = {**signal, "enabled": True}
                    custom_signals.validate(doc)
                    for dep in _signal_input_refs(doc):
                        self.field(item, dep)
                elif ref in self.factors:
                    if ref not in self.checked:
                        raise ValueError(f"本机依赖因子不可用: {ref}")
                    for dep in _factor_refs(self.factors[ref]):
                        self.field(item, dep)
            except (ValueError, TypeError, KeyError) as exc:
                item["issues"].append(str(exc))
            finally:
                self.resolving_local.remove(marker)


def _strategy_rel_path(data_dir: Path, sdef: Any) -> str | None:
    """StrategyDef → 包内相对路径; 内置策略 (app 包内) 返回 None。"""
    if sdef.file_path is None:
        return None
    try:
        rel = sdef.file_path.relative_to(data_dir)
    except ValueError:
        return None
    if rel.parts and rel.parts[0] == "strategies":
        return rel.as_posix()
    return None


def _signal_lookup(data_dir: Path) -> dict[str, dict]:
    """自定义信号定义的多键索引: id 与输出列名 (csg_*/显式 column) 都能命中。"""
    by_key: dict[str, dict] = {}
    for sig in custom_signals.load_all(data_dir):
        sid = sig.get("id")
        if not sid:
            continue
        by_key.setdefault(str(sid), sig)
        try:
            by_key.setdefault(custom_signals.resolve_column(sig), sig)
            by_key.setdefault(custom_signals.intraday_column_name(str(sid)), sig)
        except ValueError:
            continue
    return by_key


def _match_signal(ref: str, lookup: dict[str, dict]) -> dict | None:
    """策略引用名 → 信号定义。引用是列名口径 (signal_/csg_ 前缀或裸 id)。"""
    candidates = [ref]
    if ref.startswith("signal_"):
        candidates.append(ref[len("signal_") :])
    if ref.startswith("csgi_"):
        candidates.append(ref[5:])
    elif ref.startswith("csg_"):
        candidates.append(ref[4:])
    elif not ref.startswith(("signal_", "csg_", "csgi_")):
        # 引擎对裸名自动补 signal_ 前缀 (engine._collect_signal_hits 同口径)
        candidates.append(f"signal_{ref}")
    for key in candidates:
        hit = lookup.get(key)
        if hit is not None:
            return hit
    return None


def _explicit_column_values(node: ast.AST) -> list[str]:
    """从一个 AST 节点提取显式声明/调用的列引用值。"""
    values: list[str] = []
    targets = (
        node.targets
        if isinstance(node, ast.Assign)
        else [node.target]
        if isinstance(node, ast.AnnAssign)
        else []
    )
    if targets and any(isinstance(t, ast.Name) and t.id == "REQUIRED_FEATURES" for t in targets):
        try:
            value = ast.literal_eval(cast(Any, node).value)
            if isinstance(value, set | list | tuple):
                values = [v for v in value if isinstance(v, str)]
        except (ValueError, SyntaxError):
            pass
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "col"
    ):
        values.extend(
            a.value for a in node.args if isinstance(a, ast.Constant) and isinstance(a.value, str)
        )
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "matrix_feature"
    ):
        values.extend(
            a.value
            for a in node.args[1:2]
            if isinstance(a, ast.Constant) and isinstance(a.value, str)
        )
    return values


def _collect_assignment_refs(
    target_names: tuple[str, ...],
    node: ast.AST,
    *,
    include_signal_outputs: bool,
    factor_refs: set[str],
    signal_refs: list[str],
) -> None:
    """收集单个顶层赋值语句声明的因子/信号引用（就地累加）。"""
    if not isinstance(node, ast.Assign | ast.AnnAssign):
        return
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    names = {t.id for t in targets if isinstance(t, ast.Name) and t.id in target_names}
    if not names:
        return
    try:
        value = ast.literal_eval(cast(Any, node).value)
    except (ValueError, SyntaxError):
        return
    if "META" in names:
        scoring = value.get("scoring") if isinstance(value, dict) else None
        if isinstance(scoring, dict):
            factor_refs.update(str(k) for k in scoring)
        if isinstance(value, dict):
            for ref in value.get("required_features") or []:
                if isinstance(ref, str) and ref.startswith(("uf_", "cf_", "ext_")):
                    factor_refs.add(ref)
                elif isinstance(ref, str) and ref.startswith(("csg_", "csgi_", "signal_")):
                    signal_refs.append(ref)
    if "SCORING" in names and isinstance(value, dict):
        factor_refs.update(str(k) for k in value)
    if (
        include_signal_outputs
        and names & {"ENTRY_SIGNALS", "EXIT_SIGNALS"}
        and isinstance(value, list | tuple)
    ):
        signal_refs.extend(str(v) for v in value if v)


def _strategy_refs_from_code(
    code: str, *, include_signal_outputs: bool = True
) -> tuple[set[str], list[str]]:
    """AST 提取策略源码的引用: (因子引用集合, 信号引用列表)。

    只看顶层赋值字面量 (META.scoring / SCORING / ENTRY_SIGNALS / EXIT_SIGNALS),
    与引擎 getattr(mod, …) 同口径; 不执行任何代码。矩阵策略可排除进出场输出标签,
    但 REQUIRED_FEATURES、META.required_features 和源码中的显式列输入始终保留。
    """
    factor_refs: set[str] = set()
    signal_refs: list[str] = []

    try:
        tree = ast.parse(code)
    except SyntaxError:
        return factor_refs, signal_refs
    for node in tree.body:
        _collect_assignment_refs(
            ("META", "SCORING", "ENTRY_SIGNALS", "EXIT_SIGNALS"),
            node,
            include_signal_outputs=include_signal_outputs,
            factor_refs=factor_refs,
            signal_refs=signal_refs,
        )
    for walk_node in ast.walk(tree):
        # 明确声明的输入列和代码中的自定义列引用; 不猜测动态拼接的列名。
        for value in _explicit_column_values(walk_node):
            if value.startswith(("uf_", "cf_", "ext_")):
                factor_refs.add(value)
            elif value.startswith(("csg_", "csgi_", "signal_")):
                signal_refs.append(value)
    return factor_refs, signal_refs


def _archive(items: dict[str, bytes], manifest: dict) -> bytes:
    with io.BytesIO() as buffer:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, payload in sorted(items.items()):
                archive.writestr(name, payload)
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        return buffer.getvalue()


def _json(payload: bytes) -> dict:
    try:
        doc = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ResourceBundleError(f"JSON 无法解析: {exc}") from exc
    if not isinstance(doc, dict):
        raise ResourceBundleError("资源定义必须是 JSON 对象")
    return doc


def _hash(payload: bytes | None) -> str:
    return hashlib.sha256(payload).hexdigest() if payload is not None else "absent"


def _same(left: bytes, right: bytes, suffix: str) -> bool:
    if suffix == ".json":
        try:
            return json.loads(left) == json.loads(right)
        except (ValueError, UnicodeDecodeError):
            return False
    return left == right


def _zip(
    items: dict[str, bytes], roots: list[str], inputs: dict[str, list[str]] | None = None
) -> bytes:
    manifest = {
        "format": RESOURCE_FORMAT,
        "version": RESOURCE_VERSION,
        "created_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "app_version": config_backup._app_version(),
        "roots": roots,
        "inputs": inputs or {},
    }
    return _archive(items, manifest)


def _target(name: str) -> tuple[str, Path]:
    data_dir = config_backup._data_dir()
    rel = config_backup._safe_rel(name)
    classified = config_backup._classify(rel, data_dir)
    if rel.as_posix() != name or classified is None or classified[0] not in BUNDLE_CATEGORIES:
        raise ResourceBundleError(f"不允许的资源路径: {name}")
    category, target = classified
    # data_dir 自身可由部署配置指向符号链接; 包内子路径必须是普通路径。
    current = target
    while current != data_dir:
        if current.is_symlink():
            raise ResourceBundleError(f"资源目标路径包含符号链接: {name}")
        current = current.parent
    if not target.resolve().is_relative_to(data_dir.resolve()):
        raise ResourceBundleError(f"资源目标越界: {name}")
    return category, target


def _metadata(name: str, payload: bytes) -> dict:
    category, target = _target(name)
    kind = _KIND_BY_CATEGORY[category]
    rid = PurePosixPath(name).stem
    label = rid
    draft = False
    if kind == "strategy":
        if rid.startswith("_"):
            kind, rid = "code", name
        else:
            meta = AIStrategyGenerator._extract_meta(payload.decode("utf-8-sig"))
            rid = str(meta.get("id", ""))
            if not config_backup._NAME_RE.fullmatch(rid):
                raise ResourceBundleError(f"策略 META.id 格式非法: {name}")
            label = str(meta.get("name") or rid)
            draft = bool(meta.get("research_only"))
    elif kind in ("signal", "factor"):
        doc = _json(payload)
        if doc.get("id") != rid or not definitions.get_kind(kind).id_pattern.fullmatch(rid):
            raise ResourceBundleError(f"定义 ID 与文件名不一致或格式非法: {name}")
        label = str(doc.get("name") or doc.get("label") or rid)
    elif kind == "settings":
        _json(payload)
        label = f"{rid} 的调参"
    return {
        "key": f"{kind}:{rid}",
        "kind": kind,
        "id": rid,
        "label": label,
        "path": name,
        "category": category,
        "target": target,
        "payload": payload,
        "draft": draft,
    }


def _zip_entries(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    entries = [info for info in archive.infolist() if not info.is_dir()]
    if len(entries) > config_backup.MAX_ENTRIES:
        raise StrategyBundleError("资源包条目数超过上限")
    seen = set()
    total = 0
    for info in entries:
        try:
            rel = config_backup._safe_rel(info.filename)
        except config_backup.ConfigBackupError as exc:
            raise StrategyBundleError(str(exc)) from exc
        if rel.as_posix() != info.filename:
            raise StrategyBundleError(f"非法条目路径: {info.filename}")
        if info.filename in seen:
            raise StrategyBundleError(f"资源包包含重复条目: {info.filename}")
        seen.add(info.filename)
        if stat.S_ISLNK(info.external_attr >> 16):
            raise StrategyBundleError(f"资源包包含符号链接: {info.filename}")
        total += info.file_size
        if total > config_backup.MAX_TOTAL_BYTES or info.file_size > config_backup.MAX_FILE_BYTES:
            raise StrategyBundleError("资源包解压总量或单文件大小超过上限")
    return entries


def _literals(code: str) -> dict[str, Any]:
    result = {}
    for node in ast.parse(code).body:
        if isinstance(node, ast.Assign | ast.AnnAssign):
            try:
                value = ast.literal_eval(cast(Any, node).value)
            except (ValueError, SyntaxError):
                continue
            for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                if isinstance(target, ast.Name):
                    result[target.id] = value
    return result


def _factor_refs(doc: dict) -> set[str]:
    if doc.get("kind") == "composite":
        members = doc.get("members", {})
        return set(members) if isinstance(members, dict) else set()
    if doc.get("lang", "dsl") == "dsl":
        from app.factors.dsl import compile_formula

        compiled = compile_formula(doc.get("formula", ""))
        # 预检先遍历包内引用, 再注册成员并做完整校验。
        if any(error.code != "E001" for error in compiled.errors):
            raise ValueError(compiled.errors[0].message)
        return {name for name, _ in compiled.input_windows}
    return set(store.to_spec(doc).inputs)


def _condition_refs(conditions: list) -> set[str]:
    refs = set()
    for c in conditions:
        if not isinstance(c, dict):
            continue
        if isinstance(c.get("left"), str):
            refs.add(c["left"])
        right = c.get("right")
        if isinstance(right, str):
            if right.startswith("field:"):
                refs.add(right[6:])
            elif right in custom_signals.allowed_fields():
                refs.add(right)
    return refs


def _signal_input_refs(definition: dict) -> frozenset[str]:
    deps = definitions.definition_dependencies("signal", definition)
    if definition.get("timeframe") == custom_signals.TIMEFRAME_INTRADAY:
        from app.strategy.intraday_features import INTRADAY_FEATURES

        deps -= frozenset(INTRADAY_FEATURES)
    return deps


def _local_strategy_paths(data_dir: Path) -> dict[str, list[Path]]:
    local_strategies: dict[str, list[Path]] = {}
    for source in ("custom", "ai", "composite"):
        for path in (data_dir / "strategies" / source).glob("*.py"):
            if path.stem.startswith("_"):
                continue
            try:
                meta = AIStrategyGenerator._extract_meta(path.read_text(encoding="utf-8"))
                local_strategies.setdefault(str(meta.get("id")), []).append(path)
            except (ValueError, SyntaxError, UnicodeDecodeError):
                continue
    return local_strategies


def _factor_snapshot(
    resources: dict[str, dict],
    local_factors: dict[str, dict],
    snapshot: dict[str, registry.FactorSpec],
) -> tuple[dict[str, dict], set[str]]:
    # 不把当前进程中已经删除/来自其他目录的动态因子误当成可用依赖。
    for fid, spec in list(snapshot.items()):
        if spec.kind in ("custom", "composite"):
            snapshot.pop(fid)
    factors = {
        **local_factors,
        **{i["id"]: _json(i["payload"]) for i in resources.values() if i["kind"] == "factor"},
    }
    visiting: set[str] = set()
    checked: set[str] = set()

    def factor(fid: str) -> None:
        if fid in checked:
            return
        if fid in visiting:
            raise ValueError(f"因子存在循环依赖: {fid}")
        if fid not in factors:
            return
        visiting.add(fid)
        doc = factors[fid]
        try:
            for dep in _factor_refs(doc):
                factor(dep)
            snapshot[fid] = store.to_spec(doc)
            checked.add(fid)
        finally:
            visiting.remove(fid)

    for fid in factors:
        try:
            factor(fid)
        except (ValueError, TypeError, KeyError) as exc:
            if f"factor:{fid}" in resources:
                resources[f"factor:{fid}"]["issues"].append(str(exc))
    return factors, checked


def _signal_keys(resources: dict[str, dict], local_signals: dict) -> dict[str, dict]:
    signal_keys = dict(local_signals)
    for item in resources.values():
        if item["kind"] == "signal":
            doc = _json(item["payload"])
            signal_keys[doc["id"]] = doc
            signal_keys[custom_signals.resolve_column(doc)] = doc
            signal_keys[custom_signals.intraday_column_name(doc["id"])] = doc
    return signal_keys


def _closure(resources: dict[str, dict], selected: list[str]) -> list[str]:
    if not selected or any(
        key not in resources or resources[key]["kind"] not in ("strategy", "signal", "factor")
        for key in selected
    ):
        raise ResourceBundleError("请至少选择一个包内存在的策略、信号或因子")
    visited: set[str] = set()
    visiting: set[str] = set()
    order: list[str] = []

    def visit(key: str) -> None:
        if key in visiting:
            raise ResourceBundleError(f"资源存在循环依赖: {key}")
        if key in visited:
            return
        visiting.add(key)
        for dep in resources[key]["dependencies"]:
            visit(dep)
        visiting.remove(key)
        visited.add(key)
        order.append(key)

    for key in selected:
        visit(key)
    return order


def _changed_factor_specs(
    order: list[str], resources: dict[str, dict], specs: dict[str, Any]
) -> dict[str, Any]:
    needed_factors = {resources[k]["id"] for k in order if resources[k]["kind"] == "factor"}
    needed_factors.update(
        dep[7:]
        for k in order
        for dep in resources[k]["local_dependencies"]
        if dep.startswith("factor:")
    )
    pending = list(needed_factors)
    while pending:
        spec = specs.get(pending.pop())
        if spec is None:
            continue
        for fid in spec.inputs:
            if fid in specs and fid not in needed_factors:
                needed_factors.add(fid)
                pending.append(fid)
    return {
        fid: spec
        for fid, spec in specs.items()
        if fid in needed_factors and registry.get_factor(fid) != spec
    }


__all__ = [
    "BUNDLE_CATEGORIES",
    "BUNDLE_FORMAT",
    "BUNDLE_VERSION",
    "RESOURCE_FORMAT",
    "RESOURCE_VERSION",
    "_IMPORT_LOCK",
    "_KIND_BY_CATEGORY",
    "AIStrategyGenerator",
    "Any",
    "BundleSummary",
    "Callable",
    "ParsedBundle",
    "Path",
    "PurePosixPath",
    "ResourceBundleError",
    "StrategyBundleError",
    "ZoneInfo",
    "_DependencyResolver",
    "_ResourceCollection",
    "_archive",
    "_changed_factor_specs",
    "_closure",
    "_condition_refs",
    "_explicit_column_values",
    "_factor_refs",
    "_factor_snapshot",
    "_hash",
    "_json",
    "_literals",
    "_local_strategy_paths",
    "_match_signal",
    "_metadata",
    "_same",
    "_signal_input_refs",
    "_signal_keys",
    "_signal_lookup",
    "_strategy_refs_from_code",
    "_strategy_rel_path",
    "_target",
    "_zip",
    "_zip_entries",
    "ast",
    "config_backup",
    "custom_signals",
    "dataclass",
    "datetime",
    "definitions",
    "field",
    "find_meta_assignment",
    "hashlib",
    "io",
    "json",
    "logger",
    "logging",
    "re",
    "registry",
    "stat",
    "store",
    "threading",
    "zipfile",
]
