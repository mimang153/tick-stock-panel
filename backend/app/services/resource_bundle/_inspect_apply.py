"""策略、信号和因子的传输服务（分包实现）— 预检、预览与原子导入。

导入分四阶段，阶段间以同一份快照和同一指纹衔接：

1. 分析：_analyze 在 registry.isolated_registry 快照上同步本机扩展列，再结合
   包内定义算依赖；_factor_snapshot 只为只读预检复制规格，不污染全局注册表。
2. 预览：_inspect 为每项资源计算 before/action/comparison（conflict 时给出
   本机与包内各 6000 字截断对比），并给出依赖闭包、引用方与预览指纹。指纹含
   上传内容哈希、本机因子/信号/覆盖配置的 JSON 哈希，防止预览后环境变化。
3. 校验与回滚：_validate_resource_import 检查冲突与依赖缺口；任一步失败由
   _rollback_resource_import 回滚已写文件、恢复覆盖前的因子规格与注册表，
   错误统一转成用户可理解的中文原因。
4. 提交：_commit_resource_import 写盘后按 refresh 回调刷新缓存与注册表，
   apply_resources 串起全过程；manifest 计数与 UI 展示同源。

公开入口 inspect_resources/apply_resources/list_resources/build_resources 保持
旧调用签名；build_bundle/apply_bundle/inspect_bundle 为旧接口的兼容映射，
旧接口保留逐文件跳过/失败统计，原子整批导入仍由 apply_resources 负责。
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Callable
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast
from zoneinfo import ZoneInfo

from app import definitions
from app.factors import registry, store
from app.services import config_backup
from app.strategy import custom_signals
from app.strategy.ai_generator import AIStrategyGenerator

from ._bundle import _read_uploads, parse_bundle
from ._collect import collect_dependencies, collect_resources
from ._common import (
    _IMPORT_LOCK,
    BUNDLE_CATEGORIES,
    BUNDLE_FORMAT,
    BUNDLE_VERSION,
    BundleSummary,
    ResourceBundleError,
    StrategyBundleError,
    _archive,
    _changed_factor_specs,
    _closure,
    _condition_refs,
    _DependencyResolver,
    _factor_refs,
    _factor_snapshot,
    _hash,
    _json,
    _literals,
    _local_strategy_paths,
    _match_signal,
    _metadata,
    _same,
    _signal_input_refs,
    _signal_keys,
    _signal_lookup,
    _strategy_refs_from_code,
    _strategy_rel_path,
    _target,
    _zip,
    logger,
)

_PREVIEW_ENTRY_KEYS = (
    "key",
    "kind",
    "id",
    "label",
    "path",
    "dependencies",
    "local_dependencies",
    "issues",
    "action",
    "comparison",
    "referenced_by",
    "draft",
)


def _analyze_strategy(
    item: dict,
    resources: dict[str, dict],
    data_dir: Path,
    local_strategies: dict[str, list[Path]],
    field: Callable[..., None],
    kind: str,
) -> bool:
    code = item["payload"].decode("utf-8-sig")
    helpers = [
        i
        for i in resources.values()
        if i["kind"] == "code" and i["target"].parent == item["target"].parent
    ]
    dep_names = frozenset(i["target"].stem for i in helpers) | frozenset(
        p.stem for p in item["target"].parent.glob("_*.py")
    )
    config_backup._validate_strategy_code(code, is_dependency=kind == "code", dep_names=dep_names)
    values = _literals(code)
    metadata = values.get("META")
    native_signals = (
        values.get(
            "EXECUTION_BACKEND",
            metadata.get("execution_backend") if isinstance(metadata, dict) else None,
        )
        == "matrix_native"
    )
    factor_refs, signal_refs = _strategy_refs_from_code(
        code, include_signal_outputs=not native_signals
    )
    for ref in factor_refs:
        field(item, ref)
    for ref in signal_refs:
        field(item, ref, is_signal=True)
    if kind == "code":
        return True
    item["dependencies"].extend(i["key"] for i in helpers)
    settings_key = f"settings:{item['id']}"
    override = _json(resources[settings_key]["payload"]) if settings_key in resources else {}
    if settings_key in resources:
        item["dependencies"].append(settings_key)
    for ref in override.get("scoring") or {}:
        field(item, str(ref))
    for ref in _condition_refs(override.get("overlay_filter") or []):
        field(item, ref)
    for ref in (
        []
        if native_signals
        else [
            *(override.get("entry_signals") or []),
            *(override.get("exit_signals") or []),
        ]
    ):
        field(item, str(ref), is_signal=True)
    children = override.get("children") or values.get("META", {}).get("children") or []
    for child in children:
        sid = child["strategy_id"]
        key = f"strategy:{sid}"
        if key in resources:
            item["dependencies"].append(key)
        elif not any(
            (data_dir / "strategies" / source / f"{sid}.py").is_file()
            for source in ("custom", "ai", "composite")
        ):
            item["issues"].append(f"缺少子策略: {sid}")
    # ID 全局唯一, 不允许以不同来源路径绕过冲突检测。
    if any(path != item["target"] for path in local_strategies.get(item["id"], [])):
        item["issues"].append("本机同 ID 策略位于另一来源，请先处理已有策略")
    return False


def _analyze(resources: dict[str, dict]) -> dict[str, registry.FactorSpec]:
    """按包内+本机合并快照验证; 预览不执行策略源码、不更改全局注册表。"""
    data_dir = config_backup._data_dir()
    local_factors = {f["id"]: f for f in store.load_all(data_dir)}
    local_signals = _signal_lookup(data_dir)
    local_strategies = _local_strategy_paths(data_dir)
    for item in resources.values():
        item.update(dependencies=[], issues=[], local_dependencies=[])
    registry.all_factors()  # 先同步本机已配置的扩展列, 再复制只读预检快照。
    with registry.isolated_registry() as snapshot:
        factors, checked = _factor_snapshot(resources, local_factors, snapshot)
        known = set(definitions._known_input_columns()) | {"symbol", "date"}
        resolver = _DependencyResolver(
            resources, factors, checked, _signal_keys(resources, local_signals), known
        )
        for item in resources.values():
            kind, payload = item["kind"], item["payload"]
            try:
                for ref in item["inputs"]:
                    resolver.field(item, ref)
                if kind == "factor":
                    for ref in _factor_refs(_json(payload)):
                        resolver.field(item, ref)
                elif kind == "signal":
                    doc = {**_json(payload), "enabled": True}
                    custom_signals.validate(doc)
                    for ref in _signal_input_refs(doc):
                        resolver.field(item, ref)
                    smoke = definitions.smoke_definition("signal", doc)
                    item["issues"].extend(i["message"] for i in smoke["issues"])
                elif kind in ("strategy", "code"):
                    if _analyze_strategy(
                        item, resources, data_dir, local_strategies, resolver.field, kind
                    ):
                        continue
                elif kind == "settings":
                    config_backup._validate_json_object(payload, what="策略调参")
            except (ValueError, SyntaxError, TypeError, KeyError, UnicodeDecodeError) as exc:
                item["issues"].append(str(exc))
            item["dependencies"] = sorted(set(item["dependencies"]))
            item["issues"] = list(dict.fromkeys(item["issues"]))
        return {fid: snapshot[fid] for fid in checked}


def _inspect(
    uploads: list[tuple[str, bytes]],
) -> tuple[dict, dict[str, dict], dict[str, registry.FactorSpec]]:
    resources, roots = _read_uploads(uploads)
    specs = _analyze(resources)
    revision_parts: list[tuple[Any, ...]] = []
    revision_parts.extend((f"upload:{name}", _hash(payload)) for name, payload in uploads)
    for item in resources.values():
        target = item["target"]
        local = target.read_bytes() if target.exists() else None
        item["before"] = local
        item["action"] = (
            "create"
            if local is None
            else "reuse"
            if _same(local, item["payload"], target.suffix)
            else "conflict"
        )
        item["comparison"] = (
            {
                "local": local.decode("utf-8", errors="replace")[:6000],
                "incoming": item["payload"].decode("utf-8", errors="replace")[:6000],
                "truncated": len(local) > 6000 or len(item["payload"]) > 6000,
            }
            if item["action"] == "conflict" and local is not None
            else None
        )
        revision_parts.append((item["key"], _hash(local), _hash(item["payload"])))
    # 本机依赖变化也使预览失效, 防止预览后修改因子而导入另一种语义。
    data_dir = config_backup._data_dir()
    revision_parts.extend(
        (path.relative_to(data_dir).as_posix(), _hash(path.read_bytes()))
        for directory in ("custom_factors", "custom_signals", "strategy_overrides")
        for path in sorted((data_dir / "user_data" / directory).glob("*.json"))
    )
    revision_parts.extend(
        (path.relative_to(data_dir).as_posix(), _hash(path.read_bytes()))
        for source in ("custom", "ai", "composite")
        for path in sorted((data_dir / "strategies" / source).glob("*.py"))
    )
    revision = _hash(json.dumps(sorted(revision_parts), ensure_ascii=False).encode())
    impact = (
        _local_dependents(data_dir)
        if any(i["action"] == "conflict" for i in resources.values())
        else {}
    )
    for item in resources.values():
        item["referenced_by"] = impact.get(item["key"], [])
    entries = [{k: i[k] for k in _PREVIEW_ENTRY_KEYS} for i in resources.values()]
    counts = {
        kind: sum(i["kind"] == kind for i in entries)
        for kind in ("strategy", "signal", "factor", "settings", "code")
    }
    return (
        {
            "roots": roots,
            "entries": sorted(entries, key=lambda i: (i["kind"], i["label"])),
            "counts": counts,
            "revision": revision,
        },
        resources,
        specs,
    )


def inspect_resources(uploads: list[tuple[str, bytes]]) -> dict:
    """预览上传资源：返回根引用、条目明细、类别计数与预览指纹。"""
    try:
        return _inspect(uploads)[0]
    except (
        ValueError,
        SyntaxError,
        TypeError,
        KeyError,
        UnicodeDecodeError,
        OSError,
        RuntimeError,
        zipfile.BadZipFile,
    ) as exc:
        raise ResourceBundleError(str(exc)) from exc


def _local_strategy_edges(
    data_dir: Path,
    signals: dict[str, dict],
    key_for: Callable[[str], str],
) -> tuple[dict[str, str], dict[str, set[str]]]:
    """扫描已有策略源码及覆盖配置, 返回策略标签与依赖边。"""
    labels: dict[str, str] = {}
    edges: dict[str, set[str]] = {}
    for source in ("custom", "ai", "composite"):
        for path in (data_dir / "strategies" / source).glob("*.py"):
            if path.stem.startswith("_"):
                continue
            try:
                code = path.read_text(encoding="utf-8")
                meta = AIStrategyGenerator._extract_meta(code)
                key = f"strategy:{meta['id']}"
                labels[key] = str(meta.get("name") or meta["id"])
                values = _literals(code)
                native_signals = (
                    values.get("EXECUTION_BACKEND", meta.get("execution_backend"))
                    == "matrix_native"
                )
                factors, signal_refs = _strategy_refs_from_code(
                    code, include_signal_outputs=not native_signals
                )
                override_path = data_dir / "user_data/strategy_overrides" / f"{meta['id']}.json"
                override = _json(override_path.read_bytes()) if override_path.exists() else {}
                factors.update(override.get("scoring") or {})
                factors.update(_condition_refs(override.get("overlay_filter") or []))
                if not native_signals:
                    signal_refs += [
                        *(override.get("entry_signals") or []),
                        *(override.get("exit_signals") or []),
                    ]
                edges[key] = {key_for(ref) for ref in factors}
                edges[key].update(
                    f"signal:{match['id']}"
                    for ref in signal_refs
                    if (match := _match_signal(ref, signals)) is not None
                )
                edges[key].update(
                    f"strategy:{c['strategy_id']}"
                    for c in override.get("children") or meta.get("children") or []
                )
                edges[key].add(f"settings:{meta['id']}")
            except (ValueError, SyntaxError, KeyError, TypeError, UnicodeDecodeError):
                continue
    return labels, edges


def _local_dependents(data_dir: Path) -> dict[str, list[str]]:
    """替换前展示本机引用方, 包括经信号/组合因子间接引用的策略。"""
    edges: dict[str, set[str]] = {}
    labels: dict[str, str] = {}
    signals = _signal_lookup(data_dir)

    def key_for(ref: str) -> str:
        signal = (
            _match_signal(ref, signals) if ref.startswith(("csg_", "csgi_", "signal_")) else None
        )
        return f"signal:{signal['id']}" if signal is not None else f"factor:{ref}"

    for definition in store.load_all(data_dir):
        key = f"factor:{definition['id']}"
        labels[key] = str(definition.get("label") or definition["id"])
        try:
            refs = _factor_refs(definition)
        except (ValueError, TypeError, KeyError):
            refs = set()
        edges[key] = {key_for(ref) for ref in refs}
    for definition in {str(s["id"]): s for s in signals.values()}.values():
        key = f"signal:{definition['id']}"
        labels[key] = str(definition.get("name") or definition["id"])
        edges[key] = {key_for(ref) for ref in _condition_refs(definition.get("conditions") or [])}
    strategy_labels, strategy_edges = _local_strategy_edges(data_dir, signals, key_for)
    labels.update(strategy_labels)
    edges.update(strategy_edges)
    reverse: dict[str, set[str]] = {}
    for parent, deps in edges.items():
        for dep in deps:
            reverse.setdefault(dep, set()).add(parent)
    result = {}
    for key in reverse:
        seen = set()
        stack = list(reverse[key])
        while stack:
            parent = stack.pop()
            if parent == key or parent in seen:
                continue
            seen.add(parent)
            stack.extend(reverse.get(parent, ()))
        kind_labels = {"strategy": "策略", "signal": "信号", "factor": "因子"}
        result[key] = sorted(
            f"{kind_labels.get(parent.split(':')[0], '资源')}「{labels[parent]}」"
            for parent in seen
            if parent in labels
        )
    return result


def list_resources(data_dir: Path, engine: Any) -> list[dict]:
    items = [
        {"kind": d["kind"], "id": d["id"], "label": d["label"], "description": ""}
        for d in definitions.catalog(data_dir)
    ]
    if engine is not None:
        for strategy in engine.list_strategies(include_research=True):
            sdef = engine.get(strategy["id"])
            if _strategy_rel_path(data_dir, sdef) is not None:
                items.append(
                    {
                        "kind": "strategy",
                        "id": strategy["id"],
                        "label": str(sdef.meta.get("name") or strategy["id"]),
                        "description": str(sdef.meta.get("description") or ""),
                        "source": sdef.source,
                        "draft": bool(sdef.meta.get("research_only")),
                    }
                )
    return sorted(items, key=lambda i: (i["kind"], i["label"]))


def build_resources(
    data_dir: Path, engine: Any, roots: list[dict[str, str]]
) -> tuple[bytes, str, dict]:
    keys = list(dict.fromkeys(f"{ref['kind']}:{ref['id']}" for ref in roots))
    allowed = {f"{i['kind']}:{i['id']}" for i in list_resources(data_dir, engine)}
    if not keys or any(key not in allowed for key in keys):
        raise ResourceBundleError("请选择可导出的策略、信号或自定义因子")
    try:
        items, _ = collect_resources(data_dir, engine, roots)
        inputs = {}
        if engine is not None:
            for name, _payload in items:
                if name.startswith("strategies/") and not PurePosixPath(name).stem.startswith("_"):
                    sdef = engine.get(_metadata(name, _payload)["id"])
                    inputs[f"strategy:{sdef.meta['id']}"] = sorted(
                        ref
                        for ref in sdef.required_features
                        if ref.startswith(("uf_", "cf_", "ext_", "csg_", "csgi_", "signal_"))
                    )
        blob = _zip(dict(items), keys, inputs)
        preview = inspect_resources([("resources.zip", blob)])
        preview["revision"] = _hash(
            json.dumps(
                [(name, _hash(payload)) for name, payload in sorted(items)], ensure_ascii=False
            ).encode()
        )
        return blob, "tsp-resources.zip", preview
    except (StrategyBundleError, config_backup.ConfigBackupError, ValueError, OSError) as exc:
        raise ResourceBundleError(str(exc)) from exc


def _validate_resource_import(
    preview: dict,
    resources: dict[str, dict],
    selected: list[str],
    overwrite: list[str],
    revision: str,
) -> tuple[list[str], list[dict]]:
    if revision != preview["revision"]:
        raise ResourceBundleError("本机资源已变化，请重新预览后再导入")
    order = _closure(resources, selected)
    if set(overwrite) - set(resources):
        raise ResourceBundleError("替换清单包含未知资源")
    issues = [
        f"{resources[k]['label']}: {issue}" for k in order for issue in resources[k]["issues"]
    ]
    if issues:
        raise ResourceBundleError("所选资源校验失败: " + "; ".join(issues[:8]))
    conflicts = [k for k in order if resources[k]["action"] == "conflict" and k not in overwrite]
    if conflicts:
        raise ResourceBundleError(
            "请处理同 ID 内容冲突: " + ", ".join(resources[k]["label"] for k in conflicts)
        )
    writes = [resources[k] for k in order if resources[k]["action"] != "reuse"]
    return order, writes


def _rollback_resource_import(
    written: list[dict],
    previous_specs: dict[str, Any],
    engine: Any,
    exc: Exception,
) -> None:
    rollback_errors = []
    for item in reversed(written):
        try:
            if item["before"] is None:
                item["target"].unlink(missing_ok=True)
            else:
                config_backup._write_payload(item["target"], item["before"])
        except OSError as rollback_exc:
            rollback_errors.append(str(rollback_exc))
    for fid, old in previous_specs.items():
        registry.unregister_factor(fid)
        if old is not None:
            registry.register_factor(old)
    if engine is not None:
        engine.reload()
    if rollback_errors:
        logger.error("resource import rollback incomplete: %s", rollback_errors)
        raise ResourceBundleError(
            f"导入失败，部分文件回滚失败: {'; '.join(rollback_errors)}"
        ) from exc
    raise ResourceBundleError(f"导入失败，已回滚本次修改: {exc}") from exc


def _commit_resource_import(
    order: list[str],
    resources: dict[str, dict],
    writes: list[dict],
    changed_specs: dict[str, Any],
    previous_specs: dict[str, Any],
    *,
    engine: Any,
    refresh: Callable[[], None] | None,
) -> None:
    written: list[dict] = []
    with _IMPORT_LOCK:
        for key in order:
            item = resources[key]
            _target(item["path"])
            current = item["target"].read_bytes() if item["target"].exists() else None
            if current != item["before"]:
                raise ResourceBundleError("本机资源已变化，请重新预览后再导入")
        try:
            for item in writes:
                # 先记录再写: 原子替换失败也能恢复临时文件之外的原始目标。
                written.append(item)
                config_backup._write_payload(item["target"], item["payload"])
            for fid, spec in changed_specs.items():
                registry.unregister_factor(fid)
                registry.register_factor(spec)
            if engine is not None and (writes or changed_specs):
                engine.reload()
                for key in order:
                    item = resources[key]
                    if (
                        item["kind"] == "strategy"
                        and engine.get(item["id"]).file_path.resolve() != item["target"].resolve()
                    ):
                        raise ValueError(f"策略未加载到预期位置: {item['label']}")
            if refresh is not None and (writes or changed_specs):
                refresh()
        except Exception as exc:  # noqa: BLE001 - 导入失败需回滚全部已写文件
            _rollback_resource_import(written, previous_specs, engine, exc)


def apply_resources(
    uploads: list[tuple[str, bytes]],
    *,
    selected: list[str],
    overwrite: list[str],
    revision: str,
    engine: Any = None,
    refresh: Callable[[], None] | None = None,
) -> dict:
    try:
        preview, resources, specs = _inspect(uploads)
        order, writes = _validate_resource_import(preview, resources, selected, overwrite, revision)
        changed_specs = _changed_factor_specs(order, resources, specs)
        previous_specs = {fid: registry.get_factor(fid) for fid in changed_specs}
        _commit_resource_import(
            order,
            resources,
            writes,
            changed_specs,
            previous_specs,
            engine=engine,
            refresh=refresh,
        )
        return {
            "imported": len(writes),
            "reused": len(order) - len(writes),
            "counts": {
                kind: sum(resources[k]["kind"] == kind for k in order)
                for kind in ("strategy", "signal", "factor")
            },
            "resources": [
                k for k in order if resources[k]["kind"] in ("strategy", "signal", "factor")
            ],
            "changed": sorted(
                {*[i["key"] for i in writes], *[f"factor:{fid}" for fid in changed_specs]}
            ),
            "drafts": [
                resources[k]["id"]
                for k in order
                if resources[k]["kind"] == "strategy" and resources[k]["draft"]
            ],
        }
    except ResourceBundleError:
        raise
    except (
        ValueError,
        SyntaxError,
        TypeError,
        KeyError,
        OSError,
        UnicodeDecodeError,
        RuntimeError,
        zipfile.BadZipFile,
    ) as exc:
        raise ResourceBundleError(str(exc)) from exc


def build_bundle(data_dir: Path, engine: Any, strategy_id: str) -> tuple[bytes, str, BundleSummary]:
    """构建策略包 ZIP: 依赖条目 + manifest。返回 (zip 字节, 文件名, 摘要)。"""
    items, summary = collect_dependencies(data_dir, engine, strategy_id)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    manifest = {
        "format": BUNDLE_FORMAT,
        "version": BUNDLE_VERSION,
        "created_at": now.isoformat(),
        "app_version": config_backup._app_version(),
        "strategy_id": summary.strategy_id,
        "label": summary.label,
        "counts": summary.counts,
        "warnings": summary.warnings,
        "entries": [
            {
                "path": rel,
                "kind": cast(
                    tuple[str, Path], config_backup._classify(PurePosixPath(rel), data_dir)
                )[0],
            }
            for rel, _ in items
        ],
    }
    return _archive(dict(items), manifest), f"tsp-strategy-{summary.strategy_id}.zip", summary


def inspect_bundle(data: bytes) -> dict[str, Any]:
    """旧接口仅映射响应, 依赖检查复用当前资源预检。"""
    parsed = parse_bundle(data)
    try:
        resources, _ = _read_uploads([("strategy.zip", data)])
        _analyze(resources)
    except (
        ValueError,
        SyntaxError,
        TypeError,
        KeyError,
        UnicodeDecodeError,
        OSError,
        RuntimeError,
        zipfile.BadZipFile,
    ) as exc:
        raise StrategyBundleError(str(exc)) from exc
    by_path = {item["path"]: item for item in resources.values()}
    return {
        "strategy_id": parsed.manifest.get("strategy_id"),
        "label": parsed.manifest.get("label"),
        "created_at": parsed.manifest.get("created_at"),
        "counts": parsed.manifest.get("counts", {}),
        "warnings": parsed.manifest.get("warnings", []),
        "entries": [
            {
                "path": name,
                "kind": category,
                "id": PurePosixPath(name).stem,
                "action": "conflict" if target.exists() else "create",
                "bytes": len(by_path[name]["payload"]),
            }
            for name, category, target in parsed.entries
        ],
        # 旧接口把未命中的 signal_* 视为安装自带信号, 只提示缺失的自定义输入。
        "missing_deps": sorted(
            {
                issue
                for item in resources.values()
                for issue in item["issues"]
                if issue.startswith("缺少依赖: ")
                and not issue.removeprefix("缺少依赖: ").startswith("signal_")
            }
        ),
    }


def apply_bundle(data: bytes, *, allow_overwrite: bool = False) -> dict[str, Any]:
    """旧接口保留逐文件跳过/失败统计; 原子整批导入仍由 apply_resources 处理。"""
    parsed = parse_bundle(data)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        items = {name: config_backup._read_entry(archive, name) for name, _, _ in parsed.entries}
    counts = {
        category: sum(kind == category for _, kind, _ in parsed.entries)
        for category in BUNDLE_CATEGORIES
    }
    manifest = {
        "format": config_backup.BACKUP_FORMAT,
        "version": config_backup.BACKUP_VERSION,
        "created_at": parsed.manifest.get("created_at"),
        "app_version": parsed.manifest.get("app_version", ""),
        "include_secrets": False,
        "categories": [
            {"key": key, "label": config_backup._CATEGORY_LABELS[key], "count": count}
            for key, count in counts.items()
            if count
        ],
    }
    return config_backup.apply_archive(
        _archive(items, manifest), allow_overwrite=allow_overwrite, categories=BUNDLE_CATEGORIES
    )
