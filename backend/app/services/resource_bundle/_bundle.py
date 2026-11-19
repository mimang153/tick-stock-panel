"""策略、信号和因子的传输服务（分包实现）— ZIP 解析与上传收集。

parse_bundle 只做形状校验：ZIP 可读、体积上限、manifest 格式/版本、条目全部
落在四类白名单内，且策略包必须含至少一个策略文件；不执行任何策略源码。
_UploadCollector 汇集三类输入（散文件、宽松 ZIP、标准资源包），按文件名和
内容哈希去重：同名不同内容直接拒绝，避免静默覆盖用户已有定义。
finalize 返回资源字典与根引用列表，交给 _collect 展开依赖闭包。
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path, PurePosixPath

from app import definitions
from app.services import config_backup
from app.strategy.ai_generator import AIStrategyGenerator, find_meta_assignment

from ._common import (
    BUNDLE_CATEGORIES,
    BUNDLE_FORMAT,
    BUNDLE_VERSION,
    RESOURCE_FORMAT,
    ParsedBundle,
    ResourceBundleError,
    StrategyBundleError,
    _json,
    _metadata,
    _same,
    _target,
    _zip_entries,
)


def _classify_bundle_entries(
    infos: list[zipfile.ZipInfo], data_dir: Path
) -> tuple[list[tuple[str, str, Path]], list[str]]:
    """按 4 类白名单分类 ZIP 条目；返回 (合法条目, 越界文件名)。"""
    entries: list[tuple[str, str, Path]] = []
    offenders: list[str] = []
    for info in infos:
        if info.filename == "manifest.json":
            continue
        rel = config_backup._safe_rel(info.filename)
        cls = config_backup._classify(rel, data_dir) if rel is not None else None
        if cls is None or cls[0] not in BUNDLE_CATEGORIES:
            offenders.append(info.filename)
            continue
        entries.append((info.filename, cls[0], cls[1]))
    return entries, offenders


def parse_bundle(data: bytes, *, allow_resources: bool = False) -> ParsedBundle:
    """校验包形状: ZIP 可读/上限/manifest 合法/条目全部落在 4 类白名单内。"""
    if not data:
        raise StrategyBundleError("空文件")
    if len(data) > config_backup.MAX_ARCHIVE_BYTES:
        raise StrategyBundleError(
            f"策略包超过 {config_backup.MAX_ARCHIVE_BYTES // 1024 // 1024}MB 上限"
        )
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise StrategyBundleError("不是有效的策略包 ZIP 文件") from e
    with zf:
        infos = _zip_entries(zf)
        try:
            manifest = json.loads(config_backup._read_entry(zf, "manifest.json"))
        except KeyError as e:
            raise StrategyBundleError("策略包缺少 manifest.json") from e
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise StrategyBundleError(f"manifest.json 解析失败: {e}") from e
        formats = {BUNDLE_FORMAT, RESOURCE_FORMAT} if allow_resources else {BUNDLE_FORMAT}
        if not isinstance(manifest, dict) or manifest.get("format") not in formats:
            raise StrategyBundleError("不是本应用的策略包文件")
        if manifest.get("version") != BUNDLE_VERSION:
            raise StrategyBundleError(
                f"不支持的策略包版本 {manifest.get('version')!r} (当前支持 {BUNDLE_VERSION})"
            )

        data_dir = config_backup._data_dir()
        entries, offenders = _classify_bundle_entries(infos, data_dir)
        if offenders:
            raise StrategyBundleError(
                "策略包包含不允许的条目 (只允许策略/调参/信号/因子): " + ", ".join(offenders[:5])
            )
        if manifest.get("format") == BUNDLE_FORMAT and not any(
            kind == "strategies" for _, kind, _ in entries
        ):
            raise StrategyBundleError("策略包里没有策略文件")
    return ParsedBundle(manifest=manifest, entries=entries)


class _UploadCollector:
    """收集散文件/宽松 ZIP/资源包，按文件名与内容哈希去重并守体积上限。"""

    def __init__(self) -> None:
        self.items: dict[str, bytes] = {}
        self.roots: list[str] = []
        self.inputs: dict[str, set[str]] = {}
        self.total = 0

    def add(self, name: str, payload: bytes) -> None:
        _target(name)
        if name in self.items:
            if not _same(self.items[name], payload, PurePosixPath(name).suffix):
                raise ResourceBundleError(f"所选文件携带了不同内容的同名资源: {name}")
            return
        if len(payload) > config_backup.MAX_FILE_BYTES:
            raise ResourceBundleError(f"资源文件超过大小上限: {name}")
        self.total += len(payload)
        if (
            self.total > config_backup.MAX_TOTAL_BYTES
            or len(self.items) >= config_backup.MAX_ENTRIES
        ):
            raise ResourceBundleError("所选资源解压总量或条目数超过上限")
        self.items[name] = payload

    def add_loose(self, filename: str, payload: bytes) -> None:
        suffix = PurePosixPath(filename).suffix.lower()
        if suffix == ".json":
            doc = _json(payload)
            kind = (
                "signal"
                if "conditions" in doc
                else "factor"
                if "formula" in doc or doc.get("kind") == "composite"
                else None
            )
            if kind is None:
                raise ResourceBundleError(f"无法识别 {filename}，请选择信号或因子定义 JSON")
            rid = str(doc.get("id", ""))
            if not definitions.get_kind(kind).id_pattern.fullmatch(rid):
                raise ResourceBundleError(f"{filename} 的资源 ID 非法")
            category = "custom_signals" if kind == "signal" else "custom_factors"
            self.add(f"user_data/{category}/{rid}.json", payload)
            self.roots.append(f"{kind}:{rid}")
        elif suffix == ".py":
            code = payload.decode("utf-8-sig")
            meta = AIStrategyGenerator._extract_meta(code)
            rid = str(meta.get("id", ""))
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", rid):
                raise ResourceBundleError(f"{filename} 缺少有效的 META.id")
            source = (
                "composite" if meta.get("children") else "ai" if rid.startswith("ai_") else "custom"
            )
            if source == "ai" and "research_only" not in meta:
                # 与 AI 策略新建口径一致; 包内显式发布/草稿状态则原样保留。
                found = find_meta_assignment(code)
                if found is None:
                    raise ResourceBundleError("找不到 META 字典")
                node = found[1]
                encoded = code.encode("utf-8")
                lines = encoded.splitlines(keepends=True)
                position = sum(len(line) for line in lines[: node.lineno - 1]) + node.col_offset + 1
                code = (
                    encoded[:position] + b'"research_only": True, ' + encoded[position:]
                ).decode("utf-8")
            self.add(f"strategies/{source}/{rid}.py", code.encode("utf-8"))
            self.roots.append(f"strategy:{rid}")
        else:
            raise ResourceBundleError(f"不支持 {filename}，请选择 .zip、.py 或 .json")

    def add_plain_zip(self, archive: zipfile.ZipFile, entries: list[zipfile.ZipInfo]) -> None:
        root_count = len(self.roots)
        for info in entries:
            rel = PurePosixPath(info.filename)
            if (
                "__MACOSX" in rel.parts
                or rel.name == ".DS_Store"
                or rel.name.startswith("._")
                or rel.suffix.lower() in (".md", ".txt", ".rst")
            ):
                continue
            payload = config_backup._read_entry(archive, info.filename)
            # 容忍外层分享文件夹, 但保留已导出资源的来源目录、辅助模块与调参。
            for offset in range(len(rel.parts)):
                path = PurePosixPath(*rel.parts[offset:])
                classified = config_backup._classify(path, config_backup._data_dir())
                if classified is not None and classified[0] in BUNDLE_CATEGORIES:
                    self.add(path.as_posix(), payload)
                    item = _metadata(path.as_posix(), payload)
                    if item["kind"] in ("strategy", "factor", "signal"):
                        self.roots.append(item["key"])
                    break
            else:
                self.add_loose(info.filename, payload)
        if len(self.roots) == root_count:
            raise ResourceBundleError("压缩包中没有可导入的策略 .py、因子或信号 .json 文件")

    def add_bundle(self, filename: str, payload: bytes) -> None:
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                entries = _zip_entries(archive)
                if not any(info.filename == "manifest.json" for info in entries):
                    self.add_plain_zip(archive, entries)
                    return
                header = _json(config_backup._read_entry(archive, "manifest.json"))
                if header.get("format") == config_backup.BACKUP_FORMAT:
                    raise ResourceBundleError("这是配置备份，请到「设置 → 配置备份」恢复")
            parsed = parse_bundle(payload, allow_resources=True)
            declared_inputs = parsed.manifest.get("inputs", {})
            if not isinstance(declared_inputs, dict) or any(
                not isinstance(k, str)
                or not isinstance(v, list)
                or not all(isinstance(ref, str) for ref in v)
                for k, v in declared_inputs.items()
            ):
                raise ResourceBundleError("资源包的输入依赖声明格式非法")
            for key, refs in declared_inputs.items():
                self.inputs.setdefault(key, set()).update(refs)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                for name, _, _ in parsed.entries:
                    self.add(name, config_backup._read_entry(archive, name))
            if parsed.manifest.get("format") == RESOURCE_FORMAT:
                bundle_roots = parsed.manifest.get("roots")
                if (
                    not isinstance(bundle_roots, list)
                    or not bundle_roots
                    or not all(isinstance(k, str) for k in bundle_roots)
                ):
                    raise ResourceBundleError("资源包缺少有效的导出选择清单")
                self.roots.extend(bundle_roots)
            else:
                sid = parsed.manifest.get("strategy_id")
                self.roots.extend(
                    [f"strategy:{sid}"]
                    if sid
                    else [
                        f"strategy:{PurePosixPath(n).stem}"
                        for n, kind, _ in parsed.entries
                        if kind == "strategies" and not PurePosixPath(n).stem.startswith("_")
                    ]
                )
        except (zipfile.BadZipFile, KeyError, UnicodeDecodeError) as exc:
            raise ResourceBundleError(f"无法读取资源包 {filename}: {exc}") from exc

    def finalize(self) -> tuple[dict[str, dict], list[str]]:
        resources: dict[str, dict] = {}
        for name, payload in self.items.items():
            item = _metadata(name, payload)
            if item["key"] in resources:
                raise ResourceBundleError(f"同一资源 ID 出现在多个来源路径: {item['key']}")
            resources[item["key"]] = item
        for item in resources.values():
            item["inputs"] = sorted(self.inputs.get(item["key"], ()))
            if (
                item["kind"] == "settings"
                and (owner := resources.get(f"strategy:{item['id']}")) is not None
            ):
                item["label"] = f"{owner['label']}的调参"
        roots = list(dict.fromkeys(self.roots))
        if set(self.inputs) - set(resources):
            raise ResourceBundleError("输入依赖声明指向了包内不存在的资源")
        if not roots or any(
            k not in resources or resources[k]["kind"] not in ("strategy", "signal", "factor")
            for k in roots
        ):
            raise ResourceBundleError("导出选择清单指向了包内不存在的资源")
        return resources, roots


def _read_uploads(uploads: list[tuple[str, bytes]]) -> tuple[dict[str, dict], list[str]]:
    if not uploads or len(uploads) > config_backup.MAX_ENTRIES:
        raise ResourceBundleError("请选择资源文件 (数量不能超过 2000)")
    collector = _UploadCollector()
    if sum(len(payload) for _, payload in uploads) > config_backup.MAX_ARCHIVE_BYTES:
        raise ResourceBundleError("上传文件总量超过 128MB 上限")
    for filename, payload in uploads:
        if not payload:
            raise ResourceBundleError(f"空文件: {filename}")
        suffix = Path(filename).suffix.lower()
        if suffix == ".zip":
            collector.add_bundle(filename, payload)
        else:
            collector.add_loose(filename, payload)
    return collector.finalize()
