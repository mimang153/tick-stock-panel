"""日K异常及待重算状态。与一次自检报告分离, 缩小检查范围不能解除旧异常。

同一记录只有在日K校验及依赖重算均完成后才移除; 重启/取消/实时刷新不会绕过它。
当前态经 ``app.state`` 门面落到 PG, 进程内按版本号做 lru 缓存, 热路径不扫 Parquet。
"""

from __future__ import annotations

import threading
import uuid
from datetime import date
from functools import lru_cache
from pathlib import Path

from app.enriched_generation import EnrichedGenerationUnavailableError
from app.state import (
    get_daily_integrity_snapshot,
    get_integrity_revision,
    put_daily_integrity_snapshot,
)

ASSET_LABELS = {"stock": "股票", "etf": "ETF", "index": "指数"}
_LOCK = threading.RLock()


class DailyIntegrityError(EnrichedGenerationUnavailableError):
    """已知原始日K异常或其依赖重算未完成, 与暂时正在发布区分。"""


def _path(root: Path) -> Path:
    """兼容旧调用的虚拟路径；门面持久化后不再读写该文件。"""
    return Path(root) / "user_data" / "daily_integrity_state.json"


def _revision_key() -> str:
    """聚合全部资产族的完整性版本号, 作为快照缓存键。"""
    return ":".join(f"{asset}={get_integrity_revision(asset)}" for asset in ASSET_LABELS)


@lru_cache(maxsize=32)
def _read(revision_key: str) -> dict:
    """按版本号读取并校验完整性快照; revision_key 只作缓存键不参与取值。"""
    value = get_daily_integrity_snapshot()
    try:
        if value["version"] != 1 or not isinstance(value["entries"], list):
            raise ValueError("unsupported integrity state")
        for entry in value["entries"]:
            if entry["asset_type"] not in ASSET_LABELS:
                raise ValueError("invalid asset")
            date.fromisoformat(entry["day"])
            if entry["symbol"] is not None and not isinstance(entry["symbol"], str):
                raise ValueError("invalid symbol")
            if (
                not isinstance(entry["needs_raw"], bool)
                or not isinstance(entry.get("needs_enriched", True), bool)
                or entry["kind"] not in {"missing", "snapshot", "corrupt", "invalid"}
                or not isinstance(entry["reason"], str)
            ):
                raise ValueError("invalid repair state")
        revisions = value.get("revisions", {})
        if not isinstance(revisions, dict) or any(
            not isinstance(v, str) for v in revisions.values()
        ):
            raise ValueError("invalid revision")
        return value
    except (ValueError, TypeError, KeyError) as exc:
        raise DailyIntegrityError("日K完整性状态无法读取, 请检查数据目录") from exc


def snapshot(root: Path) -> dict:
    """读取待重算状态快照; root 仅为兼容旧签名而保留。"""
    return _read(_revision_key())


def revision(root: Path, asset_type: str = "stock") -> str:
    """返回资产及其基准指数的合并修订号, 供缓存键判断依赖是否已重算。"""
    own = get_integrity_revision(asset_type)
    benchmark = get_integrity_revision("index") if asset_type in {"stock", "etf"} else "0"
    return f"{own}:{benchmark}" if benchmark != "0" else own


def _key(entry: dict):
    # 门面把 None 归一化为 ""（PG NOT NULL），服务层必须用同一口径做去重键。
    return entry["asset_type"], entry["day"], entry.get("symbol") or ""


def _save(root: Path, old: dict, entries: list[dict]) -> None:
    """按资产族更新版本号后整表写回门面; root 仅为兼容旧签名而保留。"""
    entries.sort(key=lambda e: (e["asset_type"], e["day"], e["symbol"] or ""))
    if entries == old["entries"]:
        return
    revisions = dict(old.get("revisions", {}))
    for asset in ASSET_LABELS:
        if [e for e in entries if e["asset_type"] == asset] != [
            e for e in old["entries"] if e["asset_type"] == asset
        ]:
            revisions[asset] = uuid.uuid4().hex
    put_daily_integrity_snapshot({"version": 1, "entries": entries, "revisions": revisions})


def _issue_entries(issues) -> dict:
    from app.services.data_integrity import TABLE_FAMILY

    found = {}
    for issue in issues:
        if issue.kind == "unconfirmed":
            continue
        for symbol in issue.symbols or ("",):
            entry = {
                "asset_type": TABLE_FAMILY[issue.table],
                "day": issue.day.isoformat(),
                "symbol": symbol,
                "needs_raw": True,
                "needs_enriched": True,
                "kind": issue.kind,
                "reason": issue.reason,
            }
            found[_key(entry)] = entry
    return found


def record_issues(root: Path, issues) -> None:
    """登记计算中新发现的问题, 不把未检查的日期/股票标记为已校验。"""
    found = _issue_entries(issues)
    if not found:
        return
    with _LOCK:
        old = snapshot(root)
        entries = [e for e in old["entries"] if _key(e) not in found]
        _save(root, old, [*entries, *found.values()])


def record_scan(root: Path, issues, *, assets: list[str], start: date, end: date) -> None:
    """登记一次自检发现的异常; 已覆盖窗口内不再异常的旧记录会被清除。"""
    found = _issue_entries(issues)
    with _LOCK:
        old = snapshot(root)
        entries = []
        for entry in old["entries"]:
            key = _key(entry)
            if key in found:
                continue
            entry = dict(entry)
            if (
                entry["asset_type"] in assets
                and start.isoformat() <= entry["day"] <= end.isoformat()
            ):
                entry["needs_raw"] = False
            entries.append(entry)
        entries.extend(found.values())
        _save(root, old, entries)


def complete(
    root: Path,
    asset_type: str,
    *,
    start: date,
    symbols: list[str] | None,
    days: set[date] | None = None,
) -> None:
    """标记指定范围已重算完成: 原始日K与依赖指标均就绪才移除异常记录。"""
    selected_days = {d.isoformat() for d in days} if days is not None else None
    with _LOCK:
        old = snapshot(root)
        entries = [
            e
            for e in old["entries"]
            if not (
                e["asset_type"] == asset_type
                and e["day"] >= start.isoformat()
                and (selected_days is None or e["day"] in selected_days)
                and (symbols is None or e["symbol"] in symbols)
            )
        ]
        _save(root, old, entries)


def mark_enriched_ready(
    root: Path,
    asset_type: str,
    *,
    start: date,
    symbols: list[str] | None,
    days: set[date] | None = None,
) -> None:
    """基础指标已经发布; 共享市场统计可读取它们, 对外计算仍等整个修补完成。"""
    selected_days = {d.isoformat() for d in days} if days is not None else None
    with _LOCK:
        old = snapshot(root)
        entries = []
        for item in old["entries"]:
            entry = dict(item)
            if (
                entry["asset_type"] == asset_type
                and entry["day"] >= start.isoformat()
                and (selected_days is None or entry["day"] in selected_days)
                and (symbols is None or entry["symbol"] in symbols)
            ):
                entry.update(needs_raw=False, needs_enriched=False)
            entries.append(entry)
        _save(root, old, entries)


def clear_assets(root: Path, assets: set[str]) -> None:
    """仅随明确的清除数据操作清理对应状态。"""
    with _LOCK:
        if assets == set(ASSET_LABELS):
            put_daily_integrity_snapshot(
                {
                    "version": 1,
                    "entries": [],
                    "revisions": {asset: uuid.uuid4().hex for asset in assets},
                }
            )
            return
        old = snapshot(root)
        _save(root, old, [e for e in old["entries"] if e["asset_type"] not in assets])


def assert_usable(
    root: Path,
    asset_type: str = "stock",
    *,
    symbols: list[str] | None = None,
    end: date | None = None,
    enriched_only: bool = False,
) -> None:
    """存在未重算完的日K异常时抛出 DailyIntegrityError, 阻止下游读到错数据。"""
    end = end or date.max
    for entry in snapshot(root)["entries"]:
        if enriched_only and not entry["needs_raw"] and not entry.get("needs_enriched", True):
            continue
        dependency = entry["asset_type"] == "index" and asset_type in {"stock", "etf"}
        if (
            (entry["asset_type"] == asset_type or dependency)
            and entry["day"] <= end.isoformat()
            and (dependency or symbols is None or not entry["symbol"] or entry["symbol"] in symbols)
        ):
            raise DailyIntegrityError(
                f"{ASSET_LABELS[entry['asset_type']]}日K在 {entry['day']} 有未完成修补或重算, "
                "请到 数据→自检/修补 完成后再计算"
            )
