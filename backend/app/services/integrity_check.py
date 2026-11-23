"""显式日K自检报告与修补编排; 复用完整性检测与精准修补服务。

run_check 做深度扫描并把结果持久化为报告(用户确认凭证), run_repair 以报告为
凭证重新读当前数据确定修补范围, 报告一旦变化即拒绝执行, 避免修补范围漂移。
报告与备份经 ``app.state`` 门面落到 PG/blob, 不再写本地用户数据目录。
"""

from __future__ import annotations

import logging
import shutil
import uuid
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

from app.market_time import cn_now, cn_today
from app.services import daily_health, data_integrity
from app.state import (
    get_latest_integrity_report,
    save_integrity_backup,
    save_integrity_report,
)

logger = logging.getLogger(__name__)
ASSET_TABLES = {family: table for table, family in data_integrity.TABLE_FAMILY.items()}
ASSET_LABELS = {"stock": "股票", "etf": "ETF", "index": "指数"}
KIND_LABELS = {
    "missing": "日期缺失",
    "snapshot": "盘中快照未定版",
    "corrupt": "文件损坏或为空",
    "invalid": "日K数值异常",
    "unconfirmed": "缺失日期待核实",
    "recompute": "依赖指标待重算",
}


def _report_path(root: Path) -> Path:
    """兼容旧调用的虚拟路径；门面持久化后不再读写该文件。"""
    return root / "user_data" / "integrity_report.json"


def latest_report(root: Path) -> dict | None:
    """读取最近一次自检报告; 不存在或版本不符时返回 None。"""
    report = get_latest_integrity_report()
    return report if isinstance(report, dict) and report.get("version") == 1 else None


def _save(root: Path, report: dict) -> None:
    """把自检报告写入门面; root 仅为兼容旧签名而保留。"""
    save_integrity_report(report)


def _persist_backup(backup_id: str, backup_dir: Path) -> bool:
    """把修补产生的备份文件存入门面并返回是否存在备份, 随后清理临时目录。"""
    files: dict[str, bytes] = {}
    try:
        if backup_dir.exists():
            for path in sorted(backup_dir.rglob("*")):
                if path.is_file():
                    files[str(path.relative_to(backup_dir))] = path.read_bytes()
    except OSError as exc:
        logger.warning("读取修补备份失败 backup=%s: %s", backup_id, exc)
    if files:
        try:
            save_integrity_backup(backup_id, {"backup_id": backup_id}, files)
        except (OSError, ValueError) as exc:
            logger.warning("保存修补备份失败 backup=%s: %s", backup_id, exc)
    shutil.rmtree(backup_dir, ignore_errors=True)
    return bool(files)


def _repairable(issue: data_integrity.IntegrityIssue) -> bool:
    """判断异常是否可自动修补: 待核实与目录外文件不修。"""
    return issue.kind != "unconfirmed" and "目录以外" not in issue.reason


def _default_storage():
    from app.storage.runtime import get_storage

    runtime = get_storage()
    return runtime.store if runtime is not None else None


def _scan(root: Path, days: int, assets: list[str], end: date, progress=None, storage=None):
    if not 1 <= days <= 366 or not assets or not set(assets).issubset(ASSET_TABLES):
        raise ValueError("请选择资产类型和 1～366 天的检查范围")
    if end >= cn_today():
        raise ValueError("自检只检查已经结束的历史日期")

    def emit(cur, total):
        if progress:
            progress(
                "integrity_check",
                int(95 * cur / total),
                f"检查日K {cur}/{total}",
                stage_pct=int(100 * cur / total),
                skip_log=cur < total,
            )

    return data_integrity.scan_recent_integrity(
        root,
        today=end + timedelta(days=1),
        lookback_days=days,
        tables=[ASSET_TABLES[a] for a in assets],
        deep=True,
        on_progress=emit,
        storage=storage,
    )


def run_check(
    root: Path,
    *,
    lookback_days: int = 30,
    asset_types: list[str] | None = None,
    on_progress: Callable | None = None,
    end_date: date | None = None,
    storage=None,
) -> dict:
    """对历史交易日做一次深度自检并持久化报告, 返回含 issues 的报告字典。"""
    root = Path(root)
    assets = list(
        dict.fromkeys(
            asset_types
            if asset_types is not None
            else [data_integrity.TABLE_FAMILY[t] for t in data_integrity.active_daily_tables()]
        )
    )
    end = end_date or cn_today() - timedelta(days=1)
    if storage is None:
        storage = _default_storage()
    issues = _scan(root, lookback_days, assets, end, on_progress, storage=storage)
    report = _make_report(root, lookback_days, assets, end, issues, storage=storage)
    if on_progress:
        on_progress("done", 100, f"自检完成: {len(report['issues'])} 个待处理分区", stage_pct=100)
    return report


def _known_issues(root: Path, assets: list[str], end: date) -> list[data_integrity.IntegrityIssue]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for entry in daily_health.snapshot(root)["entries"]:
        if entry["asset_type"] in assets and entry["day"] <= end.isoformat():
            grouped.setdefault((entry["asset_type"], entry["day"]), []).append(entry)
    issues = []
    for (asset, day), entries in grouped.items():
        raw = any(e["needs_raw"] for e in entries)
        symbols = {e["symbol"] for e in entries}
        issues.append(
            data_integrity.IntegrityIssue(
                date.fromisoformat(day),
                ASSET_TABLES[asset],
                entries[0]["kind"] if raw else "recompute",
                symbols=tuple(sorted(symbols)) if "" not in symbols else (),
                reason=entries[0]["reason"]
                if raw
                else "日K已校验, 受影响指标或共享统计尚未完成重算",
            )
        )
    return issues


def _make_report(
    root: Path,
    lookback_days: int,
    assets: list[str],
    end: date,
    issues: list,
    storage=None,
) -> dict:
    daily_health.record_scan(
        root, issues, assets=assets, start=end - timedelta(days=lookback_days - 1), end=end
    )
    seen = {(i.table, i.day) for i in issues}
    issues = [
        *issues,
        *(i for i in _known_issues(root, assets, end) if (i.table, i.day) not in seen),
    ]
    if storage is not None:
        no_data = [a for a in assets if not storage.list_daily_dates(ASSET_TABLES[a], end=end)]
    else:
        no_data = [
            a
            for a in assets
            if not any(
                p.is_dir() and p.name[5:] <= end.isoformat()
                for p in (root / ASSET_TABLES[a]).glob("date=*")
            )
        ]
    rows = [
        {
            "asset_type": data_integrity.TABLE_FAMILY[i.table],
            "asset_label": ASSET_LABELS[data_integrity.TABLE_FAMILY[i.table]],
            "date": i.day.isoformat(),
            "kind": i.kind,
            "kind_label": KIND_LABELS[i.kind],
            "reason": i.reason or KIND_LABELS[i.kind],
            "repairable": _repairable(i),
            "symbols": list(i.symbols[:10]),
            "symbol_count": len(i.symbols),
        }
        for i in issues
    ]
    repairable_count = sum(r["repairable"] for r in rows)
    uncertain_count = sum(r["kind"] == "unconfirmed" for r in rows)
    status = (
        "issues"
        if len(rows) > uncertain_count
        else "uncertain"
        if rows
        else "empty"
        if len(no_data) == len(assets)
        else "clean"
    )
    report = {
        "version": 1,
        "id": uuid.uuid4().hex,
        "checked_at": cn_now().isoformat(),
        "start_date": (end - timedelta(days=lookback_days - 1)).isoformat(),
        "end_date": end.isoformat(),
        "lookback_days": lookback_days,
        "asset_types": assets,
        "status": status,
        "issues": rows,
        "repairable_count": repairable_count,
        "uncertain_count": uncertain_count,
        "no_data_assets": no_data,
        "data_revisions": {asset: daily_health.revision(root, asset) for asset in ASSET_LABELS},
    }
    _save(root, report)
    return report


def run_repair(repo, capset, report_id: str, on_progress: Callable | None = None) -> dict:
    """按已确认的报告执行修补; 报告变动则拒绝, 避免修补范围与用户确认不一致。"""
    from app.services.repair_daily import repair_integrity_issues

    root = Path(repo.store.data_dir)
    storage = getattr(repo, "storage", None)
    previous = latest_report(root)
    if previous is None or previous.get("id") != report_id:
        raise ValueError("自检报告已变化, 请重新自检后再修补")
    days, assets = previous["lookback_days"], previous["asset_types"]
    end = date.fromisoformat(previous["end_date"])
    # 报告只作确认凭证。实际修补范围重新读当前文件及持久化的待重算状态。
    raw = _scan(root, days, assets, end, on_progress, storage=storage)
    _make_report(root, days, assets, end, raw, storage=storage)
    # 更早自检已发现的异常也会影响当前窗口, 逐日重读以区分取数和仅需重算。
    for known in _known_issues(root, assets, end):
        if storage is not None:
            current = data_integrity.inspect_daily_frame(
                known.table,
                known.day,
                storage.read_daily(known.table, start=known.day, end=known.day),
            )
        else:
            current = data_integrity.inspect_daily_partition(root, known.table, known.day)
        found = [current] if current else []
        daily_health.record_scan(
            root,
            found,
            assets=[data_integrity.TABLE_FAMILY[known.table]],
            start=known.day,
            end=known.day,
        )
    issues = [i for i in _known_issues(root, assets, end) if _repairable(i)]
    if not issues:
        return run_check(
            root, lookback_days=days, asset_types=assets, end_date=end, storage=storage
        )
    backup_id = uuid.uuid4().hex
    backup_dir = root / "integrity_backups" / backup_id
    failure: BaseException | None = None
    try:
        repair_integrity_issues(
            repo,
            capset,
            issues,
            backup_dir=backup_dir,
            on_progress=on_progress,
        )
    except BaseException as exc:  # noqa: BLE001 — 取消也保留可重试状态, 保存报告后原样抛出
        failure = exc
    has_backup = _persist_backup(backup_id, backup_dir)

    report = run_check(root, lookback_days=days, asset_types=assets, end_date=end, storage=storage)
    if has_backup:
        report["backup_dir"] = f"integrity_backups/{backup_id}"
    if failure is not None:
        report["repair_error"] = str(failure)
    elif report["repairable_count"]:
        report["repair_error"] = (
            f"修补后仍有 {report['repairable_count']} 个问题, 请查看同步日志和数据源状态"
        )
    _save(root, report)
    if failure is not None:
        raise failure
    if report.get("repair_error"):
        raise RuntimeError(report["repair_error"])
    return report


def repair_known_data(repo, capset, on_progress=None) -> None:
    """常规管道在计算前处理已经发现的异常, 与手动修补共用同一入口。"""
    root = Path(repo.store.data_dir)
    storage = getattr(repo, "storage", None)
    active = {data_integrity.TABLE_FAMILY[t] for t in data_integrity.active_daily_tables()}
    assets = sorted({e["asset_type"] for e in daily_health.snapshot(root)["entries"]} & active)
    if assets:
        report = run_check(root, asset_types=assets, on_progress=on_progress, storage=storage)
        if report["repairable_count"]:
            run_repair(repo, capset, report["id"], on_progress=on_progress)
    for asset in active:
        daily_health.assert_usable(root, asset)
