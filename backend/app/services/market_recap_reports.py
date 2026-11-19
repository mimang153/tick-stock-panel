"""AI 大盘复盘报告持久化存储 — 薄封装 JsonReportStore, 复盘无 symbol。"""

from __future__ import annotations

from app.services.json_report_store import JsonReportStore

MAX_REPORTS = 20

_store = JsonReportStore(
    "ai_market_recaps.json",
    MAX_REPORTS,
    id_prefix="mkr",
    id_with_symbol=False,
)


def list_reports() -> list[dict]:
    """返回全部报告(按 created_at 降序)。"""
    return _store.list_reports()


def save_report(report: dict) -> dict:
    """新增一条报告并持久化。返回保存后的报告(含 id / created_at)。"""
    return _store.save_report(report)


def delete_report(report_id: str) -> bool:
    """删除指定报告。返回是否删除成功。"""
    return _store.delete_report(report_id)
