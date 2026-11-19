"""AI 财务分析报告持久化存储 — 薄封装 JsonReportStore, 固化文件名/上限/id 前缀。"""

from __future__ import annotations

from app.services.json_report_store import JsonReportStore

MAX_REPORTS = 20

_store = JsonReportStore("ai_reports.json", MAX_REPORTS, id_prefix="rpt")


def list_reports() -> list[dict]:
    """返回全部报告(按 created_at 降序)。"""
    return _store.list_reports()


def save_report(report: dict) -> dict:
    """新增一条报告并持久化, 自动补 id/created_at 并裁剪到上限。"""
    return _store.save_report(report)


def delete_report(report_id: str) -> bool:
    """删除指定报告。返回是否删除成功。"""
    return _store.delete_report(report_id)


def clear_reports() -> int:
    """清空全部报告。返回删除数量。"""
    return _store.clear_reports()
