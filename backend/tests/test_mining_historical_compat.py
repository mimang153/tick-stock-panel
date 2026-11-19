"""MN-05: 历史 run 目录/事件/摘要的读取兼容验证（默认 memory 文件版）。

这些用例固化"读取时归一化、不落盘迁移"的既有语义；失败即代表历史 run
的兼容契约被破坏。已知限制（如 artifacts 缺失导致 /result 500）单列用例标注，
不在本批修复——见 ISS-004 MN-05。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.api.mining import _project_run
from app.services.mining_jobs import MiningRunStore, MiningRunStoreError


def _store(tmp_path: Path) -> MiningRunStore:
    return MiningRunStore(tmp_path / "data")


def _write_legacy_run(
    tmp_path: Path,
    run_id: str,
    *,
    manifest: dict[str, Any],
    events: list[dict[str, Any]] | None = None,
    summary: dict[str, Any] | None = None,
    write_events: bool = True,
    write_summary: bool = True,
) -> Path:
    run_dir = _store(tmp_path).runs_root / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if write_events:
        (run_dir / "events.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in events or []), encoding="utf-8"
        )
    if write_summary:
        (run_dir / "summary.json").write_text(json.dumps(summary or {}), encoding="utf-8")
    return run_dir


def test_legacy_events_and_summary_read_with_normalized_manifest(tmp_path: Path) -> None:
    """老 run 的 manifest/events/summary 三件套在同一目录下都能读出。"""
    run_id = "legacy_full"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={
            "run_id": run_id,
            "status": "succeeded",
            "request": {"factor_names": ["quality"]},
            "data_fingerprint": "v0",
            "created_at": "2024-06-01T00:00:00+00:00",
        },
        events=[
            {
                "id": 1,
                "type": "queued",
                "timestamp": "2024-06-01T00:00:00+00:00",
                "payload": {"source": "scheduled"},
            },
            {
                "id": 2,
                "type": "running",
                "timestamp": "2024-06-01T00:00:05+00:00",
                "payload": {"status": "running"},
            },
            {
                "id": 3,
                "type": "succeeded",
                "timestamp": "2024-06-01T00:10:00+00:00",
                "payload": {"status": "succeeded"},
            },
        ],
        summary={"candidate_count": 4, "data_as_of": "2024-05-31"},
    )
    store = _store(tmp_path)

    manifest = store.get(run_id)
    assert manifest is not None
    assert manifest["status"] == "succeeded"
    assert manifest["artifacts"] == {}
    assert manifest["updated_at"] == "2024-06-01T00:00:00+00:00"

    events = store.read_events(run_id)
    assert [event["type"] for event in events] == ["queued", "running", "succeeded"]
    assert store.read_summary(run_id) == {"candidate_count": 4, "data_as_of": "2024-05-31"}


def test_legacy_run_projects_without_artifacts(tmp_path: Path) -> None:
    """API 投影只依赖 manifest/summary/events，不强求 artifacts 字段存在。"""
    run_id = "legacy_project"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={
            "run_id": run_id,
            "status": "succeeded",
            "request": {"factor_names": ["growth"]},
            "data_fingerprint": {"source": "auto"},
            "created_at": "2024-06-01T00:00:00+00:00",
        },
        events=[{"id": 1, "type": "queued", "payload": {}}],
        summary={"candidate_count": 2},
    )
    store = _store(tmp_path)
    manifest = store.get(run_id)
    assert manifest is not None

    projected = _project_run(store, manifest)
    assert projected["run_id"] == run_id
    assert projected["status"] == "succeeded"
    assert projected["source"] == "auto"
    assert projected["summary"]["candidate_count"] == 2
    assert "artifacts" not in projected


def test_legacy_missing_optional_files_degrade(tmp_path: Path) -> None:
    """老 run 若缺 events.jsonl / summary.json，分别降级为 [] / {}。"""
    run_id = "legacy_no_files"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={"run_id": run_id, "status": "failed", "error": "old failure"},
        write_events=False,
        write_summary=False,
    )
    store = _store(tmp_path)

    assert store.read_events(run_id) == []
    assert store.read_summary(run_id) == {}
    manifest = store.get(run_id)
    assert manifest is not None
    assert manifest["error"] == "old failure"


def test_legacy_runid_outside_pattern_is_invisible(tmp_path: Path) -> None:
    """已知限制：目录名不匹配当前 run_id 正则的老 run 在扫描中不可见。"""
    run_dir = _store(tmp_path).runs_root / "legacy.with.dots"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps({"run_id": "legacy.with.dots", "status": "succeeded"}), encoding="utf-8"
    )
    store = _store(tmp_path)

    assert store.list_runs() == []
    with pytest.raises(MiningRunStoreError):
        store.get("legacy.with.dots")


def test_legacy_malformed_event_fails_closed(tmp_path: Path) -> None:
    """已知限制：事件缺 id / JSON 损坏时 fail-closed，不静默跳过。"""
    run_id = "legacy_bad_event"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={"run_id": run_id, "status": "succeeded"},
        events=[{"type": "queued", "payload": {}}],  # 缺 id
    )
    store = _store(tmp_path)
    with pytest.raises(MiningRunStoreError):
        store.read_events(run_id)
