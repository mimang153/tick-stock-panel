"""MN-05: 历史 run 目录/事件/摘要的读取兼容验证（默认 memory 文件版）。

这些用例固化"读取时归一化、不落盘迁移"的既有语义；失败即代表历史 run
的兼容契约被破坏。写路径保持严格，读路径对历史数据做宽松兼容。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from app.api.mining import _project_run
from app.services.mining_jobs import ARTIFACT_NAMES, MiningRunStore, MiningRunStoreError


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


def test_legacy_standard_artifact_files_are_inferred_on_read(tmp_path: Path) -> None:
    """老 run 无 artifacts 注册表时，标准文件名仍应可被 /result 使用。"""
    store = _store(tmp_path)
    run_id = "legacy-artifacts"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={
            "run_id": run_id,
            "status": "succeeded",
            "request": {},
            "data_fingerprint": "legacy",
        },
    )
    run_dir = store.runs_root / run_id
    for name in ARTIFACT_NAMES:
        (run_dir / f"{name}.parquet").write_bytes(b"artifact")

    manifest = store.get(run_id)

    assert manifest is not None
    assert set(manifest["artifacts"]) == set(ARTIFACT_NAMES)


def test_legacy_manifest_keeps_run_id_alias_and_mtime_timestamps(tmp_path: Path) -> None:
    """老 run 的 manifest run_id 与目录名不一致时保留别名，时间由 mtime 兜底。"""
    store = _store(tmp_path)
    run_id = "legacy-2026-11-19"
    _write_legacy_run(
        tmp_path,
        run_id,
        manifest={"run_id": "old-external-id", "status": "succeeded", "request": {}},
    )

    manifest = store.get(run_id)

    assert manifest is not None
    assert manifest["run_id"] == run_id
    assert manifest["legacy_run_id"] == "old-external-id"
    assert datetime.fromisoformat(manifest["created_at"])
    assert datetime.fromisoformat(manifest["updated_at"])


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


def test_legacy_runid_outside_pattern_is_readable_but_not_writable(tmp_path: Path) -> None:
    """目录名含点号等历史字符的 run 在只读路径可见，但写路径仍拒绝。"""
    run_dir = _store(tmp_path).runs_root / "legacy.with.dots"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps({"run_id": "legacy.with.dots", "status": "succeeded"}), encoding="utf-8"
    )
    store = _store(tmp_path)

    assert [manifest["run_id"] for manifest in store.list_runs()] == ["legacy.with.dots"]
    manifest = store.get("legacy.with.dots")
    assert manifest is not None
    assert manifest["status"] == "succeeded"
    assert store.find_by_signature(manifest["run_signature"]) is not None
    with pytest.raises(MiningRunStoreError):
        store.append_event("legacy.with.dots", "queued")


def test_legacy_malformed_event_degrades_on_read(tmp_path: Path) -> None:
    """读路径对缺 id / JSON 损坏的事件降级跳过，写路径仍 fail-closed。"""
    run_id = "legacy_bad_event"
    run_dir = _write_legacy_run(
        tmp_path,
        run_id,
        manifest={"run_id": run_id, "status": "succeeded"},
        write_events=False,
    )
    (run_dir / "events.jsonl").write_text(
        '{"type": "queued", "payload": {}}\n'  # 缺 id
        "not-json\n"
        '{"id": 2, "type": "running", "payload": {}}\n',
        encoding="utf-8",
    )
    store = _store(tmp_path)

    assert [event["id"] for event in store.read_events(run_id)] == [2]
    with pytest.raises(MiningRunStoreError):
        store.append_event(run_id, "queued")


def test_legacy_malformed_summary_degrades_to_empty(tmp_path: Path) -> None:
    """读路径对损坏 summary 降级为 {}，不影响后续正常写入。"""
    run_id = "legacy_bad_summary"
    run_dir = _write_legacy_run(
        tmp_path,
        run_id,
        manifest={"run_id": run_id, "status": "succeeded"},
        write_summary=False,
    )
    (run_dir / "summary.json").write_text("not-json", encoding="utf-8")
    store = _store(tmp_path)

    assert store.read_summary(run_id) == {}
    assert store.write_summary(run_id, {"candidate_count": 1}) == {"candidate_count": 1}
    assert store.read_summary(run_id) == {"candidate_count": 1}
