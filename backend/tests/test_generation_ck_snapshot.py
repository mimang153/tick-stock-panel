"""RC-13: 在线 generation 读路径消费 CK，不依赖本地 marker。"""

from __future__ import annotations

from datetime import date, timedelta

from app.enriched_generation import (
    enriched_publication_incomplete,
    get_enriched_generation_snapshot,
)


def test_snapshot_uses_ck_history_without_marker(tmp_path, ck_runtime) -> None:
    """显式 storage 下从 CK generation 历史构造快照，本地无 marker 也可读。"""
    now = date(2026, 9, 1)
    changed = now - timedelta(days=10)
    g1 = ck_runtime.begin_generation("stock", "system", changed)
    ck_runtime.commit_generation("stock", g1, "system")
    g2 = ck_runtime.begin_generation("stock", "system", now)
    ck_runtime.commit_generation("stock", g2, "system")

    assert not (tmp_path / ".matrix_generation_stock.json").exists()
    snap = get_enriched_generation_snapshot(tmp_path, "stock")
    assert snap.generation == g2
    assert snap.through(now - timedelta(days=1)) == g1
    assert snap.through(now) == g2


def test_snapshot_full_rebuild_degrades_ranges(tmp_path, ck_runtime) -> None:
    """changed_from=None 的全量重建退化为 base=generation，所有窗口失效。"""
    gen = ck_runtime.begin_generation("stock", "system", None)
    ck_runtime.commit_generation("stock", gen, "system")
    snap = get_enriched_generation_snapshot(tmp_path, "stock")
    assert snap.generation == gen
    assert snap.base_generation == gen
    assert snap.ranges == ()
    assert snap.through(date(2000, 1, 1)) == gen


def test_incomplete_flags_only_stale_publishing(tmp_path, ck_runtime) -> None:
    """心跳新鲜的 publishing 不算 incomplete；僵死的才算。"""
    fresh = ck_runtime.begin_generation("stock", "system", None)
    ck_runtime.heartbeat_generation("stock", fresh, "system")
    assert enriched_publication_incomplete(tmp_path, "stock") is False

    # 把心跳回退到超时前：制造僵死发布。
    ck_runtime._client.execute(
        f"ALTER TABLE {ck_runtime._table('ck_enriched_generation')} "
        "UPDATE heartbeat_at = {ts:DateTime64(3)} WHERE generation_id = {g:String}",
        {"ts": __import__("datetime").datetime.now() - timedelta(seconds=1200), "g": fresh},
        settings={"mutations_sync": 2},
    )
    assert enriched_publication_incomplete(tmp_path, "stock") is True
