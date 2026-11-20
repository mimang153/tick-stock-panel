"""API 路由 — Phase 0 仅 /health 与 /api/capabilities。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Query, Request

from app import __version__
from app.storage.runtime import cache_health, storage_health
from app.tickflow import client as tf_client
from app.tickflow.policy import detect_capabilities, tier_label

router = APIRouter()


def _read_cgroup_int(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text or text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _memory_snapshot() -> dict:
    """内存可观测性 (issue #516): 主进程 RSS + 系统可用 + cgroup v2 节流态。

    让外部看门狗在「服务活着但被 cgroup MemoryHigh 节流到假死」时有数可判,
    而不是只有 status=ok。
    """
    import psutil

    vm = psutil.virtual_memory()
    from app.services.memory_budget import memory_budget

    budget = memory_budget(vm)
    snap: dict[str, object] = {
        "rss_bytes": psutil.Process().memory_info().rss,
        "system_total_bytes": vm.total,
        "system_available_bytes": vm.available,
        "system_used_pct": vm.percent,
        "effective_total_bytes": budget.total_bytes,
        "effective_available_bytes": budget.available_bytes,
    }
    cg = Path("/sys/fs/cgroup")
    if _read_cgroup_int(cg / "memory.current") is not None:
        snap["cgroup_current_bytes"] = _read_cgroup_int(cg / "memory.current")
        snap["cgroup_high_bytes"] = _read_cgroup_int(cg / "memory.high")
        snap["cgroup_max_bytes"] = _read_cgroup_int(cg / "memory.max")
        events: dict[str, int] = {}
        try:
            for line in (cg / "memory.events").read_text(encoding="utf-8").splitlines():
                parts = line.split()
                if len(parts) == 2:
                    try:
                        events[parts[0]] = int(parts[1])
                    except ValueError:
                        continue
        except OSError:
            pass
        if events:
            snap["cgroup_events"] = events
    return snap


@router.get("/health")
@router.get("/api/health")
def health() -> dict:
    ck = storage_health()
    cache = cache_health()
    return {
        "status": "ok",
        "version": __version__,
        # 三态: none(无key/无效) / free(免费key) / api_key(付费档)
        "mode": tf_client.current_mode(),
        "memory": _memory_snapshot(),
        # 存储层状态: not_initialized(未接线) / ok / degraded(写入失败的批次>0 或 ping 失败)
        "clickhouse": ck,
        "ck_status": ck["status"],
        "cache": cache,
        "cache_backend": cache["backend"],
        "cache_status": cache["status"],
    }


@router.get("/api/capabilities")
def capabilities() -> dict:
    """前端用来决定哪些功能可用、哪些灰显。"""
    capset = detect_capabilities()
    return {
        "label": tier_label(),
        "capabilities": capset.to_dict(),
    }


@router.post("/api/capabilities/redetect")
def redetect(request: Request) -> dict:
    """用户在设置页"重新检测"按钮。"""
    capset = detect_capabilities(force=True)
    # 同步刷新 app.state 快照 (minute_refresh 等服务的门控读这里) 与财务调度器,
    # 与 settings.py 各探测路径一致 — 否则重检测后服务侧仍读旧 capset 被错误门控
    request.app.state.capabilities = capset
    from app.api.settings import _sync_financial_scheduler_caps

    _sync_financial_scheduler_caps(request.app.state, capset)
    return {
        "label": tier_label(),
        "capabilities": capset.to_dict(),
    }


@router.get("/api/adj-factor")
def adj_factor(
    symbol: str = Query(..., description="标的代码"),
    start_date: str | None = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="截止日期 YYYY-MM-DD"),
    etf: bool = Query(False, description="是否读取 ETF 因子表"),
) -> dict:
    """读取除权因子，供 S3 前复权口径逐行对账。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return {"symbol": symbol, "rows": []}
    rows = runtime.store.read_adj_factor(
        symbols=[symbol],
        start=date.fromisoformat(start_date) if start_date else None,
        end=date.fromisoformat(end_date) if end_date else None,
        etf=etf,
    )
    return {"symbol": symbol, "rows": rows.to_dicts()}


@router.get("/api/sync/status")
def sync_status(
    task: str = Query("kline_sync", description="同步任务名"),
) -> dict:
    """读取 CK sync_state，暴露中断恢复后的失败状态给运维与验收。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    state = runtime.sync_state.get(task) if runtime is not None else None
    return {
        "task": task,
        "status": state.status if state is not None else "unknown",
        "error": state.last_error if state is not None else None,
        "last_error": state.last_error if state is not None else None,
        "last_ok_at": state.last_ok_at if state is not None else None,
    }
