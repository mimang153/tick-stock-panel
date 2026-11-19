import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import polars as pl

from app.services import minute_shared_pull as service


def _record(result, log, *items):
    """记录本次调用参数后返回既定结果(替代 append(x) or result 惯用法)。"""
    log.extend(items)
    return result


def test_overlapping_symbol_batches_share_pull_and_persistence(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    calls, writes = [], []
    store = SimpleNamespace(
        write_minute=lambda table, frame: writes.append(frame),
        latest_minute_datetime=lambda table: None,
    )
    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path), storage=store, _write_lock=threading.Lock()
    )

    def fetch(symbols, **kwargs):
        calls.append(symbols)
        if "a" in symbols:
            entered.set()
            assert release.wait(3)
        else:
            release.set()
        return pl.DataFrame({"symbol": symbols, "close": [10.0] * len(symbols)})

    monkeypatch.setattr(service.kline_sync, "sync_minute_batch", fetch)
    args: dict[str, Any] = {
        "asset_type": "stock",
        "start": datetime(2026, 11, 4, 9, 30),
        "end": datetime(2026, 11, 4, 15, 5),
        "batch_size": 200,
        "rpm": 48,
        "raw_basis": False,
    }
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(service.pull, repo, ["a", "b"], **args)
        assert entered.wait(3)
        second = pool.submit(service.pull, repo, ["b", "c"], **args)
        assert set(first.result(3)["symbol"]) == {"a", "b"}
        assert set(second.result(3)["symbol"]) == {"b", "c"}
    assert calls == [["a", "b"], ["c"]]
    assert sum(frame.height for frame in writes) == 3
    service.pull(repo, ["b"], **args)
    assert len(calls) == 2
    service.pull(repo, ["b"], **{**args, "asset_type": "etf"})
    assert len(calls) == 3


def test_empty_results_have_short_cooldown_and_do_not_invent_rows(tmp_path, monkeypatch):
    store = SimpleNamespace(
        write_minute=lambda table, frame: None, latest_minute_datetime=lambda table: None
    )
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path), storage=store)
    calls: list = []
    monkeypatch.setattr(
        service.kline_sync, "sync_minute_batch", lambda *a, **k: _record(pl.DataFrame(), calls, 1)
    )
    args: dict[str, Any] = {
        "asset_type": "stock",
        "start": datetime(2026, 11, 4, 9, 30),
        "end": datetime(2026, 11, 4, 15, 5),
        "batch_size": 200,
        "rpm": 48,
        "raw_basis": False,
    }
    assert service.pull(repo, ["a"], **args).is_empty()
    assert service.pull(repo, ["a"], **args).is_empty()
    assert calls == [1]
