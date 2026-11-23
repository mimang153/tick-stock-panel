"""并发写偏好不得互相覆盖 (DES-006: PG 行锁 + Redis 短缓存)。

save_preferences 在 PG 事务内 SELECT ... FOR UPDATE 做 read-modify-write,
并行写入按行锁串行化, 不会用旧快照覆盖对方; 进程内 _SAVE_LOCK 继续保留。
"""

from __future__ import annotations

import threading

import pytest

from app.services import preferences

_OTHER_KEY = "realtime_quotes_enabled"


@pytest.fixture
def prefs_path(tmp_path, monkeypatch):
    path = tmp_path / "preferences.json"  # 虚拟路径标识, 不再落盘
    monkeypatch.setattr(preferences, "_path", lambda: path)
    preferences._invalidate_cache()
    yield path
    preferences._invalidate_cache()


def test_interval_setter_does_not_clobber_a_concurrent_save(prefs_path):
    """轮询间隔写入与另一个偏好写入并发时, 两个键都要留下。

    barrier 让两线程同时写入不同键, 由 PG 行锁保证 read-modify-write 串行化。
    """
    preferences.save({_OTHER_KEY: False})
    barrier = threading.Barrier(2)

    def _set_interval() -> None:
        barrier.wait()
        preferences.set_realtime_quote_interval(9.0)

    def _save_other() -> None:
        barrier.wait()
        preferences.save({_OTHER_KEY: True})

    interval_thread = threading.Thread(target=_set_interval, name="set-interval")
    other_thread = threading.Thread(target=_save_other, name="save-other")
    interval_thread.start()
    other_thread.start()
    interval_thread.join(10)
    other_thread.join(10)
    assert not interval_thread.is_alive() and not other_thread.is_alive()

    stored = preferences.load()
    assert stored["realtime_quote_interval"] == 9.0
    assert stored[_OTHER_KEY] is True, "并发的偏好写入被间隔 setter 的旧快照覆盖了"


def test_interval_setter_keeps_existing_keys(prefs_path):
    """顺序场景: 写间隔不能丢掉已有的其它偏好。"""
    preferences.save({_OTHER_KEY: True, "minute_sync_enabled": True})

    preferences.set_realtime_quote_interval(3.0)

    assert preferences.load() == {
        _OTHER_KEY: True,
        "minute_sync_enabled": True,
        "realtime_quote_interval": 3.0,
    }


def test_interval_setter_returns_value_and_refreshes_cache(prefs_path):
    """返回值与缓存失效行为保持不变。"""
    assert preferences.set_realtime_quote_interval(12.5) == 12.5
    assert preferences.get_realtime_quote_interval() == 12.5
