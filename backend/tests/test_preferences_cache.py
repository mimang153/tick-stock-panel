"""preferences 缓存测试 (DES-006) — Redis 短缓存命中去重, 写入后立即可见。"""

from __future__ import annotations

import pytest

from app.services import preferences


def _record(result, log, *items):
    """记录本次调用参数后返回既定结果(替代 append(x) or result 惯用法)。"""
    log.extend(items)
    return result


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    path = tmp_path / "preferences.json"  # 虚拟路径标识, 不再落盘
    monkeypatch.setattr(preferences, "_path", lambda: path)
    preferences._invalidate_cache()
    yield path
    preferences._invalidate_cache()


def test_second_load_hits_cache_without_disk_parse(_isolated, monkeypatch):
    from app.state import preferences as state_preferences

    preferences.save({"realtime_quotes_enabled": True})
    calls: list = []
    real_fetch_one = state_preferences.fetch_one

    def _counting(*args, **kwargs):
        calls.append(1)
        return real_fetch_one(*args, **kwargs)

    monkeypatch.setattr(state_preferences, "fetch_one", _counting)

    assert preferences.load()["realtime_quotes_enabled"] is True
    assert preferences.load()["realtime_quotes_enabled"] is True
    assert calls == [], "Redis 短缓存命中时不应回查 PG"


def test_external_file_change_invalidates_cache(_isolated, monkeypatch):
    preferences.save({"realtime_quote_interval": 6.0})
    assert preferences.load()["realtime_quote_interval"] == 6.0

    # DES-006: 门面写入后 bump generation 并刷新 Redis 短缓存, 下一次读取即新值。
    preferences.save({"realtime_quote_interval": 3.0})

    assert preferences.load()["realtime_quote_interval"] == 3.0


def test_save_then_load_sees_merged_values(_isolated):
    preferences.save({"a": 1})
    out = preferences.save({"b": 2})
    assert out == {"a": 1, "b": 2}
    assert preferences.load() == {"a": 1, "b": 2}


def test_interval_setter_invalidates_cache(_isolated):
    preferences.set_realtime_quote_interval(2.0)
    assert preferences.load()["realtime_quote_interval"] == 2.0


def test_load_returns_copy_not_cached_object(_isolated):
    preferences.save({"k": [1, 2]})
    first = preferences.load()
    first["k"].append(3)
    first["extra"] = True
    again = preferences.load()
    assert again == {"k": [1, 2]}


def test_mining_schedule_defaults_are_disabled(_isolated):
    assert preferences.get_mining_schedule() == {
        "mining_schedule_enabled": False,
        "mining_schedule_weekday": 4,
        "mining_budget_profile": "balanced",
    }


def test_mining_schedule_invalid_stored_values_fail_closed(_isolated):
    preferences.save(
        {
            "mining_schedule_enabled": "false",
            "mining_schedule_weekday": True,
            "mining_budget_profile": None,
        }
    )

    assert preferences.get_mining_schedule() == {
        "mining_schedule_enabled": False,
        "mining_schedule_weekday": 4,
        "mining_budget_profile": "balanced",
    }


def test_mining_schedule_setter_saves_group_once(monkeypatch):
    calls: list = []
    monkeypatch.setattr(preferences, "save", lambda updates: _record(updates, calls, updates))

    result = preferences.set_mining_schedule(True, 2, "strict")

    assert result == {
        "mining_schedule_enabled": True,
        "mining_schedule_weekday": 2,
        "mining_budget_profile": "strict",
    }
    assert calls == [result]


@pytest.mark.parametrize("weekday", [-1, 5, True])
def test_mining_schedule_setter_rejects_invalid_weekday(weekday):
    with pytest.raises(ValueError, match="weekday"):
        preferences.set_mining_schedule(True, weekday, "balanced")


def test_mining_schedule_setter_rejects_invalid_profile():
    with pytest.raises(ValueError, match="profile"):
        preferences.set_mining_schedule(True, 4, "exploratory")


def test_external_push_channel_whitelists_include_custom_and_email(_isolated):
    assert preferences.set_webhook_default_channels(
        [
            "custom",
            "email",
            "custom",
            "unsupported",
        ]
    ) == ["custom", "email"]
    assert preferences.set_review_push_channels(
        [
            "email",
            "wecom",
            "unsupported",
        ]
    ) == ["email", "wecom"]


def test_email_smtp_config_falls_back_from_malformed_stored_values(_isolated):
    preferences.save(
        {
            "email_smtp_config": {
                "host": " smtp.example.com ",
                "port": "invalid",
                "security": "invalid",
                "to_addresses": "alerts@example.com",
            },
        }
    )

    assert preferences.get_email_smtp_config() == {
        "host": "smtp.example.com",
        "port": 465,
        "security": "ssl",
        "username": "",
        "from_address": "",
        "to_addresses": [],
    }
