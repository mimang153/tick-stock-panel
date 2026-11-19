"""用户偏好设置持久化。

存储位置: data/user_data/preferences.json
沿用 secrets_store 的 merge-write 模式,但不做 chmod 0600 (非敏感数据)。
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import threading
from contextlib import contextmanager
from pathlib import Path

import app.services.preferences as _self
from app.services.fs_utils import atomic_write_text

logger = logging.getLogger("app.services.preferences")

# 进程内缓存: 行情轮询线程一轮会调用 8~12 次 getter, 每次读盘+parse 是纯重复;
# 文件仅在用户改设置时变化, 以 (mtime_ns, size) 签名判断是否重读。
_cache: dict | None = None
_cache_sig: tuple[int, int] | None = None


def _path() -> Path:
    from app.config import settings

    p = settings.data_dir / "user_data" / "preferences.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _invalidate_cache() -> None:
    _self._cache = None
    _self._cache_sig = None


def data_source_stamp() -> tuple:
    """Opaque config version; routine last-fetch persistence must not invalidate pulls."""
    from app.config import settings

    data = _self.load()
    data.pop("last_fetch_ms", None)
    result: list[object] = [
        hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    ]
    for name in ("secrets.json",):
        path = settings.data_dir / "user_data" / name
        try:
            stat = path.stat()
            result.append((stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            result.append(None)
    return (str(settings.data_dir.resolve()), *result)


def load() -> dict:
    """读取 preferences.json (带 mtime 签名缓存)。返回深拷贝, 调用方可自由修改。"""
    p = _self._path()
    try:
        sig = (p.stat().st_mtime_ns, p.stat().st_size)
    except OSError:
        return {}
    if _self._cache is not None and sig == _self._cache_sig:
        return copy.deepcopy(_self._cache)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as e:  # noqa: BLE001 - 用户配置解析失败按空配置降级, 不阻断启动
        logger.warning("preferences.json malformed: %s", e)
        return {}
    _self._cache = data
    _self._cache_sig = sig
    return copy.deepcopy(_self._cache)


_SAVE_LOCK = threading.RLock()


@contextmanager
def mutation():
    """Serialize multi-step preference changes with ordinary merge-writes."""
    with _SAVE_LOCK:
        yield


def save(updates: dict) -> dict:
    """合并写入。返回新内容。

    锁内 read-modify-write: FastAPI 同步端点跑线程池, 并行 PUT 各自基于旧快照
    写盘会互相覆盖 (实测: 压缩总开关并行写分时/日K两键, 后写者把先写者覆盖)。
    """
    with _SAVE_LOCK:
        current = _self.load()
        current.update(updates)
        atomic_write_text(
            _self._path(),
            json.dumps(current, indent=2, ensure_ascii=False),
        )
        _self._invalidate_cache()
    return current


# 共享常量 (供各 part 引用)
_INTRADAY_REFRESH_INTERVAL_MIN = 3
_INTRADAY_REFRESH_INTERVAL_MAX = 60
_MONITOR_EXT_FIELDS_DEFAULT = {
    "concept": "ext_gn_ths.所属概念",
    "industry": "ext_hy_ths.所属同话顺行业",
}
_MINUTE_REFRESH_INTERVAL_MIN = 3
_MINUTE_REFRESH_INTERVAL_MAX = 120
_ALLOWED_DATA_PROVIDERS = {"tickflow"}
DATA_SOURCE_JOB_TIMEOUT_MIN_S = 60
_BACKTEST_RSS_LIMIT_GIB_DEFAULT = 6
_BACKTEST_RSS_LIMIT_GIB_MAX = 64
_REGIME_BATCH_DAYS_MIN = 25
_REGIME_BATCH_DAYS_MAX = 500
_REGIME_WARMUP_DAYS_MIN = 35
_REGIME_WARMUP_DAYS_MAX = 90
_MAINLINE_MAX_MEMBERS_MIN = 50
_MAINLINE_MAX_MEMBERS_MAX = 5000
_MAINLINE_MIN_MEMBERS_MIN = 1
_MAINLINE_MIN_MEMBERS_MAX = 200
_PIPELINE_PULL_KEYS = ("pipeline_pull_a_share", "pipeline_pull_etf", "pipeline_pull_index")
PUSH_CHANNELS = {"feishu", "wecom", "custom", "email"}
MARKET_RECAP_PROMPT_MAX_CHARS = 32_000
MINING_BUDGET_PROFILES = frozenset({"balanced", "strict"})
REVIEW_PUSH_MODES = frozenset({"auto", "manual"})
SSE_REFRESH_PAGES_DEFAULT = {
    "watchlist": True,
    "limit-ladder": False,
}
_EMAIL_SMTP_DEFAULTS = {
    "host": "",
    "port": 465,
    "security": "ssl",
    "username": "",
    "from_address": "",
    "to_addresses": [],
}
_DASHBOARD_LAYOUT_MAX_ITEMS = 40
_DASHBOARD_LAYOUT_MAX_JSON = 64 * 1024
_UI_PREFS_KEYS = (
    "alert_toast_enabled",
    "alert_toast_max",
    "alert_sound_enabled",
    "alert_sound",
    "voice_broadcast_enabled",
    "voice_broadcast_voice",
    "voice_broadcast_rate",
    "stock_external_template",
    "page_zoom",
    # 界面局部状态的跨端副本 (随配置备份迁移): 监控徽标/设置页收起/回测列表高度/导航形态
    "monitor_badge_enabled",
    "settings_nav_collapsed",
    "bt_strategy_list_maxh",
    "nav_state",
    "nav_groups",
    # AI 助手悬浮球显隐 (assistant.fab.enabled 的后端副本) — 同样受端口漂移影响
    "assistant_fab_enabled",
    # JSON 文本沿用标量契约，保存统一策略池的 ID 顺序与显式空选择。
    "strategy_pool",
    "theme",
)
_UI_PREFS_MAX_JSON = 16 * 1024
