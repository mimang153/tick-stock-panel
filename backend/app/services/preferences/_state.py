"""用户偏好设置持久化 (DES-006)。

偏好存 PG user_preference (经 app.state 门面), 不再读写 preferences.json;
Redis 承载短缓存, 写入经事务 + version 递增保证一致性。
本模块保留 load/save/mutation 等稳定签名供上层调用。
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
from app.state import generation, get_preferences, preferences_version, save_preferences

logger = logging.getLogger("app.services.preferences")

# 兼容层: 历史调用方仍从 app.services.preferences 读取这两个进程内缓存变量;
# DES-006 后偏好真值由门面缓存管理, 这里保留定义但不承载数据。
_cache: dict | None = None
_cache_sig: tuple[int, int] | None = None


def _path() -> Path:
    """返回虚拟路径标识 (DES-006 后偏好不再落本地文件, 仅兼容展示与日志)。"""
    from app.config import settings

    return settings.data_dir / "user_data" / "preferences.json"


def _invalidate_cache() -> None:
    """兼容旧调用方: 清空历史进程内缓存变量 (真值由门面缓存管理)。"""
    _self._cache = None
    _self._cache_sig = None


def data_source_stamp() -> tuple:
    """配置版本戳: 偏好摘要 + 偏好版本 + 密钥 generation, 不读取本地文件。

    last_fetch_ms 由行情轮询周期性写入, 不应触发数据源缓存失效,
    故偏好摘要中剔除该字段; 版本与 generation 由门面提供跨进程一致性。
    用户状态门面未初始化（回测 worker 子进程/模块导入期）时返回稳定戳，
    单次 worker 运行内缓存不失效，符合子进程只读场景。
    """
    from app.state.pg import is_ready

    if not is_ready():
        return ("worker", "0", "0")
    data = _self.load()
    data.pop("last_fetch_ms", None)
    digest = hashlib.sha256(
        json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return (digest, preferences_version(), generation("secret"))


def load() -> dict:
    """读取偏好配置 (经 PG 门面)。返回深拷贝, 调用方可自由修改。"""
    return copy.deepcopy(get_preferences())


_SAVE_LOCK = threading.RLock()


@contextmanager
def mutation():
    """序列化多步偏好变更, 与 save 共用进程内锁。"""
    with _SAVE_LOCK:
        yield


def save(updates: dict) -> dict:
    """合并写入 (经 PG 门面, 事务内 read-modify-write)。返回新内容。"""
    with _SAVE_LOCK:
        return save_preferences(updates)


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
