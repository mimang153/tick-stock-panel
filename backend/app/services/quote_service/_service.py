"""原 app.services.quote_service 的 QuoteService 组装（mixin 组合）。"""

from __future__ import annotations

import threading
import time
from datetime import date
from typing import ClassVar

import polars as pl

from app.strategy.intraday_signals import IntradaySignalEvaluator

from ._common import (
    QuoteSubscriber,
)
from ._part1 import QuoteServiceMixin1
from ._part2 import QuoteServiceMixin2
from ._part3 import QuoteServiceMixin3


class QuoteService(QuoteServiceMixin1, QuoteServiceMixin2, QuoteServiceMixin3):
    """全局实时行情服务 — 单例。"""

    # 档位 → 最小轮询间隔 (秒) — TickFlow 档位限速保护, 仅实时源为 tickflow 时适用
    TIER_MIN_INTERVAL: ClassVar[dict[str, float]] = {
        "expert": 1.0,
        "pro": 3.0,
        "starter": 6.0,
        "free": 6.0,
    }
    # 插件/自定义源: 不受 TickFlow 档位保护约束, 通用下限 1s (默认间隔仍为 DEFAULT_INTERVAL)
    CUSTOM_PROVIDER_MIN_INTERVAL = 1.0
    DEFAULT_INTERVAL = 6.0
    MAX_INTERVAL = 60.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 生命周期锁: 串行化 start/stop/enable 的 check-then-act。两个并发开关
        # (双标签页 PUT /preferences + boot_check) 无锁交错会各起一条轮询线程
        # (双倍拉取配额 + 重复 SSE 广播)。锁内不做 IO (join/偏好读写都在锁外)。
        self._lifecycle_lock = threading.Lock()
        # 串行化行情拉取: 手动 POST /refresh 与后台轮询线程可能并发调用
        # _fetch_quotes, 两者同时写同一批 parquet/缓存会互相覆盖
        self._fetch_lock = threading.Lock()
        # 监控评估专用锁: 评估已移出 _fetch_lock (评估链含告警落盘/模拟盘逐账户
        # IO/webhook 判定, 留在取数锁内会阻塞下一轮拉取与手动刷新), 此锁保证
        # 两轮评估不并发 (原先由取数锁隐式串行)。
        self._evaluate_lock = threading.Lock()
        self._running = False
        self._enabled = False  # 全局开关 (持久化到 preferences)
        # 暂停态: 盘后管道/数据修正运行期间临时暂停取数, 防止与管道写同一批 parquet 竞态。
        # 与 _enabled 不同 — pause 不改 preferences、不 stop 线程, 仅让轮询循环跳过取数;
        # 进程重启后 _paused 归零, 从 preferences 恢复真实开关态, 无"假关闭"副作用。
        self._paused = False
        self._interval = self.DEFAULT_INTERVAL
        self._thread: threading.Thread | None = None
        self._repo = None  # 延迟注入, 避免循环导入
        # SSE 订阅者集合: 每个 /stream 连接一个 QuoteSubscriber, 事件广播到所有订阅者
        self._subscribers: set[QuoteSubscriber] = set()
        # A process identity prevents revision collisions after a server restart.
        self._stream_id = str(time.time_ns())
        self._quote_revision = 0
        self._strategy_revision = 0
        self._data_revisions = {"daily": 0, "minute": 0}
        self._strategy_monitor = None  # 延迟注入
        self._app_state = None  # 延迟注入 (FastAPI app.state)
        # 异动边缘规则上次评估时间戳 (秒)。异动快照历史部分有 60s 缓存,
        # 但每次构建仍有全市场循环, 轮询线程里限频到 30s 一次。
        self._abnormal_last_eval = 0.0

        # 拉取元信息 (给 SSE / status 用)
        self._fetch_time: float = 0.0  # perf_counter (用于计算 quote_age_ms)
        self._fetch_ms: float = 0.0  # 拉取耗时 (毫秒)
        # _fetched_at 持久化到 preferences: 进程重启后仍能显示"最后获取"时间,
        # 不因关闭开关/重启而归零 (数据页卡片常驻显示, 方便判断上次拉取时刻)。
        try:
            from app.services import preferences as _prefs

            self._fetched_at: float = float(_prefs.load().get("last_fetch_ms", 0.0))
        except Exception:  # noqa: BLE001
            self._fetched_at = 0.0  # 拉取完成的 Unix 时间戳 (毫秒)
        self._symbol_count: int = 0
        self._index_symbol_count: int = 0
        self._etf_symbol_count: int = 0
        self._index_quotes_cache: pl.DataFrame | None = None
        self._intraday_signal_evaluator = IntradaySignalEvaluator()
        self._intraday_signal_bucket: dict[str, str] = {}
        # 午休/收盘最终同步状态: 到边界后必须成功拉取一版行情, 再进入休盘态。
        self._final_sync_done: set[tuple[date, str]] = set()
        self._final_sync_failed: dict[tuple[date, str], str] = {}
        # 最近一次 final 定版拉取是否取得边界后快照 (None=非 final 拉取)
        self._last_final_confirmed: bool | None = None
        self._holiday_active = False  # 交易日探针当前是否判休市 (日志去重)
        # 轮询放量 (volume_delta 规则): 上一轮全市场股票快照的 (累计成交量[手], 累计成交额[元])。
        # 每轮全量快照后更新 (含非连续竞价时段, 保证 13:00 恢复时 prev 是 12:59
        # 而非 11:30); 跨交易日清空; cur < prev (数据源重置) 时丢弃该轮差值。
        self._prev_stock_volume: dict[str, tuple[float, float]] | None = None
        self._prev_volume_fetched_at: float | None = None  # epoch 毫秒
        self._prev_volume_date: date | None = None
        # 最近一轮的有效差值 (vol_delta[手], amt_delta[元]) - 仅连续竞价时段内、
        # prev 不早于本时段开盘时计算
        self._volume_delta: dict[str, tuple[float, float]] = {}
        self._volume_delta_span_s: float = 0.0
        self._etf_history_cache: tuple[tuple, pl.DataFrame] | None = None
        from app.services.quote_demand import QuoteDemand

        self._quote_demand = QuoteDemand()
