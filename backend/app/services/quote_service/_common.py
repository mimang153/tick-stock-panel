"""全局实时行情服务。

集中管理全市场行情拉取 + enriched 缓存，供盘中策略、自选等所有模块复用。

架构:
  - 后台线程轮询 TickFlow get_by_universes(["CN_Equity_A", "CN_ETF"]) + 核心指数按码拉取
    (自定义源走 provider.get_realtime() + 可选 get_realtime_indices() 指数补充)
  - 拉取行情 → 写 kline_daily (不复权) + 增量计算 enriched → 写盘 + 更新缓存
  - _enriched_cache 是唯一的盘中数据源 (OHLCV + 全套技术指标)
  - _live_agg_cache 是递推状态 (只加载一次, 盘中不变)

数据流 (每轮 ~15s):
  1. API 拉取 → raw_records (临时变量)
  2. raw_records → 写 kline_daily (不复权原始价格)
  3. raw_records → 更新 _enriched_cache 的 OHLCV
  4. 增量计算 enriched 指标 (~50ms)
  5. 写 kline_daily_enriched + 替换 _enriched_cache
  6. 通知 SSE

生命周期:
  - 服务启动时读取 preferences，若 enabled 则自动启动线程
  - 运行中可通过 API 切换开关
  - 关闭时停止线程
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import time as dt_time
from typing import Any

import app.services.quote_service as _self
from app.strategy.monitor import format_alert_quote

# 告警来源 → 中文标签 (webhook 标题 / 系统通知标题共用)
SOURCE_LABELS = {
    "strategy": "策略",
    "signal": "信号",
    "price": "价格",
    "market": "异动",
    "ladder": "连板梯队",
    "sector": "板块",
    "volume_delta": "放量",
    "abnormal": "异动",
    "date": "日期提醒",
    "paper": "模拟盘",
}

# final 定版确认容差: 快照时间戳允许早于边界 5s 内 (供应商时间戳精度不一)
_FINAL_CONFIRM_SLACK_MS = 5_000

# final 定版边界与重试窗口终点 (北京时间)。收盘窗口终点 15:30, 恰与盘后管道
# 启动同时: 管道运行期间轮询本就被暂停, 此后未确认的定版不再写盘, 当日分区
# 由管道按官方日线值级校正 —— 避免定版重试与权威重建互相覆盖。
_FINAL_BOUNDARY = {"morning_final": dt_time(11, 30), "close_final": dt_time(15, 0)}
_FINAL_DEADLINE = {"morning_final": dt_time(12, 10), "close_final": dt_time(15, 30)}


def _body_with_quote(body: str, ev: dict) -> str:
    """推送正文尾部补上触发时的现价/涨跌幅 (日期提醒无行情, 自然为空)。

    默认告警的 message 已由引擎拼过引语 (monitor._default_message), 这里仅在正文
    尚未带引语时追加, 避免「现价」出现两遍 (自定义 message 的规则则补上这一句)。
    """
    quote_tail = format_alert_quote(ev.get("price"), ev.get("change_pct"))
    if not quote_tail or body.endswith(quote_tail):
        return body
    return f"{body} · {quote_tail}"


logger = logging.getLogger("app.services.quote_service")

# Webhook(飞书等)投递专用线程池 —— 与行情轮询线程隔离。
# send_feishu 内置重试(最坏 ~3×5s 超时 + 退避), 若在 _poll_loop 上同步投递,
# webhook 慢/宕机会逐条累加, 拖垮整条实时行情+告警轮询。这里 fire-and-forget,
# 失败由 webhook_adapter 记 WARNING(可见), 但绝不阻塞热路径。
_WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feishu-webhook")

# 系统通知投递专用单线程池 —— 同因隔离。notify_adapter 底层是 subprocess
# (osascript/notify-send, 每条最长 5s 超时), 在 _poll_loop 上同步逐条执行的话,
# 告警风暴会让一轮监控评估阻塞数十秒, 拖慢行情拉取与 SSE 推送。
# 单 worker 保序且限制并发, fire-and-forget。
_SYSNOTIFY_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sys-notify")


def shutdown_delivery_executors() -> None:
    """应用关闭时放弃仍在排队的投递并关闭投递线程池 (lifespan shutdown 调用)。

    不调用则 daemon 工作线程随进程退出, 排队中的 webhook/系统通知静默丢失。
    """
    _self._WEBHOOK_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    _self._SYSNOTIFY_EXECUTOR.shutdown(wait=False, cancel_futures=True)


def _deliver_system_notify(title: str, body: str) -> None:
    """在线程池里投递一条系统通知; 异常静默 (与同步路径的容错口径一致)。"""
    from app.services import notify_adapter

    try:
        notify_adapter.notify(title, body)
    except Exception:  # noqa: BLE001
        _self.logger.debug("系统通知发送异常 (不影响告警主流程)")


class QuoteSubscriber:
    """一个 SSE 连接对应一个订阅者: 独立事件 + 独立队列。

    此前四个通道共用服务级 Event + pending 列表, pop 是「取走」语义:
    多客户端 (多标签页/多设备) 时告警只会被先醒来的连接消费, 其余永远
    收不到; 共享 Event 的 clear/wait 也存在互相吞信号的竞态。
    改为每连接独立订阅者后, 事件对所有客户端广播。
    """

    # 由 __init__/wait_async 延迟赋值：None=尚未绑定事件循环，非 None=已绑定。
    _async_loop: Any
    _async_event: Any

    def __init__(self, max_alerts: int = 1000, max_reviews: int = 200) -> None:
        self._event = threading.Event()
        self._async_loop = None
        self._async_event = None
        self._lock = threading.Lock()
        self._max_alerts = max_alerts
        self._max_reviews = max_reviews
        self._quote_updated = False
        self._strategy_results_updated = False
        self._depth_updated = False
        self._daily_integrity_updated: bool | None = None
        self._data_updated: set[str] = set()
        self._alerts: list[dict] = []
        self._reviews: list[str] = []

    # ── 消费侧 (SSE generator 线程) ──────────────────────
    def wait(self, timeout: float = 5.0) -> bool:
        """阻塞等待任一通道有新信号。"""
        return self._event.wait(timeout=timeout)

    def _notify(self) -> None:
        self._event.set()
        if self._async_loop is not None and not self._async_loop.is_closed():
            with suppress(RuntimeError):  # The event loop may close during shutdown.
                self._async_loop.call_soon_threadsafe(self._async_event.set)

    async def wait_async(self, timeout: float = 5.0) -> bool:
        # One asyncio event per connection avoids occupying a thread for every
        # idle SSE client. The threading event preserves existing sync consumers.
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._async_loop is not loop:
                self._async_loop, self._async_event = loop, asyncio.Event()
            self._async_event.clear()
            if self._event.is_set():
                return True
            event = self._async_event
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def pop(self) -> dict:
        """原子取走全部待推送内容并复位事件。"""
        with self._lock:
            out = {
                "quote_updated": self._quote_updated,
                "strategy_results_updated": self._strategy_results_updated,
                "depth_updated": self._depth_updated,
                "alerts": self._alerts,
                "reviews": self._reviews,
            }
            if self._daily_integrity_updated is not None:
                out["daily_integrity_updated"] = self._daily_integrity_updated
            if self._data_updated:
                out["data_updated"] = sorted(self._data_updated)
            self._data_updated.clear()
            self._daily_integrity_updated = None
            self._quote_updated = False
            self._strategy_results_updated = False
            self._depth_updated = False
            self._alerts = []
            self._reviews = []
            self._event.clear()
            return out

    # ── 生产侧 (行情轮询 / depth / 复盘线程) ─────────────
    def push_alerts(self, alerts: list[dict]) -> None:
        with self._lock:
            self._alerts.extend(alerts)
            if len(self._alerts) > self._max_alerts:  # 背压: 丢弃最旧
                self._alerts = self._alerts[-self._max_alerts :]
            self._notify()

    def push_review(self, event_json: str) -> None:
        with self._lock:
            self._reviews.append(event_json)
            if len(self._reviews) > self._max_reviews:
                self._reviews = self._reviews[-self._max_reviews :]
            self._notify()

    def clear_alerts(self) -> None:
        with self._lock:
            self._alerts = []
            if (
                not self._quote_updated
                and not self._strategy_results_updated
                and not self._depth_updated
                and self._daily_integrity_updated is None
                and not self._data_updated
                and not self._reviews
            ):
                self._event.clear()

    def notify_quote(self) -> None:
        with self._lock:
            self._quote_updated = True
            self._notify()

    def notify_daily_integrity(self, data_changed: bool) -> None:
        with self._lock:
            self._daily_integrity_updated = bool(self._daily_integrity_updated or data_changed)
            self._notify()

    def notify_data(self, kind: str) -> None:
        with self._lock:
            self._data_updated.add(kind)
            self._notify()

    def notify_strategy_results(self) -> None:
        with self._lock:
            self._strategy_results_updated = True
            self._notify()

    def notify_depth(self) -> None:
        with self._lock:
            self._depth_updated = True
            self._notify()


# 落盘节流间隔: last_fetch_ms 仅在进程重启后用于显示"最后获取时间"(运行中读内存值),
# 每 30s 持久化一次足够, 避免 expert 档每秒一轮的全量 preferences 重写磁盘。
_LAST_FETCH_WRITE_INTERVAL_MS = 30_000.0
_last_fetch_written_at_ms: float = 0.0


def _persist_last_fetch(fetched_at_ms: float) -> None:
    """把"最后获取"时间戳持久化到 preferences, 使进程重启后仍可显示。

    放在锁外调用 (IO); 失败不影响主流程 (内存值已更新, 下次 fetch 再写)。
    距上次成功落盘不足 30s 时跳过 (节流只影响落盘频率, 内存值不受影响)。
    """
    if (fetched_at_ms - _self._last_fetch_written_at_ms) < _LAST_FETCH_WRITE_INTERVAL_MS:
        return
    try:
        from app.services import preferences

        preferences.save({"last_fetch_ms": round(fetched_at_ms, 0)})
        _self._last_fetch_written_at_ms = fetched_at_ms
    except Exception as e:  # noqa: BLE001
        _self.logger.debug("last_fetch_ms 持久化失败 (不影响行情): %s", e)


def _monitor_name_map(repo) -> dict[str, str]:
    """监控回填用的 symbol → name 映射 (股票 + ETF + 指数, 股票优先)。

    走 repo.get_name_map() 的进程内 memo (三份 instruments 维表刷新时失效),
    避免每轮监控对 ~7000 行维表 iter_rows 重建。过滤空名称与旧行为一致。
    """
    return {s: n for s, n in repo.get_name_map().items() if n}
