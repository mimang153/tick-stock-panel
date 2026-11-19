"""QuoteService mixin part 1（原单类方法分组）。"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import date
from typing import Any, ClassVar

import polars as pl

import app.services.quote_service as _self
from app.services.index_const import CORE_INDEX_SYMBOLS

from ._common import (
    QuoteSubscriber,
)


class QuoteServiceMixin1:
    """原 QuoteService 方法分组 1。"""

    # 以下属性由 QuoteService 组装类/其它 mixin 提供，仅作类型声明（不创建运行时属性）。
    _lifecycle_lock: Any
    _running: Any
    _save_enabled: Any
    _thread: Any
    _lock: Any
    _subscribers: Any
    _stream_id: Any
    _quote_revision: Any
    _strategy_revision: Any
    _data_revisions: Any
    CUSTOM_PROVIDER_MIN_INTERVAL: ClassVar[Any]
    TIER_MIN_INTERVAL: ClassVar[Any]
    DEFAULT_INTERVAL: ClassVar[Any]
    MAX_INTERVAL: ClassVar[Any]
    _index_quotes_cache: Any
    _quote_demand: Any
    _fetch_time: Any
    _market_phase: Any
    _final_sync_key: Any
    _symbol_count: Any
    _index_symbol_count: Any
    _etf_symbol_count: Any
    _is_continuous_trading: Any
    _should_poll_for_phase: Any
    _fetched_at: Any
    _final_boundary_ms: Any
    _should_fetch_for_phase: Any
    _last_final_confirmed: Any
    _past_final_deadline: Any
    _final_sync_done: Any
    _final_sync_failed: Any
    _fetch_lock: Any
    _last_quote_outcome: Any
    _evaluate_lock: Any
    _evaluate_monitors: Any
    _collect_monitor_index_symbols: Any
    _process_full_market_records: Any
    _build_daily: Any
    _etf_history_cache: Any

    def start(self, interval: float = 0.0) -> None:
        """启动后台行情轮询线程。"""
        if interval <= 0:
            from app.services import preferences

            interval = preferences.get_realtime_quote_interval()
        clamped = self._clamp_interval(interval)
        with self._lifecycle_lock:
            if self._running:
                return
            self._interval = clamped
            self._running = True
            self._enabled = True
            self._thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._thread.start()
        self._save_enabled(True)
        _self.logger.info("行情服务已启动, 轮询间隔 %.1fs", self._interval)

    def stop(self) -> None:
        """停止后台行情轮询线程。"""
        with self._lifecycle_lock:
            self._running = False
            self._enabled = False
            thread = self._thread
            self._thread = None
        if thread:
            thread.join(timeout=10)
        self._etf_history_cache = None
        # 此处不持久化关闭: lifespan shutdown (容器停止/重启) 也调用 stop,
        # 持久化 False 会让每次重启后实时行情都变关闭、需手动再开。
        # 持久化语义归 _self.disable() (用户主动关闭)。
        _self.logger.info("行情服务已停止")

    def enable(self) -> bool:
        """开启自动行情 (不立即启动线程，等下一个交易时段)。

        none 档无实时行情权限,拒绝开启并返回 False;
        free 档开启自选实时,starter+ 开启全市场实时。返回值表示是否真正开启。
        """
        if not self.is_realtime_allowed():
            _self.logger.warning("实时行情开启被拒:当前档位(none)无实时行情权限")
            return False
        self._enabled = True
        self._save_enabled(True)
        from app.services import preferences

        interval = self._clamp_interval(preferences.get_realtime_quote_interval())
        with self._lifecycle_lock:
            if not self._running:
                self._interval = interval
                self._running = True
                self._thread = threading.Thread(target=self._poll_loop, daemon=True)
                self._thread.start()
        _self.logger.info("行情服务已启用, 轮询间隔 %.1fs", self._interval)
        return True

    def disable(self) -> None:
        """关闭自动行情。"""
        self.stop()
        self._save_enabled(False)
        _self.logger.info("行情服务已关闭")

    # ================================================================
    # 临时暂停 (盘后管道/数据修正期间, 防止写盘竞态)
    # ================================================================

    def pause(self) -> None:
        """临时暂停行情轮询取数 (不关闭线程、不改 preferences)。

        用于盘后管道/数据修正运行期间, 防止实时行情覆写管道正在写的 parquet。
        与 _self.stop() 的区别: 线程继续存活但跳过 _fetch_quotes; preferences 开关态不变,
        管道结束调用 _self.resume() 即恢复。线程级检查, 即时生效, 无 join 等待。
        """
        self._paused = True
        _self.logger.info("行情轮询已临时暂停 (管道/修正运行中)")

    def resume(self) -> None:
        """恢复暂停的行情轮询取数 (对应 pause)。"""
        self._paused = False
        _self.logger.info("行情轮询已恢复")

    def is_paused(self) -> bool:
        """是否处于临时暂停态 (管道运行期间)。"""
        return self._paused

    @contextmanager
    def paused(self):
        """上下文管理器: 进入时暂停轮询取数, 退出时(含异常)自动恢复。

        供盘后管道/数据修正复用:
            with quote_service.paused():
                run_pipeline(...)
        无论正常结束还是异常/crash, finally 都会 _self.resume(除非进程直接被 kill)。
        """
        self.pause()
        try:
            yield
        finally:
            self.resume()

    def boot_check(self) -> None:
        """启动时检查 preferences，若 enabled 则自动启动。

        none 档无实时行情权限:即使 preferences 标记为 enabled,
        也不启动,并同步 preferences 为关闭(避免 UI 误显示已开启)。
        """
        from app.services import preferences

        if not self.is_realtime_allowed():
            if preferences.get_realtime_quotes_enabled():
                self._save_enabled(False)
            # 带生效源: 便于判断「该切哪个源」, 而不是只看到 TickFlow 档位
            from app.data_providers.capabilities import capability_gate

            _, label = capability_gate("realtime")
            _self.logger.info("实时行情未启动: %s 不可用", label)
            return
        if preferences.get_realtime_quotes_enabled():
            self.start()

    def set_repo(self, repo) -> None:
        """注入 KlineRepository, 用于实时落盘。"""
        self._repo = repo
        self._etf_history_cache = None

    def set_app_state(self, app_state) -> None:
        """注入 FastAPI app.state, 用于获取 strategy_monitor 等单例。"""
        self._app_state = app_state

    def set_interval(self, interval: float) -> float:
        """运行时更新轮询间隔（立即生效）。"""
        clamped = self._clamp_interval(interval)
        self._interval = clamped
        from app.services import preferences

        preferences.set_realtime_quote_interval(clamped)
        _self.logger.info("轮询间隔已更新为 %.1fs", clamped)
        return clamped

    def get_min_interval(self) -> float:
        """返回当前档位允许的最小间隔。"""
        return self._tier_min_interval()

    # ================================================================
    # SSE 订阅管理 — 每个 /stream 连接一个订阅者, 事件广播
    # ================================================================

    def subscribe(self) -> QuoteSubscriber:
        """注册一个 SSE 订阅者 (连接建立时调用)。"""
        sub = QuoteSubscriber()
        with self._lock:
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: QuoteSubscriber) -> None:
        """注销订阅者 (连接断开时调用)。"""
        with self._lock:
            self._subscribers.discard(sub)

    def _snapshot_subscribers(self) -> list[QuoteSubscriber]:
        with self._lock:
            return list(self._subscribers)

    def stream_versions(self) -> dict:
        with self._lock:
            return {
                "quote": f"{self._stream_id}:{self._quote_revision}",
                "strategy": f"{self._stream_id}:{self._strategy_revision}",
                "subscribers": len(self._subscribers),
            }

    def _broadcast_quote_updated(self) -> None:
        # 实时行情刷新后清空总览聚合缓存, 使看板 (overview-market) 在 SSE 触发的
        # 重取中拿到最新指数/聚合值。与 _broadcast 同时进行, 与侧栏 /intraday/indices
        # (无缓存, 直读实时缓存) 行为对齐, 避免看板落后于侧栏。
        # 延迟导入规避 services <-> api 层循环依赖。
        from app.api.overview import invalidate_overview_cache

        invalidate_overview_cache()
        with self._lock:
            self._quote_revision += 1
        for sub in self._snapshot_subscribers():
            sub.notify_quote()

    def notify_strategy_results_updated(self) -> None:
        """策略监控完成实时结果更新后调用，仅刷新策略页结果缓存。"""
        with self._lock:
            self._strategy_revision += 1
        for sub in self._snapshot_subscribers():
            sub.notify_strategy_results()

    def data_versions(self) -> dict[str, str]:
        with self._lock:
            return {
                kind: f"{self._stream_id}:{version}"
                for kind, version in self._data_revisions.items()
            }

    def notify_data_updated(self, kind: str) -> None:
        if kind not in {"daily", "minute"}:
            raise ValueError("unknown data kind")
        with self._lock:
            self._data_revisions[kind] += 1
        for sub in self._snapshot_subscribers():
            sub.notify_data(kind)

    def notify_daily_integrity_updated(self, assets: list[str], *, data_changed: bool) -> None:
        if data_changed:
            engine = getattr(self._app_state, "monitor_engine", None) if self._app_state else None
            affected = set(assets)
            if "index" in affected:
                affected.update({"stock", "etf"})
            if engine is not None:
                for asset in affected:
                    engine.invalidate_daily_asset(asset)
            from app.api.overview import invalidate_overview_cache
            from app.api.regime import invalidate_regime_cache

            invalidate_overview_cache()
            invalidate_regime_cache()
        for sub in self._snapshot_subscribers():
            sub.notify_daily_integrity(data_changed)

    def notify_depth_updated(self) -> None:
        """五档盘口修正完成后调用: 通知 SSE 推送 depth_updated, 触发连板梯队刷新。

        与行情/告警通道独立 — 只刷新连板梯队, 不连带刷新 watchlist 等。
        """
        for sub in self._snapshot_subscribers():
            sub.notify_depth()

    def _broadcast_alerts(self, alerts: list[dict]) -> None:
        for sub in self._snapshot_subscribers():
            sub.push_alerts(alerts)

    def push_alerts(self, alerts: list[dict]) -> None:
        self._broadcast_alerts(alerts)

    def clear_pending_alerts(self) -> None:
        for sub in self._snapshot_subscribers():
            sub.clear_alerts()

    def push_review_event(self, event_json: str) -> None:
        """广播一条复盘进度事件(JSON 字符串), 唤醒所有 SSE generator。

        事件格式与 recap_market_stream 的产出一致(meta/delta/error/done),
        前端 reviewStore 直接消费。背压在订阅者队列内做 (丢弃最旧)。
        """
        for sub in self._snapshot_subscribers():
            sub.push_review(event_json)

    # ================================================================
    # 档位感知间隔限制
    # ================================================================

    @staticmethod
    def _current_tier() -> str:
        """获取当前档位名（小写）。"""
        from app.tickflow.policy import tier_label

        return tier_label().split()[0].split("+")[0].strip().lower()

    @classmethod
    def realtime_mode(cls) -> str:
        """当前实时行情模式: none / full_market。

        TickFlow 免费档不再提供"自选前 5 只"降级实时(自定义源 fuyao 的全市场
        快照已全面覆盖且免费); TickFlow 免费档 = 无实时, 接入自定义实时源
        (如 fuyao)或升级 TickFlow 后恢复全市场模式。
        """
        from app.services import preferences

        if preferences.get_realtime_data_provider() != "tickflow":
            return "full_market"
        tier = cls._current_tier()
        if tier in ("none", "free"):
            return "none"
        return "full_market"

    @classmethod
    def is_realtime_allowed(cls) -> bool:
        """当前档位是否允许使用实时行情。"""
        return cls.realtime_mode() != "none"

    @classmethod
    def _tier_min_interval(cls) -> float:
        # 实时源路由到插件/自定义源时, TickFlow 档位限速不适用 (中立能力原则):
        # 下限放宽到通用 1s, 默认/已保存间隔不变
        from app.services import preferences

        if preferences.get_realtime_data_provider() != "tickflow":
            return cls.CUSTOM_PROVIDER_MIN_INTERVAL
        tier = cls._current_tier()
        return cls.TIER_MIN_INTERVAL.get(tier, cls.DEFAULT_INTERVAL)

    def _clamp_interval(self, interval: float) -> float:
        return max(self._tier_min_interval(), min(self.MAX_INTERVAL, interval))

    # ================================================================
    # 行情数据访问
    # ================================================================

    def get_enriched_today(self) -> tuple[pl.DataFrame, date | None]:
        """返回今天 enriched 数据 + 日期 (线程安全)。

        所有页面统一通过此方法获取实时行情 + 技术指标。
        """
        if not self._repo:
            return pl.DataFrame(), None
        return self._repo.get_enriched_latest()

    def get_quotes_compat(self) -> pl.DataFrame:
        """兼容接口: 返回行情 DataFrame (用于盘中策略等需要 last_price/prev_close 的场景)。

        从 _enriched_cache 取 today 的数据, 只选行情基础列, 补上 last_price 别名。
        不返回指标列, 避免 JOIN live_agg 时列名冲突。
        """
        df, _ = self.get_enriched_today()
        if df.is_empty():
            return df

        # 只取盘中策略需要的行情基础列
        keep = [
            c
            for c in [
                "symbol",
                "close",
                "open",
                "high",
                "low",
                "volume",
                "amount",
                "prev_close",
                "change_pct",
                "change_amount",
                "amplitude",
                "turnover_rate",
            ]
            if c in df.columns
        ]
        df = df.select(keep)

        # enriched 的 close 等价于 last_price
        if "close" in df.columns and "last_price" not in df.columns:
            df = df.with_columns(pl.col("close").alias("last_price"))
        return df

    def get_index_quotes(self, symbols: list[str] | None = None) -> pl.DataFrame:
        """返回实时指数行情缓存。不会触发 TickFlow 请求。"""
        with self._lock:
            df = (
                self._index_quotes_cache.clone()
                if self._index_quotes_cache is not None
                else pl.DataFrame()
            )
        if df.is_empty():
            return df
        if symbols:
            return df.filter(pl.col("symbol").is_in(symbols))
        return df

    def subscribe_quotes(self, client_id: str, symbols: list[str]) -> None:
        """Register demand only; HTTP readers never start their own market fetch."""
        self._quote_demand.subscribe(client_id, symbols)

    def unsubscribe_quotes(self, client_id: str) -> None:
        self._quote_demand.unsubscribe(client_id)

    def _wanted_quotes(self) -> dict[str, set[str]]:
        result: dict[str, set[str]] = {"stock": set(), "etf": set(), "index": set()}
        if not self._repo or not getattr(self._repo, "store", None):
            return result
        wanted = self._quote_demand.symbols(self._repo.store.data_dir)
        indices = self._repo.get_index_symbol_set()
        etf = self._repo.get_etf_instruments()
        etfs = (
            set(etf["symbol"].to_list())
            if not etf.is_empty() and "symbol" in etf.columns
            else set()
        )
        for symbol in wanted:
            asset = "etf" if symbol in etfs else "index" if symbol in indices else "stock"
            result[asset].add(symbol)
        return result

    def status(self) -> dict:
        """返回行情服务状态。"""
        age = (time.perf_counter() - self._fetch_time) * 1000 if self._fetch_time else -1
        mode = self.realtime_mode()
        phase = self._market_phase()
        final_key = self._final_sync_key(phase)
        final_done = bool(final_key and final_key in self._final_sync_done)
        final_failed = self._final_sync_failed.get(final_key) if final_key else None
        return {
            "enabled": self._enabled,
            "running": self._running,
            "paused": self._paused,
            "mode": mode,
            "realtime_allowed": mode != "none",
            "interval_s": self._interval,
            "symbol_count": self._symbol_count,
            "index_symbol_count": self._index_symbol_count,
            "etf_symbol_count": self._etf_symbol_count,
            "quote_age_ms": round(age, 0) if age >= 0 else None,
            # 交易时段 = 连续竞价; polling_window 另行返回,避免午休/收盘缓冲误显示为交易中。
            "is_trading_hours": self._is_continuous_trading(),
            "is_polling_window": self._should_poll_for_phase(phase),
            "market_phase": phase,
            "final_sync_done": final_done,
            "final_sync_failed": final_failed,
            "last_fetch_ms": round(self._fetched_at, 0) if self._fetched_at else None,
        }

    def refresh(self) -> dict:
        """手动触发一次行情拉取。

        午休/收盘定版阶段同样走边界确认: 避免盘后手动刷新把竞价前的陈旧收盘价
        重新写回当日分区, 覆盖盘后管道按官方日线重建的结果。
        """
        phase = self._market_phase()
        is_final = phase in {"morning_final", "close_final"}
        self._fetch_quotes(
            final=is_final,
            final_boundary_ms=self._final_boundary_ms(phase) if is_final else None,
        )
        return self.status()

    # ================================================================
    # 后台轮询
    # ================================================================

    def _poll_loop(self) -> None:
        # 线程身份检查: _self.stop() 清标志 → 释放锁 → join 前, 另一线程 _self.start() 可能把
        # _running 置回 True, 老线程只看共享标志会永不退出 (join 超时 + 双轮询)。
        # _thread 指向新线程/None 时, 老循环在下一轮检查点自行退出。
        while self._running and self._enabled and self._thread is threading.current_thread():
            try:
                # 管道/数据修正运行期间临时暂停取数, 防止与管道写同一批 parquet 竞态。
                # 线程继续存活 + 分片 sleep, _self.resume() 后即时恢复, 无需重启线程。
                if not self._paused:
                    phase = self._market_phase()
                    if self._should_fetch_for_phase(phase):
                        is_final = phase in {"morning_final", "close_final"}
                        ok = self._fetch_quotes(
                            final=is_final,
                            final_boundary_ms=self._final_boundary_ms(phase),
                        )
                        if is_final:
                            key = self._final_sync_key(phase)
                            label = "午休" if phase == "morning_final" else "收盘"
                            if key and ok and self._last_final_confirmed:
                                self._final_sync_done.add(key)
                                self._final_sync_failed.pop(key, None)
                                _self.logger.info(
                                    "%s 最终行情同步完成 (快照时间戳已达边界), 进入休盘态", label
                                )
                            elif key and self._past_final_deadline(phase):
                                # 重试窗口结束仍未取得边界后快照: 接受现状停止轮询。
                                # 实测有实时源收盘后长期返回竞价前旧价 (快照时间戳可信但价格不更新),
                                # 此时盲目落盘只会固化旧价 —— 交由 15:30 盘后管道按官方日线校正。
                                self._final_sync_done.add(key)
                                self._final_sync_failed[key] = (
                                    "fetch_failed" if not ok else "unconfirmed_snapshot"
                                )
                                _self.logger.warning(
                                    "%s 定版窗口结束仍未取得边界后快照 (%s), 停止轮询; "
                                    "当日分区由盘后管道按官方日线值级校正",
                                    label,
                                    "拉取失败" if not ok else "快照未确认",
                                )
                            elif key:
                                self._final_sync_failed[key] = (
                                    "fetch_failed" if not ok else "unconfirmed_snapshot"
                                )
                                if not ok:
                                    _self.logger.warning("%s 最终行情同步失败, 将继续重试", label)
                    else:
                        _self.logger.debug("非轮询阶段(%s), 跳过行情轮询", phase)
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("行情轮询异常: %s", e)

            waited = 0.0
            while self._running and self._enabled and waited < self._interval:
                time.sleep(0.5)
                waited += 0.5

    def _cleanup_day_scoped_state(self) -> None:
        """按日清理 final 同步状态 (键为 (date, phase), 严格无界累积)。

        旧日期键不再被 _final_sync_key 命中, 新交易日首次拉取时清掉;
        放在 _fetch_quotes 入口而非 _update_volume_delta, 避免依赖最小实例
        (测试用 __new__ 构造) 不具备的属性。
        """
        today = _self.cn_today()
        if self._final_sync_done and any(k[0] != today for k in self._final_sync_done):
            self._final_sync_done = {k for k in self._final_sync_done if k[0] == today}
        if self._final_sync_failed and any(k[0] != today for k in self._final_sync_failed):
            self._final_sync_failed = {
                k: v for k, v in self._final_sync_failed.items() if k[0] == today
            }

    def _fetch_quotes(self, *, final: bool = False, final_boundary_ms: int | None = None) -> bool:
        """拉取行情。加锁串行化 (后台轮询 vs 手动 refresh)。返回本轮是否成功更新。

        final_boundary_ms: final 定版的边界时间戳 (ms)。传入时快照时间戳未达边界
        的本轮不落盘 (见 _process_full_market_records)。
        """
        self._cleanup_day_scoped_state()
        with self._fetch_lock:
            if self._paused:
                return False
            from app.services import preferences

            demand = tuple((a, tuple(sorted(s))) for a, s in self._wanted_quotes().items())
            key = (final, final_boundary_ms, preferences.data_source_stamp(), demand)
            completed = getattr(self, "_last_quote_completed", 0.0)
            if getattr(
                self, "_last_quote_key", None
            ) == key and time.perf_counter() - completed < min(2.0, self._interval):
                return self._last_quote_outcome
            before = self._fetched_at
            if final:
                _self.logger.info("最终行情同步开始")
            snapshot = self._fetch_full_market_quotes(final_boundary_ms=final_boundary_ms)
            updated = self._fetched_at > before
            self._last_quote_key = key
            self._last_quote_outcome = updated
            self._last_quote_completed = time.perf_counter()
        # 监控评估移出取数锁执行: 评估输入是本轮快照 (daily_df/quote_extra),
        # 离锁执行结果不变; 时序不变式 (enriched 替换后才评估) 仍由调用顺序保证。
        if snapshot is not None:
            with self._evaluate_lock:
                self._evaluate_monitors(*snapshot)
        return updated

    def _fetch_custom_realtime(self, provider_name, demand, source_stamp, final_boundary_ms):
        """自定义源实时行情分支: 拉取 + 订阅/指数补拉 + 发布; 失败返回 None。"""
        from app.data_providers import custom as custom_sources
        from app.services import preferences

        if not custom_sources.provider_has_dataset(provider_name, "realtime"):
            _self.logger.warning("所选实时数据源不可用或未声明 realtime，保留已有行情")
            return None
        try:
            t0 = time.perf_counter()
            now_ts = time.perf_counter()
            provider = custom_sources.get_provider(provider_name)
            records = provider.get_realtime()
            partial_assets = set()
            fetch_symbols = getattr(provider, "get_realtime_symbols", None)
            if callable(fetch_symbols):
                received = {r.get("symbol") for r in records}
                for asset in ("stock", "etf"):
                    missing = sorted(demand[asset] - received)
                    if missing:
                        try:
                            supplement = fetch_symbols(missing, asset_type=asset)
                            if supplement:
                                records = records + supplement
                                partial_assets.add(asset)
                        except Exception as exc:  # noqa: BLE001 -- no cross-source fallback
                            _self.logger.warning("自定义源订阅行情补拉失败，保留已有数据: %s", exc)
            # 指数补充: A 股快照通常不含指数。插件可选实现
            # get_realtime_indices(symbols) 用独立端点补拉 (如 fuyao 指数快照);
            # 未实现的源指数缓存为空, 由日K兜底接管。
            replace_index_cache = True
            fetch_indices = getattr(provider, "get_realtime_indices", None)
            if callable(fetch_indices):
                # 偏离值基准指数 (科创50/创业板综指等) 一并拉取, 供盘中
                # attach_deviation_columns_today 实时外推; 展示层仍按核心
                # 四只过滤, 多拉的指数不进侧栏。
                from app.indicators.pipeline import BENCHMARK_INDEX_SYMBOLS

                wanted = sorted(
                    set(CORE_INDEX_SYMBOLS)
                    | BENCHMARK_INDEX_SYMBOLS
                    | self._collect_monitor_index_symbols()
                    | demand["index"]
                )
                try:
                    fetched_indices = fetch_indices(wanted)
                    if fetched_indices is None:
                        replace_index_cache = False
                    else:
                        records = records + fetched_indices
                except Exception as e:  # noqa: BLE001
                    _self.logger.warning("自定义源指数行情拉取失败: %s", e)
                    replace_index_cache = False
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("自定义实时行情拉取失败: %s", e)
            return
        if preferences.data_source_stamp() != source_stamp:
            _self.logger.info("行情数据源配置已变化，跳过旧轮次发布")
            return None
        return self._process_full_market_records(
            records,
            t0=t0,
            now_ts=now_ts,
            replace_index_cache=replace_index_cache,
            final_boundary_ms=final_boundary_ms,
            **({"partial_assets": partial_assets} if partial_assets else {}),
        )

    def _tickflow_fetch_universes(self, tf):
        """全市场 universe 拉取; 返回合并后的 resp。"""
        from app.services import preferences

        all_etf_symbols = set()
        if self._repo:
            etf_inst = self._repo.get_etf_instruments()
            if not etf_inst.is_empty() and "symbol" in etf_inst.columns:
                all_etf_symbols = set(etf_inst["symbol"].cast(pl.Utf8).to_list())
        universes: list[str] = []
        if preferences.get_realtime_pull_stock():
            universes.append("CN_Equity_A")
        if preferences.get_realtime_pull_etf() and all_etf_symbols:
            universes.append("CN_ETF")
        resp: list[Any] = []
        if universes:
            _u0 = time.perf_counter()
            _self.logger.info("拉取全市场行情 (universes=%s, SDK超时=30s×重试3)", universes)
            try:
                resp.extend(tf.quotes.get_by_universes(universes=universes) or [])
            except Exception as exc:  # noqa: BLE001 -- preserve targeted demand on universe failure
                _self.logger.warning("全市场行情获取失败，本轮尝试已订阅标的: %s", exc)
            _self.logger.info(
                "全市场行情拉取完成: %d 条 (%.2fs)", len(resp), time.perf_counter() - _u0
            )
        return resp

    def _tickflow_fetch_symbols(self, tf, resp, core_index_symbols, monitor_index_symbols, demand):
        """按码补拉核心指数 + 订阅标的 + 缺口股票/ETF; 返回 (resp, replace_index_cache, partial_assets)。"""
        from app.indicators.pipeline import BENCHMARK_INDEX_SYMBOLS
        from app.tickflow.capabilities import Cap, CapabilitySet
        from app.tickflow.rate_limits import chunked, resolve_limit, sleep_between_batches

        base_indices = core_index_symbols | BENCHMARK_INDEX_SYMBOLS | monitor_index_symbols
        _core_syms = sorted(base_indices | demand["index"])
        partial_assets = set()
        received = {q.get("symbol") for q in resp}
        for asset in ("stock", "etf"):
            missing = demand[asset] - received
            _core_syms.extend(sorted(missing))
            if missing:
                partial_assets.add(asset)
        _core_syms = sorted(set(_core_syms))
        replace_index_cache = True
        if _core_syms:
            capset = getattr(self._app_state, "capabilities", None) if self._app_state else None
            limit = resolve_limit(capset or CapabilitySet(), Cap.QUOTE_BATCH, default_batch=200)
            _i0 = time.perf_counter()
            failed = False
            batches = chunked(_core_syms, limit.batch)
            for i, batch in enumerate(batches):
                sleep_between_batches(i, limit.rpm)
                try:
                    resp.extend(tf.quotes.get(symbols=batch) or [])
                except Exception as exc:  # noqa: BLE001 -- isolate supplementary failures
                    _self.logger.warning("订阅行情批次拉取失败，保留已有数据: %s", exc)
                    failed = True
            if failed:
                received = {q.get("symbol") for q in resp}
                missing_core = sorted(base_indices - received)
                replace_index_cache = bool(base_indices & received)
                for i, batch in enumerate(chunked(missing_core, limit.batch), len(batches)):
                    sleep_between_batches(i, limit.rpm)
                    try:
                        resp.extend(tf.quotes.get(symbols=batch) or [])
                        replace_index_cache = True
                    except Exception as core_exc:  # noqa: BLE001
                        _self.logger.warning("核心指数行情拉取失败，保留上一轮数据: %s", core_exc)
            _self.logger.info(
                "指数及订阅行情拉取完成: %d 只 (%.2fs)",
                len(_core_syms),
                time.perf_counter() - _i0,
            )
        return resp, replace_index_cache, partial_assets

    def _pull_tickflow_quotes(self, tf, demand, t0):
        """拉取 TickFlow 全市场+订阅+指数行情; resp=None 表示失败。"""
        replace_index_cache = True
        try:
            all_index_symbols = set(self._repo.get_index_symbol_set()) if self._repo else set()
            core_index_symbols = set(CORE_INDEX_SYMBOLS)
            all_index_symbols.update(core_index_symbols)
            # 指数监控规则标的并入显式拉取 (quotes.get 按码覆盖)
            monitor_index_symbols = self._collect_monitor_index_symbols()
            all_index_symbols.update(monitor_index_symbols)
            resp = self._tickflow_fetch_universes(tf)
            # Demand supplements the existing universes; a watchlist/detail must not
            # require enabling a full-market ETF pull, nor issue duplicate stock pulls.
            resp, replace_index_cache, partial_assets = self._tickflow_fetch_symbols(
                tf,
                resp,
                core_index_symbols,
                monitor_index_symbols,
                demand,
            )
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("行情拉取失败 (%.2fs): %s", time.perf_counter() - t0, e)
            return None, replace_index_cache, set()
        return resp, replace_index_cache, partial_assets

    @staticmethod
    def _parse_tickflow_records(resp):
        """解析 API 响应为内部 records (change_pct 保持小数制, 不可乘 100)。"""
        records = []
        for q in resp:
            ext = q.get("ext") or {}
            last_price = q.get("last_price")
            prev_close = q.get("prev_close")
            change_amount = ext.get("change_amount")
            change_pct = ext.get("change_pct")
            if change_amount is None and last_price is not None and prev_close is not None:
                change_amount = float(last_price) - float(prev_close)
            if change_pct is None and change_amount is not None and prev_close not in (None, 0):
                # 与 API ext.change_pct 同为小数制 (0.0366 = 3.66%),
                # enriched 全项目约定小数 (见 pipeline.py), 此处不可乘 100
                change_pct = float(change_amount) / float(prev_close)
            records.append(
                {
                    "symbol": q.get("symbol"),
                    "name": q.get("name") or ext.get("name"),
                    "last_price": last_price,
                    "prev_close": prev_close,
                    "open": q.get("open"),
                    "high": q.get("high"),
                    "low": q.get("low"),
                    "volume": q.get("volume"),
                    "amount": q.get("amount"),
                    "change_pct": change_pct,
                    "change_amount": change_amount,
                    "amplitude": ext.get("amplitude"),
                    "turnover_rate": ext.get("turnover_rate"),
                    "timestamp": q.get("timestamp"),
                    "session": q.get("session"),
                }
            )
        return records

    def _fetch_full_market_quotes(
        self, final_boundary_ms: int | None = None
    ) -> tuple[pl.DataFrame, pl.DataFrame | None] | None:
        """拉取全市场行情 → 写 daily + 计算 enriched + 更新缓存。

        返回 (daily_df, quote_extra) 快照供调用方在锁外评估监控; 空/失败轮返回 None。
        """
        from app.services import preferences

        source_stamp = preferences.data_source_stamp()
        provider_name = preferences.get_realtime_data_provider()
        configured = str(preferences.load().get("realtime_data_provider") or "").strip().lower()
        if configured and configured != "tickflow" and provider_name == "tickflow":
            provider_name = configured
        demand = self._wanted_quotes()
        if provider_name != "tickflow":
            return self._fetch_custom_realtime(
                provider_name,
                demand,
                source_stamp,
                final_boundary_ms,
            )

        from app.tickflow.client import get_paid_realtime_client

        tf = get_paid_realtime_client()
        if tf is None:
            _self.logger.warning("实时行情拉取失败:未配置付费服务器 API Key")
            return None
        t0 = time.perf_counter()
        now_ts = time.perf_counter()
        resp, replace_index_cache, partial_assets = self._pull_tickflow_quotes(tf, demand, t0)
        if resp is None:
            return None
        if not resp:
            _self.logger.warning("行情数据为空")
            return None
        records = self._parse_tickflow_records(resp)
        if preferences.data_source_stamp() != source_stamp:
            _self.logger.info("行情数据源配置已变化，跳过旧轮次发布")
            return None
        return self._process_full_market_records(
            records,
            t0=t0,
            now_ts=now_ts,
            final_boundary_ms=final_boundary_ms,
            **({"partial_assets": partial_assets} if partial_assets else {}),
            **({"replace_index_cache": False} if not replace_index_cache else {}),
        )

    def _market_symbol_context(self):
        """构建 index/etf 标的集合, 供 records 分类。"""
        all_index_symbols = set(self._repo.get_index_symbol_set()) if self._repo else set()
        core_index_symbols = set(CORE_INDEX_SYMBOLS)
        all_index_symbols.update(core_index_symbols)
        from app.indicators.pipeline import BENCHMARK_INDEX_SYMBOLS

        all_index_symbols.update(BENCHMARK_INDEX_SYMBOLS)
        all_index_symbols.update(self._collect_monitor_index_symbols())
        all_etf_symbols = set()
        if self._repo:
            etf_inst = self._repo.get_etf_instruments()
            if not etf_inst.is_empty() and "symbol" in etf_inst.columns:
                all_etf_symbols = set(etf_inst["symbol"].cast(pl.Utf8).to_list())
        return all_index_symbols, all_etf_symbols

    def _write_live_daily(self, stock_records, etf_records, partial_assets):
        """写不复权原始 kline_daily; 返回 (daily_df, etf_daily_df)。"""
        daily_df = self._build_daily(stock_records)
        if not daily_df.is_empty() and self._repo:
            try:
                if partial_assets and "stock" in partial_assets:
                    self._repo.merge_live_daily_asset("stock", daily_df)
                else:
                    self._repo.flush_live_daily(daily_df)
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("日K写盘失败: %s", e)

        etf_daily_df = self._build_daily(etf_records)
        if not etf_daily_df.is_empty() and self._repo:
            try:
                if partial_assets and "etf" in partial_assets:
                    self._repo.merge_live_daily_asset("etf", etf_daily_df)
                else:
                    self._repo.flush_live_daily_asset("etf", etf_daily_df)
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("ETF 日K写盘失败: %s", e)
        return daily_df, etf_daily_df
