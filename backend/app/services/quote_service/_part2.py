"""QuoteService mixin part 2（原单类方法分组）。"""

from __future__ import annotations

import time
from datetime import date, datetime
from datetime import time as dt_time
from typing import Any

import polars as pl

import app.services.quote_service as _self
from app.market_time import CN_TZ

from ._common import (
    _FINAL_BOUNDARY,
    _FINAL_CONFIRM_SLACK_MS,
    _FINAL_DEADLINE,
)


class QuoteServiceMixin2:
    """原 QuoteService 方法分组 2。"""

    # 以下属性由 QuoteService 组装类/其它 mixin 提供，仅作类型声明（不创建运行时属性）。
    _repo: Any
    _flush_live_enriched: Any
    _lock: Any
    _market_symbol_context: Any
    _broadcast_quote_updated: Any
    _update_volume_delta: Any
    _write_live_daily: Any
    _app_state: Any
    _holiday_active: Any
    _final_sync_done: Any
    _abnormal_last_eval: Any
    _inject_sealed_vol: Any
    _inject_volume_delta: Any
    _inject_intraday_signals: Any
    notify_strategy_results_updated: Any
    get_index_quotes: Any
    _broadcast_alerts: Any
    _maybe_send_system_notifications: Any
    _maybe_send_paper_webhook: Any
    get_enriched_today: Any
    _maybe_send_webhook: Any
    _last_final_confirmed: Any

    def _flush_live_enriched_for_assets(
        self,
        daily_df,
        etf_daily_df,
        stock_records,
        etf_records,
        index_records,
        partial_assets,
    ):
        """增量计算股票/ETF/指数 enriched 并写盘; 返回 (daily_df, quote_extra) 快照。"""
        # ---- 构建 API 直接值的补充表 (不写 daily, 只用于 enriched 计算) ----
        quote_extra = self._build_quote_extra(stock_records)
        etf_quote_extra = self._build_quote_extra(etf_records)

        # ---- 增量计算 enriched + 写盘 + 更新缓存 ----
        if not daily_df.is_empty() and self._repo:
            self._flush_live_enriched(
                daily_df,
                quote_extra,
                asset_type="stock",
                merge=bool(partial_assets and "stock" in partial_assets),
            )
        if not etf_daily_df.is_empty() and self._repo:
            self._flush_live_enriched(
                etf_daily_df,
                etf_quote_extra,
                asset_type="etf",
                merge=bool(partial_assets and "etf" in partial_assets),
            )
        # ---- 指数: 核心四只每轮已显式拉取, 与股票/ETF 同口径 merge 写盘 ----
        # 不能再门控 has_asset_rules("index"): 默认配置无指数监控规则, 否则
        # 盘中 kline_index_enriched 停在上一交易日, 读侧守卫直接跳过不注入。
        # 指数为按码显式拉取 (部分标的) → merge 不截断分区。
        if self._repo:
            index_daily_df = self._build_daily(index_records)
            if not index_daily_df.is_empty():
                try:
                    self._repo.merge_live_daily_asset("index", index_daily_df)
                except Exception as e:  # noqa: BLE001
                    _self.logger.warning("指数日K写盘失败: %s", e)
                self._flush_live_enriched(
                    index_daily_df,
                    self._build_quote_extra(index_records),
                    asset_type="index",
                    merge=True,
                )
        return daily_df, quote_extra

    def _update_fetch_metadata(
        self,
        now_ts,
        fetch_ms,
        fetched_at,
        stock_records,
        etf_records,
        index_records,
        replace_index_cache,
    ):
        """更新展示元信息与指数缓存 (持锁)。"""
        with self._lock:
            self._fetch_time = now_ts
            self._fetch_ms = fetch_ms
            self._fetched_at = fetched_at
            self._symbol_count = len(stock_records)
            self._etf_symbol_count = len(etf_records)
            if replace_index_cache:
                self._index_symbol_count = len(index_records)
                self._index_quotes_cache = self._build_index_quotes(index_records)
            else:
                _self.logger.info("指数本轮获取失败,沿用上轮缓存: %d 只", self._index_symbol_count)
        _self._persist_last_fetch(fetched_at)

    def _confirm_final_snapshot(
        self, records: list[dict], final_boundary_ms: int | None
    ) -> tuple[bool | None, Any]:
        """判断快照是否达到 final 定版边界，并回写确认状态给 SSE/status。"""
        if final_boundary_ms is None:
            return None, None
        max_ts = max((r["timestamp"] for r in records if r.get("timestamp")), default=None)
        confirmed = max_ts is not None and max_ts >= final_boundary_ms - _FINAL_CONFIRM_SLACK_MS
        self._last_final_confirmed = confirmed
        return confirmed, max_ts

    def _process_full_market_records(
        self,
        records: list[dict],
        *,
        t0: float,
        now_ts: float,
        replace_index_cache: bool = True,
        final_boundary_ms: int | None = None,
        partial_assets: set[str] | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame | None] | None:
        """把全市场 records 写盘并增量计算 enriched。

        final_boundary_ms (final 定版边界) 传入时, 快照最大时间戳未达边界的本轮
        只更新展示缓存, 不写 daily/enriched、不评估监控 —— 防止收盘后数据源仍
        返回竞价前旧价时把陈旧收盘价固化到当日分区。
        """
        all_index_symbols, all_etf_symbols = self._market_symbol_context()
        if not records:
            _self.logger.warning("行情数据为空")
            return None

        # ---- final 定版确认: 快照最大时间戳达到边界 (含容差) 才允许落盘 ----
        confirmed_final, max_ts = self._confirm_final_snapshot(records, final_boundary_ms)

        other_symbols = all_index_symbols | all_etf_symbols
        index_records = [r for r in records if r.get("symbol") in all_index_symbols]
        etf_records = [r for r in records if r.get("symbol") in all_etf_symbols]
        stock_records = [r for r in records if r.get("symbol") not in other_symbols]

        fetch_ms = (time.perf_counter() - t0) * 1000
        fetched_at = time.time() * 1000

        # ---- 更新元信息 ----
        self._update_fetch_metadata(
            now_ts,
            fetch_ms,
            fetched_at,
            stock_records,
            etf_records,
            index_records,
            replace_index_cache,
        )
        counts = (len(stock_records), len(etf_records), len(index_records))
        _self.logger.info(
            "行情刷新: %d 只股票, %d 只ETF, %d 只指数, 耗时 %.0fms", *counts, fetch_ms
        )

        if confirmed_final is False:
            # 边界前的陈旧快照: 展示缓存已更新, 落盘与监控评估留待边界后快照。
            # 轮询线程会在定版窗口内持续重试, 窗口结束由 _poll_loop 放弃并告警。
            _self.logger.info(
                "final 快照未达定版边界 (max quote_ts=%s, 边界=%s), 本轮跳过落盘",
                max_ts,
                final_boundary_ms,
            )
            self._broadcast_quote_updated()
            return None

        # 轮询放量状态更新 (volume_delta 规则的差值来源)
        self._update_volume_delta(stock_records, fetched_at)
        daily_df, etf_daily_df = self._write_live_daily(stock_records, etf_records, partial_assets)
        daily_df, quote_extra = self._flush_live_enriched_for_assets(
            daily_df,
            etf_daily_df,
            stock_records,
            etf_records,
            index_records,
            partial_assets,
        )
        # ---- 通知 SSE ----
        self._broadcast_quote_updated()

        # ---- 策略监控 + 告警评估 ----
        # 由调用方 (_fetch_quotes) 持 _evaluate_lock 在取数锁外执行;
        # 返回本轮快照 (daily_df, quote_extra) 作为评估输入
        return daily_df, quote_extra

    # ================================================================
    # 工具
    # ================================================================

    def _collect_monitor_index_symbols(self) -> set[str]:
        """启用中的指数监控规则标的 (asset_type=index & scope=symbols)。"""
        engine = getattr(self._app_state, "monitor_engine", None) if self._app_state else None
        if not engine:
            return set()
        out: set[str] = set()
        for _r in list(getattr(engine, "rules", {}).values()):
            if (
                _r.get("enabled", True)
                and _r.get("asset_type") == "index"
                and _r.get("scope") == "symbols"
            ):
                out.update(s for s in _r.get("symbols", []) if s)
        return out

    @staticmethod
    @staticmethod
    def _build_daily(records: list[dict]) -> pl.DataFrame:
        """将 API records 转为日K格式 DataFrame (OHLCV + quote_ts, 写 kline_daily 用)。"""
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        cols_map = {
            "symbol": "symbol",
            "last_price": "close",
            "open": "open",
            "high": "high",
            "low": "low",
            "volume": "volume",
            "amount": "amount",
            "timestamp": "quote_ts",
        }
        select_exprs = []
        for src, dst in cols_map.items():
            if src in df.columns:
                select_exprs.append(
                    pl.col(src).cast(pl.Int64, strict=False).alias(dst)
                    if dst == "quote_ts"
                    else pl.col(src).alias(dst)
                )
        if not select_exprs:
            return pl.DataFrame()
        result = df.select(select_exprs).with_columns(
            pl.lit(_self.cn_today()).cast(pl.Date).alias("date"),
        )
        # 停牌股回归: 实时源对停牌标的返回停牌前最后一份快照 — OHLCV 全为旧日
        # 真实值, 仅 timestamp 停在旧日。这类记录不属于当日, 不过滤会把旧日 K 线
        # 原样复制成当日假蜡烛 (如 301266.SZ 2026-09-04)。按 quote_ts 的北京
        # 日期归属过滤; 时间戳缺失/为空的源无法判断, 维持原行为保留。
        if "quote_ts" in result.columns:
            day_start_ms = int(
                datetime.combine(_self.cn_today(), dt_time(0, 0), tzinfo=CN_TZ).timestamp() * 1000
            )
            result = result.filter(
                pl.col("quote_ts").is_null()
                | pl.col("quote_ts").is_between(
                    day_start_ms, day_start_ms + 86_400_000, closed="left"
                )
            )
        # 停牌/尚无集合竞价的记录 open/high 均为 0。必须在下方用 close 填充前
        # 过滤, 否则零成交行会被伪装成有效日K, 并在 batch 同步后作为实时残留
        # 反复触发历史完整性修复。
        from app.indicators.pipeline import filter_halt_days

        result = filter_halt_days(result)
        # 修复: API 在非交易时段可能返回 open/high/low=0 或 null,
        # 导致蜡烛从 0 开始。用 close 填充这些异常值。
        for col in ("open", "high", "low"):
            if col in result.columns:
                result = result.with_columns(
                    pl.when((pl.col(col) == 0) | pl.col(col).is_null())
                    .then(pl.col("close"))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        return result

    @staticmethod
    def _build_quote_extra(records: list[dict]) -> pl.DataFrame:
        """构建 API 直接提供的补充字段 (不写 daily, 只传给 enriched 计算)。

        包含: prev_close, change_pct, change_amount, amplitude, turnover_rate。
        """
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        keep = [
            c
            for c in [
                "symbol",
                "prev_close",
                "change_pct",
                "change_amount",
                "amplitude",
                "turnover_rate",
            ]
            if c in df.columns
        ]
        if not keep or "symbol" not in keep:
            return pl.DataFrame()
        # 整列 null = 数据源未提供该字段 (如 fuyao 的 turnover_rate/amplitude 显式置 None),
        # 必须丢弃: 下游 compute_enriched_today 对这些列是「列存在即直接采用」,
        # 转发全空列会跳过回退计算, 当日换手/振幅将永远为空且每轮实时覆写自锁
        keep = [c for c in keep if c == "symbol" or df[c].null_count() < len(df)]
        out = df.select(keep)
        # 实时 API 的 turnover_rate 入口契约为小数制(0.05 = 5%).
        # enriched 内部统一存百分数值(5 = 5%), 后续页面/筛选直接展示和比较。
        if "turnover_rate" in out.columns:
            out = out.with_columns(
                (pl.col("turnover_rate").cast(pl.Float64, strict=False) * 100).alias(
                    "turnover_rate"
                )
            )
        return out

    @staticmethod
    def _build_index_quotes(records: list[dict]) -> pl.DataFrame:
        """构建指数实时行情缓存，不落股票 parquet。

        注意: API 返回的 change_pct/amplitude 是小数 (0.0366 = 3.66%),
        统一转成百分比输出, 与 _fallback_index_quotes_from_daily 口径一致
        (前端指数侧不×100, 直接 toFixed(2)% 展示)。
        """
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        keep = [
            c
            for c in [
                "symbol",
                "name",
                "last_price",
                "prev_close",
                "open",
                "high",
                "low",
                "volume",
                "amount",
                "change_pct",
                "change_amount",
                "amplitude",
                "timestamp",
                "session",
            ]
            if c in df.columns
        ]
        if not keep or "symbol" not in keep:
            return pl.DataFrame()
        df = df.select(keep)
        # 自定义源可能不提供 change_pct/change_amount, 按 last_price/prev_close 补算
        # (TickFlow 路径在 _fetch_full_market_quotes 已算好, 此处只补缺失的)
        if (
            "change_pct" not in df.columns
            and "last_price" in df.columns
            and "prev_close" in df.columns
        ):
            # prev_close=0 → inf (非合法 JSON), prev_close=null → null; 用 when 守护
            df = df.with_columns(
                pl.when(pl.col("prev_close") != 0)
                .then((pl.col("last_price") - pl.col("prev_close")) / pl.col("prev_close"))
                .otherwise(None)
                .alias("change_pct")
            )
        if (
            "change_amount" not in df.columns
            and "last_price" in df.columns
            and "prev_close" in df.columns
        ):
            df = df.with_columns(
                (pl.col("last_price") - pl.col("prev_close")).alias("change_amount")
            )
        # change_pct / amplitude: 小数 → 百分比 (统一指数展示口径)
        for col in ("change_pct", "amplitude"):
            if col in df.columns:
                df = df.with_columns((pl.col(col).cast(pl.Float64) * 100).alias(col))
        if "last_price" in df.columns and "close" not in df.columns:
            df = df.with_columns(pl.col("last_price").alias("close"))
        return df

    @staticmethod
    def _market_phase() -> str:
        """A股行情轮询阶段(北京时间)。

        final 阶段用于午休/收盘定版: 需要至少成功拉取一版边界后的行情, 才算进入休盘。
        """
        now = _self.cn_now()
        if now.weekday() >= 5:
            return "closed"
        t = now.time()
        if dt_time(9, 15) <= t < dt_time(9, 30):
            return "preopen"
        if dt_time(9, 30) <= t < dt_time(11, 30):
            return "morning"
        if dt_time(11, 30) <= t < dt_time(12, 55):
            return "morning_final"
        if dt_time(12, 55) <= t < dt_time(13, 0):
            return "pre_afternoon"
        if dt_time(13, 0) <= t < dt_time(15, 0):
            return "afternoon"
        if t >= dt_time(15, 0):
            return "close_final"
        return "closed"

    @staticmethod
    def _final_sync_key(phase: str) -> tuple[date, str] | None:
        if phase == "morning_final":
            return (_self.cn_today(), "morning")
        if phase == "close_final":
            return (_self.cn_today(), "close")
        return None

    @classmethod
    def _final_boundary_ms(cls, phase: str) -> int | None:
        """final 阶段定版边界的 epoch ms (按北京时间当日换算, 不依赖服务器时区)。"""
        b = _FINAL_BOUNDARY.get(phase)
        if b is None:
            return None
        return int(datetime.combine(_self.cn_today(), b, tzinfo=CN_TZ).timestamp() * 1000)

    @classmethod
    def _past_final_deadline(cls, phase: str) -> bool:
        """是否已过 final 重试窗口终点 (用于放弃未确认的定版重试)。"""
        dl = _FINAL_DEADLINE.get(phase)
        return dl is not None and _self.cn_now().time() >= dl

    def _holiday_gate(self) -> bool:
        """交易日探针门控: 确定休市 → False (停止轮询, 含 final 定版)。

        探针未知 (None, 未配置 fuyao 且 tickflow 不可用/开盘缓冲窗内) → True,
        维持周几近似现状行为。探针是纯读, 不落盘; 休市结论带 TTL 定期复探,
        误判自愈。首次判定变化打一条日志, 避免每拍刷屏。
        """
        from app.services import trading_day

        holiday = trading_day.is_trading_day() is False
        if holiday != self._holiday_active:
            self._holiday_active = holiday
            if holiday:
                _self.logger.info("交易日探针判定休市, 行情轮询暂停 (30 分钟复探)")
        return not holiday

    def _should_poll_for_phase(self, phase: str) -> bool:
        """是否处于会主动拉行情的阶段。final 阶段成功后即停止。

        节假日 (工作日但休市) 由交易日探针剔除 — 周几门控覆盖不到的部分。
        """
        if not self._holiday_gate():
            return False
        if phase in {"preopen", "morning", "pre_afternoon", "afternoon"}:
            return True
        key = self._final_sync_key(phase)
        return bool(key and key not in self._final_sync_done)

    def _should_fetch_for_phase(self, phase: str) -> bool:
        return self._should_poll_for_phase(phase)

    def _is_trading_hours(self) -> bool:
        """行情轮询窗口(兼容旧调用): 包含盘前预热和未完成的午休/收盘定版。"""
        return self._should_poll_for_phase(self._market_phase())

    @staticmethod
    def _is_continuous_trading() -> bool:
        """A股连续竞价时段(北京时间): 9:30-11:30 / 13:00-15:00, 仅工作日。

        比 _is_trading_hours 严格: 排除 9:15-9:30 集合竞价(指示价, 非成交价)、
        午间与 15:00 后收盘缓冲。监控评估只在此窗口进行, 不对竞价/收盘后的陈旧价告警。
        (节假日由 _evaluate_monitors 里的「快照日期=当日」新鲜度判据兜底, 无需交易日历。)
        """
        now = _self.cn_now()
        t = now.time()
        morning = dt_time(9, 30) <= t <= dt_time(11, 30)
        afternoon = dt_time(13, 0) <= t <= dt_time(15, 0)
        return now.weekday() < 5 and (morning or afternoon)

    @staticmethod
    def _save_enabled(enabled: bool) -> None:
        from app.services import preferences

        preferences.save({"realtime_quotes_enabled": enabled})

    # ================================================================
    # 策略监控
    # ================================================================

    @staticmethod
    def _alert_from_event(ev: dict) -> dict:
        """把统一规则事件转为 SSE 推送格式 (兼容旧 alert schema)。"""
        alert = {
            "source": ev["source"],
            "type": ev["type"],
            "rule_id": ev.get("rule_id"),
            "strategy_id": ev.get("strategy_id") if ev["source"] == "strategy" else None,
            "symbol": ev["symbol"],
            "name": ev["name"],
            "message": ev["message"],
            "price": ev["price"],
            "change_pct": ev["change_pct"],
            "signals": ev["signals"],
            "severity": ev.get("severity", "info"),
            "conditions": ev.get("conditions") or [],
            "logic": ev.get("logic") or "and",
        }
        for key in (
            "sector_kind",
            "sector_key",
            "sector_name",
            "sector_source_field",
            "sector_value",
            "sector_level",
            "window_change_pct",
            "coverage_ratio",
            "valid_count",
            "total_count",
            "up_count",
            "down_count",
            "leader",
            "abnormal_window",
            "abnormal_value",
            "abnormal_threshold",
            "abnormal_closeness",
            "volume_delta",
            "volume_delta_span",
            "volume_delta_amount",
        ):
            if key in ev:
                alert[key] = ev[key]
        return alert

    def _evaluate_abnormal_rules(self, engine):
        """异动边缘规则轮 (30s 限频, 独立降级); 返回新增 rule_events。"""
        rule_events: list[dict] = []
        # 快照 (enriched 偏离列 + 实时叠加) 由 abnormal_moves.build_overview
        # 统一构建, 引擎只做边缘触发判定。30s 限频 —— 快照历史部分 60s 缓存。
        if engine.has_rule_type("abnormal") and self._repo is not None:
            _now_ts = time.time()
            if _now_ts - self._abnormal_last_eval >= 30.0:
                self._abnormal_last_eval = _now_ts
                try:
                    from app.services import abnormal_moves

                    _overview = abnormal_moves.build_overview(
                        self._repo,
                        self,
                        min_closeness=engine.min_abnormal_closeness(),
                        limit=1000,
                    )
                    rule_events += engine.evaluate_abnormal(_overview.get("rows") or [])
                except Exception as e:  # noqa: BLE001
                    _self.logger.warning("异动监控规则评估失败 (不影响其他告警): %s", e)
        return rule_events

    def _evaluate_stock_and_common_rules(
        self,
        engine,
        stock_ready,
        enriched_today,
        enriched_date,
    ):
        """股票轮 + 板块/异动/日期规则; 返回 rule_events。"""
        # 预构建 symbol → name 映射 (enriched 已 drop name 列, 引擎触发时回填用)。
        # 股票 + ETF + 指数三表合并走 _monitor_name_map -> repo.get_name_map()
        # 的进程内 memo, 避免每轮监控对 ~7000 行维表 iter_rows 重建。
        try:
            name_map = _self._monitor_name_map(self._app_state.repo)
            if name_map:
                engine.set_name_map(name_map)
        except Exception as e:  # noqa: BLE001
            _self.logger.debug("name_map 构建失败 (不影响监控): %s", e)
        rule_events: list[dict] = []
        # 股票轮: 快照未就绪时跳过 (ladder 封单也依赖股票快照日期, 一并跳过)
        if stock_ready:
            eval_df = enriched_today
            if engine.has_rule_type("ladder"):
                eval_df = self._inject_sealed_vol(enriched_today, enriched_date)
            if engine.has_rule_type("volume_delta"):
                eval_df = self._inject_volume_delta(eval_df)
            eval_df = self._inject_intraday_signals(eval_df, engine, "stock")
            rule_events = engine.evaluate(eval_df, asset_type="stock")
            if engine.consume_strategy_result_updates():
                self.notify_strategy_results_updated()
        if engine.has_rule_type("sector"):
            rule_events += engine.evaluate_sectors(
                enriched_today if stock_ready else pl.DataFrame(),
                self.get_index_quotes(),
            )
        rule_events += self._evaluate_abnormal_rules(engine)
        # 日期提醒轮: 纯日历、无行情, 已在盘中; 引擎内按天 cooldown 保证每天一次
        if engine.has_rule_type("date"):
            try:
                rule_events = rule_events + engine.evaluate_date_rules()
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("日期提醒评估失败 (不影响其他告警): %s", e)
        return rule_events

    def _evaluate_etf_index_rules(self, engine):
        """ETF/指数规则轮 (独立 try, 各自日期守卫); 返回新增 rule_events。"""
        rule_events: list[dict] = []
        # ETF 规则轮: 股票快照不含 ETF, 用 ETF enriched 快照单独评估。
        # 独立 try —— ETF 轮任何异常都不得丢弃本轮已算出的股票告警。
        # refresh=False —— 不在轮询线程上触发 ETF 冷缓存的同步重算 (缓存由 ETF 实时
        # flush 焐热; 未焐热说明无 ETF 实时数据, 跳过本轮 ETF 评估)。
        # 日期守卫同指数轮: 自选/策略等页面会把磁盘上一交易日的 ETF 快照读进缓存,
        # ETF 实时拉取关闭 (默认) 或休市时它不会被当日数据替换, 不得当作当日评估。
        if engine.has_asset_rules("etf") and self._repo is not None:
            try:
                etf_enriched, etf_date = self._repo.get_enriched_latest_asset("etf", refresh=False)
                if not etf_enriched.is_empty() and etf_date == _self.cn_today():
                    etf_enriched = self._inject_intraday_signals(etf_enriched, engine, "etf")
                    rule_events = rule_events + engine.evaluate(
                        etf_enriched,
                        asset_type="etf",
                        reset_strategy_results=False,
                    )
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("ETF 监控评估失败 (不影响股票告警): %s", e)
        # 指数规则轮: 复刻 ETF 轮。快照由指数实时 flush 焐热;
        # refresh=False 冷缓存不同步重算; 显式日期守卫防陈旧 parquet 误告警
        # (与 ETF 轮同口径)。
        if engine.has_asset_rules("index") and self._repo is not None:
            try:
                index_enriched, index_date = self._repo.get_enriched_latest_asset(
                    "index", refresh=False
                )
                if not index_enriched.is_empty() and index_date == _self.cn_today():
                    index_enriched = self._inject_intraday_signals(index_enriched, engine, "index")
                    rule_events = rule_events + engine.evaluate(
                        index_enriched,
                        asset_type="index",
                        reset_strategy_results=False,
                    )
            except Exception as e:  # noqa: BLE001
                _self.logger.warning("指数监控评估失败 (不影响股票/ETF 告警): %s", e)
        return rule_events

    def _persist_and_format_alerts(self, rule_events, all_alerts):
        """格式化扩展通知 + 落盘 alerts.jsonl + 填充 all_alerts; 返回格式化后的 rule_events。"""
        rule_events = self._format_extension_notifications(rule_events)
        # 落盘到 alerts.jsonl
        try:
            from app.services import alert_store

            alert_store.append_many(self._app_state.repo.store.data_dir, rule_events)
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("告警落盘失败: %s", e)
        for ev in rule_events:
            all_alerts.append(self._alert_from_event(ev))
        return rule_events

    def _run_monitor_engine(self, stock_ready, enriched_today, enriched_date):
        """统一引擎评估, 返回 (all_alerts, rule_events, paper_watch_events, engine)。"""
        all_alerts: list[dict] = []
        rule_events: list[dict] = []
        paper_watch_events: list[dict] = []  # 模拟盘合成规则的沙盒信号 (只喂模拟盘)
        engine = None
        if not self._app_state:
            return all_alerts, rule_events, paper_watch_events, engine
        engine = getattr(self._app_state, "monitor_engine", None)
        if not engine or engine.rule_count <= 0:
            return all_alerts, rule_events, paper_watch_events, engine
        rule_events = self._evaluate_stock_and_common_rules(
            engine,
            stock_ready,
            enriched_today,
            enriched_date,
        )
        rule_events = rule_events + self._evaluate_etf_index_rules(engine)
        # 模拟盘「绑定即评估」的合成规则事件是沙盒信号: 只喂下面的
        # 模拟盘钩子, 不进告警流 (留痕/SSE/系统通知/webhook/扩展格式化)。
        from app.strategy.paper_watch import is_paper_watch_event

        paper_watch_events = [ev for ev in rule_events if is_paper_watch_event(ev)]
        rule_events = [ev for ev in rule_events if not is_paper_watch_event(ev)]
        if rule_events:
            rule_events = self._persist_and_format_alerts(rule_events, all_alerts)
        return all_alerts, rule_events, paper_watch_events, engine

    def _run_paper_hook(self, stock_ready, enriched_today, rule_events, paper_watch_events):
        """模拟盘钩子: 同频即时撮合 + 自动跟单 + 静默降级推送。"""
        if not (stock_ready and self._app_state is not None):
            return
        try:
            from app.strategy import paper as paper_trading
            from app.strategy import paper_auto

            data_dir = self._app_state.repo.store.data_dir
            snapshot = dict(
                zip(
                    enriched_today["symbol"].to_list(),
                    enriched_today["raw_close"].to_list(),
                    strict=False,
                )
            )
            paper_events: list[dict] = []
            for acc_id in paper_trading.list_account_ids(data_dir):
                paper_events.extend(
                    paper_trading.evaluate_intraday(data_dir, snapshot, account_id=acc_id)
                )
                feed = rule_events + paper_watch_events
                if feed:
                    created = paper_auto.on_rule_events(data_dir, feed, account_id=acc_id)
                    paper_events.extend(paper_auto.auto_order_events(created, account_id=acc_id))
            # 成交/自动跟单下单推送 (V3): 复用监控中心既有管道 —— SSE toast /
            # 语音 (前端按 source 拼文案) / 系统通知 / alert_store 留痕 /
            # Webhook。全部静默降级。
            if paper_events:
                self._broadcast_alerts(paper_events)
                try:
                    from app.services import alert_store

                    alert_store.append_many(data_dir, paper_events)
                except Exception as e:  # noqa: BLE001
                    _self.logger.warning("模拟盘成交留痕失败: %s", e)
                self._maybe_send_system_notifications(paper_events)
                self._maybe_send_paper_webhook(paper_events)
        except Exception as e:  # noqa: BLE001 - 模拟盘钩子失败不影响监控主流程
            _self.logger.warning("模拟盘钩子失败 (不影响监控): %s", e)

    def _evaluate_monitors(self, daily_df: pl.DataFrame, quote_extra: pl.DataFrame | None) -> None:
        """行情更新后评估统一监控规则引擎,并刷新策略结果缓存。"""
        try:
            # 仅在「交易日 + 连续竞价时段」评估监控 —— 避开集合竞价指示价、盘前/收盘后
            # 缓冲。轮询窗口(_is_trading_hours)更宽是为盘前预热/收盘捕捉, 但告警不应
            # 基于这些非连续竞价价格。
            if not self._is_continuous_trading():
                return
            # 获取 enriched 数据 (刚算好的)
            enriched_today, enriched_date = self.get_enriched_today()
            # 股票快照就绪 = 非空 + 日期为当日。未就绪时仅跳过股票轮,
            # ETF/指数轮有各自的空表+日期守卫, 不受影响 (纯指数行情/自选场景可独立评估)。
            stock_ready = (not enriched_today.is_empty()) and (enriched_date == _self.cn_today())
            if not stock_ready:
                _self.logger.debug(
                    "股票快照未就绪(空=%s, 日期=%s), 跳过股票轮",
                    enriched_today.is_empty(),
                    enriched_date,
                )

            all_alerts, rule_events, paper_watch_events, engine = self._run_monitor_engine(
                stock_ready,
                enriched_today,
                enriched_date,
            )

            # 策略页实时回显: 不写文件 (实时行情每轮更新 enriched, 写文件会被 read_cache
            # 的 mtime 校验判过期, 反复读不到)。监控引擎本轮已算出的结果存在内存
            # (latest_strategy_results), 由 /api/screener/cached 端点直接叠加读取。

            # 广播到所有 SSE 订阅者 (背压保护在订阅者队列内做)
            if all_alerts:
                # 按 symbol 富化行业/概念 ext 字段, 使 toast + 触发记录统一展示板块标签。
                self._enrich_alerts_ext(all_alerts)
                self._broadcast_alerts(all_alerts)
                _self.logger.info("监控评估完成: %d 条通知", len(all_alerts))

                # 系统通知 (可选通道, 由 preferences 开关控制)。
                # cooldown 去重已在 MonitorRuleEngine 做过, 这里只负责转发。
                self._maybe_send_system_notifications(all_alerts)

            # Webhook 推送 (飞书等外部 IM, 由规则 webhook_channels 指定渠道)。
            # 紧随系统通知, 同样静默降级不阻断主流程。
            if rule_events:
                self._maybe_send_webhook(rule_events, engine)

            # 模拟盘钩子: 与监控评估同频 —— ① 用同一份实时快照撮合 pending 即时单;
            # ② rule_events 喂给自动跟单规则触发自动下单。逐账户执行 (账户间规则与订单隔离)。
            # 即时撮合仅股票 (ETF 即时单在下单时已转次日开盘); 独立 try ——
            # 模拟盘任何异常只留痕, 不得影响监控告警链路。
            self._run_paper_hook(stock_ready, enriched_today, rule_events, paper_watch_events)

        except Exception as e:  # noqa: BLE001
            _self.logger.warning("监控评估失败: %s", e)

    def _format_extension_notifications(self, events: list[dict]) -> list[dict]:
        """Apply optional copy formatters after evaluation and before every output channel."""
        registry = (
            getattr(self._app_state, "extension_registry", None)
            if self._app_state is not None
            else None
        )
        if registry is None or not registry.has_notification_formatters:
            return events

        from app.extensions.contracts import (
            BACKEND_EXTENSION_API_VERSION,
            NotificationFormatContext,
        )

        formatted_events: list[dict] = []
        for event in events:
            formatted = dict(event)
            context = NotificationFormatContext(
                api_version=BACKEND_EXTENSION_API_VERSION,
            )
            for registered in registry.notification_formatters():
                try:
                    message = registered.implementation.format_message(dict(formatted), context)
                    if not isinstance(message, str):
                        raise TypeError("notification formatter must return str")
                    formatted["message"] = message
                except Exception as exc:  # noqa: BLE001 - 外部通知格式化器不可控，失败降级默认文案
                    _self.logger.warning(
                        "notification formatter failed %s: %s",
                        registered.implementation_id,
                        exc,
                    )
            formatted_events.append(formatted)
        return formatted_events

    def _enrich_alerts_ext(self, alerts: list[dict]) -> None:
        """就地给告警事件按 symbol 追加行业/概念 ext 字段。

        读 preferences.get_monitor_ext_fields() 取字段配置, 用 screener._load_ext_value_maps
        (带 parquet mtime 缓存) 富化。富化失败静默降级 (告警照常推送, 只是没标签)。
        每条事件新增 {configId}__{fieldName} 键 (与 watchlist/screener 输出约定一致)。
        """
        if not alerts or not self._app_state or self._repo is None:
            return
        try:
            from app.services import preferences

            fields = preferences.get_monitor_ext_fields()
            # 新结构 {field, maxTags, hiddenIndices}, 后端只需 .field
            parts = []
            for key in ("concept", "industry"):
                item = fields.get(key)
                if isinstance(item, dict) and item.get("field"):
                    parts.append(item["field"])
                elif isinstance(item, str) and item:
                    parts.append(item)  # 兼容旧格式
            if not parts:
                return
            ext_columns = ",".join(parts)
            from app.api.screener import _load_ext_value_maps

            value_maps = _load_ext_value_maps(self._repo, ext_columns)
            if not value_maps:
                return
            for ev in alerts:
                sym = ev.get("symbol")
                if not sym:
                    continue
                for out_col, vmap in value_maps.items():
                    ev[out_col] = vmap.get(str(sym))
        except Exception as e:  # noqa: BLE001
            _self.logger.debug("告警 ext 富化失败 (不影响推送): %s", e)
