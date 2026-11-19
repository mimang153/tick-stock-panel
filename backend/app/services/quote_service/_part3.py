"""QuoteService mixin part 3（原单类方法分组）。"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Any, cast

import polars as pl

import app.services.quote_service as _self
from app.parquet import scan_daily_parquet

from ._common import (
    SOURCE_LABELS,
)

_OHLCV_COLS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "quote_ts",
]


class QuoteServiceMixin3:
    """原 QuoteService 方法分组 3。"""

    # 以下属性由 QuoteService 组装类/其它 mixin 提供，仅作类型声明（不创建运行时属性）。
    _intraday_signal_bucket: Any
    _intraday_signal_evaluator: Any
    _app_state: Any
    _repo: Any
    get_index_quotes: Any
    _prev_volume_date: Any
    _volume_delta: Any
    _volume_delta_span_s: Any

    def _inject_intraday_signals(
        self, enriched: pl.DataFrame, engine, asset_type: str
    ) -> pl.DataFrame:
        """每分钟为分时信号规则批量获取一次数据并注入临时布尔列。"""
        get_symbols = getattr(engine, "intraday_signal_symbols", None)
        if not callable(get_symbols):
            return enriched
        symbols = get_symbols(asset_type)
        if not symbols:
            return enriched

        now = _self.cn_now()
        bucket = now.strftime("%Y%m%d%H%M")
        if self._intraday_signal_bucket.get(asset_type) == bucket:
            return self._intraday_signal_evaluator.inject(enriched, [])
        self._intraday_signal_bucket[asset_type] = bucket

        from app.services.kline_sync import (
            fetch_intraday_monitor_batch,
            intraday_monitor_support,
        )

        capset = getattr(self._app_state, "capabilities", None)

        # 全量分钟健康时股票读本地分区 (服务按间隔持续落盘, 与 API 同一列契约),
        # 免去每分钟 bucket 一次的全量 API 拉取; ETF 不在服务 universe 内,
        # 本地读空/异常回落原 API 路径 (含能力与上限检查)
        minute_df = pl.DataFrame()
        if asset_type == "stock":
            svc = getattr(self._app_state, "minute_refresh", None) if self._app_state else None
            if svc is not None and svc.is_healthy() and self._repo is not None:
                try:
                    minute_df = self._repo.get_minute_batch(sorted(symbols), _self.cn_today())
                except Exception as e:  # noqa: BLE001 - 本地读异常回落 API
                    _self.logger.warning("分时信号本地读失败, 回退 API 路径: %s", e)
                    minute_df = pl.DataFrame()
        if minute_df.is_empty():
            support = intraday_monitor_support(capset)
            if not support["available"] or len(symbols) > int(cast(int, support["max_symbols"])):
                return self._intraday_signal_evaluator.inject(enriched, [])
            minute_df = fetch_intraday_monitor_batch(sorted(symbols), capset, now=now)
        prev_close = self._prev_close_from_enriched(enriched, symbols)

        signals = self._intraday_signal_evaluator.evaluate(
            minute_df,
            symbols=symbols,
            prev_close=prev_close,
            asset_type=asset_type,
            now=now,
            signals=self._load_intraday_signal_defs(),
        )
        return self._intraday_signal_evaluator.inject(enriched, signals)

    @staticmethod
    def _prev_close_from_enriched(enriched: pl.DataFrame, symbols: Any) -> dict[str, float]:
        """从 enriched 快照提取昨收；缺失时按 close/(1+change_pct) 反推。"""
        prev_close: dict[str, float] = {}
        available_cols = set(enriched.columns)
        for row in enriched.filter(pl.col("symbol").is_in(sorted(symbols))).iter_rows(named=True):
            symbol = str(row.get("symbol") or "")
            reference = row.get("prev_close") if "prev_close" in available_cols else None
            if reference is None and "close" in available_cols and "change_pct" in available_cols:
                close = row.get("close")
                change_pct = row.get("change_pct")
                if close is not None and change_pct is not None and float(change_pct) > -1:
                    reference = float(close) / (1.0 + float(change_pct))
            if symbol and reference is not None:
                prev_close[symbol] = float(reference)
        return prev_close

    def _load_intraday_signal_defs(self) -> list[dict]:
        """加载自定义盘中信号定义(带指纹缓存); 失败时退化为仅内置 4 信号。"""
        try:
            from app.strategy import custom_signals

            return custom_signals.load_intraday_all(self._repo.store.data_dir)
        except Exception as e:  # noqa: BLE001 - 信号定义加载失败按空列表降级
            _self.logger.warning("load intraday signal defs failed: %s", e)
            return []

    @staticmethod
    def _continuous_session_start_ms() -> float:
        """当前连续竞价时段的起点 (北京时间 9:30 或 13:00) 的 epoch 毫秒。"""
        now = _self.cn_now()
        start_time = dt_time(13, 0) if now.time() >= dt_time(13, 0) else dt_time(9, 30)
        return datetime.combine(now.date(), start_time, tzinfo=now.tzinfo).timestamp() * 1000.0

    def _update_volume_delta(self, stock_records: list[dict], fetched_at_ms: float) -> None:
        """全市场相邻两次快照的股票累计成交量差值 (手), 供 volume_delta 规则。

        - prev 每轮都更新 (含非连续竞价时段); 差值只在连续竞价时段内计算
        - 开盘保护: prev 早于本时段起点 (9:30/13:00) 时本轮差值无效 -- 避免
          9:25 集合竞价撮合量 / 午休缺口被当成"突然放量"
        - cur < prev (数据源重置/口径跳变) 的个股丢弃差值; 跨交易日清空
        """
        today = _self.cn_today()
        if self._prev_volume_date != today:
            self._prev_stock_volume = None
            self._prev_volume_fetched_at = None
            self._prev_volume_date = today
            self._volume_delta = {}

        cur: dict[str, tuple[float, float]] = {}
        for r in stock_records:
            sym = r.get("symbol")
            vol = r.get("volume")
            amt = r.get("amount")
            if not sym or not isinstance(vol, int | float):
                continue
            cur[str(sym)] = (
                float(vol),
                float(amt) if isinstance(amt, int | float) else 0.0,
            )

        prev = self._prev_stock_volume
        prev_ts = self._prev_volume_fetched_at
        if (
            prev is not None
            and prev_ts is not None
            and self._is_continuous_trading()
            and prev_ts >= self._continuous_session_start_ms()
        ):
            delta = {
                sym: (v - prev[sym][0], a - prev[sym][1])
                for sym, (v, a) in cur.items()
                if sym in prev and v >= prev[sym][0] and a >= prev[sym][1] and v - prev[sym][0] > 0
            }
            self._volume_delta = delta
            self._volume_delta_span_s = max((fetched_at_ms - prev_ts) / 1000.0, 0.001)
        else:
            self._volume_delta = {}

        self._prev_stock_volume = cur
        self._prev_volume_fetched_at = fetched_at_ms

    def _inject_volume_delta(self, enriched_today: pl.DataFrame) -> pl.DataFrame:
        """把最近一轮快照差值作为临时列注入 enriched 副本。

        _volume_delta (手) / _volume_delta_amount (元) / _volume_delta_span (秒, 快照间隔)。
        无有效差值 (首轮/开盘保护/暂停后恢复) 时返回原 df, 规则安全降级不触发。
        """
        try:
            delta = self._volume_delta
            if not delta:
                return enriched_today
            span = self._volume_delta_span_s
            delta_df = pl.DataFrame(
                {
                    "symbol": list(delta.keys()),
                    "_volume_delta": [v for v, _ in delta.values()],
                    "_volume_delta_amount": [a for _, a in delta.values()],
                    "_volume_delta_span": [span] * len(delta),
                }
            )
            drop_cols = [
                c
                for c in ("_volume_delta", "_volume_delta_amount", "_volume_delta_span")
                if c in enriched_today.columns
            ]
            df = enriched_today.drop(drop_cols) if drop_cols else enriched_today
            return df.join(delta_df, on="symbol", how="left")
        except Exception as e:  # noqa: BLE001
            _self.logger.debug("快照差值注入失败 (volume_delta 规则将不触发): %s", e)
            return enriched_today

    def _inject_sealed_vol(self, enriched_today: pl.DataFrame, enriched_date) -> pl.DataFrame:
        """从 depth_service 取封单量, 作为临时列 _sealed_vol 注入 enriched 副本。

        涨停封单(买一量) + 跌停封单(卖一量)合并, 供 ladder 规则评估。
        depth 未就绪时返回原 df (不注入, ladder 规则安全降级不触发)。
        """
        try:
            depth_svc = getattr(self._app_state, "depth_service", None)
            if not depth_svc:
                return enriched_today
            # enriched_date 可能是 date 或字符串, 统一为 date
            from datetime import date as date_cls

            target_date = (
                enriched_date
                if isinstance(enriched_date, date_cls)
                else date_cls.fromisoformat(str(enriched_date))
            )
            # 取涨停 + 跌停封单, 合并 {symbol: vol}
            up_map = depth_svc.get_sealed_map(target_date, is_down=False)
            down_map = depth_svc.get_sealed_map(target_date, is_down=True)
            sealed: dict[str, int] = {}
            for m in (up_map, down_map):
                for sym, info in m.items():
                    vol = (info or {}).get("vol")
                    if vol and vol > 0:
                        sealed[sym] = vol  # 后者覆盖前者 (同 symbol 不可能在涨跌停都封单)
            if not sealed:
                return enriched_today
            # 构造 (symbol, _sealed_vol) DataFrame, join 到 enriched 副本
            sealed_df = pl.DataFrame(
                {
                    "symbol": list(sealed.keys()),
                    "_sealed_vol": list(sealed.values()),
                }
            )
            # 若已有残留列先移除 (避免重复 join 报错)
            df = (
                enriched_today.drop("_sealed_vol")
                if "_sealed_vol" in enriched_today.columns
                else enriched_today
            )
            return df.join(sealed_df, on="symbol", how="left")
        except Exception as e:  # noqa: BLE001
            _self.logger.debug("封单注入失败 (ladder 规则将不触发): %s", e)
            return enriched_today

    def _dispatch_webhook_channels(
        self,
        ev: dict,
        channels,
        feishu_url,
        feishu_secret,
        wecom_url,
        custom_url,
        custom_secret,
        email_config,
        email_password,
    ) -> int:
        from app.services import email_adapter, webhook_adapter

        source = ev.get("source", "")
        source_label = SOURCE_LABELS.get(source, source or "通知")
        symbol = ev.get("symbol") or ""
        name = ev.get("name") or ""
        message = ev.get("message") or ""
        title = source_label
        body = f"{symbol} {name} {message}".strip() if symbol else (message or name)
        body = _self._body_with_quote(body, ev)
        enqueued = 0
        if feishu_url and "feishu" in channels:
            _self._WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_feishu, feishu_url, title, body, feishu_secret
            )
            enqueued += 1
        if wecom_url and "wecom" in channels:
            _self._WEBHOOK_EXECUTOR.submit(webhook_adapter.send_wecom, wecom_url, title, body)
            enqueued += 1
        if custom_url and "custom" in channels:
            _self._WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_custom,
                custom_url,
                title,
                body,
                "monitor_alert",
                ev,
                custom_secret,
            )
            enqueued += 1
        if email_adapter.is_configured(email_config) and "email" in channels:
            _self._WEBHOOK_EXECUTOR.submit(
                email_adapter.send_email,
                email_config,
                email_password,
                title,
                body,
            )
            enqueued += 1
        return enqueued

    def _maybe_send_webhook(self, rule_events: list[dict], engine) -> None:
        """把告警通过 Webhook 推送到外部 IM (由规则 webhook_channels 指定渠道)。

        - 飞书 / 企业微信 / 第三方 Webhook / 邮件均按规则独立选择
        - 仅推送 webhook_channels 非空的规则触发的告警, 且只投递被勾选的渠道
        - 失败静默, 不阻断主流程
        - 去重: 复用 MonitorRuleEngine 的 cooldown, 此处不重复去重

        注意: 用 rule_events (含 rule_id) 而非重建后的 all_alerts,
        以便反查引擎规则判断是否启用推送。
        """
        try:
            from app import secrets_store
            from app.services import email_adapter, preferences

            feishu_url = preferences.get_feishu_webhook_url()
            feishu_secret = preferences.get_feishu_webhook_secret()
            wecom_url = preferences.get_wecom_webhook_url()
            custom_url = preferences.get_custom_webhook_url()
            custom_secret = secrets_store.get_custom_webhook_secret()
            email_config = preferences.get_email_smtp_config()
            email_password = secrets_store.get_email_smtp_password()
            if not any(
                (feishu_url, wecom_url, custom_url, email_adapter.is_configured(email_config))
            ):
                return

            # 反查规则, 过滤出启用推送的事件
            rules = engine.rules if engine is not None else {}
            enqueued = 0
            for ev in rule_events:
                rule = rules.get(ev.get("rule_id"))
                # webhook_channels 指定本规则需要投递的外部渠道。
                # 空列表 = 该规则不推送。仅推送「渠道已选 + 对应地址已配置」的组合。
                channels = rule.get("webhook_channels") if rule else None
                if not channels:
                    continue
                enqueued += self._dispatch_webhook_channels(
                    ev,
                    channels,
                    feishu_url,
                    feishu_secret,
                    wecom_url,
                    custom_url,
                    custom_secret,
                    email_config,
                    email_password,
                )
            if enqueued:
                _self.logger.info(
                    "Webhook 已提交 %d 条 (异步投递, 按渠道独立投递, 失败记 WARNING)", enqueued
                )
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("Webhook 提交异常 (不影响告警主流程): %s", e)

    def _maybe_send_paper_webhook(self, events: list[dict]) -> None:
        """模拟盘成交 → 已配置的飞书/企业微信/自定义 Webhook (异步投递, 失败静默)。

        与监控规则的按规则勾选不同: 成交事件无规则载体, 渠道地址已配置即投递。
        模拟盘成交低频 (手动 + cooldown 规则), 刷屏风险低。
        """
        try:
            from app import secrets_store
            from app.services import preferences, webhook_adapter

            feishu_url = preferences.get_feishu_webhook_url()
            feishu_secret = preferences.get_feishu_webhook_secret()
            wecom_url = preferences.get_wecom_webhook_url()
            custom_url = preferences.get_custom_webhook_url()
            custom_secret = secrets_store.get_custom_webhook_secret()
            if not any((feishu_url, wecom_url, custom_url)):
                return

            enqueued = 0
            for ev in events:
                body = f"{ev.get('symbol', '')} {ev.get('message', '')}".strip()
                if feishu_url:
                    _self._WEBHOOK_EXECUTOR.submit(
                        webhook_adapter.send_feishu, feishu_url, "模拟盘成交", body, feishu_secret
                    )
                    enqueued += 1
                if wecom_url:
                    _self._WEBHOOK_EXECUTOR.submit(
                        webhook_adapter.send_wecom, wecom_url, "模拟盘成交", body
                    )
                    enqueued += 1
                if custom_url:
                    _self._WEBHOOK_EXECUTOR.submit(
                        webhook_adapter.send_custom,
                        custom_url,
                        "模拟盘成交",
                        body,
                        "paper_fill",
                        ev,
                        custom_secret,
                    )
                    enqueued += 1
            if enqueued:
                _self.logger.info(
                    "模拟盘 Webhook 已提交 %d 条 (异步投递, 失败记 WARNING)", enqueued
                )
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("模拟盘 Webhook 提交异常 (不影响主流程): %s", e)

    def _maybe_send_system_notifications(self, all_alerts: list[dict]) -> None:
        """把告警转发到操作系统通知中心 (由 preferences 开关控制)。

        - 开关关闭: 直接返回
        - 开关开启: 逐条发系统通知; 失败静默, 不阻断主流程
        - 去重: 复用 MonitorRuleEngine 的 cooldown, 此处不重复去重
        - 批量策略事件 (symbol="") 聚合为一条通知, 避免刷屏
        - 投递在专用线程池执行 (subprocess 每条最长 5s), 不阻塞行情轮询线程
        """
        try:
            from app.services import preferences

            if not preferences.get_system_notify_enabled():
                return

            for ev in all_alerts:
                # 通知标题: 用 source 分类 (策略/信号/价格/异动)
                source = ev.get("source", "")
                source_label = SOURCE_LABELS.get(source, source or "通知")

                name = ev.get("name") or ""
                symbol = ev.get("symbol") or ""
                message = ev.get("message") or ""

                # 正文: 优先用现成 message, 拼上 symbol/name 让用户一眼定位
                body = f"{symbol} {name} {message}".strip() if symbol else message or name
                # 补上触发时的现价/涨跌幅 (日期提醒无行情, 自然为空)
                body = _self._body_with_quote(body, ev)

                title = f"TickFlow · {source_label}"
                _self._SYSNOTIFY_EXECUTOR.submit(_self._deliver_system_notify, title, body)
        except Exception as e:  # noqa: BLE001
            _self.logger.debug("系统通知发送异常 (不影响告警主流程): %s", e)

    @staticmethod
    def _get_strategy_monitor():
        """获取 StrategyMonitorService — 不再使用, 改用 _app_state 注入。"""
        return None

    # ================================================================
    # enriched 增量计算
    # ================================================================

    @staticmethod
    def _etf_history_files(root: Path, today: date) -> tuple:
        # Daily storage uses date partitions. Probe only the existing 150-day
        # warmup horizon, so old years and today's repeated writes cost no scan.
        files = []
        for offset in range(150, 0, -1):
            folder = root / f"date={today - timedelta(days=offset)}"
            for path in sorted(folder.glob("*.parquet")):
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                files.append(
                    (str(path), stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino)
                )
        return tuple(files)

    def _etf_live_history(self, today: date, columns: list[str]) -> pl.DataFrame | None:
        root = self._repo.store.data_dir / "kline_etf_daily"
        files = self._etf_history_files(root, today)
        if not files:
            self._etf_history_cache = None
            return None  # Preserve the original empty-history fallback.
        key = (str(root), today, files)
        cached = self._etf_history_cache
        if cached is not None and cached[0] == key:
            return cached[1]
        frame = _self.guarded_collect(
            scan_daily_parquet([entry[0] for entry in files])
            .select(columns)
            .filter((pl.col("date") >= today - timedelta(days=150)) & (pl.col("date") != today))
            .sort(["symbol", "date"]),
            priority="background",
        )
        # A bounded immutable input cache only: formulas, adjustment factors and
        # signals are still evaluated on each tick by the original pipeline.
        self._etf_history_cache = None
        if frame.estimated_size() <= 64 * 1024**2 and files == self._etf_history_files(root, today):
            self._etf_history_cache = (key, frame)
        return frame

    def _compute_enriched_incremental(self, daily_df, quote_extra, live_agg, prev_enriched):
        """增量递推只算今天的 enriched。"""
        from app.indicators.pipeline import compute_enriched_today
        from app.market_time import trading_minutes_elapsed, trading_minutes_elapsed_from_ts

        instruments = self._repo.get_instruments()
        # 将 API 直接提供的补充字段 JOIN 到 daily_df
        today_ohlcv = daily_df
        if quote_extra is not None and not quote_extra.is_empty():
            today_ohlcv = daily_df.join(quote_extra, on="symbol", how="left")
        # 量比时间折算: 优先用行情 quote_ts (真实成交时间), 缺失则兜底服务端时间
        elapsed_minutes: float | None = None
        if "quote_ts" in daily_df.columns and not daily_df.is_empty():
            valid_ts = daily_df["quote_ts"].drop_nulls()
            if not valid_ts.is_empty():
                elapsed_minutes = trading_minutes_elapsed_from_ts(valid_ts.median())
        if elapsed_minutes is None:
            elapsed_minutes = trading_minutes_elapsed()
        return compute_enriched_today(
            live_agg=live_agg,
            prev_enriched=prev_enriched,
            today_ohlcv=today_ohlcv,
            instruments=instruments,
            elapsed_minutes=elapsed_minutes,
        )

    def _etf_live_history_window(self, cutoff, daily_glob, ohlcv_cols, asset_type, today):
        """读取 90+60 自然日历史帧; etf 走专用快照。"""
        from datetime import timedelta

        hist_df = self._etf_live_history(today, ohlcv_cols) if asset_type == "etf" else None
        if hist_df is None:
            hist_df = _self.guarded_collect(
                scan_daily_parquet(daily_glob)
                # 多读一段自然日, 再按交易日计数确定窗口起点 (长假前后 90 个自然日不足 60 个交易日)
                .filter(pl.col("date") >= cutoff - timedelta(days=60))
                .select(ohlcv_cols)
                .sort(["symbol", "date"]),
                priority="background",
            )
        return hist_df

    def _load_adj_factors(self, asset_type):
        """加载复权因子; 读取失败 fail-visible 留痕后退化为不复权口径。"""
        factor_dir = {"stock": "adj_factor", "etf": "adj_factor_etf"}.get(asset_type)
        factor_path = self._repo.store.data_dir / factor_dir / "all.parquet" if factor_dir else None
        factors = pl.DataFrame()
        if factor_path and factor_path.exists():
            try:
                factors = pl.read_parquet(factor_path)
            except Exception as e:  # noqa: BLE001
                # fail-visible: 复权因子读失败继续算会让本轮 enriched 以
                # 不复权口径落盘并进缓存, 后续增量轮以此为基准 — 必须留痕
                _self.logger.warning(
                    "%s 复权因子读取失败, 本轮 enriched 退化为不复权口径: %s",
                    asset_type,
                    e,
                )
        return factors

    def _compute_enriched_full(self, daily_df, today, live_agg, prev_date, asset_type, ohlcv_cols):
        """全量回退路径; 返回 enriched_today。"""
        from datetime import timedelta

        from app.indicators.pipeline import compute_enriched
        from app.tickflow.repository import _live_agg_window_start

        _self.logger.info(
            "enriched 全量计算 (live_agg=%s, 上次日期=%s)",
            "ok" if not live_agg.is_empty() else "空",
            prev_date,
        )
        cutoff = today - timedelta(days=90)
        table = {"etf": "kline_etf_daily", "index": "kline_index_daily"}.get(
            asset_type, "kline_daily"
        )
        daily_glob = str(self._repo.store.data_dir / table / "**" / "*.parquet")
        hist_df = self._etf_live_history_window(cutoff, daily_glob, ohlcv_cols, asset_type, today)
        if hist_df.is_empty():
            return pl.DataFrame()
        hist_cols = [c for c in ohlcv_cols if c in hist_df.columns]
        hist_df = hist_df.select(hist_cols).filter(pl.col("date") != today)
        # 与股票盘中递推窗口同口径: 至少覆盖 60 个交易日, 否则 MA60/60 日极值/60 日动量为空
        window_start = _live_agg_window_start(hist_df["date"], today, cutoff)
        hist_df = hist_df.filter(pl.col("date") >= window_start)
        daily_ohlcv = daily_df.select([c for c in ohlcv_cols if c in daily_df.columns])
        full_df = pl.concat([hist_df, daily_ohlcv], how="diagonal_relaxed")
        full_df = full_df.sort(["symbol", "date"])
        factors = self._load_adj_factors(asset_type)
        instruments = self._repo.get_instruments() if asset_type == "stock" else None
        enriched_full = compute_enriched(
            full_df,
            factors=factors,
            instruments=instruments,
            historical_shares=(
                self._repo.get_historical_shares() if asset_type == "stock" else None
            ),
        )
        # momentum_3d 不在指标全集里, 但 deviate_3d 需要; 多日帧上 shift 补算
        enriched_full = enriched_full.sort(["symbol", "date"]).with_columns(
            (pl.col("close") / pl.col("close").shift(3).over("symbol") - 1).alias("momentum_3d")
        )
        return enriched_full.filter(pl.col("date") == today)

    def _flush_live_enriched(
        self,
        daily_df: pl.DataFrame,
        quote_extra: pl.DataFrame = None,
        asset_type: str = "stock",
        merge: bool = False,
    ) -> None:
        """增量计算今天的 enriched: 用昨天的递推状态 + 今天 OHLCV → 只算今天 5500 行。

        quote_extra: API 直接提供的补充字段 (prev_close, change_pct 等),
                     不写 daily, 直接传给 compute_enriched_today 避免重复计算。
        """
        try:
            today = _self.cn_today()
            t0 = time.perf_counter()

            # ---- 尝试增量路径 ----
            live_agg = self._repo.get_live_agg() if asset_type == "stock" else pl.DataFrame()
            prev_enriched, prev_date = (
                self._repo.get_enriched_latest()
                if asset_type == "stock"
                else self._repo.get_enriched_latest_asset(asset_type)
            )

            use_incremental = asset_type == "stock" and not live_agg.is_empty()
            use_incremental = (
                use_incremental and not prev_enriched.is_empty() and prev_date is not None
            )
            ohlcv_cols = _OHLCV_COLS
            if use_incremental:
                enriched_today = self._compute_enriched_incremental(
                    daily_df, quote_extra, live_agg, prev_enriched
                )
                if enriched_today.is_empty():
                    _self.logger.warning("增量计算结果为空, 回退到全量计算")
                    use_incremental = False

            # ---- 全量回退路径 ----
            if not use_incremental:
                enriched_today = self._compute_enriched_full(
                    daily_df, today, live_agg, prev_date, asset_type, ohlcv_cols
                )
            if enriched_today.is_empty():
                return

            # 异动偏离列: 盘中路径不经过 _refresh_enriched 冷刷新,
            # 需在此附着 (基准 = 历史帧 + 指数实时外推), 否则盘中异动列表为空
            if asset_type == "stock":
                from app.indicators.pipeline import attach_deviation_columns_today

                try:
                    index_quotes = self.get_index_quotes()
                except Exception:  # noqa: BLE001 - 获取指数行情失败时降级为无指数快照
                    index_quotes = None
                enriched_today = attach_deviation_columns_today(
                    enriched_today, self._repo.store.data_dir, index_quotes
                )

            # ---- 写盘 + 更新缓存 ----
            if merge:
                self._repo.merge_live_enriched_asset(asset_type, enriched_today)
            else:
                self._repo.flush_live_enriched_asset(asset_type, enriched_today)

            elapsed = time.perf_counter() - t0
            mode_label = "增量" if use_incremental else "全量"
            _self.logger.info(
                "enriched %s: %d 只, %s, 耗时 %.0fms",
                mode_label,
                len(enriched_today),
                today,
                elapsed * 1000,
            )
        except Exception as e:  # noqa: BLE001
            _self.logger.warning("enriched 计算失败: %s", e)
