"""KlineRepository mixin: _QueryMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    EnrichedGenerationUnavailableError,
    cn_today,
    date,
    logger,
    pl,
)


class _QueryMixin(_RepoMixinBase):
    def _assert_daily_usable(self, asset_type="stock", *, symbols=None, end=None) -> None:
        from app.services.daily_health import assert_usable

        root = getattr(getattr(self, "store", None), "data_dir", None)
        if root is not None:
            assert_usable(root, asset_type, symbols=symbols, end=end, enriched_only=True)

    def get_enriched_latest(self, *, symbols=None, end=None) -> tuple[pl.DataFrame, date | None]:
        """返回缓存的 enriched 最新日 DataFrame + 日期。如无缓存则懒加载。

        后台预热期间 (_enriched_warming=True) 返回空表, 不触发同步重算 ——
        否则首次访问会把异步化想避免的 50s+ 计算拉回同步路径。
        """
        self._assert_daily_usable(symbols=symbols, end=end)
        if self._enriched_cache is None:
            if self._enriched_warming:
                return pl.DataFrame(), None
            self._refresh_enriched()
        if self._enriched_cache is None:
            return pl.DataFrame(), self._enriched_cache_date
        frame = self._enriched_cache
        if symbols is not None:
            frame = frame.filter(pl.col("symbol").is_in(symbols))
        return frame, self._enriched_cache_date

    def get_enriched_latest_asset(
        self, asset_type: str, refresh: bool = True
    ) -> tuple[pl.DataFrame, date | None]:
        """按资产类型返回最新 enriched 缓存。stock 保持旧缓存语义。

        refresh=False: 缓存冷时不触发同步 _refresh_etf_enriched(300 天 scan+compute)。
        供行情轮询线程使用 —— 避免在热路径上做重活阻塞股票行情/告警;缓存由 ETF 实时
        flush 焐热, 未焐热(无 ETF 实时数据)时返回空表, 本轮跳过 ETF 评估即可。
        """
        self._assert_daily_usable(asset_type)
        if asset_type == "stock":
            return self.get_enriched_latest()
        if asset_type == "etf":
            if self._etf_enriched_cache is None and refresh:
                self._refresh_etf_enriched()
            if self._etf_enriched_cache is None:
                return pl.DataFrame(), self._etf_enriched_cache_date
            return self._etf_enriched_cache, self._etf_enriched_cache_date
        if asset_type == "index":
            if self._index_enriched_cache is None and refresh:
                self._refresh_index_enriched()
            if self._index_enriched_cache is None:
                return pl.DataFrame(), self._index_enriched_cache_date
            return self._index_enriched_cache, self._index_enriched_cache_date
        return pl.DataFrame(), None

    def remember_prepared_daily(self, asset_type: str, frame: pl.DataFrame) -> None:
        """保留定向补齐的最新展示行，按已有 generation 验证有效性。"""
        if frame.is_empty():
            return
        snapshot = self.get_enriched_generation_snapshot(asset_type)
        latest = frame.sort("date").unique(subset=["symbol"], keep="last")
        with self._prepared_daily_lock:
            for part in latest.partition_by("symbol"):
                key = (asset_type, part["symbol"][0])
                day = part["date"][0]
                self._prepared_daily[key] = (day, snapshot.through(day), part)
                self._prepared_daily.move_to_end(key)
            while len(self._prepared_daily) > 1024:
                self._prepared_daily.popitem(last=False)

    def get_prepared_daily(self, asset_type: str, symbols: list[str]) -> pl.DataFrame:
        """热路径仅读有界快照与版本标记，不扫描历史文件或访问上游。"""
        snapshot = self.get_enriched_generation_snapshot(asset_type)
        parts = []
        with self._prepared_daily_lock:
            for symbol in symbols:
                key = (asset_type, symbol)
                entry = self._prepared_daily.get(key)
                if entry is None:
                    continue
                day, version, frame = entry
                if snapshot.through(day) != version:
                    del self._prepared_daily[key]
                else:
                    parts.append(frame)
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()

    def get_enriched_history(self, target_date: date, lookback_days: int) -> pl.DataFrame | None:
        """返回预计算的 enriched 历史数据 (仅 lookback 范围, 不含 warmup)。

        warmup 部分在 _refresh_enriched 计算指标时已使用, 策略只需要最终的 lookback 窗口。
        返回 ~33万行 (90日历天) 而非 ~107万行, filter_history 策略的 group_by 快 20x+。
        """
        self._assert_daily_usable(end=target_date)
        cache = self._enriched_history_cache
        if cache is None or cache.is_empty():
            return None
        if "date" not in cache.columns:
            return None
        cache_max = cache["date"].max()
        cache_min = cache["date"].min()
        from datetime import timedelta

        # 验证缓存覆盖完整范围 (含 warmup)。lookback_days 是交易日语义, 用 ×2 日历日
        # 放宽确保覆盖 (节假日/周末), 与 warmup 60 一起留足余量。
        warmup_start = target_date - timedelta(days=(lookback_days + 60) * 2)
        if cache_min > warmup_start or cache_max < target_date:
            return None
        # 按交易日计数裁剪: 从数据里实际存在的交易日序列取最后 lookback_days 个交易日。
        # 不能用 timedelta(days=N) (自然日), 否则周末/节假日会让窗口只有 ~N×5/7 个交易日,
        # 导致 filter_history 策略的滚动窗口/行号差(_gap)漏算, 与回测结果不一致。
        # 只数目标日及之前的交易日: 历史日期运行策略时缓存里还有更晚的交易日
        trading_dates = cache["date"].filter(cache["date"] <= target_date).unique().sort()
        if len(trading_dates) > lookback_days:
            lookback_start = trading_dates[-(lookback_days + 1)]
        else:
            lookback_start = trading_dates[0]
        return cache.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= target_date))

    def get_enriched_range(  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
        self,
        start: date,
        end: date,
        symbols: list[str] | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame | None:
        """从预计算 enriched 历史缓存返回完整区间；缓存不覆盖时返回 None。"""
        self._assert_daily_usable(symbols=symbols, end=end)
        if self._enriched_history_cache is None:
            if self._enriched_warming:
                # 后台预热中: 返回 None (缓存不覆盖), 调用方各自走慢路径;
                # 否则请求线程会与预热线程并发重复 300 天全量重算
                # (同 get_enriched_latest 的守卫语义)。
                return None
            self._refresh_enriched()
        cache = self._enriched_history_cache
        data_dir = getattr(getattr(self, "store", None), "data_dir", None)
        if data_dir is not None:
            try:
                current = self.get_enriched_generation_snapshot("stock")
            except EnrichedGenerationUnavailableError:
                return None
            snapshot = self._enriched_history_snapshot
            if snapshot is not None and snapshot[0] is cache:
                if snapshot[1].through(end) != current.through(end):
                    return None
            elif self._enriched_history_generation != current.generation:
                return None
        if cache is None or cache.is_empty() or "date" not in cache.columns:
            return None

        cache_min = cache["date"].min()
        cache_max = cache["date"].max()
        if cache_min > start or cache_max < end:
            return None

        df = cache
        if columns:
            existing = [c for c in columns if c in df.columns]
            if "symbol" not in existing and "symbol" in df.columns:
                existing.insert(0, "symbol")
            if "date" not in existing and "date" in df.columns:
                existing.insert(1, "date")
            df = df.select(list(dict.fromkeys(existing)))
        df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(symbols))
        if columns:
            # 保持旧接口空结果的完整 schema, 非空时沿用请求列校验。
            df = cache.clear() if df.is_empty() else df.select(existing)
        return df.sort(["symbol", "date"])

    def get_enriched_history_span(self) -> tuple[date, date] | None:
        """内存 enriched 历史缓存的可用日期区间 (含端点); 不可用/预热中返回 None。

        纯读 O(1), 不触发刷新: 调用方 (随行情 tick 反复调用的自选 enriched 端点) 需要
        区分「预热中」与「日期超窗」, 而 get_enriched_range 对两者都返回 None。
        """
        cache = self._enriched_history_cache
        start = self._enriched_history_start
        if cache is None or cache.is_empty() or start is None:
            return None
        # 历史缓存与最新日缓存同批写入/清空, end 通常即 _enriched_cache_date;
        # 仅当首次刷新在写 latest 之前中断时缺失, 回退取日期列最大值
        return start, self._enriched_cache_date or cache["date"].max()

    def get_live_agg(self) -> pl.DataFrame:
        """返回盘中实时指标预计算聚合表。如无缓存则懒加载。

        live_agg 的核心列 _prev_consec_up/down (昨日连板数) 取自基准日 enriched。
        基准日由 _live_agg_baseline_date 决定: 盘中(today 有实时分区) 取上一交易日,
        非盘中(磁盘最新日 < today) 取该最新日本身。一旦跨日, 期望基准日会前移,
        旧缓存会把连板数整体少算一档, 故这里除首次懒加载外还要校验基准日是否仍
        符合当前预期, 不符则重建 (无需等盘后管道刷缓存)。

        性能: get_live_agg 被每轮实时行情调用 (expert 档 1s 一次)。跨日只在
        date.today() 翻天时发生, 故先用 today 做廉价的 fast-path (μs 级),
        仅当 today 变化时才查磁盘确认 (DuckDB 扫 132 万行约 100ms+) 并按需重建。
        """
        if self._live_agg_cache is None:
            if self._enriched_warming:
                # 后台预热中: 返回空表, 不触发同步重算 (同 get_enriched_latest 守卫)
                return pl.DataFrame()
            self._refresh_enriched()
            self._live_agg_check_date = cn_today()  # 刚建过, 当天不必再查磁盘
        else:
            today = cn_today()
            if self._live_agg_check_date != today:
                # today 翻天了 (次日开盘首次轮询): 校验基准日是否需要前移重建。
                # 同一天内多次调用直接跳过, 避免每轮都扫 parquet。
                self._live_agg_check_date = today
                disk_latest = self._latest_enriched_date_duckdb()
                if disk_latest is not None:
                    expected = self._live_agg_baseline_date(disk_latest)
                    if self._live_agg_cache_date != expected:
                        logger.info(
                            "live_agg 跨日失效, 重建: 缓存基准=%s, 期望基准=%s",
                            self._live_agg_cache_date,
                            expected,
                        )
                        self._refresh_enriched()
        if self._live_agg_cache is None:
            return pl.DataFrame()
        return self._live_agg_cache

    def get_instruments(self) -> pl.DataFrame:
        """返回缓存的 instruments DataFrame。如无缓存则懒加载。"""
        if self._instruments_cache is None:
            self._refresh_instruments()
        if self._instruments_cache is None:
            return pl.DataFrame()
        return self._instruments_cache

    def get_historical_shares(self) -> pl.DataFrame:
        """读取财务股本历史，并在文件更新后自动刷新缓存。"""
        path = self.store.data_dir / "financials" / "shares" / "part.parquet"
        mtime_ns = path.stat().st_mtime_ns if path.exists() else None
        if self._historical_shares_cache is None or mtime_ns != self._historical_shares_mtime_ns:
            from app.share_capital import load_share_history

            self._historical_shares_cache = load_share_history(self.store.data_dir)
            self._historical_shares_mtime_ns = mtime_ns
        return self._historical_shares_cache

    def get_index_instruments(self) -> pl.DataFrame:
        """返回缓存的指数 instruments DataFrame。如无缓存则懒加载。"""
        if self._index_instruments_cache is None:
            self._refresh_index_instruments()
        if self._index_instruments_cache is None:
            return pl.DataFrame()
        return self._index_instruments_cache

    def get_etf_instruments(self) -> pl.DataFrame:
        """返回缓存的 ETF instruments DataFrame；兼容旧版 instruments_index 中的 ETF。"""
        if self._etf_instruments_cache is None:
            self._refresh_etf_instruments()
        if self._etf_instruments_cache is None:
            return pl.DataFrame()
        return self._etf_instruments_cache

    def get_instruments_asset(self, asset_type: str) -> pl.DataFrame:
        """按资产类型返回 instruments；老 stock 路径保持原样。"""
        if asset_type == "stock":
            return self.get_instruments()
        if asset_type == "index":
            df = self.get_index_instruments()
            if not df.is_empty() and "asset_type" in df.columns:
                return df.filter(pl.col("asset_type") != "etf")
            return df
        if asset_type == "etf":
            return self.get_etf_instruments()
        return pl.DataFrame()

    def get_index_symbol_set(self) -> set[str]:
        """返回已缓存指数 symbol 集合 (memo, 随 instruments 缓存失效)。"""
        if self._index_symbol_set_cache is None:
            df = self.get_index_instruments()
            if df.is_empty() or "symbol" not in df.columns:
                return set()
            self._index_symbol_set_cache = set(df["symbol"].cast(pl.Utf8).to_list())
        return self._index_symbol_set_cache

    def get_etf_symbol_set(self) -> set[str]:
        """返回已缓存 ETF symbol 集合 (memo, 随 instruments 缓存失效)。"""
        if self._etf_symbol_set_cache is None:
            df = self.get_etf_instruments()
            if df.is_empty() or "symbol" not in df.columns:
                return set()
            self._etf_symbol_set_cache = set(df["symbol"].cast(pl.Utf8).to_list())
        return self._etf_symbol_set_cache

    def resolve_asset_type(self, symbol: str) -> str:
        """按 symbol 判定资产类型: etf / index / stock(默认)。

        供 API 层对单标的查询做资产分流 (get_daily_asset 等)。
        ETF/指数集合为 memo, 每请求查询成本可忽略。
        """
        if symbol in self.get_etf_symbol_set():
            return "etf"
        if symbol in self.get_index_symbol_set():
            return "index"
        return "stock"

    def get_name_map(self, symbols: list[str] | None = None) -> dict[str, str]:
        """返回 {symbol: name} 映射, 合并股票 + ETF + 指数 instruments (股票优先去重)。

        自选列表/名称批查等场景的统一名称解析入口, 避免各调用方自行合并两份缓存。
        symbols 非 None 时只返回命中的条目。
        全量结果缓存在 _name_map_cache (随三份 instruments 维表刷新失效),
        避免每请求对 ~7000 行维表做 iter_rows 重建。
        """
        if self._name_map_cache is not None:
            if symbols is None:
                return dict(self._name_map_cache)
            wanted = set(symbols)
            return {s: n for s, n in self._name_map_cache.items() if s in wanted}
        # 只构建并缓存全量映射; symbols 过滤只作用于返回值。
        # 若把过滤后的结果写入缓存, 后续不同 symbols 的查询会命中残缺缓存,
        # 导致新加入自选的标的查不到名称。
        name_map: dict[str, str] = {}
        for df in (
            self.get_instruments(),
            self.get_etf_instruments(),
            self.get_instruments_asset("index"),
        ):
            if df.is_empty() or "symbol" not in df.columns or "name" not in df.columns:
                continue
            for symbol, name in df.select(["symbol", "name"]).iter_rows():
                name_map.setdefault(symbol, name)
        self._name_map_cache = name_map
        if symbols is None:
            return dict(name_map)
        wanted = set(symbols)
        return {s: n for s, n in name_map.items() if s in wanted}

    def enriched_latest_date(self) -> date | None:
        """返回缓存中的 enriched 最新日期。"""
        return self._enriched_cache_date
