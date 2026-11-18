"""Screener 服务(§6.3)。

性能优化:
  - enriched parquet 仅存 14 列基础数据, 指标和信号即时计算
  - preset 策略: 从内存缓存或即时计算获取完整指标, ~10-50ms
  - custom SQL: DuckDB (用户传 SQL WHERE 字符串), ~10-50ms
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from app.tickflow.repository import KlineRepository

logger = logging.getLogger(__name__)

# 常用指标 (MA5/10/20、BOLL(20)、量比等) 的近似最小暖机窗口。
# 用于没有精确回看需求的场景 (自定义 SQL 筛选、盘后管道) 的数据充足性提示 (#303);
# 策略运行用 engine.required_history_bars 的精确值。
MIN_INDICATOR_WARMUP_DAYS = 30


def enriched_history_days(
    data_dir=None, asset_type: str = "stock", as_of: date | None = None
) -> int:
    """enriched 在 as_of 及之前覆盖的交易日数 (#303)。

    ``data_dir`` 仅为兼容既有调用签名保留；计数走 CK 的 distinct date，
    只做数据充足性提示，不读取指标宽表。
    """
    from app.storage.runtime import get_storage
    from app.tickflow.repository import enriched_dirname

    runtime = get_storage()
    if runtime is None:
        return 0
    try:
        return len(runtime.store.list_daily_dates(enriched_dirname(asset_type), end=as_of))
    except Exception as e:  # noqa: BLE001 - 仅提示用途，失败按 0 天处理
        logger.warning("enriched 天数统计失败: %s", e)
        return 0


# ── 进程级历史数据缓存 (避免 run_all 每次重新扫描 parquet + 计算指标) ──
_history_cache: dict[tuple[str, date, int], tuple[float, pl.DataFrame]] = {}
_HISTORY_CACHE_TTL = 120.0  # 秒

# load_prior_consecutive 最多回看多少个已存在的日分区 (缺列时继续往前找的上限)
_PRIOR_PARTITION_SCAN = 10


def _join_instruments(df: pl.DataFrame, instruments: pl.DataFrame) -> pl.DataFrame:
    if instruments.is_empty():
        return df
    if "name" not in df.columns:
        columns = [
            c
            for c in ["symbol", "name", "total_shares", "float_shares"]
            if c in instruments.columns
        ]
        return df.join(instruments.select(columns), on="symbol", how="left")
    if "name" not in instruments.columns:
        return df
    missing = pl.col("name").is_null() | (pl.col("name").cast(pl.Utf8).str.strip_chars() == "")
    if not df.select(missing.any()).item():
        return df
    # 实时/历史缓存可能已有 name 列却只有部分名称，按行补齐且保持缓存本身不变。
    return (
        df.join(
            instruments.select("symbol", pl.col("name").alias("_instrument_name")),
            on="symbol",
            how="left",
            maintain_order="left",
        )
        .with_columns(
            pl.when(missing)
            .then(pl.col("_instrument_name").cast(pl.Utf8).str.strip_chars().replace("", None))
            .otherwise(pl.col("name"))
            .alias("name")
        )
        .drop("_instrument_name")
    )


@dataclass
class ScreenerResult:
    as_of: date
    strategy: str | None
    rows: list[dict] = field(default_factory=list)
    total: int = 0
    elapsed_ms: float = 0.0


class ScreenerService:
    def __init__(self, repo: KlineRepository, asset_type: str = "stock") -> None:
        self.repo = repo
        self.asset_type = asset_type
        from app.tickflow.repository import enriched_dirname

        self._enriched_dirname = enriched_dirname(asset_type)

    @staticmethod
    def clear_history_cache(asset_type: str | None = None) -> None:
        """清空进程级 _history_cache (TTL 缓存)。

        清除数据后调用, 避免内存里的旧历史窗口残留导致策略/看板仍命中旧数据。
        """
        if asset_type is None:
            _history_cache.clear()
        else:
            for key in list(_history_cache):
                if key[0] == asset_type:
                    _history_cache.pop(key, None)

    def _assert_integrity(self, end: date, *, enriched_only=False) -> None:
        from app.services.daily_health import assert_usable

        root = getattr(getattr(self.repo, "store", None), "data_dir", None)
        if root is not None:
            assert_usable(root, self.asset_type, end=end, enriched_only=enriched_only)

    def _enriched_store(self):
        """enriched 存储门面（CK 唯一读源）。"""
        from app.storage.runtime import require_storage

        return require_storage().store

    def _read_enriched_window(
        self,
        start: date,
        end: date,
        columns: list[str],
    ) -> pl.DataFrame:
        """按窗口读 enriched 存储列（列裁剪 + 主键裁剪），未知列自动丢弃。"""
        store = self._enriched_store()
        known = set(store.table_columns(self._enriched_dirname))
        available = [c for c in columns if c in known]
        if not available:
            return pl.DataFrame()
        df = store.read_daily(
            self._enriched_dirname,
            start=start,
            end=end,
            columns=available,
        )
        sort_keys = [c for c in ("symbol", "date") if c in df.columns]
        return df.sort(sort_keys) if sort_keys else df

    def _load_enriched_for_date(self, target_date: date) -> pl.DataFrame:
        """从 enriched parquet 读取指定日期的基础数据并即时计算完整指标+信号。

        enriched parquet 仅存 14 列。读取后需要即时计算 ma/ema/macd/kdj/rsi/boll/momentum/signal 等列。
        对于最新日, 优先使用内存缓存 (已包含完整指标)。
        """
        self._assert_integrity(target_date, enriched_only=True)
        # 优先使用 repo 最新日缓存
        cache, cache_date = self.repo.get_enriched_latest_asset(self.asset_type)
        if cache is not None and not cache.is_empty() and cache_date == target_date:
            return _join_instruments(cache, self.repo.get_instruments_asset(self.asset_type))

        # 尝试从 repo 级预计算历史缓存中提取目标日期 (仅 stock: 该缓存为股票专用)
        if self.asset_type == "stock":
            cached_hist = self.repo.get_enriched_history(target_date, 1)
            if (
                cached_hist is not None
                and not cached_hist.is_empty()
                and "date" in cached_hist.columns
            ):
                df = cached_hist.filter(pl.col("date") == target_date)
                if not df.is_empty():
                    logger.debug("_load_enriched_for_date: repo history cache for %s", target_date)
                    return _join_instruments(df, self.repo.get_instruments_asset(self.asset_type))

        # 历史日期: 从 CK 读取存储列, 即时计算指标 (慢路径)
        try:
            df = self._read_enriched_window(
                target_date,
                target_date,
                [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "raw_close",
                    "raw_high",
                    "raw_low",
                    "turnover_rate",
                ],
            )
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("load_enriched_for_date failed: %s", e)
            return pl.DataFrame()

        if df.is_empty():
            return df

        # 即时计算指标: 需要加载历史窗口作 warmup
        df_full = self._compute_enriched_full(df, target_date)
        return df_full

    def load_prior_consecutive(self, as_of: date, consec_col: str) -> pl.DataFrame:
        """窄读: 仅取前一交易日的 [symbol, consec_col] 两列 (谓词下推到单日 parquet)。

        consecutive_limit_ups / consecutive_limit_downs 是 enriched 的存储列,
        可直接从 parquet 读取, 无需 _load_enriched_for_date 的全量指标重算
        (历史日期该慢路径最坏会触发 9 次全市场 compute_enriched_full)。

        由近到远取 as_of 之前已存在的日分区 (即前一交易日), 读取其
        symbol + consec_col。存储列的值与重算值逐位一致 (连板计数为 run-length,
        150 天 warmup 完全覆盖 A 股最长连板, 二者相等)。

        返回列: symbol, prev_consec。找不到前一交易日时返回空 DataFrame。
        """
        store = self._enriched_store()
        known = set(store.table_columns(self._enriched_dirname))
        # 存储列理论上必含 consec_col; 缺列时直接返回空, 不再逐日试探
        if "symbol" not in known or consec_col not in known:
            return pl.DataFrame()
        for candidate in self._prior_partition_dates(as_of, _PRIOR_PARTITION_SCAN):
            try:
                df = store.read_daily(
                    self._enriched_dirname,
                    start=candidate,
                    end=candidate,
                    columns=["symbol", consec_col],
                )
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("load_prior_consecutive read failed for %s: %s", candidate, e)
                return pl.DataFrame()
            if df.is_empty():
                continue
            return df.rename({consec_col: "prev_consec"})
        return pl.DataFrame()

    def _prior_partition_dates(self, as_of: date, limit: int) -> list[date]:
        """enriched 目录里早于 as_of 的分区日期, 由近到远最多 limit 个。

        枚举分区目录而不是按自然日回看固定天数: 春节长假连着调休周末,
        相邻两个交易日能隔 10~11 个自然日 (如 2024-02-08 → 2024-02-19),
        固定窗口会整段落空。与 auction_benchmark._prev_trading_day
        「本地日K分区日期 = 已知交易日集合」同口径。
        """
        try:
            days: list[date] = self._enriched_store().list_daily_dates(
                self._enriched_dirname,
                end=as_of - timedelta(days=1),
            )
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("enriched 交易日枚举失败: %s", e)
            return []
        days.sort(reverse=True)
        return days[:limit]

    def _compute_enriched_full(self, df_target: pl.DataFrame, target_date: date) -> pl.DataFrame:
        """从 14 列基础数据即时计算完整 enriched (含全部指标和信号)。

        读取历史数据作为指标计算的 warmup, 计算完成后只返回目标日期的行。
        """
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
        )

        # 加载 warmup 历史 (目标日期前 ~120 天)
        start = target_date - timedelta(days=150)
        # turnover_rate 是 enriched 存储列, 必须随行透传: 否则即时计算后该列
        # 丢失, 自定义 SQL 用它做条件会 Binder Error 被吞成空结果 (#187)
        read_cols = [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "raw_close",
            "raw_high",
            "raw_low",
            "turnover_rate",
        ]

        try:
            df_hist = self._read_enriched_window(start, target_date, read_cols)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("warmup history load failed: %s", e)
            df_hist = df_target

        if df_hist.is_empty():
            df_hist = df_target

        # 计算指标
        df_full = compute_indicators(df_hist)
        df_full = compute_signals(df_full)

        # 计算涨跌停信号 (需要 instruments; 涨停为股票专有, ETF 跳过)
        instruments = self.repo.get_instruments_asset(self.asset_type)
        if self.asset_type == "stock" and instruments is not None and not instruments.is_empty():
            df_full = compute_limit_signals(
                df_full,
                instruments,
                historical_shares=self.repo.get_historical_shares(),
            )

        # 只保留目标日期
        df_result = df_full.filter(pl.col("date") == target_date)

        return _join_instruments(df_result, instruments)

    # ---- _load_enriched_history 分段 helper (语义等价) ----

    def _enriched_repo_cache(self, target_date, lookback_days, t0):
        # 优先级 1: repo 级预计算缓存 (启动时 _refresh_enriched 已计算完整历史; 仅 stock)
        if self.asset_type == "stock":
            cached = self.repo.get_enriched_history(target_date, lookback_days)
            if cached is not None and not cached.is_empty():
                # JOIN instruments (repo 缓存不含 name 等列)
                instruments = self.repo.get_instruments_asset(self.asset_type)
                if (
                    instruments is not None
                    and not instruments.is_empty()
                    and "name" not in cached.columns
                ):
                    inst_cols = [
                        c
                        for c in ["symbol", "name", "total_shares", "float_shares"]
                        if c in instruments.columns
                    ]
                    cached = cached.join(instruments.select(inst_cols), on="symbol", how="left")
                elapsed = (time.perf_counter() - t0) * 1000
                logger.info(
                    "_load_enriched_history(%s, %d): repo cache hit, %.1fms, %d rows",
                    target_date,
                    lookback_days,
                    elapsed,
                    len(cached),
                )
                return cached
        return None

    def _enriched_ttl_cache(self, cache_key, target_date, lookback_days):
        # 优先级 2: 进程级 history_cache (之前的 TTL 缓存)
        now = time.monotonic()
        ttl_cached = _history_cache.get(cache_key)
        if ttl_cached is not None:
            ts, cached_df = ttl_cached
            if now - ts < _HISTORY_CACHE_TTL:
                logger.debug("history TTL cache hit: %s lookback=%d", target_date, lookback_days)
                return cached_df, now
            del _history_cache[cache_key]
        return None, now

    def _enriched_compute_window(self, target_date, lookback_days, start, read_cols):
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
        )

        try:
            df_hist = self._read_enriched_window(start, target_date, read_cols)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("load_enriched_history failed: %s", e)
            return None
        if df_hist.is_empty():
            return None

        df_full = compute_indicators(df_hist)
        df_full = compute_signals(df_full)

        instruments = self.repo.get_instruments_asset(self.asset_type)
        if self.asset_type == "stock" and instruments is not None and not instruments.is_empty():
            df_full = compute_limit_signals(
                df_full,
                instruments,
                historical_shares=self.repo.get_historical_shares(),
            )

        if instruments is not None and not instruments.is_empty():
            inst_cols = [
                c
                for c in ["symbol", "name", "total_shares", "float_shares"]
                if c in instruments.columns
            ]
            if "name" not in df_full.columns:
                df_full = df_full.join(instruments.select(inst_cols), on="symbol", how="left")

        # 裁剪掉 warmup 部分, 只保留 lookback 范围 (减少 group_by 开销)。
        # 按交易日计数: 从数据里实际存在的交易日序列取最后 lookback_days 个交易日,
        # 不能用 timedelta(days=N) (自然日), 否则周末/节假日会让窗口偏少, 与回测不一致。
        if "date" in df_full.columns:
            trading_dates = df_full["date"].unique().sort()
            if len(trading_dates) > lookback_days:
                lookback_start = trading_dates[-(lookback_days + 1)]
            else:
                lookback_start = trading_dates[0]
            df_full = df_full.filter(pl.col("date") >= lookback_start)

        return df_full.sort(["symbol", "date"])

    def _enriched_cache_store(self, cache_key, now, df_full):
        _history_cache[cache_key] = (now, df_full)
        # TTL 先清一轮; 仍超上限时按最旧无条件淘汰 — 否则两分钟内出现 >10 个
        # 不同 (asset_type, date, lookback) 键时全部新鲜、零逐出, 数 GB 宽帧驻留
        if len(_history_cache) > 10:
            expired = [k for k, (ts, _) in _history_cache.items() if now - ts > _HISTORY_CACHE_TTL]
            for k in expired:
                del _history_cache[k]
            while len(_history_cache) > 10:
                oldest = min(_history_cache, key=lambda k: _history_cache[k][0])
                del _history_cache[oldest]

    def _load_enriched_history(self, target_date: date, lookback_days: int) -> pl.DataFrame:
        """读取目标日期之前的基础行情数据, 供历史窗口策略使用。

        优先从 repo 内存缓存获取 (启动时已预计算), 命中时 0ms。
        缓存 miss 时走 scan_parquet + compute_indicators 慢路径。
        """
        self._assert_integrity(target_date, enriched_only=True)
        t0 = time.perf_counter()
        cached = self._enriched_repo_cache(target_date, lookback_days, t0)
        if cached is not None:
            return cached

        cache_key = (self.asset_type, target_date, lookback_days)
        ttl_cached, now = self._enriched_ttl_cache(cache_key, target_date, lookback_days)
        if ttl_cached is not None:
            return ttl_cached

        # 优先级 3: scan_parquet + compute_indicators (慢路径, ~5s)
        logger.warning(
            "_load_enriched_history cache miss, computing indicators (%s, %d)...",
            target_date,
            lookback_days,
        )
        warmup = 60
        start = target_date - timedelta(days=min((lookback_days + warmup) * 2, 180))

        # 同 _compute_enriched_full: turnover_rate 存储列随行透传 (#187)
        read_cols = [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "raw_close",
            "raw_high",
            "raw_low",
            "turnover_rate",
        ]
        df_full = self._enriched_compute_window(target_date, lookback_days, start, read_cols)
        if df_full is None:
            return pl.DataFrame()

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            "_load_enriched_history(%s, %d): computed in %.1fms, %d rows",
            target_date,
            lookback_days,
            elapsed,
            len(df_full),
        )
        self._enriched_cache_store(cache_key, now, df_full)
        return df_full

    def run(
        self,
        as_of: date,
        conditions: list[str],
        order_by: str | None = None,
        limit: int = 30,
        pool: list[str] | None = None,
    ) -> ScreenerResult:
        """自定义 SQL 条件筛选。

        先通过 Polars 即时计算完整指标, 再用 DuckDB 做 SQL WHERE 过滤。
        kline_enriched DuckDB 视图只有 14 列, 不能直接用于指标过滤。
        """
        self._assert_integrity(as_of)
        t0 = time.perf_counter()

        if not conditions:
            return ScreenerResult(as_of=as_of, strategy=None)

        # 从即时计算获取完整 enriched 数据
        df = self._load_enriched_for_date(as_of)
        if df.is_empty():
            return ScreenerResult(as_of=as_of, strategy=None)

        # Pool 过滤
        if pool:
            df = df.filter(pl.col("symbol").is_in(pool))

        # 用 DuckDB 做 SQL 过滤 (注册临时视图)
        # 用独立的 :memory: 连接 (而非复用 repo 共享连接的 cursor): conditions 是用户
        # 传入的 SQL 片段, 隔离连接下注入至多能碰 read_csv/read_parquet 文件; 若复用共享
        # 连接则会把 app 已注册的真实业务表也暴露给注入, 扩大攻击面。隔离连接创建开销极低。
        # 再关闭 external_access, 让注入的文件读写函数 (read_parquet/COPY 等) 直接报错,
        # 视图数据仍通过 con.register 注入, 不受该开关影响 (#224)。
        con = None
        try:
            import duckdb

            con = duckdb.connect(database=":memory:", config={"enable_external_access": False})
            con.register("enriched", df.to_arrow())
            where = " AND ".join(f"({c})" for c in conditions)
            sql = f"SELECT * FROM enriched WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            if limit:
                sql += f" LIMIT {limit}"
            df_result = con.execute(sql).pl()
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("screener SQL query failed: %s", e)
            df_result = pl.DataFrame()
        finally:
            if con is not None:
                try:  # noqa: SIM105 - SIM105: 显式except表达降级路径, contextlib写法更晦涩
                    con.close()
                except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                    pass

        rows = df_result.to_dicts() if not df_result.is_empty() else []
        elapsed = (time.perf_counter() - t0) * 1000

        return ScreenerResult(
            as_of=as_of,
            strategy=None,
            rows=rows,
            total=len(rows),
            elapsed_ms=elapsed,
        )

    def build_strategy_context(
        self,
        engine,
        as_of: date,
        strategy_ids: list[str],
        *,
        timeframe: str = "1d",
        params_map: dict[str, dict] | None = None,
        overrides_map: dict[str, dict] | None = None,
        current: pl.DataFrame | None = None,
        market=None,
        cache_key: str | None = None,
    ):
        """按调用方要求装配标准策略数据上下文，不解释策略公式。"""
        from app.strategy.engine import StrategyDataContext

        self._assert_integrity(as_of)
        if current is None:
            current = self._load_enriched_for_date(as_of)
        if timeframe == "1m":
            # 分钟策略数据源是本地当日分钟K分区 (单分区文件直读), 与日线
            # enriched 历史窗口无关, 不走 required_history_bars 日线路径。
            history = self._load_minute_history(as_of, current)
            # 策略声明 META["daily_history_bars"] 时额外装配日线 enriched 窗口,
            # 供分钟策略叠加日线维度条件 (如 N 日内涨停过)。
            daily_history = None
            if engine is not None:
                daily_bars = engine.minute_daily_history_bars(strategy_ids)
                if daily_bars > 0:
                    daily_history = self._load_enriched_history(as_of, daily_bars)
            return StrategyDataContext(
                asset_type=self.asset_type,
                timeframe=timeframe,
                as_of=as_of,
                current=current,
                history=history,
                daily_history=daily_history,
                market=None,
                cache_key=cache_key,
            )
        history_bars = engine.required_history_bars(
            strategy_ids,
            params_map=params_map,
            overrides_map=overrides_map,
        )
        history = None
        if history_bars > 1:
            history = self._load_enriched_history(as_of, history_bars)
        # enriched 换代号: 供 engine.build_shared_matrix 的矩阵缓存做失效指纹
        # (取不到时 None → 缓存 fail-closed 不启用)
        try:
            data_generation = self.repo.get_matrix_data_generation(self.asset_type)
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            data_generation = None
        return StrategyDataContext(
            asset_type=self.asset_type,
            timeframe=timeframe,
            as_of=as_of,
            current=current,
            history=history,
            market=market,
            cache_key=cache_key,
            data_generation=data_generation,
        )

    def _load_minute_history(self, as_of: date, current: pl.DataFrame | None) -> pl.DataFrame:
        """分钟策略数据源: 优先 as_of 当日分钟分区, 缺失时回退全市场最近分区。

        只按日期直读单个分区文件 (get_minute_by_dates), 与全量 glob 扫描解耦,
        内存只随当日分区大小 (~67万行) 走。标的池限定为 enriched 快照 universe;
        分区与快照的日期差是允许的 (分钟分区可能比 enriched 更新, 行自带时间戳)。
        """
        if self.asset_type != "stock":
            raise ValueError("分钟策略当前仅支持 A 股")
        symbols: list[str] = []
        if current is not None and not current.is_empty():
            symbols = current["symbol"].cast(pl.Utf8).unique().to_list()
        if not symbols:
            return pl.DataFrame()
        df = self.repo.get_minute_by_dates(symbols, [as_of])
        if df.is_empty():
            fallback = self.repo.latest_minute_date_global()
            if fallback is None:
                raise ValueError("无分钟K数据 — 请先在 数据→分钟K 完成同步, 或开启盘中增量刷新")
            if fallback != as_of:
                df = self.repo.get_minute_by_dates(symbols, [fallback])
        return df

    def latest_date(self) -> date | None:
        if self.asset_type != "stock":
            _, d = self.repo.get_enriched_latest_asset(self.asset_type)
            return d
        d = self.repo.enriched_latest_date()
        if d:
            return d
        # 回退 CK enriched 最新日期
        try:
            store = self._enriched_store()
            return store.latest_daily_date("kline_daily_enriched")
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def coverage_warnings(self, as_of: date, *, required_bars: int | None = None) -> list[str]:
        """数据充足性提示 (#303): enriched 覆盖不足时返回用户可读警告, 充足返回 []。

        空库首跑只拉到 1 个交易日时, 均线/动量/量比等指标暖机不足, 策略会静默
        全 0 — 这里把"数据不够"显式说出来。required_bars 缺省用通用暖机窗口
        (自定义 SQL 筛选); 策略运行传 engine.required_history_bars 的精确值。
        available 为 0 时 enriched 为空, 上层 latest_date 已 400, 不重复提示。
        """
        available = enriched_history_days(self.repo.store.data_dir, self.asset_type, as_of)
        if available == 0:
            return []
        need = required_bars if required_bars and required_bars > 0 else MIN_INDICATOR_WARMUP_DAYS
        if available >= need:
            return []
        return [
            f"本地数据仅覆盖 {available} 个交易日, 低于本次计算所需约 {need} 天暖机窗口 — "
            "指标可能失真或全部落空 (策略命中 0)。建议先全量回填日K并重算指标 "
            "(数据页「日K批量同步」, 符号需带交易所后缀)"
        ]
