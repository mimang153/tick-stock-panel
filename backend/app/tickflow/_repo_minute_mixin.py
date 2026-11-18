"""KlineRepository mixin: _MinuteMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    Path,
    apply_minute_adjustment,
    date,
    guarded_collect,
    logger,
    minute_basis_is_raw,
    pl,
    scan_parquet_compat,
)


class _MinuteMixin(_RepoMixinBase):
    def _minute_glob_for(self, asset_type: str) -> str:
        """按资产类型选择分钟K parquet glob。ETF 分钟数据独立存储于 kline_etf_minute。"""
        return self._etf_minute_glob if asset_type == "etf" else self._minute_glob

    def _maybe_adjust_minute(self, df: pl.DataFrame, asset_type: str) -> pl.DataFrame:
        """原始基准标记开启时应用读取时复权投影 (services/minute_adjust 三层架构)。

        标记未开启 (存量未迁移) 原样返回, 行为与旧版逐字节一致; 投影异常也按原样
        返回 (fail-open, 与日K缺因子语义一致), 不让复权层破坏数据可用性。
        """
        if df.is_empty() or not minute_basis_is_raw(self.store.data_dir):
            return df
        try:
            return apply_minute_adjustment(df, self.store.data_dir, asset_type)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("分钟复权投影失败, 按原始数据返回: %s", e)
            return df

    def _minute_partition_path(self, asset_type: str, trade_date: date) -> str | None:
        """单日分钟K分区文件路径; 不存在 (含存量未迁移的旧 symbol= 分区) 返回 None。"""
        base = self._minute_glob_for(asset_type).rsplit("/", 2)[0]
        p = f"{base}/date={trade_date.isoformat()}/part.parquet"
        return p if Path(p).exists() else None

    def get_minute(
        self,
        symbol: str,
        trade_date: date,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """分钟K查询 — 优先直读当日分区文件 (免全树 footer IO), 分区缺失回退全树扫描。

        该查询在自选分时热路径上被每 tick、每客户端调用, 全树 glob 扫描成本随
        本地分钟历史线性增长; 单日数据按约定完整落在 date= 分区内, 谓词不变。
        """
        try:
            part = self._minute_partition_path(asset_type, trade_date)
            source = part if part is not None else self._minute_glob_for(asset_type)
            df = guarded_collect(
                scan_parquet_compat(source)
                .filter((pl.col("symbol") == symbol) & (pl.col("datetime").dt.date() == trade_date))
                .sort("datetime")
            )
            return self._maybe_adjust_minute(df, asset_type)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("分钟K查询失败: %s", e)
            return pl.DataFrame()

    def get_minute_batch(
        self,
        symbols: list[str],
        trade_date: date,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """批量分钟K查询 — 多 symbol 一次 scan, 优先直读当日分区文件。

        用于自选列表分时图: 一次 predicate pushdown 读多只股票当日分钟K,
        避免逐只查询的 N 次 I/O; 单日直读避免全树扫描 (同 get_minute)。
        """
        if not symbols:
            return pl.DataFrame()
        try:
            part = self._minute_partition_path(asset_type, trade_date)
            source = part if part is not None else self._minute_glob_for(asset_type)
            df = guarded_collect(
                scan_parquet_compat(source)
                .filter(
                    pl.col("symbol").is_in(symbols) & (pl.col("datetime").dt.date() == trade_date)
                )
                .sort(["symbol", "datetime"])
            )
            return self._maybe_adjust_minute(df, asset_type)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("批量分钟K查询失败: %s", e)
            return pl.DataFrame()

    def get_minute_range(
        self,
        symbols: list[str],
        start: date,
        end: date,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """多 symbol × 日期范围的分钟K查询 (分钟K精确回测用)。

        一次 scan_parquet + predicate pushdown 读多只股票在 [start, end] 内的所有分钟K。
        返回列: symbol, datetime, open, high, low, close, volume, amount。
        """
        if not symbols:
            return pl.DataFrame()
        try:
            lf = scan_parquet_compat(self._minute_glob_for(asset_type))
            available = set(lf.collect_schema().names())
            select_cols = [
                c
                for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
                if c in available
            ]
            df = guarded_collect(
                lf.select(select_cols)
                .filter(
                    pl.col("symbol").is_in(symbols)
                    & (pl.col("datetime").dt.date() >= start)
                    & (pl.col("datetime").dt.date() <= end)
                )
                .sort(["symbol", "datetime"]),
                streaming=True,
            )
            return self._maybe_adjust_minute(df, asset_type)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("分钟K范围查询失败: %s", e)
            return pl.DataFrame()

    def get_minute_by_dates(
        self,
        symbols: list[str],
        dates: list[date],
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """按日期列表精确读取分钟K分区文件 (分钟K精确回测用)。

        与 get_minute_range 的区别: 后者扫描 [start, end] 区间全部日期的 parquet
        (触发日稀疏时会读大量无关日期 → 爆内存); 本方法只读 dates 里列出的日期
        对应的分区文件 (date=YYYY-MM-DD/part.parquet), 内存与回测区间长度解耦,
        只随触发日数量增长。

        缺失的日期分区直接跳过 (该日无分钟K数据)。
        返回列: symbol, datetime, open, high, low, close, volume, amount。
        """
        if not symbols or not dates:
            return pl.DataFrame()
        base = (
            self._etf_minute_glob.rsplit("/", 2)[0]
            if asset_type == "etf"
            else self._minute_glob.rsplit("/", 2)[0]
        )
        # 收集存在的分区文件路径, 避免对不存在的文件 scan 报错
        parts: list[str] = []
        for d in dates:
            p = f"{base}/date={d.isoformat()}/part.parquet"
            if Path(p).exists():
                parts.append(p)
        if not parts:
            return pl.DataFrame()
        try:
            lf = scan_parquet_compat(parts)
            available = set(lf.collect_schema().names())
            select_cols = [
                c
                for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
                if c in available
            ]
            df = guarded_collect(
                lf.select(select_cols)
                .filter(pl.col("symbol").is_in(symbols))
                .sort(["symbol", "datetime"]),
                streaming=True,
            )
            return self._maybe_adjust_minute(df, asset_type)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("分钟K按日期查询失败: %s", e)
            return pl.DataFrame()
