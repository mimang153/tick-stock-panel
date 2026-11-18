"""KlineRepository mixin: _MinuteQueryMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    EnrichedGenerationSnapshot,
    bump_enriched_generation,
    date,
    enriched_dirname,
    get_enriched_generation,
    get_enriched_generation_snapshot,
    pl,
)


class _MinuteQueryMixin(_RepoMixinBase):
    # ================================================================
    # DuckDB 查询 (冷路径: 统计/元数据/自定义SQL)
    # ================================================================

    def latest_minute_date(self, symbol: str, asset_type: str = "stock") -> date | None:
        """个股最近分钟K日期（CK kline_minute 唯一读源）。"""
        table = "kline_etf_minute" if asset_type == "etf" else "kline_minute"
        try:
            store = self._ck_store()
            if store is None:
                return None
            df = store.read_minute(table, symbols=[symbol], columns=["datetime"])
            if df.is_empty():
                return None
            return df["datetime"].max().date()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def latest_minute_date_global(self) -> date | None:
        """全市场最近分钟K日期 (不分 symbol)。用于非交易日回退到上一交易日。"""
        try:
            store = self._ck_store()
            if store is None:
                return None
            latest = store.latest_minute_datetime("kline_minute")
            return latest.date() if latest is not None else None
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def earliest_daily_date(self) -> date | None:
        """本地日K数据的最早日期（CK 唯一读源）。"""
        try:
            store = self._ck_store()
            if store is None:
                return None
            dates = store.list_daily_dates("kline_daily")
            return dates[0] if dates else None
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def earliest_minute_date(self) -> date | None:
        """本地分钟K数据的最早日期（CK 唯一读源）。"""
        try:
            store = self._ck_store()
            if store is None:
                return None
            stats = store.table_stats("kline_minute", date_column="datetime")
            earliest = stats.get("earliest_date") if stats else None
            return date.fromisoformat(str(earliest)[:10]) if earliest else None
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def list_minute_dates(self, start: date, end: date, asset_type: str = "stock") -> list[date]:
        """枚举 [start, end] 内存在的分钟K分区日 (目录名直读, 零 parquet 扫描)。

        分钟回测按交易日精确对日: 缺分区的日子由调用方显式跳过,
        不做"回退最近分区" (那是实盘策略的语义, 回放会串日)。
        """
        dirname = "kline_minute" if asset_type == "stock" else f"kline_{asset_type}_minute"
        minute_dir = self.store.data_dir / dirname
        if not minute_dir.exists():
            return []
        out: list[date] = []
        for entry in minute_dir.iterdir():
            if not (entry.is_dir() and entry.name.startswith("date=")):
                continue
            try:
                day = date.fromisoformat(entry.name[5:])
            except ValueError:
                continue
            if start <= day <= end:
                out.append(day)
        out.sort()
        return out

    def latest_daily_date(self) -> date | None:
        """本地日K数据的最新日期（CK 唯一读源）。"""
        try:
            store = self._ck_store()
            return store.latest_daily_date("kline_daily") if store is not None else None
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def latest_enriched_date(self, asset_type: str = "stock") -> date | None:
        """Return the newest partition available to matrix-native consumers."""
        dirname = enriched_dirname(asset_type)
        root = self.store.data_dir / dirname
        latest: date | None = None
        if not root.exists():
            return None
        for partition in root.glob("date=*"):
            try:
                value = date.fromisoformat(partition.name.removeprefix("date="))
            except ValueError:
                continue
            if latest is None or value > latest:
                latest = value
        return latest

    def get_matrix_data_generation(self, asset_type: str = "stock") -> str:
        """Return the stable generation for managed enriched readers."""
        return get_enriched_generation(self.store.data_dir, asset_type)

    def get_enriched_generation_snapshot(
        self, asset_type: str = "stock"
    ) -> EnrichedGenerationSnapshot:
        return get_enriched_generation_snapshot(self.store.data_dir, asset_type)

    def _bump_matrix_data_generation(self, asset_type: str) -> str:
        return bump_enriched_generation(self.store.data_dir, asset_type)

    def symbols_lagging(self, reference_date: date, min_gap_days: int = 3) -> list[str]:
        """返回日K覆盖落后的标的: 其最新 bar 早于 reference_date - min_gap_days。

        按 symbol 聚合最新日期找出掉队者（CK kline_daily 唯一读源）。
        """
        from datetime import timedelta

        try:
            store = self._ck_store()
            if store is None:
                return []
            cutoff = reference_date - timedelta(days=min_gap_days)
            df = store.read_daily("kline_daily", columns=["symbol", "date"])
            if df.is_empty():
                return []
            latest = df.group_by("symbol").agg(pl.col("date").max().alias("mx"))
            return latest.filter(pl.col("mx") < cutoff)["symbol"].to_list()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return []

    def _latest_enriched_date_duckdb(self) -> date | None:
        """兼容旧调用名：从 CK enriched 表取最新日期。"""
        try:
            store = self._ck_store()
            return store.latest_daily_date("kline_daily_enriched") if store is not None else None
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    # ================================================================
    # 写入 (Pipeline / Sync)
    # ================================================================
