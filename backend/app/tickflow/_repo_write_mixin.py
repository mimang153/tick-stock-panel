"""KlineRepository mixin: _WriteMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

import json
from collections.abc import Callable

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    CN_TZ,
    EnrichedPublication,
    Path,
    daily_dirname,
    date,
    datetime,
    dt_time,
    enriched_dirname,
    guarded_collect,
    logger,
    pl,
    replace_with_retry,
    scan_parquet_compat,
)


class _WriteMixin(_RepoMixinBase):
    @staticmethod
    def _repair_diff_frame(
        before: pl.DataFrame,
        after: pl.DataFrame,
        symbols: list[str] | None,
        removals: set[str] | None = None,
    ) -> pl.DataFrame:
        targets = set(symbols or [])
        targets.update(removals or [])

        def _payload(frame: pl.DataFrame, symbol: str) -> str | None:
            if frame.is_empty() or "symbol" not in frame.columns:
                return None
            rows = frame.filter(pl.col("symbol") == symbol)
            if rows.is_empty():
                return None
            return json.dumps(
                rows.row(0, named=True), ensure_ascii=False, sort_keys=True, default=str
            )

        records = []
        for symbol in sorted(targets):
            old, new = _payload(before, symbol), _payload(after, symbol)
            change = "delete" if new is None else "insert" if old is None else "update"
            records.append(
                {"symbol": symbol, "change_type": change, "before_json": old, "after_json": new}
            )
        return pl.DataFrame(
            records,
            schema={
                "symbol": pl.String,
                "change_type": pl.String,
                "before_json": pl.String,
                "after_json": pl.String,
            },
        )

    def _nontrading_symbols_from_rows(
        self, rows: pl.DataFrame, asset_type: str, day: date
    ) -> set[str]:
        # 指数的零成交量可能是源缺字段，不能套用股票/ETF 的清理规则。
        if asset_type not in {"stock", "etf"} or rows.is_empty() or "symbol" not in rows.columns:
            return set()
        eligible = pl.lit(False)
        if {"volume", "amount"}.issubset(rows.columns):
            eligible = eligible | (
                (pl.col("volume").cast(pl.Float64, strict=False) == 0)
                & (pl.col("amount").cast(pl.Float64, strict=False) == 0)
            )
        if "quote_ts" in rows.columns:
            start_ms = int(datetime.combine(day, dt_time.min, tzinfo=CN_TZ).timestamp() * 1000)
            ts = pl.col("quote_ts").cast(pl.Int64, strict=False)
            eligible = eligible | ((ts > 0) & (ts < start_ms))
        instruments = self.get_instruments_asset(asset_type)
        if {"symbol", "listing_date"}.issubset(instruments.columns):
            prelisting = instruments.filter(
                pl.col("listing_date").cast(pl.Date, strict=False) > day,
            )["symbol"].to_list()
            eligible = eligible | pl.col("symbol").is_in(prelisting)
        confirmed = rows.group_by("symbol").agg(eligible.fill_null(False).all().alias("nontrading"))
        return set(confirmed.filter(pl.col("nontrading"))["symbol"].to_list())

    def append_daily(self, df: pl.DataFrame) -> None:
        """按日分区写入日K数据 (merge-upsert)。"""
        if df.is_empty():
            return
        self._write_daily_partition(df, "kline_daily")

    def append_enriched(self, df: pl.DataFrame) -> None:
        """按日分区写入 enriched 数据 (merge-upsert)。磁盘仅写入 14 列存储列。"""
        if df.is_empty():
            return
        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        storage_cols = [c for c in ENRICHED_STORAGE_COLS if c in df.columns]
        df_storage = df.select(storage_cols)
        self._write_daily_partition(df_storage, "kline_daily_enriched")

    def append_index_daily(self, df: pl.DataFrame) -> None:
        """按日分区写入指数日K数据 (merge-upsert)。"""
        if df.is_empty():
            return
        self._write_daily_partition(df, "kline_index_daily")

    def append_index_enriched(self, df: pl.DataFrame) -> None:
        """按日分区写入指数 enriched 数据。磁盘仅写入通用基础行情窄表。"""
        if df.is_empty():
            return
        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        storage_cols = [c for c in ENRICHED_STORAGE_COLS if c in df.columns]
        df_storage = df.select(storage_cols)
        self._write_daily_partition(df_storage, "kline_index_enriched")

    def append_etf_daily(self, df: pl.DataFrame) -> None:
        """按日分区写入 ETF 日K数据 (merge-upsert)。"""
        if df.is_empty():
            return
        self._write_daily_partition(df, "kline_etf_daily")

    def append_etf_enriched(self, df: pl.DataFrame) -> None:
        """按日分区写入 ETF enriched 数据。磁盘仅写入基础行情窄表。"""
        if df.is_empty():
            return
        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        storage_cols = [c for c in ENRICHED_STORAGE_COLS if c in df.columns]
        df_storage = df.select(storage_cols)
        self._write_daily_partition(df_storage, "kline_etf_enriched")

    def append_daily_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """按资产类型写入日K；stock/index 保持旧目录兼容。"""
        if asset_type == "stock":
            self.append_daily(df)
        elif asset_type == "index":
            self.append_index_daily(df)
        elif asset_type == "etf":
            self.append_etf_daily(df)

    def _replace_daily_rows_ck(
        self,
        store,
        asset_type: str,
        day: date,
        incoming: pl.DataFrame,
        symbols: list[str] | None,
        removals: set[str],
        actor: str,
    ) -> None:
        table = daily_dirname(asset_type)
        self._validate_daily_repair_payload(day, incoming, symbols, removals)
        if removals:
            columns = [
                c
                for c in ("symbol", "volume", "amount", "quote_ts")
                if c in store.table_columns(table)
            ]
            rows = store.read_daily(
                table,
                symbols=sorted(removals),
                start=day,
                end=day,
                columns=columns,
            )
            if removals - self._nontrading_symbols_from_rows(rows, asset_type, day):
                raise ValueError("无法确认待清理记录为非交易记录, 原数据保持不变")
        day_rows = store.read_daily(table, start=day, end=day)
        if symbols is None:
            before, base = day_rows, pl.DataFrame()
        else:
            before = day_rows.filter(pl.col("symbol").is_in(symbols))
            base = day_rows.filter(~pl.col("symbol").is_in(symbols))
        merged = (
            pl.concat([base, incoming], how="diagonal_relaxed")
            .unique(subset=["symbol", "date"], keep="last")
            .sort(["symbol", "date"])
        )
        if merged.is_empty():
            raise ValueError("清理后整日日K为空, 需要完整日K数据, 原数据保持不变")
        # CK 没有文件指纹，用写入前重读确认目标行未被并发改写。
        current = store.read_daily(table, start=day, end=day)
        if not current.equals(day_rows):
            raise ValueError("修补期间发生变化, 请重新自检")
        repair_id = store.begin_repair(table, day, "precise", actor)
        try:
            store.append_repair_snapshot(repair_id, table, day, before, actor)
            diffs = self._repair_diff_frame(before, incoming, symbols, removals)
            store.append_repair_audit(repair_id, table, day, diffs, actor)
            if symbols is None:
                store.delete_daily_date(table, day=day)
            else:
                store.delete_daily(table, day=day, symbols=list(symbols))
            store.write_daily(table, merged)
            store.set_repair_status(repair_id, "committed", actor)
        except BaseException:
            store.set_repair_status(repair_id, "failed", actor)
            raise

    def replace_daily_rows(
        self,
        asset_type: str,
        day: date,
        incoming: pl.DataFrame,
        *,
        symbols: list[str] | None,
        backup_dir: Path,
        remove_symbols: list[str] | None = None,
        actor: str = "system",
    ) -> None:
        """修正一个日期的指定股票; symbols=None 仅用于无法读取的整日分区。

        在线路径写 CK 审计/快照；离线路径保留本地备份。其他日期不参与改写。
        """
        store = getattr(self, "storage", None)
        if store is not None:
            self._replace_daily_rows_ck(
                store, asset_type, day, incoming, symbols, set(remove_symbols or []), actor
            )
            return

        import shutil

        part = self.store.data_dir / daily_dirname(asset_type) / f"date={day}"
        self._validate_repair_path(part)
        self._validate_repair_path(backup_dir)
        removals = set(remove_symbols or [])
        self._validate_daily_repair_payload(day, incoming, symbols, removals)
        paths = sorted(part.glob("*.parquet"))
        fingerprints = {p: self._partition_fingerprint(p) for p in paths}
        if removals - self.daily_repair_nontrading_symbols(asset_type, day, sorted(removals)):
            raise ValueError("无法确认待清理记录为非交易记录, 原数据保持不变")
        base = pl.DataFrame()
        if symbols is not None and paths:
            base = pl.concat([pl.read_parquet(p) for p in paths], how="diagonal_relaxed")
            base = base.filter(~pl.col("symbol").is_in(symbols))
        merged = (
            pl.concat([base, incoming], how="diagonal_relaxed")
            .unique(
                subset=["symbol", "date"],
                keep="last",
            )
            .sort(["symbol", "date"])
        )
        if merged.is_empty():
            raise ValueError("清理后整日日K为空, 需要完整日K数据, 原数据保持不变")
        with self._write_lock:
            if {p: self._partition_fingerprint(p) for p in part.glob("*.parquet")} != fingerprints:
                raise ValueError("日K在修补期间发生变化, 请重新自检")
            backup_dir.mkdir(parents=True, exist_ok=True)
            for path in paths:
                self._validate_repair_path(path)
                shutil.copy2(path, backup_dir / path.name)
            part.mkdir(parents=True, exist_ok=True)
            out = part / "part.parquet"
            self._atomic_write_parquet(merged, out)
            for path in paths:
                if path != out:
                    path.unlink()
            # CK 镜像同步: 删除被修补/清理的 symbol 当日行, 再写回合并结果
            # 镜像失败不阻断本地修补 (CK 仍是权威读源, 需下一轮重算收敛)
            try:
                self._mirror_daily_repair_to_ck(asset_type, day, symbols, merged)
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.debug("CK repair mirror failed: %s", e)

    @staticmethod
    def _validate_daily_repair_payload(
        day: date,
        incoming: pl.DataFrame,
        symbols: list[str] | None,
        removals: set[str],
    ) -> set[str]:
        """校验一次日K修补的数据; 返回收到的 symbol 集合。"""
        if (
            (incoming.is_empty() and not removals)
            or not {"symbol", "date"}.issubset(incoming.columns)
            or incoming.filter(pl.col("date") != day).height
        ):
            raise ValueError("修补日K为空或日期与目标分区不符")
        received = set(incoming["symbol"].to_list())
        if symbols is not None and not (received | removals).issubset(symbols):
            raise ValueError("修补日K超出目标股票范围")
        if received & removals:
            raise ValueError("修补和清理的股票不能重叠")
        if symbols is not None and set(symbols) != received | removals:
            raise ValueError("修补日K不完整, 不能丢弃尚未获取的股票")
        return received

    def _mirror_daily_repair_to_ck(
        self,
        asset_type: str,
        day: date,
        symbols: list[str] | None,
        merged: pl.DataFrame,
    ) -> None:
        """把一次日K修补同步到 CK 权威读源。

        symbols=None 表示整日分区不可读而整体替换: 必须清空当日旧行, 否则 CK
        仍会读到旧的坏行, 修补后重算会再次判坏。
        """
        store = self._ck_store()
        if store is None:
            return
        table = daily_dirname(asset_type)
        if symbols is None:
            store.delete_daily_date(table, day=day)
        else:
            store.delete_daily(table, day=day, symbols=list(symbols))
        store.write_daily(table, merged)

    def validate_daily_repair_target(self, asset_type: str, day: date) -> None:
        store = getattr(self, "storage", None)
        if store is not None:
            store.table_columns(daily_dirname(asset_type))
            return
        self._validate_repair_path(self.store.data_dir / daily_dirname(asset_type) / f"date={day}")

    def daily_repair_nontrading_symbols(
        self, asset_type: str, day: date, symbols: list[str]
    ) -> set[str]:
        """仅确认本地已有的上市前、零成交及旧日快照遗留, 不从源的空返回推断停牌。

        零成交判据沿用 filter_halt_days; 重复行必须全部有依据, 防止删掉同股真实成交。
        指数的零成交量可能是源缺字段, 不套用股票/ETF 的清理规则。
        """
        if not symbols or asset_type not in {"stock", "etf"}:
            return set()
        store = getattr(self, "storage", None)
        if store is not None:
            table = daily_dirname(asset_type)
            columns = [
                c
                for c in ("symbol", "volume", "amount", "quote_ts")
                if c in store.table_columns(table)
            ]
            rows = store.read_daily(table, symbols=symbols, start=day, end=day, columns=columns)
            return self._nontrading_symbols_from_rows(rows, asset_type, day)
        part = self.store.data_dir / daily_dirname(asset_type) / f"date={day}"
        self._validate_repair_path(part)
        frames = []
        for path in sorted(part.glob("*.parquet")):
            self._validate_repair_path(path)
            scan = scan_parquet_compat(path)
            schema_columns = set(scan.collect_schema().names())
            if "symbol" not in schema_columns:
                return set()
            frames.append(
                scan.filter(pl.col("symbol").is_in(symbols))
                .select(
                    [c for c in ("symbol", "volume", "amount", "quote_ts") if c in schema_columns],
                )
                .pipe(guarded_collect, priority="interactive")
            )
        if not frames:
            return set()
        rows = pl.concat(frames, how="diagonal_relaxed")
        eligible = pl.lit(False)
        if {"volume", "amount"}.issubset(rows.columns):
            eligible = eligible | (
                (pl.col("volume").cast(pl.Float64, strict=False) == 0)
                & (pl.col("amount").cast(pl.Float64, strict=False) == 0)
            )
        if "quote_ts" in rows.columns:
            start_ms = int(datetime.combine(day, dt_time.min, tzinfo=CN_TZ).timestamp() * 1000)
            ts = pl.col("quote_ts").cast(pl.Int64, strict=False)
            eligible = eligible | ((ts > 0) & (ts < start_ms))
        instruments = self.get_instruments_asset(asset_type)
        if {"symbol", "listing_date"}.issubset(instruments.columns):
            prelisting = instruments.filter(
                pl.col("listing_date").cast(pl.Date, strict=False) > day,
            )["symbol"].to_list()
            eligible = eligible | pl.col("symbol").is_in(prelisting)
        confirmed = rows.group_by("symbol").agg(eligible.fill_null(False).all().alias("nontrading"))
        return set(confirmed.filter(pl.col("nontrading"))["symbol"].to_list())

    def daily_repair_files(self, asset_type: str) -> list[Path]:
        """重算原始历史前验证全部输入路径, 不沿目录或文件链接读取。"""
        base = self.store.data_dir / daily_dirname(asset_type)
        self._validate_repair_path(base)
        paths = sorted(base.glob("date=*/*.parquet"))
        for path in paths:
            self._validate_repair_path(path)
        return paths

    def _daily_repair_universe_ck(
        self, store, asset_type: str, day: date
    ) -> tuple[list[str], set[str]]:
        table = daily_dirname(asset_type)
        known_rows = store.read_daily(table, start=day, end=day, columns=["symbol"])
        known = (
            set(known_rows["symbol"].drop_nulls().to_list()) if not known_rows.is_empty() else set()
        )
        instruments = self.get_instruments_asset(asset_type)
        pool = (
            set(instruments["symbol"].drop_nulls().to_list())
            if not instruments.is_empty() and "symbol" in instruments.columns
            else set()
        )
        dates = [d for d in store.list_daily_dates(table) if d != day]
        for before in (True, False):
            ordered = sorted(
                (d for d in dates if (d < day) == before),
                key=lambda value: abs((value - day).days),
            )
            for value in ordered:
                values = set(
                    store.read_daily(table, start=value, end=value, columns=["symbol"])["symbol"]
                    .drop_nulls()
                    .to_list()
                )
                if values:
                    pool.update(values)
                    break
        if not (pool | known) or (not pool and known_rows.is_empty()):
            raise ValueError("无法确定整日回补的标的清单, 请先同步对应资产的维表")
        return sorted(pool | known), known

    def daily_repair_universe(self, asset_type: str, day: date) -> tuple[list[str], set[str]]:
        """整日回补清单含当前维表和最近前/后完整分区, 保留历史退市标的。

        另返回当日尚可读取的标的: 上游若遗漏这些行, 不能因替换损坏分片把它们丢掉。
        """
        store = getattr(self, "storage", None)
        if store is not None:
            return self._daily_repair_universe_ck(store, asset_type, day)
        base = self.store.data_dir / daily_dirname(asset_type)

        def read_symbols(part: Path) -> tuple[set[str], bool]:
            paths = list(part.glob("*.parquet"))
            symbols: set[str] = set()
            complete = bool(paths)
            for path in paths:
                self._validate_repair_path(path)
                try:
                    symbols.update(
                        pl.read_parquet(path, columns=["symbol"])["symbol"].drop_nulls().to_list()
                    )
                except (OSError, pl.exceptions.PolarsError):
                    complete = False
            return symbols, complete

        known, complete = read_symbols(base / f"date={day}")
        instruments = self.get_instruments_asset(asset_type)
        pool = (
            set(instruments["symbol"].drop_nulls().to_list())
            if not instruments.is_empty() and "symbol" in instruments.columns
            else set()
        )
        parts = []
        for part in base.glob("date=*"):
            try:
                value = date.fromisoformat(part.name[5:])
                if value != day:
                    parts.append((value, part))
            except ValueError:
                continue
        for before in (True, False):
            ordered = sorted(
                (p for p in parts if (p[0] < day) == before),
                key=lambda item: abs((item[0] - day).days),
            )
            for _, part in ordered:
                values, readable = read_symbols(part)
                if values and readable:
                    pool.update(values)
                    break
        if not (pool | known) or (not pool and not complete):
            raise ValueError("无法确定整日回补的标的清单, 请先同步对应资产的维表")
        return sorted(pool | known), known

    def _validate_repair_path(self, path: Path) -> None:
        root = self.store.data_dir.resolve()
        relative = path.relative_to(self.store.data_dir)
        candidate = self.store.data_dir
        for name in relative.parts:
            candidate /= name
            if candidate.is_symlink():
                raise ValueError("日K目录包含链接, 请手动检查目录后再修补")
        if not path.resolve().is_relative_to(root):
            raise ValueError("修补目标位于数据目录以外")

    def _replace_enriched_rows_ck(
        self, store, table: str, frame: pl.DataFrame, start: date, symbols: list[str]
    ) -> int:
        dates = set(store.list_daily_dates(table, start=start))
        if not frame.is_empty():
            dates.update(frame["date"].to_list())
        written = 0
        for day in sorted(d for d in dates if d >= start):
            incoming = frame.filter(pl.col("date") == day) if not frame.is_empty() else frame
            store.delete_daily(table, day=day, symbols=symbols)
            if not incoming.is_empty():
                store.write_daily(table, incoming)
            written += incoming.height
        return written

    def replace_enriched_rows(
        self,
        asset_type: str,
        frame: pl.DataFrame,
        *,
        start: date,
        symbols: list[str],
        publication: EnrichedPublication,
    ) -> int:
        """发布受影响股票从 start 起的基础指标, 保留同分区其他股票。

        同时处理停牌过滤后消失的旧行, 避免仅 upsert 留下旧的错误状态。
        调用方负责一次逻辑重算的 publication.commit()。
        """
        table = enriched_dirname(asset_type)
        store = getattr(self, "storage", None)
        if store is not None:
            return self._replace_enriched_rows_ck(store, table, frame, start, symbols)
        base = self.store.data_dir / table
        dates = {date.fromisoformat(p.name[5:]) for p in base.glob("date=*") if p.is_dir()}
        if not frame.is_empty():
            dates.update(frame["date"].to_list())
        written = 0
        for day in sorted(d for d in dates if d >= start):
            part = base / f"date={day}"
            self._validate_repair_path(part)
            paths = sorted(part.glob("*.parquet"))
            for path in paths:
                self._validate_repair_path(path)
            fingerprints = {p: self._partition_fingerprint(p) for p in paths}
            existing = (
                pl.concat([pl.read_parquet(p) for p in paths], how="diagonal_relaxed")
                if paths
                else pl.DataFrame()
            )
            keep = (
                existing.filter(~pl.col("symbol").is_in(symbols))
                if not existing.is_empty()
                else existing
            )
            incoming = frame.filter(pl.col("date") == day) if not frame.is_empty() else frame
            merged = pl.concat([keep, incoming], how="diagonal_relaxed").sort(["symbol", "date"])
            unchanged = merged.equals(existing) and len(paths) <= 1
            if not unchanged:
                with self._write_lock:
                    if {
                        p: self._partition_fingerprint(p) for p in part.glob("*.parquet")
                    } != fingerprints:
                        raise ValueError("指标数据在修补期间发生变化, 请重新执行修补")
                    publication.write_parquet(merged, part / "part.parquet")
                    for path in paths:
                        if path.name != "part.parquet":
                            path.unlink()
                written += incoming.height
            if unchanged:
                continue
        return written

    def invalidate_enriched_asset(self, asset_type: str) -> None:
        """只失效本次资产的运行时快照; 下次读取按新版本懒加载。"""
        if asset_type == "stock":
            for name in (
                "_enriched_cache",
                "_enriched_cache_date",
                "_enriched_history_cache",
                "_enriched_history_start",
                "_enriched_history_generation",
                "_enriched_history_snapshot",
                "_live_agg_cache",
                "_live_agg_cache_date",
                "_live_agg_check_date",
            ):
                setattr(self, name, None)
        elif asset_type in {"etf", "index"}:
            for suffix in (
                "enriched_cache",
                "enriched_cache_date",
                "live_agg_cache",
                "live_agg_cache_date",
            ):
                setattr(self, f"_{asset_type}_{suffix}", None)

    def append_enriched_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """按资产类型写入 enriched；stock/index 保持旧目录兼容。"""
        if asset_type == "stock":
            self.append_enriched(df)
        elif asset_type == "index":
            self.append_index_enriched(df)
        elif asset_type == "etf":
            self.append_etf_enriched(df)

    def _persist_instruments(self, asset_type: str, df: pl.DataFrame, local_path: Path) -> None:
        """在线只写 CK；无门面时（离线/兼容性测试）才写本地 parquet。"""
        clean = df.unique(subset=["symbol"], keep="last").sort("symbol")
        store = getattr(self, "storage", None)
        if store is not None:
            store.write_instruments(asset_type, clean)
            return
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write_parquet(clean, local_path)

    def save_instruments(self, df: pl.DataFrame) -> None:
        """保存个股维表（在线 CK-only）。"""
        if df.is_empty() or "symbol" not in df.columns:
            return
        self._persist_instruments(
            "stock", df, self.store.data_dir / "instruments" / "instruments.parquet"
        )
        self._instruments_cache = None
        self._name_map_cache = None
        self._refresh_instruments()

    def save_index_instruments(self, df: pl.DataFrame) -> None:
        """保存指数标的维表（在线 CK-only）。"""
        if df.is_empty() or "symbol" not in df.columns:
            return
        self._persist_instruments(
            "index", df, self.store.data_dir / "instruments_index" / "instruments_index.parquet"
        )
        self._index_instruments_cache = None
        self._etf_instruments_cache = None
        self._name_map_cache = None
        self._refresh_index_instruments()

    def save_etf_instruments(self, df: pl.DataFrame) -> None:
        """保存 ETF 标的维表到独立目录（在线 CK-only）。"""
        if df.is_empty() or "symbol" not in df.columns:
            return
        if "asset_type" not in df.columns:
            df = df.with_columns(pl.lit("etf").alias("asset_type"))
        self._persist_instruments(
            "etf", df, self.store.data_dir / "instruments_etf" / "instruments_etf.parquet"
        )
        self._etf_instruments_cache = None
        self._name_map_cache = None
        self._refresh_etf_instruments()

    @staticmethod
    def _atomic_write_parquet(df: pl.DataFrame, out: Path) -> None:
        """先写临时文件再原子替换, 避免进程中断留下损坏的 parquet。

        直接 write_parquet(out) 在进程被 kill (dev.sh 清端口用 kill -9)
        或断电时会留下半截文件, 之后 scan_parquet glob 整条链路报错。
        临时文件后缀 .tmp 不匹配 *.parquet glob, 不会被扫描误读。
        Windows 下目标正被并发读取时由 replace_with_retry 短退避穿过。
        """
        tmp = out.with_name(out.name + ".tmp")
        df.write_parquet(tmp)
        replace_with_retry(tmp, out)

    def _run_ck_enriched_generation(
        self, store, asset_type: str, day, action: Callable[[], object]
    ):
        if asset_type not in {"stock", "etf", "index"}:
            return action()
        generation_id = store.begin_generation(asset_type, "system", day)
        try:
            result = action()
        except BaseException:
            store.fail_generation(asset_type, generation_id, "system")
            raise
        store.commit_generation(asset_type, generation_id, "system")
        return result

    def _write_daily_partition_ck(
        self, store, df: pl.DataFrame, table: str, generation_asset: str | None
    ) -> None:
        def write() -> None:
            for date_df in df.partition_by("date"):
                store.write_daily(table, date_df)

        if generation_asset is None:
            write()
            return
        self._run_ck_enriched_generation(store, generation_asset, df["date"].min(), write)

    def _write_daily_partition(self, df: pl.DataFrame, table: str) -> None:
        """按 date 分区写入 parquet，每个日期一个文件，支持 merge-upsert。"""
        base = self.store.data_dir / table
        generation_asset = {
            "kline_daily_enriched": "stock",
            "kline_etf_enriched": "etf",
            "kline_index_enriched": "index",
        }.get(table)
        explicit_store = getattr(self, "storage", None)
        if explicit_store is not None:
            if not df.is_empty():
                self._write_daily_partition_ck(explicit_store, df, table, generation_asset)
            return
        # recover=True: 外部进程残留的僵死 publishing 标记不应阻塞实时/管道
        # enriched 落盘, 首次写入即接管自愈; 活进程的发布仍会抛错保护竞态。
        publication = (
            EnrichedPublication(self.store.data_dir, generation_asset, recover=True)
            if generation_asset is not None
            else None
        )
        store = self._ck_store()
        for date_df in df.partition_by("date"):
            dt = date_df["date"][0]
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            self._optimistic_upsert_partition(out, date_df, publication)
            # CK 镜像写入 (B2)：parquet 仍是页面读源，CK 侧只做镜像；
            # 未注入 storage 时降级为只写 parquet，不打断既有链路。
            if store is not None:
                try:
                    store.write_daily(table, date_df)
                except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                    logger.debug("CK mirror write %s skipped: %s", table, e)

    @staticmethod
    def _partition_fingerprint(path: Path) -> tuple[int, int] | None:
        """分区文件的修改指纹 (mtime_ns, size); 不存在返回 None。"""
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size)

    def _optimistic_upsert_partition(
        self,
        out: Path,
        incoming: pl.DataFrame,
        publication: EnrichedPublication | None,
        *,
        retries: int = 3,
    ) -> None:
        """单分区 merge-upsert: polars 读/合并/排序在 _write_lock 外, 锁内只做
        指纹校验 + 原子替换 + commit。

        背景: polars 并发执行存在死锁风险 (见 app.polars_guard), 重活若在
        _write_lock 内悬死, 全局写锁被永久持有, 所有写路径排队冻结。乐观模式
        把读/算移出锁外; 锁内用指纹确认基底未被其他写入者改动, 失配则重试,
        重试耗尽退回锁内直读直写 (正确性优先, 牺牲隔离性)。
        """

        def _merge(existing: pl.DataFrame) -> pl.DataFrame:
            if existing.is_empty():
                return incoming.sort(["symbol", "date"])
            return (
                pl.concat([existing, incoming], how="diagonal_relaxed")
                .unique(subset=["symbol", "date"], keep="last")
                .sort(["symbol", "date"])
            )

        for _ in range(retries):
            existing = pl.read_parquet(out) if out.exists() else pl.DataFrame()
            base_fp = self._partition_fingerprint(out)
            merged = _merge(existing)
            with self._write_lock:
                if self._partition_fingerprint(out) != base_fp:
                    continue  # 基底被并发写入者改过, 出锁重读重算
                self._write_partition_locked(out, merged, existing, publication)
            return
        # 乐观重试耗尽 (罕见: 高频并发写同一分区): 退回锁内全量模式保证正确性
        with self._write_lock:
            existing = pl.read_parquet(out) if out.exists() else pl.DataFrame()
            self._write_partition_locked(out, _merge(existing), existing, publication)

    def _write_partition_locked(
        self,
        out: Path,
        merged: pl.DataFrame,
        existing: pl.DataFrame,
        publication: EnrichedPublication | None,
    ) -> None:
        """锁内的纯文件阶段: 无变化跳过; 否则原子替换 + 提交 generation。"""
        if not existing.is_empty() and existing.equals(merged):
            if publication is not None:
                publication.commit()  # 未写入时为无害空提交
            return
        if publication is None:
            self._atomic_write_parquet(merged, out)
        else:
            # 写/提交失败必须结束发布会话, 否则一次瞬时失败会把 publishing
            # 标记挂到心跳超时, 期间所有读写都报"正在发布"。abandon 按是否
            # 已有分区落盘决定: 未写入 → 恢复 ready; 已写入 → 保持 fail-closed
            # (磁盘可能已换新内容, 不能沿用旧 generation 放行缓存)。
            try:
                publication.write_parquet(merged, out)
                publication.commit()
            except BaseException:
                publication.abandon()
                raise

    def merge_live_daily_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """按 symbol 合并当天指定资产日K分区。用于少量自选实时，不覆盖全市场。"""
        if df.is_empty() or "date" not in df.columns:
            return
        table = {
            "stock": "kline_daily",
            "index": "kline_index_daily",
            "etf": "kline_etf_daily",
        }.get(asset_type)
        if not table:
            return
        explicit_store = getattr(self, "storage", None)
        if explicit_store is not None:
            explicit_store.write_daily(table, df)
            return
        base = self.store.data_dir / table
        dt = df["date"][0]
        ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        out = base / f"date={ds}" / "part.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        self._optimistic_upsert_partition(out, df, None)

    def _with_instrument_metadata(self, asset_type: str, df: pl.DataFrame) -> pl.DataFrame:
        """补齐实时内存缓存所需的维表字段；这些字段不会写入 enriched 分区。"""
        if asset_type not in {"stock", "etf"} or df.is_empty():
            return df
        instruments = self.get_instruments_asset(asset_type)
        if instruments.is_empty() or "symbol" not in instruments.columns:
            return df
        metadata_cols = [
            c
            for c in ("name", "total_shares", "float_shares")
            if c in instruments.columns and c not in df.columns
        ]
        if not metadata_cols:
            return df
        metadata = instruments.select(["symbol", *metadata_cols]).unique(
            subset=["symbol"],
            keep="last",
        )
        return df.join(metadata, on="symbol", how="left")

    def merge_live_enriched_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """按 symbol 合并当天 enriched 分区和内存缓存。用于少量自选实时。"""
        if df.is_empty() or "date" not in df.columns:
            return
        dt = df["date"][0]
        if asset_type == "stock":
            table = "kline_daily_enriched"
            existing_cache = (
                self._enriched_cache if self._enriched_cache_date == dt else pl.DataFrame()
            )
        elif asset_type == "etf":
            table = "kline_etf_enriched"
            existing_cache = (
                self._etf_enriched_cache if self._etf_enriched_cache_date == dt else pl.DataFrame()
            )
        elif asset_type == "index":
            table = "kline_index_enriched"
            existing_cache = (
                self._index_enriched_cache
                if self._index_enriched_cache_date == dt
                else pl.DataFrame()
            )
        else:
            return

        cache_df = self._with_instrument_metadata(asset_type, df)
        merged_cache = cache_df
        if existing_cache is not None and not existing_cache.is_empty():
            merged_cache = pl.concat([existing_cache, cache_df], how="diagonal_relaxed").unique(
                subset=["symbol", "date"], keep="last"
            )
        merged_cache = merged_cache.sort(["symbol"])

        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        storage_cols = [c for c in ENRICHED_STORAGE_COLS if c in df.columns]
        df_storage = df.select(storage_cols).sort(["symbol"])
        explicit_store = getattr(self, "storage", None)
        if explicit_store is not None:
            self._run_ck_enriched_generation(
                explicit_store,
                asset_type,
                dt,
                lambda: explicit_store.write_daily(table, df_storage),
            )
        else:
            base = self.store.data_dir / table
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            publication = (
                EnrichedPublication(self.store.data_dir, asset_type, recover=True)
                if asset_type in {"stock", "etf"}
                else None
            )
            self._optimistic_upsert_partition(out, df_storage, publication)

        if asset_type == "stock":
            self._enriched_cache = merged_cache
            self._enriched_cache_date = dt
        elif asset_type == "etf":
            self._etf_enriched_cache = merged_cache
            self._etf_enriched_cache_date = dt
        elif asset_type == "index":
            self._index_enriched_cache = merged_cache
            self._index_enriched_cache_date = dt

    def flush_live_daily(self, df: pl.DataFrame) -> None:
        """覆写当天 kline_daily 分区 (实时行情落盘, 非merge)。"""
        if df.is_empty() or "date" not in df.columns:
            return
        self.flush_live_daily_asset("stock", df)

    def flush_live_daily_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """覆写当天指定资产日K分区 (实时行情落盘, 非merge)。"""
        if df.is_empty() or "date" not in df.columns:
            return
        table = {
            "stock": "kline_daily",
            "index": "kline_index_daily",
            "etf": "kline_etf_daily",
        }.get(asset_type)
        if not table:
            return
        explicit_store = getattr(self, "storage", None)
        if explicit_store is not None:
            explicit_store.delete_daily_date(table, day=df["date"][0])
            explicit_store.write_daily(table, df.sort(["symbol", "date"]))
            return
        base = self.store.data_dir / table
        dt = df["date"][0]
        ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        out = base / f"date={ds}" / "part.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        # 覆写语义: 排序在锁外, 锁内只做原子替换。
        df_sorted = df.sort(["symbol", "date"])
        with self._write_lock:
            self._atomic_write_parquet(df_sorted, out)
            # CK 镜像: 覆写当天分区 (删整日后写回)
            store = self._ck_store()
            if store is not None:
                try:
                    store.delete_daily_date(table, day=dt)
                    store.write_daily(table, df_sorted)
                except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                    logger.debug("CK flush mirror failed: %s", e)

    def flush_live_enriched(self, df: pl.DataFrame) -> None:
        """覆写当天 kline_daily_enriched 分区 (实时 enriched 落盘, 非merge)。

        内存缓存保留完整指标列供各服务使用，磁盘仅写入 14 列存储列。
        """
        self.flush_live_enriched_asset("stock", df)

    def flush_live_enriched_asset(self, asset_type: str, df: pl.DataFrame) -> None:
        """覆写当天指定资产 enriched 分区 (实时 enriched 落盘, 非merge)。"""
        if df.is_empty() or "date" not in df.columns:
            return
        dt = df["date"][0]
        cache_df = self._with_instrument_metadata(asset_type, df).sort(["symbol"])
        if asset_type == "stock":
            table = "kline_daily_enriched"
        elif asset_type == "etf":
            table = "kline_etf_enriched"
        elif asset_type == "index":
            table = "kline_index_enriched"
        else:
            return

        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        storage_cols = [c for c in ENRICHED_STORAGE_COLS if c in df.columns]
        df_storage = df.select(storage_cols).sort(["symbol"])
        explicit_store = getattr(self, "storage", None)
        if explicit_store is not None:

            def overwrite() -> None:
                explicit_store.delete_daily_date(table, day=dt)
                explicit_store.write_daily(table, df_storage)

            self._run_ck_enriched_generation(explicit_store, asset_type, dt, overwrite)
        else:
            base = self.store.data_dir / table
            ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out = base / f"date={ds}" / "part.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            publication = (
                EnrichedPublication(self.store.data_dir, asset_type, recover=True)
                if asset_type in {"stock", "etf"}
                else None
            )
            # 覆写语义: 读旧内容只为跳过无变化的写, 读在锁外 (误判最多造成一次
            # 冗余覆写, 不影响正确性); 锁内只做替换 + commit。
            existing = pl.read_parquet(out) if out.exists() else pl.DataFrame()
            with self._write_lock:
                self._write_partition_locked(out, df_storage, existing, publication)

        if asset_type == "stock":
            self._enriched_cache = cache_df
            self._enriched_cache_date = dt
        elif asset_type == "etf":
            self._etf_enriched_cache = cache_df
            self._etf_enriched_cache_date = dt
        elif asset_type == "index":
            self._index_enriched_cache = cache_df
            self._index_enriched_cache_date = dt
