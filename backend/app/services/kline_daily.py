"""从 services/kline_sync.py 抽取的 kline_daily 子模块（保持原公开 API）。"""

from __future__ import annotations

import contextlib
import shutil
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

import polars as pl

from app.data_providers.minute_normalizer import (
    _compact_klines_to_df,
    _datetime_to_ms,
    _timestamp_to_beijing_datetime,
)
from app.indicators.pipeline import filter_halt_days
from app.parquet import scan_daily_parquet, scan_parquet_compat
from app.polars_guard import guarded_collect
from app.services import preferences
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.rate_limits import chunked, sleep_between_batches
from app.tickflow.repository import KlineRepository
from app.tickflow.request_guard import (
    SDKCallTimeoutError as SDKCallTimeoutError,
)


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.kline_sync"), name)


_rootmod = _RootProxy()


def _normalize_daily(df_in, default_symbol: str | None = None) -> pl.DataFrame:
    """把 SDK 返回的 pandas/任意 DataFrame 规范成 canonical 列。"""
    if df_in is None or len(df_in) == 0:
        return pl.DataFrame()

    if not isinstance(df_in, pl.DataFrame):
        df = pl.from_pandas(df_in.reset_index() if hasattr(df_in, "reset_index") else df_in)
    else:
        df = df_in

    # 兼容字段名差异
    rename_map = {
        "ts_code": "symbol",
        "trade_date": "date",
        "vol": "volume",
        "amt": "amount",
        "datetime": "date",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    if "symbol" not in df.columns and default_symbol is not None:
        df = df.with_columns(pl.lit(default_symbol).alias("symbol"))

    # 类型规范
    if "date" in df.columns and df.schema["date"] != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date, strict=False))

    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    for col in ("volume", "amount"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    # 过滤停牌日 (open/high 为 0; close 可能被填充为前收盘价, 不能用全零判断)
    df = filter_halt_days(df)

    # 只保留 canonical 列
    keep = [c for c in _rootmod.CANONICAL_DAILY_COLS if c in df.columns]
    return df.select(keep)


def _instruments_symbol_index(repo: KlineRepository) -> dict[str, list[str]] | None:
    """裸代码 → instruments 维表完整符号列表; 维表缺失/不可读返回 None。"""
    try:
        df = repo.get_instruments()
        symbols = df["symbol"].drop_nulls().to_list() if "symbol" in df.columns else []
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("instruments 维表读取失败, 裸符号无法补全后缀: %s", e)
        return None
    if not symbols:
        return None
    index: dict[str, list[str]] = {}
    for s in symbols:
        code, _, _suffix = str(s).partition(".")
        if code:
            index.setdefault(code, []).append(str(s))
    return index


def _normalize_bare_symbols(
    symbols: list[str],
    repo: KlineRepository,
) -> tuple[list[str], dict[str, str], list[str]]:
    """裸符号(无交易所后缀, 如 600000)按 instruments 维表补全为 600000.SH (#302)。

    返回 (规范化后的去重请求列表, {裸符号: 补全后符号}, 被跳过的裸符号)。
    上游对裸符号返回 200 空 payload, 本地静默写 0 行 — 补全失败时显式跳过并告警,
    不再带着已知无效的符号发请求。全部带后缀时不读维表, 行为与开销同旧版;
    带后缀符号原样透传, 由上游照常处理。
    """
    bare = [s for s in symbols if "." not in s]
    if not bare:
        return symbols, {}, []

    index = _instruments_symbol_index(repo)
    resolved: dict[str, str] = {}
    skipped: list[str] = []
    for s in bare:
        matches = (index or {}).get(s, [])
        if len(matches) == 1:
            resolved[s] = matches[0]
        else:
            skipped.append(s)

    if index is None:
        _rootmod.logger.warning(
            "日K同步: instruments 维表不可用, %d 个裸符号(无交易所后缀)将被跳过 — "
            "请先同步标的维表, 或改用带后缀符号 (如 600000.SH)",
            len(skipped),
        )
    elif skipped:
        _rootmod.logger.warning(
            "日K同步: %d 个裸符号在维表中无唯一匹配(不支持或非股票), 已跳过: %s",
            len(skipped),
            skipped[:20],
        )
    if resolved:
        _rootmod.logger.info(
            "日K同步: 已按维表补全 %d 个裸符号后缀 (样例: %s)",
            len(resolved),
            list(resolved.items())[:5],
        )

    out: list[str] = []
    seen: set[str] = set()
    skipped_set = set(skipped)
    for s in symbols:
        if s in skipped_set:
            continue  # 无法补全的裸符号不发请求 (上游必然 200 空 payload)
        full = resolved.get(s, s)
        if full not in seen:
            seen.add(full)
            out.append(full)
    return out, resolved, skipped


def _fetch_batch_klines(tf, chunk, count, start_time, end_time):
    """按区间或条数拉取单块日K原始返回。"""
    if start_time and end_time:
        return _rootmod._sdk_call_with_deadline(
            lambda: tf.klines.batch(
                chunk,
                period="1d",
                adjust="none",
                start_time=_datetime_to_ms(start_time),
                end_time=_datetime_to_ms(end_time),
                count=10000,
                as_dataframe=False,
                show_progress=False,
            )
        )
    return _rootmod._sdk_call_with_deadline(
        lambda: tf.klines.batch(
            chunk,
            period="1d",
            count=count or 250,
            adjust="none",
            as_dataframe=False,
            show_progress=False,
        )
    )


def sync_daily_batch(
    symbols: list[str],
    count: int | None = None,
    batch_size: int | None = None,
    rpm: int | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    on_chunk_done: Callable[[int, int], None] | None = None,
    failed_out: list[str] | None = None,
) -> pl.DataFrame:
    """批量拉取多股日 K。

    优先使用 start_time / end_time 区间 + count=10000,确保覆盖完整时间段。
    仅传 count 时按条数回溯。

    failed_out: 可选出参。拉取失败的分块标的会追加进该 list, 供上层判定「部分失败」
                而非静默当成功(某分块断网 → 这些标的本轮未更新, 保持旧数据)。
    """
    tf = _rootmod.get_client()
    out: list[pl.DataFrame] = []
    chunks = chunked(symbols, batch_size)
    failed_syms: list[str] = []

    for i, chunk in enumerate(chunks):
        sleep_between_batches(i, rpm)
        try:
            raw = _fetch_batch_klines(tf, chunk, count, start_time, end_time)
        except SDKCallTimeoutError as e:
            _rootmod.logger.warning(
                "batch fetch timed out for %d symbols (chunk %d/%d): %s",
                len(chunk),
                i + 1,
                len(chunks),
                e,
            )
            failed_syms.extend(chunk)
            if on_chunk_done:
                # 心跳: 分块失败也推进进度/取消检查点, 防持续超时的同步
                # 长时间无上报(reap 误判)且协作取消无边界可退出。
                on_chunk_done(i + 1, len(chunks))
            continue
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            _rootmod.logger.warning(
                "batch fetch failed for %d symbols (chunk %d/%d): %s",
                len(chunk),
                i + 1,
                len(chunks),
                e,
            )
            failed_syms.extend(chunk)
            continue

        # False 直转: timestamp(UTC 毫秒) → 北京墙钟 datetime 列,
        # _normalize_daily 将 datetime 映射为 date — 与 SDK True 路径的
        # trade_date 字符串列同口径 (fromtimestamp(ts/1000, Asia/Shanghai))。
        seg = _compact_klines_to_df(raw)
        if not seg.is_empty():
            seg = seg.with_columns(
                _timestamp_to_beijing_datetime(pl.col("timestamp")).alias("datetime")
            ).drop("timestamp")
            out.append(_normalize_daily(seg))

        if on_chunk_done:
            on_chunk_done(i + 1, len(chunks))

    # 部分失败可见化: 聚合一条 WARNING(而非只有逐块 debug/warning), 并回传出参。
    if failed_syms:
        _rootmod.logger.warning(
            "日K批量同步部分失败: %d/%d 标的未获取, 本轮保持旧数据 (样例: %s)",
            len(failed_syms),
            len(symbols),
            failed_syms[:10],
        )
        if failed_out is not None:
            failed_out.extend(failed_syms)

    if not out:
        return pl.DataFrame()
    return pl.concat(out, how="diagonal_relaxed")


def iter_custom_daily_frames(provider, symbols, *, asset_type="stock", **kwargs):
    """复用流式/单帧插件契约; 旧股票插件可继续省略 asset_type 参数。"""
    if asset_type != "stock":
        kwargs["asset_type"] = asset_type
    stream = getattr(provider, "iter_daily", None)
    if callable(stream):
        yield from stream(symbols, **kwargs)
    else:
        frame = provider.get_daily(symbols, **kwargs)
        if not frame.is_empty():
            yield frame


class DailySourceUnavailableError(RuntimeError):
    """有效日K路由没有可用数据能力。"""


def iter_daily_frames(
    symbols: list[str],
    capset: CapabilitySet,
    *,
    start_time: datetime,
    end_time: datetime,
    asset_type: str = "stock",
    on_chunk_done=None,
):
    """只取标准日K帧, 不写正式数据、不计算指标; 调用方决定校验和发布边界。"""
    if asset_type not in {"stock", "etf", "index"}:
        raise ValueError("不支持的日K资产类型")
    from app.data_providers import custom

    provider_name = preferences.get_daily_data_provider()
    _rootmod.logger.info(
        "日K修补取数: provider=%s asset=%s symbols=%d start=%s end=%s",
        provider_name,
        asset_type,
        len(symbols),
        start_time.isoformat(),
        end_time.isoformat(),
    )
    if provider_name != "tickflow" and custom.provider_has_dataset(provider_name, "daily"):
        found = False
        for frame in iter_custom_daily_frames(
            custom.get_provider(provider_name),
            symbols,
            asset_type=asset_type,
            start_time=start_time,
            end_time=end_time,
            on_chunk_done=on_chunk_done,
        ):
            if not frame.is_empty():
                found = True
                yield frame
        # 非股票源沿用指数/ETF 原有的「明确不支持时回退」契约。
        if found or asset_type == "stock":
            return
        _rootmod.logger.warning(
            "日K源 %s 未返回 %s 数据, 修补回退 TickFlow", provider_name, asset_type
        )
    elif provider_name != "tickflow":
        _rootmod.logger.warning("数据源 %s 未提供 daily 能力, 修补回退 TickFlow", provider_name)
    if not capset.has(Cap.KLINE_DAILY_BATCH):
        raise DailySourceUnavailableError("当前日K数据源不可用, 请检查数据源配置")
    limit = _rootmod.resolve_limit(capset, Cap.KLINE_DAILY_BATCH, default_batch=100)
    failures: list[str] = []
    frame = _rootmod.sync_daily_batch(
        symbols,
        start_time=start_time,
        end_time=end_time,
        batch_size=limit.batch,
        rpm=limit.rpm,
        on_chunk_done=on_chunk_done,
        failed_out=failures,
    )
    if failures:
        raise RuntimeError(f"日K获取失败: {', '.join(failures[:10])}")
    if not frame.is_empty():
        yield frame


def _make_daily_finalizer(
    requested, orig_by_full, skipped_bare, failed_syms, failed_out, zero_row_out, seen
):
    def _finalize(written: int) -> int:
        if failed_out is not None and failed_syms:
            # 单票失败上浮给调用方 (daily_pipeline 汇入 stage_errors, #514)
            failed_out.extend(failed_syms)
        zero_full = [s for s in requested if s not in seen and s not in failed_syms]
        if zero_full or skipped_bare:
            zero = [orig_by_full.get(s, s) for s in zero_full] + skipped_bare
            _rootmod.logger.warning(
                "日K批量同步: %d/%d 标的本轮入库 0 行 (符号无法识别/停牌/窗口内无数据; 样例: %s)",
                len(zero),
                len(requested) + len(skipped_bare),
                zero[:10],
            )
            if zero_row_out is not None:
                zero_row_out.extend(zero)
        return written

    return _finalize


def _sync_daily_custom(provider_name, symbols, count, start_date, end_date, on_chunk_done, repo):
    from app.data_providers import custom as custom_sources

    if not custom_sources.provider_has_dataset(provider_name, "daily"):
        return None, set()
    provider = custom_sources.get_provider(provider_name)
    end_time = end_date or datetime.now()
    days = count or 365
    start_time = start_date or (end_time - timedelta(days=days))
    seen: set[str] = set()
    iter_daily = getattr(provider, "iter_daily", None)
    if callable(iter_daily):

        def _tracking_chunks(chunks):
            for df in chunks:
                if not df.is_empty() and "symbol" in df.columns:
                    seen.update(df["symbol"].cast(pl.Utf8).unique().to_list())
                yield df

        return (
            _persist_daily_chunks(
                _tracking_chunks(
                    iter_daily(
                        symbols,
                        start_time=start_time,
                        end_time=end_time,
                        on_chunk_done=on_chunk_done,
                    )
                ),
                repo,
            ),
            seen,
        )
    df = provider.get_daily(
        symbols,
        start_time=start_time,
        end_time=end_time,
        on_chunk_done=on_chunk_done,
    )
    if df.is_empty():
        return 0, seen
    if "symbol" in df.columns:
        seen.update(df["symbol"].cast(pl.Utf8).unique().to_list())
    repo.append_daily(df)
    repo.store.data_dir.as_posix()
    return df.height, seen


def sync_and_persist_daily_batch(
    symbols: list[str],
    repo: KlineRepository,
    capset: CapabilitySet,
    count: int | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    on_chunk_done: Callable[[int, int], None] | None = None,
    zero_row_out: list[str] | None = None,
    failed_out: list[str] | None = None,
) -> int:
    """批量同步日 K 并落到 Parquet。返回写入的行数。

    start_date/end_date: 外部传入的时间范围(由 pipeline 根据已有数据计算)。
    未传入时默认拉最近 1 年。
    zero_row_out: 可选出参。本轮实际入库 0 行的标的(含无法识别被跳过的裸符号、
                  上游 200 空 payload 的静默 0 行)按调用方原始写法追加进该 list,
                  供上层 fail-loud 展示, 不再"显示成功实则全空" (#302)。
    failed_out: 可选出参。分块拉取失败(超时/异常)的标的, 供上层把单票失败
                上浮为 stage_errors 而非静默当成功 (#514)。
    """
    seen: set[str] = set()
    failed_syms: list[str] = []
    skipped_bare: list[str] = []

    original = list(symbols)
    symbols, resolved, skipped_bare = _normalize_bare_symbols(symbols, repo)
    requested = symbols
    # 完整符号 → 调用方原始写法: 0 行回报用用户输入的形式, 裸符号也能对上
    orig_by_full: dict[str, str] = {}
    for orig in original:
        orig_by_full.setdefault(resolved.get(orig, orig), orig)
    _finalize = _make_daily_finalizer(
        requested, orig_by_full, skipped_bare, failed_syms, failed_out, zero_row_out, seen
    )

    if not symbols:
        return _finalize(0)

    provider_name = preferences.get_daily_data_provider()
    if provider_name != "tickflow":
        written, seen_custom = _sync_daily_custom(
            provider_name, symbols, count, start_date, end_date, on_chunk_done, repo
        )
        seen.update(seen_custom)
        if written is not None:
            return _finalize(written)
        # 自定义源未配置 daily → 回退 TickFlow

    if not capset.has(Cap.KLINE_DAILY_BATCH):
        _rootmod._log_cap_unavailable("daily", "日K同步")
        return _finalize(0)

    limit = _rootmod.resolve_limit(capset, Cap.KLINE_DAILY_BATCH, default_batch=100)

    end_time = end_date or datetime.now()
    start_time = start_date or (end_time - timedelta(days=365))

    df = _rootmod.sync_daily_batch(
        symbols,
        count=count,
        batch_size=limit.batch,
        rpm=limit.rpm,
        start_time=start_time,
        end_time=end_time,
        on_chunk_done=on_chunk_done,
        failed_out=failed_syms,
    )

    if not df.is_empty() and "symbol" in df.columns:
        seen.update(df["symbol"].cast(pl.Utf8).unique().to_list())

    if df.is_empty():
        return _finalize(0)

    repo.append_daily(df)

    repo.store.data_dir.as_posix()

    return _finalize(df.height)


def _persist_daily_chunks(chunks, repo: KlineRepository) -> int:
    """先把流式 provider 结果写入私有 staging,完整取数后再提交正式分区。"""
    staging_base = repo.store.data_dir / ".daily_sync_staging"
    _sweep_stale_daily_staging(staging_base)
    root = staging_base / uuid.uuid4().hex
    written = 0
    try:
        for index, df in enumerate(chunks):
            if df.is_empty():
                continue
            for date_df in df.partition_by("date"):
                dt = date_df["date"][0]
                ds = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
                out = root / f"date={ds}" / f"part-{index}.parquet"
                out.parent.mkdir(parents=True, exist_ok=True)
                date_df.write_parquet(out)
                written += date_df.height

        for date_dir in sorted(root.glob("date=*")):
            files = sorted(date_dir.glob("*.parquet"))
            if not files:
                continue
            target_dir = repo.store.data_dir / "kline_daily" / date_dir.name
            target_dir.mkdir(parents=True, exist_ok=True)
            existing = sorted(target_dir.glob("*.parquet"))
            if not existing:
                for index, staged in enumerate(files):
                    _rootmod.replace_with_retry(staged, target_dir / f"part-stream-{index}.parquet")
                continue
            # 当天分区可能混有实时快照写的 quote_ts 列, 按存储 schema 读才不抛 SchemaError。
            base = scan_daily_parquet(existing).collect(engine="streaming")
            incoming = guarded_collect(
                scan_parquet_compat(files), priority="background", engine="streaming"
            )
            merged = pl.concat([base, incoming], how="diagonal_relaxed").unique(
                subset=["symbol", "date"], keep="last"
            )
            repo.append_daily(merged)
            for part in target_dir.glob("part-stream-*.parquet"):
                part.unlink(missing_ok=True)
    finally:
        shutil.rmtree(root, ignore_errors=True)
        with contextlib.suppress(OSError):
            root.parent.rmdir()

    return written


def _sweep_stale_daily_staging(staging_base, max_age_s: int = 24 * 60 * 60) -> None:
    """清理崩溃遗留的旧同步目录,不碰仍可能活跃的新目录。"""
    if not staging_base.exists():
        return
    cutoff = time.time() - max_age_s
    for run_dir in staging_base.iterdir():
        try:
            if run_dir.is_dir() and run_dir.stat().st_mtime < cutoff:
                shutil.rmtree(run_dir)
        except OSError:
            _rootmod.logger.warning("failed to clean stale daily staging: %s", run_dir)


def sync_daily_by_quotes(repo: KlineRepository) -> int:
    """用实时行情接口拉全市场当日数据,覆写 kline_daily 今天分区。

    一个请求覆盖 ~5500 只股票,比 batch K-line 快几个数量级。
    返回写入的行数。
    """

    from app.tickflow.client import get_client

    tf = get_client()
    try:
        resp = tf.quotes.get_by_universes(universes=["CN_Equity_A"])
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("get_by_universes failed: %s", e)
        return 0

    if not resp:
        _rootmod.logger.warning("get_by_universes returned empty")
        return 0

    records = []
    for q in resp:
        q.get("ext") or {}
        records.append(
            {
                "symbol": q.get("symbol"),
                "open": q.get("open"),
                "high": q.get("high"),
                "low": q.get("low"),
                "close": q.get("last_price"),
                "volume": q.get("volume"),
                "amount": q.get("amount"),
                # 快照时刻标记: data_integrity 靠 quote_ts 区分盘中快照与盘后权威历史,
                # 缺失会让盘中覆写的分区在停机后被当成完整历史, 永远不进修复。
                "quote_ts": q.get("timestamp"),
            }
        )

    df = pl.DataFrame(records)
    if df.is_empty():
        return 0
    df = df.with_columns(pl.col("quote_ts").cast(pl.Int64, strict=False))

    # 分区日期用北京交易日 (与 quote_service._build_daily 的 cn_today 一致),
    # 避免 UTC 服务器在盘中把日分区写成服务器本地日期。
    today = _rootmod.cn_today()
    daily_df = df.with_columns(pl.lit(today).cast(pl.Date).alias("date"))

    # 过滤停牌 (open/high 为 0; close 可能被填充为前收盘价, 不能用全零判断)
    daily_df = filter_halt_days(daily_df)

    repo.flush_live_daily(daily_df)
    _rootmod.logger.info("sync_daily_by_quotes: %d symbols flushed for %s", daily_df.height, today)
    return daily_df.height
