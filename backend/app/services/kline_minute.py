"""从 services/kline_sync.py 抽取的 kline_minute 子模块（保持原公开 API）。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import NamedTuple

import polars as pl

from app.data_providers.base import AssetType, ProviderRequestError
from app.data_providers.minute_normalizer import (
    CANONICAL_MINUTE_COLS as CANONICAL_MINUTE_COLS,
)
from app.data_providers.minute_normalizer import (
    _compact_klines_to_df,
    _datetime_to_ms,
    _normalize_minute,
)
from app.data_providers.minute_stream import iter_minute_frames
from app.market_time import CN_TZ
from app.services import preferences
from app.services.shared_work import WorkBusyError
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.repository import KlineRepository


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.kline_sync"), name)


_rootmod = _RootProxy()


def _write_minute_partition(
    df: pl.DataFrame, minute_dir: Path, *, missing_only: bool = False
) -> int:
    """按日期原子合并。普通同步返回写入分区行数，定日修补与旧分区迁移只统计新增。"""
    if df.is_empty():
        return 0
    keys = ["symbol", "datetime"]
    df = df.drop_nulls(keys)
    if df.is_empty():
        return 0
    df = df.with_columns(pl.col("datetime").dt.date().alias("_trade_date"))
    table = minute_dir.name
    written = 0
    for day_df in df.partition_by("_trade_date"):
        day = day_df["_trade_date"][0]
        out = minute_dir / f"date={day}" / "part.parquet"
        incoming = day_df.drop("_trade_date")
        if missing_only:
            incoming = incoming.unique(subset=keys, keep="last")
        existing = _read_minute_day(table, day, fallback_part=out)
        if not existing.is_empty():
            if missing_only:
                # 老分区可能用毫秒，新数据用微秒。按北京墙钟对齐后仅插入缺失键。
                incoming = incoming.with_columns(pl.col("datetime").cast(pl.Datetime("us")))
                existing_keys = existing.select(
                    "symbol", pl.col("datetime").cast(pl.Datetime("us"))
                )
                incoming = incoming.join(existing_keys, on=keys, how="anti")
                if incoming.is_empty():
                    continue
                merged = pl.concat([existing, incoming], how="diagonal_relaxed")
            else:
                # 普通同步允许更新已存在的分钟，只对触及的标的去重。
                touched = pl.col("symbol").is_in(incoming["symbol"].unique().to_list())
                updated = pl.concat([existing.filter(touched), incoming], how="diagonal_relaxed")
                merged = pl.concat(
                    [existing.filter(~touched), updated.unique(subset=keys, keep="last")],
                    how="diagonal_relaxed",
                )
        else:
            merged = incoming
        out.parent.mkdir(parents=True, exist_ok=True)
        merged = merged.sort(keys)
        _rootmod._atomic_write_parquet(merged, out)
        # 只镜像本次变更行（existing 已在 CK 中），避免整日重复写
        _mirror_minute_to_ck(minute_dir, incoming)
        written += incoming.height if missing_only else merged.height
    return written


def _read_minute_day(table: str, day, *, fallback_part: Path | None = None) -> pl.DataFrame:
    """读某日分钟K（CK 为唯一读源；存储未初始化时退回本地分区，供离线脚本/单测）。"""
    if table not in ("kline_minute", "kline_etf_minute"):
        return pl.DataFrame()
    from datetime import datetime as _datetime
    from datetime import time as _time

    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        if fallback_part is not None and fallback_part.exists():
            return pl.read_parquet(fallback_part).filter(pl.col("datetime").is_not_null())
        return pl.DataFrame()
    return runtime.store.read_minute(
        table,
        start=_datetime.combine(day, _time.min),
        end=_datetime.combine(day, _time.max),
    )


def _mirror_minute_to_ck(minute_dir: Path, df: pl.DataFrame) -> None:
    """分钟分区落盘后镜像到 CK（读路径已切 CK）。

    只写 Parquet 会让分钟K页面永远看不到新数据；表名由落盘目录反推。
    存储层未初始化（离线脚本、单测）时跳过，保持原有 Parquet 行为。
    """
    table = minute_dir.name
    if table not in ("kline_minute", "kline_etf_minute"):
        return
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return
    runtime.store.write_minute(table, df)


def _selected_minute_provider_name() -> str:
    """识别偏好 getter 静默回退的失效自定义分钟源。"""
    saved = str(preferences.load().get("minute_data_provider", "tickflow") or "tickflow").lower()
    selected = preferences.get_minute_data_provider()
    if saved != "tickflow" and selected == "tickflow":
        return f"unavailable:{saved}"
    return selected


class MinuteSourceStamp(NamedTuple):
    data_dir: Path
    provider: str
    fingerprint: bytes


def minute_source_stamp() -> MinuteSourceStamp:
    """分钟请求只随当前分钟源、该源的配置和凭据失效。"""
    from app import secrets_store
    from app.config import settings

    name = _rootmod._selected_minute_provider_name()
    if name == "tickflow":
        from app.tickflow.client import PAID_ENDPOINT

        endpoint = secrets_store.load().get("tickflow_base_url") or PAID_ENDPOINT
        config = (endpoint, secrets_store.get_tickflow_key())
        fingerprint = hashlib.sha256(json.dumps(config).encode()).digest()
    else:
        from app.data_providers import custom

        fingerprint = custom.minute_config_fingerprint(name)
    return MinuteSourceStamp(settings.data_dir, name, fingerprint)


def _resolve_minute_provider(
    provider_name: str,
) -> tuple[object | None, bool, str | None]:
    """统一解析 custom minute provider, 把所有 resolver 调用纳入同一异常边界。

    供分钟拉取和落盘共用，解析失败统一返回错误，不跨源补数据。

    返回 (provider, should_fallback_to_tickflow, error_msg):
      - provider_name == "tickflow" → (None, True, None)
      - 自定义源未配置 minute dataset 或解析失败 → (None, False, error_msg)
      - 自定义源可用 → (provider, False, None)

    只有用户明确选择 TickFlow 时才请求 TickFlow,避免静默跨源。
    注意: provider.get_minute() 仍由调用方在自身 try 块内调用 (业务异常, 非解析异常)。
    """
    if provider_name == "tickflow":
        return (None, True, None)
    from app.data_providers import custom as custom_sources

    try:
        provider = custom_sources.get_provider(provider_name)
        datasets = provider.config.datasets
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return (None, False, str(e))
    if "minute" not in datasets:
        return (None, False, f"provider {provider_name} does not configure minute data")
    return (provider, False, None)


def _wrap_minute_callback(on_chunk_done, callback_failed):
    wrapped_cb: Callable[[int, int], None] | None = None
    if on_chunk_done is not None:

        def _wrapped_cb(cur: int, total: int) -> None:
            try:
                on_chunk_done(cur, total, "custom")
            except BaseException:
                callback_failed[0] = True
                raise

        wrapped_cb = _wrapped_cb
    return wrapped_cb


def _stream_custom_minutes(
    provider,
    provider_name,
    symbols,
    start_time,
    end_time,
    asset_type,
    freq,
    wrapped_cb,
    on_segment,
    callback_failed,
):
    stream = iter_minute_frames(
        provider,
        symbols,
        start_time=start_time,
        end_time=end_time,
        asset_type=asset_type,
        freq=freq,
        on_chunk_done=wrapped_cb,
    )
    try:
        while True:
            try:
                incoming = next(stream)
            except StopIteration:
                break
            except ProviderRequestError:
                raise
            except Exception as exc:  # iterator errors happen on next()
                if callback_failed[0]:
                    raise
                raise ProviderRequestError(
                    f"分钟源 {provider_name} 取数失败: {type(exc).__name__}"
                ) from exc
            try:
                if not isinstance(incoming, pl.DataFrame):
                    raise ValueError("分钟源未返回标准数据帧")
                df = _normalize_minute(incoming, source=provider_name)
                if not df.is_empty():
                    required = set(CANONICAL_MINUTE_COLS) - {"amount"}
                    if required - set(df.columns):
                        raise ValueError("分钟数据缺少必要字段")
                    if df["symbol"].null_count() or df["datetime"].null_count():
                        raise ValueError("分钟数据标的或时间为空")
            except Exception as exc:  # malformed frames must not be published
                raise ProviderRequestError(f"分钟源 {provider_name} 数据校验失败") from exc
            if not df.is_empty():
                on_segment(df)
    finally:
        stream.close()  # type: ignore[attr-defined]


def _call_custom_minute(
    provider,
    provider_name,
    symbols,
    start_time,
    end_time,
    asset_type,
    freq,
    wrapped_cb,
    callback_failed,
):
    try:
        df = provider.get_minute(  # type: ignore[attr-defined]
            symbols,
            start_time=start_time,
            end_time=end_time,
            asset_type=asset_type,
            freq=freq,
            on_chunk_done=wrapped_cb,
        )
    except ProviderRequestError:
        raise
    except Exception as e:
        if callback_failed[0]:
            raise
        _rootmod.logger.warning(
            "custom minute provider %s call failed; not falling back to TickFlow: %s",
            provider_name,
            e,
        )
        return (pl.DataFrame(), False)
    try:
        # 时区契约守卫: 插件/自定义源帧同样收口为北京墙钟 (CONTRIBUTING §3.3)
        df = _normalize_minute(df, source=provider_name)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning(
            "custom minute provider %s datetime 契约校验失败; not falling back to TickFlow: %s",
            provider_name,
            e,
        )
        return (pl.DataFrame(), False)
    return (df, False)


def _try_custom_minute(
    symbols: list[str],
    start_time: datetime | None,
    end_time: datetime | None,
    asset_type: AssetType,
    freq: str = "1m",
    on_chunk_done: Callable[[int, int, str], None] | None = None,
    on_segment: Callable[[pl.DataFrame], None] | None = None,
) -> tuple[pl.DataFrame | None, bool]:
    """尝试从自定义分钟源拉取。返回 (df, should_fallback_to_tickflow)。

    返回契约:
      (None, True)   → 用户明确选择 TickFlow
      (df, False)    → 自定义源成功(含空 df)或失败 → 直接使用结果, 不回退

    用户明确选择了自定义分钟源后,其请求失败或解析异常均 fail-closed;
    避免自定义源增广的 batch 能力被误认为 TickFlow 原生权限,造成静默跨源请求。

    resolver 异常边界由 _resolve_minute_provider 统一兜底; 业务调用
    (provider.get_minute) 仍在本函数 try 块内, 与 resolver 异常分离
    便于日志区分 ("resolution failed" vs "call failed")。

    on_chunk_done 适配: 上层回调是 3 参 (cur, total, seg_label), provider
    实现内部以 2 参 (cur, total) 调用。这里包装一层, provider 调 2 参时补
    默认 seg_label="custom" 转发给上层, 保证进度展示不降级。
    """
    provider_name = _rootmod._selected_minute_provider_name()
    if provider_name.startswith("unavailable:"):
        _rootmod.logger.warning(
            "custom minute provider %s is unavailable; not falling back to TickFlow",
            provider_name.removeprefix("unavailable:"),
        )
        return (pl.DataFrame(), False)
    provider, fallback, err = _rootmod._resolve_minute_provider(provider_name)
    if fallback:
        if err is not None:
            _rootmod.logger.warning(
                "custom minute provider %s unavailable; not falling back to TickFlow: %s",
                provider_name,
                err,
            )
            return (pl.DataFrame(), False)
        return (None, True)
    if err is not None or provider is None:
        _rootmod.logger.warning(
            "custom minute provider %s has no usable minute dataset: %s", provider_name, err
        )
        return (pl.DataFrame(), False)

    # 包装 on_chunk_done: provider 调 2 参 → 补 seg_label="custom" → 转发上层 3 参
    callback_failed = [False]
    wrapped_cb = _wrap_minute_callback(on_chunk_done, callback_failed)

    if on_segment is not None:
        _stream_custom_minutes(
            provider,
            provider_name,
            symbols,
            start_time,
            end_time,
            asset_type,
            freq,
            wrapped_cb,
            on_segment,
            callback_failed,
        )
        return (pl.DataFrame(), False)

    return _call_custom_minute(
        provider,
        provider_name,
        symbols,
        start_time,
        end_time,
        asset_type,
        freq,
        wrapped_cb,
        callback_failed,
    )


def sync_minute_batch(
    symbols: list[str],
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    count: int | None = None,
    batch_size: int | None = None,
    rpm: int | None = None,
    on_chunk_done: Callable[[int, int, str] | None] | None = None,  # type: ignore[misc]
    segment_trading_days: int = 20,
    on_segment: Callable[[pl.DataFrame], None] | None = None,
    asset_type: AssetType = "stock",
    raw_basis: bool = False,
) -> pl.DataFrame:
    """向选中的分钟源交付完整标的列表和时间窗;消费源返回的标准帧。

    核心不拆网络请求。原生 iter_minute 由源管理标的分批、时间分段、限速、
    重试和有界输出;旧 get_minute 仍一次收到完整请求,返回一帧。
    batch_size / rpm / count / segment_trading_days / raw_basis 为旧 TickFlow
    调用兼容选项,仅交给 TickFlow Provider,不覆盖第三方源配置。
    on_chunk_done(cur,total,label) 转发源进度;回调异常原样透传以支持取消。
    on_segment 每收到一帧立即调用(并非固定时间段),用于有界流式落盘;
    不传时保持原返回契约,合并帧后返回 DataFrame。
    """
    if not symbols:
        return pl.DataFrame()
    df, fallback = _rootmod._try_custom_minute(
        symbols,
        start_time=start_time,
        end_time=end_time,
        asset_type=asset_type,
        freq="1m",
        on_chunk_done=on_chunk_done,
        on_segment=on_segment,
    )
    if not fallback:
        if on_segment is not None:
            if df is not None and not df.is_empty():
                on_segment(df)
            return pl.DataFrame()
        return df if df is not None else pl.DataFrame()

    # 旧业务入口只转交选项;SDK 请求分批与时间分段由 TickFlow Provider 决定。
    from app.data_providers.tickflow_provider import TickFlowMinuteOptions, TickFlowProvider

    provider = TickFlowProvider(
        minute_options=TickFlowMinuteOptions(
            batch_size=batch_size,
            rpm=rpm,
            count=count,
            segment_trading_days=segment_trading_days,
            raw_basis=raw_basis,
        ),
        minute_client=_rootmod.get_client(),
        minute_call=_rootmod._sdk_call_with_deadline,
        minute_progress=on_chunk_done,
    )
    stream = provider.iter_minute(symbols, start_time, end_time, asset_type=asset_type)
    try:
        if on_segment is not None:
            for frame in stream:
                on_segment(frame)
            return pl.DataFrame()
        parts = [frame for frame in stream if not frame.is_empty()]
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()
    finally:
        stream.close()  # type: ignore[attr-defined]


def fetch_minute_single(
    symbol: str,
    trade_date: date,
    asset_type: AssetType = "stock",
    *,
    capset: CapabilitySet,
    raw_basis: bool = False,
) -> pl.DataFrame:
    source = minute_source_stamp()
    key = (source, capset, symbol, trade_date, asset_type, raw_basis)

    def fetch():
        frame = _fetch_minute_single(
            symbol, trade_date, asset_type, capset=capset, raw_basis=raw_basis
        )
        if source != minute_source_stamp():
            raise WorkBusyError("分钟数据源配置已变化，请重新加载")
        return frame

    return _rootmod._single_minute_work.run(key, fetch, ttl_s=2)


def _fetch_minute_single(
    symbol: str,
    trade_date: date,
    asset_type: AssetType,
    *,
    capset: CapabilitySet,
    raw_basis: bool,
) -> pl.DataFrame:
    """实时拉取单股单日分钟 K(不写入本地)。

    优先使用当前自定义分钟源。只有明确选择 TickFlow 且具备原生权限时才请求 TickFlow；
    已选自定义源缺少能力或请求失败时 fail-closed，不混用来源。
    """
    from datetime import datetime

    # 北京时间窗口必须带时区: naive datetime 会被 .timestamp() 按服务器本地时区解释,
    # UTC 容器上窗口整体偏移 8 小时, 分时补拉必然为空。
    start_time = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 25, 0, tzinfo=CN_TZ)
    end_time = datetime(trade_date.year, trade_date.month, trade_date.day, 15, 5, 0, tzinfo=CN_TZ)

    # 按用户当前选择的分钟数据源路由，避免无提示跨源。
    df, fallback = _rootmod._try_custom_minute(
        [symbol],
        start_time=start_time,
        end_time=end_time,
        asset_type=asset_type,
        freq="1m",
    )
    if not fallback:
        # 见 sync_minute_batch 同分支注释: df 在此必非 None。
        return df if df is not None else pl.DataFrame()

    if not capset.has(Cap.KLINE_MINUTE_BY_SYMBOL):
        return pl.DataFrame()

    tf = _rootmod.get_client()
    try:
        raw = tf.klines.batch(
            [symbol],
            period="1m",
            start_time=_datetime_to_ms(start_time),
            end_time=_datetime_to_ms(end_time),
            count=10000,
            adjust="none" if raw_basis else "forward",
            as_dataframe=False,
            show_progress=False,
        )
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("fetch_minute_single(%s, %s) failed: %s", symbol, trade_date, e)
        return pl.DataFrame()

    return _normalize_minute(_compact_klines_to_df(raw, default_symbol=symbol))


def _as_beijing(d: datetime) -> datetime:
    """落盘的分钟 datetime 是北京墙钟 naive, 带上北京时区再交给取数窗口。

    naive 值经 _datetime_to_ms 会被 .timestamp() 按服务器本地时区解释, 与同
    窗口另一端的服务器本地时间混用后整体错位 (UTC 容器上错 8 小时)。
    """
    return d if d.tzinfo is not None else d.replace(tzinfo=CN_TZ)


def _minute_bounds(
    repo: KlineRepository, asset_type: AssetType
) -> tuple[datetime | None, datetime | None]:
    """分钟 K  earliest/latest（CK）。minute_bounds 是 store 方法，不是 repository 方法。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    if runtime is None:
        return None, None
    table = "kline_etf_minute" if asset_type == "etf" else "kline_minute"
    return runtime.store.minute_bounds(table)


def _latest_minute_datetime(
    repo: KlineRepository, asset_type: AssetType = "stock"
) -> datetime | None:
    """本地分钟 K 数据的最新时间 (北京时区, CK)。"""
    try:
        _, latest = _minute_bounds(repo, asset_type)
        if latest is not None:
            return _as_beijing(latest)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.debug("最新分钟时间查询失败: %s", e)
    return None


def _earliest_minute_datetime(
    repo: KlineRepository, asset_type: AssetType = "stock"
) -> datetime | None:
    """本地分钟 K 数据的最早时间 (北京时区, 用于向前扩展的起点, CK)。"""
    try:
        earliest, _ = _minute_bounds(repo, asset_type)
        if earliest is not None:
            return _as_beijing(earliest)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.debug("最早分钟时间查询失败: %s", e)
    return None


def _cleanup_null_datetime_minute(repo: KlineRepository) -> None:
    """检测并清除 datetime 全为 null 的旧版分钟 K 数据(迁移用)。"""
    minute_dir = repo.store.data_dir / "kline_minute"
    if not minute_dir.exists():
        return
    try:
        # 直接按本地 parquet 文件判断旧版全 null 分区（不再经 DuckDB 视图计数，
        # 视图滞后会让判断失真；这里本来就是要清理磁盘上的旧文件）。
        with repo._write_lock:
            for part in minute_dir.rglob("*.parquet"):
                times = pl.read_parquet(part, columns=["datetime"])["datetime"]
                if len(times) and times.null_count() == len(times):
                    part.unlink()
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.debug("minute cleanup check failed: %s", e)


def _migrate_symbol_to_date_partition(repo: KlineRepository) -> None:
    """将旧版 symbol= 分区迁移为 date= 分区。迁移完成后删除旧目录。"""
    minute_dir = repo.store.data_dir / "kline_minute"
    if next(minute_dir.glob("symbol=*/*.parquet"), None) is None:
        return
    with repo._write_lock:
        sources = sorted(minute_dir.glob("symbol=*/*.parquet"))
        frames = []
        for part in sources:
            df = pl.read_parquet(part)
            if "datetime" not in df.columns:
                raise ValueError(f"旧分钟分区缺少 datetime: {part.name}")
            symbol = part.parent.name.removeprefix("symbol=")
            df = df.with_columns(
                (
                    pl.col("symbol").fill_null(symbol) if "symbol" in df.columns else pl.lit(symbol)
                ).alias("symbol")
            )
            frames.append(df)
        if not frames:
            return
        # 全部读入且写入成功才删除源文件。中途失败可以安全重试。
        added = _rootmod._write_minute_partition(
            pl.concat(frames, how="diagonal_relaxed"), minute_dir, missing_only=True
        )
        for part in sources:
            part.unlink()
        for folder in {part.parent for part in sources}:
            if not any(folder.iterdir()):
                folder.rmdir()
        _rootmod.logger.info("minute-K migration done: %d rows added", added)
