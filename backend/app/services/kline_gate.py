"""从 services/kline_sync.py 抽取的 kline_gate 子模块（保持原公开 API）。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from datetime import time as clock_time
from typing import TYPE_CHECKING

import polars as pl

from app.data_providers.base import AssetType
from app.market_time import CN_TZ
from app.services import minute_adjust, preferences
from app.services.shared_work import WorkBusyError
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.repository import KlineRepository

if TYPE_CHECKING:
    from .kline_minute import MinuteSourceStamp


def __getattr__(name):
    if name == "MinuteSourceStamp":
        from .kline_minute import MinuteSourceStamp

        return MinuteSourceStamp
    raise AttributeError(name)


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.kline_sync"), name)


_rootmod = _RootProxy()


def _minute_split_by_asset(
    symbols,
    repo,
    capset,
    days,
    on_chunk_done,
    extend_backward,
    force_full_days,
    repair_date,
    source_stamp,
):
    etf_set = repo.get_etf_symbol_set()
    etf_symbols = [symbol for symbol in symbols if symbol in etf_set]
    if not etf_symbols:
        return None
    stock_symbols = [symbol for symbol in symbols if symbol not in etf_set]
    written = 0
    for asset, group in (("stock", stock_symbols), ("etf", etf_symbols)):
        if group:
            written += _rootmod.sync_and_persist_minute(
                group,
                repo,
                capset,
                days=days,
                on_chunk_done=on_chunk_done,
                extend_backward=extend_backward,
                force_full_days=force_full_days,
                asset_type=asset,  # type: ignore[arg-type]
                repair_date=repair_date,
                expected_source=source_stamp,
            )
    return written


def _minute_gate(repo, capset, asset_type, repair_date, source_stamp):
    minute_provider = source_stamp.provider
    # 自定义源的能力和请求失败时都 fail-closed；共享能力集可能包含第三方源增补。
    _, fallback, resolve_err = _rootmod._resolve_minute_provider(minute_provider)
    if resolve_err is not None:
        _rootmod.logger.warning(
            "custom minute provider %s resolution failed at sync_and_persist_minute: %s",
            minute_provider,
            resolve_err,
        )
        return False
    if fallback and not capset.has(Cap.KLINE_MINUTE_BATCH):
        return False

    # 迁移:旧版 _normalize_minute 未转换 timestamp→datetime,导致全部 datetime 为 null
    # 检测到后直接清除(这些数据无法使用)
    if asset_type == "stock":
        if repair_date is None:
            _rootmod._cleanup_null_datetime_minute(repo)
        # 迁移:旧版按 symbol= 分区转为 date= 分区
        _rootmod._migrate_symbol_to_date_partition(repo)
    return True


def _minute_window(repo, asset_type, repair_date, days, extend_backward, force_full_days):
    # 窗口两端统一为北京时区: 起止点会与本地分钟 K 的北京墙钟混用, 用服务器
    # 本地时间会让窗口整体错位 (UTC 容器上起点晚于终点, 增量补拉一个请求都发不出)。
    now = _rootmod.cn_now()
    if repair_date is not None:
        start_time = datetime.combine(repair_date, clock_time(9, 25), CN_TZ)
        end_time = min(datetime.combine(repair_date, clock_time(15, 5), CN_TZ), now)
        if end_time <= start_time:
            return None
        return start_time, end_time
    end_time = now
    calendar_days = days
    last_dt = None
    if extend_backward:
        end_time = _rootmod._earliest_minute_datetime(repo, asset_type) or now
        calendar_days = int(days * 7 / 5) + (10 if days > 41 else 0)
    elif force_full_days:
        calendar_days = int(days * 7 / 5) + 5
    else:
        last_dt = _rootmod._latest_minute_datetime(repo, asset_type)
    # 只有追尾增量沿用精确分钟。历史首日从开盘前取，避免继承点击时刻。
    start_time = last_dt or datetime.combine(
        end_time.date() - timedelta(days=calendar_days),
        clock_time(9, 25),
        CN_TZ,
    )
    return start_time, end_time


def _minute_sync_window(
    symbols,
    repo,
    capset,
    on_chunk_done,
    asset_type,
    start_time,
    end_time,
    repair_date,
    source_stamp,
):
    limit = _rootmod.resolve_limit(
        capset,
        Cap.KLINE_MINUTE_BATCH,
        default_batch=100,
        default_rpm=30,
        default_rpm_when_unset=False,
    )

    # 原生流式源每帧立即写盘, 核心不再积累全量;旧源仍保留 get_minute 返回契约。
    # 全量攒内存曾导致 1 年全市场分钟 K OOM 卡死 (3 亿行 / 数十 GB)。
    minute_table = "kline_etf_minute" if asset_type == "etf" else "kline_minute"
    minute_dir = repo.store.data_dir / minute_table
    raw_basis = minute_adjust.minute_basis_is_raw(repo.store.data_dir)
    written_box = [0]  # list 闭包, 绕过 Python 闭包外层赋值

    def _persist(seg_df: pl.DataFrame) -> None:
        if repair_date is not None:
            seg_df = seg_df.filter(
                pl.col("symbol").is_in(symbols)
                & pl.col("datetime").is_between(
                    start_time.replace(tzinfo=None), end_time.replace(tzinfo=None)
                )
            )
        # 单股自动补齐可能与另一个补齐请求同时写同一日期分区。Windows 不允许
        # 替换仍被另一写入占用的临时文件,因此读-改-写必须复用仓库写锁。
        with repo._write_lock:
            if raw_basis != minute_adjust.minute_basis_is_raw(repo.store.data_dir):
                raise WorkBusyError("分钟价格基准已变化，请重新补齐")
            if source_stamp != _rootmod.minute_source_stamp():
                raise WorkBusyError("分钟数据源配置已变化，请重新补齐")
            if repair_date is not None:
                written_box[0] += _rootmod._write_minute_partition(
                    seg_df, minute_dir, missing_only=True
                )
            else:
                written_box[0] += _rootmod._write_minute_partition(seg_df, minute_dir)

    segment_days = preferences.get_minute_sync_segment_days()
    try:
        _rootmod.sync_minute_batch(
            symbols,
            start_time=start_time,
            end_time=end_time,
            batch_size=limit.batch,
            rpm=limit.rpm,
            on_chunk_done=on_chunk_done,
            segment_trading_days=segment_days,
            on_segment=_persist,
            asset_type=asset_type,
            raw_basis=raw_basis,
        )
    finally:
        # 后续批次失败时，先前已落盘的股票/ETF 分区仍需可查询。
        if written_box[0]:
            repo.store.data_dir.as_posix()
    return written_box[0]


def sync_and_persist_minute(
    symbols: list[str],
    repo: KlineRepository,
    capset: CapabilitySet,
    days: int = 5,
    on_chunk_done: Callable[[int, int, str], None] | None = None,
    extend_backward: bool = False,
    force_full_days: bool = False,
    *,
    asset_type: AssetType | None = None,
    repair_date: date | None = None,
    expected_source: MinuteSourceStamp | None = None,
) -> int:
    """同步分钟 K 并存到 Parquet。返回写入行数。

    存储口径由基准标记决定 (services/minute_adjust): 存量未迁移 → SDK adjust=qfq
    前复权 (旧行为); 已迁移 → adjust='none' 原始价落盘, 复权读取时投影。
    使用 start_time / end_time 区间拉取, 确保所有标的覆盖同一时间段。
    on_chunk_done(current, total) 每个 chunk 完成后回调。
    force_full_days=True 时强制回溯 days 自然日 (不增量补, 用于个股补齐历史)。
    repair_date 指定北京时间的单日窗口，只补缺失记录，返回实际新增根数。
    未指定资产类型时按维表拆分股票/ETF，分别取数、落盘与刷新视图。
    """
    if not symbols:
        return 0
    source_stamp = _rootmod.minute_source_stamp()
    if expected_source is not None and source_stamp != expected_source:
        raise WorkBusyError("分钟数据源配置已变化，请重新补齐")
    if asset_type is None:
        written = _minute_split_by_asset(
            symbols,
            repo,
            capset,
            days,
            on_chunk_done,
            extend_backward,
            force_full_days,
            repair_date,
            source_stamp,
        )
        if written is not None:
            return written
        asset_type = "stock"
    if asset_type not in {"stock", "etf"}:
        raise ValueError("分钟落库同步仅支持股票和 ETF")

    if not _minute_gate(repo, capset, asset_type, repair_date, source_stamp):
        return 0

    window = _minute_window(repo, asset_type, repair_date, days, extend_backward, force_full_days)
    if window is None:
        return 0
    start_time, end_time = window

    written = _minute_sync_window(
        symbols,
        repo,
        capset,
        on_chunk_done,
        asset_type,
        start_time,
        end_time,
        repair_date,
        source_stamp,
    )
    if written == 0:
        return 0
    _rootmod.logger.info("minute K synced: %d rows (%d symbols)", written, len(symbols))
    return written
