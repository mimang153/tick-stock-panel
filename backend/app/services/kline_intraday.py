"""从 services/kline_sync.py 抽取的 kline_intraday 子模块（保持原公开 API）。"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from app.data_providers.minute_normalizer import (
    _compact_klines_to_df,
    _enforce_minute_beijing_wallclock,
    _normalize_minute,
)
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.rate_limits import chunked


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.kline_sync"), name)


_rootmod = _RootProxy()


def intraday_monitor_support(capset: CapabilitySet | None) -> dict[str, object]:
    """返回分时信号监控可用的数据能力和单轮标的上限。"""
    provider_name = _rootmod._selected_minute_provider_name()
    _, fallback, error = _rootmod._resolve_minute_provider(provider_name)
    if error is not None:
        _rootmod.logger.warning(
            "minute provider resolution failed while checking monitor support: %s", error
        )
        return {
            "available": False,
            "source": None,
            "max_symbols": 0,
            "reason": "当前分钟数据源不可用，请检查数据源配置",
        }
    if not fallback:
        return {
            "available": True,
            "source": "custom_minute",
            "max_symbols": 100,
            "reason": "使用已配置的分钟数据插件",
        }
    if capset is None:
        return {
            "available": False,
            "source": None,
            "max_symbols": 0,
            "reason": "需要分钟 K 或日内分时数据权限",
        }
    for cap, source in (
        (Cap.INTRADAY_BATCH, "intraday_batch"),
        (Cap.KLINE_MINUTE_BATCH, "minute_batch"),
    ):
        if capset.has(cap):
            limits = capset.limits(cap)
            return {
                "available": True,
                "source": source,
                "max_symbols": max(1, int(limits.batch or 100)) if limits else 100,
                "reason": "日内分时数据可用" if cap == Cap.INTRADAY_BATCH else "分钟 K 数据可用",
            }
    for cap, source in (
        (Cap.INTRADAY, "intraday_single"),
        (Cap.KLINE_MINUTE_BY_SYMBOL, "minute_single"),
    ):
        if capset.has(cap):
            return {
                "available": True,
                "source": source,
                "max_symbols": 1,
                "reason": "当前权限仅支持单标的分时监控",
            }
    return {
        "available": False,
        "source": None,
        "max_symbols": 0,
        "reason": "需要分钟 K 或日内分时数据权限",
    }


def fetch_intraday_monitor_batch(
    symbols: list[str],
    capset: CapabilitySet | None,
    *,
    now: datetime | None = None,
) -> pl.DataFrame:
    """按当前能力获取分时信号所需的当日分钟数据，不落盘。"""
    if not symbols:
        return pl.DataFrame()
    support = _rootmod.intraday_monitor_support(capset)
    if not support["available"] or len(symbols) > int(support["max_symbols"]):  # type: ignore[call-overload]
        return pl.DataFrame()

    now = now or _rootmod.cn_now()
    start_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
    source = support["source"]
    if source in {"custom_minute", "minute_batch"}:
        limits = (
            capset.limits(Cap.KLINE_MINUTE_BATCH)
            if capset and capset.has(Cap.KLINE_MINUTE_BATCH)
            else None
        )
        return _rootmod.sync_minute_batch(
            symbols,
            start_time=start_time,
            end_time=now,
            batch_size=limits.batch if limits else None,
            rpm=limits.rpm if limits else None,
        )

    tf = _rootmod.get_client()
    frames: list[pl.DataFrame] = []
    try:
        if source == "intraday_batch":
            limits = capset.limits(Cap.INTRADAY_BATCH) if capset else None
            raw = tf.klines.intraday_batch(
                symbols,
                count=300,
                as_dataframe=False,
                show_progress=False,
                batch_size=limits.batch if limits and limits.batch else 100,
            )
            df = _normalize_minute(_compact_klines_to_df(raw))
        elif source == "intraday_single":
            raw = tf.klines.intraday(symbols[0], count=300, as_dataframe=False)
            df = _normalize_minute(_compact_klines_to_df(raw, default_symbol=symbols[0]))
        elif source == "minute_single":
            raw = tf.klines.get(symbols[0], period="1m", count=300, as_dataframe=False)
            df = _normalize_minute(_compact_klines_to_df(raw, default_symbol=symbols[0]))
        else:
            df = pl.DataFrame()
        if not df.is_empty():
            frames.append(df)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning(
            "intraday monitor fetch failed (%s, %d symbols): %s", source, len(symbols), e
        )
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _fetch_intraday_chunk(
    tf, chunk: list[str], count: int
) -> tuple[list[pl.DataFrame], Exception | None]:
    """单块分钟K容错拉取；异常作为返回值上交而非抛出。"""
    try:
        raw = tf.klines.intraday_batch(
            chunk,
            count=count,
            as_dataframe=False,
            show_progress=False,
            batch_size=len(chunk),
        )
        seg = _normalize_minute(_compact_klines_to_df(raw))
        return ([seg] if not seg.is_empty() else []), None
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        return [], e


def fetch_intraday_full_market_burst(
    symbols: list[str],
    capset: CapabilitySet | None,
    *,
    count: int = 300,
) -> tuple[pl.DataFrame, int]:
    """全市场当日分钟K并发脉冲拉取 (盘中增量刷新专用, 不落盘)。

    与 fetch_intraday_monitor_batch 的区别:
    - 监控路径每轮只拉少量标的 (≤ batch 上限, 单请求);
      本函数按 batch_size 把全市场切块后用线程池一次全部打出
      (5546/200 = 28 并发), 配合 >=60s 的固定轮节奏, 任何 60s
      滑动窗口至多一个脉冲 (28 < 48 安全 rpm)。
    - 单块失败不拖垮整轮: 成功块照常返回落盘; 失败块立即单独重试一次,
      仍失败则跳过该块 (本函数每轮拉全天, 下一轮天然自愈)。失败块过多
      (>4, 系统性故障/限流风暴) 时跳过重试, 避免向已过载的服务端加压。

    限流口径: 只用 intraday.batch 独立池 (Cap.INTRADAY_BATCH, Expert 专有),
    不与 kline.minute.batch (盘后分钟同步) 共享配额。
    返回 (当日全市场分钟K, 请求数)。
    """
    if not symbols:
        return (pl.DataFrame(), 0)
    limits = (
        capset.limits(Cap.INTRADAY_BATCH) if capset and capset.has(Cap.INTRADAY_BATCH) else None
    )
    batch_size = max(1, int(limits.batch) if limits and limits.batch else 200)
    chunks = list(chunked(symbols, batch_size))
    if not chunks:
        return (pl.DataFrame(), 0)

    from concurrent.futures import ThreadPoolExecutor

    tf = _rootmod.get_client()

    def _fetch(chunk: list[str]) -> tuple[list[pl.DataFrame], Exception | None]:
        return _fetch_intraday_chunk(tf, chunk, count)

    frames: list[pl.DataFrame] = []
    failed: list[list[str]] = []
    with ThreadPoolExecutor(max_workers=min(len(chunks), 32)) as pool:
        for chunk, (sub, err) in zip(chunks, pool.map(_fetch, chunks), strict=True):
            if err is not None:
                failed.append(chunk)
            else:
                frames.extend(sub)

    requests = len(chunks)
    if failed:
        if len(failed) > 4:
            missed = sum(len(chunk) for chunk in failed)
            _rootmod.logger.warning(
                "intraday burst: %d/%d chunks failed (%d symbols), systemic — skip retry, next round re-pulls full day",
                len(failed),
                len(chunks),
                missed,
            )
        else:
            _rootmod.logger.warning(
                "intraday burst: %d/%d chunks failed, retrying once", len(failed), len(chunks)
            )
            for chunk in failed:
                sub, err = _fetch(chunk)
                requests += 1
                if err is None:
                    frames.extend(sub)
                else:
                    _rootmod.logger.warning(
                        "intraday burst: chunk retry still failed, skip %d symbols this round: %s",
                        len(chunk),
                        err,
                    )
    if not frames:
        return (pl.DataFrame(), requests)
    return (pl.concat(frames, how="diagonal_relaxed"), requests)


def fetch_intraday_universe_increment(
    universe: str = "CN_Equity_A",
    *,
    count: int = 3,
) -> tuple[pl.DataFrame, int]:
    """全市场当日分钟K增量拉取 (盘中稳态轮专用, 不落盘)。

    /v1/klines/intraday/universe: 传 universe ID 一次请求返回全市场每只标的
    最新 count 根分钟K (服务端实测上限 3 根/标的), 替代稳态场景下 28 块并发
    的 intraday.batch 脉冲 (请求量 28→1, 传输量 ~40 倍降)。缺口回补
    (冷启动/长时间断档/全天修复) 仍走 fetch_intraday_full_market_burst。
    返回 (增量分钟K, 请求数); 拉取失败返回空 df 由调用方按失败轮处理。

    as_dataframe=False: raw 的 CompactKlineData 已按列组织 (字段→数组),
    直接用列数组建 polars 帧 — 无 pandas 中转、无逐行转换、无 _resolve_names
    名称解析, 全市场单轮本地转换 ~1.1s → ~0.1s。时区/dtype/列序契约
    复用 _normalize_minute (timestamp→北京墙钟 datetime, 输出 canonical 8 列)。
    """
    tf = _rootmod.get_client()
    try:
        raw = tf.klines.intraday_universe(universe, count=count, as_dataframe=False)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("intraday universe fetch failed (%s): %s", universe, e)
        return (pl.DataFrame(), 0)
    seg = _compact_klines_to_df(raw)
    if seg.is_empty():
        return (pl.DataFrame(), 0)
    return (_normalize_minute(seg), 1)


def _resolve_full_minute_provider(
    provider_name: str,
) -> tuple[object | None, bool, str | None]:
    """解析全量分钟生效的自定义源。返回 (provider, should_use_tickflow, error_msg):
    返回 (provider, should_use_tickflow, error_msg):

        - 明确选择 tickflow → (None, True, None)
        - 自定义源缺少 full_minute 或解析失败 → (None, False, error_msg)
        - 成功 → (provider, False, None)
    """
    if provider_name == "tickflow":
        return (None, True, None)
    from app.data_providers import custom as custom_sources

    try:
        provider = custom_sources.get_provider(provider_name)
        datasets = provider.config.datasets
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return (None, False, str(e))
    if "full_minute" not in datasets:
        return (None, False, f"provider {provider_name} does not configure full_minute data")
    return (provider, False, None)


def fetch_intraday_custom_batch(
    provider: object,
    provider_name: str,
    symbols: list[str],
) -> tuple[pl.DataFrame, int]:
    """自定义源全量分钟修复轮: 当日窗口全市场批量拉取 (不落盘)。

    provider 契约 (见 docs/plugin-development.md):
    - get_intraday_batch(symbols, count, asset_type) 优先 — 源自管批量端点;
    - 未实现则回退 get_minute(symbols, 当日窗口) — 复用逐标的分钟K机制,
      请求数按 chunk 回调统计。
    帧统一过北京墙钟守卫 (与 _try_custom_minute 同纪律)。
    返回 (当日分钟K, 请求数); 失败返回空 df 由调用方按空轮处理。
    """
    try:
        method = getattr(provider, "get_intraday_batch", None)
        if callable(method):
            df = method(symbols)
            requests = 1
        else:
            counted = {"requests": 0}

            def _count_requests(cur: int, total: int) -> None:
                counted["requests"] = max(counted["requests"], int(total))

            end = _rootmod.cn_now()
            start = end.replace(hour=0, minute=0, second=0, microsecond=0)
            df = provider.get_minute(  # type: ignore[attr-defined]
                symbols,
                start_time=start,
                end_time=end,
                asset_type="stock",
                freq="1m",
                on_chunk_done=_count_requests,
            )
            requests = counted["requests"]
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("custom full_minute batch via %s failed: %s", provider_name, e)
        return (pl.DataFrame(), 0)
    try:
        df = _enforce_minute_beijing_wallclock(df, source=provider_name)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning(
            "custom full_minute datetime 契约校验失败 (%s): %s", provider_name, e
        )
        return (pl.DataFrame(), 0)
    return (df, max(requests, 1))


def fetch_intraday_custom_latest(
    provider: object,
    provider_name: str,
    *,
    count: int = 3,
) -> tuple[pl.DataFrame, int] | None:
    """自定义源全量分钟稳态增量轮 (不落盘)。

    provider 可选实现 get_intraday_latest(symbols=None, count) → 每只标的最新
    count 根分钟K, 尽量单请求/低请求量 (TickFlow 的 intraday.universe 同义)。
    未实现返回 None — 调用方降级为仅修复轮模式。失败返回 (空 df, 0) 按空轮处理。
    """
    method = getattr(provider, "get_intraday_latest", None)
    if not callable(method):
        return None
    try:
        df = method(count=count)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("custom full_minute latest via %s failed: %s", provider_name, e)
        return (pl.DataFrame(), 0)
    try:
        df = _enforce_minute_beijing_wallclock(df, source=provider_name)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning(
            "custom full_minute datetime 契约校验失败 (%s): %s", provider_name, e
        )
        return (pl.DataFrame(), 0)
    return (df, 1)
