"""从 services/kline_sync.py 抽取的 kline_adj 子模块（保持原公开 API）。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import polars as pl

from app.data_providers.base import AssetType, ProviderRequestError
from app.data_providers.minute_normalizer import _datetime_to_ms, _timestamp_to_beijing_datetime
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


def _normalize_adj_factor(raw) -> pl.DataFrame:  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
    """Normalize SDK ex_factors response to symbol/trade_date/ex_factor."""
    if raw is None or len(raw) == 0:
        return pl.DataFrame()
    if isinstance(raw, dict):
        rows: list[dict] = []
        for sym, values in raw.items():
            for item in values or []:
                row = dict(item or {})
                row.setdefault("symbol", sym)
                rows.append(row)
        df = pl.DataFrame(rows) if rows else pl.DataFrame()
    elif isinstance(raw, pl.DataFrame):
        df = raw
    else:
        df = pl.from_pandas(raw.reset_index() if hasattr(raw, "reset_index") else raw)
    if df.is_empty():
        return df
    # rename: timestamp/date → trade_date, adj_factor → ex_factor
    # 注意: 新版 SDK 可能同时返回 timestamp 和 trade_date (或 adj_factor 和 ex_factor),
    # 直接 rename 会产生重复列报错。仅当目标列不存在时才 rename。
    rename_map: dict[str, str] = {}
    for src, dst in (
        ("timestamp", "trade_date"),
        ("date", "trade_date"),
        ("adj_factor", "ex_factor"),
    ):
        if src in df.columns and dst not in df.columns:
            rename_map[src] = dst  # noqa: PERF403 - PERF403: 循环体含复杂逻辑, 推导式不适用
    df = df.rename(rename_map)
    if "trade_date" in df.columns:
        if df.schema["trade_date"] in {
            pl.Int64,
            pl.Int32,
            pl.UInt64,
            pl.UInt32,
            pl.Float64,
            pl.Float32,
        }:
            # 毫秒时间戳 → 北京墙钟日期。不能直接 from_epoch().dt.date():
            # 那是 UTC 日期, 除权事件时间戳为北京零点 (= UTC 前一日 16:00),
            # 会整体早一天 (与 SDK True 路径 fromtimestamp(ts/1000, Asia/Shanghai) 不一致)。
            df = df.with_columns(
                _timestamp_to_beijing_datetime(pl.col("trade_date")).dt.date().alias("trade_date")
            )
        else:
            df = df.with_columns(pl.col("trade_date").cast(pl.Date, strict=False))
    if "ex_factor" in df.columns:
        df = df.with_columns(pl.col("ex_factor").cast(pl.Float64, strict=False))
    if not all(c in df.columns for c in ("symbol", "trade_date", "ex_factor")):
        return pl.DataFrame()
    # 明细列存在则透传 (缺省不影响规范三列); drop_nulls 只作用于规范三列 ——
    # 明细列合法可为 null (如纯送转事件的 dividend)。
    df = df.with_columns(
        [
            pl.col(c).cast(pl.Float64, strict=False)
            for c in _rootmod.ADJ_DETAIL_COLS
            if c in df.columns
        ]
    )
    cols = ["symbol", "trade_date", "ex_factor"] + [
        c for c in _rootmod.ADJ_DETAIL_COLS if c in df.columns
    ]
    return df.select(cols).drop_nulls(subset=["symbol", "trade_date", "ex_factor"])


def _align_adj_factor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """除权因子帧对齐为统一 schema: 规范三列 + 明细列 (缺则补 null 列)。"""
    df = df.with_columns(
        pl.col("trade_date").cast(pl.Date, strict=False),
        pl.col("ex_factor").cast(pl.Float64, strict=False),
        *[
            pl.col(c).cast(pl.Float64, strict=False)
            for c in _rootmod.ADJ_DETAIL_COLS
            if c in df.columns
        ],
    )
    df = df.with_columns(
        [
            pl.lit(None, dtype=pl.Float64).alias(c)
            for c in _rootmod.ADJ_DETAIL_COLS
            if c not in df.columns
        ]
    )
    return df.select(["symbol", "trade_date", "ex_factor", *_rootmod.ADJ_DETAIL_COLS])


def _mirror_adj_to_ck(repo: KlineRepository, table: str, df: pl.DataFrame) -> None:
    """过渡期双写：除权因子同时写 CK（设计文档 §8.4）。

    parquet 的 `all.parquet` 仍是读源，CK 侧 ReplacingMergeTree 按 (symbol, trade_date)
    收敛，重复写等价 upsert。失败只告警：Phase 2 读路径切 CK 后改为 fail-closed。
    """
    if df.is_empty():
        return
    store = repo._ck_store()
    if store is None:
        return
    try:
        store.write_adj_factor(_align_adj_factor_frame(df), etf=(table == "adj_factor_etf"))
    except Exception as e:  # noqa: BLE001 - 过渡期降级
        _rootmod.logger.warning("CK 双写除权因子失败 %s: %s", table, e)


def _merge_adj_factor_store(
    out: Path,
    new_data: pl.DataFrame,
    *,
    store=None,
) -> tuple[int, list[str]]:
    """合并除权因子, 返回 (新增行数, 因子发生变化的 symbol 列表)。

    CK 为存量读源, ReplacingMergeTree 按 (symbol, trade_date) 收敛 → 新行直接写
    即 upsert; ``all.parquet`` 在 B5 前仅作镜像。

    变化判定只看 ex_factor (新键 + 因子值差异 > 1e-9): 仅明细列回填
    (dividend/prev_close 等补 null) 不改变复权价格 → 不计入变化列表,
    enriched 局部重算因此不会被明细回填触发全市场重算。
    """
    from app.storage.runtime import get_storage

    runtime = get_storage()
    store = store or (runtime.store if runtime is not None else None)
    etf = out.parent.name == "adj_factor_etf"
    new_data = _align_adj_factor_frame(new_data)
    if store is not None:
        existing = store.read_adj_factor(etf=etf)
    else:
        # 离线脚本/单测（存储未初始化）退回 parquet 合并；线上 CK 是唯一存量来源
        existing = pl.read_parquet(out) if out.exists() else pl.DataFrame()
    before = existing.height
    if existing.is_empty():
        changed_symbols = sorted(new_data["symbol"].unique().to_list())
        merged = new_data.sort(["symbol", "trade_date"])
    else:
        # ex_factor 变化判定 (明细回填不算): 与存量按 (symbol, trade_date) 对齐,
        # 旧值缺失 (新事件) 或值差超容差即视为变化。
        diffed = (
            new_data.select(["symbol", "trade_date", "ex_factor"])
            .join(
                existing,
                on=["symbol", "trade_date"],
                how="left",
                suffix="_old",
            )
            .filter(
                pl.col("ex_factor_old").is_null()
                | ((pl.col("ex_factor") - pl.col("ex_factor_old")).abs() > 1e-9)
            )
        )
        changed_symbols = sorted(set(diffed["symbol"].to_list()))
        merged = (
            pl.concat([existing, new_data], how="diagonal")
            .unique(
                subset=["symbol", "trade_date"],
                keep="last",
            )
            .sort(["symbol", "trade_date"])
        )
    if store is not None:
        # B5：生产路径 CK 为唯一落盘目标；本地 all.parquet 只服务离线脚本/单测。
        store.write_adj_factor(new_data, etf=etf)
        return merged.height - before, changed_symbols
    out.parent.mkdir(parents=True, exist_ok=True)
    _rootmod._atomic_write_parquet(merged, out)
    return merged.height - before, changed_symbols


def _fetch_adj_factor_rows(tf, request) -> list[dict]:
    """读取除权因子单页并按当前格式归一。"""
    rows = _fetch_adj_factor_rows(tf, request)

    return rows


def _adj_custom_sync(provider_name, symbols, start_time, end_time, asset_type, on_chunk_done, repo):
    from app.data_providers import custom as custom_sources

    if not custom_sources.provider_has_dataset(provider_name, "adj_factor"):
        return None
    provider = custom_sources.get_provider(provider_name)
    new_data = provider.get_adj_factors(
        symbols,
        start_time=start_time,
        end_time=end_time,
        asset_type=asset_type,
        on_chunk_done=on_chunk_done,
    )
    if new_data.is_empty():
        # 扶摇等自定义源对 ETF 直接空返回, 与「该 ETF 无除权」无法区分;
        # 有 TickFlow 除权能力时回退, 否则 ETF 日K永远不复权。
        if asset_type == "etf":
            _rootmod.logger.info(
                "custom adj_factor provider %s returned no ETF rows, falling back to TickFlow",
                provider_name,
            )
            return None
        return 0, []
    factor_dir = "adj_factor_etf" if asset_type == "etf" else "adj_factor"
    added, affected = _merge_adj_factor_store(
        repo.store.data_dir / factor_dir / "all.parquet",
        new_data,
    )
    return added, affected


def _adj_fetch_tickflow(symbols, capset, start_time, end_time, on_chunk_done):
    tf = _rootmod.get_client()
    limit = _rootmod.resolve_limit(
        capset,
        Cap.ADJ_FACTOR,
        default_batch=50,
        default_rpm=30,
        default_rpm_when_unset=False,
    )

    # 构建 SDK 参数 (False: _normalize_adj_factor 的 dict 分支原生支持)
    sdk_kwargs: dict = {"as_dataframe": False, "batch_size": limit.batch, "show_progress": False}
    if start_time:
        sdk_kwargs["start_time"] = _datetime_to_ms(start_time)
    if end_time:
        sdk_kwargs["end_time"] = _datetime_to_ms(end_time)

    chunks = chunked(symbols, limit.batch)
    all_dfs: list[pl.DataFrame] = []
    failed_syms: list[str] = []

    for i, chunk in enumerate(chunks):
        sleep_between_batches(i, limit.rpm)
        try:
            raw = _rootmod._sdk_call_with_deadline(
                lambda: tf.klines.ex_factors(chunk, **sdk_kwargs)  # noqa: B023 - 闭包在本次循环内立即调用, 无延迟绑定风险
            )
            normalized = _normalize_adj_factor(raw)
            if not normalized.is_empty():
                all_dfs.append(normalized)
            _rootmod.logger.debug(
                "adj_factor chunk %d/%d: %d symbols", i + 1, len(chunks), len(chunk)
            )
        except SDKCallTimeoutError as e:
            # on_chunk_done 在 try 外, 成败都会走到 → 心跳/取消检查点不缺
            _rootmod.logger.warning("adj_factor chunk %d/%d timed out: %s", i + 1, len(chunks), e)
            failed_syms.extend(chunk)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            _rootmod.logger.warning("adj_factor chunk %d/%d failed: %s", i + 1, len(chunks), e)
            failed_syms.extend(chunk)

        if on_chunk_done:
            on_chunk_done(i + 1, len(chunks))

    # 部分失败可见化: 失败分块的标的不在 affected 里 → enriched 不会重算它们,
    # 它们会保持**旧的前复权价**直到下次成功同步。聚合一条 WARNING 让其可见。
    if failed_syms:
        _rootmod.logger.warning(
            "adj_factor 同步部分失败: %d/%d 标的未获取复权因子, 将保持旧复权价 (样例: %s)",
            len(failed_syms),
            len(symbols),
            failed_syms[:10],
        )

    if not all_dfs:
        return None
    return pl.concat(all_dfs, how="diagonal_relaxed") if len(all_dfs) > 1 else all_dfs[0]


def sync_adj_factor(
    symbols: list[str],
    repo: KlineRepository,
    capset: CapabilitySet,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    on_chunk_done: Callable[[int, int], None] | None = None,
    asset_type: str = "stock",
) -> tuple[int, list[str]]:
    """同步除权因子(Starter+)。SDK 接口:`tf.klines.ex_factors(symbols=...)`。

    支持增量: 传 start_time/end_time 只拉取该时间范围内的新除权事件。
    返回 (写入行数, 受影响的 symbol 列表) — 供 enriched 局部重算使用。
    受影响 = 因子新增或值变化 (ex_factor diff) 的标的; 仅事件明细列
    (dividend 等) 回填不改变复权价, 不计入受影响列表。
    """
    if not symbols:
        return 0, []

    provider_name = preferences.get_adj_factor_provider()
    if provider_name != "tickflow":
        result = _adj_custom_sync(
            provider_name, symbols, start_time, end_time, asset_type, on_chunk_done, repo
        )
        if result is not None:
            return result
        # 自定义源未配置 adj_factor → 回退 TickFlow

    if not capset.has(Cap.ADJ_FACTOR):
        _rootmod._log_cap_unavailable("adj_factor", "除权因子同步")
        return 0, []

    new_data = _adj_fetch_tickflow(symbols, capset, start_time, end_time, on_chunk_done)
    if new_data is None:
        return 0, []

    factor_dir = "adj_factor_etf" if asset_type == "etf" else "adj_factor"
    _mirror_adj_to_ck(repo, factor_dir, new_data)
    added, affected = _merge_adj_factor_store(
        repo.store.data_dir / factor_dir / "all.parquet", new_data
    )
    _rootmod.logger.info(
        "adj_factor merged: +%d new rows, %d changed symbols, %d fetched / %d symbols",
        added,
        len(affected),
        new_data.height,
        len(symbols),
    )
    return added, affected


def fetch_adj_factor_single(
    symbol: str,
    *,
    asset_type: AssetType = "stock",
    capset: CapabilitySet | None = None,
    strict: bool = False,
) -> pl.DataFrame:
    """按已配置的数据源获取单标的除权因子，不写入本地。

    返回结构: symbol, trade_date, ex_factor (空 DataFrame 表示无除权事件或拉取失败)。
    与 _apply_adj_factor / compute_enriched 的 factors 参数格式一致。
    """
    provider_name = preferences.get_adj_factor_provider()
    if provider_name != "tickflow":
        from app.data_providers import custom

        if custom.provider_has_dataset(provider_name, "adj_factor"):
            frame = custom.get_provider(provider_name).get_adj_factors(
                [symbol],
                start_time=None,
                end_time=None,
                asset_type=asset_type,
            )
            if not frame.is_empty() or asset_type != "etf":
                return frame
    if capset is not None and not capset.has(Cap.ADJ_FACTOR):
        return pl.DataFrame()
    tf = _rootmod.get_client()
    try:
        raw = tf.klines.ex_factors([symbol], as_dataframe=False, show_progress=False)
    except Exception as e:
        _rootmod.logger.warning("fetch_adj_factor_single(%s) failed: %s", symbol, e)
        if strict:
            raise ProviderRequestError("复权因子获取失败") from e
        return pl.DataFrame()
    return _normalize_adj_factor(raw)
