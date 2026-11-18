"""分钟 K 辅助：本地优先、缺洞分类、补拉与响应组装。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Request

from app.api._root_ref import bind as _bind
from app.services import kline_sync, minute_adjust, trading_day

from ._kline_http import _batch_frames_json, _gzip_payload, _gzip_raw

_root = _bind("app.api.kline")
logger = _root.logger


def _minute_batch_since(since_str):
    from datetime import datetime

    since_dt = None
    if since_str:
        try:
            since_dt = datetime.fromisoformat(str(since_str))
            # 带 Z/偏移的 aware 输入 → 转北京墙钟再去 tz, 否则与 naive 北京时间比较会出错
            if since_dt.tzinfo is not None:
                since_dt = since_dt.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
        except ValueError:
            since_dt = None
    return since_dt


def _minute_batch_trade_date(repo, trade_date_str):
    trade_date = date.fromisoformat(trade_date_str) if trade_date_str else _root.cn_today()
    # 非交易日(周末/节假日)才回退到最近有数据的交易日; 否则盘中会显示昨天而非今天。
    # 判据: 周末必回退; 工作日收盘后(>=15:30)仍无今日日K → 节假日, 回退。
    if not trade_date_str:
        today = _root.cn_today()
        # 周六/周日必非交易日; 工作日休市以交易日探针的「确定休市」为准, 未知维持收盘后判据
        need_fallback = today.weekday() >= 5 or trading_day.is_trading_day() is False
        if not need_fallback:
            now_cn = _root.cn_now()
            after_close = now_cn.hour > 15 or (now_cn.hour == 15 and now_cn.minute >= 30)
            if after_close:
                latest_daily = repo.latest_daily_date()
                if latest_daily is None or latest_daily < today:
                    need_fallback = True
        if need_fallback:
            recent_date = repo.latest_minute_date_global()
            if recent_date is None:
                recent_date = repo.latest_daily_date()
            if recent_date is not None:
                trade_date = recent_date
    return trade_date


def _minute_batch_local(repo, stock_syms, etf_syms, trade_date):
    import polars as pl

    df_local = repo.get_minute_batch(stock_syms, trade_date)
    if etf_syms:
        df_etf = repo.get_minute_batch(etf_syms, trade_date, asset_type="etf")
        if df_local.is_empty():
            df_local = df_etf
        elif not df_etf.is_empty():
            df_local = pl.concat([df_local, df_etf], how="diagonal_relaxed")
    return df_local


def _minute_batch_expected(trade_date):
    now = _root.cn_now()
    h, m = now.hour, now.minute
    if trade_date != _root.cn_today():
        return 240
    if h < 9 or (h == 9 and m < 30):
        return 0
    if h < 11 or (h == 11 and m <= 30):
        return (h - 9) * 60 + m - 30
    if h < 13:
        return 120
    if h < 15:
        return 120 + (h - 13) * 60 + m
    return 240


def _minute_has_holes(sub, day_open_floor) -> bool:
    if not sub.is_empty() and sub["datetime"][0] > day_open_floor:
        return True
    gaps = sub["datetime"].diff().dt.total_minutes().drop_nulls()
    return gaps.filter((gaps != 1) & (gaps != 91)).len() > 0  # 91 = 11:30→13:01 午休


def _minute_batch_classify(symbols, df_local, expected, trade_date):
    import polars as pl

    # 本地状态分类 (补拉已改为取到即落盘, 完整性判定随之收紧):
    # - fresh:  根数 >= 期望-2 (时间边界容差), 直接用本地。
    # - holes:  缺K → 全天重拉回填, 否则"最后一根+1min"的增量窗口永远不会回看洞。
    #           含中间的洞 (相邻间距非 1 分钟 / 非午休 91 分钟) 与前部的洞
    #           (首根显著晚于开盘 — 盘中重启/停机跨开盘的残留)。
    # - stale:  仅尾部落后 → 增量拉, 请求量从"每轮全天"降为"每轮一根"量级。
    # 前部洞基准: 开盘后 6 分钟 (容许无集合竞价K的数据源)。晚开/停牌复牌的票
    # 也会命中 → 全天拉幂等, 至多多一次批量请求, 与中间洞同一代价模型。
    day_open_floor = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 36, 0)
    result: dict[str, pl.DataFrame] = {}
    full_pull: list[str] = []  # 无数据或中间有洞 → 全天拉
    stale_last: dict[str, datetime] = {}  # 尾部落后 → symbol → 最后一根时间
    local_parts: dict[str, pl.DataFrame] = {}
    if not df_local.is_empty():
        for part in df_local.partition_by("symbol", maintain_order=True):
            local_parts[part["symbol"][0]] = part.sort("datetime")
    fresh_floor = max(0, expected - 2)
    for sym in symbols:
        sub = local_parts.get(sym, pl.DataFrame())
        if expected == 0 or sub.height >= fresh_floor:
            if not sub.is_empty():
                result[sym] = sub
            continue
        if sub.is_empty() or _minute_has_holes(sub, day_open_floor):
            full_pull.append(sym)
        else:
            stale_last[sym] = sub["datetime"][-1]
    return result, full_pull, stale_last, local_parts


def _minute_batch_suppress_stale(result, stale_last, local_parts, etf_set):
    # 纯尾部落后 (stale_last): 服务的增量轮下一轮就会补上, 股票不补拉省请求。
    # 空洞 (full_pull): 服务增量锚定本地最新时间, 不会回看洞 → 端点全天拉取修复。
    # ETF 不在服务 universe 内, 两类均维持补拉。
    for sym in stale_last:
        if sym not in etf_set:
            sub = local_parts.get(sym)
            if sub is not None and not sub.is_empty():
                result[sym] = sub
    return {s: t for s, t in stale_last.items() if s in etf_set}


def _minute_batch_pull_one(repo, request, asset, sym_list, start, session_end, lim):
    if not sym_list:
        return {}
    raw_basis = minute_adjust.minute_basis_is_raw(repo.store.data_dir)
    from app.services.minute_shared_pull import pull

    qs = getattr(request.app.state, "quote_service", None)
    df_live = pull(
        repo,
        sym_list,
        asset_type=asset,
        start=start,
        end=session_end,
        batch_size=lim.batch,  # type: ignore[arg-type]
        rpm=lim.rpm,  # type: ignore[arg-type]
        raw_basis=raw_basis,
        on_updated=qs.notify_data_updated if qs else None,
    )
    if df_live.is_empty():
        return {}
    if raw_basis:
        df_live = minute_adjust.apply_minute_adjustment(df_live, repo.store.data_dir, asset)
    return {
        part["symbol"][0]: part.sort("datetime")
        for part in df_live.partition_by("symbol", maintain_order=True)
    }


def _minute_batch_pull(repo, request, capset, full_pull, stale_last, etf_set, trade_date):
    import polars as pl

    from app.tickflow.capabilities import Cap
    from app.tickflow.rate_limits import resolve_limit

    day_start = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 25, 0)
    session_end = datetime(trade_date.year, trade_date.month, trade_date.day, 15, 5, 0)
    lim = resolve_limit(capset, Cap.KLINE_MINUTE_BATCH)
    live_map: dict[str, pl.DataFrame] = {}
    live_map.update(
        _minute_batch_pull_one(
            repo,
            request,
            "stock",
            [s for s in full_pull if s not in etf_set],
            day_start,
            session_end,
            lim,
        )
    )
    live_map.update(
        _minute_batch_pull_one(
            repo,
            request,
            "etf",
            [s for s in full_pull if s in etf_set],
            day_start,
            session_end,
            lim,
        )
    )
    if stale_last:
        # 增量公共起点 = 最旧的最后一根本身: 最后一根是形成中的动态K, 必须重拉并
        # 以定版值覆盖; 重叠由 upsert/合并去重吸收。
        inc_start = min(stale_last.values())
        if inc_start < session_end:
            live_map.update(
                _minute_batch_pull_one(
                    repo,
                    request,
                    "stock",
                    [s for s in stale_last if s not in etf_set],
                    inc_start,
                    session_end,
                    lim,
                )
            )
            live_map.update(
                _minute_batch_pull_one(
                    repo,
                    request,
                    "etf",
                    [s for s in stale_last if s in etf_set],
                    inc_start,
                    session_end,
                    lim,
                )
            )
    return live_map


def _minute_batch_merge(result, local_parts, live_map):
    import polars as pl

    # 合并: 有增量/回填的 symbol = 本地 + 拉取 upsert; 仅拉到的 (missing) 直接进结果。
    # 补拉落空兜底 (truncated): 本地已有部分K但本轮补拉失败/为空时, 返回本地已落盘的行,
    # 图冻结在最后落盘时刻, 而不是整只从结果里消失。truncated 标记残缺快照供前端提示。
    truncated: set[str] = set()
    for sym, sub in local_parts.items():
        live = live_map.get(sym)
        if live is not None:
            merged = (
                pl.concat([sub, live], how="diagonal_relaxed")
                .unique(subset=["symbol", "datetime"], keep="last")
                .sort("datetime")
            )
            result[sym] = merged
        elif sym not in result and not sub.is_empty():
            result[sym] = sub
            truncated.add(sym)
    for sym, live in live_map.items():
        if sym not in result:
            result[sym] = live
    return result, truncated


def _minute_batch_response(request, result, truncated, since_dt, full_minute_healthy):
    import polars as pl

    # since 增量过滤: 只回 >= since 的K (含动态最后一根), 无新增的 symbol 不回
    if since_dt is not None:
        result = {sym: sub.filter(pl.col("datetime") >= since_dt) for sym, sub in result.items()}
        result = {sym: sub for sym, sub in result.items() if sub.height}
    # since 过滤后已无行的 symbol 不再标记 truncated (标记与 data 键一致)
    truncated &= result.keys()
    extra = {"full_minute_local": full_minute_healthy, "incremental": since_dt is not None}
    if truncated:
        extra["truncated"] = sorted(truncated)  # type: ignore[assignment]
    return _gzip_raw(
        request,
        _batch_frames_json(result, "datetime", "%Y-%m-%dT%H:%M:%S", extra),
        pref_key="minute_batch_compress",
    )


@dataclass
class _MinuteCtx:
    request: Request
    symbol: str
    stock_name: Any
    stock_info: dict
    asset_type: str
    price_limit: Any
    prev_close: Any


def _minute_payload(ctx, trade_date, rows, source, expected=None, actual=None):
    payload = {
        "symbol": ctx.symbol,
        "name": ctx.stock_name,
        "stock_info": ctx.stock_info,
        "date": str(trade_date),
        "rows": rows,
        "source": source,
    }
    if expected is not None:
        payload["expected"] = expected
        payload["actual"] = actual
    payload.update(
        {
            "asset_type": ctx.asset_type,
            "price_limit": ctx.price_limit,
            "prev_close": ctx.prev_close,
        }
    )
    return _gzip_payload(ctx.request, payload, pref_key="minute_batch_compress")


def _minute_resolve_trade_date(repo, trade_date, asset_type, symbol):
    if trade_date is None:
        # 默认看今天, 而不是本地落盘的最近日 (盘中后者是昨天)。
        # 非交易日(周末/节假日)才回退到本地最近有数据的交易日。
        today = _root.cn_today()
        need_fallback = today.weekday() >= 5 or trading_day.is_trading_day() is False
        if not need_fallback:
            now_cn = _root.cn_now()
            after_close = now_cn.hour > 15 or (now_cn.hour == 15 and now_cn.minute >= 30)
            if after_close:
                latest_daily = repo.latest_daily_date()
                if latest_daily is None or latest_daily < today:
                    need_fallback = True
        if need_fallback:
            recent = repo.latest_minute_date(symbol, asset_type=asset_type)
            if recent is None:
                recent = repo.latest_daily_date()
            trade_date = recent if recent is not None else today
        else:
            trade_date = today
    return trade_date


def _minute_first_live_response(
    request, repo, capset, raw_basis, symbol, asset_type, stock_name, stock_info
):
    # 本地无任何分钟K, 尝试从当前有效分钟源拉取当天
    trade_date = _root.cn_today()
    df = kline_sync.fetch_minute_single(
        symbol, trade_date, asset_type=asset_type, capset=capset, raw_basis=raw_basis
    )
    if raw_basis:
        df = minute_adjust.apply_minute_adjustment(df, repo.store.data_dir, asset_type)
    price_limit = _root._get_price_limit_info(repo, symbol, trade_date, asset_type, stock_name)
    prev_close = _root._get_previous_closes(repo, symbol, [trade_date], asset_type).get(trade_date)
    ctx = _MinuteCtx(request, symbol, stock_name, stock_info, asset_type, price_limit, prev_close)
    return _minute_payload(ctx, trade_date, df.to_dicts(), "live" if not df.is_empty() else "none")


def _minute_fetch_adjust(
    symbol, trade_date, asset_type, capset, raw_basis, data_dir, *, skip_if_empty=False
):
    df = kline_sync.fetch_minute_single(
        symbol, trade_date, asset_type=asset_type, capset=capset, raw_basis=raw_basis
    )
    if raw_basis and (not skip_if_empty or not df.is_empty()):
        df = minute_adjust.apply_minute_adjustment(df, data_dir, asset_type)
    return df


def _minute_expected(trade_date):
    # 完整交易日应有 240 条分钟K；如果是今天(盘中)，期望条数按已交易分钟估算
    if trade_date != _root.cn_today():
        return 240
    now = _root.cn_now()
    h, m = now.hour, now.minute
    if h < 9 or (h == 9 and m < 30):
        return 0  # 还没开盘
    if h < 11 or (h == 11 and m <= 30):
        return (h - 9) * 60 + m - 30  # 9:30 起
    if h < 13:
        return 120  # 午休
    if h < 15:
        return 120 + (h - 13) * 60 + m
    return 240
