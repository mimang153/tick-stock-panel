"""涨跌停梯队辅助：封板状态、连板计算与响应组装。"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from app.api._root_ref import bind as _bind

_root = _bind("app.api.screener")
logger = _root.logger


def _one_word_limit_expr(status_main: str, columns: list[str]) -> Any:
    required = {"open", "high", "low", "close", "status"}
    if not required.issubset(columns):
        import polars as pl

        return pl.lit(False)

    import polars as pl

    return (
        (pl.col("status") == status_main)
        & (pl.col("close") > 0)
        & (pl.col("open") == pl.col("high"))
        & (pl.col("high") == pl.col("low"))
        & (pl.col("low") == pl.col("close"))
    ).fill_null(False)


@dataclass
class _LadderSealed:
    count_up: int
    count_down: int
    counts_up: dict
    counts_down: dict
    sealed_map: dict | None
    sealed_ready: bool
    sealed_age: float | None


def _limit_ladder_fields(is_down: bool) -> tuple[str, str, str, str, str, str]:
    if is_down:
        return (
            "signal_limit_down",
            "consecutive_limit_downs",
            "signal_limit_down_recovery",
            "limit_down",
            "recovery",
            "failed",
        )
    return (
        "signal_limit_up",
        "consecutive_limit_ups",
        "signal_broken_limit_up",
        "limit_up",
        "broken",
        "failed",
    )


def _count_sealed(m: dict, ready: bool) -> dict:
    if not m or not ready:
        return {"real": 0, "fake": 0, "pending": 0}
    real = sum(1 for v in m.values() if v.get("sealed") is True)
    fake = sum(1 for v in m.values() if v.get("sealed") is False)
    pending = sum(1 for v in m.values() if v.get("sealed") is None)
    return {"real": real, "fake": fake, "pending": pending}


def _limit_ladder_sealed(depth_svc, as_of, is_down, count_up_raw, count_down_raw) -> _LadderSealed:
    fake_up = 0
    fake_down = 0
    sealed_up_ready = False
    sealed_down_ready = False
    sealed_ready = False
    sealed_age: float | None = None
    if depth_svc:
        # 复用同一次请求已读取的 sealed map, 不重复读盘。
        up_map = depth_svc.get_sealed_map(as_of, is_down=False)
        down_map = depth_svc.get_sealed_map(as_of, is_down=True)
        sealed_up_ready = bool(up_map) and depth_svc.is_sealed_ready(as_of)
        sealed_down_ready = bool(down_map) and depth_svc.is_sealed_ready(as_of)
        if up_map:
            fake_up = sum(1 for v in up_map.values() if v.get("sealed") is False)
        if down_map:
            fake_down = sum(1 for v in down_map.values() if v.get("sealed") is False)
        sealed_map = down_map if is_down else up_map
        sealed_ready = bool(sealed_map) and depth_svc.is_sealed_ready(as_of)
        sealed_age = depth_svc.get_sealed_age(as_of) if sealed_ready else None
    count_up = count_up_raw - fake_up if sealed_up_ready else count_up_raw
    count_down = count_down_raw - fake_down if sealed_down_ready else count_down_raw
    sealed_counts_up = _count_sealed(up_map, sealed_up_ready)
    sealed_counts_down = _count_sealed(down_map, sealed_down_ready)
    return _LadderSealed(
        count_up,
        count_down,
        sealed_counts_up,
        sealed_counts_down,
        sealed_map if depth_svc else None,
        sealed_ready,
        sealed_age,
    )


def _limit_ladder_status(
    df, svc, as_of, sig_col, consec_col, broken_col, status_main, status_broken, status_failed
):
    import polars as pl

    # 窄读: 仅取前一交易日的 [symbol, consec_col] 两列, 避免全量指标重算。
    prev_consec = svc.load_prior_consecutive(as_of, consec_col)
    if not prev_consec.is_empty():
        df = df.join(prev_consec, on="symbol", how="left")
    else:
        df = df.with_columns(pl.lit(0).cast(pl.UInt32).alias("prev_consec"))

    is_limit = pl.col(sig_col).fill_null(False) if sig_col in df.columns else pl.lit(False)
    is_broken = pl.col(broken_col).fill_null(False) if broken_col in df.columns else pl.lit(False)
    consec = pl.col(consec_col).fill_null(0) if consec_col in df.columns else pl.lit(0)
    prev_c = pl.col("prev_consec").fill_null(0)

    is_failed = ~is_limit & ~is_broken & (prev_c > 0)
    df = df.with_columns(
        [
            pl.when(is_limit)
            .then(pl.lit(status_main))
            .when(is_broken)
            .then(pl.lit(status_broken))
            .when(is_failed)
            .then(pl.lit(status_failed))
            .otherwise(None)
            .alias("status"),
            pl.when(is_limit)
            .then(consec)
            .when(is_broken | is_failed)
            .then(prev_c + 1)
            .otherwise(0)
            .cast(pl.UInt32)
            .alias("boards"),
        ]
    )
    return df.filter(pl.col("status").is_not_null() & (pl.col("boards") > 0))


def _limit_ladder_sealed_overlay(df, sealed_map, status_main, status_broken):
    import polars as pl

    # sealed 叠加是独立旁路, 不改 signal_limit_up; 无明细时补空列。
    if not sealed_map:
        return df.with_columns(
            pl.lit(None).alias("sealed_status"),
            pl.lit(None).alias("sealed_vol"),
        )
    sym_sealed = {s: v.get("sealed") for s, v in sealed_map.items()}
    sym_vol = {s: v.get("vol") for s, v in sealed_map.items()}
    sealed_rows = (
        pl.DataFrame(
            {
                "symbol": list(sym_sealed.keys()),
                "_sealed": list(sym_sealed.values()),
                "_sealed_vol": list(sym_vol.values()),
            }
        )
        if sym_sealed
        else pl.DataFrame()
    )
    if sealed_rows.is_empty():
        return df.with_columns(
            pl.lit(None).alias("sealed_status"),
            pl.lit(None).alias("sealed_vol"),
        )
    df = df.join(sealed_rows, on="symbol", how="left")
    return df.with_columns(
        pl.when(
            (pl.col("status") == status_main)
            & pl.col("_sealed").is_not_null()
            & (pl.col("_sealed") == False)  # noqa: E712 - E712: 与 Polars 表达式语义对齐
        )
        .then(pl.lit(status_broken))
        .otherwise(pl.col("status"))
        .alias("status"),
        pl.when(
            (pl.col("status") == status_main) & (pl.col("_sealed") == True)  # noqa: E712 - E712: 与 Polars 表达式语义对齐
        )
        .then(pl.lit("real"))
        .when(
            pl.col("_sealed") == False  # noqa: E712 - E712: 与 Polars 表达式语义对齐
        )
        .then(pl.lit("fake"))
        .when((pl.col("status") == status_main) & pl.col("_sealed").is_null())
        .then(pl.lit("pending"))
        .otherwise(None)
        .alias("sealed_status"),
        pl.col("_sealed_vol").alias("sealed_vol"),
    ).drop(["_sealed", "_sealed_vol"])


def _dedup_ext(frame, field: str, out_col: str, at: str | None, time_field: str | None):
    """(symbol, 字段) 两列收敛为每标的一行; @时刻取该时刻或之前最近一盘。"""
    from app.services.ext_data import collapse_ext_rows_at

    if frame.is_empty() or "symbol" not in frame.columns or field not in frame.columns:
        return None
    # 收敛在 select 之前: 时间列被裁掉后 collapse 会静默回退末行
    return (
        collapse_ext_rows_at(frame, at, time_field)
        .select(["symbol", field])
        .rename({field: out_col})
    )


def _limit_ladder_ext_join(df, ext_specs, data_dir):
    from app.api.ext_data import _read_ext_dataframe
    from app.services.ext_data import ExtConfigStore

    ext_store = ExtConfigStore(data_dir)
    configs = {c.id: c for c in ext_store.load_all()}
    ext_col_names: list[str] = []
    for config_id, field_name, at in ext_specs:
        ext_col_name = f"{config_id}__{field_name}"
        try:
            # 扩展时序数据只取最新分区, 否则一只票会按历史分区数被 JOIN 放大。
            cfg = configs.get(config_id)
            if not cfg:
                continue
            ext_df, _ = _read_ext_dataframe(cfg, data_dir)
            time_field = cfg.pull.time_field if cfg.pull else None
            joined = _dedup_ext(ext_df, field_name, ext_col_name, at, time_field)
            if joined is not None:
                df = df.join(joined, on="symbol", how="left")
                ext_col_names.append(ext_col_name)
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            cfg = configs.get(config_id)
            if cfg:
                try:
                    ext_df, _ = _read_ext_dataframe(cfg, data_dir)
                    time_field = cfg.pull.time_field if cfg.pull else None
                    joined = _dedup_ext(ext_df, field_name, ext_col_name, at, time_field)
                    if joined is not None:
                        df = df.join(joined, on="symbol", how="left")
                        ext_col_names.append(ext_col_name)
                except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                    pass
    return df, ext_col_names


def _limit_ladder_tier_list(df, consec_col, status_main, status_broken, ext_col_names):
    import polars as pl

    cols = [
        "symbol",
        "name",
        "close",
        "change_pct",
        "boards",
        "status",
        consec_col,
        "sealed_status",
        "sealed_vol",
        "is_one_word",
        *ext_col_names,
    ]
    df = df.select([c for c in cols if c in df.columns])
    # 排序: boards 降序, status 按主状态→炸/翘→断/止
    status_order = pl.when(pl.col("status") == status_main).then(0)
    status_order = status_order.when(pl.col("status") == status_broken).then(1)
    status_order = status_order.otherwise(2).alias("_status_order")
    df = (
        df.with_columns(status_order)
        .sort(["boards", "_status_order"], descending=[True, False])
        .drop("_status_order")
    )
    rows = df.to_dicts()
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, float) and not math.isfinite(v):
                r[k] = None
    tiers: dict[int, list] = {}
    for r in rows:
        n = int(r.get("boards") or 0)
        tiers.setdefault(n, []).append(r)
    return [
        {"boards": n, "count": len(stocks), "stocks": stocks}
        for n, stocks in sorted(tiers.items(), key=lambda x: -x[0])
    ]


def _limit_ladder_response(as_of, tier_list, sealed, count_up_raw, count_down_raw):
    return {
        "as_of": str(as_of),
        "tiers": tier_list,
        "counts": {"up": sealed.count_up, "down": sealed.count_down},
        "counts_raw": {"up": count_up_raw, "down": count_down_raw},
        "sealed_ready": sealed.sealed_ready,
        "sealed_age": round(sealed.sealed_age, 0) if sealed.sealed_age is not None else None,
        "sealed_counts": {
            "real": sum(
                1  # type: ignore[misc]
                for t in tier_list
                for s in t.get("stocks", [])  # type: ignore[attr-defined]
                if s.get("sealed_status") == "real"
            ),
            "fake": sum(
                1  # type: ignore[misc]
                for t in tier_list
                for s in t.get("stocks", [])  # type: ignore[attr-defined]
                if s.get("sealed_status") == "fake"
            ),
            "pending": sum(
                1  # type: ignore[misc]
                for t in tier_list
                for s in t.get("stocks", [])  # type: ignore[attr-defined]
                if s.get("sealed_status") == "pending"
            ),
        },
        "sealed_counts_up": sealed.counts_up,
        "sealed_counts_down": sealed.counts_down,
    }
