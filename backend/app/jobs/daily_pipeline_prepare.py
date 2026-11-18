"""daily_pipeline_prepare: daily 拆分出的内部子模块。

所有可 patch 的名字通过 _root.<name> 延迟引用，保持 monkeypatch 可观察性。
"""

from __future__ import annotations

import app.jobs.daily_pipeline as _root


def _prune_partial_enriched_partitions(
    store,
    daily_table: str,
    enriched_table: str,
) -> list[str]:
    """删除 symbol 覆盖不完整的 enriched 日期, 返回被删的日期 (#223)。

    自选实时路径会在全市场 enriched 生成前提前创建当日分区 (只有几只自选),
    仅按日期目录计数比较会把它误判为完整分区而跳过计算, 造成日K缺失与
    均线错误。按与加工相同的停牌过滤口径检查 symbol 覆盖, 不能直接比较
    行数: 正常剔除停牌记录会让 enriched 少行, 导致每次管道都删除重算。
    删除后 run_pipeline(new_dates_only=True) 会把它们当"新日期"全市场补齐。
    daily 同日无数据 (今日日K尚未同步) 时不处理, 留给当日正常流程。
    """
    import app.jobs.daily_pipeline as _root

    pruned: list[str] = []
    daily_days = set(store.list_daily_dates(daily_table))
    if not daily_days:
        return pruned
    for day in store.list_daily_dates(enriched_table):
        if day not in daily_days:
            continue
        try:
            # 每次只读单日的停牌判定列, 不加载全历史或指标宽表。
            daily = store.read_daily(
                daily_table,
                start=day,
                end=day,
                columns=["symbol", "open", "high", "volume", "amount"],
            )
            expected = set(_root.filter_halt_days(daily)["symbol"].drop_nulls().to_list())
            actual = set(
                store.read_daily(
                    enriched_table,
                    start=day,
                    end=day,
                    columns=["symbol"],
                )["symbol"]
                .drop_nulls()
                .to_list()
            )
        except Exception as e:  # noqa: BLE001 - 单日失败不阻断整轮自愈
            _root.logger.warning("enriched 覆盖检查跳过 %s, 保留分区: %s", day, e)
            continue
        if expected - actual:
            store.delete_daily_date(enriched_table, day=day)
            pruned.append(day.isoformat())
    return pruned


def _prune_stale_price_partitions(
    store,
    daily_table: str,
    enriched_table: str,
    max_dates: int = 5,
) -> list[str]:
    """删除收盘价与官方日线不一致的 enriched 日期。

    实时 flush 写入的当日分区行数与 daily 相同, 但收盘价可能停留在收盘集合
    竞价前的快照 (实测: TickFlow 实时端点收盘后仍长期返回旧价, 3392/5554 只
    股票当日收盘价与官方日线不符), #223 的行数校验识别不到。对最近若干交易日
    做值级比对: enriched.raw_close 与 daily.close 任一标的差超过半个最小报价
    单位即删该日, 由后续增量重算按官方日线全市场重建。
    """
    import app.jobs.daily_pipeline as _root

    common = sorted(
        set(store.list_daily_dates(daily_table)) & set(store.list_daily_dates(enriched_table)),
        reverse=True,
    )[:max_dates]
    pruned: list[str] = []
    for day in common:
        try:
            daily = store.read_daily(
                daily_table,
                start=day,
                end=day,
                columns=["symbol", "close"],
            )
            enr = store.read_daily(
                enriched_table,
                start=day,
                end=day,
                columns=["symbol", "raw_close"],
            )
        except Exception:  # noqa: BLE001 - 列缺失/不可读交给完整性检查兜底
            continue
        joined = enr.join(daily, on="symbol", how="inner").drop_nulls()
        if joined.is_empty():
            continue
        bad = joined.filter((_root.pl.col("raw_close") - _root.pl.col("close")).abs() > 0.005)
        if not bad.is_empty():
            store.delete_daily_date(enriched_table, day=day)
            pruned.append(day.isoformat())
    return pruned


class PipelineStageError(RuntimeError):
    """管道有阶段软失败(数据可能陈旧)时抛出, 让上层 job_store 把任务标记为 failed。

    这些阶段单独 try/except 吞掉异常以不中断整条管道, 但一旦失败即代表对应数据陈旧。
    抛出前进度协议已走完(done/100), 故前端进度条正常收尾, 仅终态如实反映为 failed ——
    不再"部分失败却报成功"。
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("盘后管道部分阶段失败: " + "; ".join(errors))


def _noop(stage: str, pct: int, msg: str, **kwargs) -> None:
    pass


def _invalidate(table: str | None = None) -> None:
    """stage 写完调用,让 /api/data/status 只重算被影响的那张表。"""
    from app.api.data import invalidate_data_cache

    invalidate_data_cache(table)


def _resolve_universe(capset: _root.CapabilitySet, repo=None) -> list[str]:
    """解析标的池 — 本地 instruments 维表 + 自选 (单一来源, 无池兜底)。

    不再拉 CN_Equity_A 池: 其 universe 匹配曾错配到 Expired 变体、触发
    SW1 行业聚合兜底, 产出 4224 只的残缺池。维表由盘后管道 Step 0 先同步,
    这里只读 parquet; 维表缺失时仅剩自选, 宁缺毋假 (不塞演示标的),
    由上游按空池结果如实呈现。

    repo 传入时过滤自选里的指数 symbol (指数日K走独立 kline_index_* 存储,
    进股票池会污染 kline_daily/kline_minute)。ETF 刻意保留 (既有行为)。
    """
    import app.jobs.daily_pipeline as _root

    base: set[str] = set(_root.get_pool("watchlist"))
    try:
        from app.services.instrument_sync import load_instruments

        inst = load_instruments()
        base.update(inst["symbol"].to_list())
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("instruments 维表读取失败, 标的池仅剩自选 %d 只: %s", len(base), e)
    if repo is not None:
        base -= set(repo.get_index_symbol_set())
    return sorted(base)


def run_instruments_sync(repo: _root.KlineRepository) -> dict:
    """盘前同步个股维表。

    维表含当日涨跌停价 (limit_up/down), 同步完成后刷新 enriched 内存缓存,
    确保跨天后连板梯队/策略等读到的是基于最新维表的数据 (而非前一交易日残留)。
    """
    import app.jobs.daily_pipeline as _root

    rows = _root.instrument_sync.sync_instruments(repo.store.data_dir)
    _root._invalidate("instruments")
    # 维表更新后重建 enriched 缓存 (clear + refresh, 与设置页「清理并刷新」同等效果)
    if rows > 0:
        repo.clear_cache()
        repo.refresh_cache()
    return {"instruments_rows": rows}


def _daily_chunk_progress(emit):
    def _progress(cur: int, tot: int) -> None:
        emit(
            "sync_daily",
            12 + int(33 * cur / tot),
            f"日K 批次 {cur}/{tot}",
            stage_pct=int(100 * cur / tot),
            skip_log=True,
        )

    return _progress


def _run_prepare(s: _root._RunState) -> None:
    """Step 0: 同步维表 → 解析标的池 → 修复已知异常数据。"""
    import app.jobs.daily_pipeline as _root

    s.emit("sync_instruments", 2, "同步个股维表…")
    inst_rows = _root.instrument_sync.sync_instruments(s.repo.store.data_dir)
    if inst_rows > 0:
        s.emit("sync_instruments", 8, f"个股维表同步完成,{inst_rows} 只标的")
    _root._invalidate("instruments")
    s.emit("resolve_universe", 9, "解析标的池…")
    s.universe = _root._resolve_universe(s.capset, s.repo)
    s.emit("resolve_universe", 10, f"标的池规模:{len(s.universe)} 只")
    from app.services.integrity_check import repair_known_data

    repair_known_data(s.repo, s.capset, on_progress=s.emit)


def _run_scan_integrity(s: _root._RunState) -> None:
    """最近交易日完整性扫描; 溢出时降级为范围拉取, 软失败按无坏数据处理。"""
    import app.jobs.daily_pipeline as _root

    if s.override_start_date is not None:
        return
    try:
        from app.services import data_integrity

        s.integrity_issues = data_integrity.scan_recent_integrity(
            s.repo.store.data_dir, today=s.today
        )
        s.integrity_issues = [
            i for i in s.integrity_issues if i.table in data_integrity.active_daily_tables()
        ]
        if s.integrity_issues:
            s.stale_day = data_integrity.earliest_issue_day(s.integrity_issues, ("kline_daily",))
            s.etf_stale_day = data_integrity.earliest_issue_day(
                s.integrity_issues, ("kline_etf_daily",)
            )
            s.index_stale_day = data_integrity.earliest_issue_day(
                s.integrity_issues, ("kline_index_daily",)
            )
            _root.logger.warning(
                "integrity: 检测到 %d 个不完整分区(%s), 本次管道改走范围拉取修复",
                len(s.integrity_issues),
                data_integrity.describe_issues(s.integrity_issues),
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        _root.logger.warning("integrity scan failed (soft, 按无坏数据处理): %s", e)
        s.integrity_issues = []


def _run_daily_override(s: _root._RunState) -> None:
    """数据修正: 强制用传入日期作起点 batch 拉取, 忽略实时行情覆写分支。"""
    from datetime import datetime as _dt

    import app.jobs.daily_pipeline as _root

    start_date = s.override_start_date
    s.daily_range_start = start_date
    s.emit("sync_daily", 12, f"获取日K [{start_date} ~ {s.today}]…")
    _root.logger.info("sync_daily: [%s ~ %s] repair/override", start_date, s.today)
    # 带坏 symbol 清单时只拉这些标的; 单票失败也要上浮 (failed_out)。
    symbols_for_repair = list(s.override_symbols) if s.override_symbols else s.universe
    repair_failed_symbols: list[str] = []
    _root.kline_sync.sync_and_persist_daily_batch(
        symbols_for_repair,
        s.repo,
        s.capset,
        start_date=_dt.combine(start_date, _dt.min.time()),  # type: ignore[arg-type]
        end_date=_dt.combine(s.today, _dt.min.time()),
        on_chunk_done=_root._daily_chunk_progress(s.emit),
        failed_out=repair_failed_symbols,
    )
    if s.override_symbols:
        s.emit(
            "sync_daily",
            45,
            f"日K 完成(定向修复 {len(symbols_for_repair)} 只, 失败 {len(repair_failed_symbols)} 只)",
        )
    else:
        gap_days = (s.today - start_date).days
        s.new_daily_days = gap_days
        s.emit("sync_daily", 45, f"日K 完成,覆盖 {gap_days} 天")
    _root.logger.info(
        "sync_daily: [%s ~ %s] done, symbols=%d(%s) failed=%d",
        start_date,
        s.today,
        len(symbols_for_repair),
        "targeted" if s.override_symbols else "universe",
        len(repair_failed_symbols),
    )
    if repair_failed_symbols:
        preview = ", ".join(repair_failed_symbols[:10])
        more = "" if len(repair_failed_symbols) <= 10 else f" 等 {len(repair_failed_symbols)} 只"
        s.stage_errors.append(
            f"日K修复有 {len(repair_failed_symbols)} 只标的拉取失败: {preview}{more}"
        )
