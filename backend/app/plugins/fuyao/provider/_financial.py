"""FuyaoProvider 财务 mixin: 三表多期 + 指标单期 + bps 反推及非路由能力。"""

from __future__ import annotations

import sys
import time
from typing import Any, ClassVar

import polars as pl

from app.plugins.fuyao.client import FuyaoError

from ._common import (
    _FINANCIAL_HISTORY_PERIODS,
    _STATEMENT_ENDPOINTS,
    _VALUATION_BATCH,
    _date_of_ms,
    _iso_of_ms,
    _report_quarter,
    _to_float,
    logger,
)

_fp = sys.modules[__package__]


class FuyaoProviderFinancial:
    """财务: 字段映射 TickFlow canonical, 扶摇独有字段原名透传为扩展列。"""

    # 客户端工厂由 FuyaoProviderBase 提供（分包后 mypy 不可见）。
    _get_client: Any

    # 字段映射: 扶摇原始字段 → 项目 canonical 列名(TickFlow 口径, 前端财务页与回测
    # FUNDAMENTAL_FACTORS 按此消费)。映射表之外的扶摇独有字段以原名透传为扩展列。
    _INCOME_FIELD_MAP: ClassVar[dict[str, str]] = {
        "operating_income": "revenue",
        "operating_costs": "operating_cost",
        "sales_fee": "selling_expense",
        "manage_fee": "admin_expense",
        "research_and_development_expenses": "rd_expense",
        "operating_profit": "operating_profit",
        "interest_expenses": "financial_expense",  # 近似口径: 扶摇只给利息费用
        "profit_total": "total_profit",
        "income_tax_expense": "income_tax",
        "net_profit": "net_income",
        "parent_holder_net_profit": "net_income_attributable",
        "basic_eps": "basic_eps",
    }
    _BALANCE_FIELD_MAP: ClassVar[dict[str, str]] = {
        "assets_total": "total_assets",
        "total_current_assets": "total_current_assets",
        "non_current_nets_total": "total_non_current_assets",
        "cash": "cash_and_equivalents",
        "accounts_receivable": "accounts_receivable",
        "total_debt": "total_liabilities",
        "holder_equity_total": "total_equity",
    }
    _CASHFLOW_FIELD_MAP: ClassVar[dict[str, str]] = {
        "act_cash_flow_net": "net_operating_cash_flow",
        "invest_cash_flow_net": "net_investing_cash_flow",
        "financing_cash_flow_net": "net_financing_cash_flow",
        "pay_fixed_assets_etc_cash": "capex",
        "cash_equivalents_net_addition": "net_cash_change",
    }
    # 官方指标 index_id → canonical。归母净利同比近似 tickflow net_income_yoy;
    # 实测 index_id 与文档有出入(calculate_ 前缀等), 以实测为准。
    _METRICS_FIELD_MAP: ClassVar[dict[str, str]] = {
        "index_weighted_avg_roe": "roe",
        "total_assets_net_ratio": "roa",
        "sale_gross_margin": "gross_margin",
        "sale_net_interest_ratio": "net_margin",
        "assets_debt_ratio": "debt_to_asset_ratio",
        "calculate_operating_income_yoy_growth_ratio": "revenue_yoy",
        "calculate_parent_holder_net_profit_yoy_growth_ratio": "net_income_yoy",
        "operating_cash_flow_net_divide_income": "operating_cash_to_revenue",
        "inventory_turnover_ratio": "inventory_turnover",
    }

    def get_financials(
        self,
        table: str,
        symbols: list[str],
        latest_only: bool = True,
    ) -> pl.DataFrame:
        """拉取财务数据, 映射为 canonical 列(symbol/period_end/announce_date/指标)。

        - 三大报表: 单股单请求, latest_only 决定最近 1 期还是 8 期季报;
        - metrics: 指标接口为单股单期, 恒只拉最新一期(bps 由估值快照 pb_mrq 反推,
          eps_basic 顺带取自利润表); 历史各期建议切回 TickFlow 同步补齐 —
          报告期合并写入会让两源数据共存, 互不覆盖;
        - shares: 扶摇无股本接口, 恒返回空(已有存量靠合并写入保留)。
        """
        if table == "shares":
            logger.info("扶摇无股本接口, shares 表跳过 (已有数据保留)")
            return pl.DataFrame()
        if table in _STATEMENT_ENDPOINTS:
            field_map = {
                "income": self._INCOME_FIELD_MAP,
                "balance_sheet": self._BALANCE_FIELD_MAP,
                "cash_flow": self._CASHFLOW_FIELD_MAP,
            }[table]
            return self._financial_statements(table, field_map, symbols, latest_only)
        if table == "metrics":
            return self._financial_metrics(symbols)
        return pl.DataFrame()

    def _financial_statements(
        self,
        stmt: str,
        field_map: dict[str, str],
        symbols: list[str],
        latest_only: bool,
    ) -> pl.DataFrame:
        client = self._get_client()
        limit = 1 if latest_only else _FINANCIAL_HISTORY_PERIODS
        rows_out: list[dict] = []
        for i, sym in enumerate(symbols):
            if i:
                time.sleep(_fp._HIST_INTERVAL_S)
            try:
                rows = client.financial_statements(stmt, sym, limit=limit)
            except FuyaoError as e:
                logger.warning("扶摇财务 %s %s 失败: %s", stmt, sym, e)
                continue
            for r in rows:
                row: dict = {
                    "symbol": sym,
                    "period_end": _iso_of_ms(r.get("period_end_ms")),
                    "announce_date": _iso_of_ms(r.get("report_date_ms")),
                }
                for src, dst in field_map.items():
                    row[dst] = _to_float(r.get(src))
                # 扶摇独有字段以原名透传为扩展列 (canonical 之外的增量信息)
                for src, value in r.items():
                    if src not in field_map and src not in row and isinstance(value, int | float):
                        row[src] = value
                rows_out.append(row)
        # 稀疏列(如仅银行有值的 financial_expense)前 100 行可能全空, 默认采样会
        # 推断成 Null 列, 后续数值 append 失败 → 全量扫描推断
        return pl.DataFrame(rows_out, infer_schema_length=None) if rows_out else pl.DataFrame()

    def _apply_indicator_values(self, client, sym: str, report: str, row: dict) -> None:
        """把单期财务指标按 canonical 列名写入 row；失败仅告警降级。"""
        try:
            abilities = client.financial_indicators(sym, report)
        except FuyaoError as e:
            logger.warning("扶摇指标 %s %s 失败: %s", sym, report, e)
            return
        for ability in abilities:
            for ind in ability.get("indicators") or []:
                index_id = ind.get("index_id")
                if not index_id:
                    continue
                value = _to_float(ind.get("value"))
                if value is not None:
                    row[self._METRICS_FIELD_MAP.get(index_id, index_id)] = value

    def _financial_metrics(self, symbols: list[str]) -> pl.DataFrame:
        client = self._get_client()
        # 指标接口按 report(yyyy-N) 单期查询 → 先用利润表 limit=1 反查每股最新披露期
        latest: dict[str, dict] = {}
        for i, sym in enumerate(symbols):
            if i:
                time.sleep(_fp._HIST_INTERVAL_S)
            try:
                rows = client.financial_statements("income", sym, limit=1)
            except FuyaoError as e:
                logger.warning("扶摇财务 income %s 失败: %s", sym, e)
                continue
            if rows:
                latest[sym] = rows[0]
        if not latest:
            return pl.DataFrame()
        bps_by_sym = self._derive_bps(sorted(latest))
        rows_out: list[dict] = []
        for sym, r in latest.items():
            quarter = _report_quarter(r.get("fiscal_period"))
            report = f"{r.get('fiscal_year')}-{quarter}" if quarter else None
            row: dict = {
                "symbol": sym,
                "period_end": _iso_of_ms(r.get("period_end_ms")),
                "announce_date": _iso_of_ms(r.get("report_date_ms")),
                "eps_basic": _to_float(r.get("basic_eps")),
                "bps": bps_by_sym.get(sym),
            }
            if report:
                self._apply_indicator_values(client, sym, report, row)
            rows_out.append(row)
        # 稀疏列(仅个别行业有值的指标)前 100 行可能不存在或全空, 默认采样会
        # 静默丢列 / 推断成 Null 后 append 失败 → 全量扫描推断 (与 _financial_
        # statements 同修, #458)
        return pl.DataFrame(rows_out, infer_schema_length=None) if rows_out else pl.DataFrame()

    def trading_days(self) -> set:
        """近一年交易日集合 (供交易日探针)。失败抛 FuyaoError, 由探针兜为未知。"""
        rows = self._get_client().trading_days()
        return {d for d in (_date_of_ms(r.get("date_ms")) for r in rows) if d is not None}

    def dragon_tiger(self, board_type: str = "all", date: str | None = None) -> dict:
        """龙虎榜单榜 (复盘页卡片 + AI 复盘上下文)。返回原始 data 容器。

        非路由数据集 (tickflow 无对应能力), 不进 plugin.yaml datasets,
        由 services.dragon_tiger 统一做三榜聚合/缓存/交易日回退。
        """
        return self._get_client().dragon_tiger_list(board_type, date)

    def short_term_benchmark(self, date: str | None = None) -> dict:
        """短线风向标竞价基准 (复盘页卡片 + AI 复盘上下文)。返回原始 data 容器。

        非路由数据集 (tickflow 无对应能力), 不进 plugin.yaml datasets,
        由 services.auction_benchmark 统一做按日缓存/收益enrich/交易日回退。
        """
        return self._get_client().short_term_benchmark(date)

    def _derive_bps(self, symbols: list[str]) -> dict[str, float]:
        """估值快照 pb_mrq 与行情快照最新价同源同刻 → bps = price / pb_mrq。

        与财报口径 bps 可能差几个百分点(上游权益基准不完全透明), 用于补齐
        metrics.bps 使回测 pb_latest 因子可用。接口失败只影响 bps 列, 不致命。
        """
        client = self._get_client()
        pb: dict[str, float] = {}
        price: dict[str, float] = {}
        for i in range(0, len(symbols), _VALUATION_BATCH):
            if i:
                time.sleep(_fp._HIST_INTERVAL_S)
            batch = symbols[i : i + _VALUATION_BATCH]
            for fetch, store in (
                (client.valuations_snapshot, pb),
                (client.price_snapshot_batch, price),
            ):
                time.sleep(_fp._HIST_INTERVAL_S)
                try:
                    for r in fetch(batch):
                        value = _to_float(r.get("pb_mrq" if store is pb else "last_price"))
                        code = r.get("thscode")
                        if code and value is not None and value != 0:
                            store[code] = value
                except FuyaoError as e:
                    logger.warning("扶摇 bps 推导快照失败(%d 只): %s", len(batch), e)
        return {
            sym: price[sym] / pb[sym] for sym in symbols if sym in pb and pb[sym] and sym in price
        }
