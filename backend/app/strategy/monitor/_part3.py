"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import contextlib
import math
from typing import Any

import polars as pl

from app.strategy import config as _strategy_config

from ._common import _build_condition_mask, logger


class _MonitorRuleEngineMixin3:
    # 以下成员由 MonitorRuleEngine 的其它分片/初始化提供（分包后 mypy 不可见）。
    _name_map: Any
    _strategy_engine: Any
    _data_dir: Any
    _last_fire: Any
    _strategy_pools: Any
    _strategy_signal_state: Any
    _strategy_signal_seen: Any
    _prepare_strategy_context: Any  # Mixin2 提供
    _run_strategy: Any  # Mixin2 提供
    _cache_strategy_result: Any  # Mixin2 提供

    def _strategy_eligible_symbols(
        self,
        result: Any,
        score_min: Any,
        score_max: Any,
        score_filter_enabled: bool,
    ) -> set[str]:
        eligible_symbols: set[str] = set()
        if score_filter_enabled:
            for row in result.rows:
                symbol = str(row.get("symbol", ""))
                score = row.get("score", result.scores.get(symbol))
                if isinstance(score, bool) or not isinstance(score, int | float):
                    continue
                if not math.isfinite(score):
                    continue
                if score_min is not None and score < score_min:
                    continue
                if score_max is not None and score > score_max:
                    continue
                eligible_symbols.add(symbol)
        else:
            eligible_symbols = {str(row["symbol"]) for row in result.rows}
        return eligible_symbols

    def _strategy_event_changes(
        self,
        pool_key: tuple[str, str, str],
        current_pool: set[str],
        prev_pool: set[str] | None,
        result: Any,
        eligible_symbols: set[str],
        score_filter_enabled: bool,
    ) -> tuple[dict[str, set[str]], list[dict]]:
        changes, entry_signal_hits = self._strategy_event_changes(
            pool_key,
            current_pool,
            prev_pool,
            result,
            eligible_symbols,
            score_filter_enabled,
        )
        return changes, entry_signal_hits

    def _build_strategy_events(
        self,
        changes: dict[str, set[str]],
        notify_events: set[str],
        sname: str,
        row_map: dict[str, dict],
        entry_signal_hits: list[dict],
        exit_signal_hits: list[dict],
    ) -> list[tuple[str, str, Any, Any, Any, list[str]]]:
        results: list[tuple[str, str, Any, Any, Any, list[str]]] = []
        signal_map = {
            "buy_signal": {
                str(hit["symbol"]): list(hit.get("signals") or []) for hit in entry_signal_hits
            },
            "sell_signal": {
                str(hit["symbol"]): list(hit.get("signals") or []) for hit in exit_signal_hits
            },
        }
        action_labels = {
            "buy_signal": "买入信号",
            "sell_signal": "卖出信号",
            "pool_entry": "进入策略结果",
            "pool_exit": "移出策略结果",
        }
        for event_type, symbols in changes.items():
            if event_type not in notify_events or not symbols:
                continue
            symbol_list = sorted(symbols)
            if len(symbol_list) > 5:
                names = [
                    str(row_map.get(symbol, {}).get("name") or self._name_map.get(symbol, symbol))
                    for symbol in symbol_list
                ]
                message = (
                    f"策略「{sname}」{action_labels[event_type]} {len(symbol_list)} 只: "
                    f"{'、'.join(names)}"
                )
                hit_signals = sorted(
                    {
                        signal
                        for symbol in symbol_list
                        for signal in signal_map.get(event_type, {}).get(symbol, [])
                    }
                )
                results.append((event_type, "_batch", message, None, None, hit_signals))
                continue
            for symbol in symbol_list:
                row = row_map.get(symbol, {})
                name = row.get("name") or self._name_map.get(symbol, symbol)
                results.append(
                    (
                        event_type,
                        symbol,
                        name,
                        row.get("close"),
                        row.get("change_pct"),
                        signal_map.get(event_type, {}).get(symbol, []),
                    )
                )
        return results

    def _strategy_eligible(self, rule: dict, result: Any) -> set[str]:
        score_min = rule.get("score_min")
        score_max = rule.get("score_max")
        return self._strategy_eligible_symbols(
            result,
            score_min,
            score_max,
            score_min is not None or score_max is not None,
        )

    def _strategy_row_map(self, df: pl.DataFrame, result: Any) -> dict[str, dict]:
        row_map: dict[str, dict] = {r["symbol"]: r for r in result.rows}
        try:
            for row in df.iter_rows(named=True):
                row_map.setdefault(str(row.get("symbol", "")), row)
        except Exception:  # noqa: BLE001
            pass
        return row_map

    def _strategy_changes(
        self,
        pool_key: tuple,
        prev_pool: set[str] | None,
        current_pool: set[str],
        result: Any,
        entry_signal_hits: list[dict],
    ) -> dict[str, set[str]]:
        return {
            "buy_signal": self._new_strategy_signals(
                pool_key, "buy_signal", result.as_of, entry_signal_hits
            ),
            "sell_signal": self._new_strategy_signals(
                pool_key, "sell_signal", result.as_of, result.exit_signal_hits
            ),
            "pool_entry": set() if prev_pool is None else current_pool - prev_pool,
            "pool_exit": set() if prev_pool is None else prev_pool - current_pool,
        }

    def _match_strategy(
        self,
        df: pl.DataFrame,
        rule: dict,
    ) -> list[tuple[str, str, Any, Any, Any, list[str]]]:
        """策略类型评估: 一次执行同时产出交易信号和结果池变更事件。

        返回 [(event_type, symbol, name, price, pct, signals)]
        event_type: buy_signal | sell_signal | pool_entry | pool_exit
        同类事件超过 5 只时合并为一条批量事件 (symbol="_batch")
        """
        if self._strategy_engine is None:
            return []
        sid = rule.get("strategy_id")
        if not sid:
            return []
        at = rule.get("asset_type", "stock")
        pool_key = (str(rule.get("id", sid)), sid, at)
        try:
            s = self._strategy_engine.get(sid)
        except Exception:  # noqa: BLE001 - 策略不存在时跳过该规则并留debug线索
            # 规则引用了不存在的策略(内置策略已下线): 静默空转最容易骗人,
            # 至少留一条 debug 线索, 避免"看起来在监控其实什么都没跑" (#490)
            logger.debug("monitor: 策略 %s 不存在, 规则 %s 本轮跳过", sid, rule.get("id"))
            return []
        if s is None:
            return []

        # 运行策略: 复用当前 enriched DataFrame 跳过数据加载
        overrides = {}
        if self._data_dir:
            with contextlib.suppress(Exception):
                overrides = _strategy_config.load_override(self._data_dir, sid)

        current_context = self._prepare_strategy_context(
            s,
            sid,
            at,
            df,
            rule,
            overrides,
        )
        if current_context is None:
            return []
        strategy_ok, result = self._run_strategy(sid, s, df, current_context, overrides)
        if not strategy_ok:
            return []

        self._cache_strategy_result(sid, at, result)

        score_filter_enabled = (
            rule.get("score_min") is not None or rule.get("score_max") is not None
        )
        eligible_symbols = self._strategy_eligible(rule, result)

        current_pool = eligible_symbols
        prev_pool = self._strategy_pools.get(pool_key)
        self._strategy_pools[pool_key] = current_pool

        notify_events = set(rule.get("notify_events") or ("pool_entry", "pool_exit"))
        sname = s.meta.get("name", "") or s.meta.get("id", sid)
        row_map = self._strategy_row_map(df, result)

        entry_signal_hits = result.entry_signal_hits
        if score_filter_enabled:
            entry_signal_hits = [
                hit for hit in entry_signal_hits if str(hit.get("symbol", "")) in eligible_symbols
            ]
        changes = self._strategy_changes(
            pool_key, prev_pool, current_pool, result, entry_signal_hits
        )

        return self._build_strategy_events(
            changes,
            notify_events,
            sname,
            row_map,
            entry_signal_hits,
            result.exit_signal_hits,
        )

    def _new_strategy_signals(
        self,
        pool_key: tuple[str, str, str],
        event_type: str,
        as_of: Any,
        hits: list[dict],
    ) -> set[str]:
        rule_id, strategy_id, asset_type = pool_key
        state_key = (rule_id, strategy_id, asset_type, event_type)
        date_key = str(as_of)
        current = {str(hit["symbol"]) for hit in hits}
        previous = self._strategy_signal_state.get(state_key)
        self._strategy_signal_state[state_key] = (date_key, current)

        if previous is None:
            for symbol in current:
                self._strategy_signal_seen[(*state_key, symbol)] = date_key
            return set()

        previous_date, previous_symbols = previous
        candidates = current if previous_date != date_key else current - previous_symbols
        fresh = {
            symbol
            for symbol in candidates
            if self._strategy_signal_seen.get((*state_key, symbol)) != date_key
        }
        for symbol in fresh:
            self._strategy_signal_seen[(*state_key, symbol)] = date_key
        return fresh

    @staticmethod
    def _match_conditions(
        df: pl.DataFrame,
        rule: dict,
    ) -> list[tuple[str, Any, Any, Any, list[str]]]:
        """按 conditions + logic 匹配,返回命中行 [(symbol,name,price,pct,signals)]。"""
        conditions = rule.get("conditions", [])
        logic = rule.get("logic", "and")
        if not conditions:
            return []
        hit_df = _build_condition_mask(df, conditions, logic)
        results = []
        for row in hit_df.iter_rows(named=True):
            sym = row.get("symbol", "")
            name = row.get("name")
            price = row.get("close")
            pct = row.get("change_pct")
            # 收集命中的信号列名 (仅 op=truth 且为真的)
            hit_sigs = [
                c["field"] for c in conditions if c.get("op") == "truth" and row.get(c["field"])
            ]
            results.append((sym, name, price, pct, hit_sigs))
        return results

    @staticmethod
    def _volume_delta_basic_mask(
        df: pl.DataFrame, bf: dict, name_map: dict[str, str]
    ) -> pl.Expr | None:
        """轮询放量基础过滤掩码 (与策略 basic_filter 语义对齐, 字段缺失时该项跳过)。

        支持: price_min/max (收盘价), market_cap_min (总市值=close x total_shares),
        float_cap_min/max (流通市值), amount_min (当日累计成交额), exclude_st (名称含 ST)。
        """
        masks: list[pl.Expr] = []
        if bf.get("price_min") is not None:
            masks.append(pl.col("close") >= float(bf["price_min"]))
        if bf.get("price_max") is not None:
            masks.append(pl.col("close") <= float(bf["price_max"]))
        if bf.get("amount_min") is not None and "amount" in df.columns:
            masks.append(pl.col("amount") >= float(bf["amount_min"]))
        if bf.get("market_cap_min") is not None and "total_shares" in df.columns:
            masks.append((pl.col("close") * pl.col("total_shares")) >= float(bf["market_cap_min"]))
        if bf.get("float_cap_min") is not None and "float_shares" in df.columns:
            masks.append((pl.col("close") * pl.col("float_shares")) >= float(bf["float_cap_min"]))
        if bf.get("float_cap_max") is not None and "float_shares" in df.columns:
            masks.append((pl.col("close") * pl.col("float_shares")) <= float(bf["float_cap_max"]))
        if bf.get("exclude_st") and name_map:
            st_symbols = [
                sym for sym, name in name_map.items() if name and "ST" in str(name).upper()
            ]
            if st_symbols:
                masks.append(~pl.col("symbol").is_in(st_symbols))
        if not masks:
            return None
        return pl.all_horizontal(masks)

    def _volume_delta_events(
        self,
        hit_rows: list[dict],
        rule: dict,
        now: float,
        cmp_col: str,
        metric: str,
        th_text: str,
        span_text: str,
        span_s: float,
        cooldown: float,
        severity: str,
    ) -> list[dict]:
        """根据已排序的命中行构造轮询放量事件并处理冷却。"""

        def _name_of(row: dict) -> str:
            sym = row.get("symbol", "")
            return row.get("name") or self._name_map.get(sym) or sym

        def _fmt(v) -> str:
            if metric == "amount":
                return f"{v / 1e4:,.0f} 万元"
            return f"{v:,.0f} 手"

        def _event(symbol, name, message, *, delta=None, price=None, pct=None) -> dict:
            ev = {
                "ts": int(now * 1000),
                "rule_id": rule["id"],
                "rule_name": rule.get("name", ""),
                "source": "volume_delta",
                "type": "轮询放量",
                "symbol": symbol,
                "name": name,
                "message": message,
                "price": price,
                "change_pct": pct,
                "signals": [],
                "severity": severity,
                "conditions": [],
                "logic": "and",
                "volume_delta": delta,
                "volume_delta_span": round(span_s, 1),
            }
            if metric == "amount":
                ev["volume_delta_amount"] = delta
            return ev

        if len(hit_rows) > 5:
            top = "、".join(_name_of(r) for r in hit_rows[:8])
            suffix = "等" if len(hit_rows) > 8 else ""
            message = (
                f"放量 · 单轮增量 >= {th_text}{span_text} · 共 {len(hit_rows)} 只: {top}{suffix}"
            )
            key = (rule["id"], "_volume_delta_batch", "volume_delta")
            last = self._last_fire.get(key)
            if last is not None and (now - last) < cooldown:
                return []
            self._last_fire[key] = now
            return [_event("", "", message)]

        events: list[dict] = []
        for row in hit_rows:
            sym = row.get("symbol", "")
            key = (rule["id"], sym, "volume_delta")
            last = self._last_fire.get(key)
            if last is not None and (now - last) < cooldown:
                continue
            self._last_fire[key] = now
            delta = row.get(cmp_col)
            message = f"放量 · 单轮增量 {_fmt(delta)} >= {th_text}{span_text}"
            price = row.get("close")
            pct = row.get("change_pct")
            event = _event(sym, _name_of(row), message, delta=delta, price=price, pct=pct)
            events.append(event)
        return events

    def _evaluate_volume_delta(self, scoped: pl.DataFrame, rule: dict, now: float) -> list[dict]:
        """评估轮询放量监控: 相邻两次全市场快照的成交量/成交额差值。

        差值列 _volume_delta(手)/_volume_delta_amount(元)/间隔列 _volume_delta_span
        由 quote_service 评估前注入。metric=volume 按手数、amount 按金额比较阈值;
        basic_filter 先行过滤 (股价/市值/成交额/ST, 与策略 basic_filter 语义对齐)。
        命中 >5 只时合并为一条批量事件防刷屏。
        """
        if "_volume_delta" not in scoped.columns:
            return []  # 无差值数据 (首轮/开盘保护/非全市场轮询), 安全降级

        metric = rule.get("metric", "volume")
        if metric == "amount" and "_volume_delta_amount" in scoped.columns:
            cmp_col, threshold = "_volume_delta_amount", rule.get("threshold_amount", 1e6)
            th_text = f"{threshold / 1e4:,.0f} 万元"
        else:
            cmp_col, threshold = "_volume_delta", rule.get("threshold_volume", 9000)
            th_text = f"{threshold:,.0f} 手"

        cooldown = rule.get("cooldown_seconds", 300)
        severity = rule.get("severity", "warn")
        span_s = 0.0
        if "_volume_delta_span" in scoped.columns and scoped.height > 0:
            v = scoped["_volume_delta_span"][0]
            span_s = float(v) if v is not None else 0.0
        span_text = f" (间隔 {span_s:.0f}s)" if span_s > 0 else ""

        candidate = scoped
        bf = rule.get("basic_filter") or {}
        if bf:
            mask = self._volume_delta_basic_mask(candidate, bf, self._name_map)
            if mask is not None:
                candidate = candidate.filter(mask)

        hit = candidate.filter(pl.col(cmp_col).is_not_null() & (pl.col(cmp_col) >= threshold)).sort(
            cmp_col, descending=True
        )
        if hit.is_empty():
            return []
        hit_rows = list(hit.iter_rows(named=True))

        return self._volume_delta_events(
            hit_rows,
            rule,
            now,
            cmp_col,
            metric,
            th_text,
            span_text,
            span_s,
            cooldown,
            severity,
        )

    def _ladder_sealed_display(
        self,
        sealed_vol,
        price,
        metric: str,
        threshold,
        unit: str,
        warn_label: str,
    ):
        """计算封单展示值及预警文案。"""
        sealed_value = sealed_vol * 100 * (price or 0) if metric == "sealed_amount" else sealed_vol
        if metric == "sealed_amount":
            sv_text = f"{sealed_value / 1e4:.0f}万{unit}"
            th_text = f"{threshold / 1e4:.0f}万{unit}"
        else:
            sv_text = f"{sealed_value:,.0f} {unit}"
            th_text = f"{threshold:,.0f} {unit}"
        return sealed_value, f"{warn_label} · 封单 {sv_text} ≤ {th_text}"

    def _evaluate_ladder(self, scoped: pl.DataFrame, rule: dict, now: float) -> list[dict]:
        """评估连板梯队封单监控规则。

        封单量从注入的临时列 _sealed_vol (手) 读取 (由 quote_service 评估前注入)。
        命中条件: 封单比较值 <= threshold (且封单 > 0, 排除无 depth 数据的股票)。
        涨停(direction=up) → 炸板预警; 跌停(direction=down) → 翘板预警。
        """
        if "_sealed_vol" not in scoped.columns:
            return []  # 无封单数据 (depth 未拉取), 安全降级

        metric = rule.get("metric", "sealed_vol")
        threshold = rule.get("threshold", 0)
        direction = rule.get("direction", "up")
        cooldown = rule.get("cooldown_seconds", 600)
        severity = rule.get("severity", "warn")

        # 比较值: sealed_vol 直接用 (手), sealed_amount = 手 × 100股 × close
        if metric == "sealed_amount":
            cmp_expr = pl.col("_sealed_vol") * 100 * pl.col("close")
            unit = "元"
        else:
            cmp_expr = pl.col("_sealed_vol")
            unit = "手"

        # 命中: 封单 > 0 (有数据) 且 比较值 <= 阈值
        hit = scoped.filter(
            pl.col("_sealed_vol").is_not_null()
            & (pl.col("_sealed_vol") > 0)
            & (cmp_expr <= threshold)
        )
        if hit.is_empty():
            return []

        warn_label = "炸板预警" if direction == "up" else "翘板预警"
        events: list[dict] = []
        for row in hit.iter_rows(named=True):
            sym = row.get("symbol", "")
            key = (rule["id"], sym, "ladder")
            last = self._last_fire.get(key)
            if last is not None and (now - last) < cooldown:
                continue
            self._last_fire[key] = now

            name = row.get("name") or self._name_map.get(sym) or sym
            price = row.get("close")
            pct = row.get("change_pct")
            sealed_vol = row.get("_sealed_vol")
            sealed_value, message = self._ladder_sealed_display(
                sealed_vol, price, metric, threshold, unit, warn_label
            )

            events.append(
                {
                    "ts": int(now * 1000),
                    "rule_id": rule["id"],
                    "rule_name": rule.get("name", ""),
                    "source": "ladder",
                    "type": warn_label,
                    "symbol": sym,
                    "name": name,
                    "message": message,
                    "price": price,
                    "change_pct": pct,
                    "signals": [],
                    "severity": severity,
                    "conditions": [],
                    "logic": "and",
                    "sealed_value": sealed_value,  # 预警封单量/额 (飞书+记录展示)
                    "sealed_metric": metric,
                }
            )
        return events
