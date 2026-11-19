"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import math
import time
from typing import Any, cast

import polars as pl

import app.strategy.monitor as _monitor_pkg

from ._common import _group_members_or_none, logger

_GROUP_MISSING = object()  # 动态分组已删除: 本轮整体跳过（区别于「无 scope 过滤」）


def _scope_symbols_for(rule: dict):
    """解析异动规则的标的作用域；返回 None=不过滤, _GROUP_MISSING=分组已删除。"""
    if rule.get("scope") == "symbols":
        return {str(s) for s in rule.get("symbols", []) if s}
    if rule.get("scope") == "watchlist_group":
        members = _group_members_or_none(rule)
        return set(members) if members is not None else _GROUP_MISSING
    return None


def _best_abnormal_window(row: dict, window_filter: str, direction: str):
    """方向/窗口过滤后取接近度最高的窗口 (窗口, 接近度, 偏离值, 阈值)。"""
    best = None
    for key, win in (row.get("windows") or {}).items():
        if window_filter != "any" and key != window_filter:
            continue
        value = win.get("value")
        if value is None:
            continue
        if direction == "up" and value <= 0:
            continue
        if direction == "down" and value >= 0:
            continue
        closeness = float(win.get("closeness") or 0)
        if best is None or closeness > best[1]:
            best = (key, closeness, float(value), float(win.get("threshold") or 0))
    return best


class _MonitorRuleEngineMixin2:
    # 以下成员由 MonitorRuleEngine 的其它分片/初始化提供（分包后 mypy 不可见）。
    _sector_condition_state: Any
    _abnormal_condition_state: Any
    _last_fire: Any
    _alert_handler: Any
    _rules: Any
    _name_map: Any
    _default_message: Any  # Mixin4 提供
    _active_matrix_snapshots: Any
    _strategy_engine: Any
    _building_strategy_results: Any
    _latest_strategy_result_ids: Any
    _match_strategy: Any  # Mixin3 提供
    _match_conditions: Any  # Mixin3 提供
    _evaluate_ladder: Any  # Mixin3 提供
    _evaluate_volume_delta: Any  # Mixin3 提供
    _history_loader_for: Any  # Mixin1 提供

    def _evaluate_sector_rule(
        self, rule: dict, snapshots: dict[str, dict], now: float
    ) -> list[dict]:
        events: list[dict] = []
        direction = rule.get("direction", "up")
        trigger = rule.get("sector_trigger", "change_pct")
        threshold = float(rule.get("threshold_pct", 1.0)) / 100
        window = int(rule.get("window_minutes", 5))

        for target in rule.get("sector_targets", []):
            target_key = str(target.get("key") or "")
            snapshot = snapshots.get(target_key)
            if not snapshot or not snapshot.get("valid"):
                continue
            value = (
                snapshot.get("change_pct")
                if trigger == "change_pct"
                else snapshot.get("window_changes", {}).get(window)
            )
            condition = value is not None and (
                value >= threshold if direction == "up" else value <= -threshold
            )
            state_key = (rule["id"], target_key)
            previous = self._sector_condition_state.get(state_key)
            self._sector_condition_state[state_key] = condition
            if previous is None or previous or not condition:
                continue

            event_type = f"sector_{trigger}_{direction}"
            cooldown_key = (rule["id"], target_key, event_type)
            last = self._last_fire.get(cooldown_key)
            cooldown = int(rule.get("cooldown_seconds", 3600))
            if last is not None and now - last < cooldown:
                continue
            self._last_fire[cooldown_key] = now
            message = rule.get("message", "") or self._sector_message(
                snapshot,
                trigger,
                direction,
                threshold,
                window,
                value,
            )
            event = {
                "ts": int(now * 1000),
                "rule_id": rule["id"],
                "rule_name": rule.get("name", ""),
                "strategy_id": None,
                "source": "sector",
                "type": event_type,
                "symbol": snapshot.get("symbol") if snapshot.get("kind") == "index" else "",
                "name": snapshot.get("name"),
                "message": message,
                "price": snapshot.get("price"),
                "change_pct": snapshot.get("change_pct"),
                "window_change_pct": value if trigger == "momentum" else None,
                "signals": [],
                "severity": rule.get("severity", "info"),
                "conditions": [],
                "logic": "and",
                "sector_kind": snapshot.get("kind"),
                "sector_key": target_key,
                "sector_name": snapshot.get("name"),
                "sector_source_field": snapshot.get("source_field"),
                "sector_value": snapshot.get("value"),
                "sector_level": snapshot.get("level"),
                "coverage_ratio": snapshot.get("coverage_ratio"),
                "valid_count": snapshot.get("valid_count"),
                "total_count": snapshot.get("total_count"),
                "up_count": snapshot.get("up_count"),
                "down_count": snapshot.get("down_count"),
                "leader": snapshot.get("leader"),
            }
            events.append(event)
            if self._alert_handler:
                try:
                    self._alert_handler(event)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("alert handler failed: %s", exc)
        return events

    @staticmethod
    def _sector_message(
        snapshot: dict,
        trigger: str,
        direction: str,
        threshold: float,
        window: int,
        value: float | None,
    ) -> str:
        kind_label = {
            "index": "指数",
            "concept": "概念",
            "industry": "行业",
        }.get(cast(str, snapshot.get("kind")), "板块")
        current = float(snapshot.get("change_pct") or 0)
        if trigger == "momentum":
            action = "快速拉升" if direction == "up" else "快速下跌"
            head = (
                f"{kind_label}「{snapshot.get('name')}」{window}分钟{action} "
                f"{float(value or 0) * 100:+.2f}%"
            )
        else:
            action = "涨幅上穿" if direction == "up" else "跌幅下穿"
            head = f"{kind_label}「{snapshot.get('name')}」{action} {threshold * 100:.2f}%"
        parts = [head, f"当前 {current * 100:+.2f}%"]
        if snapshot.get("kind") != "index":
            parts.append(f"上涨 {snapshot.get('up_count', 0)}/{snapshot.get('valid_count', 0)}")
            parts.append(f"覆盖 {float(snapshot.get('coverage_ratio') or 0) * 100:.0f}%")
            leader = snapshot.get("leader") or {}
            if leader.get("name") or leader.get("symbol"):
                parts.append(
                    f"领涨 {leader.get('name') or leader.get('symbol')} "
                    f"{float(leader.get('change_pct') or 0) * 100:+.2f}%"
                )
        return "｜".join(parts)

    def min_abnormal_closeness(self) -> float:
        """启用的 abnormal 规则中最小的接近度阈值 (小数)。

        供调用方 (quote_service) 构建异动快照时预过滤, 不必按最高阈值拉全量。
        """
        thresholds = [
            float(r.get("threshold_pct", 70)) / 100
            for r in list(self._rules.values())
            if r.get("enabled", True) and r.get("type") == "abnormal"
        ]
        return min(thresholds) if thresholds else 1.0

    def evaluate_abnormal(self, rows: list[dict], *, now: float | None = None) -> list[dict]:
        """按异动边缘快照评估 type=abnormal 规则。

        rows 为 abnormal_moves.build_overview 的 rows (调用方已按
        min_abnormal_closeness 预过滤)。rows 为空也照常评估 —— 用于把
        已消失标的的边缘状态清理回 False。
        """
        rules = [
            rule
            for rule in list(self._rules.values())
            if rule.get("enabled", True) and rule.get("type") == "abnormal"
        ]
        if not rules:
            return []
        timestamp = time.time() if now is None else now
        events: list[dict] = []
        for rule in rules:
            try:
                events.extend(self._evaluate_abnormal_rule(rule, rows, timestamp))
            except Exception as exc:  # noqa: BLE001
                logger.warning("异动规则评估失败 %s: %s", rule.get("id"), exc)
        return events

    def _build_abnormal_event(
        self,
        rule: dict,
        row: dict,
        symbol: str,
        best: tuple[str, float, float, float],
        event_type: str,
        now: float,
    ) -> dict:
        return {
            "ts": int(now * 1000),
            "rule_id": rule["id"],
            "rule_name": rule.get("name", ""),
            "strategy_id": None,
            "source": "abnormal",
            "type": event_type,
            "symbol": symbol,
            "name": row.get("name"),
            "message": rule.get("message", "") or self._abnormal_message(row, best),
            "price": row.get("close"),
            "change_pct": row.get("rt_pct"),
            "signals": [],
            "severity": rule.get("severity", "info"),
            "conditions": [],
            "logic": "and",
            "abnormal_window": best[0],
            "abnormal_value": round(best[2], 4),
            "abnormal_threshold": best[3],
            "abnormal_closeness": round(best[1], 4),
        }

    def _evaluate_abnormal_rule(self, rule: dict, rows: list[dict], now: float) -> list[dict]:
        events: list[dict] = []
        threshold = float(rule.get("threshold_pct", 70)) / 100
        if not 0 < threshold <= 1.5:
            threshold = 0.7
        direction = rule.get("direction", "both")
        window_filter = str(rule.get("abnormal_window", "any"))
        scope_symbols = _scope_symbols_for(rule)
        if scope_symbols is _GROUP_MISSING:
            return events

        seen: set[str] = set()
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if not symbol or (scope_symbols is not None and symbol not in scope_symbols):
                continue
            seen.add(symbol)
            best = _best_abnormal_window(row, window_filter, direction)
            condition = best is not None and best[1] >= threshold
            state_key = (rule["id"], symbol)
            previous = self._abnormal_condition_state.get(state_key)
            self._abnormal_condition_state[state_key] = condition
            if previous is None or previous or not condition:
                continue

            best_event = cast(tuple[str, float, float, float], best)  # condition 已保证非 None
            event_type = f"abnormal_{'up' if best_event[2] > 0 else 'down'}"
            cooldown_key = (rule["id"], symbol, event_type)
            last = self._last_fire.get(cooldown_key)
            cooldown = int(rule.get("cooldown_seconds", 3600))
            if last is not None and now - last < cooldown:
                continue
            self._last_fire[cooldown_key] = now
            event = self._build_abnormal_event(rule, row, symbol, best_event, event_type, now)
            events.append(event)
            if self._alert_handler:
                try:
                    self._alert_handler(event)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("alert handler failed: %s", exc)
        # 本轮未出现的标的 (跌出预过滤区间) 状态置 False 而非删除:
        # 删除会被当成「首轮观测」而不触发, 置 False 才能在回升穿过阈值时再次告警。
        for key, value in list(self._abnormal_condition_state.items()):
            if key[0] == rule["id"] and key[1] not in seen and value:
                self._abnormal_condition_state[key] = False
        return events

    @staticmethod
    def _abnormal_message(row: dict, best: tuple[str, float, float, float]) -> str:
        window, closeness, value, threshold = best
        board = row.get("board") or ""
        tag = f"{board}{'·ST' if row.get('st') else ''}"
        state = "已达异常波动阈值" if closeness >= 1 else "接近异常波动阈值"
        return (
            f"{row.get('name') or row.get('symbol')} {window}偏离值 "
            f"{value * 100:+.2f}%/阈值{threshold * 100:.0f}% ({tag}) "
            f"接近度{closeness * 100:.0f}%, {state}"
        )

    def _fire_rule_events(
        self,
        hit_rows: list[tuple[str, str, Any, Any, Any, list[str]]],
        rule: dict,
        rtype: str,
        now: float,
    ) -> list[dict]:
        """对命中行做 cooldown 去重并生成 events（含告警回调）。"""
        cooldown = rule.get("cooldown_seconds", 3600)
        severity = rule.get("severity", "info")
        events: list[dict] = []
        for ev_type, sym, name, price, pct, hit_sigs in hit_rows:
            is_batch = sym == "_batch"
            key_symbol = f"_{ev_type}_batch" if is_batch else sym
            key = (rule["id"], key_symbol, ev_type)
            last = self._last_fire.get(key)
            if last is not None and (now - last) < cooldown:
                continue
            self._last_fire[key] = now

            if is_batch:
                resolved_name = ""
                message = name
            else:
                resolved_name = name if name else self._name_map.get(sym)
                message = rule.get("message", "") or self._default_message(
                    rule,
                    ev_type=ev_type,
                    sym=sym,
                    name=resolved_name,
                    pct=pct,
                    price=price,
                    conditions=list(rule.get("conditions", []))
                    if rule.get("type") != "strategy"
                    else None,
                    signals=hit_sigs,
                )

            events.append(
                {
                    "ts": int(now * 1000),
                    "rule_id": rule["id"],
                    "rule_name": rule.get("name", ""),
                    "strategy_id": rule.get("strategy_id") if rtype == "strategy" else None,
                    "source": rtype,
                    "type": ev_type,
                    "symbol": "" if is_batch else sym,
                    "name": resolved_name,
                    "message": message,
                    "price": price,
                    "change_pct": pct,
                    "signals": hit_sigs,
                    "severity": severity,
                    "conditions": list(rule.get("conditions", [])) if rtype != "strategy" else [],
                    "logic": rule.get("logic", "and") if rtype != "strategy" else "and",
                }
            )
            if self._alert_handler:
                try:
                    self._alert_handler(events[-1])
                except Exception as e:  # noqa: BLE001 - 告警回调外部注入, 失败隔离不影响规则评估
                    logger.warning("alert handler failed: %s", e)
        return events

    def _evaluate_rule(self, df: pl.DataFrame, rule: dict, now: float) -> list[dict]:
        """评估单条规则,返回触发的 events。"""
        # 1. 按 scope 过滤作用域
        scoped = self._apply_scope(df, rule)
        if scoped.is_empty():
            return []

        # 2. 根据 type 构建命中集
        #    元组格式: (event_type, symbol, name, price, pct, signals)
        hit_rows: list[tuple[str, str, Any, Any, Any, list[str]]] = []

        rtype = rule.get("type", "signal")
        if rtype == "strategy":
            # 策略类型: 跑策略, 同时产出所选的信号和结果池变更事件
            hit_rows = self._match_strategy(scoped, rule)
        elif rtype == "ladder":
            # 连板梯队封单监控: 独立处理 (需带预警封单值, 走专属 message)
            return self._evaluate_ladder(scoped, rule, now)
        elif rtype == "volume_delta":
            # 轮询放量监控: 相邻两次全市场快照的成交量差值, 独立处理走专属 message
            return self._evaluate_volume_delta(scoped, rule, now)
        else:
            # signal / price / market: 通用条件匹配
            for sym, name, price, pct, hit_sigs in self._match_conditions(scoped, rule):
                hit_rows.append((rtype, sym, name, price, pct, hit_sigs))

        if not hit_rows:
            return []

        return self._fire_rule_events(hit_rows, rule, rtype, now)

    @staticmethod
    def _apply_scope(df: pl.DataFrame, rule: dict) -> pl.DataFrame:
        """按 scope 过滤 DataFrame。"""
        scope = rule.get("scope", "symbols")
        if scope == "all":
            return df
        if scope == "symbols":
            syms = rule.get("symbols", [])
            if not syms:
                return df.head(0)
            return df.filter(pl.col("symbol").is_in(syms))
        if scope == "watchlist_group":
            # 动态绑定自选分组: 每轮评估按分组当前成员过滤 (带版本号缓存)。
            # 分组已删除/暂时为空 → fail-closed 返回空, 绝不退化为全市场。
            members = _group_members_or_none(rule)
            if not members:
                return df.head(0)
            return df.filter(pl.col("symbol").is_in(list(members)))
        if scope == "sector":
            # sector 过滤需 df 含板块列 (后续接入 ext_data JOIN)。在 JOIN 落地前
            # fail-closed 返回空 —— 绝不退化为「全市场」误触发 (旧行为 return df 会让
            # 一条板块规则对全市场每只命中都告警)。新建 sector 规则已在 validate 拦截,
            # 此处兜底任何历史遗留的 sector 规则。
            logger.warning(
                "scope=sector 规则 %s 暂不支持(板块 JOIN 未实现), 本轮跳过", rule.get("id")
            )
            return df.head(0)
        return df

    def _strategy_overrides(self, sid: str) -> dict:
        overrides = self._strategy_overrides(sid)
        return overrides

    def _prepare_strategy_context(
        self,
        s: Any,
        sid: str,
        at: str,
        df: pl.DataFrame,
        rule: dict,
        overrides: dict,
    ) -> Any | None:
        # 声明 filter_history 的策略 (如反包) 需要多日历史窗口才能判定形态。
        # 旧实现因"实时监控不支持 history loader"直接跳过 → 反包等策略盘中永不触发。
        # 现接入 history_loader, 拼历史窗口 + 今日实时行情, 经 precomputed_history 喂给引擎。
        # loader 为 None (未装配) 时退回跳过, 保持旧行为, 不破坏无历史场景。
        from app.strategy.engine import StrategyDataContext

        current_context = StrategyDataContext(
            asset_type=at,
            timeframe="1d",
            as_of=_monitor_pkg.cn_today(),
            current=df,
        )
        if getattr(s, "execution_backend", "polars_expr") == "composite":
            # 叠加策略首版不支持实时监控: 各子策略需独立预热实时矩阵, 热路径成本为 N 倍,
            # 违反"实时热路径不得随历史数据量线性增长"约束。fail-closed 跳过本轮,
            # /cached 端点回退到盘后 strategy_cache.json 的批量结果。
            logger.debug("叠加策略 %s 暂不支持实时监控, 跳过", sid)
            return None
        if getattr(s, "execution_backend", "polars_expr") == "matrix_native":
            matrix = self._active_matrix_snapshots.get(at)
            if matrix is None:
                logger.debug("策略 %s 缺少本轮实时矩阵快照, 跳过", sid)
                return None
            current_context = StrategyDataContext(
                asset_type=at,
                timeframe="1d",
                as_of=_monitor_pkg.cn_today(),
                current=df,
                market=matrix,
            )
        required_history_bars = 1
        history_resolver = getattr(self._strategy_engine, "required_history_bars", None)
        if callable(history_resolver):
            required_history_bars = history_resolver(
                [sid],
                overrides_map={sid: overrides},
            )
        backend = getattr(s, "execution_backend", "polars_expr")
        # 叠加条件带「前 N 日」偏移时同样需要历史窗口: polars 侧已由
        # required_history_bars 抬升 (>1); 矩阵策略的信号走实时矩阵快照, 平时
        # 不加载历史, 仅此情形补窗口供 overlay 在历史行上求值。
        overlay_days = 0
        overlay_conditions = (overrides or {}).get("overlay_filter")
        if overlay_conditions:
            from app.strategy import custom_signals as _custom_signals

            overlay_days = _custom_signals.overlay_max_days(overlay_conditions)
        needs_history = (
            backend not in {"composite", "matrix_native"}
            and (s.filter_history_fn or required_history_bars > 1)
        ) or (backend == "matrix_native" and overlay_days > 0)
        if not needs_history:
            return current_context
        return self._load_strategy_history_window(
            current_context,
            s,
            sid,
            at,
            df,
            rule,
            required_history_bars,
            backend,
            overlay_days,
        )

    def _load_strategy_history_window(
        self,
        current_context: Any,
        s: Any,
        sid: str,
        at: str,
        df: pl.DataFrame,
        rule: dict,
        required_history_bars: int,
        backend: str,
        overlay_days: int,
    ) -> Any | None:
        from app.strategy.engine import StrategyDataContext

        history_loader = self._history_loader_for(rule)
        if history_loader is None:
            logger.debug(
                "策略 %s 需要历史数据但未注入 history_loader (asset_type=%s), 跳过实时监控",
                sid,
                rule.get("asset_type", "stock"),
            )
            return None
        try:
            today = _monitor_pkg.cn_today()
            lookback = max(1, getattr(s, "lookback_days", 1), required_history_bars)
            if backend == "matrix_native":
                # 矩阵补窗口只为 overlay 偏移求值, 按回看深度取窗口即可
                lookback = max(1, overlay_days + 1)
            hist_df = history_loader(today, lookback)
            if hist_df is None or hist_df.is_empty():
                logger.debug("策略 %s 历史数据为空, 跳过本轮实时监控", sid)
                return None
            # 历史窗口可能与今日已落盘数据重叠: 排掉 hist_df 中 date==today 的行,
            # 今日行情始终以实时 df 为准 (盘中逐轮更新, 最接近收盘真相)。
            # 否则 today 行重复会污染 filter_history 的 .over("symbol") 窗口判定。
            if "date" in hist_df.columns:
                hist_df = hist_df.filter(pl.col("date") != today)
            # 拼接历史窗口 + 今日实时行情 (filter_history / overlay 偏移都用
            # .over("symbol") 窗口, 多日天然可用)
            current_context = StrategyDataContext(
                asset_type=at,
                timeframe="1d",
                as_of=today,
                current=df,
                market=current_context.market,
                history=pl.concat([hist_df, df], how="diagonal_relaxed"),
            )
            return current_context
        except Exception as e:  # noqa: BLE001 - 历史窗口加载失败则跳过该策略, 保持监控可用
            logger.warning("策略 %s 加载历史窗口失败, 跳过: %s", sid, e)
            return None

    def _run_strategy(
        self,
        sid: str,
        s: Any,
        df: pl.DataFrame,
        current_context: Any,
        overrides: dict,
    ) -> tuple[bool, Any]:
        try:
            result = self._strategy_engine.run(
                sid,
                current_context,
                pool=(
                    df["symbol"].cast(pl.Utf8).to_list()
                    if getattr(s, "execution_backend", "polars_expr") == "matrix_native"
                    else None
                ),
                overrides=overrides,
                params=dict(overrides.get("params") or {}),
            )
        except Exception as e:  # noqa: BLE001 - 策略执行异常降级为未触发, 不中断评估
            logger.warning("策略 %s 执行失败: %s", sid, e)
            return False, None
        return True, result

    def _cache_strategy_result(self, sid: str, at: str, result: Any) -> None:
        # 记录本轮完整策略结果 (供策略页实时回显: /cached 端点直接读取, 不落盘)。
        # 策略结果缓存仅用于股票策略页 /cached 回显; ETF 策略页走实时单跑, 不写入。
        # 写到 evaluate 提供的临时容器 (_building_strategy_results), 算完后整体替换,
        # 避免并发读到半填充状态。
        if at == "stock":
            try:
                self._building_strategy_results[sid] = {
                    "total": result.total,
                    "as_of": str(_monitor_pkg.cn_today()),
                    "rows": [
                        {
                            k: (None if isinstance(v, float) and not math.isfinite(v) else v)
                            for k, v in row.items()
                        }
                        for row in result.rows
                    ],
                }
                self._latest_strategy_result_ids.add(sid)
            except Exception:  # noqa: BLE001
                pass
