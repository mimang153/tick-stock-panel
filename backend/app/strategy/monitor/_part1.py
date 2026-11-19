"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import datetime as _dt
import time
from collections.abc import Callable
from typing import Any

import polars as pl

import app.strategy.monitor as _monitor_pkg
from app.strategy import config as _strategy_config
from app.strategy.custom_signals import signal_names as _custom_signal_names
from app.strategy.intraday_signals import uses_intraday_signals
from app.strategy.monitor_rules import date_rule_in_window

from ._common import _signal_cn_name, logger


class _MonitorRuleEngineMixin1:
    """通用监控规则引擎 — 接收实时行情 DataFrame,评估所有规则,返回 AlertEvent。

    与 StrategyMonitorService 的区别:
      - 规则来自 monitor_rules 存储 (用户可配), 而非写死的 strategy config
      - 支持 scope (symbols/all/sector) 过滤作用域
      - 支持 conditions + logic (AND/OR) 任意组合
      - ★ cooldown 去重: 同一 (rule_id, symbol, event_type) 在冷却期内不重复触发
    """

    # 由 Mixin2 提供（分包后 mypy 不可见）。
    _evaluate_rule: Any

    def __init__(self, alert_handler: Callable[[dict], None] | None = None):
        self._alert_handler = alert_handler
        self._rules: dict[str, dict] = {}  # rule_id → rule
        # (rule_id, symbol, event_type) → 上次触发时间戳(秒)。用于 cooldown 去重。
        self._last_fire: dict[tuple[str, str, str], float] = {}
        # date 规则每个交易日只在首个轮询评估一次; 规则集变更时失效重评
        self._date_eval_day: str | None = None
        self._date_eval_rules_version = -1
        self._rules_version = 0  # set/add/remove/clear 递增, 供 date 缓存失效
        self._strategy_engine: Any = None  # 延迟注入, type=strategy 规则用它跑策略
        # symbol → 股票名 (enriched DataFrame 已 drop name 列, 触发时从此映射回填)
        self._name_map: dict[str, str] = {}
        # 策略池状态: (rule_id, strategy_id, asset_type) → 上期入选符号集合
        self._strategy_pools: dict[tuple[str, str, str], set[str]] = {}
        # 策略信号状态: (rule_id, strategy_id, asset_type, event_type) → (K线日期, 命中集合)
        self._strategy_signal_state: dict[tuple[str, str, str, str], tuple[str, set[str]]] = {}
        # 同一根 K 线的信号即使盘中回落后再次命中也只通知一次。
        self._strategy_signal_seen: dict[tuple[str, str, str, str, str], str] = {}
        # 数据目录 (用于加载策略 overrides)
        self._data_dir = None
        # 历史窗口加载器: (target_date, lookback_days) → 多日 enriched DataFrame。
        # 用于声明 filter_history 的策略 (如反包), 实时监控时拼历史窗口 + 今日行情跑策略。
        # 为 None 时, filter_history 策略仍会被跳过 (保持旧行为, 不破坏无历史场景)。
        self._history_loader: Callable[[_dt.date, int], pl.DataFrame] | None = None
        # ETF 版历史窗口加载器 (asset_type=etf 的规则用)。为 None 时 ETF filter_history 策略跳过。
        self._history_loader_etf: Callable[[_dt.date, int], pl.DataFrame] | None = None
        self._active_matrix_snapshots: dict[str, Any] = {}
        # 本轮 evaluate() 产出的策略结果: strategy_id → {rows, total, as_of}
        # 供策略页实时回显复用 (/api/screener/cached 端点直接读取, 避免重跑)。
        # 注意: 始终是「完整」的 dict —— evaluate 重算时先写到 _building_strategy_results,
        # 算完后整体替换此属性, 保证 /cached 并发读取永远拿到完整结果, 不会读到空中间态。
        self._latest_strategy_results: dict[str, dict] = {}
        # 本轮重算的临时容器 (_match_strategy 写入它); reset 轮开始时初始化为空 dict,
        # evaluate 结束后一次性替换 _latest_strategy_results。
        self._building_strategy_results: dict[str, dict] = {}
        # 本轮成功写入股票策略实时结果的策略 ID, 供 QuoteService 在计算完成后精确通知策略页。
        self._latest_strategy_result_ids: set[str] = set()
        self._sector_monitor_service = None
        self._sector_condition_state: dict[tuple[str, str], bool] = {}
        # abnormal 规则边缘触发状态: (rule_id, symbol) → 上一轮是否已达阈值。
        # 只在 False → True 跳变时告警 (首轮观测不触发, 防止新建规则瞬间刷屏)。
        self._abnormal_condition_state: dict[tuple[str, str], bool] = {}

    def set_strategy_engine(self, engine) -> None:
        """注入 StrategyEngine, type=strategy 规则据此跑策略。"""
        self._strategy_engine = engine

    def set_data_dir(self, data_dir) -> None:
        """注入数据目录, 用于加载策略的用户覆盖配置。"""
        self._data_dir = data_dir

    def _signal_label(self, field: str) -> str:
        """信号/字段 → 中文名: 信号列 (csg_/csgi_/signal_*) 查用户定义, 其余查 _SIGNAL_CN。

        信号命名从 data_dir 的 custom_signals 定义加载 (指纹缓存, 含迁移自
        内置、携带显式 signal_* 列名的定义); 未注入 data_dir 或查不到时回退原始列名。
        """
        if field.startswith(("csg_", "csgi_", "signal_")) and self._data_dir is not None:
            name = _custom_signal_names(self._data_dir).get(field)
            if name:
                return name
        return _signal_cn_name(field)

    def set_sector_monitor_service(self, service) -> None:
        self._sector_monitor_service = service

    def invalidate_strategy_state(self) -> None:
        """策略注册表变更后清除策略池、结果和矩阵快照。"""
        self._strategy_pools.clear()
        self._strategy_signal_state.clear()
        self._strategy_signal_seen.clear()
        self._latest_strategy_results = {}
        self._building_strategy_results = {}
        self._latest_strategy_result_ids.clear()
        self._active_matrix_snapshots.clear()

    def set_history_loader(self, fn) -> None:
        """注入历史窗口加载器, 用于声明 filter_history 的策略跑实时监控。

        loader 签名: (target_date, lookback_days) → 多日 enriched DataFrame。
        复用 ScreenerService._load_enriched_history (三级缓存, 命中 ~0ms)。
        为 None 时 filter_history 策略退回到跳过逻辑 (不破坏无历史场景)。
        """
        self._history_loader = fn

    def set_history_loader_etf(self, fn) -> None:
        """注入 ETF 版历史窗口加载器 (asset_type=etf 的 strategy 型规则用)。

        签名同 set_history_loader; 复用 ScreenerService(asset_type='etf')._load_enriched_history。
        为 None 时 ETF filter_history 策略退回到跳过逻辑。
        """
        self._history_loader_etf = fn

    def _history_loader_for(self, rule: dict):
        """按规则的 asset_type 选历史加载器。etf → ETF 加载器, 否则股票加载器。"""
        if rule.get("asset_type") == "etf":
            return self._history_loader_etf
        return self._history_loader

    def set_name_map(self, name_map: dict[str, str]) -> None:
        """注入 symbol → 股票名 映射, 用于在告警事件里回填 name 字段。

        enriched DataFrame 在 pipeline 计算后不含 name 列 (见 indicators/pipeline.py),
        触发时从 instruments 表预构建此映射, 保证 AlertEvent.name 有值。
        """
        self._name_map = name_map or {}

    # ── 规则管理 ───────────────────────────────────────
    @staticmethod
    def _rule_state_signature(rule: dict) -> tuple[Any, ...]:
        return (
            rule.get("type"),
            rule.get("strategy_id"),
            rule.get("score_min"),
            rule.get("score_max"),
            rule.get("asset_type", "stock"),
            rule.get("scope", "symbols"),
            tuple(sorted(str(symbol) for symbol in rule.get("symbols", []))),
            rule.get("sector"),
            rule.get("sector_kind"),
            tuple(sorted(str(target.get("key")) for target in rule.get("sector_targets", []))),
            rule.get("sector_trigger"),
            rule.get("direction"),
            rule.get("threshold_pct"),
            rule.get("window_minutes"),
            rule.get("abnormal_window"),
            rule.get("remind_date"),
            rule.get("lead_days"),
        )

    def set_rules(self, rules: list[dict]) -> None:
        """批量设置规则 (覆盖)。用于启动时 reload。

        先构建完整 dict 再原子替换 self._rules, 避免评估线程 (行情轮询)
        读到装载了一半的规则表。
        """
        new_rules: dict[str, dict] = {}
        for r in rules:
            if r.get("enabled") is not False:
                new_rules[r["id"]] = r
        changed_ids = {
            rule_id
            for rule_id, rule in new_rules.items()
            if rule_id in self._rules
            and self._rule_state_signature(self._rules[rule_id]) != self._rule_state_signature(rule)
        }
        self._rules = new_rules
        active_ids = set(new_rules) - changed_ids
        self._last_fire = {
            key: value for key, value in list(self._last_fire.items()) if key[0] in active_ids
        }
        self._strategy_pools = {
            key: value for key, value in list(self._strategy_pools.items()) if key[0] in active_ids
        }
        self._strategy_signal_state = {
            key: value
            for key, value in list(self._strategy_signal_state.items())
            if key[0] in active_ids
        }
        self._strategy_signal_seen = {
            key: value
            for key, value in list(self._strategy_signal_seen.items())
            if key[0] in active_ids
        }
        self._sector_condition_state = {
            key: value
            for key, value in list(self._sector_condition_state.items())
            if key[0] in active_ids
        }
        self._abnormal_condition_state = {
            key: value
            for key, value in list(self._abnormal_condition_state.items())
            if key[0] in active_ids
        }
        logger.info("MonitorRuleEngine: 装载 %d 条规则", len(self._rules))
        self._rules_version += 1

    def add_rule(self, rule: dict) -> None:
        if rule.get("enabled") is not False:
            self._rules[rule["id"]] = rule
        else:
            self._rules.pop(rule["id"], None)
        self._rules_version += 1

    def remove_rule(self, rule_id: str) -> None:
        self._rules.pop(rule_id, None)
        self._last_fire = {k: v for k, v in list(self._last_fire.items()) if k[0] != rule_id}
        self._strategy_pools = {
            k: v for k, v in list(self._strategy_pools.items()) if k[0] != rule_id
        }
        self._strategy_signal_state = {
            k: v for k, v in list(self._strategy_signal_state.items()) if k[0] != rule_id
        }
        self._strategy_signal_seen = {
            k: v for k, v in list(self._strategy_signal_seen.items()) if k[0] != rule_id
        }
        self._sector_condition_state = {
            k: v for k, v in self._sector_condition_state.items() if k[0] != rule_id
        }
        self._rules_version += 1

    def clear(self) -> None:
        self._rules.clear()
        self._last_fire.clear()
        self._strategy_pools.clear()
        self._strategy_signal_state.clear()
        self._strategy_signal_seen.clear()
        self._sector_condition_state.clear()
        self._rules_version += 1

    @property
    def rules(self) -> dict[str, dict]:
        return dict(self._rules)

    @property
    def rule_count(self) -> int:
        return len(self._rules)

    def latest_strategy_results(self) -> dict[str, dict]:
        """返回本轮 evaluate() 产出的策略结果 (strategy_id → {rows, total, as_of})。

        供策略页实时回显复用: /api/screener/cached 端点直接读取此内存结果,
        避免对被监控的策略重跑第二遍。无 type=strategy 规则时返回空 dict。
        """
        if self._data_dir is not None:
            from app.services.daily_health import DailyIntegrityError, assert_usable

            try:
                assert_usable(self._data_dir)
            except DailyIntegrityError:
                return {}
        return self._latest_strategy_results

    def invalidate_daily_asset(self, asset_type: str) -> None:
        """历史价格修正后失效计算快照; 保留告警去重和入选池, 避免重复通知。"""
        self._active_matrix_snapshots.pop(asset_type, None)
        if asset_type == "stock":
            self._latest_strategy_results = {}

    def consume_strategy_result_updates(self) -> bool:
        """返回并清除本轮成功写入的股票策略实时结果标记。"""
        updated = bool(self._latest_strategy_result_ids)
        self._latest_strategy_result_ids.clear()
        return updated

    def has_rule_type(self, rtype: str) -> bool:
        """是否存在指定类型的 (已启用) 规则。供 quote_service 判断是否需要注入特殊数据。"""
        if not self._rules:
            return False
        # list() 快照: API 线程可能并发增删规则, 直接迭代 dict 会抛 RuntimeError
        return any(
            r.get("enabled", True) and r.get("type") == rtype for r in list(self._rules.values())
        )

    def intraday_signal_symbols(self, asset_type: str) -> set[str]:
        """返回启用的分时信号规则所需标的并集。"""
        symbols: set[str] = set()
        for rule in list(self._rules.values()):
            if (
                rule.get("enabled", True)
                and rule.get("asset_type", "stock") == asset_type
                and rule.get("scope") == "symbols"
                and uses_intraday_signals(rule)
            ):
                symbols.update(str(symbol) for symbol in rule.get("symbols", []) if symbol)
        return symbols

    # ── 评估 ───────────────────────────────────────────
    def has_asset_rules(self, asset_type: str) -> bool:
        """是否存在指定资产类型的 (已启用) 规则。供 quote_service 判断是否需要 ETF 评估轮。"""
        if not self._rules:
            return False
        return any(
            r.get("enabled", True) and r.get("asset_type", "stock") == asset_type
            for r in list(self._rules.values())
        )

    def _prepare_matrix_snapshot(
        self,
        df: pl.DataFrame,
        asset_type: str,
        matrix_rules: list[dict],
        params_map: dict[str, dict],
        overrides_map: dict[str, dict],
    ) -> None:
        try:
            history_loader = self._history_loader_for(matrix_rules[0])
            if history_loader is None:
                raise ValueError("matrix strategy monitor requires history loader")
            matrix_ids = [str(rule["strategy_id"]) for rule in matrix_rules]
            history_bars = self._strategy_engine.required_history_bars(
                matrix_ids,
                params_map=params_map,
                overrides_map=overrides_map,
            )
            from app.services.daily_health import revision
            from app.strategy.engine import StrategyDataContext

            data_revision = (
                revision(self._data_dir, asset_type) if self._data_dir is not None else None
            )
            context = StrategyDataContext(
                asset_type=asset_type,
                timeframe="1d",
                as_of=_monitor_pkg.cn_today(),
                current=df,
                cache_key=f"monitor:{asset_type}",
                data_generation=data_revision,
            )
            try:
                snapshot = self._strategy_engine.prepare_realtime_matrix(
                    context,
                    matrix_ids,
                    params_map=params_map,
                    overrides_map=overrides_map,
                )
            except ValueError as exc:
                if "requires history data" not in str(exc):
                    raise
                history = history_loader(_monitor_pkg.cn_today(), history_bars)
                snapshot = self._strategy_engine.prepare_realtime_matrix(
                    StrategyDataContext(
                        asset_type=asset_type,
                        timeframe="1d",
                        as_of=_monitor_pkg.cn_today(),
                        current=df,
                        history=history,
                        cache_key=f"monitor:{asset_type}",
                        data_generation=data_revision,
                    ),
                    matrix_ids,
                    params_map=params_map,
                    overrides_map=overrides_map,
                )
            self._active_matrix_snapshots[asset_type] = snapshot
        except Exception as e:  # noqa: BLE001 - 矩阵快照准备失败时清理缓存并降级
            self._active_matrix_snapshots.pop(asset_type, None)
            logger.warning("%s 矩阵策略实时缓存准备失败: %s", asset_type, e)

    def _prepare_realtime_matrix(self, df: pl.DataFrame, asset_type: str) -> None:
        matrix_rules: list[dict] = []
        params_map: dict[str, dict] = {}
        overrides_map: dict[str, dict] = {}
        if self._strategy_engine is not None:
            for rule in list(self._rules.values()):
                if (
                    not rule.get("enabled", True)
                    or rule.get("type") != "strategy"
                    or rule.get("asset_type", "stock") != asset_type
                ):
                    continue
                sid = rule.get("strategy_id")
                if not sid:
                    continue
                try:
                    strategy = self._strategy_engine.get(sid)
                except Exception:  # noqa: BLE001 - 策略实例获取失败(已下线)则跳过该规则
                    continue
                if getattr(strategy, "execution_backend", "polars_expr") != "matrix_native":
                    continue
                overrides: dict[str, Any] = {}
                if self._data_dir:
                    overrides = _strategy_config.load_override(self._data_dir, sid)
                matrix_rules.append(rule)
                overrides_map[sid] = overrides
                params_map[sid] = dict(overrides.get("params") or {})
        if matrix_rules:
            self._prepare_matrix_snapshot(df, asset_type, matrix_rules, params_map, overrides_map)

    def evaluate(
        self, df: pl.DataFrame, asset_type: str = "stock", reset_strategy_results: bool = True
    ) -> list[dict]:
        """行情更新后评估规则。

        按 asset_type 只评估匹配资产类型的规则; ETF 规则应传 ETF enriched 快照。
        股票/ETF 分两轮评估时, 仅股票轮重置 _latest_strategy_results (它供股票策略页
        /cached 回显; ETF 策略页走实时单跑, 不依赖它)。

        Args:
            df: 实时 enriched 数据 (含 signal_/csg_/指标列)
            asset_type: 只评估该资产类型的规则 (默认 stock, 向后兼容)
            reset_strategy_results: 是否重置策略结果缓存 (多轮评估时仅首轮 True)
        Returns:
            触发的 AlertEvent dict 列表 (含 ts/rule_id/source/type/symbol/...)
        """
        if not self._rules or df.is_empty():
            return []

        if self._data_dir is not None:
            from app.services.daily_health import DailyIntegrityError, assert_usable

            try:
                assert_usable(self._data_dir, asset_type)
            except DailyIntegrityError:
                return []

        now = time.time()
        events: list[dict] = []
        # 原子化: reset 轮 (股票轮) 时先把本轮结果写到临时容器, 算完后一次性替换
        # _latest_strategy_results。这样 /cached 并发读取永远拿到完整结果,
        # 不会在「清空 → 逐个回填」窗口里读到空中间态 (曾导致策略页闪烁)。
        # 非 reset 轮 (ETF 轮) 继续往同一临时容器追加 (_match_strategy 仅写 stock, 实际不追加)。
        if reset_strategy_results:
            self._building_strategy_results = {}
            self._latest_strategy_result_ids.clear()

        self._prepare_realtime_matrix(df, asset_type)

        # list() 快照: 本方法跑在行情轮询线程, API 线程同时 add/remove 规则
        # 会触发 "dictionary changed size during iteration", 整轮告警丢失
        for rule_id, rule in list(self._rules.items()):
            if rule.get("asset_type", "stock") != asset_type:
                continue
            if rule.get("type") in ("sector", "abnormal", "date"):
                # 三者不走行情 DataFrame 评估, 各走 evaluate_sectors / evaluate_abnormal /
                # evaluate_date_rules 专用路径
                continue
            try:
                events.extend(self._evaluate_rule(df, rule, now))
            except Exception as e:  # noqa: BLE001 - 单条规则评估失败不阻断其它规则
                logger.warning("规则评估失败 %s: %s", rule_id, e)

        # 一次性提交本轮结果 (原子替换): /cached 读方要么拿到上一轮完整结果,
        # 要么拿到本轮完整结果, 不会读到空中间态。
        self._latest_strategy_results = self._building_strategy_results
        self._active_matrix_snapshots.pop(asset_type, None)

        return events

    def evaluate_date_rules(self, now: float | None = None) -> list[dict]:
        """纯日历评估 date 规则: 窗口命中 + 每天最多一次, 无行情条件。

        由行情轮询在盘中调用 (quote_service._evaluate_monitors), 事件与 _evaluate_rule 同构。
        窗口按自然日; 到期落在休市/节假日时需 lead_days 覆盖 (交易日历口径待 issue 定夺)。
        每个交易日只在首个轮询完整评估一次, 其余轮次命中缓存直接跳过。
        """
        now = now if now is not None else time.time()
        today_iso = _monitor_pkg.cn_today().isoformat()
        if (
            self._date_eval_day == today_iso
            and self._date_eval_rules_version == self._rules_version
        ):
            return []
        # 跨天首轮清掉已过期日期的按天 cooldown 键, 避免 _last_fire 无限累积
        self._last_fire = {
            key: value
            for key, value in self._last_fire.items()
            if not (key[1].startswith("_date_") and key[1] != f"_date_{today_iso}")
        }

        today_d = _dt.date.fromisoformat(today_iso)
        events: list[dict] = []
        for rule in list(self._rules.values()):
            if rule.get("type") != "date" or rule.get("enabled") is False:
                continue
            remind = rule.get("remind_date") or ""
            if not date_rule_in_window(remind, int(rule.get("lead_days", 0)), today_iso):
                continue
            # 按天隔离: 窗口内每天最多触发一次
            key = (rule["id"], f"_date_{today_iso}", "date")
            cooldown = int(rule.get("cooldown_seconds") or 86400)
            last = self._last_fire.get(key)
            if last is not None and (now - last) < cooldown:
                continue
            self._last_fire[key] = now

            symbols = [s for s in rule.get("symbols", []) if s]
            single_symbol = symbols[0] if len(symbols) == 1 else None
            msg = rule.get("message") or f"日期提醒 · {today_iso}"
            try:
                remain = (_dt.date.fromisoformat(remind) - today_d).days
            except ValueError:
                remain = 0
            msg += " · 今日到期" if remain <= 0 else f" · {remain}天后到期"
            # 单标的由 ev.symbol 携带; 仅多标的时拼列表
            if len(symbols) > 1:
                shown = "、".join(symbols[:3]) + ("等" if len(symbols) > 3 else "")
                msg = f"{msg} · {shown}"

            ev = {
                "ts": int(now * 1000),
                "rule_id": rule["id"],
                "rule_name": rule.get("name", ""),
                "strategy_id": None,
                "source": "date",
                "type": "date_reminder",
                "symbol": single_symbol or "",
                "name": (self._name_map.get(single_symbol) or single_symbol)
                if single_symbol
                else None,
                "message": msg,
                "price": None,
                "change_pct": None,
                "signals": [],
                "severity": rule.get("severity", "info"),
                "conditions": [],
                "logic": "and",
            }
            events.append(ev)
            if self._alert_handler:
                try:
                    self._alert_handler(ev)
                except Exception as e:  # noqa: BLE001
                    logger.warning("alert handler failed: %s", e)
        self._date_eval_day = today_iso
        self._date_eval_rules_version = self._rules_version
        return events

    def evaluate_sectors(
        self,
        stock_df: pl.DataFrame,
        index_df: pl.DataFrame,
        *,
        now: float | None = None,
    ) -> list[dict]:
        """按板块聚合快照评估 type=sector 规则。"""
        if self._sector_monitor_service is None:
            return []
        rules = [
            rule
            for rule in list(self._rules.values())
            if rule.get("enabled", True) and rule.get("type") == "sector"
        ]
        if not rules:
            return []

        targets_by_key: dict[str, dict] = {}
        windows: set[int] = set()
        for rule in rules:
            for target in rule.get("sector_targets", []):
                if target.get("key"):
                    targets_by_key[str(target["key"])] = target
            if rule.get("sector_trigger") == "momentum":
                windows.add(int(rule.get("window_minutes", 5)))

        timestamp = time.time() if now is None else now
        snapshots = self._sector_monitor_service.build_snapshots(
            stock_df,
            index_df,
            list(targets_by_key.values()),
            windows,
            now=timestamp,
        )
        events: list[dict] = []
        for rule in rules:
            try:
                events.extend(self._evaluate_sector_rule(rule, snapshots, timestamp))
            except Exception as exc:  # noqa: BLE001
                logger.warning("板块规则评估失败 %s: %s", rule.get("id"), exc)
        return events
