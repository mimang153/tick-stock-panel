"""MonitorRuleEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ._common import _signal_cn_name, format_alert_quote


class _MonitorRuleEngineMixin4:
    # 由 Mixin1 提供（分包后 mypy 不可见）。
    _strategy_engine: Any
    _signal_label: Any

    def _default_message(
        self,
        rule: dict,
        ev_type: str = "",
        sym: str = "",
        name: str = "",
        pct: Any = None,
        price: Any = None,
        conditions: list[dict] | None = None,
        signals: list[str] | None = None,
    ) -> str:
        """生成默认 message。

        - strategy: 按变更方向生成 (进入/移出 + 涨跌幅)
        - signal/price/market: 命中信号 (signals 非空时) 或条件摘要 + 现价 + 涨跌幅
        """
        rtype = rule.get("type", "signal")
        if rtype == "strategy":
            # 从 StrategyEngine 取策略名; 失败则退化为 rule_name 里截取的部分
            sname = ""
            sid = rule.get("strategy_id")
            if sid and self._strategy_engine is not None:
                try:
                    s = self._strategy_engine.get(sid)
                    sname = s.meta.get("name", "") or s.meta.get("id", "")
                except Exception:  # noqa: BLE001
                    sname = ""
            if not sname:
                rn = rule.get("name", "")
                sname = rn.split(" · ", 1)[1] if " · " in rn else (rn or "策略")

            action = {
                "buy_signal": "买入信号",
                "sell_signal": "卖出信号",
                "pool_entry": "进入策略结果",
                "pool_exit": "移出策略结果",
                "new_entry": "进入策略结果",
                "dropped": "移出策略结果",
            }.get(ev_type)
            if action:
                pct_text = ""
                if pct is not None:
                    sign = "+" if pct >= 0 else ""
                    pct_text = f" {sign}{pct * 100:.1f}%"
                return f"策略「{sname}」{action} {name}{pct_text}"
            return f"策略「{sname}」事件"

        # signal / price / market: 命中信号 + 现价 + 涨跌幅
        # 有实际命中信号 (op=truth 且为真的子集) 时以命中信号开头 — 全量规则
        # 条件仍保留在 event.conditions 供前端展示, message 不再复读 (OR 规则
        # 条件多时全文复读会淹没真正触发的条件)。
        tail = format_alert_quote(price, pct)
        if signals:
            hit_text = "命中 " + "、".join(self._signal_label(s) for s in signals)
            # AND 规则: 比较条件同样全部满足, 补进 message 保持信息完整。
            # OR 规则无法判定哪些比较条件为真 (hit_sigs 只收集 truth 信号), 不补。
            if rule.get("logic", "and") == "and":
                comp = [
                    c
                    for c in (conditions if conditions is not None else rule.get("conditions", []))
                    if c.get("op") != "truth"
                ]
                if comp:
                    comp_text = self._format_conditions_text(
                        rule, comp, resolver=self._signal_label
                    )
                    if comp_text:
                        hit_text = f"{hit_text} 且 {comp_text}"
            return f"{hit_text} · {tail}" if tail else hit_text
        # 无 truth 命中 (纯比较条件规则): 回退条件摘要
        # 条件摘要: 把 conditions (truth/比较) 拼成可读串, 如 "MA20金叉 且 量比>2"
        cond_text = self._format_conditions_text(rule, conditions, resolver=self._signal_label)
        if cond_text and tail:
            return f"{cond_text} · {tail}"
        return cond_text or tail or "监控触发"

    @staticmethod
    def _format_conditions_text(
        rule: dict, conditions: list[dict] | None, resolver: Callable[[str], str] | None = None
    ) -> str:
        """把 rule.conditions 拼成可读文本 (用于 message / 推送)。

        op=truth: 直接用信号中文名 (如 "MA20金叉")
        op=比较: 字段中文名 + 操作符 + 值 (如 "涨跌幅≥5")
        logic: and → "且", or → "或"
        resolver: 自定义信号名解析器 (默认 _signal_cn_name); 引擎内传
            self._signal_label 以解析 csg_/csgi_ 用户命名, 外部 (lots.py) 不传。
        """
        conds = conditions if conditions is not None else list(rule.get("conditions", []))
        if not conds:
            return ""
        logic_word = "且" if rule.get("logic", "and") == "and" else "或"
        label_of = resolver or _signal_cn_name
        parts: list[str] = []
        for c in conds:
            field = c.get("field", "")
            op = c.get("op", "truth")
            value = c.get("value")
            label = label_of(field) or field
            if op == "truth":
                parts.append(label)
            else:
                op_map = {"gte": "≥", "lte": "≤", "gt": ">", "lt": "<", "eq": "="}
                parts.append(f"{label}{op_map.get(op, op)}{value}")
        return f" {logic_word} ".join(parts)
