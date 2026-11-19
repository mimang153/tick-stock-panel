"""用户偏好设置拆分 part 3（原 app.services.preferences）。"""

from __future__ import annotations

import json
from typing import cast

import app.services.preferences as _self

from ._state import (
    _DASHBOARD_LAYOUT_MAX_ITEMS,
    _DASHBOARD_LAYOUT_MAX_JSON,
    _UI_PREFS_KEYS,
    _UI_PREFS_MAX_JSON,
)


def _clamp_int(v: object, lo: int, hi: int, fallback: int) -> int:
    try:
        n = int(cast(str | int | float, v))
    except (TypeError, ValueError):
        return fallback
    return max(lo, min(hi, n))


def set_dashboard_layout(value: dict | None) -> dict | None:
    """保存看板布局; None = 清除(回默认布局)。

    护栏: items 数量、字段类型/取值范围、整体 JSON 体积;
    前端渲染前还会再做一轮规范化(未知组件剔除等), 此处只防脏数据写盘。
    """
    if value is None:
        _self.save({"dashboard_layout": None})
        return None
    items = value.get("items") if isinstance(value, dict) else None
    if not isinstance(items, list) or not items:
        raise ValueError("布局须为 {v:1, items:[...]}; 恢复默认请传 null")
    cleaned: list[dict] = []
    for it in items[:_DASHBOARD_LAYOUT_MAX_ITEMS]:
        if not isinstance(it, dict):
            continue
        row: dict = {
            "i": str(it.get("i") or "")[:64],
            "t": str(it.get("t") or "")[:32],
            "x": _self._clamp_int(it.get("x"), 0, 11, 0),
            "y": _self._clamp_int(it.get("y"), 0, 100_000, 0),
            "w": _self._clamp_int(it.get("w"), 1, 12, 4),
            "h": _self._clamp_int(it.get("h"), 1, 30, 8),
        }
        if not row["i"] or not row["t"]:
            continue
        if isinstance(it.get("p"), dict):
            row["p"] = {str(k)[:32]: str(v)[:500] for k, v in it["p"].items()}
        cleaned.append(row)
    if not cleaned:
        raise ValueError("布局 items 无有效项")
    blob = {"v": 1, "items": cleaned}
    if len(json.dumps(blob, ensure_ascii=False)) > _DASHBOARD_LAYOUT_MAX_JSON:
        raise ValueError("布局数据过大 (>64KB)")
    _self.save({"dashboard_layout": blob})
    return blob


# ---------------------------------------------------------------------------
# 界面偏好 (UI prefs) — 原先只存浏览器 localStorage 的设置
# ---------------------------------------------------------------------------

# 这些键历史上只存 localStorage: 浏览器按 origin (协议+主机+端口) 隔离存储,
# 桌面版端口从 3018 起递增, 换端口后旧值全部读不到, 表现为「设置保存了重开又变回去」。
# 改存 preferences.json 并与 localStorage 双向同步: 后端是权威副本 (跨端口/多端一致),
# localStorage 保留为同步读取的本地缓存 (通知/语音/缩放等在非 React 路径即时读取)。


def get_ui_prefs() -> dict:
    """返回界面偏好 (只含用户显式设置过的键; 未涉及的键不出现, 前端用自身默认值)。"""
    raw = _self.load().get("ui_prefs")
    if not isinstance(raw, dict):
        return {}
    return {k: raw[k] for k in _UI_PREFS_KEYS if k in raw}


def set_ui_prefs(updates: dict) -> dict:
    """合并保存界面偏好; 未知键忽略, 只接受 JSON 标量。

    白名单 + 类型护栏: 该 blob 由前端自由读写, 不让它成为写任意结构的入口。
    值为 None 表示删除该项 (回前端默认)。
    """
    if not isinstance(updates, dict):
        raise ValueError("ui_prefs 需为对象")
    # 多项界面设置可同时写入，整个 blob 的读改写须与普通偏好保存共用锁。
    with _self.mutation():
        current = _self.get_ui_prefs()
        for key, value in updates.items():
            if key not in _UI_PREFS_KEYS:
                continue
            if key == "theme" and value not in ("light", "dark", None):
                raise ValueError("ui_prefs.theme 仅接受 light / dark")
            if key == "strategy_pool" and value is not None:
                try:
                    pool = json.loads(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("ui_prefs.strategy_pool 须为策略 ID 列表的 JSON 文本") from exc
                if not isinstance(pool, list) or any(not isinstance(item, str) for item in pool):
                    raise ValueError("ui_prefs.strategy_pool 须为策略 ID 列表的 JSON 文本")
            if value is None:
                current.pop(key, None)
            elif isinstance(value, bool | int | float | str):
                current[key] = value
            else:
                raise ValueError(f"ui_prefs.{key} 只接受标量")
        if len(json.dumps(current, ensure_ascii=False)) > _UI_PREFS_MAX_JSON:
            raise ValueError("ui_prefs 数据过大 (>16KB)")
        _self.save({"ui_prefs": current})
    return current
