"""导航布局 v1：分类只容纳页面，页面 ID 沿用宿主路由。"""

from __future__ import annotations

import json
import re


def _validate_nav_page(raw: object, pages: set[str], *, child: bool = False) -> dict:
    """校验并规范化单个页面项, 同时登记页面 ID 用于去重。"""
    if not isinstance(raw, dict) or raw.get("type") != "page":
        raise ValueError("分类下面只能放页面，不支持嵌套分类")
    path = raw.get("id")
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or path.startswith("//")
        or len(path) > 256
        or any(c in path for c in ("?", "#", "\\"))
        or any(ord(c) < 32 for c in path)
    ):
        raise ValueError("菜单 ID 须为站内页面路径")
    if path in pages:
        raise ValueError("同一页面只能出现一次")
    if child and path == "/":
        raise ValueError("看板固定在顶层，不能移入分类")
    pages.add(path)
    return {"type": "page", "id": path}


def _validate_nav_group(raw: dict, groups: set[str], pages: set[str]) -> dict:
    """校验并规范化一个分类及其子页面。"""
    group_id, label, children = raw.get("id"), raw.get("label"), raw.get("children")
    if not isinstance(group_id, str) or not re.fullmatch(r"group:[a-zA-Z0-9_-]{1,64}", group_id):
        raise ValueError("分类 ID 无效")
    if group_id in groups:
        raise ValueError("分类 ID 不能重复")
    if not isinstance(label, str) or not label.strip() or len(label.strip()) > 32:
        raise ValueError("分类名称须为 1~32 个字符")
    if not isinstance(children, list) or len(children) > 500:
        raise ValueError("分类 children 须为列表，最多 500 项")
    groups.add(group_id)
    return {
        "type": "group",
        "id": group_id,
        "label": label.strip(),
        "children": [_validate_nav_page(child, pages, child=True) for child in children],
    }


def validate_nav_layout(value: dict) -> dict:
    if not isinstance(value, dict) or type(value.get("v")) is not int or value["v"] != 1:
        raise ValueError("不支持的导航布局版本")
    items = value.get("items")
    if not isinstance(items, list) or len(items) > 500:
        raise ValueError("导航布局 items 须为列表，最多 500 项")
    if len(json.dumps(value, ensure_ascii=False)) > 64 * 1024:
        raise ValueError("导航布局不能超过 64KB")
    pages: set[str] = set()
    groups: set[str] = set()

    cleaned = []
    for raw in items:
        if isinstance(raw, dict) and raw.get("type") == "group":
            cleaned.append(_validate_nav_group(raw, groups, pages))
        else:
            cleaned.append(_validate_nav_page(raw, pages))
    # 导入旧数据或客户端省略看板时也保留固定入口。
    return {"v": 1, "items": [{"type": "page", "id": "/"}, *[x for x in cleaned if x["id"] != "/"]]}


def flat_nav_order(layout: dict) -> list[str]:
    return [
        page["id"]
        for node in layout["items"]
        for page in (node["children"] if node["type"] == "group" else [node])
    ]
