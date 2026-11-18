"""延迟解析根 API 模块属性，保留既有测试 monkeypatch 的可观察性。

拆分出的子模块通过 ``_root.xxx`` 访问根模块符号：代理在每次属性访问时才
从 ``sys.modules`` 取值，因此 ``monkeypatch.setattr(根模块, ...)`` 始终生效。
"""

from __future__ import annotations

import sys
from typing import Any


class _RootRef:
    def __init__(self, module_name: str) -> None:
        self._module_name = module_name

    def __getattr__(self, name: str) -> Any:
        return getattr(sys.modules[self._module_name], name)


def bind(module_name: str) -> Any:
    """绑定一个延迟解析 ``module_name`` 属性的只读代理。"""
    return _RootRef(module_name)
