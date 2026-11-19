"""DuckDB 标识符转义与校验原语 (SQL 注入防护)。"""

from __future__ import annotations

import re

EXT_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def quote_ident(name: str) -> str:
    """转义为 DuckDB 双引号标识符：内嵌双引号双写，任意字符都不会被解释为 SQL。"""
    return '"' + name.replace('"', '""') + '"'


def is_valid_ext_ident(name: str) -> bool:
    """config_id 等纯标识符字段是否仅含合法字符 (字母/数字/下划线)。"""
    return bool(EXT_IDENT_RE.match(name))
