#!/usr/bin/env python3
"""Python 函数/方法物理行数门禁（pre-commit 增量钩子后端脚本）。

统计口径与整改计划一致:
    行数 = end_lineno - lineno + 1（含 def 行、docstring、嵌套函数）。
阈值默认 80，可用环境变量 FUNC_LINE_LIMIT 覆盖。

豁免机制（显式、可追踪，禁止静默忽略）:
    在函数签名所在行或续行写 ``# func-length-allow: <理由>``，理由必填。
    仅写标记而缺理由视为无效豁免，仍会拦截。
    用自定义标记而非 ruff 的 noqa，避免与 ruff 规则码命名空间冲突、
    也避免 ruff 对 external 码报 warning；grep ``func-length-allow`` 即可追踪全部例外。

用法（pre-commit 传入本次改动的 .py 文件）:
    FUNC_LINE_LIMIT=80 python check_function_length.py file1.py file2.py
退出码: 0=全部达标; 1=存在超标; 2=用法/配置错误或源码无法解析。
"""

from __future__ import annotations

import ast
import io
import os
import re
import sys
import tokenize

# 匹配 func-length-allow: <理由>，理由至少含一个非空白字符。
_ALLOW_RE = re.compile(r"func-length-allow:\s*(?P<reason>\S.*?)\s*$")


def _comments_by_line(src: str) -> dict[int, str]:
    """按行号建立注释映射（用 tokenize 避免把字符串里的 # 误判为注释）。"""
    comments: dict[int, str] = {}
    try:
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type == tokenize.COMMENT:
                comments[tok.start[0]] = tok.string
    except (tokenize.TokenError, IndentationError):
        # 语法错误交由 ast.parse 报告，这里仅返回已收集的注释。
        pass
    return comments


def _allow_reason(comment: str) -> str | None:
    """返回豁免理由；无标记返回 None，有标记但缺理由返回空串。"""
    match = _ALLOW_RE.search(comment)
    if match is None:
        return None
    return (match.group("reason") or "").strip()


class _FuncCollector(ast.NodeVisitor):
    """收集所有 def/async def 的限定名、起止行与签名末行。"""

    def __init__(self) -> None:
        self.stack: list[str] = []
        self.funcs: list[tuple[str, int, int, int]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast.NodeVisitor 约定方法名
        self._collect(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 - ast.NodeVisitor 约定方法名
        self._collect(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast.NodeVisitor 约定方法名
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def _collect(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        qualname = ".".join([*self.stack, node.name])
        first_body_line = node.body[0].lineno if node.body else node.lineno
        header_end = max(node.lineno, first_body_line - 1)
        self.funcs.append((qualname, node.lineno, node.end_lineno or node.lineno, header_end))
        # 继续深入到嵌套函数，使其也被单独统计。
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()


def check_source(src: str, path: str, limit: int) -> tuple[list[str], int]:
    """返回 (违规信息列表, 有效豁免数量)。"""
    comments = _comments_by_line(src)
    collector = _FuncCollector()
    collector.visit(ast.parse(src, filename=path))

    violations: list[str] = []
    waived = 0
    for qualname, lineno, end_lineno, header_end in collector.funcs:
        length = end_lineno - lineno + 1
        if length <= limit:
            continue
        reasons = [
            reason
            for line in range(lineno, header_end + 1)
            if (reason := _allow_reason(comments.get(line, ""))) is not None
        ]
        if any(reasons):
            waived += 1
            continue
        if reasons:
            violations.append(
                f"{path}:{lineno} {qualname} ({length} 行 > {limit} 行) "
                "豁免无效: func-length-allow 缺少理由"
            )
        else:
            violations.append(f"{path}:{lineno} {qualname} ({length} 行 > {limit} 行)")
    return violations, waived


def main(argv: list[str]) -> int:
    files = argv[1:]
    if not files:
        print(
            "用法: FUNC_LINE_LIMIT=80 python check_function_length.py <file.py> [...]",
            file=sys.stderr,
        )
        return 2
    raw_limit = os.environ.get("FUNC_LINE_LIMIT", "80")
    try:
        limit = int(raw_limit)
    except ValueError:
        print(f"配置错误: FUNC_LINE_LIMIT({raw_limit!r}) 不是整数", file=sys.stderr)
        return 2
    if limit <= 0:
        print(f"配置错误: FUNC_LINE_LIMIT({limit}) 必须为正整数", file=sys.stderr)
        return 2

    fail = 0
    waived_total = 0
    for path in files:
        if not os.path.isfile(path) or not path.endswith(".py"):
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                src = fh.read()
            violations, waived = check_source(src, path, limit)
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            print(f"解析失败(不静默跳过): {path}: {exc}", file=sys.stderr)
            fail = 1
            continue
        waived_total += waived
        for violation in violations:
            print(f"[FAIL] 函数行数超标: {violation}")
            fail = 1

    if waived_total:
        print(f"显式豁免(带理由): {waived_total} 个函数。")
    if fail:
        print(
            "函数行数门禁未通过: 请拆分函数至 <= 阈值行，"
            "或在签名行写 # func-length-allow: <理由> 显式承认。",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
