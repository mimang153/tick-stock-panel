#!/usr/bin/env python3
"""计算 Python 文件的文档覆盖率（行注释 + docstring），用于 pre-commit 门禁。

文档覆盖率定义（区间制，可上下限双向卡）:
    覆盖率 = 文档行 / 代码行 * 100
    文档行 = 行注释行 + docstring 行
    代码行 = 参与统计的非空行 - docstring 行

为什么要计入 docstring:
    tsp-rule/code-quality.mdc 强制「公共接口必须有 docstring」。旧口径把 docstring
    算作代码、只认 # 行注释，与规约直接矛盾：写满 docstring 的合规代码反而被判低覆盖。
    计入 docstring 后口径与规约一致，且经全仓测算能消除绝大多数误报。

统计用标准库 tokenize + ast:
    - tokenize 精确区分「真正的 # 注释」与字符串内的 # ；
    - ast 精确识别模块/类/函数/方法的 docstring 行（不把普通字符串误计为文档）。

硬门禁语义（区间）:
    - 低于 COMMENT_COVERAGE_MIN → 文档化不足，拦截
    - 高于 COMMENT_COVERAGE_MAX → 文档异常冗余，拦截（提示凑注释/夹带停用代码）
    - 区间内 → 通过

注意：本门禁是「增量」的，只检查 pre-commit 传入的本次改动文件。测试文件由钩子
exclude 豁免（测试即文档，用例名与断言自解释，注释率天然低）。
用法（pre-commit 传入文件）:
    COMMENT_COVERAGE_MIN=5 COMMENT_COVERAGE_MAX=60 python comment_coverage.py f1.py f2.py
退出码: 0=全部在区间内; 1=存在区间外。
"""

from __future__ import annotations

import ast
import io
import os
import sys
import tokenize


def _comment_vs_code_lines(src: str) -> tuple[set[int], set[int]]:
    """返回 (行注释行号集合, 代码非空行号集合)。docstring 此时仍算在代码行内。"""
    comment_lines: set[int] = set()
    code_lines: set[int] = set()
    try:
        tokens = tokenize.generate_tokens(io.StringIO(src).readline)
        for tok in tokens:
            typ, _str, (srow, _), (_erow, _ecol), _ = tok
            if typ == tokenize.COMMENT:
                comment_lines.add(srow)
            elif typ in (tokenize.NAME, tokenize.OP, tokenize.NUMBER, tokenize.STRING):
                code_lines.add(srow)
    except (tokenize.TokenError, IndentationError):
        # 语法错误交由调用方按「统计不可靠」处理；这里仅返回已收集结果。
        pass
    return comment_lines, code_lines


def _docstring_lines(src: str) -> set[int]:
    """用 ast 返回模块/类/函数/方法 docstring 的行号集合。"""
    doc_lines: set[int] = set()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return doc_lines
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                expr = body[0]
                doc_lines.update(range(expr.lineno, (expr.end_lineno or expr.lineno) + 1))
    return doc_lines


def _doc_counts(src: str) -> tuple[int, int] | None:
    """返回 (文档行数, 代码行数)；空文件返回 None。

    注意 tokenize 对三引号字符串只产出一个 STRING token（srow 在首行），
    续行不会进入 code_lines，因此 docstring 必须用 ast 的完整行号集合计入分子，
    不能与 code_lines 取交集（否则丢失续行）。分母仅从 code_lines 扣除重叠部分。
    """
    comment_lines, code_lines = _comment_vs_code_lines(src)
    doc_lines = _docstring_lines(src)
    code = len(code_lines - (doc_lines & code_lines))
    doc = len(comment_lines | doc_lines)
    if code <= 0:
        return None
    return doc, code


def _coverage(doc: int, code: int) -> float:
    """返回文档覆盖率百分比；调用方保证 code > 0。"""
    return doc / code * 100.0


def _report_one(f: str, doc: int, code: int, min_pct: float, max_pct: float) -> bool:
    """打印单文件结果；返回是否达标（True=达标）。"""
    cov = _coverage(doc, code)
    status = "OK"
    if cov < min_pct:
        status = "LOW"
    elif cov > max_pct:
        status = "HIGH"

    print(
        f"[{status}] 文档覆盖率 {cov:.1f}% (文档行 {doc} / 代码行 {code})  "
        f"范围 [ {min_pct:.0f}% , {max_pct:.0f}% ]  {f}"
    )
    if status == "LOW":
        print(
            "  → 文档偏少。确保公共接口有 docstring、关键分支/复杂边界有行注释"
            "(见 tsp-rule/code-quality.mdc)。"
        )
    elif status == "HIGH":
        print("  → 文档偏多。检查是否冗余说明、凑比例、或夹带注释掉的停用代码，精简之。")
    return status == "OK"


def main() -> int:
    files = sys.argv[1:]
    if not files:
        print(
            "用法: COMMENT_COVERAGE_MIN=5 COMMENT_COVERAGE_MAX=60 "
            "python comment_coverage.py <file.py> [...]",
            file=sys.stderr,
        )
        return 2

    try:
        min_pct = float(os.environ.get("COMMENT_COVERAGE_MIN", "5"))
        max_pct = float(os.environ.get("COMMENT_COVERAGE_MAX", "60"))
    except ValueError:
        print("配置错误: COMMENT_COVERAGE_MIN/MAX 不是有效数字", file=sys.stderr)
        return 2
    if min_pct > max_pct:
        print(f"配置错误: COMMENT_COVERAGE_MIN({min_pct}) > MAX({max_pct})", file=sys.stderr)
        return 2

    fail = 0
    for f in files:
        if not os.path.isfile(f) or not f.endswith(".py"):
            continue
        try:
            with open(f, encoding="utf-8") as fh:
                src = fh.read()
        except (OSError, UnicodeDecodeError) as e:
            print(f"统计失败(跳过): {f}: {e}", file=sys.stderr)
            continue

        counts = _doc_counts(src)
        if counts is None:
            print(f"空文件(跳过): {f} (0 代码行)")
            continue
        doc, code = counts
        if not _report_one(f, doc, code, min_pct, max_pct):
            fail = 1
    if fail:
        print(
            "文档覆盖率门禁未通过：请在不堆冗余注释的前提下补齐必要的文档化内容。", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
