"""文件系统小工具 — 原子写(文本/parquet)统一实现, 避免各处内联重复。"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 只做类型标注; JSON 原子写的调用方 (preferences/secrets) 不必加载 polars
    import polars as pl


def atomic_write_text(path: Path, text: str, *, mode: int | None = None) -> None:
    """临时文件 + os.replace 原子替换; mode 在替换前打到临时文件以消除权限窗口。

    Windows 上 chmod 只影响只读位, 失败可忽略(与原调用处一致)。
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    if mode is not None:
        with contextlib.suppress(OSError):
            os.chmod(tmp, mode)
    os.replace(tmp, path)


def atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """parquet 版原子写: 先写 .tmp 再替换, 避免 kill/断电/写满留半截文件。

    .tmp 后缀不匹配 *.parquet glob; Windows 并发读由 replace_with_retry 退避穿过。
    """
    from app.tickflow.repository import replace_with_retry  # 惰性导入, 避免模块级环

    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp)
    replace_with_retry(tmp, path)
