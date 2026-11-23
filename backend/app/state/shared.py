"""shared 目录门面（DES-006）。

只有 strategies/ 和 plugins/ 允许落在宿主机共享目录。
"""

from __future__ import annotations

from pathlib import Path

from app.config import settings


def shared_data_root() -> Path:
    """返回共享根目录并保证存在。"""
    root = settings.shared_data_root or (settings.data_dir / "shared")
    root.mkdir(parents=True, exist_ok=True)
    return root


def shared_plugin_root() -> Path:
    """返回用户插件根目录并保证存在。"""
    root = shared_data_root() / "plugins"
    root.mkdir(parents=True, exist_ok=True)
    return root


def shared_strategy_root() -> Path:
    """返回用户策略根目录并保证存在。"""
    root = shared_data_root() / "strategies"
    root.mkdir(parents=True, exist_ok=True)
    return root


def shared_strategy_dirs() -> list[Path]:
    """返回 StrategyEngine 使用的三个策略目录。"""
    dirs = [shared_strategy_root() / name for name in ("custom", "ai", "composite")]
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)
    return dirs


def shared_strategy_path(source: str, strategy_id: str) -> Path:
    """返回指定来源策略的文件路径。"""
    if source not in {"custom", "ai", "composite"}:
        raise ValueError(f"unsupported strategy source: {source}")
    if Path(strategy_id).name != strategy_id or not strategy_id.endswith(".py"):
        raise ValueError(f"invalid strategy id: {strategy_id}")
    return shared_strategy_root() / source / strategy_id
