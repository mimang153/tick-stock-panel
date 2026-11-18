"""从 strategy/paper.py 抽取的 paper_positions 子模块（保持原公开 API）。"""

from __future__ import annotations

import json
from pathlib import Path

from app.services.fs_utils import atomic_write_text

DEFAULT_ACCOUNT_ID = "default"


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.strategy.paper"), name)


_rootmod = _RootProxy()


def _fills_path(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> Path:
    return _rootmod._root(data_dir, account_id) / "fills.jsonl"


def load_fills(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    p = _fills_path(data_dir, account_id)
    if not p.exists():
        return []
    out: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            _rootmod.logger.warning("paper fill line skipped: %s", e)
    return out


def load_positions(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    """读物化持仓 (无文件时从台账重建)。"""
    p = _rootmod._root(data_dir, account_id) / "positions.json"
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return data.get("positions", {})
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            _rootmod.logger.warning("paper positions load failed, rebuilding: %s", e)
    return replay_positions(data_dir, account_id=account_id)[0]


def replay_positions(
    data_dir: Path,
    fills: list[dict] | None = None,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> tuple[dict[str, dict], float]:
    """由台账重放推导 (持仓, 现金)。纯函数 — 重建与校验的唯一权威实现。

    持仓结构: {symbol: {asset_type, qty, avg_cost, available_qty,
                        lots: [{date: 'YYYY-MM-DD', qty}]}}
    corp_action 行: 数量乘 factor、成本除以 factor、日期归属保持原 buy_date。
    """
    acc = _rootmod.get_account(data_dir, account_id)
    cash = float(acc["initial_cash"]) if acc else 0.0
    positions: dict[str, dict] = {}
    for f in fills if fills is not None else load_fills(data_dir, account_id):
        kind = f.get("kind", "fill")
        symbol = f["symbol"]
        if kind == "corp_action":
            pos = positions.get(symbol)
            if pos is None:
                continue
            factor = float(f["factor"])
            for lot in pos["lots"]:
                lot["qty"] = round(lot["qty"] * factor, 6)
            pos["qty"] = round(pos["qty"] * factor, 6)
            pos["avg_cost"] = round(pos["avg_cost"] / factor, 6) if factor else 0.0
            pos["available_qty"] = round(sum(lot["qty"] for lot in pos["lots"]), 6)
            continue
        side, qty, price, fee = f["side"], int(f["qty"]), float(f["price"]), float(f.get("fee", 0))
        if side == "buy":
            cash -= qty * price + fee
            pos = positions.setdefault(
                symbol,
                {
                    "asset_type": f.get("asset_type", "stock"),
                    "qty": 0,
                    "avg_cost": 0.0,
                    "available_qty": 0,
                    "lots": [],
                },
            )
            total_cost = pos["avg_cost"] * pos["qty"] + qty * price + fee
            pos["qty"] += qty
            pos["avg_cost"] = round(total_cost / pos["qty"], 6) if pos["qty"] else 0.0
            pos["lots"].append({"date": f.get("date", ""), "qty": qty})
        else:
            cash += qty * price - fee
            pos = positions.get(symbol)
            if pos is None:
                _rootmod.logger.warning("replay: sell without position %s (台账异常)", symbol)
                pos = positions.setdefault(
                    symbol,
                    {
                        "asset_type": f.get("asset_type", "stock"),
                        "qty": 0,
                        "avg_cost": 0.0,
                        "available_qty": 0,
                        "lots": [],
                    },
                )
            remain = qty
            # FIFO 消耗批次; 台账正常时不会透支, 防御性 clamp
            for lot in pos["lots"]:
                if remain <= 0:
                    break
                take = min(lot["qty"], remain)
                lot["qty"] -= take
                remain -= take
            pos["lots"] = [lot for lot in pos["lots"] if lot["qty"] > 1e-9]
            pos["qty"] = max(0, pos["qty"] - qty)
            if pos["qty"] == 0:
                pos["avg_cost"] = 0.0
        pos["available_qty"] = _available_of(pos)
    return positions, round(cash, 2)


def sell_capacity(
    data_dir: Path,
    symbol: str,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> tuple[int, int] | None:
    """(T+1 可卖数量, 同 symbol 已挂 pending 卖出合计); 无持仓返回 None。

    可卖数量按当前交易日现算 (与 _fill_order / overview 同口径): 物化文件里的
    available_qty 是上次重建时的 T+1 口径, 跨日后不会更新, 次日仍会是 0。
    pending 卖出单占用可卖额度, 否则多张单各自通过校验、成交时逐张扣减会超额。
    """
    pos = load_positions(data_dir, account_id).get(symbol)
    if pos is None or pos["qty"] <= 0:
        return None
    available = _available_of(pos, _rootmod.cn_today().isoformat())
    pending_sell = sum(
        int(o["qty"])
        for o in _rootmod.load_orders(data_dir, account_id)
        if o["status"] == "pending" and o["side"] == "sell" and o["symbol"] == symbol
    )
    return available, pending_sell


def _available_of(pos: dict, today: str | None = None) -> int:
    """T+1 可卖数量: buy_date < today 的批次之和。today 为空则全部可卖 (重建口径)。"""
    if today is None:
        return int(sum(lot["qty"] for lot in pos["lots"]))
    return int(sum(lot["qty"] for lot in pos["lots"] if lot["date"] < today))


def _append_fill(data_dir: Path, fill: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    p = _fills_path(data_dir, account_id)
    with _rootmod.PAPER_LOCK, p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(fill, ensure_ascii=False) + "\n")


def _materialize(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    """由台账重建 positions.json (每次成交后调用, 保持物化缓存与台账一致)。"""
    positions, _cash = replay_positions(data_dir, account_id=account_id)
    acc = _rootmod.get_account(data_dir, account_id)
    if acc is not None:
        today = _rootmod.cn_today().isoformat()
        for pos in positions.values():
            pos["available_qty"] = _available_of(pos, today)
    atomic_write_text(
        _rootmod._root(data_dir, account_id) / "positions.json",
        json.dumps(
            {"updated_at": _rootmod._now_iso(), "positions": positions},
            ensure_ascii=False,
            indent=2,
        ),
    )


def rebuild_positions(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    """修复兜底: 强制由台账重建物化持仓。"""
    with _rootmod.PAPER_LOCK:
        positions, cash = replay_positions(data_dir, account_id=account_id)
        acc = _rootmod.get_account(data_dir, account_id)
        if acc is not None:
            acc["cash"] = cash
            _rootmod.save_account(data_dir, acc, account_id)
        today = _rootmod.cn_today().isoformat()
        for pos in positions.values():
            pos["available_qty"] = _available_of(pos, today)
        atomic_write_text(
            _rootmod._root(data_dir, account_id) / "positions.json",
            json.dumps(
                {"updated_at": _rootmod._now_iso(), "positions": positions},
                ensure_ascii=False,
                indent=2,
            ),
        )
        return positions
