"""HTTP 双栈接口对照工具（CH-07 / T6.7 自动化方向）。

同一批只读接口分别请求 origin（旧栈，oracle）与 dev（分布式新栈），做结构化
比对：先比状态码，再比 JSON 结构与值；对已知允许差异字段按 DEP-003 §1.3 归一。
数据层对照见 cmp_stacks.py；本脚本补的是 HTTP 接口口径层。

用法：
    python scripts/cmp_api_stacks.py --token tsp_xxx
    python scripts/cmp_api_stacks.py --token tsp_xxx --out reports/cmp_api.json

dev 栈需 Bearer token（read:market/read:ext/read:analysis）；origin 免登录。
判定：OK（一致）/ DIFF（差异）/ BLOCKED（dev 数据健康闸门挡住，非口径差异）。
退出码：0 = 无差异；1 = 存在差异；2 = 服务不可达/未授权。
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ORIGIN_BASE = "http://localhost:4018"
DEV_BASE = "http://localhost:3018"
DEFAULT_START = "2026-11-12"
DEFAULT_END = "2026-11-13"
SYMBOL = "600519.SH"
FLOAT_RTOL = 1e-6
# DEP-003 §1.3 已知允许差异字段：origin 与 dev 时点/统计口径不同，归一后不比。
ALLOWED_DIFF_FIELDS = {"as_of", "total", "updated_at", "generated_at", "timestamp"}


@dataclass(frozen=True)
class Case:
    """一个接口对照用例：路径 + 查询参数。"""

    name: str
    path: str
    params: dict[str, str]


def build_cases(start: str, end: str) -> list[Case]:
    """构造稳定的只读对照用例；避开实时行情/最新交易日/写操作。"""
    return [
        Case("instruments_search", "/api/kline/instruments/search", {"q": "600519"}),
        Case(
            "kline_daily",
            "/api/kline/daily",
            {"symbol": SYMBOL, "start_date": start, "end_date": end},
        ),
        Case(
            "index_daily",
            "/api/index/daily",
            {"symbol": "000001.SH", "start_date": start, "end_date": end},
        ),
        Case("overview_market", "/api/overview/market", {}),
        Case("regime_latest", "/api/regime/latest", {}),
        Case("screener_market_snapshot", "/api/screener/market-snapshot", {}),
        Case("ext_schema_all", "/api/ext-data/schema-all", {}),
        Case("alerts", "/api/alerts", {}),
    ]


def fetch(base: str, path: str, params: dict[str, str], token: str | None) -> tuple[int, Any]:
    """GET 一个接口，返回 (状态码, 解析后的 JSON)。"""
    url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, body


def normalize(value: Any) -> Any:
    """对 JSON 做归一：剔除已知允许差异字段，规整浮点。"""
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items() if k not in ALLOWED_DIFF_FIELDS}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, float):
        return round(value, 6)
    return value


def diff(origin: Any, dev: Any, path: str = "") -> list[str]:
    """递归比较两个 JSON 值，返回差异描述列表（最多 5 条）。"""
    out: list[str] = []
    if isinstance(origin, dict) and isinstance(dev, dict):
        for key in sorted(set(origin) | set(dev)):
            if key not in origin:
                out.append(f"{path}.{key}: origin 缺失")
            elif key not in dev:
                out.append(f"{path}.{key}: dev 缺失")
            else:
                out.extend(diff(origin[key], dev[key], f"{path}.{key}"))
    elif isinstance(origin, list) and isinstance(dev, list):
        if len(origin) != len(dev):
            out.append(f"{path}: 长度 origin={len(origin)} dev={len(dev)}")
        for i, (a, b) in enumerate(zip(origin, dev, strict=False)):
            out.extend(diff(a, b, f"{path}[{i}]"))
    elif isinstance(origin, int | float) and isinstance(dev, int | float):
        if abs(origin - dev) > FLOAT_RTOL * max(1.0, abs(origin)):
            out.append(f"{path}: origin={origin} dev={dev}")
    elif origin != dev:
        out.append(f"{path}: origin={origin!r} dev={dev!r}")
    return out[:5]


def run_case(case: Case, token: str) -> dict[str, Any]:
    """执行单个用例并返回比对结果。"""
    o_code, o_body = fetch(ORIGIN_BASE, case.path, case.params, None)
    d_code, d_body = fetch(DEV_BASE, case.path, case.params, token)
    result: dict[str, Any] = {
        "case": case.name,
        "path": case.path,
        "params": case.params,
        "origin_status": o_code,
        "dev_status": d_code,
    }
    # dev 端完整性闸门（新栈特有的数据健康保护）导致无法对照：归 BLOCKED，
    # 不算口径差异，也不算通过。需先完成 dev 数据修补再重测。
    if isinstance(d_body, dict) and d_body.get("code") == "daily_integrity":
        result.update(verdict="BLOCKED", reason=f"dev 数据健康闸门: {d_body.get('detail', '')}")
        return result
    if o_code != d_code:
        result.update(verdict="DIFF", reason=f"状态码不同: {o_code} vs {d_code}")
        return result
    diffs = diff(normalize(o_body), normalize(d_body))
    if diffs:
        result.update(verdict="DIFF", reason="; ".join(diffs))
    else:
        result.update(verdict="OK", reason="")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="HTTP 双栈接口对照")
    parser.add_argument("--token", required=True, help="dev 栈 Bearer token")
    parser.add_argument("--start", default=DEFAULT_START, help="对比起始交易日")
    parser.add_argument("--end", default=DEFAULT_END, help="对比结束交易日")
    parser.add_argument("--out", help="报告输出路径（JSON）")
    args = parser.parse_args()

    # 先探活，服务/凭据不可用时直接返回 2，避免产出误导性全差异报告。
    for base, token in ((ORIGIN_BASE, None), (DEV_BASE, args.token)):
        code, _ = fetch(base, "/api/auth/status", {}, token)
        if code >= 400:
            print(f"服务不可达或未授权: {base} -> {code}", file=sys.stderr)
            return 2

    results = [run_case(c, args.token) for c in build_cases(args.start, args.end)]
    n_diff = sum(r["verdict"] == "DIFF" for r in results)
    n_blocked = sum(r["verdict"] == "BLOCKED" for r in results)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window": {"start": args.start, "end": args.end},
        "summary": {
            "total": len(results),
            "ok": len(results) - n_diff - n_blocked,
            "diff": n_diff,
            "blocked": n_blocked,
        },
        "cases": results,
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        print(f"报告已写入: {out}")
    print(
        f"对照结果: {len(results) - n_diff - n_blocked}/{len(results)} 一致, "
        f"{n_diff} 差异, {n_blocked} 阻塞(dev 数据健康闸门)"
    )
    for r in results:
        if r["verdict"] != "OK":
            print(f"  {r['verdict']} {r['case']}: {r['reason']}")
    return 1 if n_diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
