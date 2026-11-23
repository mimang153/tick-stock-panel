"""全页面双栈 HTTP 接口对照（origin 旧栈 vs dev 新栈）。

在 cmp_api_stacks.py 的固定 8 用例之外，按前端页面逐个覆盖只读 GET 接口：
全局外壳、看板、自选、选股、回测、因子、数据、指数、市场环境、设置、
开发者工具、模拟盘、预警、异动、复盘、财务、个股分析等。

dev 使用登录会话 Cookie（管理接口不接受 API Token），origin 免登录。
写接口、SSE、动态 ID 路径、敏感 token 接口不纳入。

用法：
    python scripts/cmp_api_all_pages.py --cookie 'tf_session=...' --out /tmp/all.json
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cmp_api_stacks import (
    DEFAULT_END,
    DEFAULT_START,
    DEV_BASE,
    ORIGIN_BASE,
    SYMBOL,
    diff,
    normalize,
)

INDEX_SYMBOL = "000001.SH"
AS_OF = "2026-11-13"


@dataclass(frozen=True)
class PageCase:
    """一条按页面归类的只读接口用例。"""

    page: str
    name: str
    path: str
    params: dict[str, str]


def _case(page: str, name: str, path: str, params: dict[str, str] | None = None) -> PageCase:
    """构造一个页面用例。"""
    return PageCase(page=page, name=name, path=path, params=params or {})


def _shell_cases() -> list[PageCase]:
    """所有页面共享的外壳与全局状态接口。"""
    return [
        _case("全局", "auth_status", "/api/auth/status"),
        _case("全局", "capabilities", "/api/capabilities"),
        _case("全局", "settings", "/api/settings"),
        _case("全局", "preferences", "/api/settings/preferences"),
        _case("全局", "capability_matrix", "/api/settings/capability-matrix"),
        _case("全局", "data_integrity", "/api/data/integrity"),
        _case("全局", "intraday_status", "/api/intraday/status"),
        _case("全局", "quote_interval", "/api/settings/preferences/quote-interval"),
        _case("全局", "data_version", "/api/data/version"),
        _case("全局", "data_status", "/api/data/status"),
        _case("全局", "intraday_versions", "/api/intraday/data-versions"),
        _case("全局", "task_registry", "/api/task-registry"),
    ]


def _market_cases(start: str, end: str) -> list[PageCase]:
    """看板、自选、选股、指数、异动、复盘等行情页接口。"""
    return [
        _case("看板", "overview_market", "/api/overview/market", {"as_of": AS_OF}),
        _case("看板", "pipeline_jobs", "/api/pipeline/jobs", {"limit": "1"}),
        _case("看板", "alerts", "/api/alerts", {"days": "7", "limit": "10"}),
        _case("看板", "emotion_config", "/api/settings/emotion-score/config"),
        _case("自选", "instruments_search", "/api/kline/instruments/search", {"q": "600519"}),
        _case("自选", "watchlist", "/api/watchlist"),
        _case("自选", "watchlist_groups", "/api/watchlist/groups"),
        _case("自选", "watchlist_enriched", "/api/watchlist/enriched"),
        _case("选股", "strategies", "/api/strategies", {"asset_type": "stock", "timeframe": "day"}),
        _case("选股", "screener_cached", "/api/screener/cached"),
        _case("选股", "market_snapshot", "/api/screener/market-snapshot"),
        _case("选股", "limit_ladder", "/api/screener/limit-ladder", {"as_of": AS_OF}),
        _case("选股", "rps_rotation", "/api/rps/rotation"),
        _case("指数", "intraday_indices", "/api/intraday/indices", {"symbols": INDEX_SYMBOL}),
        _case(
            "指数",
            "index_daily",
            "/api/index/daily",
            {"symbol": INDEX_SYMBOL, "start_date": start, "end_date": end},
        ),
        _case("指数", "index_minute", "/api/index/minute", {"symbol": INDEX_SYMBOL, "date": end}),
        _case("异动", "abnormal_overview", "/api/abnormal/overview"),
        _case("异动", "abnormal_intraday", "/api/abnormal/intraday"),
        _case("复盘", "auction_benchmark", "/api/market-recap/auction-benchmark"),
        _case("复盘", "market_recap_reports", "/api/market-recap/reports"),
        _case("复盘", "dragon_tiger", "/api/market-recap/dragon-tiger", {"date": end}),
    ]


def _kline_cases(start: str, end: str) -> list[PageCase]:
    """K 线与数据画像接口，使用固定历史窗口降低时点噪声。"""
    return [
        _case(
            "K线",
            "daily",
            "/api/kline/daily",
            {"symbol": SYMBOL, "start_date": start, "end_date": end},
        ),
        _case("K线", "daily_latest", "/api/kline/daily/latest", {"symbol": SYMBOL}),
        _case("K线", "minute", "/api/kline/minute", {"symbol": SYMBOL, "date": end}),
        _case("K线", "minute_range", "/api/kline/minute-range", {"symbol": SYMBOL, "days": "2"}),
        _case("K线", "minute_gaps", "/api/kline/minute-gaps"),
        _case("数据", "regime_coverage", "/api/regime/coverage"),
        _case("数据", "data_workload", "/api/data/workload"),
        _case("数据", "minute_refresh_status", "/api/settings/minute-refresh/status"),
        _case("数据", "ext_data_list", "/api/ext-data"),
        _case("数据", "financial_status", "/api/financials/status"),
        _case("数据", "pipeline_jobs_data", "/api/pipeline/jobs", {"limit": "15"}),
    ]


def _analysis_cases(start: str, end: str) -> list[PageCase]:
    """回测、因子、市场环境与分析页接口。"""
    return [
        _case("回测", "status", "/api/backtest/status"),
        _case("回测", "candidates", "/api/backtest/candidates"),
        _case("回测", "factor_columns", "/api/backtest/factor/columns"),
        _case("回测", "factors", "/api/factors", {"asset_type": "stock"}),
        _case("回测", "guardian_overview", "/api/backtest/guardian/overview"),
        _case("回测", "guardian_runs", "/api/backtest/guardian/runs", {"limit": "10"}),
        _case("回测", "guardian_config", "/api/backtest/guardian/config"),
        _case("回测", "lab_report", "/api/backtest/lab/report"),
        _case("回测", "mining_availability", "/api/backtest/mining/availability"),
        _case("回测", "mining_lock", "/api/backtest/mining/lock"),
        _case("回测", "mining_runs", "/api/backtest/mining/runs"),
        _case("回测", "mining_config", "/api/backtest/mining/config"),
        _case("回测", "position_policies", "/api/backtest/position-policies"),
        _case("回测", "position_templates", "/api/backtest/position-policies/templates"),
        _case("因子", "factors_all", "/api/factors"),
        _case("因子", "definitions", "/api/definitions"),
        _case("因子", "definition_resources", "/api/definitions/resources"),
        _case("市场环境", "regime_history", "/api/regime/history", {"start": start, "end": end}),
        _case("市场环境", "regime_latest", "/api/regime/latest"),
        _case("市场环境", "regime_states", "/api/regime/states", {"days": "7"}),
        _case("市场环境", "regime_phases", "/api/regime/phases", {"start": start, "end": end}),
        _case("市场环境", "regime_mainline", "/api/regime/mainline", {"start": start, "end": end}),
    ]


def _extension_cases(start: str, end: str) -> list[PageCase]:
    """扩展页与设置页接口，跳过敏感 token 与非确定性 AI 流。"""
    return [
        _case("设置", "data_sources", "/api/settings/data-sources"),
        _case("设置", "monitor_rules", "/api/monitor-rules"),
        _case("设置", "monitor_options", "/api/monitor-rules/options"),
        _case("设置", "lots", "/api/lots"),
        _case("设置", "ext_pages", "/api/ext-pages"),
        _case("设置", "ext_namespaces", "/api/ext-pages/import-source/namespaces"),
        _case("设置", "analysis_menus", "/api/analysis-menus"),
        _case("设置", "strategy_pool", "/api/strategy-pool"),
        _case("设置", "extension_packages", "/api/extension-packages"),
        _case("设置", "sound_packs", "/api/settings/sound-packs"),
        _case("设置", "endpoints", "/api/settings/endpoints"),
        _case("设置", "upgrade_guard", "/api/upgrade-guard/status"),
        _case("设置", "market_recap_prompt", "/api/settings/preferences/market-recap-prompt"),
        _case("开发者", "kline_minute", "/api/kline/minute", {"symbol": SYMBOL, "date": end}),
        _case("模拟盘", "paper_accounts", "/api/paper/accounts"),
        _case("模拟盘", "paper_account", "/api/paper/account"),
        _case("模拟盘", "paper_positions", "/api/paper/positions"),
        _case("模拟盘", "paper_compare", "/api/paper/compare"),
        _case("预警", "alerts_500", "/api/alerts", {"days": "7", "limit": "500"}),
        _case("持仓提醒", "lots_page", "/api/lots"),
        _case("信号", "custom_signals", "/api/custom-signals"),
        _case("信号", "signal_options", "/api/custom-signals/options"),
        _case("财务", "metrics", "/api/financials/metrics", {"symbol": SYMBOL}),
        _case("财务", "income", "/api/financials/income", {"symbol": SYMBOL}),
        _case("财务", "balance_sheet", "/api/financials/balance-sheet", {"symbol": SYMBOL}),
        _case("财务", "cash_flow", "/api/financials/cash-flow", {"symbol": SYMBOL}),
        _case("财务", "shares", "/api/financials/shares", {"symbol": SYMBOL}),
        _case("财务", "reports", "/api/financials/reports"),
        _case(
            "个股分析",
            "stock_levels",
            "/api/stock-analysis/levels",
            {"symbol": SYMBOL, "days": "30"},
        ),
        _case("个股分析", "stock_reports", "/api/stock-analysis/reports"),
        _case("ext-data", "schema_all", "/api/ext-data/schema-all"),
    ]


def build_cases(start: str, end: str) -> list[PageCase]:
    """汇总所有适合自动对照的页面级只读接口。"""
    return [
        *_shell_cases(),
        *_market_cases(start, end),
        *_kline_cases(start, end),
        *_analysis_cases(start, end),
        *_extension_cases(start, end),
    ]


def fetch_json(base: str, path: str, params: dict[str, str], cookie: str | None) -> tuple[int, Any]:
    """请求一个接口；dev 带会话 Cookie，origin 不带认证。"""
    url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
    request = urllib.request.Request(url)
    if cookie:
        request.add_header("Cookie", cookie)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, body


def run_case(case: PageCase, cookie: str) -> dict[str, Any]:
    """对单个页面接口执行双栈请求与结构化比较。"""
    origin_status, origin_body = fetch_json(ORIGIN_BASE, case.path, case.params, None)
    dev_status, dev_body = fetch_json(DEV_BASE, case.path, case.params, cookie)
    result: dict[str, Any] = {
        "page": case.page,
        "case": case.name,
        "path": case.path,
        "params": case.params,
        "origin_status": origin_status,
        "dev_status": dev_status,
    }
    if isinstance(dev_body, dict) and dev_body.get("code") == "daily_integrity":
        result.update(verdict="BLOCKED", reason=str(dev_body.get("detail", "")))
        return result
    if origin_status != dev_status:
        result.update(verdict="DIFF", reason=f"状态码不同: {origin_status} vs {dev_status}")
        return result
    differences = diff(normalize(origin_body), normalize(dev_body))
    if differences:
        result.update(verdict="DIFF", reason="; ".join(differences))
    else:
        result.update(verdict="OK", reason="")
    return result


def summarize(results: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """按页面汇总 OK/DIFF/BLOCKED 数量。"""
    grouped: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "ok": 0, "diff": 0, "blocked": 0}
    )
    for item in results:
        row = grouped[item["page"]]
        row["total"] += 1
        key = item["verdict"].lower()
        row[key] = row.get(key, 0) + 1
    return dict(grouped)


def print_report(report: dict[str, Any]) -> None:
    """把报告按页面逐条打印，便于用户逐页核对。"""
    print(
        f"全页面对照: {report['summary']['ok']}/{report['summary']['total']} 一致, "
        f"{report['summary']['diff']} 差异, {report['summary']['blocked']} 阻塞"
    )
    by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in report["cases"]:
        by_page[item["page"]].append(item)
    for page, items in by_page.items():
        stats = report["by_page"][page]
        print(f"\n[{page}] {stats['ok']}/{stats['total']} 一致")
        for item in items:
            marker = "OK" if item["verdict"] == "OK" else item["verdict"]
            detail = f" - {item['reason']}" if item["reason"] else ""
            print(f"  {marker:7} {item['case']}{detail}")


def main() -> int:
    """执行全页面双栈对照，输出 JSON 报告并按页面打印结果。"""
    parser = argparse.ArgumentParser(description="全页面双栈接口对照")
    parser.add_argument("--cookie", required=True, help="dev 登录会话 Cookie，如 tf_session=...")
    parser.add_argument("--start", default=DEFAULT_START, help="对比起始交易日")
    parser.add_argument("--end", default=DEFAULT_END, help="对比结束交易日")
    parser.add_argument("--out", help="报告输出路径（JSON）")
    args = parser.parse_args()

    for base, cookie in ((ORIGIN_BASE, None), (DEV_BASE, args.cookie)):
        status, _ = fetch_json(base, "/api/auth/status", {}, cookie)
        if status >= 400:
            print(f"服务不可达或未授权: {base} -> {status}", file=sys.stderr)
            return 2

    results = [run_case(case, args.cookie) for case in build_cases(args.start, args.end)]
    n_diff = sum(item["verdict"] == "DIFF" for item in results)
    n_blocked = sum(item["verdict"] == "BLOCKED" for item in results)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window": {"start": args.start, "end": args.end},
        "summary": {
            "total": len(results),
            "ok": len(results) - n_diff - n_blocked,
            "diff": n_diff,
            "blocked": n_blocked,
        },
        "by_page": summarize(results),
        "cases": results,
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        output = Path(args.out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
        print(f"报告已写入: {output}")
    print_report(report)
    return 1 if n_diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
