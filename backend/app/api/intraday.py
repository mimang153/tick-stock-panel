"""行情状态 / SSE 推送 API。

盘中策略相关端点已迁移至策略页面，此处仅保留全局行情基础设施。
SSE 推送四种事件 (使用标准 SSE event 字段):
  - quotes_updated: 行情数据刷新，前端 invalidate 对应 query
  - strategy_results_updated: 策略监控已写入最新结果，前端刷新策略个股列表
  - strategy_alert: 策略监控/告警触发，前端弹通知
  - depth_updated: 五档盘口修正完成，前端刷新连板梯队/看板封单数据
"""

from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

router = APIRouter(prefix="/api/intraday", tags=["quotes"])


class QuoteSubscription(BaseModel):
    symbols: list[str] = Field(default_factory=list, max_length=64)


@router.put("/subscriptions/{client_id}")
def subscribe_quotes(
    request: Request,
    body: QuoteSubscription,
    client_id: str = Path(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9_-]+$"),
):
    """Renew a detail lease. The shared worker performs all upstream fetching."""
    from app.services.quote_demand import LEASE_SECONDS

    qs = _get_quote_service(request)
    if qs is None:
        raise HTTPException(503, "行情服务暂不可用，请稍后重试")
    try:
        qs.subscribe_quotes(client_id, body.symbols)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"lease_seconds": LEASE_SECONDS}


@router.delete("/subscriptions/{client_id}")
def unsubscribe_quotes(
    request: Request,
    client_id: str = Path(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9_-]+$"),
):
    qs = _get_quote_service(request)
    if qs is not None:
        qs.unsubscribe_quotes(client_id)
    return {"status": "ok"}


def _get_quote_service(request: Request):
    """获取全局 QuoteService。"""
    return getattr(request.app.state, "quote_service", None)


def _fallback_index_quotes_from_daily(
    request: Request, symbols: list[str] | None = None
) -> list[dict]:
    """实时指数缓存为空时，从 CK 指数日 K 取最近收盘价作为兜底。"""
    try:
        from app.storage.runtime import get_storage

        runtime = get_storage()
        if runtime is None:
            return []
        df = runtime.store.read_daily(
            "kline_index_daily",
            symbols=symbols,
            columns=["symbol", "date", "close"],
        )
        if df.is_empty():
            return []
        # 每个 symbol 取最近两日 (指数数据量小，Python 侧聚合即可)
        rows = []
        for (symbol,), group in df.sort(["symbol", "date"], descending=[False, True]).group_by(
            "symbol", maintain_order=True
        ):
            group = group.sort("date", descending=True)
            dates = group["date"].to_list()
            closes = group["close"].to_list()
            last_price = closes[0] if closes else None
            prev_close = closes[1] if len(closes) > 1 else None
            rows.append((symbol, dates[0] if dates else None, last_price, prev_close))
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return []

    out: list[dict] = []
    for symbol, dt, last_price, prev_close in rows:
        change_amount = None
        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_amount = float(last_price) - float(prev_close)
            change_pct = change_amount / float(prev_close) * 100
        out.append(
            {
                "symbol": symbol,
                "name": None,
                "date": str(dt) if dt else None,
                "last_price": float(last_price) if last_price is not None else None,
                "close": float(last_price) if last_price is not None else None,
                "prev_close": float(prev_close) if prev_close is not None else None,
                "change_amount": change_amount,
                "change_pct": change_pct,
                "source": "index_daily",
            }
        )
    return out


@router.get("/status")
def status(request: Request):
    """行情状态 (来自全局 QuoteService)。"""
    qs = _get_quote_service(request)
    if qs:
        return qs.status()
    return {
        "enabled": False,
        "running": False,
        "symbol_count": 0,
        "index_symbol_count": 0,
        "quote_age_ms": None,
        "is_trading_hours": False,
        "last_fetch_ms": None,
    }


@router.get("/indices")
def index_quotes(
    request: Request,
    symbols: str | None = Query(None, description="逗号分隔的指数 symbol 列表"),
):
    """返回实时指数行情缓存，不触发 TickFlow 请求。"""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None
    qs = _get_quote_service(request)
    if not qs:
        rows = _fallback_index_quotes_from_daily(request, symbol_list)
        return {"rows": rows, "count": len(rows), "source": "index_daily"}
    df = qs.get_index_quotes(symbol_list)
    rows = df.to_dicts() if not df.is_empty() else []
    if not rows:
        rows = _fallback_index_quotes_from_daily(request, symbol_list)
        return {"rows": rows, "count": len(rows), "source": "index_daily"}
    return {"rows": rows, "count": len(rows), "source": "realtime"}


def _sse_alert_events(alerts: list) -> list[dict]:
    """告警分片推送, 避免单条 SSE 过大。"""
    return [
        {
            "event": "strategy_alert",
            "data": json.dumps(
                {"ts": int(time.time() * 1000), "alerts": alerts[i : i + 20]},
                ensure_ascii=False,
            ),
        }
        for i in range(0, len(alerts), 20)
    ]


def _sse_quote_events(qs, data: dict) -> list[dict]:
    """行情/策略/五档等状态通知, 顺序沿用原 SSE 推送顺序。"""
    events: list[dict] = []
    if data["quote_updated"]:
        events.append(
            {
                "event": "quotes_updated",
                "data": json.dumps(
                    {
                        "ts": int(time.time() * 1000),
                        "symbol_count": qs._symbol_count,
                        "revision": qs.stream_versions()["quote"],
                    }
                ),
            }
        )
    if "daily_integrity_updated" in data:
        events.append(
            {
                "event": "daily_integrity_updated",
                "data": json.dumps({"data_changed": data["daily_integrity_updated"]}),
            }
        )
    if data.get("data_updated"):
        events.append(
            {
                "event": "data_updated",
                "data": json.dumps({"versions": qs.data_versions()}),
            }
        )
    if data["strategy_results_updated"]:
        events.append(
            {
                "event": "strategy_results_updated",
                "data": json.dumps(
                    {
                        "ts": int(time.time() * 1000),
                        "revision": qs.stream_versions()["strategy"],
                    }
                ),
            }
        )
    if data["depth_updated"]:
        events.append(
            {
                "event": "depth_updated",
                "data": json.dumps({"ts": int(time.time() * 1000)}),
            }
        )
    return events


async def _quote_event_generator(qs):
    """单连接事件循环: 订阅、等待、按固定顺序转发事件。"""
    if qs is None:
        # 无行情服务: 保持连接 (EventSourceResponse 自带 ping), 不推事件
        while True:
            await asyncio.sleep(30)

    sub = qs.subscribe()
    try:
        while True:
            # 等待任一通道有新信号 (5s 超时保持循环, 便于断线时尽快退出)
            await sub.wait_async(5.0)
            data = sub.pop()

            for event in _sse_alert_events(data["alerts"]):
                yield event

            # 复盘进度事件已是 recap_market_stream 产出的 JSON 字符串, 逐条转发
            for evt_json in data["reviews"]:
                yield {"event": "review_progress", "data": evt_json}

            for event in _sse_quote_events(qs, data):
                yield event
    finally:
        qs.unsubscribe(sub)


@router.get("/stream")
async def quote_stream(request: Request):
    """SSE 端点: 行情更新 + 告警推送 + 五档修正 + 复盘进度。

    使用 sse-starlette EventSourceResponse:
    - 标准 SSE event 字段，前端按 event name 监听
    - 内置断线检测，客户端断开立即终止 generator
    - 内置 ping 心跳，保持连接活跃

    每个连接注册一个独立订阅者 (QuoteSubscriber: 独立事件 + 独立队列),
    事件由 QuoteService 广播 — 多客户端 (多标签页/设备) 各自收到全量事件。
    """
    return EventSourceResponse(_quote_event_generator(_get_quote_service(request)))


@router.get("/data-versions")
def data_versions(request: Request):
    """补齐数据版本的只读出口，供重连/漏事件校准，不创建取数任务。"""
    qs = _get_quote_service(request)
    return {"versions": qs.data_versions() if qs else {}}


@router.post("/refresh")
def refresh_quotes(request: Request):
    """手动刷新一次行情数据。"""
    qs = _get_quote_service(request)
    if qs:
        return qs.refresh()
    return {"error": "QuoteService not available"}
