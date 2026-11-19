"""Per-symbol quote provenance for API readers; never fetch or mutate market data.

Purely derived from the caller's row plus injected clock/trading state.
"""

from __future__ import annotations

import math
from datetime import datetime, time

from app.market_time import CN_TZ, cn_now
from app.services import trading_day


def _parse_quote_stamp(row: dict) -> tuple[float | None, datetime | None, str | None]:
    """解析行情时间戳; 非法值按缺失处理, 并返回回退后的数据日期。"""
    data_date = str(row.get("date") or "")[:10] or None
    raw_stamp = row.get("quote_ts", row.get("timestamp"))
    if raw_stamp is None:
        return None, None, data_date
    try:
        stamp = float(raw_stamp)
        if not math.isfinite(stamp) or stamp <= 0:
            raise ValueError
        quote_dt = datetime.fromtimestamp(stamp / 1000, CN_TZ)
        return stamp, quote_dt, quote_dt.date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None, None, data_date


def _has_valid_price(row: dict) -> bool:
    """行情价须为有限正数; 缺失或异常都视为无可用行情。"""
    raw = row.get("close", row.get("last_price"))
    if raw is None:
        return False
    try:
        price = float(raw)
    except (TypeError, ValueError):
        return False
    return math.isfinite(price) and price > 0


class QuoteFreshness:
    """按数据日期/时间戳判定逐标的行情新鲜度, 只读不取数。"""

    def __init__(
        self,
        *,
        now: datetime | None = None,
        trading: bool | None = None,
        enabled: bool = True,
        paused: bool = False,
        interval: float = 6,
    ) -> None:
        self.now = (now or cn_now()).astimezone(CN_TZ)
        self.trading = trading
        self.enabled, self.paused = enabled, paused
        self.max_age_ms = max(90_000, interval * 3_000)

    @classmethod
    def for_request(cls, request):
        now = cn_now()
        qs = getattr(request.app.state, "quote_service", None)
        return cls(
            now=now,
            trading=trading_day.cached_verdict(now),
            enabled=bool(qs and getattr(qs, "_enabled", False)),
            paused=bool(qs and getattr(qs, "_paused", False)),
            interval=getattr(qs, "_interval", 6) if qs else 6,
        )

    def describe(self, row: dict | None, *, historical: bool = False) -> dict:
        """返回行情状态与提示文案: live/delayed/stale/historical/paused/disabled/unknown/missing。"""
        row = row or {}
        stamp, quote_dt, data_date = _parse_quote_stamp(row)
        out = {
            "data_date": data_date,
            "quote_ts": stamp,
            "checked_at": int(self.now.timestamp() * 1000),
            "max_age_ms": self.max_age_ms,
        }

        def result(state, message):
            return {**out, "state": state, "message": message}

        if not _has_valid_price(row):
            return result("missing", "暂无可用行情，请检查数据源或稍后重试")
        if (
            quote_dt
            and quote_dt > self.now.replace(microsecond=0)
            and (quote_dt - self.now).total_seconds() > 60
        ):
            return result("unknown", "行情时间异常，请检查数据源时间")
        today, clock = self.now.date(), self.now.time()
        trading = self.trading is not False and self.now.weekday() < 5
        if historical or not trading or clock < time(9, 30):
            return result("historical", "历史行情，按实际数据日期显示")
        if data_date is None or data_date < today.isoformat():
            return result(
                "stale", "行情已过期，保留最近有效数据；请检查连接及所选源对该标的的支持范围"
            )
        if data_date > today.isoformat():
            return result("unknown", "行情日期异常，请检查数据源")
        if row.get("prepared_daily"):
            return result("historical", "日K补齐数据，尚未取得实时行情")
        if self.paused:
            return result("paused", "实时行情暂时暂停，保留最近有效数据")
        if not self.enabled:
            return result("disabled", "实时行情未开启，保留最近有效数据")
        if stamp is None:
            return result("unknown", "当日数据缺少行情时间，无法确认实时性")
        active = clock < time(11, 30) or time(13) <= clock < time(15)
        reference = (
            self.now
            if active
            else datetime.combine(today, time(11, 30) if clock < time(13) else time(15), CN_TZ)
        )
        if reference.timestamp() * 1000 - stamp > self.max_age_ms:
            return result("delayed", "行情更新延迟，保留最近有效数据，请检查数据源与连接")
        return (
            result("live", "实时行情")
            if active
            else result("historical", "休盘行情，按最后有效时间显示")
        )
