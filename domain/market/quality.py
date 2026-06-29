from __future__ import annotations

from datetime import datetime

from domain.broker.market import BrokerPriceTick
from domain.broker.utils import parse_timestamp, utc_now
from domain.market.models import MarketDataQualityStatus


def assess_price_tick_quality(tick: BrokerPriceTick) -> MarketDataQualityStatus:
    if tick.price <= 1 or tick.volume < 0 or tick.trade_value < 0:
        return MarketDataQualityStatus.INVALID
    if tick.best_bid and tick.best_ask and tick.best_ask < tick.best_bid:
        return MarketDataQualityStatus.INVALID
    if tick.day_high < tick.day_low:
        return MarketDataQualityStatus.INVALID
    if tick.spread_ticks == 0 and tick.best_bid and tick.best_ask and tick.best_ask > tick.best_bid:
        return MarketDataQualityStatus.DEGRADED
    return MarketDataQualityStatus.FRESH


def tick_age_seconds(event_ts: datetime | str, *, now: datetime | None = None) -> float:
    reference = parse_timestamp(now or utc_now(), "now")
    event_time = parse_timestamp(event_ts, "event_ts")
    return max((reference - event_time).total_seconds(), 0.0)


def freshness_status(
    event_ts: datetime | str | None,
    *,
    stale_sec: int,
    degraded_sec: int,
    now: datetime | None = None,
    base_status: MarketDataQualityStatus = MarketDataQualityStatus.FRESH,
) -> MarketDataQualityStatus:
    if event_ts is None:
        return MarketDataQualityStatus.MISSING
    if base_status is MarketDataQualityStatus.INVALID:
        return MarketDataQualityStatus.INVALID
    if base_status is MarketDataQualityStatus.DEGRADED:
        return MarketDataQualityStatus.DEGRADED

    age = tick_age_seconds(event_ts, now=now)
    if age <= stale_sec:
        return MarketDataQualityStatus.FRESH
    if age <= degraded_sec:
        return MarketDataQualityStatus.STALE
    return MarketDataQualityStatus.DEGRADED
