from __future__ import annotations

from datetime import datetime, timedelta

from domain.broker.utils import parse_timestamp


def bucket_start_for(timestamp: datetime | str, interval_sec: int) -> datetime:
    if interval_sec <= 0:
        raise ValueError("interval_sec must be positive")
    parsed = parse_timestamp(timestamp, "timestamp")
    epoch = int(parsed.timestamp())
    bucket_epoch = epoch - (epoch % interval_sec)
    return datetime.fromtimestamp(bucket_epoch, tz=parsed.tzinfo)


def calculate_vwap(
    *,
    cumulative_trade_value: float,
    cumulative_volume: int,
    bar_trade_value_delta: float,
    bar_volume_delta: int,
) -> float | None:
    if cumulative_volume > 0 and cumulative_trade_value > 0:
        return cumulative_trade_value / cumulative_volume
    if bar_volume_delta > 0 and bar_trade_value_delta > 0:
        return bar_trade_value_delta / bar_volume_delta
    return None


def normalize_interval_list(intervals: list[int]) -> tuple[int, ...]:
    normalized = tuple(sorted(set(intervals)))
    if not normalized:
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must contain at least one interval")
    for interval in normalized:
        if interval <= 0:
            raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC values must be positive")
        if interval % 60 != 0:
            raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC values must be minute-aligned")
    return normalized


def add_seconds(timestamp: datetime | str, seconds: int) -> datetime:
    return parse_timestamp(timestamp, "timestamp") + timedelta(seconds=seconds)
