from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_float,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)


class MarketDataQualityStatus(StrEnum):
    MISSING = "MISSING"
    FRESH = "FRESH"
    STALE = "STALE"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


@dataclass(frozen=True, kw_only=True)
class MarketTickSnapshot:
    code: str
    name: str
    price: int
    change_rate: float
    cumulative_volume: int
    cumulative_trade_value: float
    execution_strength: float
    best_bid: int
    best_ask: int
    spread_ticks: int
    day_high: int
    day_low: int
    trade_time: datetime
    event_ts: datetime
    received_at: datetime
    source: str
    event_id: str
    quality_status: MarketDataQualityStatus
    tick_age_sec: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "price", parse_int(self.price, "price", min_value=1))
        object.__setattr__(self, "change_rate", parse_float(self.change_rate, "change_rate"))
        object.__setattr__(
            self,
            "cumulative_volume",
            parse_int(self.cumulative_volume, "cumulative_volume", min_value=0),
        )
        object.__setattr__(
            self,
            "cumulative_trade_value",
            parse_float(
                self.cumulative_trade_value,
                "cumulative_trade_value",
                min_value=0.0,
            ),
        )
        object.__setattr__(
            self,
            "execution_strength",
            parse_float(self.execution_strength, "execution_strength", min_value=0.0),
        )
        object.__setattr__(self, "best_bid", parse_int(self.best_bid, "best_bid", min_value=0))
        object.__setattr__(self, "best_ask", parse_int(self.best_ask, "best_ask", min_value=0))
        object.__setattr__(
            self,
            "spread_ticks",
            parse_int(self.spread_ticks, "spread_ticks", min_value=0),
        )
        object.__setattr__(self, "day_high", parse_int(self.day_high, "day_high", min_value=1))
        object.__setattr__(self, "day_low", parse_int(self.day_low, "day_low", min_value=1))
        object.__setattr__(self, "trade_time", parse_timestamp(self.trade_time, "trade_time"))
        object.__setattr__(self, "event_ts", parse_timestamp(self.event_ts, "event_ts"))
        object.__setattr__(self, "received_at", parse_timestamp(self.received_at, "received_at"))
        object.__setattr__(self, "source", require_non_empty_str(self.source, "source"))
        object.__setattr__(self, "event_id", require_non_empty_str(self.event_id, "event_id"))
        object.__setattr__(
            self,
            "quality_status",
            parse_str_enum(self.quality_status, MarketDataQualityStatus, "quality_status"),
        )
        if self.tick_age_sec is not None:
            object.__setattr__(
                self,
                "tick_age_sec",
                parse_float(self.tick_age_sec, "tick_age_sec", min_value=0.0),
            )

    def to_dict(self) -> dict[str, Any]:
        data = {
            "code": self.code,
            "name": self.name,
            "price": self.price,
            "change_rate": self.change_rate,
            "cumulative_volume": self.cumulative_volume,
            "cumulative_trade_value": self.cumulative_trade_value,
            "execution_strength": self.execution_strength,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_ticks": self.spread_ticks,
            "day_high": self.day_high,
            "day_low": self.day_low,
            "trade_time": datetime_to_wire(self.trade_time),
            "event_ts": datetime_to_wire(self.event_ts),
            "received_at": datetime_to_wire(self.received_at),
            "source": self.source,
            "event_id": self.event_id,
            "quality_status": self.quality_status.value,
        }
        if self.tick_age_sec is not None:
            data["tick_age_sec"] = self.tick_age_sec
        return data


@dataclass(frozen=True, kw_only=True)
class MarketTickSample:
    event_id: str
    code: str
    price: int
    cumulative_volume: int
    cumulative_trade_value: float
    volume_delta: int
    trade_value_delta: float
    execution_strength: float
    event_ts: datetime
    received_at: datetime
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "code": self.code,
            "price": self.price,
            "cumulative_volume": self.cumulative_volume,
            "cumulative_trade_value": self.cumulative_trade_value,
            "volume_delta": self.volume_delta,
            "trade_value_delta": self.trade_value_delta,
            "execution_strength": self.execution_strength,
            "event_ts": datetime_to_wire(parse_timestamp(self.event_ts, "event_ts")),
            "received_at": datetime_to_wire(parse_timestamp(self.received_at, "received_at")),
            "source": self.source,
        }


@dataclass(frozen=True, kw_only=True)
class MarketMinuteBar:
    code: str
    interval_sec: int
    bucket_start: datetime
    open: int
    high: int
    low: int
    close: int
    volume_delta: int
    trade_value_delta: float
    tick_count: int
    vwap: float | None
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "interval_sec": self.interval_sec,
            "bucket_start": datetime_to_wire(parse_timestamp(self.bucket_start, "bucket_start")),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume_delta": self.volume_delta,
            "trade_value_delta": self.trade_value_delta,
            "tick_count": self.tick_count,
            "vwap": self.vwap,
            "updated_at": datetime_to_wire(parse_timestamp(self.updated_at, "updated_at")),
        }


@dataclass(frozen=True, kw_only=True)
class MarketDataReadiness:
    code: str
    quality_status: MarketDataQualityStatus
    has_latest_tick: bool
    tick_age_sec: float | None
    has_1m_bar: bool
    has_3m_bar: bool
    has_5m_bar: bool
    vwap_ready: bool
    reason_codes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "quality_status": self.quality_status.value,
            "has_latest_tick": self.has_latest_tick,
            "tick_age_sec": self.tick_age_sec,
            "has_1m_bar": self.has_1m_bar,
            "has_3m_bar": self.has_3m_bar,
            "has_5m_bar": self.has_5m_bar,
            "vwap_ready": self.vwap_ready,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class ConditionSignalSnapshot:
    condition_id: str
    condition_name: str
    code: str
    name: str
    action: str
    price: int | None
    event_ts: datetime
    received_at: datetime
    source: str
    event_id: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "condition_name": self.condition_name,
            "code": self.code,
            "name": self.name,
            "action": self.action,
            "price": self.price,
            "event_ts": datetime_to_wire(parse_timestamp(self.event_ts, "event_ts")),
            "received_at": datetime_to_wire(parse_timestamp(self.received_at, "received_at")),
            "source": self.source,
            "event_id": self.event_id,
            "metadata": normalize_payload(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class TrMarketSnapshot:
    request_id: str
    tr_code: str
    request_name: str
    code: str | None
    row_json: Mapping[str, Any]
    event_id: str
    event_ts: datetime
    received_at: datetime
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "tr_code": self.tr_code,
            "request_name": self.request_name,
            "code": self.code,
            "row": normalize_payload(self.row_json),
            "event_id": self.event_id,
            "event_ts": datetime_to_wire(parse_timestamp(self.event_ts, "event_ts")),
            "received_at": datetime_to_wire(parse_timestamp(self.received_at, "received_at")),
            "source": self.source,
        }


def row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
