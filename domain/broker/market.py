from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    parse_float,
    parse_int,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)


@dataclass(frozen=True, kw_only=True)
class BrokerPriceTick:
    code: str
    name: str
    price: int
    change_rate: float
    volume: int
    trade_value: int
    execution_strength: float
    best_bid: int
    best_ask: int
    spread_ticks: int
    day_high: int
    day_low: int
    trade_time: datetime
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "price", parse_int(self.price, "price", min_value=1))
        object.__setattr__(self, "change_rate", parse_float(self.change_rate, "change_rate"))
        object.__setattr__(self, "volume", parse_int(self.volume, "volume", min_value=0))
        object.__setattr__(
            self,
            "trade_value",
            parse_int(self.trade_value, "trade_value", min_value=0),
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
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

        if self.best_bid and self.best_ask and self.best_ask < self.best_bid:
            raise ValueError("best_ask must be >= best_bid when both are present")
        if self.day_high < self.day_low:
            raise ValueError("day_high must be >= day_low")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "price": self.price,
            "change_rate": self.change_rate,
            "volume": self.volume,
            "trade_value": self.trade_value,
            "execution_strength": self.execution_strength,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_ticks": self.spread_ticks,
            "day_high": self.day_high,
            "day_low": self.day_low,
            "trade_time": datetime_to_wire(self.trade_time),
            "ts": datetime_to_wire(self.ts),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerPriceTick:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "code",
                "name",
                "price",
                "change_rate",
                "volume",
                "trade_value",
                "execution_strength",
                "best_bid",
                "best_ask",
                "spread_ticks",
                "day_high",
                "day_low",
                "trade_time",
            ),
            cls.__name__,
        )
        return cls(
            code=mapping["code"],
            name=mapping["name"],
            price=mapping["price"],
            change_rate=mapping["change_rate"],
            volume=mapping["volume"],
            trade_value=mapping["trade_value"],
            execution_strength=mapping["execution_strength"],
            best_bid=mapping["best_bid"],
            best_ask=mapping["best_ask"],
            spread_ticks=mapping["spread_ticks"],
            day_high=mapping["day_high"],
            day_low=mapping["day_low"],
            trade_time=parse_timestamp(mapping["trade_time"], "trade_time"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )
