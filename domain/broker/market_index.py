from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_float,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
)

DEFAULT_ALLOWED_INDEX_CODES: frozenset[str] = frozenset({"KOSPI", "KOSDAQ", "KOSPI200"})


@dataclass(frozen=True, kw_only=True)
class BrokerMarketIndexTick:
    index_code: str
    index_name: str
    price: float
    change_rate: float
    change_value: float
    trade_time: datetime
    ts: datetime = field(default_factory=utc_now)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    allowed_index_codes: frozenset[str] = DEFAULT_ALLOWED_INDEX_CODES

    def __post_init__(self) -> None:
        index_code = require_non_empty_str(self.index_code, "index_code").upper()
        allowed = {code.upper() for code in self.allowed_index_codes}
        if index_code not in allowed:
            allowed_text = ", ".join(sorted(allowed))
            raise ValueError(f"index_code must be one of: {allowed_text}")
        object.__setattr__(self, "index_code", index_code)
        object.__setattr__(
            self,
            "index_name",
            require_non_empty_str(self.index_name, "index_name"),
        )
        object.__setattr__(self, "price", parse_float(self.price, "price", min_value=0.0))
        if self.price <= 0:
            raise ValueError("price must be > 0")
        object.__setattr__(self, "change_rate", parse_float(self.change_rate, "change_rate"))
        object.__setattr__(self, "change_value", parse_float(self.change_value, "change_value"))
        object.__setattr__(self, "trade_time", parse_timestamp(self.trade_time, "trade_time"))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))
        object.__setattr__(self, "metadata", normalize_payload(self.metadata))
        object.__setattr__(self, "allowed_index_codes", frozenset(allowed))

    def to_dict(self) -> dict[str, Any]:
        return {
            "index_code": self.index_code,
            "index_name": self.index_name,
            "price": self.price,
            "change_rate": self.change_rate,
            "change_value": self.change_value,
            "trade_time": datetime_to_wire(self.trade_time),
            "ts": datetime_to_wire(self.ts),
            "metadata": normalize_payload(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerMarketIndexTick:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "index_code",
                "index_name",
                "price",
                "change_rate",
                "change_value",
                "trade_time",
            ),
            cls.__name__,
        )
        return cls(
            index_code=mapping["index_code"],
            index_name=mapping["index_name"],
            price=mapping["price"],
            change_rate=mapping["change_rate"],
            change_value=mapping["change_value"],
            trade_time=parse_timestamp(mapping["trade_time"], "trade_time"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
            metadata=mapping.get("metadata", {}),
        )
