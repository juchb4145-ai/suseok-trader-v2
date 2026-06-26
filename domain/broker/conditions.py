from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)


class ConditionEventAction(StrEnum):
    ENTER = "ENTER"
    EXIT = "EXIT"


@dataclass(frozen=True, kw_only=True)
class BrokerConditionEvent:
    condition_id: str
    condition_name: str
    code: str
    name: str
    action: ConditionEventAction
    price: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "condition_id",
            require_non_empty_str(self.condition_id, "condition_id"),
        )
        object.__setattr__(
            self,
            "condition_name",
            require_non_empty_str(self.condition_name, "condition_name"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "action",
            parse_str_enum(self.action, ConditionEventAction, "action"),
        )
        if self.price is not None:
            object.__setattr__(self, "price", parse_int(self.price, "price", min_value=1))
        object.__setattr__(self, "metadata", normalize_payload(self.metadata))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "condition_id": self.condition_id,
            "condition_name": self.condition_name,
            "code": self.code,
            "name": self.name,
            "action": self.action.value,
            "metadata": normalize_payload(self.metadata),
            "ts": datetime_to_wire(self.ts),
        }
        if self.price is not None:
            data["price"] = self.price
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerConditionEvent:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            ("condition_id", "condition_name", "code", "name", "action"),
            cls.__name__,
        )
        return cls(
            condition_id=mapping["condition_id"],
            condition_name=mapping["condition_name"],
            code=mapping["code"],
            name=mapping["name"],
            action=parse_str_enum(mapping["action"], ConditionEventAction, "action"),
            price=mapping.get("price"),
            metadata=normalize_payload(mapping.get("metadata", {})),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )
