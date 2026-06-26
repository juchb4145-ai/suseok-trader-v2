from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    optional_non_empty_str,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
)


@dataclass(frozen=True, kw_only=True)
class EventEnvelope:
    event_type: str
    source: str
    payload: Mapping[str, Any]
    event_id: str = field(default_factory=lambda: new_message_id("evt"))
    ts: datetime = field(default_factory=utc_now)
    command_id: str | None = None
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", require_non_empty_str(self.event_id, "event_id"))
        object.__setattr__(self, "event_type", require_non_empty_str(self.event_type, "event_type"))
        object.__setattr__(self, "source", require_non_empty_str(self.source, "source"))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))
        object.__setattr__(self, "payload", normalize_payload(self.payload))
        object.__setattr__(
            self,
            "command_id",
            optional_non_empty_str(self.command_id, "command_id"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            optional_non_empty_str(self.idempotency_key, "idempotency_key"),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "ts": datetime_to_wire(self.ts),
            "payload": normalize_payload(self.payload),
        }
        if self.command_id is not None:
            data["command_id"] = self.command_id
        if self.idempotency_key is not None:
            data["idempotency_key"] = self.idempotency_key
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventEnvelope:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("event_type", "source", "payload"), cls.__name__)
        return cls(
            event_id=mapping.get("event_id", new_message_id("evt")),
            event_type=mapping["event_type"],
            source=mapping["source"],
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
            payload=normalize_payload(mapping["payload"]),
            command_id=mapping.get("command_id"),
            idempotency_key=mapping.get("idempotency_key"),
        )


class GatewayEvent(EventEnvelope):
    """Broker observation delivered from Gateway to Core."""
