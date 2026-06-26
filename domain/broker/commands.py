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
class CommandEnvelope:
    command_type: str
    source: str
    payload: Mapping[str, Any]
    command_id: str = field(default_factory=lambda: new_message_id("cmd"))
    ts: datetime = field(default_factory=utc_now)
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "command_id",
            require_non_empty_str(self.command_id, "command_id"),
        )
        object.__setattr__(
            self,
            "command_type",
            require_non_empty_str(self.command_type, "command_type"),
        )
        object.__setattr__(self, "source", require_non_empty_str(self.source, "source"))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))
        object.__setattr__(self, "payload", normalize_payload(self.payload))
        object.__setattr__(
            self,
            "idempotency_key",
            optional_non_empty_str(self.idempotency_key, "idempotency_key"),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "command_id": self.command_id,
            "command_type": self.command_type,
            "source": self.source,
            "ts": datetime_to_wire(self.ts),
            "payload": normalize_payload(self.payload),
        }
        if self.idempotency_key is not None:
            data["idempotency_key"] = self.idempotency_key
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CommandEnvelope:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("command_type", "source", "payload"), cls.__name__)
        return cls(
            command_id=mapping.get("command_id", new_message_id("cmd")),
            command_type=mapping["command_type"],
            source=mapping["source"],
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
            payload=normalize_payload(mapping["payload"]),
            idempotency_key=mapping.get("idempotency_key"),
        )


class GatewayCommand(CommandEnvelope):
    """Core instruction delivered to a future Gateway process."""
