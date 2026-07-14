from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    normalize_value,
    optional_non_empty_str,
    parse_bool,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
)


@dataclass(frozen=True, kw_only=True)
class BrokerTrRequest:
    tr_code: str
    request_name: str
    params: Mapping[str, Any]
    request_id: str = field(default_factory=lambda: new_message_id("tr"))
    continuation_key: str | None = None
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", require_non_empty_str(self.request_id, "request_id"))
        object.__setattr__(self, "tr_code", require_non_empty_str(self.tr_code, "tr_code").upper())
        object.__setattr__(
            self,
            "request_name",
            require_non_empty_str(self.request_name, "request_name"),
        )
        object.__setattr__(self, "params", normalize_payload(self.params))
        object.__setattr__(
            self,
            "continuation_key",
            optional_non_empty_str(self.continuation_key, "continuation_key"),
        )
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "request_id": self.request_id,
            "tr_code": self.tr_code,
            "request_name": self.request_name,
            "params": normalize_payload(self.params),
            "ts": datetime_to_wire(self.ts),
        }
        if self.continuation_key is not None:
            data["continuation_key"] = self.continuation_key
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerTrRequest:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("tr_code", "request_name", "params"), cls.__name__)
        return cls(
            request_id=mapping.get("request_id", new_message_id("tr")),
            tr_code=mapping["tr_code"],
            request_name=mapping["request_name"],
            params=normalize_payload(mapping["params"]),
            continuation_key=mapping.get("continuation_key"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )


@dataclass(frozen=True, kw_only=True)
class BrokerTrResponse:
    request_id: str
    tr_code: str
    request_name: str
    success: bool
    rows: list[dict[str, Any]]
    message: str | None = None
    continuation_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", require_non_empty_str(self.request_id, "request_id"))
        object.__setattr__(self, "tr_code", require_non_empty_str(self.tr_code, "tr_code").upper())
        object.__setattr__(
            self,
            "request_name",
            require_non_empty_str(self.request_name, "request_name"),
        )
        object.__setattr__(self, "success", parse_bool(self.success, "success"))
        object.__setattr__(self, "rows", _normalize_rows(self.rows))
        object.__setattr__(self, "message", optional_non_empty_str(self.message, "message"))
        object.__setattr__(
            self,
            "continuation_key",
            optional_non_empty_str(self.continuation_key, "continuation_key"),
        )
        object.__setattr__(self, "metadata", normalize_payload(self.metadata))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "request_id": self.request_id,
            "tr_code": self.tr_code,
            "request_name": self.request_name,
            "success": self.success,
            "rows": _normalize_rows(self.rows),
            "ts": datetime_to_wire(self.ts),
        }
        if self.message is not None:
            data["message"] = self.message
        if self.continuation_key is not None:
            data["continuation_key"] = self.continuation_key
        if self.metadata:
            data["metadata"] = normalize_payload(self.metadata)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerTrResponse:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            ("request_id", "tr_code", "request_name", "success", "rows"),
            cls.__name__,
        )
        return cls(
            request_id=mapping["request_id"],
            tr_code=mapping["tr_code"],
            request_name=mapping["request_name"],
            success=mapping["success"],
            rows=_normalize_rows(mapping["rows"]),
            message=mapping.get("message"),
            continuation_key=mapping.get("continuation_key"),
            metadata=normalize_payload(mapping.get("metadata", {})),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )


def _normalize_rows(rows: object) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise ValueError("rows must be a list of objects")
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized = normalize_value(row)
        if not isinstance(normalized, dict):
            raise ValueError("each row must be an object")
        normalized_rows.append(normalized)
    return normalized_rows
