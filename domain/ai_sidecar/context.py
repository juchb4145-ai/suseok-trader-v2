from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from typing import Any

from domain.ai_sidecar.schemas import AISidecarValidationError
from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import (
    new_message_id,
    normalize_payload,
    normalize_value,
    optional_non_empty_str,
    require_fields,
    require_mapping,
    require_non_empty_str,
)

SCHEMA_VERSION = "ai-sidecar-context.v1"


@dataclass(frozen=True, kw_only=True)
class AISidecarContextSection:
    section_name: str
    source: str
    payload: Mapping[str, Any]
    row_count: int = 0
    truncated: bool = False
    missing: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "section_name",
            require_non_empty_str(self.section_name, "section_name"),
        )
        object.__setattr__(self, "source", require_non_empty_str(self.source, "source"))
        object.__setattr__(self, "payload", normalize_payload(self.payload))
        object.__setattr__(self, "row_count", max(int(self.row_count), 0))
        object.__setattr__(self, "truncated", bool(self.truncated))
        object.__setattr__(self, "missing", bool(self.missing))

    def to_dict(self) -> dict[str, Any]:
        return {
            "section_name": self.section_name,
            "source": self.source,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "missing": self.missing,
            "payload": normalize_payload(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AISidecarContextSection:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("section_name", "source", "payload"), cls.__name__)
        _reject_unknown_fields(mapping, cls)
        return cls(
            section_name=mapping["section_name"],
            source=mapping["source"],
            row_count=int(mapping.get("row_count", 0)),
            truncated=bool(mapping.get("truncated", False)),
            missing=bool(mapping.get("missing", False)),
            payload=mapping["payload"],
        )


@dataclass(frozen=True, kw_only=True)
class AISidecarContextPacket:
    task_type: AISidecarTaskType
    trade_date: str | None
    generated_at: str
    source_sections: Sequence[AISidecarContextSection]
    payload: Mapping[str, Any]
    context_hash: str
    size_chars: int
    max_size_chars: int
    context_id: str = field(default_factory=lambda: new_message_id("ai_ctx"))
    schema_version: str = SCHEMA_VERSION
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    truncated: bool = False
    redaction_applied: bool = False
    order_context_included: bool = False
    missing_sections: Sequence[str] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "context_id", require_non_empty_str(self.context_id, "context_id"))
        object.__setattr__(self, "task_type", parse_task_type(self.task_type))
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )
        object.__setattr__(
            self,
            "trade_date",
            optional_non_empty_str(self.trade_date, "trade_date"),
        )
        object.__setattr__(
            self,
            "related_entity_type",
            optional_non_empty_str(self.related_entity_type, "related_entity_type"),
        )
        object.__setattr__(
            self,
            "related_entity_id",
            optional_non_empty_str(self.related_entity_id, "related_entity_id"),
        )
        object.__setattr__(
            self,
            "generated_at",
            require_non_empty_str(self.generated_at, "generated_at"),
        )
        object.__setattr__(
            self,
            "context_hash",
            require_non_empty_str(self.context_hash, "context_hash"),
        )
        object.__setattr__(self, "size_chars", max(int(self.size_chars), 0))
        object.__setattr__(self, "max_size_chars", max(int(self.max_size_chars), 1))
        object.__setattr__(self, "truncated", bool(self.truncated))
        object.__setattr__(self, "redaction_applied", bool(self.redaction_applied))
        object.__setattr__(self, "order_context_included", bool(self.order_context_included))
        object.__setattr__(self, "payload", normalize_payload(self.payload))
        object.__setattr__(
            self,
            "source_sections",
            tuple(_parse_section(section) for section in self.source_sections),
        )
        object.__setattr__(
            self,
            "missing_sections",
            tuple(_non_empty_str_list(self.missing_sections, "missing_sections")),
        )
        object.__setattr__(
            self,
            "warnings",
            tuple(_non_empty_str_list(self.warnings, "warnings")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_id": self.context_id,
            "task_type": self.task_type.value,
            "schema_version": self.schema_version,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "generated_at": self.generated_at,
            "source_sections": [section.to_dict() for section in self.source_sections],
            "context_hash": self.context_hash,
            "size_chars": self.size_chars,
            "max_size_chars": self.max_size_chars,
            "truncated": self.truncated,
            "redaction_applied": self.redaction_applied,
            "order_context_included": self.order_context_included,
            "missing_sections": list(self.missing_sections),
            "warnings": list(self.warnings),
            "payload": normalize_payload(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AISidecarContextPacket:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "context_id",
                "task_type",
                "schema_version",
                "generated_at",
                "source_sections",
                "context_hash",
                "size_chars",
                "max_size_chars",
                "payload",
            ),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            context_id=mapping["context_id"],
            task_type=mapping["task_type"],
            schema_version=mapping.get("schema_version", SCHEMA_VERSION),
            trade_date=mapping.get("trade_date"),
            related_entity_type=mapping.get("related_entity_type"),
            related_entity_id=mapping.get("related_entity_id"),
            generated_at=mapping["generated_at"],
            source_sections=[
                AISidecarContextSection.from_dict(section)
                for section in mapping.get("source_sections", [])
            ],
            context_hash=mapping["context_hash"],
            size_chars=int(mapping["size_chars"]),
            max_size_chars=int(mapping["max_size_chars"]),
            truncated=bool(mapping.get("truncated", False)),
            redaction_applied=bool(mapping.get("redaction_applied", False)),
            order_context_included=bool(mapping.get("order_context_included", False)),
            missing_sections=mapping.get("missing_sections", []),
            warnings=mapping.get("warnings", []),
            payload=mapping["payload"],
        )


@dataclass(frozen=True, kw_only=True)
class AISidecarContextBuildResult:
    ok: bool
    context_packet: AISidecarContextPacket | None = None
    error_message: str | None = None
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ok", bool(self.ok))
        object.__setattr__(
            self,
            "error_message",
            optional_non_empty_str(self.error_message, "error_message"),
        )
        object.__setattr__(self, "warnings", tuple(_non_empty_str_list(self.warnings, "warnings")))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "context_packet": (
                None if self.context_packet is None else self.context_packet.to_dict()
            ),
            "error_message": self.error_message,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, kw_only=True)
class AISidecarContextTaskSpec:
    task_type: AISidecarTaskType
    required_sections: Sequence[str]
    optional_sections: Sequence[str] = field(default_factory=tuple)
    related_entity_type: str | None = None
    allow_order_context: bool = False
    default_limit: int = 50
    max_limit: int = 200

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_type", parse_task_type(self.task_type))
        object.__setattr__(
            self,
            "required_sections",
            tuple(_non_empty_str_list(self.required_sections, "required_sections")),
        )
        object.__setattr__(
            self,
            "optional_sections",
            tuple(_non_empty_str_list(self.optional_sections, "optional_sections")),
        )
        object.__setattr__(
            self,
            "related_entity_type",
            optional_non_empty_str(self.related_entity_type, "related_entity_type"),
        )
        object.__setattr__(self, "allow_order_context", bool(self.allow_order_context))
        object.__setattr__(self, "default_limit", max(int(self.default_limit), 1))
        object.__setattr__(self, "max_limit", max(int(self.max_limit), 1))

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_type": self.task_type.value,
            "required_sections": list(self.required_sections),
            "optional_sections": list(self.optional_sections),
            "related_entity_type": self.related_entity_type,
            "allow_order_context": self.allow_order_context,
            "default_limit": self.default_limit,
            "max_limit": self.max_limit,
        }


def parse_task_type(value: AISidecarTaskType | str | object) -> AISidecarTaskType:
    if isinstance(value, AISidecarTaskType):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for task_type in AISidecarTaskType:
            if normalized in {task_type.name.upper(), task_type.value.upper()}:
                return task_type
    allowed = ", ".join(task_type.value for task_type in AISidecarTaskType)
    raise AISidecarValidationError(f"task_type must be one of: {allowed}")


def canonical_context_json(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def calculate_context_hash(value: object) -> str:
    return hashlib.sha256(canonical_context_json(value).encode("utf-8")).hexdigest()


def packet_hash_material(packet: AISidecarContextPacket | Mapping[str, Any]) -> dict[str, Any]:
    data = packet.to_dict() if isinstance(packet, AISidecarContextPacket) else dict(packet)
    return {
        "task_type": data.get("task_type"),
        "schema_version": data.get("schema_version"),
        "trade_date": data.get("trade_date"),
        "related_entity_type": data.get("related_entity_type"),
        "related_entity_id": data.get("related_entity_id"),
        "source_sections": data.get("source_sections", []),
        "truncated": data.get("truncated", False),
        "redaction_applied": data.get("redaction_applied", False),
        "order_context_included": data.get("order_context_included", False),
        "missing_sections": data.get("missing_sections", []),
        "warnings": data.get("warnings", []),
        "payload": data.get("payload", {}),
    }


def packet_row_count(sections: Sequence[AISidecarContextSection]) -> int:
    return sum(section.row_count for section in sections)


def _parse_section(value: AISidecarContextSection | Mapping[str, Any]) -> AISidecarContextSection:
    if isinstance(value, AISidecarContextSection):
        return value
    if isinstance(value, Mapping):
        return AISidecarContextSection.from_dict(value)
    raise AISidecarValidationError("source_sections must contain section mappings")


def _non_empty_str_list(values: Sequence[object], field_name: str) -> list[str]:
    if isinstance(values, str) or not isinstance(values, Sequence):
        raise AISidecarValidationError(f"{field_name} must be a sequence")
    return [require_non_empty_str(value, f"{field_name} item") for value in values]


def _reject_unknown_fields(data: Mapping[str, Any], model_type: type) -> None:
    allowed_fields = {model_field.name for model_field in fields(model_type)}
    unknown = sorted(str(key) for key in data if str(key) not in allowed_fields)
    if unknown:
        joined = ", ".join(unknown)
        raise AISidecarValidationError(f"{model_type.__name__} unknown field(s): {joined}")
