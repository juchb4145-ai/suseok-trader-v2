from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from enum import StrEnum
from typing import Any, Self

from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    optional_non_empty_str,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
)

SCHEMA_VERSION = "ai-sidecar.v1"


class AISidecarValidationError(ValueError):
    """Raised when AI Sidecar data violates the read-only contract."""


class AISidecarInsightSeverity(StrEnum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, kw_only=True)
class AISidecarRequest:
    task_type: AISidecarTaskType
    context: Mapping[str, Any]
    request_id: str = field(default_factory=lambda: new_message_id("ai_req"))
    trade_date: str | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    model: str = ""
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", require_non_empty_str(self.request_id, "request_id"))
        object.__setattr__(
            self,
            "task_type",
            _parse_task_type(self.task_type),
        )
        object.__setattr__(self, "context", normalize_payload(self.context))
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
        object.__setattr__(self, "model", self.model.strip())
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "task_type": self.task_type.value,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "context": normalize_payload(self.context),
            "model": self.model,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AISidecarRequest:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("task_type", "context"), cls.__name__)
        _reject_unknown_fields(mapping, cls)
        return cls(
            request_id=mapping.get("request_id", new_message_id("ai_req")),
            task_type=mapping["task_type"],
            trade_date=mapping.get("trade_date"),
            related_entity_type=mapping.get("related_entity_type"),
            related_entity_id=mapping.get("related_entity_id"),
            context=mapping["context"],
            model=str(mapping.get("model", "")),
            schema_version=str(mapping.get("schema_version", SCHEMA_VERSION)),
        )


@dataclass(frozen=True, kw_only=True)
class AISidecarInsight:
    request_id: str
    task_type: AISidecarTaskType
    summary: str
    output: Mapping[str, Any]
    insight_id: str = field(default_factory=lambda: new_message_id("ai_insight"))
    trade_date: str | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    root_cause: str | None = None
    severity: AISidecarInsightSeverity | None = None
    operator_action: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "insight_id", require_non_empty_str(self.insight_id, "insight_id"))
        object.__setattr__(self, "request_id", require_non_empty_str(self.request_id, "request_id"))
        object.__setattr__(
            self,
            "task_type",
            _parse_task_type(self.task_type),
        )
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(self, "output", normalize_payload(self.output))
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
            "root_cause",
            optional_non_empty_str(self.root_cause, "root_cause"),
        )
        if self.severity is not None:
            object.__setattr__(
                self,
                "severity",
                _parse_severity(self.severity),
            )
        object.__setattr__(
            self,
            "operator_action",
            optional_non_empty_str(self.operator_action, "operator_action"),
        )
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "insight_id": self.insight_id,
            "request_id": self.request_id,
            "task_type": self.task_type.value,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "summary": self.summary,
            "root_cause": self.root_cause,
            "severity": self.severity.value if self.severity is not None else None,
            "operator_action": self.operator_action,
            "output": normalize_payload(self.output),
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True, kw_only=True)
class AISidecarBaseOutput:
    summary: str
    severity: AISidecarInsightSeverity
    root_cause: str
    operator_action: str
    suggested_checks: Sequence[str]
    confidence: float
    forbidden_actions_confirmed: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(
            self,
            "severity",
            _parse_severity(self.severity),
        )
        object.__setattr__(
            self,
            "root_cause",
            require_non_empty_str(self.root_cause, "root_cause"),
        )
        object.__setattr__(
            self,
            "operator_action",
            require_non_empty_str(self.operator_action, "operator_action"),
        )
        object.__setattr__(
            self,
            "suggested_checks",
            tuple(_parse_suggested_checks(self.suggested_checks)),
        )
        object.__setattr__(self, "confidence", _parse_confidence(self.confidence))
        if self.forbidden_actions_confirmed is not True:
            raise AISidecarValidationError("forbidden_actions_confirmed must be true")

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "severity": self.severity.value,
            "root_cause": self.root_cause,
            "operator_action": self.operator_action,
            "suggested_checks": list(self.suggested_checks),
            "confidence": self.confidence,
            "forbidden_actions_confirmed": self.forbidden_actions_confirmed,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "summary",
                "severity",
                "root_cause",
                "operator_action",
                "suggested_checks",
                "confidence",
                "forbidden_actions_confirmed",
            ),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            summary=mapping["summary"],
            severity=mapping["severity"],
            root_cause=mapping["root_cause"],
            operator_action=mapping["operator_action"],
            suggested_checks=mapping["suggested_checks"],
            confidence=mapping["confidence"],
            forbidden_actions_confirmed=mapping["forbidden_actions_confirmed"],
        )


class DailyMarketBriefOutput(AISidecarBaseOutput):
    """Pre/post-market read-only market summary for operator review."""


class ThemeBriefOutput(AISidecarBaseOutput):
    """Read-only theme explanation for dashboard/report surfaces."""


class CandidateBlockRCAOutput(AISidecarBaseOutput):
    """Explanation of why a candidate was blocked by deterministic gates."""


class NoTradeRCAOutput(AISidecarBaseOutput):
    """Read-only root-cause analysis for a no-trade session."""


class TradeReviewOutput(AISidecarBaseOutput):
    """After-the-fact trade review without order authority."""


class OpsIncidentSummaryOutput(AISidecarBaseOutput):
    """Operations incident summary for human review."""


@dataclass(frozen=True, kw_only=True)
class CodexPromptDraftOutput(AISidecarBaseOutput):
    prompt_draft: str

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(
            self,
            "prompt_draft",
            require_non_empty_str(self.prompt_draft, "prompt_draft"),
        )

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["prompt_draft"] = self.prompt_draft
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CodexPromptDraftOutput:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "summary",
                "severity",
                "root_cause",
                "operator_action",
                "suggested_checks",
                "confidence",
                "forbidden_actions_confirmed",
                "prompt_draft",
            ),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            summary=mapping["summary"],
            severity=mapping["severity"],
            root_cause=mapping["root_cause"],
            operator_action=mapping["operator_action"],
            suggested_checks=mapping["suggested_checks"],
            confidence=mapping["confidence"],
            forbidden_actions_confirmed=mapping["forbidden_actions_confirmed"],
            prompt_draft=mapping["prompt_draft"],
        )


def _parse_suggested_checks(value: object) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise AISidecarValidationError("suggested_checks must be a list of strings")
    checks = tuple(require_non_empty_str(item, "suggested_checks item") for item in value)
    if not checks:
        raise AISidecarValidationError("suggested_checks must not be empty")
    return checks


def _parse_confidence(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AISidecarValidationError("confidence must be a number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > 1.0:
        raise AISidecarValidationError("confidence must be between 0.0 and 1.0")
    return parsed


def _reject_unknown_fields(data: Mapping[str, Any], model_type: type) -> None:
    allowed_fields = {model_field.name for model_field in fields(model_type)}
    unknown = sorted(str(key) for key in data if str(key) not in allowed_fields)
    if unknown:
        joined = ", ".join(unknown)
        raise AISidecarValidationError(f"{model_type.__name__} unknown field(s): {joined}")


def _parse_task_type(value: object) -> AISidecarTaskType:
    if isinstance(value, AISidecarTaskType):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for task_type in AISidecarTaskType:
            if normalized in {task_type.name.upper(), task_type.value.upper()}:
                return task_type
    allowed = ", ".join(task_type.value for task_type in AISidecarTaskType)
    raise AISidecarValidationError(f"task_type must be one of: {allowed}")


def _parse_severity(value: object) -> AISidecarInsightSeverity:
    if isinstance(value, AISidecarInsightSeverity):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for severity in AISidecarInsightSeverity:
            if normalized in {severity.name.upper(), severity.value.upper()}:
                return severity
    allowed = ", ".join(severity.value for severity in AISidecarInsightSeverity)
    raise AISidecarValidationError(f"severity must be one of: {allowed}")


def insight_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    created_at = row["created_at"]
    return {
        "insight_id": row["insight_id"],
        "request_id": row["request_id"],
        "task_type": row["task_type"],
        "trade_date": row["trade_date"],
        "related_entity_type": row["related_entity_type"],
        "related_entity_id": row["related_entity_id"],
        "summary": row["summary"],
        "root_cause": row["root_cause"],
        "severity": row["severity"],
        "operator_action": row["operator_action"],
        "schema_version": row["schema_version"],
        "created_at": datetime_to_wire(parse_timestamp(created_at, "created_at")),
    }
