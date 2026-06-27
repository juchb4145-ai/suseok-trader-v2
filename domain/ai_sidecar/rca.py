from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from enum import StrEnum
from typing import Any, Self

from domain.ai_sidecar.schemas import AISidecarValidationError
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    normalize_value,
    optional_non_empty_str,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
)

RCA_SCHEMA_VERSION = "ai-rca-report.v1"


class AIRCAReportType(StrEnum):
    NO_TRADE_RCA = "NO_TRADE_RCA"
    CANDIDATE_BLOCK_RCA = "CANDIDATE_BLOCK_RCA"


class AIRCAReportStatus(StrEnum):
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    AI_DISABLED = "AI_DISABLED"
    AI_UNAVAILABLE = "AI_UNAVAILABLE"
    AI_OUTPUT_INVALID = "AI_OUTPUT_INVALID"
    POLICY_REJECTED = "POLICY_REJECTED"


class AIRCASeverity(StrEnum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AIRCARootCauseCategory(StrEnum):
    OBSERVE_ONLY_PIPELINE = "OBSERVE_ONLY_PIPELINE"
    NO_ORDER_PATH_BY_DESIGN = "NO_ORDER_PATH_BY_DESIGN"
    DATA_QUALITY = "DATA_QUALITY"
    THEME_CONTEXT = "THEME_CONTEXT"
    CANDIDATE_CONTEXT = "CANDIDATE_CONTEXT"
    STRATEGY_CONTEXT = "STRATEGY_CONTEXT"
    RISK_CONTEXT = "RISK_CONTEXT"
    GATEWAY_TRANSPORT = "GATEWAY_TRANSPORT"
    MARKET_DATA_PROJECTION = "MARKET_DATA_PROJECTION"
    THEME_PROJECTION = "THEME_PROJECTION"
    CANDIDATE_PROJECTION = "CANDIDATE_PROJECTION"
    STRATEGY_EVALUATION = "STRATEGY_EVALUATION"
    RISK_EVALUATION = "RISK_EVALUATION"
    AI_EXECUTION = "AI_EXECUTION"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, kw_only=True)
class AIRCASection:
    section_name: str
    status: str
    severity: AIRCASeverity
    summary: str
    reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    source_refs: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "section_name",
            require_non_empty_str(self.section_name, "section_name"),
        )
        object.__setattr__(self, "status", require_non_empty_str(self.status, "status"))
        object.__setattr__(self, "severity", _parse_enum(self.severity, AIRCASeverity, "severity"))
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_non_empty_str_list(self.reason_codes, "reason_codes")),
        )
        object.__setattr__(self, "evidence_json", normalize_payload(self.evidence_json))
        object.__setattr__(
            self,
            "source_refs",
            tuple(_non_empty_str_list(self.source_refs, "source_refs")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "section_name": self.section_name,
            "status": self.status,
            "severity": self.severity.value,
            "summary": self.summary,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
            "source_refs": list(self.source_refs),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            ("section_name", "status", "severity", "summary"),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            section_name=mapping["section_name"],
            status=mapping["status"],
            severity=mapping["severity"],
            summary=mapping["summary"],
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            source_refs=mapping.get("source_refs", ()),
        )


@dataclass(frozen=True, kw_only=True)
class AIRCAReport:
    report_type: AIRCAReportType
    title: str
    summary: str
    status: AIRCAReportStatus
    severity: AIRCASeverity
    root_cause_category: AIRCARootCauseCategory
    root_cause: str
    report_id: str = field(default_factory=lambda: new_message_id("ai_rca_report"))
    trade_date: str | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    generated_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    context_id: str | None = None
    ai_request_id: str | None = None
    ai_insight_id: str | None = None
    deterministic_sections: Sequence[AIRCASection] = field(default_factory=tuple)
    ai_summary: str | None = None
    suggested_checks: Sequence[str] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    observe_only: bool = True
    no_trading_side_effects: bool = True
    schema_version: str = RCA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "report_id", require_non_empty_str(self.report_id, "report_id"))
        object.__setattr__(
            self,
            "report_type",
            _parse_enum(self.report_type, AIRCAReportType, "report_type"),
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
        object.__setattr__(self, "title", require_non_empty_str(self.title, "title"))
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(
            self,
            "status",
            _parse_enum(self.status, AIRCAReportStatus, "status"),
        )
        object.__setattr__(self, "severity", _parse_enum(self.severity, AIRCASeverity, "severity"))
        object.__setattr__(
            self,
            "root_cause_category",
            _parse_enum(self.root_cause_category, AIRCARootCauseCategory, "root_cause_category"),
        )
        object.__setattr__(
            self,
            "root_cause",
            require_non_empty_str(self.root_cause, "root_cause"),
        )
        object.__setattr__(
            self,
            "generated_at",
            require_non_empty_str(self.generated_at, "generated_at"),
        )
        object.__setattr__(
            self,
            "context_id",
            optional_non_empty_str(self.context_id, "context_id"),
        )
        object.__setattr__(
            self,
            "ai_request_id",
            optional_non_empty_str(self.ai_request_id, "ai_request_id"),
        )
        object.__setattr__(
            self,
            "ai_insight_id",
            optional_non_empty_str(self.ai_insight_id, "ai_insight_id"),
        )
        object.__setattr__(
            self,
            "deterministic_sections",
            tuple(_parse_section(section) for section in self.deterministic_sections),
        )
        object.__setattr__(
            self,
            "ai_summary",
            optional_non_empty_str(self.ai_summary, "ai_summary"),
        )
        object.__setattr__(
            self,
            "suggested_checks",
            tuple(_non_empty_str_list(self.suggested_checks, "suggested_checks")),
        )
        object.__setattr__(
            self,
            "warnings",
            tuple(_non_empty_str_list(self.warnings, "warnings")),
        )
        object.__setattr__(self, "metadata", normalize_payload(self.metadata))
        object.__setattr__(self, "observe_only", bool(self.observe_only))
        object.__setattr__(self, "no_trading_side_effects", bool(self.no_trading_side_effects))
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "report_type": self.report_type.value,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "title": self.title,
            "summary": self.summary,
            "status": self.status.value,
            "severity": self.severity.value,
            "root_cause_category": self.root_cause_category.value,
            "root_cause": self.root_cause,
            "generated_at": self.generated_at,
            "context_id": self.context_id,
            "ai_request_id": self.ai_request_id,
            "ai_insight_id": self.ai_insight_id,
            "deterministic_sections": [
                section.to_dict() for section in self.deterministic_sections
            ],
            "ai_summary": self.ai_summary,
            "suggested_checks": list(self.suggested_checks),
            "warnings": list(self.warnings),
            "metadata": normalize_payload(self.metadata),
            "observe_only": self.observe_only,
            "no_trading_side_effects": self.no_trading_side_effects,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "report_type",
                "title",
                "summary",
                "status",
                "severity",
                "root_cause_category",
                "root_cause",
            ),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            report_id=mapping.get("report_id", new_message_id("ai_rca_report")),
            report_type=mapping["report_type"],
            trade_date=mapping.get("trade_date"),
            related_entity_type=mapping.get("related_entity_type"),
            related_entity_id=mapping.get("related_entity_id"),
            title=mapping["title"],
            summary=mapping["summary"],
            status=mapping["status"],
            severity=mapping["severity"],
            root_cause_category=mapping["root_cause_category"],
            root_cause=mapping["root_cause"],
            generated_at=mapping.get("generated_at", datetime_to_wire(utc_now())),
            context_id=mapping.get("context_id"),
            ai_request_id=mapping.get("ai_request_id"),
            ai_insight_id=mapping.get("ai_insight_id"),
            deterministic_sections=mapping.get("deterministic_sections", ()),
            ai_summary=mapping.get("ai_summary"),
            suggested_checks=mapping.get("suggested_checks", ()),
            warnings=mapping.get("warnings", ()),
            metadata=mapping.get("metadata", {}),
            observe_only=bool(mapping.get("observe_only", True)),
            no_trading_side_effects=bool(mapping.get("no_trading_side_effects", True)),
            schema_version=str(mapping.get("schema_version", RCA_SCHEMA_VERSION)),
        )


@dataclass(frozen=True, kw_only=True)
class AIRCAReportBuildResult:
    ok: bool
    report: AIRCAReport | None = None
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
            "report": None if self.report is None else self.report.to_dict(),
            "error_message": self.error_message,
            "warnings": list(self.warnings),
        }


def parse_report_type(value: AIRCAReportType | str) -> AIRCAReportType:
    return _parse_enum(value, AIRCAReportType, "report_type")


def parse_report_status(value: AIRCAReportStatus | str) -> AIRCAReportStatus:
    return _parse_enum(value, AIRCAReportStatus, "status")


def parse_root_cause_category(
    value: AIRCARootCauseCategory | str,
) -> AIRCARootCauseCategory:
    return _parse_enum(value, AIRCARootCauseCategory, "root_cause_category")


def _parse_section(value: AIRCASection | Mapping[str, Any]) -> AIRCASection:
    if isinstance(value, AIRCASection):
        return value
    if isinstance(value, Mapping):
        return AIRCASection.from_dict(value)
    raise AISidecarValidationError("deterministic_sections must contain section mappings")


def _parse_enum(value: Any, enum_type: type[Any], field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for member in enum_type:
            if normalized in {member.name.upper(), member.value.upper()}:
                return member
    allowed = ", ".join(member.value for member in enum_type)
    raise AISidecarValidationError(f"{field_name} must be one of: {allowed}")


def _non_empty_str_list(values: Sequence[object], field_name: str) -> list[str]:
    if isinstance(values, str) or not isinstance(values, Sequence):
        raise AISidecarValidationError(f"{field_name} must be a sequence")
    normalized = []
    for value in normalize_value(values):
        if value is None:
            continue
        normalized.append(require_non_empty_str(str(value), f"{field_name} item"))
    return normalized


def _reject_unknown_fields(data: Mapping[str, Any], model_type: type) -> None:
    allowed_fields = {model_field.name for model_field in fields(model_type)}
    unknown = sorted(str(key) for key in data if str(key) not in allowed_fields)
    if unknown:
        joined = ", ".join(unknown)
        raise AISidecarValidationError(f"{model_type.__name__} unknown field(s): {joined}")
