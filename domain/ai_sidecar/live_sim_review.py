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
    parse_bool,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
)

LIVE_SIM_REVIEW_SCHEMA_VERSION = "ai-live-sim-review.v1"


class LiveSimReviewReportType(StrEnum):
    LIVE_SIM_SESSION_REVIEW = "LIVE_SIM_SESSION_REVIEW"
    LIVE_SIM_ORDER_REVIEW = "LIVE_SIM_ORDER_REVIEW"
    LIVE_SIM_RECONCILE_REVIEW = "LIVE_SIM_RECONCILE_REVIEW"
    LIVE_SIM_INCIDENT_REVIEW = "LIVE_SIM_INCIDENT_REVIEW"


class LiveSimReviewStatus(StrEnum):
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    AI_DISABLED = "AI_DISABLED"
    AI_UNAVAILABLE = "AI_UNAVAILABLE"
    AI_OUTPUT_INVALID = "AI_OUTPUT_INVALID"
    POLICY_REJECTED = "POLICY_REJECTED"


class LiveSimReviewSeverity(StrEnum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class LiveSimReviewRootCauseCategory(StrEnum):
    SAFETY_GATE = "SAFETY_GATE"
    ELIGIBILITY = "ELIGIBILITY"
    ORDER_COMMAND_QUEUE = "ORDER_COMMAND_QUEUE"
    GATEWAY_TRANSPORT = "GATEWAY_TRANSPORT"
    BROKER_ACK = "BROKER_ACK"
    BROKER_REJECTION = "BROKER_REJECTION"
    EXECUTION_EVENT = "EXECUTION_EVENT"
    PARTIAL_FILL = "PARTIAL_FILL"
    NO_FILL = "NO_FILL"
    RECONCILE_MISMATCH = "RECONCILE_MISMATCH"
    LOCAL_ONLY_RECONCILE = "LOCAL_ONLY_RECONCILE"
    DUPLICATE_IDEMPOTENCY = "DUPLICATE_IDEMPOTENCY"
    LIMIT_GUARD = "LIMIT_GUARD"
    ACCOUNT_GUARD = "ACCOUNT_GUARD"
    CONFIGURATION = "CONFIGURATION"
    AI_EXECUTION = "AI_EXECUTION"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, kw_only=True)
class LiveSimReviewSection:
    section_name: str
    status: str
    severity: LiveSimReviewSeverity
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
        object.__setattr__(self, "status", require_non_empty_str(self.status, "status").upper())
        object.__setattr__(self, "severity", _parse_enum(self.severity, LiveSimReviewSeverity))
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
        require_fields(mapping, ("section_name", "status", "severity", "summary"), cls.__name__)
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
class LiveSimReviewReport:
    report_type: LiveSimReviewReportType
    title: str
    summary: str
    status: LiveSimReviewStatus
    severity: LiveSimReviewSeverity
    root_cause_category: LiveSimReviewRootCauseCategory
    root_cause: str
    review_id: str = field(default_factory=lambda: new_message_id("live_sim_review"))
    trade_date: str | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    live_sim_intent_id: str | None = None
    live_sim_order_id: str | None = None
    live_sim_execution_id: str | None = None
    reconcile_id: str | None = None
    generated_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    ai_request_id: str | None = None
    ai_insight_id: str | None = None
    context_id: str | None = None
    deterministic_sections: Sequence[LiveSimReviewSection] = field(default_factory=tuple)
    ai_summary: str | None = None
    suggested_checks: Sequence[str] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    observe_only: bool = True
    review_only: bool = True
    no_trading_side_effects: bool = True
    live_real_allowed: bool = False
    order_action_allowed: bool = False
    gateway_command_allowed: bool = False
    schema_version: str = LIVE_SIM_REVIEW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "review_id", require_non_empty_str(self.review_id, "review_id"))
        object.__setattr__(
            self,
            "report_type",
            _parse_enum(self.report_type, LiveSimReviewReportType),
        )
        for field_name in (
            "trade_date",
            "related_entity_type",
            "related_entity_id",
            "live_sim_intent_id",
            "live_sim_order_id",
            "live_sim_execution_id",
            "reconcile_id",
            "ai_request_id",
            "ai_insight_id",
            "context_id",
            "ai_summary",
        ):
            object.__setattr__(
                self,
                field_name,
                optional_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "title", require_non_empty_str(self.title, "title"))
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(self, "status", _parse_enum(self.status, LiveSimReviewStatus))
        object.__setattr__(self, "severity", _parse_enum(self.severity, LiveSimReviewSeverity))
        object.__setattr__(
            self,
            "root_cause_category",
            _parse_enum(self.root_cause_category, LiveSimReviewRootCauseCategory),
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
            "deterministic_sections",
            tuple(_parse_section(section) for section in self.deterministic_sections),
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
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )
        _ensure_safe_flags(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_id": self.review_id,
            "report_type": self.report_type.value,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "live_sim_intent_id": self.live_sim_intent_id,
            "live_sim_order_id": self.live_sim_order_id,
            "live_sim_execution_id": self.live_sim_execution_id,
            "reconcile_id": self.reconcile_id,
            "title": self.title,
            "summary": self.summary,
            "status": self.status.value,
            "severity": self.severity.value,
            "root_cause_category": self.root_cause_category.value,
            "root_cause": self.root_cause,
            "generated_at": self.generated_at,
            "ai_request_id": self.ai_request_id,
            "ai_insight_id": self.ai_insight_id,
            "context_id": self.context_id,
            "deterministic_sections": [
                section.to_dict() for section in self.deterministic_sections
            ],
            "ai_summary": self.ai_summary,
            "suggested_checks": list(self.suggested_checks),
            "warnings": list(self.warnings),
            "metadata": normalize_payload(self.metadata),
            "observe_only": True,
            "review_only": True,
            "no_trading_side_effects": True,
            "live_real_allowed": False,
            "order_action_allowed": False,
            "gateway_command_allowed": False,
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
            review_id=mapping.get("review_id", new_message_id("live_sim_review")),
            report_type=mapping["report_type"],
            trade_date=mapping.get("trade_date"),
            related_entity_type=mapping.get("related_entity_type"),
            related_entity_id=mapping.get("related_entity_id"),
            live_sim_intent_id=mapping.get("live_sim_intent_id"),
            live_sim_order_id=mapping.get("live_sim_order_id"),
            live_sim_execution_id=mapping.get("live_sim_execution_id"),
            reconcile_id=mapping.get("reconcile_id"),
            title=mapping["title"],
            summary=mapping["summary"],
            status=mapping["status"],
            severity=mapping["severity"],
            root_cause_category=mapping["root_cause_category"],
            root_cause=mapping["root_cause"],
            generated_at=mapping.get("generated_at", datetime_to_wire(utc_now())),
            ai_request_id=mapping.get("ai_request_id"),
            ai_insight_id=mapping.get("ai_insight_id"),
            context_id=mapping.get("context_id"),
            deterministic_sections=mapping.get("deterministic_sections", ()),
            ai_summary=mapping.get("ai_summary"),
            suggested_checks=mapping.get("suggested_checks", ()),
            warnings=mapping.get("warnings", ()),
            metadata=mapping.get("metadata", {}),
            observe_only=mapping.get("observe_only", True),
            review_only=mapping.get("review_only", True),
            no_trading_side_effects=mapping.get("no_trading_side_effects", True),
            live_real_allowed=mapping.get("live_real_allowed", False),
            order_action_allowed=mapping.get("order_action_allowed", False),
            gateway_command_allowed=mapping.get("gateway_command_allowed", False),
            schema_version=str(mapping.get("schema_version", LIVE_SIM_REVIEW_SCHEMA_VERSION)),
        )


@dataclass(frozen=True, kw_only=True)
class LiveSimReviewBuildResult:
    ok: bool
    report: LiveSimReviewReport | None = None
    error_message: str | None = None
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ok", parse_bool(self.ok, "ok"))
        object.__setattr__(
            self,
            "error_message",
            optional_non_empty_str(self.error_message, "error_message"),
        )
        object.__setattr__(
            self,
            "warnings",
            tuple(_non_empty_str_list(self.warnings, "warnings")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "report": None if self.report is None else self.report.to_dict(),
            "error_message": self.error_message,
            "warnings": list(self.warnings),
        }


def ensure_live_sim_review_safety(report: LiveSimReviewReport) -> None:
    _ensure_safe_flags(report)


def parse_live_sim_review_report_type(
    value: LiveSimReviewReportType | str,
) -> LiveSimReviewReportType:
    return _parse_enum(value, LiveSimReviewReportType)


def parse_live_sim_review_status(value: LiveSimReviewStatus | str) -> LiveSimReviewStatus:
    return _parse_enum(value, LiveSimReviewStatus)


def _ensure_safe_flags(report: LiveSimReviewReport) -> None:
    checks = {
        "observe_only": (report.observe_only, True),
        "review_only": (report.review_only, True),
        "no_trading_side_effects": (report.no_trading_side_effects, True),
        "live_real_allowed": (report.live_real_allowed, False),
        "order_action_allowed": (report.order_action_allowed, False),
        "gateway_command_allowed": (report.gateway_command_allowed, False),
    }
    for field_name, (value, expected) in checks.items():
        parsed = parse_bool(value, field_name)
        if parsed is not expected:
            raise AISidecarValidationError(f"{field_name} must be {expected!s}.")
        object.__setattr__(report, field_name, expected)


def _parse_section(value: LiveSimReviewSection | Mapping[str, Any]) -> LiveSimReviewSection:
    if isinstance(value, LiveSimReviewSection):
        return value
    if isinstance(value, Mapping):
        return LiveSimReviewSection.from_dict(value)
    raise AISidecarValidationError("deterministic_sections must contain section mappings")


def _parse_enum(value: Any, enum_type: type[Any]) -> Any:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for member in enum_type:
            if normalized in {member.name.upper(), member.value.upper()}:
                return member
    allowed = ", ".join(member.value for member in enum_type)
    raise AISidecarValidationError(f"value must be one of: {allowed}")


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
