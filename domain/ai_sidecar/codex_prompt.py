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

CODEX_PROMPT_SCHEMA_VERSION = "ai-codex-prompt-draft.v1"


class AICodexPromptDraftStatus(StrEnum):
    CREATED = "CREATED"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    AI_DISABLED = "AI_DISABLED"
    AI_UNAVAILABLE = "AI_UNAVAILABLE"
    AI_OUTPUT_INVALID = "AI_OUTPUT_INVALID"
    POLICY_REJECTED = "POLICY_REJECTED"


class AICodexPromptSourceType(StrEnum):
    RCA_REPORT = "RCA_REPORT"
    CANDIDATE = "CANDIDATE"
    NO_TRADE = "NO_TRADE"
    OPS_INCIDENT = "OPS_INCIDENT"
    AI_CONTEXT_PACKET = "AI_CONTEXT_PACKET"
    AI_INSIGHT = "AI_INSIGHT"
    MANUAL_NOTE = "MANUAL_NOTE"


class AICodexPromptTargetArea(StrEnum):
    GATEWAY_TRANSPORT = "GATEWAY_TRANSPORT"
    MARKET_DATA = "MARKET_DATA"
    THEME_SERVICE = "THEME_SERVICE"
    CANDIDATE_FSM = "CANDIDATE_FSM"
    STRATEGY_ENGINE = "STRATEGY_ENGINE"
    RISK_GATE = "RISK_GATE"
    DASHBOARD = "DASHBOARD"
    AI_SIDECAR = "AI_SIDECAR"
    RCA_WORKFLOW = "RCA_WORKFLOW"
    TESTING = "TESTING"
    DOCS = "DOCS"
    SAFETY_REVIEW = "SAFETY_REVIEW"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, kw_only=True)
class AICodexPromptSection:
    section_name: str
    title: str
    body: str
    source_refs: Sequence[str] = field(default_factory=tuple)
    required: bool = True
    order_index: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "section_name",
            require_non_empty_str(self.section_name, "section_name"),
        )
        object.__setattr__(self, "title", require_non_empty_str(self.title, "title"))
        object.__setattr__(self, "body", require_non_empty_str(self.body, "body"))
        object.__setattr__(
            self,
            "source_refs",
            tuple(_non_empty_str_list(self.source_refs, "source_refs")),
        )
        object.__setattr__(self, "required", bool(self.required))
        object.__setattr__(self, "order_index", max(int(self.order_index), 0))

    def to_dict(self) -> dict[str, Any]:
        return {
            "section_name": self.section_name,
            "title": self.title,
            "body": self.body,
            "source_refs": list(self.source_refs),
            "required": self.required,
            "order_index": self.order_index,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("section_name", "title", "body"), cls.__name__)
        _reject_unknown_fields(mapping, cls)
        return cls(
            section_name=mapping["section_name"],
            title=mapping["title"],
            body=mapping["body"],
            source_refs=mapping.get("source_refs", ()),
            required=bool(mapping.get("required", True)),
            order_index=int(mapping.get("order_index", 0)),
        )


@dataclass(frozen=True, kw_only=True)
class AICodexPromptDraft:
    title: str
    source_type: AICodexPromptSourceType
    target_area: AICodexPromptTargetArea
    status: AICodexPromptDraftStatus
    summary: str
    prompt_text: str
    draft_id: str = field(default_factory=lambda: new_message_id("ai_codex_prompt"))
    trade_date: str | None = None
    related_entity_type: str | None = None
    related_entity_id: str | None = None
    rca_report_id: str | None = None
    context_id: str | None = None
    ai_request_id: str | None = None
    ai_insight_id: str | None = None
    safety_notes: Sequence[str] = field(default_factory=tuple)
    acceptance_criteria: Sequence[str] = field(default_factory=tuple)
    forbidden_scope: Sequence[str] = field(default_factory=tuple)
    test_plan: Sequence[str] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    sections: Sequence[AICodexPromptSection] = field(default_factory=tuple)
    generated_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    generated_by: str = "deterministic_codex_prompt_generator"
    run_ai: bool = False
    observe_only: bool = True
    human_review_required: bool = True
    auto_apply_allowed: bool = False
    github_write_allowed: bool = False
    codex_execution_allowed: bool = False
    no_trading_side_effects: bool = True
    schema_version: str = CODEX_PROMPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "draft_id", require_non_empty_str(self.draft_id, "draft_id"))
        object.__setattr__(self, "title", require_non_empty_str(self.title, "title"))
        object.__setattr__(
            self,
            "source_type",
            parse_prompt_source_type(self.source_type),
        )
        object.__setattr__(
            self,
            "target_area",
            parse_prompt_target_area(self.target_area),
        )
        object.__setattr__(
            self,
            "status",
            parse_prompt_status(self.status),
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
            "rca_report_id",
            optional_non_empty_str(self.rca_report_id, "rca_report_id"),
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
        object.__setattr__(self, "summary", require_non_empty_str(self.summary, "summary"))
        object.__setattr__(
            self,
            "prompt_text",
            sanitize_prompt_text(require_non_empty_str(self.prompt_text, "prompt_text")),
        )
        object.__setattr__(
            self,
            "safety_notes",
            tuple(_non_empty_str_list(self.safety_notes, "safety_notes")),
        )
        object.__setattr__(
            self,
            "acceptance_criteria",
            tuple(_non_empty_str_list(self.acceptance_criteria, "acceptance_criteria")),
        )
        object.__setattr__(
            self,
            "forbidden_scope",
            tuple(_non_empty_str_list(self.forbidden_scope, "forbidden_scope")),
        )
        object.__setattr__(
            self,
            "test_plan",
            tuple(_non_empty_str_list(self.test_plan, "test_plan")),
        )
        object.__setattr__(
            self,
            "warnings",
            tuple(_non_empty_str_list(self.warnings, "warnings")),
        )
        object.__setattr__(self, "metadata", normalize_payload(self.metadata))
        object.__setattr__(
            self,
            "sections",
            tuple(_parse_section(section) for section in self.sections),
        )
        object.__setattr__(
            self,
            "generated_at",
            require_non_empty_str(self.generated_at, "generated_at"),
        )
        object.__setattr__(
            self,
            "generated_by",
            require_non_empty_str(self.generated_by, "generated_by"),
        )
        object.__setattr__(self, "run_ai", bool(self.run_ai))
        object.__setattr__(self, "observe_only", bool(self.observe_only))
        object.__setattr__(
            self,
            "human_review_required",
            bool(self.human_review_required),
        )
        object.__setattr__(self, "auto_apply_allowed", bool(self.auto_apply_allowed))
        object.__setattr__(self, "github_write_allowed", bool(self.github_write_allowed))
        object.__setattr__(
            self,
            "codex_execution_allowed",
            bool(self.codex_execution_allowed),
        )
        object.__setattr__(
            self,
            "no_trading_side_effects",
            bool(self.no_trading_side_effects),
        )
        object.__setattr__(
            self,
            "schema_version",
            require_non_empty_str(self.schema_version, "schema_version"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "draft_id": self.draft_id,
            "title": self.title,
            "source_type": self.source_type.value,
            "target_area": self.target_area.value,
            "status": self.status.value,
            "trade_date": self.trade_date,
            "related_entity_type": self.related_entity_type,
            "related_entity_id": self.related_entity_id,
            "rca_report_id": self.rca_report_id,
            "context_id": self.context_id,
            "ai_request_id": self.ai_request_id,
            "ai_insight_id": self.ai_insight_id,
            "summary": self.summary,
            "prompt_text": self.prompt_text,
            "safety_notes": list(self.safety_notes),
            "acceptance_criteria": list(self.acceptance_criteria),
            "forbidden_scope": list(self.forbidden_scope),
            "test_plan": list(self.test_plan),
            "warnings": list(self.warnings),
            "metadata": normalize_payload(self.metadata),
            "sections": [section.to_dict() for section in self.sections],
            "generated_at": self.generated_at,
            "generated_by": self.generated_by,
            "run_ai": self.run_ai,
            "observe_only": self.observe_only,
            "human_review_required": self.human_review_required,
            "auto_apply_allowed": self.auto_apply_allowed,
            "github_write_allowed": self.github_write_allowed,
            "codex_execution_allowed": self.codex_execution_allowed,
            "no_trading_side_effects": self.no_trading_side_effects,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            (
                "title",
                "source_type",
                "target_area",
                "status",
                "summary",
                "prompt_text",
            ),
            cls.__name__,
        )
        _reject_unknown_fields(mapping, cls)
        return cls(
            draft_id=mapping.get("draft_id", new_message_id("ai_codex_prompt")),
            title=mapping["title"],
            source_type=mapping["source_type"],
            target_area=mapping["target_area"],
            status=mapping["status"],
            trade_date=mapping.get("trade_date"),
            related_entity_type=mapping.get("related_entity_type"),
            related_entity_id=mapping.get("related_entity_id"),
            rca_report_id=mapping.get("rca_report_id"),
            context_id=mapping.get("context_id"),
            ai_request_id=mapping.get("ai_request_id"),
            ai_insight_id=mapping.get("ai_insight_id"),
            summary=mapping["summary"],
            prompt_text=mapping["prompt_text"],
            safety_notes=mapping.get("safety_notes", ()),
            acceptance_criteria=mapping.get("acceptance_criteria", ()),
            forbidden_scope=mapping.get("forbidden_scope", ()),
            test_plan=mapping.get("test_plan", ()),
            warnings=mapping.get("warnings", ()),
            metadata=mapping.get("metadata", {}),
            sections=mapping.get("sections", ()),
            generated_at=mapping.get("generated_at", datetime_to_wire(utc_now())),
            generated_by=str(
                mapping.get("generated_by", "deterministic_codex_prompt_generator")
            ),
            run_ai=bool(mapping.get("run_ai", False)),
            observe_only=bool(mapping.get("observe_only", True)),
            human_review_required=bool(mapping.get("human_review_required", True)),
            auto_apply_allowed=bool(mapping.get("auto_apply_allowed", False)),
            github_write_allowed=bool(mapping.get("github_write_allowed", False)),
            codex_execution_allowed=bool(mapping.get("codex_execution_allowed", False)),
            no_trading_side_effects=bool(mapping.get("no_trading_side_effects", True)),
            schema_version=str(mapping.get("schema_version", CODEX_PROMPT_SCHEMA_VERSION)),
        )


@dataclass(frozen=True, kw_only=True)
class AICodexPromptBuildResult:
    ok: bool
    draft: AICodexPromptDraft | None = None
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
            "draft": None if self.draft is None else self.draft.to_dict(),
            "error_message": self.error_message,
            "warnings": list(self.warnings),
        }


SAFE_PROMPT_NEGATION_TOKENS: tuple[str, ...] = (
    "하지 말",
    "하지말",
    "만들지 말",
    "생성하지 말",
    "호출하지 말",
    "연결하지 말",
    "연결되지",
    "사용하지 말",
    "금지",
    "없음",
    "없다",
    "아니다",
    "않",
    "차단",
    "비활성",
    "검토용",
    "표시용",
    "read-only",
    "review-only",
    "copy-only",
    "do not",
    "don't",
    "never",
    "not ",
    " no ",
    "disabled",
    "false",
    "forbidden",
    "without",
)

UNSAFE_PROMPT_TOKENS: tuple[str, ...] = (
    "send_order",
    "cancel_order",
    "modify_order",
    "orderintent 생성",
    "orderintent를 생성",
    "orderintent 만들",
    "orderintent를 만들",
    "orderintent 호출",
    "orderintent를 호출",
    "orderintent 사용",
    "call orderintent",
    "use orderintent",
    "order intent create",
    "order intent 생성",
    "gatewaycommand 생성",
    "gatewaycommand를 생성",
    "gatewaycommand 만들",
    "gatewaycommand 호출",
    "gatewaycommand를 호출",
    "gatewaycommand 사용",
    "call gatewaycommand",
    "use gatewaycommand",
    "gateway command create",
    "gateway command 생성",
    "order execution",
    "place order",
    "execute order",
    "buy order",
    "sell order",
    "주문 실행",
    "주문 전송",
    "자동 주문",
    "자동 매수",
    "자동 매도",
    "매수 주문",
    "매도 주문",
    "live_real",
    "live_sim",
    "create branch",
    "create commit",
    "commit and push",
    "commit changes",
    "commit the",
    "git commit",
    "git push",
    "push to",
    "push changes",
    "push the",
    "pull request",
    "open pr",
    "create pr",
    "github branch",
    "github commit",
    "github push",
    "github pr",
    "브랜치",
    "커밋",
    "푸시",
    "pr 생성",
    "pull request 생성",
    "codex 실행",
    "자동 수정",
)


def parse_prompt_status(value: AICodexPromptDraftStatus | str) -> AICodexPromptDraftStatus:
    return _parse_enum(value, AICodexPromptDraftStatus, "status")


def parse_prompt_source_type(
    value: AICodexPromptSourceType | str,
) -> AICodexPromptSourceType:
    return _parse_enum(value, AICodexPromptSourceType, "source_type")


def parse_prompt_target_area(
    value: AICodexPromptTargetArea | str,
) -> AICodexPromptTargetArea:
    return _parse_enum(value, AICodexPromptTargetArea, "target_area")


def sanitize_prompt_text(value: str) -> str:
    lines = value.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\n".join(line.rstrip() for line in lines).strip()


def codex_prompt_policy_violation(prompt_text: str) -> str | None:
    text = sanitize_prompt_text(prompt_text)
    if not text:
        return "prompt_text must not be empty"
    for line in text.splitlines():
        normalized = f" {line.strip().lower()} "
        if not normalized.strip():
            continue
        for token in UNSAFE_PROMPT_TOKENS:
            if token in normalized and not _line_contains_negation(normalized):
                return f"unsafe Codex prompt instruction contains {token!r}: {line.strip()}"
    return None


def ensure_codex_prompt_policy(prompt_text: str) -> None:
    violation = codex_prompt_policy_violation(prompt_text)
    if violation is not None:
        raise AISidecarValidationError(violation)


def _line_contains_negation(normalized_line: str) -> bool:
    return any(token in normalized_line for token in SAFE_PROMPT_NEGATION_TOKENS)


def _parse_section(value: AICodexPromptSection | Mapping[str, Any]) -> AICodexPromptSection:
    if isinstance(value, AICodexPromptSection):
        return value
    if isinstance(value, Mapping):
        return AICodexPromptSection.from_dict(value)
    raise AISidecarValidationError("sections must contain section mappings")


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
