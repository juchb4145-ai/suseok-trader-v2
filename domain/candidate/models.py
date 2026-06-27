from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from domain.candidate.reasons import CandidateReasonCode
from domain.candidate.source import CandidateEventType, CandidateSourceType
from domain.candidate.state import CandidateState


@dataclass(frozen=True, kw_only=True)
class CandidateIdentity:
    candidate_instance_id: str
    trade_date: str
    code: str
    name: str
    primary_source_type: CandidateSourceType
    primary_source_id: str
    generation: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "primary_source_type",
            parse_str_enum(self.primary_source_type, CandidateSourceType, "primary_source_type"),
        )
        object.__setattr__(
            self,
            "primary_source_id",
            require_non_empty_str(self.primary_source_id, "primary_source_id"),
        )
        object.__setattr__(
            self,
            "generation",
            parse_int(self.generation, "generation", min_value=1),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "primary_source_type": self.primary_source_type.value,
            "primary_source_id": self.primary_source_id,
            "generation": self.generation,
        }


@dataclass(frozen=True, kw_only=True)
class CandidateSourceEvent:
    source_event_id: str
    trade_date: str
    code: str
    name: str
    source_type: CandidateSourceType
    source_id: str
    action: str
    event_ts: datetime | str
    observed_at: datetime | str
    candidate_instance_id: str | None = None
    theme_id: str | None = None
    theme_name: str | None = None
    condition_id: str | None = None
    condition_name: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: Sequence[CandidateReasonCode | str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_event_id",
            require_non_empty_str(self.source_event_id, "source_event_id"),
        )
        if self.candidate_instance_id is not None:
            object.__setattr__(
                self,
                "candidate_instance_id",
                require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
            )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "source_type",
            parse_str_enum(self.source_type, CandidateSourceType, "source_type"),
        )
        object.__setattr__(self, "source_id", require_non_empty_str(self.source_id, "source_id"))
        object.__setattr__(self, "action", require_non_empty_str(self.action, "action").upper())
        if self.theme_id is not None:
            object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        if self.theme_name is not None:
            object.__setattr__(
                self,
                "theme_name",
                require_non_empty_str(self.theme_name, "theme_name"),
            )
        if self.condition_id is not None:
            object.__setattr__(
                self,
                "condition_id",
                require_non_empty_str(self.condition_id, "condition_id"),
            )
        if self.condition_name is not None:
            object.__setattr__(
                self,
                "condition_name",
                require_non_empty_str(self.condition_name, "condition_name"),
            )
        object.__setattr__(self, "event_ts", parse_timestamp(self.event_ts, "event_ts"))
        object.__setattr__(self, "observed_at", parse_timestamp(self.observed_at, "observed_at"))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )

    def with_candidate(self, candidate_instance_id: str | None) -> CandidateSourceEvent:
        return CandidateSourceEvent(
            source_event_id=self.source_event_id,
            candidate_instance_id=candidate_instance_id,
            trade_date=self.trade_date,
            code=self.code,
            name=self.name,
            source_type=self.source_type,
            source_id=self.source_id,
            action=self.action,
            theme_id=self.theme_id,
            theme_name=self.theme_name,
            condition_id=self.condition_id,
            condition_name=self.condition_name,
            event_ts=self.event_ts,
            observed_at=self.observed_at,
            payload=self.payload,
            reason_codes=self.reason_codes,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_event_id": self.source_event_id,
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "source_type": self.source_type.value,
            "source_id": self.source_id,
            "action": self.action,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "condition_id": self.condition_id,
            "condition_name": self.condition_name,
            "event_ts": datetime_to_wire(parse_timestamp(self.event_ts, "event_ts")),
            "observed_at": datetime_to_wire(parse_timestamp(self.observed_at, "observed_at")),
            "payload": normalize_payload(self.payload),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class CandidateSnapshot:
    candidate_instance_id: str
    trade_date: str
    code: str
    name: str
    state: CandidateState
    previous_state: CandidateState | str | None
    detected_at: datetime | str
    last_seen_at: datetime | str
    state_updated_at: datetime | str
    source_count: int
    active_source_count: int
    primary_source_type: CandidateSourceType
    primary_source_id: str
    generation: int
    theme_id: str | None = None
    theme_name: str | None = None
    theme_state: str | None = None
    theme_role: str | None = None
    market_readiness_status: str | None = None
    tick_age_sec: float | None = None
    vwap_ready: bool = False
    bar_1m_ready: bool = False
    bar_3m_ready: bool = False
    bar_5m_ready: bool = False
    reason_codes: Sequence[CandidateReasonCode | str] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "state", parse_str_enum(self.state, CandidateState, "state"))
        if self.previous_state is not None:
            object.__setattr__(
                self,
                "previous_state",
                parse_str_enum(self.previous_state, CandidateState, "previous_state"),
            )
        for field_name in ("detected_at", "last_seen_at", "state_updated_at"):
            object.__setattr__(
                self,
                field_name,
                parse_timestamp(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "source_count",
            parse_int(self.source_count, "source_count", min_value=0),
        )
        object.__setattr__(
            self,
            "active_source_count",
            parse_int(self.active_source_count, "active_source_count", min_value=0),
        )
        object.__setattr__(
            self,
            "primary_source_type",
            parse_str_enum(self.primary_source_type, CandidateSourceType, "primary_source_type"),
        )
        object.__setattr__(
            self,
            "primary_source_id",
            require_non_empty_str(self.primary_source_id, "primary_source_id"),
        )
        object.__setattr__(
            self,
            "generation",
            parse_int(self.generation, "generation", min_value=1),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "generation": self.generation,
            "state": self.state.value,
            "previous_state": self.previous_state.value if self.previous_state else None,
            "detected_at": datetime_to_wire(parse_timestamp(self.detected_at, "detected_at")),
            "last_seen_at": datetime_to_wire(parse_timestamp(self.last_seen_at, "last_seen_at")),
            "state_updated_at": datetime_to_wire(
                parse_timestamp(self.state_updated_at, "state_updated_at")
            ),
            "source_count": self.source_count,
            "active_source_count": self.active_source_count,
            "primary_source_type": self.primary_source_type.value,
            "primary_source_id": self.primary_source_id,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state,
            "theme_role": self.theme_role,
            "market_readiness_status": self.market_readiness_status,
            "tick_age_sec": self.tick_age_sec,
            "vwap_ready": self.vwap_ready,
            "bar_1m_ready": self.bar_1m_ready,
            "bar_3m_ready": self.bar_3m_ready,
            "bar_5m_ready": self.bar_5m_ready,
            "reason_codes": list(self.reason_codes),
            "metadata": normalize_payload(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class CandidateStateTransition:
    transition_id: str
    candidate_instance_id: str
    trade_date: str
    code: str
    from_state: CandidateState | str | None
    to_state: CandidateState
    reason_codes: Sequence[CandidateReasonCode | str]
    event_type: CandidateEventType
    transitioned_at: datetime | str
    source_event_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "transition_id",
            require_non_empty_str(self.transition_id, "transition_id"),
        )
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        if self.from_state is not None:
            object.__setattr__(
                self,
                "from_state",
                parse_str_enum(self.from_state, CandidateState, "from_state"),
            )
        object.__setattr__(
            self,
            "to_state",
            parse_str_enum(self.to_state, CandidateState, "to_state"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        object.__setattr__(
            self,
            "event_type",
            parse_str_enum(self.event_type, CandidateEventType, "event_type"),
        )
        if self.source_event_id is not None:
            object.__setattr__(
                self,
                "source_event_id",
                require_non_empty_str(self.source_event_id, "source_event_id"),
            )
        object.__setattr__(
            self,
            "transitioned_at",
            parse_timestamp(self.transitioned_at, "transitioned_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "from_state": self.from_state.value if self.from_state else None,
            "to_state": self.to_state.value,
            "reason_codes": list(self.reason_codes),
            "event_type": self.event_type.value,
            "source_event_id": self.source_event_id,
            "transitioned_at": datetime_to_wire(
                parse_timestamp(self.transitioned_at, "transitioned_at")
            ),
            "metadata": normalize_payload(self.metadata),
        }


def _reason_value(reason: CandidateReasonCode | str) -> str:
    if isinstance(reason, CandidateReasonCode):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()
