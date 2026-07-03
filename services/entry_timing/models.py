from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_bool,
    parse_float,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)


class EntryTimingState(StrEnum):
    GOOD_PULLBACK = "GOOD_PULLBACK"
    PULLBACK_RECLAIM = "PULLBACK_RECLAIM"
    VWAP_RECLAIM = "VWAP_RECLAIM"
    MOMENTUM_CONTINUATION = "MOMENTUM_CONTINUATION"
    CHASE_HIGH = "CHASE_HIGH"
    VWAP_OVEREXTENDED = "VWAP_OVEREXTENDED"
    FAILED_BREAKOUT = "FAILED_BREAKOUT"
    NO_SETUP = "NO_SETUP"
    DATA_WAIT = "DATA_WAIT"
    STALE = "STALE"
    BLOCKED_CONTEXT = "BLOCKED_CONTEXT"


class PriceLocationState(StrEnum):
    NEAR_VWAP = "NEAR_VWAP"
    ABOVE_VWAP = "ABOVE_VWAP"
    BELOW_VWAP = "BELOW_VWAP"
    NEAR_DAY_HIGH = "NEAR_DAY_HIGH"
    PULLBACK_FROM_HIGH = "PULLBACK_FROM_HIGH"
    DEEP_PULLBACK = "DEEP_PULLBACK"
    EXTENDED_FROM_VWAP = "EXTENDED_FROM_VWAP"
    UNKNOWN = "UNKNOWN"


class SetupType(StrEnum):
    THEME_LEADER_PULLBACK = "THEME_LEADER_PULLBACK"
    VWAP_RECLAIM = "VWAP_RECLAIM"
    BREAKOUT_RETEST = "BREAKOUT_RETEST"
    MOMENTUM_CONTINUATION = "MOMENTUM_CONTINUATION"
    THEME_FOLLOWER_EXPANSION = "THEME_FOLLOWER_EXPANSION"
    NO_SETUP = "NO_SETUP"


class OrderPlanStatus(StrEnum):
    PLAN_READY = "PLAN_READY"
    WAIT_RETRY = "WAIT_RETRY"
    DATA_WAIT = "DATA_WAIT"
    BLOCKED_CHASE = "BLOCKED_CHASE"
    BLOCKED_OVERHEAT = "BLOCKED_OVERHEAT"
    BLOCKED_STALE = "BLOCKED_STALE"
    BLOCKED_RISK = "BLOCKED_RISK"
    NO_PLAN = "NO_PLAN"


@dataclass(frozen=True, kw_only=True)
class PriceLocationResult:
    state: PriceLocationState
    metrics: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "state",
            parse_str_enum(self.state, PriceLocationState, "price_location_state"),
        )
        object.__setattr__(self, "reason_codes", tuple(_normalize_reasons(self.reason_codes)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "metrics": normalize_payload(self.metrics),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class EntryTimingInput:
    trade_date: str
    candidate_instance_id: str
    code: str
    name: str
    theme_id: str | None
    theme_name: str | None
    theme_state: str | None
    theme_rank: int | None
    stock_role: str | None
    theme_priority_score: float | None
    current_price: float | None
    prev_close: float | None
    open_price: float | None
    day_high: float | None
    day_low: float | None
    change_rate_pct: float | None
    turnover_krw: float | None
    execution_strength: float | None
    momentum_1m: float | None
    momentum_3m: float | None
    momentum_5m: float | None
    vwap: float | None
    pullback_from_high_pct: float | None
    spread_ticks: int | None
    stale: bool
    vi_active: bool
    upper_limit_near: bool
    theme_reason_codes: Sequence[str] = field(default_factory=tuple)
    candidate_state: str | None = None
    strategy_observation_status: str | None = None
    strategy_setup_type: str | None = None
    strategy_score: float | None = None
    strategy_confidence: float | None = None
    risk_observation_status: str | None = None
    risk_reason_codes: Sequence[str] = field(default_factory=tuple)
    condition_fusion_priority_score: float | None = None
    active_condition_roles: Sequence[str] = field(default_factory=tuple)
    condition_risk_blocked: bool = False
    condition_fusion_reason_codes: Sequence[str] = field(default_factory=tuple)
    condition_names: Sequence[str] = field(default_factory=tuple)
    condition_latest_hit_at: str | None = None
    premarket_gap: float | None = None
    observed_at: datetime | str = field(default_factory=datetime.utcnow)
    best_bid: float | None = None
    best_ask: float | None = None
    tick_age_sec: float | None = None
    source: str = "candidate"
    raw_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        for field_name in (
            "theme_id",
            "theme_name",
            "theme_state",
            "stock_role",
            "candidate_state",
            "strategy_observation_status",
            "strategy_setup_type",
            "risk_observation_status",
            "source",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_non_empty_str(value, field_name))
        if self.theme_rank is not None:
            object.__setattr__(
                self,
                "theme_rank",
                parse_int(self.theme_rank, "theme_rank", min_value=0),
            )
        if self.spread_ticks is not None:
            object.__setattr__(
                self,
                "spread_ticks",
                parse_int(self.spread_ticks, "spread_ticks", min_value=0),
            )
        for field_name in (
            "theme_priority_score",
            "current_price",
            "prev_close",
            "open_price",
            "day_high",
            "day_low",
            "change_rate_pct",
            "turnover_krw",
            "execution_strength",
            "momentum_1m",
            "momentum_3m",
            "momentum_5m",
            "vwap",
            "pullback_from_high_pct",
            "strategy_score",
            "strategy_confidence",
            "condition_fusion_priority_score",
            "premarket_gap",
            "best_bid",
            "best_ask",
            "tick_age_sec",
        ):
            value = getattr(self, field_name)
            if value is not None:
                min_value = (
                    0.0
                    if field_name
                    not in {
                        "change_rate_pct",
                        "momentum_1m",
                        "momentum_3m",
                        "momentum_5m",
                        "premarket_gap",
                    }
                    else None
                )
                object.__setattr__(
                    self,
                    field_name,
                    parse_float(value, field_name, min_value=min_value),
                )
        for field_name in ("stale", "vi_active", "upper_limit_near"):
            object.__setattr__(self, field_name, parse_bool(getattr(self, field_name), field_name))
        object.__setattr__(
            self,
            "theme_reason_codes",
            tuple(_normalize_reasons(self.theme_reason_codes)),
        )
        object.__setattr__(
            self,
            "risk_reason_codes",
            tuple(_normalize_reasons(self.risk_reason_codes)),
        )
        object.__setattr__(
            self,
            "active_condition_roles",
            tuple(_normalize_reasons(self.active_condition_roles)),
        )
        object.__setattr__(
            self,
            "condition_fusion_reason_codes",
            tuple(_normalize_reasons(self.condition_fusion_reason_codes)),
        )
        object.__setattr__(
            self,
            "condition_names",
            tuple(str(value) for value in self.condition_names if str(value).strip()),
        )
        object.__setattr__(
            self,
            "condition_risk_blocked",
            parse_bool(self.condition_risk_blocked, "condition_risk_blocked"),
        )
        if self.condition_latest_hit_at is not None:
            object.__setattr__(
                self,
                "condition_latest_hit_at",
                require_non_empty_str(
                    self.condition_latest_hit_at,
                    "condition_latest_hit_at",
                ),
            )
        object.__setattr__(self, "observed_at", parse_timestamp(self.observed_at, "observed_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "candidate_instance_id": self.candidate_instance_id,
            "code": self.code,
            "name": self.name,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state,
            "theme_rank": self.theme_rank,
            "stock_role": self.stock_role,
            "theme_priority_score": self.theme_priority_score,
            "current_price": self.current_price,
            "prev_close": self.prev_close,
            "open_price": self.open_price,
            "day_high": self.day_high,
            "day_low": self.day_low,
            "change_rate_pct": self.change_rate_pct,
            "turnover_krw": self.turnover_krw,
            "execution_strength": self.execution_strength,
            "momentum_1m": self.momentum_1m,
            "momentum_3m": self.momentum_3m,
            "momentum_5m": self.momentum_5m,
            "vwap": self.vwap,
            "pullback_from_high_pct": self.pullback_from_high_pct,
            "spread_ticks": self.spread_ticks,
            "stale": self.stale,
            "vi_active": self.vi_active,
            "upper_limit_near": self.upper_limit_near,
            "theme_reason_codes": list(self.theme_reason_codes),
            "candidate_state": self.candidate_state,
            "strategy_observation_status": self.strategy_observation_status,
            "strategy_setup_type": self.strategy_setup_type,
            "strategy_score": self.strategy_score,
            "strategy_confidence": self.strategy_confidence,
            "risk_observation_status": self.risk_observation_status,
            "risk_reason_codes": list(self.risk_reason_codes),
            "condition_fusion_priority_score": self.condition_fusion_priority_score,
            "active_condition_roles": list(self.active_condition_roles),
            "condition_risk_blocked": self.condition_risk_blocked,
            "condition_fusion_reason_codes": list(self.condition_fusion_reason_codes),
            "condition_names": list(self.condition_names),
            "condition_latest_hit_at": self.condition_latest_hit_at,
            "premarket_gap": self.premarket_gap,
            "observed_at": datetime_to_wire(self.observed_at),
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "tick_age_sec": self.tick_age_sec,
            "source": self.source,
            "raw_context": normalize_payload(self.raw_context),
        }


@dataclass(frozen=True, kw_only=True)
class EntryTimingEvaluation:
    entry_timing_evaluation_id: str
    trade_date: str
    candidate_instance_id: str
    code: str
    name: str
    evaluated_at: datetime | str
    setup_type: SetupType
    entry_timing_state: EntryTimingState
    price_location_state: PriceLocationState
    status: OrderPlanStatus
    order_plan_id: str | None = None
    reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    observe_only: bool = True
    not_order_intent: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "entry_timing_evaluation_id",
            require_non_empty_str(
                self.entry_timing_evaluation_id,
                "entry_timing_evaluation_id",
            ),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "evaluated_at", parse_timestamp(self.evaluated_at, "evaluated_at"))
        object.__setattr__(
            self,
            "setup_type",
            parse_str_enum(self.setup_type, SetupType, "setup_type"),
        )
        object.__setattr__(
            self,
            "entry_timing_state",
            parse_str_enum(self.entry_timing_state, EntryTimingState, "entry_timing_state"),
        )
        object.__setattr__(
            self,
            "price_location_state",
            parse_str_enum(self.price_location_state, PriceLocationState, "price_location_state"),
        )
        object.__setattr__(self, "status", parse_str_enum(self.status, OrderPlanStatus, "status"))
        if self.order_plan_id is not None:
            object.__setattr__(
                self,
                "order_plan_id",
                require_non_empty_str(self.order_plan_id, "order_plan_id"),
            )
        object.__setattr__(self, "reason_codes", tuple(_normalize_reasons(self.reason_codes)))
        object.__setattr__(self, "observe_only", True)
        object.__setattr__(self, "not_order_intent", True)

    def with_order_plan_id(self, order_plan_id: str | None) -> EntryTimingEvaluation:
        return EntryTimingEvaluation(
            entry_timing_evaluation_id=self.entry_timing_evaluation_id,
            trade_date=self.trade_date,
            candidate_instance_id=self.candidate_instance_id,
            code=self.code,
            name=self.name,
            evaluated_at=self.evaluated_at,
            setup_type=self.setup_type,
            entry_timing_state=self.entry_timing_state,
            price_location_state=self.price_location_state,
            status=self.status,
            order_plan_id=order_plan_id,
            reason_codes=self.reason_codes,
            evidence_json=self.evidence_json,
        )

    def with_status(
        self,
        status: OrderPlanStatus,
        *,
        order_plan_id: str | None = None,
        reason_codes: Sequence[str] | None = None,
    ) -> EntryTimingEvaluation:
        return EntryTimingEvaluation(
            entry_timing_evaluation_id=self.entry_timing_evaluation_id,
            trade_date=self.trade_date,
            candidate_instance_id=self.candidate_instance_id,
            code=self.code,
            name=self.name,
            evaluated_at=self.evaluated_at,
            setup_type=self.setup_type,
            entry_timing_state=self.entry_timing_state,
            price_location_state=self.price_location_state,
            status=status,
            order_plan_id=order_plan_id,
            reason_codes=reason_codes if reason_codes is not None else self.reason_codes,
            evidence_json=self.evidence_json,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_timing_evaluation_id": self.entry_timing_evaluation_id,
            "trade_date": self.trade_date,
            "candidate_instance_id": self.candidate_instance_id,
            "code": self.code,
            "name": self.name,
            "evaluated_at": datetime_to_wire(self.evaluated_at),
            "setup_type": self.setup_type.value,
            "entry_timing_state": self.entry_timing_state.value,
            "price_location_state": self.price_location_state.value,
            "status": self.status.value,
            "order_plan_id": self.order_plan_id,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
            "observe_only": True,
            "not_order_intent": True,
        }


@dataclass(frozen=True, kw_only=True)
class OrderPlanDraft:
    order_plan_id: str
    trade_date: str
    candidate_instance_id: str
    code: str
    name: str
    side: str
    status: OrderPlanStatus
    setup_type: SetupType
    entry_timing_state: EntryTimingState
    price_location_state: PriceLocationState
    theme_id: str | None
    theme_name: str | None
    theme_state: str | None
    theme_rank: int | None
    stock_role: str | None
    priority_score: float | None
    current_price: float
    limit_price: float
    limit_price_source: str
    limit_price_offset_ticks: int
    suggested_quantity: int
    suggested_notional: float
    max_notional: float
    risk_budget_source: str
    expires_at: datetime | str
    idempotency_key: str
    reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    observe_only: bool = True
    not_order_intent: bool = True
    created_at: datetime | str = field(default_factory=datetime.utcnow)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "order_plan_id",
            require_non_empty_str(self.order_plan_id, "order_plan_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        side = require_non_empty_str(self.side, "side").upper()
        if side != "BUY":
            raise ValueError("entry timing order plan drafts support BUY side only")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "status", parse_str_enum(self.status, OrderPlanStatus, "status"))
        object.__setattr__(
            self,
            "setup_type",
            parse_str_enum(self.setup_type, SetupType, "setup_type"),
        )
        object.__setattr__(
            self,
            "entry_timing_state",
            parse_str_enum(self.entry_timing_state, EntryTimingState, "entry_timing_state"),
        )
        object.__setattr__(
            self,
            "price_location_state",
            parse_str_enum(self.price_location_state, PriceLocationState, "price_location_state"),
        )
        for field_name in ("theme_id", "theme_name", "theme_state", "stock_role"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_non_empty_str(value, field_name))
        if self.theme_rank is not None:
            object.__setattr__(
                self,
                "theme_rank",
                parse_int(self.theme_rank, "theme_rank", min_value=0),
            )
        if self.priority_score is not None:
            object.__setattr__(
                self,
                "priority_score",
                parse_float(self.priority_score, "priority_score", min_value=0.0),
            )
        for field_name in ("current_price", "limit_price", "suggested_notional", "max_notional"):
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=0.0),
            )
        if self.current_price <= 0 or self.limit_price <= 0:
            raise ValueError("current_price and limit_price must be > 0")
        object.__setattr__(
            self,
            "limit_price_source",
            require_non_empty_str(self.limit_price_source, "limit_price_source").upper(),
        )
        object.__setattr__(
            self,
            "limit_price_offset_ticks",
            parse_int(self.limit_price_offset_ticks, "limit_price_offset_ticks", min_value=0),
        )
        object.__setattr__(
            self,
            "suggested_quantity",
            parse_int(self.suggested_quantity, "suggested_quantity", min_value=0),
        )
        object.__setattr__(
            self,
            "risk_budget_source",
            require_non_empty_str(self.risk_budget_source, "risk_budget_source").upper(),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            require_non_empty_str(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(self, "reason_codes", tuple(_normalize_reasons(self.reason_codes)))
        object.__setattr__(self, "expires_at", parse_timestamp(self.expires_at, "expires_at"))
        object.__setattr__(self, "created_at", parse_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "observe_only", True)
        object.__setattr__(self, "not_order_intent", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_plan_id": self.order_plan_id,
            "trade_date": self.trade_date,
            "candidate_instance_id": self.candidate_instance_id,
            "code": self.code,
            "name": self.name,
            "side": self.side,
            "status": self.status.value,
            "setup_type": self.setup_type.value,
            "entry_timing_state": self.entry_timing_state.value,
            "price_location_state": self.price_location_state.value,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state,
            "theme_rank": self.theme_rank,
            "stock_role": self.stock_role,
            "priority_score": self.priority_score,
            "current_price": self.current_price,
            "limit_price": self.limit_price,
            "limit_price_source": self.limit_price_source,
            "limit_price_offset_ticks": self.limit_price_offset_ticks,
            "suggested_quantity": self.suggested_quantity,
            "suggested_notional": self.suggested_notional,
            "max_notional": self.max_notional,
            "risk_budget_source": self.risk_budget_source,
            "expires_at": datetime_to_wire(self.expires_at),
            "idempotency_key": self.idempotency_key,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
            "observe_only": True,
            "not_order_intent": True,
            "created_at": datetime_to_wire(self.created_at),
        }


def _normalize_reasons(reasons: Sequence[str]) -> list[str]:
    return [
        *dict.fromkeys(
            require_non_empty_str(reason, "reason_code").upper() for reason in reasons
        )
    ]
