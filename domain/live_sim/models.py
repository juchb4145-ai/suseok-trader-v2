from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    optional_non_empty_str,
    parse_bool,
    parse_float,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_mapping,
    require_non_empty_str,
    validate_stock_code,
)
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import (
    LiveSimIntentStatus,
    LiveSimOrderStatus,
    LiveSimOrderType,
    LiveSimSide,
)


@dataclass(frozen=True, kw_only=True)
class LiveSimEligibility:
    eligible: bool
    candidate_instance_id: str
    strategy_observation_id: str | None
    risk_observation_id: str | None
    status: str
    reason_codes: Sequence[LiveSimReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    safety_gate_result: Mapping[str, Any] = field(default_factory=dict)
    computed_at: datetime | str | None = None
    live_sim_only: bool = True
    live_real_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "eligible", parse_bool(self.eligible, "eligible"))
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(
            self,
            "strategy_observation_id",
            optional_non_empty_str(self.strategy_observation_id, "strategy_observation_id"),
        )
        object.__setattr__(
            self,
            "risk_observation_id",
            optional_non_empty_str(self.risk_observation_id, "risk_observation_id"),
        )
        object.__setattr__(self, "status", require_non_empty_str(self.status, "status").upper())
        object.__setattr__(
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        if self.computed_at is not None:
            object.__setattr__(
                self, "computed_at", parse_timestamp(self.computed_at, "computed_at")
            )
        object.__setattr__(self, "live_sim_only", True)
        object.__setattr__(self, "live_real_allowed", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "candidate_instance_id": self.candidate_instance_id,
            "strategy_observation_id": self.strategy_observation_id,
            "risk_observation_id": self.risk_observation_id,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "safety_gate_result": normalize_payload(dict(self.safety_gate_result)),
            "computed_at": _optional_wire(self.computed_at),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }


@dataclass(frozen=True, kw_only=True)
class LiveSimIntent:
    live_sim_intent_id: str
    candidate_instance_id: str
    strategy_observation_id: str | None
    risk_observation_id: str | None
    trade_date: str
    account_id: str
    code: str
    name: str
    order_plan_id: str | None = None
    side: LiveSimSide | str = LiveSimSide.BUY
    order_type: LiveSimOrderType | str = LiveSimOrderType.LIMIT
    quantity: int = 0
    limit_price: float | None = None
    notional: float = 0.0
    status: LiveSimIntentStatus | str = LiveSimIntentStatus.CREATED
    reason_codes: Sequence[LiveSimReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""
    dry_run_intent_id: str | None = None
    dry_run_order_id: str | None = None
    created_at: datetime | str | None = None
    expires_at: datetime | str | None = None
    gateway_command_id: str | None = None
    live_sim_only: bool = True
    live_real_allowed: bool = False
    broker_order_sent: bool = False

    def __post_init__(self) -> None:
        for field_name in (
            "live_sim_intent_id",
            "candidate_instance_id",
            "trade_date",
            "account_id",
            "name",
        ):
            object.__setattr__(
                self, field_name, require_non_empty_str(getattr(self, field_name), field_name)
            )
        object.__setattr__(
            self,
            "strategy_observation_id",
            optional_non_empty_str(self.strategy_observation_id, "strategy_observation_id"),
        )
        object.__setattr__(
            self,
            "risk_observation_id",
            optional_non_empty_str(self.risk_observation_id, "risk_observation_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, LiveSimSide, "side"))
        object.__setattr__(
            self, "order_type", parse_str_enum(self.order_type, LiveSimOrderType, "order_type")
        )
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=0))
        if self.limit_price is not None:
            object.__setattr__(
                self, "limit_price", parse_float(self.limit_price, "limit_price", min_value=0.0)
            )
        object.__setattr__(self, "notional", parse_float(self.notional, "notional", min_value=0.0))
        object.__setattr__(
            self, "status", parse_str_enum(self.status, LiveSimIntentStatus, "status")
        )
        object.__setattr__(
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        object.__setattr__(
            self, "idempotency_key", require_non_empty_str(self.idempotency_key, "idempotency_key")
        )
        for field_name in (
            "order_plan_id",
            "dry_run_intent_id",
            "dry_run_order_id",
            "gateway_command_id",
        ):
            object.__setattr__(
                self, field_name, optional_non_empty_str(getattr(self, field_name), field_name)
            )
        for field_name in ("created_at", "expires_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(self, "live_sim_only", True)
        object.__setattr__(self, "live_real_allowed", False)
        object.__setattr__(
            self, "broker_order_sent", parse_bool(self.broker_order_sent, "broker_order_sent")
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "live_sim_intent_id": self.live_sim_intent_id,
            "candidate_instance_id": self.candidate_instance_id,
            "strategy_observation_id": self.strategy_observation_id,
            "risk_observation_id": self.risk_observation_id,
            "order_plan_id": self.order_plan_id,
            "dry_run_intent_id": self.dry_run_intent_id,
            "dry_run_order_id": self.dry_run_order_id,
            "trade_date": self.trade_date,
            "account_id": self.account_id,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "notional": self.notional,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "idempotency_key": self.idempotency_key,
            "created_at": _optional_wire(self.created_at),
            "expires_at": _optional_wire(self.expires_at),
            "gateway_command_id": self.gateway_command_id,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_sent": self.broker_order_sent,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LiveSimIntent:
        mapping = require_mapping(data, "LiveSimIntent")
        return cls(
            live_sim_intent_id=mapping["live_sim_intent_id"],
            candidate_instance_id=mapping["candidate_instance_id"],
            strategy_observation_id=mapping.get("strategy_observation_id"),
            risk_observation_id=mapping.get("risk_observation_id"),
            order_plan_id=mapping.get("order_plan_id"),
            dry_run_intent_id=mapping.get("dry_run_intent_id"),
            dry_run_order_id=mapping.get("dry_run_order_id"),
            trade_date=mapping["trade_date"],
            account_id=mapping["account_id"],
            code=mapping["code"],
            name=mapping["name"],
            side=mapping.get("side", LiveSimSide.BUY.value),
            order_type=mapping.get("order_type", LiveSimOrderType.LIMIT.value),
            quantity=mapping.get("quantity", 0),
            limit_price=mapping.get("limit_price"),
            notional=mapping.get("notional", 0),
            status=mapping.get("status", LiveSimIntentStatus.CREATED.value),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            idempotency_key=mapping["idempotency_key"],
            created_at=mapping.get("created_at"),
            expires_at=mapping.get("expires_at"),
            gateway_command_id=mapping.get("gateway_command_id"),
            broker_order_sent=mapping.get("broker_order_sent", False),
        )


@dataclass(frozen=True, kw_only=True)
class LiveSimOrderRecord:
    live_sim_order_id: str
    live_sim_intent_id: str
    gateway_command_id: str | None
    account_id: str
    code: str
    name: str
    side: LiveSimSide | str
    order_type: LiveSimOrderType | str
    quantity: int
    limit_price: float | None
    notional: float
    status: LiveSimOrderStatus | str
    broker_order_no: str | None = None
    broker_result_code: str | None = None
    broker_message: str | None = None
    filled_quantity: int = 0
    remaining_quantity: int = 0
    avg_fill_price: float | None = None
    idempotency_key: str = ""
    created_at: datetime | str | None = None
    command_queued_at: datetime | str | None = None
    command_dispatched_at: datetime | str | None = None
    broker_acked_at: datetime | str | None = None
    last_event_at: datetime | str | None = None
    live_sim_only: bool = True
    live_real_allowed: bool = False

    def __post_init__(self) -> None:
        for field_name in ("live_sim_order_id", "live_sim_intent_id", "account_id", "name"):
            object.__setattr__(
                self, field_name, require_non_empty_str(getattr(self, field_name), field_name)
            )
        object.__setattr__(
            self,
            "gateway_command_id",
            optional_non_empty_str(self.gateway_command_id, "gateway_command_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, LiveSimSide, "side"))
        object.__setattr__(
            self, "order_type", parse_str_enum(self.order_type, LiveSimOrderType, "order_type")
        )
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=0))
        if self.limit_price is not None:
            object.__setattr__(
                self, "limit_price", parse_float(self.limit_price, "limit_price", min_value=0.0)
            )
        object.__setattr__(self, "notional", parse_float(self.notional, "notional", min_value=0.0))
        object.__setattr__(
            self, "status", parse_str_enum(self.status, LiveSimOrderStatus, "status")
        )
        object.__setattr__(
            self, "filled_quantity", parse_int(self.filled_quantity, "filled_quantity", min_value=0)
        )
        object.__setattr__(
            self,
            "remaining_quantity",
            parse_int(self.remaining_quantity, "remaining_quantity", min_value=0),
        )
        if self.avg_fill_price is not None:
            object.__setattr__(
                self,
                "avg_fill_price",
                parse_float(self.avg_fill_price, "avg_fill_price", min_value=0.0),
            )
        object.__setattr__(
            self, "idempotency_key", require_non_empty_str(self.idempotency_key, "idempotency_key")
        )
        for field_name in ("broker_order_no", "broker_result_code", "broker_message"):
            object.__setattr__(
                self, field_name, optional_non_empty_str(getattr(self, field_name), field_name)
            )
        for field_name in (
            "created_at",
            "command_queued_at",
            "command_dispatched_at",
            "broker_acked_at",
            "last_event_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(self, "live_sim_only", True)
        object.__setattr__(self, "live_real_allowed", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "live_sim_order_id": self.live_sim_order_id,
            "live_sim_intent_id": self.live_sim_intent_id,
            "gateway_command_id": self.gateway_command_id,
            "account_id": self.account_id,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "notional": self.notional,
            "status": self.status.value,
            "broker_order_no": self.broker_order_no,
            "broker_result_code": self.broker_result_code,
            "broker_message": self.broker_message,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "avg_fill_price": self.avg_fill_price,
            "created_at": _optional_wire(self.created_at),
            "command_queued_at": _optional_wire(self.command_queued_at),
            "command_dispatched_at": _optional_wire(self.command_dispatched_at),
            "broker_acked_at": _optional_wire(self.broker_acked_at),
            "last_event_at": _optional_wire(self.last_event_at),
            "idempotency_key": self.idempotency_key,
            "live_sim_only": True,
            "live_real_allowed": False,
        }


@dataclass(frozen=True, kw_only=True)
class LiveSimExecutionRecord:
    live_sim_execution_id: str
    live_sim_order_id: str | None
    live_sim_intent_id: str | None
    broker_order_no: str | None
    account_id: str
    code: str
    side: LiveSimSide | str
    quantity: int
    price: float
    notional: float
    executed_at: datetime | str
    raw_event_json: Mapping[str, Any] = field(default_factory=dict)
    live_sim_only: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "live_sim_execution_id",
            require_non_empty_str(self.live_sim_execution_id, "live_sim_execution_id"),
        )
        for field_name in ("live_sim_order_id", "live_sim_intent_id", "broker_order_no"):
            object.__setattr__(
                self, field_name, optional_non_empty_str(getattr(self, field_name), field_name)
            )
        object.__setattr__(self, "account_id", require_non_empty_str(self.account_id, "account_id"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, LiveSimSide, "side"))
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=1))
        object.__setattr__(self, "price", parse_float(self.price, "price", min_value=0.0))
        object.__setattr__(self, "notional", parse_float(self.notional, "notional", min_value=0.0))
        object.__setattr__(self, "executed_at", parse_timestamp(self.executed_at, "executed_at"))
        object.__setattr__(self, "live_sim_only", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "live_sim_execution_id": self.live_sim_execution_id,
            "live_sim_order_id": self.live_sim_order_id,
            "live_sim_intent_id": self.live_sim_intent_id,
            "broker_order_no": self.broker_order_no,
            "account_id": self.account_id,
            "code": self.code,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "notional": self.notional,
            "executed_at": _optional_wire(self.executed_at),
            "raw_event_json": normalize_payload(dict(self.raw_event_json)),
            "live_sim_only": True,
            "live_real_allowed": False,
        }


@dataclass(frozen=True, kw_only=True)
class LiveSimReconcileSnapshot:
    reconcile_id: str
    account_id: str
    trade_date: str
    code: str | None = None
    broker_open_order_count: int = 0
    broker_position_count: int = 0
    local_open_order_count: int = 0
    local_position_count: int = 0
    mismatch_count: int = 0
    status: str = "LOCAL_ONLY"
    snapshot_json: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | str | None = None
    live_sim_only: bool = True

    def __post_init__(self) -> None:
        for field_name in ("reconcile_id", "account_id", "trade_date", "status"):
            object.__setattr__(
                self, field_name, require_non_empty_str(getattr(self, field_name), field_name)
            )
        if self.code is not None:
            object.__setattr__(self, "code", validate_stock_code(self.code))
        for field_name in (
            "broker_open_order_count",
            "broker_position_count",
            "local_open_order_count",
            "local_position_count",
            "mismatch_count",
        ):
            object.__setattr__(
                self, field_name, parse_int(getattr(self, field_name), field_name, min_value=0)
            )
        if self.created_at is not None:
            object.__setattr__(self, "created_at", parse_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "status", self.status.upper())
        object.__setattr__(self, "live_sim_only", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reconcile_id": self.reconcile_id,
            "account_id": self.account_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "broker_open_order_count": self.broker_open_order_count,
            "broker_position_count": self.broker_position_count,
            "local_open_order_count": self.local_open_order_count,
            "local_position_count": self.local_position_count,
            "mismatch_count": self.mismatch_count,
            "status": self.status,
            "snapshot_json": normalize_payload(dict(self.snapshot_json)),
            "created_at": _optional_wire(self.created_at),
            "live_sim_only": True,
            "live_real_allowed": False,
        }


def _reason_value(reason: LiveSimReasonCode | str) -> str:
    if isinstance(reason, LiveSimReasonCode):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()


def _optional_wire(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return datetime_to_wire(parse_timestamp(value, "timestamp"))
