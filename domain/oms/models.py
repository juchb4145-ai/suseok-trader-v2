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
from domain.oms.reasons import DryRunRejectionReason
from domain.oms.sides import DryRunOrderType, DryRunSide
from domain.oms.status import DryRunIntentStatus, DryRunOrderStatus


@dataclass(frozen=True, kw_only=True)
class DryRunIntent:
    dry_run_intent_id: str
    candidate_instance_id: str
    strategy_observation_id: str | None
    risk_observation_id: str | None
    trade_date: str
    code: str
    name: str
    side: DryRunSide | str = DryRunSide.BUY
    order_type: DryRunOrderType | str = DryRunOrderType.MARKET_SIM
    intended_price: float = 0.0
    quantity: int = 0
    notional: float = 0.0
    status: DryRunIntentStatus | str = DryRunIntentStatus.CREATED
    reason_codes: Sequence[DryRunRejectionReason | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | str | None = None
    expires_at: datetime | str | None = None
    source: str = "manual"
    observe_only: bool = True
    dry_run_only: bool = True
    live_order_allowed: bool = False
    gateway_command_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dry_run_intent_id",
            require_non_empty_str(self.dry_run_intent_id, "dry_run_intent_id"),
        )
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
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "side", parse_str_enum(self.side, DryRunSide, "side"))
        object.__setattr__(
            self,
            "order_type",
            parse_str_enum(self.order_type, DryRunOrderType, "order_type"),
        )
        object.__setattr__(
            self,
            "intended_price",
            parse_float(self.intended_price, "intended_price", min_value=0.0),
        )
        object.__setattr__(
            self,
            "quantity",
            parse_int(self.quantity, "quantity", min_value=0),
        )
        object.__setattr__(self, "notional", parse_float(self.notional, "notional", min_value=0.0))
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, DryRunIntentStatus, "status"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        if self.created_at is not None:
            object.__setattr__(self, "created_at", parse_timestamp(self.created_at, "created_at"))
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", parse_timestamp(self.expires_at, "expires_at"))
        object.__setattr__(self, "source", require_non_empty_str(self.source, "source"))
        object.__setattr__(self, "observe_only", True)
        object.__setattr__(self, "dry_run_only", True)
        object.__setattr__(self, "live_order_allowed", False)
        object.__setattr__(self, "gateway_command_allowed", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_intent_id": self.dry_run_intent_id,
            "candidate_instance_id": self.candidate_instance_id,
            "strategy_observation_id": self.strategy_observation_id,
            "risk_observation_id": self.risk_observation_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "intended_price": self.intended_price,
            "quantity": self.quantity,
            "notional": self.notional,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "created_at": _optional_wire(self.created_at),
            "expires_at": _optional_wire(self.expires_at),
            "source": self.source,
            "observe_only": True,
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunIntent:
        mapping = require_mapping(data, "DryRunIntent")
        return cls(
            dry_run_intent_id=mapping["dry_run_intent_id"],
            candidate_instance_id=mapping["candidate_instance_id"],
            strategy_observation_id=mapping.get("strategy_observation_id"),
            risk_observation_id=mapping.get("risk_observation_id"),
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            side=mapping.get("side", DryRunSide.BUY.value),
            order_type=mapping.get("order_type", DryRunOrderType.MARKET_SIM.value),
            intended_price=mapping.get("intended_price", 0),
            quantity=mapping.get("quantity", 0),
            notional=mapping.get("notional", 0),
            status=mapping.get("status", DryRunIntentStatus.CREATED.value),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            created_at=mapping.get("created_at"),
            expires_at=mapping.get("expires_at"),
            source=mapping.get("source", "manual"),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunOrder:
    dry_run_order_id: str
    dry_run_intent_id: str
    trade_date: str
    code: str
    name: str
    side: DryRunSide | str = DryRunSide.BUY
    order_type: DryRunOrderType | str = DryRunOrderType.MARKET_SIM
    quantity: int = 0
    requested_price: float = 0.0
    simulated_fill_price: float | None = None
    filled_quantity: int = 0
    remaining_quantity: int = 0
    status: DryRunOrderStatus | str = DryRunOrderStatus.CREATED
    created_at: datetime | str | None = None
    simulated_submitted_at: datetime | str | None = None
    simulated_filled_at: datetime | str | None = None
    expires_at: datetime | str | None = None
    reason_codes: Sequence[DryRunRejectionReason | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    dry_run_only: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dry_run_order_id",
            require_non_empty_str(self.dry_run_order_id, "dry_run_order_id"),
        )
        object.__setattr__(
            self,
            "dry_run_intent_id",
            require_non_empty_str(self.dry_run_intent_id, "dry_run_intent_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "side", parse_str_enum(self.side, DryRunSide, "side"))
        object.__setattr__(
            self,
            "order_type",
            parse_str_enum(self.order_type, DryRunOrderType, "order_type"),
        )
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=0))
        object.__setattr__(
            self,
            "requested_price",
            parse_float(self.requested_price, "requested_price", min_value=0.0),
        )
        if self.simulated_fill_price is not None:
            object.__setattr__(
                self,
                "simulated_fill_price",
                parse_float(self.simulated_fill_price, "simulated_fill_price", min_value=0.0),
            )
        object.__setattr__(
            self,
            "filled_quantity",
            parse_int(self.filled_quantity, "filled_quantity", min_value=0),
        )
        object.__setattr__(
            self,
            "remaining_quantity",
            parse_int(self.remaining_quantity, "remaining_quantity", min_value=0),
        )
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, DryRunOrderStatus, "status"),
        )
        for field_name in (
            "created_at",
            "simulated_submitted_at",
            "simulated_filled_at",
            "expires_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        object.__setattr__(self, "dry_run_only", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_order_id": self.dry_run_order_id,
            "dry_run_intent_id": self.dry_run_intent_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "requested_price": self.requested_price,
            "simulated_fill_price": self.simulated_fill_price,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "status": self.status.value,
            "created_at": _optional_wire(self.created_at),
            "simulated_submitted_at": _optional_wire(self.simulated_submitted_at),
            "simulated_filled_at": _optional_wire(self.simulated_filled_at),
            "expires_at": _optional_wire(self.expires_at),
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunOrder:
        mapping = require_mapping(data, "DryRunOrder")
        return cls(
            dry_run_order_id=mapping["dry_run_order_id"],
            dry_run_intent_id=mapping["dry_run_intent_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            side=mapping.get("side", DryRunSide.BUY.value),
            order_type=mapping.get("order_type", DryRunOrderType.MARKET_SIM.value),
            quantity=mapping.get("quantity", 0),
            requested_price=mapping.get("requested_price", 0),
            simulated_fill_price=mapping.get("simulated_fill_price"),
            filled_quantity=mapping.get("filled_quantity", 0),
            remaining_quantity=mapping.get("remaining_quantity", 0),
            status=mapping.get("status", DryRunOrderStatus.CREATED.value),
            created_at=mapping.get("created_at"),
            simulated_submitted_at=mapping.get("simulated_submitted_at"),
            simulated_filled_at=mapping.get("simulated_filled_at"),
            expires_at=mapping.get("expires_at"),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunExecution:
    dry_run_execution_id: str
    dry_run_order_id: str
    dry_run_intent_id: str
    trade_date: str
    code: str
    side: DryRunSide | str
    quantity: int
    price: float
    notional: float
    commission: float = 0.0
    tax: float = 0.0
    executed_at: datetime | str | None = None
    execution_type: str = "SIMULATED"
    dry_run_only: bool = True

    def __post_init__(self) -> None:
        for field_name in (
            "dry_run_execution_id",
            "dry_run_order_id",
            "dry_run_intent_id",
            "trade_date",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, DryRunSide, "side"))
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=1))
        for field_name in ("price", "notional", "commission", "tax"):
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=0.0),
            )
        if self.executed_at is not None:
            object.__setattr__(
                self,
                "executed_at",
                parse_timestamp(self.executed_at, "executed_at"),
            )
        object.__setattr__(
            self,
            "execution_type",
            require_non_empty_str(self.execution_type, "execution_type").upper(),
        )
        object.__setattr__(self, "dry_run_only", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_execution_id": self.dry_run_execution_id,
            "dry_run_order_id": self.dry_run_order_id,
            "dry_run_intent_id": self.dry_run_intent_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "notional": self.notional,
            "commission": self.commission,
            "tax": self.tax,
            "executed_at": _optional_wire(self.executed_at),
            "execution_type": self.execution_type,
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExecution:
        mapping = require_mapping(data, "DryRunExecution")
        return cls(
            dry_run_execution_id=mapping["dry_run_execution_id"],
            dry_run_order_id=mapping["dry_run_order_id"],
            dry_run_intent_id=mapping["dry_run_intent_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            side=mapping["side"],
            quantity=mapping["quantity"],
            price=mapping["price"],
            notional=mapping["notional"],
            commission=mapping.get("commission", 0),
            tax=mapping.get("tax", 0),
            executed_at=mapping.get("executed_at"),
            execution_type=mapping.get("execution_type", "SIMULATED"),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunPosition:
    dry_run_position_id: str
    trade_date: str
    code: str
    name: str
    quantity: int
    avg_price: float
    invested_notional: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    last_price: float | None = None
    status: str = "OPEN"
    opened_at: datetime | str | None = None
    updated_at: datetime | str | None = None
    closed_at: datetime | str | None = None
    dry_run_only: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dry_run_position_id",
            require_non_empty_str(self.dry_run_position_id, "dry_run_position_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=0))
        for field_name in ("avg_price", "invested_notional"):
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=0.0),
            )
        for field_name in ("realized_pnl", "unrealized_pnl"):
            object.__setattr__(self, field_name, parse_float(getattr(self, field_name), field_name))
        if self.last_price is not None:
            object.__setattr__(
                self,
                "last_price",
                parse_float(self.last_price, "last_price", min_value=0.0),
            )
        object.__setattr__(self, "status", require_non_empty_str(self.status, "status").upper())
        for field_name in ("opened_at", "updated_at", "closed_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(self, "dry_run_only", True)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_position_id": self.dry_run_position_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "quantity": self.quantity,
            "avg_price": self.avg_price,
            "invested_notional": self.invested_notional,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "last_price": self.last_price,
            "status": self.status,
            "opened_at": _optional_wire(self.opened_at),
            "updated_at": _optional_wire(self.updated_at),
            "closed_at": _optional_wire(self.closed_at),
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunPosition:
        mapping = require_mapping(data, "DryRunPosition")
        return cls(
            dry_run_position_id=mapping["dry_run_position_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            quantity=mapping["quantity"],
            avg_price=mapping["avg_price"],
            invested_notional=mapping["invested_notional"],
            realized_pnl=mapping.get("realized_pnl", 0),
            unrealized_pnl=mapping.get("unrealized_pnl", 0),
            last_price=mapping.get("last_price"),
            status=mapping.get("status", "OPEN"),
            opened_at=mapping.get("opened_at"),
            updated_at=mapping.get("updated_at"),
            closed_at=mapping.get("closed_at"),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunEligibility:
    eligible: bool
    candidate_instance_id: str
    strategy_observation_id: str | None
    risk_observation_id: str | None
    status: str
    reason_codes: Sequence[DryRunRejectionReason | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    safety_gate_result: Mapping[str, Any] = field(default_factory=dict)
    computed_at: datetime | str | None = None

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
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        if self.computed_at is not None:
            object.__setattr__(
                self,
                "computed_at",
                parse_timestamp(self.computed_at, "computed_at"),
            )

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
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunEligibility:
        mapping = require_mapping(data, "DryRunEligibility")
        return cls(
            eligible=mapping["eligible"],
            candidate_instance_id=mapping["candidate_instance_id"],
            strategy_observation_id=mapping.get("strategy_observation_id"),
            risk_observation_id=mapping.get("risk_observation_id"),
            status=mapping["status"],
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            safety_gate_result=mapping.get("safety_gate_result", {}),
            computed_at=mapping.get("computed_at"),
        )


def _reason_value(reason: DryRunRejectionReason | str) -> str:
    if isinstance(reason, DryRunRejectionReason):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()


def _optional_wire(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return datetime_to_wire(parse_timestamp(value, "timestamp"))
