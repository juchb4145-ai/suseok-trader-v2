from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_bool,
    parse_float,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_mapping,
    require_non_empty_str,
    validate_stock_code,
)
from domain.exit.reasons import DryRunExitReasonCode
from domain.exit.status import (
    DryRunExitEvaluationStatus,
    DryRunExitIntentStatus,
    DryRunExitOrderStatus,
    DryRunExitSignalType,
)
from domain.oms.sides import DryRunSide


@dataclass(frozen=True, kw_only=True)
class DryRunExitEvaluation:
    exit_evaluation_id: str
    dry_run_position_id: str
    trade_date: str
    code: str
    name: str
    evaluated_at: datetime | str | None = None
    status: DryRunExitEvaluationStatus | str = DryRunExitEvaluationStatus.NOT_EVALUATED
    primary_signal_type: DryRunExitSignalType | str | None = None
    signal_count: int = 0
    caution_count: int = 0
    hold_count: int = 0
    last_price: float | None = None
    avg_price: float = 0.0
    quantity: int = 0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    high_watermark_price: float | None = None
    drawdown_from_high_pct: float | None = None
    hold_sec: float | None = None
    reason_codes: Sequence[DryRunExitReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    config_version: str = "exit_dry_run_v1"
    dry_run_only: bool = True
    broker_order_allowed: bool = False
    gateway_command_allowed: bool = False
    invalid: bool = False

    def __post_init__(self) -> None:
        _reject_invalid_flag(self.invalid)
        object.__setattr__(
            self,
            "exit_evaluation_id",
            require_non_empty_str(self.exit_evaluation_id, "exit_evaluation_id"),
        )
        object.__setattr__(
            self,
            "dry_run_position_id",
            require_non_empty_str(self.dry_run_position_id, "dry_run_position_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        if self.evaluated_at is not None:
            object.__setattr__(
                self, "evaluated_at", parse_timestamp(self.evaluated_at, "evaluated_at")
            )
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, DryRunExitEvaluationStatus, "status"),
        )
        if self.primary_signal_type is not None:
            object.__setattr__(
                self,
                "primary_signal_type",
                parse_str_enum(
                    self.primary_signal_type, DryRunExitSignalType, "primary_signal_type"
                ),
            )
        for field_name in ("signal_count", "caution_count", "hold_count", "quantity"):
            object.__setattr__(
                self, field_name, parse_int(getattr(self, field_name), field_name, min_value=0)
            )
        for field_name in ("avg_price",):
            object.__setattr__(
                self, field_name, parse_float(getattr(self, field_name), field_name, min_value=0.0)
            )
        for field_name in ("unrealized_pnl", "unrealized_pnl_pct"):
            object.__setattr__(self, field_name, parse_float(getattr(self, field_name), field_name))
        for field_name in (
            "last_price",
            "high_watermark_price",
            "drawdown_from_high_pct",
            "hold_sec",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_float(value, field_name, min_value=0.0))
        object.__setattr__(
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        object.__setattr__(
            self, "config_version", require_non_empty_str(self.config_version, "config_version")
        )
        object.__setattr__(self, "dry_run_only", True)
        object.__setattr__(self, "broker_order_allowed", False)
        object.__setattr__(self, "gateway_command_allowed", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "exit_evaluation_id": self.exit_evaluation_id,
            "dry_run_position_id": self.dry_run_position_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "evaluated_at": _optional_wire(self.evaluated_at),
            "status": self.status.value,
            "primary_signal_type": _optional_enum_value(self.primary_signal_type),
            "signal_count": self.signal_count,
            "caution_count": self.caution_count,
            "hold_count": self.hold_count,
            "last_price": self.last_price,
            "avg_price": self.avg_price,
            "quantity": self.quantity,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "high_watermark_price": self.high_watermark_price,
            "drawdown_from_high_pct": self.drawdown_from_high_pct,
            "hold_sec": self.hold_sec,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "config_version": self.config_version,
            "dry_run_only": True,
            "broker_order_allowed": False,
            "gateway_command_allowed": False,
            "live_order_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExitEvaluation:
        mapping = require_mapping(data, "DryRunExitEvaluation")
        return cls(
            exit_evaluation_id=mapping["exit_evaluation_id"],
            dry_run_position_id=mapping["dry_run_position_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            evaluated_at=mapping.get("evaluated_at"),
            status=mapping.get("status", DryRunExitEvaluationStatus.NOT_EVALUATED.value),
            primary_signal_type=mapping.get("primary_signal_type"),
            signal_count=mapping.get("signal_count", 0),
            caution_count=mapping.get("caution_count", 0),
            hold_count=mapping.get("hold_count", 0),
            last_price=mapping.get("last_price"),
            avg_price=mapping.get("avg_price", 0),
            quantity=mapping.get("quantity", 0),
            unrealized_pnl=mapping.get("unrealized_pnl", 0),
            unrealized_pnl_pct=mapping.get("unrealized_pnl_pct", 0),
            high_watermark_price=mapping.get("high_watermark_price"),
            drawdown_from_high_pct=mapping.get("drawdown_from_high_pct"),
            hold_sec=mapping.get("hold_sec"),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            config_version=mapping.get("config_version", "exit_dry_run_v1"),
            invalid=mapping.get("invalid", False),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunExitSignal:
    exit_signal_id: str
    exit_evaluation_id: str
    dry_run_position_id: str
    signal_type: DryRunExitSignalType | str
    status: str
    severity: str
    reason_codes: Sequence[DryRunExitReasonCode | str] = field(default_factory=tuple)
    trigger_price: float | None = None
    current_price: float | None = None
    threshold_value: float | None = None
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    observed_at: datetime | str | None = None
    invalid: bool = False

    def __post_init__(self) -> None:
        _reject_invalid_flag(self.invalid)
        for field_name in ("exit_signal_id", "exit_evaluation_id", "dry_run_position_id"):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "signal_type",
            parse_str_enum(self.signal_type, DryRunExitSignalType, "signal_type"),
        )
        object.__setattr__(self, "status", require_non_empty_str(self.status, "status").upper())
        object.__setattr__(
            self, "severity", require_non_empty_str(self.severity, "severity").upper()
        )
        object.__setattr__(
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        for field_name in ("trigger_price", "current_price", "threshold_value"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_float(value, field_name, min_value=0.0))
        if self.observed_at is not None:
            object.__setattr__(
                self, "observed_at", parse_timestamp(self.observed_at, "observed_at")
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "exit_signal_id": self.exit_signal_id,
            "exit_evaluation_id": self.exit_evaluation_id,
            "dry_run_position_id": self.dry_run_position_id,
            "signal_type": self.signal_type.value,
            "status": self.status,
            "severity": self.severity,
            "reason_codes": list(self.reason_codes),
            "trigger_price": self.trigger_price,
            "current_price": self.current_price,
            "threshold_value": self.threshold_value,
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "observed_at": _optional_wire(self.observed_at),
            "dry_run_only": True,
            "close_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExitSignal:
        mapping = require_mapping(data, "DryRunExitSignal")
        return cls(
            exit_signal_id=mapping["exit_signal_id"],
            exit_evaluation_id=mapping["exit_evaluation_id"],
            dry_run_position_id=mapping["dry_run_position_id"],
            signal_type=mapping["signal_type"],
            status=mapping["status"],
            severity=mapping["severity"],
            reason_codes=mapping.get("reason_codes", ()),
            trigger_price=mapping.get("trigger_price"),
            current_price=mapping.get("current_price"),
            threshold_value=mapping.get("threshold_value"),
            evidence_json=mapping.get("evidence_json", {}),
            observed_at=mapping.get("observed_at"),
            invalid=mapping.get("invalid", False),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunExitIntent:
    dry_run_exit_intent_id: str
    exit_evaluation_id: str
    dry_run_position_id: str
    trade_date: str
    code: str
    name: str
    side: DryRunSide | str = DryRunSide.SELL
    quantity: int = 0
    intended_price: float = 0.0
    notional: float = 0.0
    status: DryRunExitIntentStatus | str = DryRunExitIntentStatus.CREATED
    reason_codes: Sequence[DryRunExitReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | str | None = None
    expires_at: datetime | str | None = None
    dry_run_only: bool = True
    close_only: bool = True
    live_order_allowed: bool = False
    gateway_command_allowed: bool = False
    broker_order_sent: bool = False
    invalid: bool = False

    def __post_init__(self) -> None:
        _reject_invalid_flag(self.invalid)
        for field_name in (
            "dry_run_exit_intent_id",
            "exit_evaluation_id",
            "dry_run_position_id",
            "trade_date",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "side", DryRunSide.SELL)
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=0))
        object.__setattr__(
            self,
            "intended_price",
            parse_float(self.intended_price, "intended_price", min_value=0.0),
        )
        object.__setattr__(self, "notional", parse_float(self.notional, "notional", min_value=0.0))
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, DryRunExitIntentStatus, "status"),
        )
        object.__setattr__(
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        for field_name in ("created_at", "expires_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(self, "dry_run_only", True)
        object.__setattr__(self, "close_only", True)
        object.__setattr__(self, "live_order_allowed", False)
        object.__setattr__(self, "gateway_command_allowed", False)
        object.__setattr__(self, "broker_order_sent", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_exit_intent_id": self.dry_run_exit_intent_id,
            "exit_evaluation_id": self.exit_evaluation_id,
            "dry_run_position_id": self.dry_run_position_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
            "quantity": self.quantity,
            "intended_price": self.intended_price,
            "notional": self.notional,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(dict(self.evidence_json)),
            "created_at": _optional_wire(self.created_at),
            "expires_at": _optional_wire(self.expires_at),
            "dry_run_only": True,
            "close_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExitIntent:
        mapping = require_mapping(data, "DryRunExitIntent")
        return cls(
            dry_run_exit_intent_id=mapping["dry_run_exit_intent_id"],
            exit_evaluation_id=mapping["exit_evaluation_id"],
            dry_run_position_id=mapping["dry_run_position_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            quantity=mapping.get("quantity", 0),
            intended_price=mapping.get("intended_price", 0),
            notional=mapping.get("notional", 0),
            status=mapping.get("status", DryRunExitIntentStatus.CREATED.value),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            created_at=mapping.get("created_at"),
            expires_at=mapping.get("expires_at"),
            invalid=mapping.get("invalid", False),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunExitOrder:
    dry_run_exit_order_id: str
    dry_run_exit_intent_id: str
    dry_run_position_id: str
    trade_date: str
    code: str
    name: str
    side: DryRunSide | str = DryRunSide.SELL
    quantity: int = 0
    requested_price: float = 0.0
    simulated_fill_price: float | None = None
    filled_quantity: int = 0
    remaining_quantity: int = 0
    status: DryRunExitOrderStatus | str = DryRunExitOrderStatus.CREATED
    created_at: datetime | str | None = None
    simulated_submitted_at: datetime | str | None = None
    simulated_filled_at: datetime | str | None = None
    expires_at: datetime | str | None = None
    reason_codes: Sequence[DryRunExitReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    dry_run_only: bool = True
    close_only: bool = True
    broker_order_sent: bool = False
    invalid: bool = False

    def __post_init__(self) -> None:
        _reject_invalid_flag(self.invalid)
        for field_name in (
            "dry_run_exit_order_id",
            "dry_run_exit_intent_id",
            "dry_run_position_id",
            "trade_date",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "side", DryRunSide.SELL)
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
        for field_name in ("filled_quantity", "remaining_quantity"):
            object.__setattr__(
                self, field_name, parse_int(getattr(self, field_name), field_name, min_value=0)
            )
        object.__setattr__(
            self, "status", parse_str_enum(self.status, DryRunExitOrderStatus, "status")
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
            self, "reason_codes", tuple(_reason_value(reason) for reason in self.reason_codes)
        )
        object.__setattr__(self, "dry_run_only", True)
        object.__setattr__(self, "close_only", True)
        object.__setattr__(self, "broker_order_sent", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_exit_order_id": self.dry_run_exit_order_id,
            "dry_run_exit_intent_id": self.dry_run_exit_intent_id,
            "dry_run_position_id": self.dry_run_position_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "side": self.side.value,
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
            "close_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExitOrder:
        mapping = require_mapping(data, "DryRunExitOrder")
        return cls(
            dry_run_exit_order_id=mapping["dry_run_exit_order_id"],
            dry_run_exit_intent_id=mapping["dry_run_exit_intent_id"],
            dry_run_position_id=mapping["dry_run_position_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            name=mapping["name"],
            quantity=mapping.get("quantity", 0),
            requested_price=mapping.get("requested_price", 0),
            simulated_fill_price=mapping.get("simulated_fill_price"),
            filled_quantity=mapping.get("filled_quantity", 0),
            remaining_quantity=mapping.get("remaining_quantity", 0),
            status=mapping.get("status", DryRunExitOrderStatus.CREATED.value),
            created_at=mapping.get("created_at"),
            simulated_submitted_at=mapping.get("simulated_submitted_at"),
            simulated_filled_at=mapping.get("simulated_filled_at"),
            expires_at=mapping.get("expires_at"),
            reason_codes=mapping.get("reason_codes", ()),
            evidence_json=mapping.get("evidence_json", {}),
            invalid=mapping.get("invalid", False),
        )


@dataclass(frozen=True, kw_only=True)
class DryRunExitExecution:
    dry_run_exit_execution_id: str
    dry_run_exit_order_id: str
    dry_run_exit_intent_id: str
    dry_run_position_id: str
    trade_date: str
    code: str
    side: DryRunSide | str = DryRunSide.SELL
    quantity: int = 0
    price: float = 0.0
    notional: float = 0.0
    realized_pnl: float = 0.0
    commission: float = 0.0
    tax: float = 0.0
    executed_at: datetime | str | None = None
    execution_type: str = "SIMULATED_EXIT"
    dry_run_only: bool = True
    broker_order_sent: bool = False
    invalid: bool = False

    def __post_init__(self) -> None:
        _reject_invalid_flag(self.invalid)
        for field_name in (
            "dry_run_exit_execution_id",
            "dry_run_exit_order_id",
            "dry_run_exit_intent_id",
            "dry_run_position_id",
            "trade_date",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", DryRunSide.SELL)
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=1))
        for field_name in ("price", "notional", "commission", "tax"):
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=0.0),
            )
        object.__setattr__(self, "realized_pnl", parse_float(self.realized_pnl, "realized_pnl"))
        if self.executed_at is not None:
            object.__setattr__(
                self, "executed_at", parse_timestamp(self.executed_at, "executed_at")
            )
        object.__setattr__(
            self,
            "execution_type",
            require_non_empty_str(self.execution_type, "execution_type").upper(),
        )
        object.__setattr__(self, "dry_run_only", True)
        object.__setattr__(self, "broker_order_sent", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run_exit_execution_id": self.dry_run_exit_execution_id,
            "dry_run_exit_order_id": self.dry_run_exit_order_id,
            "dry_run_exit_intent_id": self.dry_run_exit_intent_id,
            "dry_run_position_id": self.dry_run_position_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "notional": self.notional,
            "realized_pnl": self.realized_pnl,
            "commission": self.commission,
            "tax": self.tax,
            "executed_at": _optional_wire(self.executed_at),
            "execution_type": self.execution_type,
            "dry_run_only": True,
            "close_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DryRunExitExecution:
        mapping = require_mapping(data, "DryRunExitExecution")
        return cls(
            dry_run_exit_execution_id=mapping["dry_run_exit_execution_id"],
            dry_run_exit_order_id=mapping["dry_run_exit_order_id"],
            dry_run_exit_intent_id=mapping["dry_run_exit_intent_id"],
            dry_run_position_id=mapping["dry_run_position_id"],
            trade_date=mapping["trade_date"],
            code=mapping["code"],
            quantity=mapping["quantity"],
            price=mapping["price"],
            notional=mapping["notional"],
            realized_pnl=mapping.get("realized_pnl", 0),
            commission=mapping.get("commission", 0),
            tax=mapping.get("tax", 0),
            executed_at=mapping.get("executed_at"),
            execution_type=mapping.get("execution_type", "SIMULATED_EXIT"),
            invalid=mapping.get("invalid", False),
        )


def _reject_invalid_flag(value: bool) -> None:
    if parse_bool(value, "invalid"):
        raise ValueError("invalid dry-run exit model flag is true")


def _reason_value(reason: DryRunExitReasonCode | str) -> str:
    if isinstance(reason, DryRunExitReasonCode):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()


def _optional_wire(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return datetime_to_wire(parse_timestamp(value, "timestamp"))


def _optional_enum_value(value: object) -> str | None:
    if value is None:
        return None
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)
