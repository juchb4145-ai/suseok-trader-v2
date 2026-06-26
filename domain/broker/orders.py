from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    optional_non_empty_str,
    parse_bool,
    parse_float,
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_fields,
    require_mapping,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderResultStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, kw_only=True)
class BrokerOrderRequest:
    code: str
    side: OrderSide
    quantity: int
    price: int | None = None
    order_type: OrderType = OrderType.LIMIT
    client_order_id: str = field(default_factory=lambda: new_message_id("order"))
    account_id: str | None = None
    idempotency_key: str | None = None
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "client_order_id",
            require_non_empty_str(self.client_order_id, "client_order_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, OrderSide, "side"))
        object.__setattr__(
            self,
            "order_type",
            parse_str_enum(self.order_type, OrderType, "order_type"),
        )
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=1))
        if self.price is not None:
            object.__setattr__(self, "price", parse_int(self.price, "price", min_value=0))
        if self.order_type is OrderType.LIMIT and self.price is None:
            raise ValueError("price is required for LIMIT orders")
        if self.order_type is OrderType.LIMIT and self.price == 0:
            raise ValueError("price must be > 0 for LIMIT orders")
        object.__setattr__(
            self,
            "account_id",
            optional_non_empty_str(self.account_id, "account_id"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            optional_non_empty_str(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "client_order_id": self.client_order_id,
            "code": self.code,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "ts": datetime_to_wire(self.ts),
        }
        if self.account_id is not None:
            data["account_id"] = self.account_id
        if self.idempotency_key is not None:
            data["idempotency_key"] = self.idempotency_key
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerOrderRequest:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("code", "side", "quantity"), cls.__name__)
        return cls(
            client_order_id=mapping.get("client_order_id", new_message_id("order")),
            code=mapping["code"],
            side=parse_str_enum(mapping["side"], OrderSide, "side"),
            order_type=parse_str_enum(
                mapping.get("order_type", OrderType.LIMIT),
                OrderType,
                "order_type",
            ),
            quantity=mapping["quantity"],
            price=mapping.get("price"),
            account_id=mapping.get("account_id"),
            idempotency_key=mapping.get("idempotency_key"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )


@dataclass(frozen=True, kw_only=True)
class BrokerOrderResult:
    client_order_id: str
    code: str
    accepted: bool
    status: OrderResultStatus
    broker_order_id: str | None = None
    reason: str | None = None
    command_id: str | None = None
    idempotency_key: str | None = None
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "client_order_id",
            require_non_empty_str(self.client_order_id, "client_order_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "accepted", parse_bool(self.accepted, "accepted"))
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, OrderResultStatus, "status"),
        )
        object.__setattr__(
            self,
            "broker_order_id",
            optional_non_empty_str(self.broker_order_id, "broker_order_id"),
        )
        object.__setattr__(self, "reason", optional_non_empty_str(self.reason, "reason"))
        object.__setattr__(
            self,
            "command_id",
            optional_non_empty_str(self.command_id, "command_id"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            optional_non_empty_str(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "client_order_id": self.client_order_id,
            "code": self.code,
            "accepted": self.accepted,
            "status": self.status.value,
            "ts": datetime_to_wire(self.ts),
        }
        if self.broker_order_id is not None:
            data["broker_order_id"] = self.broker_order_id
        if self.reason is not None:
            data["reason"] = self.reason
        if self.command_id is not None:
            data["command_id"] = self.command_id
        if self.idempotency_key is not None:
            data["idempotency_key"] = self.idempotency_key
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerOrderResult:
        mapping = require_mapping(data, cls.__name__)
        require_fields(mapping, ("client_order_id", "code", "accepted", "status"), cls.__name__)
        return cls(
            client_order_id=mapping["client_order_id"],
            code=mapping["code"],
            accepted=mapping["accepted"],
            status=parse_str_enum(mapping["status"], OrderResultStatus, "status"),
            broker_order_id=mapping.get("broker_order_id"),
            reason=mapping.get("reason"),
            command_id=mapping.get("command_id"),
            idempotency_key=mapping.get("idempotency_key"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )


@dataclass(frozen=True, kw_only=True)
class BrokerExecutionEvent:
    execution_id: str
    broker_order_id: str
    code: str
    side: OrderSide
    quantity: int
    price: int
    executed_at: datetime
    client_order_id: str | None = None
    account_id: str | None = None
    remaining_quantity: int | None = None
    commission: float | None = None
    tax: float | None = None
    ts: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "execution_id",
            require_non_empty_str(self.execution_id, "execution_id"),
        )
        object.__setattr__(
            self,
            "broker_order_id",
            require_non_empty_str(self.broker_order_id, "broker_order_id"),
        )
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "side", parse_str_enum(self.side, OrderSide, "side"))
        object.__setattr__(self, "quantity", parse_int(self.quantity, "quantity", min_value=1))
        object.__setattr__(self, "price", parse_int(self.price, "price", min_value=1))
        object.__setattr__(self, "executed_at", parse_timestamp(self.executed_at, "executed_at"))
        object.__setattr__(
            self,
            "client_order_id",
            optional_non_empty_str(self.client_order_id, "client_order_id"),
        )
        object.__setattr__(
            self,
            "account_id",
            optional_non_empty_str(self.account_id, "account_id"),
        )
        if self.remaining_quantity is not None:
            object.__setattr__(
                self,
                "remaining_quantity",
                parse_int(self.remaining_quantity, "remaining_quantity", min_value=0),
            )
        if self.commission is not None:
            object.__setattr__(
                self,
                "commission",
                parse_float(self.commission, "commission", min_value=0.0),
            )
        if self.tax is not None:
            object.__setattr__(self, "tax", parse_float(self.tax, "tax", min_value=0.0))
        object.__setattr__(self, "ts", parse_timestamp(self.ts, "ts"))

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "execution_id": self.execution_id,
            "broker_order_id": self.broker_order_id,
            "code": self.code,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "executed_at": datetime_to_wire(self.executed_at),
            "ts": datetime_to_wire(self.ts),
        }
        if self.client_order_id is not None:
            data["client_order_id"] = self.client_order_id
        if self.account_id is not None:
            data["account_id"] = self.account_id
        if self.remaining_quantity is not None:
            data["remaining_quantity"] = self.remaining_quantity
        if self.commission is not None:
            data["commission"] = self.commission
        if self.tax is not None:
            data["tax"] = self.tax
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerExecutionEvent:
        mapping = require_mapping(data, cls.__name__)
        require_fields(
            mapping,
            ("execution_id", "broker_order_id", "code", "side", "quantity", "price", "executed_at"),
            cls.__name__,
        )
        return cls(
            execution_id=mapping["execution_id"],
            broker_order_id=mapping["broker_order_id"],
            code=mapping["code"],
            side=parse_str_enum(mapping["side"], OrderSide, "side"),
            quantity=mapping["quantity"],
            price=mapping["price"],
            executed_at=parse_timestamp(mapping["executed_at"], "executed_at"),
            client_order_id=mapping.get("client_order_id"),
            account_id=mapping.get("account_id"),
            remaining_quantity=mapping.get("remaining_quantity"),
            commission=mapping.get("commission"),
            tax=mapping.get("tax"),
            ts=parse_timestamp(mapping["ts"], "ts") if "ts" in mapping else utc_now(),
        )
