from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    optional_non_empty_str,
    parse_bool,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from storage.gateway_command_store import canonical_json

from services.profit_lab.models import ProfitLabSignal

PARALLEL_SHADOW_INPUT_FORMAT = "parallel-shadow-input/v1"
PARALLEL_SHADOW_REPORT_FORMAT = "parallel-shadow-report/v1"
_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json({"value": value}).encode("utf-8")).hexdigest()


@dataclass(frozen=True, kw_only=True)
class ShadowPreflight:
    status: str
    kill_switch_active: bool
    live_buy_allowed: bool
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        status = require_non_empty_str(self.status, "preflight.status").upper()
        if status not in {"PASS", "BLOCK"}:
            raise ValueError("preflight.status must be PASS or BLOCK")
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "kill_switch_active",
            parse_bool(self.kill_switch_active, "kill_switch_active"),
        )
        object.__setattr__(
            self,
            "live_buy_allowed",
            parse_bool(self.live_buy_allowed, "live_buy_allowed"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                sorted(
                    {
                        str(item).strip().upper()
                        for item in self.reason_codes
                        if str(item).strip()
                    }
                )
            ),
        )
        if self.kill_switch_active and self.live_buy_allowed:
            raise ValueError("live_buy_allowed must be false when kill switch is active")
        if self.status == "BLOCK" and self.live_buy_allowed:
            raise ValueError("live_buy_allowed must be false when preflight is BLOCK")

    @property
    def live_buy_blocked(self) -> bool:
        return self.status == "BLOCK" or self.kill_switch_active or not self.live_buy_allowed

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "kill_switch_active": self.kill_switch_active,
            "live_buy_allowed": self.live_buy_allowed,
            "live_buy_blocked": self.live_buy_blocked,
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ShadowPreflight:
        return cls(
            status=str(value.get("status") or ""),
            kill_switch_active=value.get("kill_switch_active", False),
            live_buy_allowed=value.get("live_buy_allowed", False),
            reason_codes=tuple(str(item) for item in value.get("reason_codes") or ()),
        )


@dataclass(frozen=True, kw_only=True)
class ShadowPlan:
    order_plan_id: str
    trade_date: str
    code: str
    created_at: datetime | str
    limit_price: float
    quantity: int
    entry_timing_evaluation_id: str
    strategy_observation_id: str
    risk_observation_id: str
    source_run_id: str
    source_watermark: Mapping[str, Any]
    source_watermark_hash: str
    status: str = "PLAN_READY"
    coherent: bool = True
    exchange: str = "KRX"
    setup_type: str = "UNKNOWN"
    regime: str = "UNKNOWN"
    theme: str = "UNKNOWN"
    ai_influenced: bool = False

    def __post_init__(self) -> None:
        for field_name in (
            "order_plan_id",
            "entry_timing_evaluation_id",
            "strategy_observation_id",
            "risk_observation_id",
            "source_run_id",
        ):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        trade_date = require_non_empty_str(self.trade_date, "trade_date")
        parsed_trade_date = date.fromisoformat(trade_date)
        object.__setattr__(self, "trade_date", trade_date)
        object.__setattr__(self, "code", validate_stock_code(self.code))
        created_at = parse_timestamp(self.created_at, "created_at")
        if created_at.astimezone(_SEOUL_TIMEZONE).date() != parsed_trade_date:
            raise ValueError("trade_date must match created_at in Asia/Seoul")
        object.__setattr__(self, "created_at", created_at)
        if float(self.limit_price) <= 0:
            raise ValueError("limit_price must be > 0")
        if int(self.quantity) <= 0:
            raise ValueError("quantity must be > 0")
        object.__setattr__(self, "limit_price", float(self.limit_price))
        object.__setattr__(self, "quantity", int(self.quantity))
        status = require_non_empty_str(self.status, "status").upper()
        object.__setattr__(self, "status", status)
        exchange = require_non_empty_str(self.exchange, "exchange").upper()
        if exchange not in {"KRX", "NXT"}:
            raise ValueError("exchange must be KRX or NXT")
        object.__setattr__(self, "exchange", exchange)
        for field_name in ("setup_type", "regime", "theme"):
            value = str(getattr(self, field_name) or "UNKNOWN").strip().upper() or "UNKNOWN"
            object.__setattr__(self, field_name, value)
        watermark = dict(self.source_watermark)
        if not watermark:
            raise ValueError("source_watermark must not be empty")
        object.__setattr__(self, "source_watermark", watermark)
        expected_hash = canonical_sha256(watermark)
        actual_hash = require_non_empty_str(
            self.source_watermark_hash,
            "source_watermark_hash",
        ).lower()
        if actual_hash != expected_hash:
            raise ValueError("source_watermark_hash does not match source_watermark")
        object.__setattr__(self, "source_watermark_hash", actual_hash)
        object.__setattr__(self, "coherent", parse_bool(self.coherent, "coherent"))
        object.__setattr__(
            self,
            "ai_influenced",
            parse_bool(self.ai_influenced, "ai_influenced"),
        )

    @property
    def shadow_eligible(self) -> bool:
        return self.status == "PLAN_READY" and self.coherent

    def to_profit_lab_signal(self) -> ProfitLabSignal:
        return ProfitLabSignal(
            signal_id=self.order_plan_id,
            order_plan_id=self.order_plan_id,
            trade_date=self.trade_date,
            code=self.code,
            signal_at=parse_timestamp(self.created_at, "created_at"),
            limit_price=self.limit_price,
            quantity=self.quantity,
            exchange=self.exchange,
            coherent=self.coherent,
            setup_type=self.setup_type,
            regime=self.regime,
            theme=self.theme,
            source_lineage=self.lineage,
        )

    @property
    def lineage(self) -> dict[str, Any]:
        return {
            "order_plan_id": self.order_plan_id,
            "entry_timing_evaluation_id": self.entry_timing_evaluation_id,
            "strategy_observation_id": self.strategy_observation_id,
            "risk_observation_id": self.risk_observation_id,
            "source_run_id": self.source_run_id,
            "source_watermark": dict(self.source_watermark),
            "source_watermark_hash": self.source_watermark_hash,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_plan_id": self.order_plan_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "created_at": datetime_to_wire(parse_timestamp(self.created_at, "created_at")),
            "limit_price": self.limit_price,
            "quantity": self.quantity,
            "status": self.status,
            "coherent": self.coherent,
            "exchange": self.exchange,
            "setup_type": self.setup_type,
            "regime": self.regime,
            "theme": self.theme,
            "ai_influenced": self.ai_influenced,
            **self.lineage,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ShadowPlan:
        return cls(**dict(value))


@dataclass(frozen=True, kw_only=True)
class LiveSimObservation:
    order_plan_id: str
    live_sim_intent_id: str
    live_sim_order_id: str
    requested_quantity: int
    filled_quantity: int
    execution_ids: tuple[str, ...] = ()
    position_id: str | None = None
    avg_fill_price: float | None = None
    first_filled_at: datetime | str | None = None
    exit_reason: str | None = None
    closed_at: datetime | str | None = None
    holding_sec: float | None = None
    gross_pnl: float | None = None
    net_pnl: float | None = None

    def __post_init__(self) -> None:
        for field_name in ("order_plan_id", "live_sim_intent_id", "live_sim_order_id"):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        requested = int(self.requested_quantity)
        filled = int(self.filled_quantity)
        if requested < 1:
            raise ValueError("requested_quantity must be >= 1")
        if filled < 0 or filled > requested:
            raise ValueError("filled_quantity must be between 0 and requested_quantity")
        object.__setattr__(self, "requested_quantity", requested)
        object.__setattr__(self, "filled_quantity", filled)
        execution_ids = tuple(
            require_non_empty_str(item, "execution_id") for item in self.execution_ids
        )
        if len(execution_ids) != len(set(execution_ids)):
            raise ValueError("execution_ids must be unique")
        object.__setattr__(self, "execution_ids", execution_ids)
        object.__setattr__(
            self,
            "position_id",
            optional_non_empty_str(self.position_id, "position_id"),
        )
        if self.avg_fill_price is not None:
            avg_fill_price = float(self.avg_fill_price)
            if avg_fill_price <= 0:
                raise ValueError("avg_fill_price must be > 0")
            object.__setattr__(self, "avg_fill_price", avg_fill_price)
        if filled > 0:
            if self.avg_fill_price is None or self.first_filled_at is None:
                raise ValueError("filled observation requires avg_fill_price and first_filled_at")
            if not execution_ids or self.position_id is None:
                raise ValueError("filled observation requires execution_ids and position_id")
        elif any(
            value is not None
            for value in (self.avg_fill_price, self.first_filled_at, self.position_id)
        ) or execution_ids:
            raise ValueError("unfilled observation cannot contain fill or position evidence")
        for field_name in ("first_filled_at", "closed_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_timestamp(value, field_name))
        object.__setattr__(
            self,
            "exit_reason",
            optional_non_empty_str(self.exit_reason, "exit_reason"),
        )
        if self.closed_at is not None and self.exit_reason is None:
            raise ValueError("closed observation requires exit_reason")
        if self.closed_at is not None:
            if not self.filled or self.first_filled_at is None:
                raise ValueError("closed observation requires entry fill evidence")
            if parse_timestamp(self.closed_at, "closed_at") < parse_timestamp(
                self.first_filled_at,
                "first_filled_at",
            ):
                raise ValueError("closed_at must not be before first_filled_at")
            if any(
                value is None
                for value in (self.holding_sec, self.gross_pnl, self.net_pnl)
            ):
                raise ValueError("closed observation requires holding_sec and PnL")
        for field_name in ("holding_sec", "gross_pnl", "net_pnl"):
            value = getattr(self, field_name)
            if value is not None:
                normalized = float(value)
                if field_name == "holding_sec" and normalized < 0:
                    raise ValueError("holding_sec must be >= 0")
                object.__setattr__(self, field_name, normalized)

    @property
    def filled(self) -> bool:
        return self.filled_quantity > 0

    @property
    def partial_fill(self) -> bool:
        return 0 < self.filled_quantity < self.requested_quantity

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["execution_ids"] = list(self.execution_ids)
        result["filled"] = self.filled
        result["partial_fill"] = self.partial_fill
        for field_name in ("first_filled_at", "closed_at"):
            value = getattr(self, field_name)
            result[field_name] = (
                datetime_to_wire(parse_timestamp(value, field_name)) if value is not None else None
            )
        return result

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> LiveSimObservation:
        data = dict(value)
        data["execution_ids"] = tuple(str(item) for item in data.get("execution_ids") or ())
        return cls(**data)


@dataclass(frozen=True, kw_only=True)
class ShadowExecution:
    shadow_execution_id: str
    shadow_fill_id: str | None
    shadow_position_id: str | None
    order_plan_id: str
    entry_timing_evaluation_id: str
    strategy_observation_id: str
    risk_observation_id: str
    source_run_id: str
    source_watermark_hash: str
    status: str
    entry_filled_at: str | None
    entry_fill_price: int | None
    quantity: int
    exit_reason: str | None
    exit_filled_at: str | None
    holding_sec: float | None
    gross_pnl: float | None
    net_pnl: float | None

    @property
    def filled(self) -> bool:
        return self.entry_fill_price is not None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"filled": self.filled}


@dataclass(frozen=True, kw_only=True)
class ShadowLiveComparison:
    order_plan_id: str
    shadow_execution_id: str | None
    shadow_fill_id: str | None
    shadow_position_id: str | None
    live_sim_intent_id: str
    live_sim_order_id: str
    live_sim_execution_ids: tuple[str, ...]
    live_sim_position_id: str | None
    linkage_complete: bool
    linkage_gaps: tuple[str, ...]
    shadow_filled: bool
    live_filled: bool
    fill_disagreement: bool
    fill_time_delta_sec: float | None
    slippage_ticks: int | None
    slippage_pct: float | None
    partial_fill: bool
    shadow_exit_reason: str | None
    live_exit_reason: str | None
    exit_reason_disagreement: bool
    holding_time_delta_sec: float | None
    gross_pnl_delta: float | None
    net_pnl_delta: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {
            "live_sim_execution_ids": list(self.live_sim_execution_ids),
            "linkage_gaps": list(self.linkage_gaps),
        }


def validate_plan_identity(
    plans: Sequence[ShadowPlan],
    *,
    source_plan_count: int,
    source_plan_ids_sha256: str,
) -> None:
    plan_ids = [plan.order_plan_id for plan in plans]
    if len(plan_ids) != len(set(plan_ids)):
        raise ValueError("duplicate order_plan_id in parallel shadow input")
    if source_plan_count != len(plans):
        raise ValueError("source_plan_count does not match plans")
    actual_hash = canonical_sha256(sorted(plan_ids))
    if source_plan_ids_sha256.lower() != actual_hash:
        raise ValueError("source_plan_ids_sha256 does not match plans")
