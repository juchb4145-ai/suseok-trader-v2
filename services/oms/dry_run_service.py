from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_value,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.oms.models import (
    DryRunEligibility,
    DryRunExecution,
    DryRunIntent,
    DryRunOrder,
    DryRunPosition,
)
from domain.oms.reasons import DryRunRejectionReason
from domain.oms.sides import DryRunOrderType, DryRunSide
from domain.oms.status import DryRunIntentStatus, DryRunOrderStatus

from services.admission import AdmissionPolicy, AdmissionReason, evaluate_trade_admission
from services.config import Settings, TradingMode, load_settings
from services.oms.safety_gate import check_pr10_safety_gate

ACTIVE_INTENT_STATUSES = {
    DryRunIntentStatus.CREATED.value,
}
ACTIVE_ORDER_STATUSES = {
    DryRunOrderStatus.CREATED.value,
    DryRunOrderStatus.SIMULATED_SUBMITTED.value,
    DryRunOrderStatus.SIMULATED_PARTIALLY_FILLED.value,
}
ACTIVE_POSITION_STATUSES = {"OPEN"}

DRY_RUN_ADMISSION_REASON_MAP = {
    AdmissionReason.CANDIDATE_NOT_FOUND.value: DryRunRejectionReason.CANDIDATE_NOT_FOUND.value,
    AdmissionReason.CANDIDATE_NOT_CONTEXT_READY.value: (
        DryRunRejectionReason.CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.CANDIDATE_CONTEXT_MISSING.value: (
        DryRunRejectionReason.CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.STRATEGY_OBSERVATION_MISSING.value: (
        DryRunRejectionReason.STRATEGY_OBSERVATION_MISSING.value
    ),
    AdmissionReason.STRATEGY_NOT_MATCHED.value: DryRunRejectionReason.STRATEGY_NOT_MATCHED.value,
    AdmissionReason.STRATEGY_OBSERVE_ONLY_MISMATCH.value: (
        DryRunRejectionReason.STRATEGY_NOT_MATCHED.value
    ),
    AdmissionReason.RISK_OBSERVATION_MISSING.value: (
        DryRunRejectionReason.RISK_OBSERVATION_MISSING.value
    ),
    AdmissionReason.RISK_NOT_OBSERVE_PASS.value: (
        DryRunRejectionReason.RISK_NOT_OBSERVE_PASS.value
    ),
    AdmissionReason.RISK_OBSERVE_ONLY_MISMATCH.value: (
        DryRunRejectionReason.RISK_NOT_OBSERVE_PASS.value
    ),
    AdmissionReason.LATEST_TICK_MISSING.value: DryRunRejectionReason.LATEST_TICK_MISSING.value,
    AdmissionReason.LATEST_TICK_STALE.value: DryRunRejectionReason.LATEST_TICK_STALE.value,
    AdmissionReason.DRY_RUN_EVIDENCE_MISSING.value: (
        DryRunRejectionReason.STRATEGY_NOT_MATCHED.value
    ),
}


@dataclass(frozen=True, kw_only=True)
class DryRunRunResult:
    run_id: str
    trade_date: str | None = None
    evaluated_count: int = 0
    eligible_count: int = 0
    intent_count: int = 0
    order_count: int = 0
    execution_count: int = 0
    rejection_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    dry_run_only: bool = True
    live_order_allowed: bool = False
    gateway_command_allowed: bool = False
    broker_order_sent: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "evaluated_count": self.evaluated_count,
            "eligible_count": self.eligible_count,
            "intent_count": self.intent_count,
            "order_count": self.order_count,
            "execution_count": self.execution_count,
            "rejection_count": self.rejection_count,
            "error_count": self.error_count,
            "status": self.status,
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }


def evaluate_dry_run_eligibility(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
) -> DryRunEligibility:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    safety_gate = check_pr10_safety_gate(connection, resolved_settings)
    admission = evaluate_trade_admission(
        connection,
        normalized_id,
        AdmissionPolicy(
            name="dry_run_shadow",
            require_candidate_context_ready=(
                resolved_settings.dry_run_require_candidate_context_ready
            ),
            require_strategy_matched=resolved_settings.dry_run_require_strategy_matched,
            require_risk_observe_pass=resolved_settings.dry_run_require_risk_observe_pass,
            require_fresh_tick=True,
            stale_tick_sec=resolved_settings.dry_run_stale_tick_sec,
        ),
    )

    reason_codes: list[str] = _map_admission_reasons(
        admission.reason_codes,
        DRY_RUN_ADMISSION_REASON_MAP,
    )
    evidence: dict[str, Any] = {
        "candidate_instance_id": normalized_id,
        "observe_only": True,
        "dry_run_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
    }
    evidence.update(admission.to_evidence())

    shadow_live_sim = _live_sim_pilot_shadow_dry_run_allowed(resolved_settings)
    safety_reason_codes = list(safety_gate.reason_codes)
    ignored_safety_reason_codes: list[str] = []
    if shadow_live_sim:
        safety_reason_codes = [
            reason
            for reason in safety_reason_codes
            if reason != DryRunRejectionReason.LIVE_FLAGS_ENABLED.value
        ]
        ignored_safety_reason_codes = [
            reason
            for reason in safety_gate.reason_codes
            if reason == DryRunRejectionReason.LIVE_FLAGS_ENABLED.value
        ]
        evidence["live_sim_pilot_shadow_dry_run"] = True
        if ignored_safety_reason_codes:
            evidence["ignored_safety_reason_codes"] = ignored_safety_reason_codes

    if resolved_settings.dry_run_require_safety_gate and safety_reason_codes:
        reason_codes.append(DryRunRejectionReason.SAFETY_GATE_FAILED.value)
        reason_codes.extend(safety_reason_codes)
    if not resolved_settings.dry_run_oms_enabled:
        reason_codes.append(DryRunRejectionReason.DRY_RUN_DISABLED.value)
    if not resolved_settings.dry_run_intent_creation_enabled:
        reason_codes.append(DryRunRejectionReason.DRY_RUN_DISABLED.value)
    if (
        resolved_settings.live_sim_allowed or resolved_settings.live_real_allowed
    ) and not shadow_live_sim:
        reason_codes.append(DryRunRejectionReason.LIVE_FLAGS_ENABLED.value)
    if resolved_settings.dry_run_order_routing_enabled:
        reason_codes.append(DryRunRejectionReason.ORDER_ROUTING_DISABLED.value)
    if resolved_settings.dry_run_gateway_command_enabled:
        reason_codes.append(DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value)

    trade_date = admission.trade_date
    code = admission.code
    name = admission.name

    if code is not None and trade_date is not None:
        if _active_position_count_for_code(connection, trade_date, code) > 0:
            reason_codes.append(DryRunRejectionReason.DUPLICATE_DRY_RUN_POSITION.value)
        if _recent_active_intent_count_for_code(connection, code, resolved_settings) > 0:
            reason_codes.append(DryRunRejectionReason.DUPLICATE_DRY_RUN_INTENT.value)
        if _recent_active_order_count_for_code(connection, code, resolved_settings) > 0:
            reason_codes.append(DryRunRejectionReason.DUPLICATE_DRY_RUN_ORDER.value)
        daily_intent_limit_reached = (
            _daily_intent_count(connection, trade_date)
            >= resolved_settings.dry_run_max_daily_intents
        )
        if daily_intent_limit_reached:
            reason_codes.append(DryRunRejectionReason.MAX_DAILY_INTENTS_REACHED.value)
    if _active_position_count(connection) >= resolved_settings.dry_run_max_active_positions:
        reason_codes.append(DryRunRejectionReason.MAX_ACTIVE_POSITIONS_REACHED.value)

    if admission.latest_tick_evidence:
        quantity, notional = _calculate_quantity_and_notional(
            float(admission.latest_tick_evidence["price"]),
            resolved_settings,
        )
        evidence["sizing"] = {
            "default_position_notional": resolved_settings.dry_run_default_position_notional,
            "max_position_notional": resolved_settings.dry_run_max_position_notional,
            "min_quantity": resolved_settings.dry_run_min_quantity,
            "quantity": quantity,
            "notional": notional,
        }
        if quantity < resolved_settings.dry_run_min_quantity:
            reason_codes.append(DryRunRejectionReason.INVALID_QUANTITY.value)
        if notional <= 0 or notional > resolved_settings.dry_run_max_position_notional:
            reason_codes.append(DryRunRejectionReason.INVALID_NOTIONAL.value)

    if not resolved_settings.dry_run_allow_market_sim:
        reason_codes.append(DryRunRejectionReason.INVALID_SIDE.value)
    reason_codes.extend(
        [
            DryRunRejectionReason.ORDER_ROUTING_DISABLED.value,
            DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value,
            DryRunRejectionReason.OBSERVE_ONLY_PIPELINE.value,
        ]
    )
    reason_codes = _merge_reasons(reason_codes)
    blocking_reasons = [
        reason
        for reason in reason_codes
        if reason
        not in {
            DryRunRejectionReason.ORDER_ROUTING_DISABLED.value,
            DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value,
            DryRunRejectionReason.OBSERVE_ONLY_PIPELINE.value,
        }
    ]
    eligibility = DryRunEligibility(
        eligible=not blocking_reasons,
        candidate_instance_id=normalized_id,
        strategy_observation_id=admission.strategy_observation_id,
        risk_observation_id=admission.risk_observation_id,
        status="ELIGIBLE" if not blocking_reasons else "INELIGIBLE",
        reason_codes=reason_codes,
        evidence_json=evidence
        | {
            "trade_date": trade_date,
            "code": code,
            "name": name,
            "blocking_reason_codes": blocking_reasons,
        },
        safety_gate_result=safety_gate.to_dict(),
        computed_at=datetime_to_wire(utc_now()),
    )
    _save_eligibility_check(connection, eligibility)
    connection.commit()
    return eligibility


def _live_sim_pilot_shadow_dry_run_allowed(settings: Settings) -> bool:
    capabilities = settings.trading_capabilities
    return (
        capabilities.dry_run_shadow_allowed
        and settings.trading_mode is TradingMode.LIVE_SIM
        and settings.live_sim_allowed
        and not settings.live_real_allowed
        and not settings.dry_run_order_routing_enabled
        and not settings.dry_run_gateway_command_enabled
    )


def create_dry_run_intent(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
    source: str = "manual",
) -> DryRunIntent:
    resolved_settings = settings or load_settings()
    eligibility = evaluate_dry_run_eligibility(connection, candidate_instance_id, resolved_settings)
    evidence = dict(eligibility.evidence_json)
    candidate = _candidate_row(connection, candidate_instance_id)
    strategy = _strategy_latest_row(connection, candidate_instance_id)
    risk = _risk_latest_row(connection, candidate_instance_id)

    if not eligibility.eligible:
        _save_intent_rejection(
            connection,
            candidate_instance_id=candidate_instance_id,
            strategy_observation_id=eligibility.strategy_observation_id,
            risk_observation_id=eligibility.risk_observation_id,
            trade_date=evidence.get("trade_date"),
            code=evidence.get("code"),
            reason_codes=eligibility.reason_codes,
            evidence=evidence,
        )
        connection.commit()
        return DryRunIntent(
            dry_run_intent_id=new_message_id("dry_run_intent_rejected"),
            candidate_instance_id=candidate_instance_id,
            strategy_observation_id=eligibility.strategy_observation_id,
            risk_observation_id=eligibility.risk_observation_id,
            trade_date=str(evidence.get("trade_date") or "UNKNOWN"),
            code=str(evidence.get("code") or "000000"),
            name=str(evidence.get("name") or "UNKNOWN"),
            intended_price=float(_json_object(evidence.get("latest_tick")).get("price") or 0),
            quantity=0,
            notional=0,
            status=DryRunIntentStatus.REJECTED,
            reason_codes=eligibility.reason_codes,
            evidence_json=evidence,
            created_at=datetime_to_wire(utc_now()),
            source=source,
        )

    if candidate is None or strategy is None or risk is None:
        raise ValueError("eligible dry-run intent requires candidate, strategy, and risk rows")
    tick = _latest_tick_row(connection, candidate["code"])
    if tick is None:
        raise ValueError("eligible dry-run intent requires latest tick")

    price = float(tick["price"])
    quantity, notional = _calculate_quantity_and_notional(price, resolved_settings)
    if quantity < resolved_settings.dry_run_min_quantity:
        raise ValueError("calculated dry-run quantity is below minimum")

    now = utc_now()
    intent = DryRunIntent(
        dry_run_intent_id=new_message_id("dry_run_intent"),
        candidate_instance_id=str(candidate["candidate_instance_id"]),
        strategy_observation_id=str(strategy["strategy_observation_id"]),
        risk_observation_id=str(risk["risk_observation_id"]),
        trade_date=str(candidate["trade_date"]),
        code=str(candidate["code"]),
        name=str(candidate["name"]),
        side=DryRunSide.BUY,
        order_type=DryRunOrderType.MARKET_SIM,
        intended_price=price,
        quantity=quantity,
        notional=notional,
        status=DryRunIntentStatus.CREATED,
        reason_codes=[
            DryRunRejectionReason.OBSERVE_ONLY_PIPELINE.value,
            DryRunRejectionReason.ORDER_ROUTING_DISABLED.value,
            DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value,
        ],
        evidence_json=evidence
        | {
            "source": source,
            "latest_tick_event_ts": tick["event_ts"],
            "quantity": quantity,
            "notional": notional,
        },
        created_at=now,
        expires_at=now + timedelta(seconds=resolved_settings.dry_run_intent_ttl_sec),
        source=source,
    )
    _insert_intent(connection, intent)
    _insert_ledger(
        connection,
        trade_date=intent.trade_date,
        event_type="INTENT_CREATED",
        related_entity_type="dry_run_intent",
        related_entity_id=intent.dry_run_intent_id,
        code=intent.code,
        amount=intent.notional,
        quantity=intent.quantity,
        payload=intent.to_dict(),
    )
    connection.commit()
    return intent


def convert_intent_to_dry_run_order(
    connection: sqlite3.Connection,
    dry_run_intent_id: str,
    settings: Settings | None = None,
) -> DryRunOrder:
    resolved_settings = settings or load_settings()
    _ensure_dry_run_oms_enabled(resolved_settings)
    intent = get_dry_run_intent(connection, dry_run_intent_id)
    if intent is None:
        raise ValueError(f"dry-run intent not found: {dry_run_intent_id}")
    if intent["status"] != DryRunIntentStatus.CREATED.value:
        raise ValueError(f"dry-run intent is not creatable as order: {intent['status']}")

    now = utc_now()
    order = DryRunOrder(
        dry_run_order_id=new_message_id("dry_run_order"),
        dry_run_intent_id=intent["dry_run_intent_id"],
        trade_date=intent["trade_date"],
        code=intent["code"],
        name=intent["name"],
        side=intent["side"],
        order_type=intent["order_type"],
        quantity=int(intent["quantity"]),
        requested_price=float(intent["intended_price"]),
        filled_quantity=0,
        remaining_quantity=int(intent["quantity"]),
        status=DryRunOrderStatus.CREATED,
        created_at=now,
        reason_codes=[
            DryRunRejectionReason.OBSERVE_ONLY_PIPELINE.value,
            DryRunRejectionReason.ORDER_ROUTING_DISABLED.value,
            DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value,
        ],
        evidence_json={
            "dry_run_intent_id": intent["dry_run_intent_id"],
            "dry_run_only": True,
            "broker_order_sent": False,
        },
        expires_at=intent.get("expires_at"),
    )
    _insert_order(connection, order)
    connection.execute(
        """
        UPDATE dry_run_intents
        SET status = ?
        WHERE dry_run_intent_id = ?
        """,
        (DryRunIntentStatus.CONVERTED_TO_DRY_RUN_ORDER.value, dry_run_intent_id),
    )
    _insert_ledger(
        connection,
        trade_date=order.trade_date,
        event_type="ORDER_CREATED",
        related_entity_type="dry_run_order",
        related_entity_id=order.dry_run_order_id,
        code=order.code,
        amount=order.requested_price * order.quantity,
        quantity=order.quantity,
        payload=order.to_dict(),
    )
    connection.commit()
    return order


def simulate_fill_dry_run_order(
    connection: sqlite3.Connection,
    dry_run_order_id: str,
    settings: Settings | None = None,
) -> DryRunExecution:
    resolved_settings = settings or load_settings()
    _ensure_dry_run_oms_enabled(resolved_settings)
    if not resolved_settings.dry_run_simulated_fill_enabled:
        raise ValueError(DryRunRejectionReason.SIMULATED_FILL_DISABLED.value)
    order = get_dry_run_order(connection, dry_run_order_id)
    if order is None:
        raise ValueError(f"dry-run order not found: {dry_run_order_id}")
    if order["status"] not in {
        DryRunOrderStatus.CREATED.value,
        DryRunOrderStatus.SIMULATED_SUBMITTED.value,
    }:
        raise ValueError(f"dry-run order cannot be filled from status: {order['status']}")
    tick = _latest_tick_row(connection, order["code"])
    if tick is None:
        raise ValueError(DryRunRejectionReason.LATEST_TICK_MISSING.value)

    quantity = int(order["remaining_quantity"] or order["quantity"])
    price = float(tick["price"] or order["requested_price"])
    notional = price * quantity
    commission = notional * resolved_settings.dry_run_commission_rate
    tax = notional * resolved_settings.dry_run_tax_rate
    executed_at = utc_now()
    execution = DryRunExecution(
        dry_run_execution_id=new_message_id("dry_run_execution"),
        dry_run_order_id=order["dry_run_order_id"],
        dry_run_intent_id=order["dry_run_intent_id"],
        trade_date=order["trade_date"],
        code=order["code"],
        side=order["side"],
        quantity=quantity,
        price=price,
        notional=notional,
        commission=commission,
        tax=tax,
        executed_at=executed_at,
    )
    _insert_execution(connection, execution)
    connection.execute(
        """
        UPDATE dry_run_orders
        SET status = ?,
            simulated_fill_price = ?,
            filled_quantity = ?,
            remaining_quantity = 0,
            simulated_submitted_at = COALESCE(simulated_submitted_at, ?),
            simulated_filled_at = ?
        WHERE dry_run_order_id = ?
        """,
        (
            DryRunOrderStatus.SIMULATED_FILLED.value,
            price,
            quantity,
            datetime_to_wire(executed_at),
            datetime_to_wire(executed_at),
            order["dry_run_order_id"],
        ),
    )
    position_event = _upsert_position_for_execution(connection, order, execution, price)
    _insert_ledger(
        connection,
        trade_date=execution.trade_date,
        event_type="SIMULATED_FILL",
        related_entity_type="dry_run_execution",
        related_entity_id=execution.dry_run_execution_id,
        code=execution.code,
        amount=execution.notional,
        quantity=execution.quantity,
        payload=execution.to_dict(),
    )
    _insert_ledger(
        connection,
        trade_date=execution.trade_date,
        event_type=position_event["event_type"],
        related_entity_type="dry_run_position",
        related_entity_id=position_event["dry_run_position_id"],
        code=execution.code,
        amount=execution.notional,
        quantity=execution.quantity,
        payload=position_event,
    )
    connection.commit()
    return execution


def update_dry_run_positions_mark_to_market(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    _ensure_dry_run_oms_enabled(resolved_settings)
    rows = connection.execute(
        """
        SELECT *
        FROM dry_run_positions
        WHERE status IN ('OPEN')
        ORDER BY updated_at DESC, dry_run_position_id DESC
        """
    ).fetchall()
    updated = 0
    skipped = 0
    for row in rows:
        tick = _latest_tick_row(connection, row["code"])
        if tick is None:
            skipped += 1
            continue
        last_price = float(tick["price"])
        unrealized_pnl = (last_price - float(row["avg_price"])) * int(row["quantity"])
        now = datetime_to_wire(utc_now())
        connection.execute(
            """
            UPDATE dry_run_positions
            SET last_price = ?,
                unrealized_pnl = ?,
                updated_at = ?
            WHERE dry_run_position_id = ?
            """,
            (last_price, unrealized_pnl, now, row["dry_run_position_id"]),
        )
        _insert_ledger(
            connection,
            trade_date=row["trade_date"],
            event_type="MARK_TO_MARKET",
            related_entity_type="dry_run_position",
            related_entity_id=row["dry_run_position_id"],
            code=row["code"],
            amount=unrealized_pnl,
            quantity=int(row["quantity"]),
            payload={"last_price": last_price, "unrealized_pnl": unrealized_pnl},
        )
        updated += 1
    connection.commit()
    return {
        "updated_count": updated,
        "skipped_count": skipped,
        "dry_run_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
    }


def get_dry_run_status(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    safety_gate = check_pr10_safety_gate(connection, resolved_settings)
    return {
        "enabled": resolved_settings.dry_run_oms_enabled,
        "intent_creation_enabled": resolved_settings.dry_run_intent_creation_enabled,
        "simulated_fill_enabled": resolved_settings.dry_run_simulated_fill_enabled,
        "order_routing_enabled": False,
        "gateway_command_enabled": False,
        "live_order_allowed": False,
        "active_position_count": _active_position_count(connection),
        "intent_count": _count_rows(connection, "dry_run_intents"),
        "order_count": _count_rows(connection, "dry_run_orders"),
        "execution_count": _count_rows(connection, "dry_run_executions"),
        "rejection_count": _count_rows(connection, "dry_run_intent_rejections"),
        "error_count": _count_rows(connection, "dry_run_errors"),
        "safety_gate": safety_gate.to_dict(),
        "dry_run_only": True,
        "broker_order_sent": False,
    }


def list_dry_run_eligibility_checks(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    code: str | None = None,
    eligible: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    if eligible is not None:
        clauses.append("eligible = ?")
        params.append(1 if eligible else 0)
    return _list_rows(
        connection,
        "dry_run_eligibility_checks",
        clauses=clauses,
        params=params,
        order_by="computed_at DESC, check_id DESC",
        limit=limit,
        mapper=_eligibility_check_row_to_dict,
    )


def list_dry_run_intents(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: DryRunIntentStatus | str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    return _list_rows(
        connection,
        "dry_run_intents",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, dry_run_intent_id DESC",
        limit=limit,
        mapper=_intent_row_to_dict,
    )


def get_dry_run_intent(
    connection: sqlite3.Connection,
    dry_run_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_intents
        WHERE dry_run_intent_id = ?
        """,
        (require_non_empty_str(dry_run_intent_id, "dry_run_intent_id"),),
    ).fetchone()
    return None if row is None else _intent_row_to_dict(row)


def list_dry_run_orders(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: DryRunOrderStatus | str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    return _list_rows(
        connection,
        "dry_run_orders",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, dry_run_order_id DESC",
        limit=limit,
        mapper=_order_row_to_dict,
    )


def get_dry_run_order(
    connection: sqlite3.Connection,
    dry_run_order_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_orders
        WHERE dry_run_order_id = ?
        """,
        (require_non_empty_str(dry_run_order_id, "dry_run_order_id"),),
    ).fetchone()
    return None if row is None else _order_row_to_dict(row)


def list_dry_run_executions(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, code=code)
    return _list_rows(
        connection,
        "dry_run_executions",
        clauses=clauses,
        params=params,
        order_by="executed_at DESC, dry_run_execution_id DESC",
        limit=limit,
        mapper=_execution_row_to_dict,
    )


def list_dry_run_positions(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    return _list_rows(
        connection,
        "dry_run_positions",
        clauses=clauses,
        params=params,
        order_by="updated_at DESC, dry_run_position_id DESC",
        limit=limit,
        mapper=_position_row_to_dict,
    )


def get_dry_run_position(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_positions
        WHERE dry_run_position_id = ?
        """,
        (require_non_empty_str(dry_run_position_id, "dry_run_position_id"),),
    ).fetchone()
    return None if row is None else _position_row_to_dict(row)


def list_dry_run_ledger(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    event_type: str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if event_type is not None:
        clauses.append("event_type = ?")
        params.append(require_non_empty_str(event_type, "event_type").upper())
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    return _list_rows(
        connection,
        "dry_run_ledger",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, ledger_id DESC",
        limit=limit,
        mapper=_ledger_row_to_dict,
    )


def list_dry_run_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _list_rows(
        connection,
        "dry_run_errors",
        clauses=[],
        params=[],
        order_by="created_at DESC, id DESC",
        limit=limit,
        mapper=_error_row_to_dict,
    )


def evaluate_dry_run_candidates(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    *,
    candidate_instance_id: str | None = None,
) -> DryRunRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("dry_run_run")
    started_at = datetime_to_wire(utc_now())
    _insert_run(connection, run_id=run_id, trade_date=trade_date, started_at=started_at)
    targets = (
        [{"candidate_instance_id": candidate_instance_id}]
        if candidate_instance_id is not None
        else _dry_run_candidate_targets(connection, trade_date=trade_date, limit=limit or 100)
    )
    evaluated_count = 0
    eligible_count = 0
    error_count = 0
    for target in targets:
        candidate_id = str(target["candidate_instance_id"])
        try:
            eligibility = evaluate_dry_run_eligibility(connection, candidate_id, resolved_settings)
            evaluated_count += 1
            if eligibility.eligible:
                eligible_count += 1
        except Exception as exc:
            _record_error(
                connection,
                run_id=run_id,
                candidate_instance_id=candidate_id,
                code=None,
                error_message=str(exc),
                payload={"candidate_instance_id": candidate_id},
            )
            error_count += 1
    status = "COMPLETED_WITH_ERRORS" if error_count else "COMPLETED"
    _complete_run(
        connection,
        run_id=run_id,
        evaluated_count=evaluated_count,
        eligible_count=eligible_count,
        status=status,
        error_count=error_count,
    )
    connection.commit()
    return DryRunRunResult(
        run_id=run_id,
        trade_date=trade_date,
        evaluated_count=evaluated_count,
        eligible_count=eligible_count,
        error_count=error_count,
        status=status,
    )


def create_dry_run_intents_for_eligible(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> DryRunRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("dry_run_run")
    started_at = datetime_to_wire(utc_now())
    _insert_run(connection, run_id=run_id, trade_date=trade_date, started_at=started_at)
    targets = _dry_run_candidate_targets(connection, trade_date=trade_date, limit=limit or 100)
    evaluated_count = 0
    eligible_count = 0
    intent_count = 0
    rejection_count = 0
    error_count = 0
    for target in targets:
        candidate_id = str(target["candidate_instance_id"])
        try:
            intent = create_dry_run_intent(connection, candidate_id, resolved_settings)
            evaluated_count += 1
            if intent.status is DryRunIntentStatus.CREATED:
                eligible_count += 1
                intent_count += 1
            else:
                rejection_count += 1
        except Exception as exc:
            _record_error(
                connection,
                run_id=run_id,
                candidate_instance_id=candidate_id,
                code=target.get("code"),
                error_message=str(exc),
                payload={"candidate_instance_id": candidate_id},
            )
            error_count += 1
    status = "COMPLETED_WITH_ERRORS" if error_count else "COMPLETED"
    _complete_run(
        connection,
        run_id=run_id,
        evaluated_count=evaluated_count,
        eligible_count=eligible_count,
        intent_count=intent_count,
        rejection_count=rejection_count,
        error_count=error_count,
        status=status,
    )
    connection.commit()
    return DryRunRunResult(
        run_id=run_id,
        trade_date=trade_date,
        evaluated_count=evaluated_count,
        eligible_count=eligible_count,
        intent_count=intent_count,
        rejection_count=rejection_count,
        error_count=error_count,
        status=status,
    )


def _save_eligibility_check(
    connection: sqlite3.Connection,
    eligibility: DryRunEligibility,
) -> None:
    data = eligibility.to_dict()
    evidence = data["evidence_json"]
    connection.execute(
        """
        INSERT INTO dry_run_eligibility_checks (
            check_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            trade_date,
            code,
            eligible,
            status,
            reason_codes_json,
            evidence_json,
            safety_gate_json,
            computed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_message_id("dry_run_check"),
            data["candidate_instance_id"],
            data["strategy_observation_id"],
            data["risk_observation_id"],
            str(evidence.get("trade_date") or "UNKNOWN"),
            str(evidence.get("code") or "000000"),
            1 if data["eligible"] else 0,
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(evidence),
            _json_dumps(data["safety_gate_result"]),
            data["computed_at"] or datetime_to_wire(utc_now()),
        ),
    )


def _insert_intent(connection: sqlite3.Connection, intent: DryRunIntent) -> None:
    data = intent.to_dict()
    if not data["strategy_observation_id"] or not data["risk_observation_id"]:
        raise ValueError("created dry-run intent requires strategy and risk observation ids")
    connection.execute(
        """
        INSERT INTO dry_run_intents (
            dry_run_intent_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            trade_date,
            code,
            name,
            side,
            order_type,
            intended_price,
            quantity,
            notional,
            status,
            reason_codes_json,
            evidence_json,
            source,
            observe_only,
            dry_run_only,
            live_order_allowed,
            gateway_command_allowed,
            created_at,
            expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0, ?, ?)
        """,
        (
            data["dry_run_intent_id"],
            data["candidate_instance_id"],
            data["strategy_observation_id"],
            data["risk_observation_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["side"],
            data["order_type"],
            data["intended_price"],
            data["quantity"],
            data["notional"],
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["source"],
            1 if data["observe_only"] else 0,
            data["created_at"],
            data["expires_at"],
        ),
    )


def _insert_order(connection: sqlite3.Connection, order: DryRunOrder) -> None:
    data = order.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_orders (
            dry_run_order_id,
            dry_run_intent_id,
            trade_date,
            code,
            name,
            side,
            order_type,
            quantity,
            requested_price,
            simulated_fill_price,
            filled_quantity,
            remaining_quantity,
            status,
            reason_codes_json,
            evidence_json,
            dry_run_only,
            created_at,
            simulated_submitted_at,
            simulated_filled_at,
            expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """,
        (
            data["dry_run_order_id"],
            data["dry_run_intent_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["side"],
            data["order_type"],
            data["quantity"],
            data["requested_price"],
            data["simulated_fill_price"],
            data["filled_quantity"],
            data["remaining_quantity"],
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["created_at"],
            data["simulated_submitted_at"],
            data["simulated_filled_at"],
            data["expires_at"],
        ),
    )


def _insert_execution(connection: sqlite3.Connection, execution: DryRunExecution) -> None:
    data = execution.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_executions (
            dry_run_execution_id,
            dry_run_order_id,
            dry_run_intent_id,
            trade_date,
            code,
            side,
            quantity,
            price,
            notional,
            commission,
            tax,
            executed_at,
            execution_type,
            dry_run_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            data["dry_run_execution_id"],
            data["dry_run_order_id"],
            data["dry_run_intent_id"],
            data["trade_date"],
            data["code"],
            data["side"],
            data["quantity"],
            data["price"],
            data["notional"],
            data["commission"],
            data["tax"],
            data["executed_at"],
            data["execution_type"],
        ),
    )


def _upsert_position_for_execution(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    execution: DryRunExecution,
    last_price: float,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_positions
        WHERE trade_date = ?
            AND code = ?
            AND status = 'OPEN'
        ORDER BY opened_at ASC
        LIMIT 1
        """,
        (execution.trade_date, execution.code),
    ).fetchone()
    now = datetime_to_wire(utc_now())
    if row is None:
        position = DryRunPosition(
            dry_run_position_id=new_message_id("dry_run_position"),
            trade_date=execution.trade_date,
            code=execution.code,
            name=str(order["name"]),
            quantity=execution.quantity,
            avg_price=execution.price,
            invested_notional=execution.notional,
            unrealized_pnl=0,
            last_price=last_price,
            status="OPEN",
            opened_at=now,
            updated_at=now,
        )
        data = position.to_dict()
        connection.execute(
            """
            INSERT INTO dry_run_positions (
                dry_run_position_id,
                trade_date,
                code,
                name,
                quantity,
                avg_price,
                invested_notional,
                realized_pnl,
                unrealized_pnl,
                last_price,
                status,
                opened_at,
                updated_at,
                closed_at,
                dry_run_only
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                data["dry_run_position_id"],
                data["trade_date"],
                data["code"],
                data["name"],
                data["quantity"],
                data["avg_price"],
                data["invested_notional"],
                data["realized_pnl"],
                data["unrealized_pnl"],
                data["last_price"],
                data["status"],
                data["opened_at"],
                data["updated_at"],
                data["closed_at"],
            ),
        )
        return data | {"event_type": "POSITION_OPENED"}

    old_quantity = int(row["quantity"])
    old_notional = float(row["invested_notional"])
    new_quantity = old_quantity + execution.quantity
    new_notional = old_notional + execution.notional
    avg_price = new_notional / new_quantity if new_quantity else 0
    unrealized_pnl = (last_price - avg_price) * new_quantity
    connection.execute(
        """
        UPDATE dry_run_positions
        SET quantity = ?,
            avg_price = ?,
            invested_notional = ?,
            unrealized_pnl = ?,
            last_price = ?,
            updated_at = ?
        WHERE dry_run_position_id = ?
        """,
        (
            new_quantity,
            avg_price,
            new_notional,
            unrealized_pnl,
            last_price,
            now,
            row["dry_run_position_id"],
        ),
    )
    return {
        "event_type": "POSITION_UPDATED",
        "dry_run_position_id": row["dry_run_position_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "quantity": new_quantity,
        "avg_price": avg_price,
        "invested_notional": new_notional,
        "unrealized_pnl": unrealized_pnl,
        "last_price": last_price,
        "status": row["status"],
        "updated_at": now,
        "dry_run_only": True,
    }


def _save_intent_rejection(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None,
    strategy_observation_id: str | None,
    risk_observation_id: str | None,
    trade_date: object,
    code: object,
    reason_codes: list[str] | tuple[str, ...],
    evidence: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_intent_rejections (
            rejection_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            trade_date,
            code,
            reason_codes_json,
            evidence_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_message_id("dry_run_rejection"),
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            None if trade_date is None else str(trade_date),
            None if code is None else str(code),
            _json_dumps(reason_codes),
            _json_dumps(evidence),
        ),
    )


def _insert_ledger(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    event_type: str,
    related_entity_type: str,
    related_entity_id: str,
    code: str | None,
    amount: float,
    quantity: int,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_ledger (
            ledger_id,
            trade_date,
            event_type,
            related_entity_type,
            related_entity_id,
            code,
            amount,
            quantity,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_message_id("dry_run_ledger"),
            trade_date,
            event_type.upper(),
            related_entity_type,
            related_entity_id,
            None if code is None else validate_stock_code(code),
            amount,
            quantity,
            _json_dumps(payload),
        ),
    )


def _insert_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    started_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_runs (run_id, trade_date, started_at, status)
        VALUES (?, ?, ?, 'RUNNING')
        """,
        (run_id, trade_date, started_at),
    )


def _complete_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    evaluated_count: int,
    eligible_count: int,
    status: str,
    intent_count: int = 0,
    order_count: int = 0,
    execution_count: int = 0,
    rejection_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE dry_run_runs
        SET completed_at = ?,
            evaluated_count = ?,
            eligible_count = ?,
            intent_count = ?,
            order_count = ?,
            execution_count = ?,
            rejection_count = ?,
            error_count = ?,
            status = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        (
            datetime_to_wire(utc_now()),
            evaluated_count,
            eligible_count,
            intent_count,
            order_count,
            execution_count,
            rejection_count,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )


def _record_error(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    candidate_instance_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_errors (
            run_id,
            candidate_instance_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            candidate_instance_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            _json_dumps(payload),
        ),
    )


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _strategy_latest_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM strategy_observations_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _risk_latest_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM risk_observations_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _latest_tick_row(connection: sqlite3.Connection, code: str | None) -> sqlite3.Row | None:
    if code is None:
        return None
    return connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (validate_stock_code(code),),
    ).fetchone()


def _dry_run_candidate_targets(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    clauses = ["overall_status = 'OBSERVE_PASS'"]
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT candidate_instance_id, trade_date, code
        FROM risk_observations_latest
        WHERE {" AND ".join(clauses)}
        ORDER BY evaluated_at DESC, candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _calculate_quantity_and_notional(price: float, settings: Settings) -> tuple[int, float]:
    if price <= 0:
        return 0, 0.0
    target_notional = min(
        settings.dry_run_default_position_notional,
        settings.dry_run_max_position_notional,
    )
    quantity = math.floor(target_notional / price)
    if quantity < settings.dry_run_min_quantity:
        return quantity, price * quantity
    notional = price * quantity
    if notional > settings.dry_run_max_position_notional:
        quantity = math.floor(settings.dry_run_max_position_notional / price)
        notional = price * quantity
    return quantity, notional


def _active_position_count_for_code(
    connection: sqlite3.Connection,
    trade_date: str,
    code: str,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM dry_run_positions
        WHERE trade_date = ?
            AND code = ?
            AND status IN ('OPEN')
        """,
        (trade_date, validate_stock_code(code)),
    ).fetchone()
    return int(row["count"])


def _active_position_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM dry_run_positions
        WHERE status IN ('OPEN')
        """
    ).fetchone()
    return int(row["count"])


def _recent_active_intent_count_for_code(
    connection: sqlite3.Connection,
    code: str,
    settings: Settings,
) -> int:
    cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=settings.dry_run_duplicate_cooldown_sec)
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM dry_run_intents
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_INTENT_STATUSES)})
            AND created_at >= ?
        """,
        (validate_stock_code(code), *sorted(ACTIVE_INTENT_STATUSES), cutoff),
    ).fetchone()
    return int(row["count"])


def _recent_active_order_count_for_code(
    connection: sqlite3.Connection,
    code: str,
    settings: Settings,
) -> int:
    cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=settings.dry_run_duplicate_cooldown_sec)
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM dry_run_orders
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_ORDER_STATUSES)})
            AND created_at >= ?
        """,
        (validate_stock_code(code), *sorted(ACTIVE_ORDER_STATUSES), cutoff),
    ).fetchone()
    return int(row["count"])


def _daily_intent_count(connection: sqlite3.Connection, trade_date: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM dry_run_intents
        WHERE trade_date = ?
        """,
        (trade_date,),
    ).fetchone()
    return int(row["count"])


def _ensure_dry_run_oms_enabled(settings: Settings) -> None:
    if not settings.dry_run_oms_enabled:
        raise ValueError(DryRunRejectionReason.DRY_RUN_DISABLED.value)
    if settings.dry_run_order_routing_enabled or settings.dry_run_gateway_command_enabled:
        raise ValueError(DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value)


def _list_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    clauses: list[str],
    params: list[Any],
    order_by: str,
    limit: int,
    mapper,
) -> list[dict[str, Any]]:
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    rows = connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        {where_sql}
        ORDER BY {order_by}
        LIMIT ?
        """,
        (*params, _bounded_limit(limit)),
    ).fetchall()
    return [mapper(row) for row in rows]


def _common_filters(
    *,
    trade_date: str | None = None,
    status: object | None = None,
    code: str | None = None,
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if status is not None:
        clauses.append("status = ?")
        params.append(str(getattr(status, "value", status)).upper())
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    return clauses, params


def _intent_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["observe_only"] = bool(item["observe_only"])
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["live_order_allowed"] = bool(item["live_order_allowed"])
    item["gateway_command_allowed"] = bool(item["gateway_command_allowed"])
    item["broker_order_sent"] = False
    return item


def _order_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    item["broker_order_sent"] = False
    return item


def _execution_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    item["broker_order_sent"] = False
    return item


def _position_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    return item


def _eligibility_check_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["eligible"] = bool(item["eligible"])
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["safety_gate"] = _json_object(item.pop("safety_gate_json"))
    item["dry_run_only"] = True
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    return item


def _ledger_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["payload"] = _json_object(item.pop("payload_json"))
    item["dry_run_only"] = True
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    return item


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["payload"] = _json_object(item.pop("payload_json"))
    return item


def _candidate_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "state": row["state"],
    }


def _strategy_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "strategy_observation_id": row["strategy_observation_id"],
        "overall_status": row["overall_status"],
        "evaluated_at": row["evaluated_at"],
        "observe_only": bool(row["observe_only"]),
    }


def _risk_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "risk_observation_id": row["risk_observation_id"],
        "strategy_observation_id": row["strategy_observation_id"],
        "overall_status": row["overall_status"],
        "reason_codes": _json_array(row["reason_codes_json"]),
        "evaluated_at": row["evaluated_at"],
        "observe_only": bool(row["observe_only"]),
    }


def _tick_evidence(row: sqlite3.Row, tick_age: float) -> dict[str, Any]:
    return {
        "code": row["code"],
        "name": row["name"],
        "price": row["price"],
        "event_ts": row["event_ts"],
        "quality_status": row["quality_status"],
        "tick_age_sec": tick_age,
    }


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _placeholders(values: set[str]) -> str:
    return ",".join("?" for _ in values)


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


def _map_admission_reasons(
    reasons: Sequence[str],
    reason_map: Mapping[str, str],
) -> list[str]:
    return [reason_map.get(str(reason).upper(), str(reason).upper()) for reason in reasons]


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _json_array(value: str | None) -> list[str]:
    if not value:
        return []
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
