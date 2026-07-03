from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    datetime_to_wire,
    market_time_str,
    market_today,
    new_message_id,
    normalize_value,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.live_sim.models import (
    LiveSimEligibility,
    LiveSimExecutionRecord,
    LiveSimIntent,
    LiveSimOrderRecord,
    LiveSimReconcileSnapshot,
)
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import (
    LiveSimIntentStatus,
    LiveSimOrderStatus,
    LiveSimOrderType,
    LiveSimSide,
)
from domain.market.quality import tick_age_seconds
from storage.gateway_command_store import enqueue_command

from services.admission import AdmissionPolicy, AdmissionReason, evaluate_trade_admission
from services.config import Settings, load_settings
from services.entry_timing.tick_size import add_ticks
from services.live_sim.safety_gate import check_live_sim_safety_gate, is_simulation_like

ACTIVE_LIVE_SIM_INTENT_STATUSES = {
    LiveSimIntentStatus.CREATED.value,
    LiveSimIntentStatus.COMMAND_QUEUED.value,
}
ACTIVE_LIVE_SIM_ORDER_STATUSES = {
    LiveSimOrderStatus.INTENT_CREATED.value,
    LiveSimOrderStatus.COMMAND_QUEUED.value,
    LiveSimOrderStatus.COMMAND_DISPATCHED.value,
    LiveSimOrderStatus.BROKER_ACKED.value,
    LiveSimOrderStatus.PARTIALLY_FILLED.value,
    LiveSimOrderStatus.CANCEL_REQUESTED.value,
    LiveSimOrderStatus.CANCEL_COMMAND_QUEUED.value,
    LiveSimOrderStatus.EXIT_REQUESTED.value,
    LiveSimOrderStatus.EXIT_COMMAND_QUEUED.value,
}
TERMINAL_LIVE_SIM_ORDER_STATUSES = {
    LiveSimOrderStatus.BROKER_REJECTED.value,
    LiveSimOrderStatus.FILLED.value,
    LiveSimOrderStatus.CANCELLED.value,
    LiveSimOrderStatus.FAILED.value,
    LiveSimOrderStatus.EXPIRED.value,
    LiveSimOrderStatus.CANCEL_ACKED.value,
    LiveSimOrderStatus.CANCEL_REJECTED.value,
    LiveSimOrderStatus.EXIT_FILLED.value,
}
ACTIVE_LIVE_SIM_POSITION_STATUSES = {"OPEN", "CLOSING", "RECONCILE_MISMATCH"}
ACTIVE_CANCEL_INTENT_STATUSES = {"CREATED", "COMMAND_QUEUED"}
ACTIVE_EXIT_INTENT_STATUSES = {"CREATED", "COMMAND_QUEUED"}
ACTIVE_EXIT_SIGNAL_STATUSES = {"SIGNALLED", "EXIT_INTENT_CREATED", "COMMAND_QUEUED"}

LIVE_SIM_ADMISSION_REASON_MAP = {
    AdmissionReason.CANDIDATE_NOT_FOUND.value: LiveSimReasonCode.CANDIDATE_NOT_FOUND.value,
    AdmissionReason.CANDIDATE_NOT_CONTEXT_READY.value: (
        LiveSimReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.CANDIDATE_CONTEXT_MISSING.value: (
        LiveSimReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.STRATEGY_OBSERVATION_MISSING.value: (
        LiveSimReasonCode.STRATEGY_OBSERVATION_MISSING.value
    ),
    AdmissionReason.STRATEGY_NOT_MATCHED.value: LiveSimReasonCode.STRATEGY_NOT_MATCHED.value,
    AdmissionReason.STRATEGY_OBSERVE_ONLY_MISMATCH.value: (
        LiveSimReasonCode.STRATEGY_NOT_MATCHED.value
    ),
    AdmissionReason.RISK_OBSERVATION_MISSING.value: (
        LiveSimReasonCode.RISK_OBSERVATION_MISSING.value
    ),
    AdmissionReason.RISK_NOT_OBSERVE_PASS.value: LiveSimReasonCode.RISK_NOT_OBSERVE_PASS.value,
    AdmissionReason.RISK_OBSERVE_ONLY_MISMATCH.value: (
        LiveSimReasonCode.RISK_NOT_OBSERVE_PASS.value
    ),
    AdmissionReason.LATEST_TICK_MISSING.value: LiveSimReasonCode.LATEST_TICK_MISSING.value,
    AdmissionReason.LATEST_TICK_STALE.value: LiveSimReasonCode.LATEST_TICK_STALE.value,
    AdmissionReason.DRY_RUN_EVIDENCE_MISSING.value: (
        LiveSimReasonCode.DRY_RUN_EVIDENCE_MISSING.value
    ),
}


@dataclass(frozen=True, kw_only=True)
class LiveSimRunResult:
    run_id: str
    trade_date: str | None = None
    evaluated_count: int = 0
    eligible_count: int = 0
    intent_count: int = 0
    command_count: int = 0
    rejection_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    live_sim_only: bool = True
    live_real_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "evaluated_count": self.evaluated_count,
            "eligible_count": self.eligible_count,
            "intent_count": self.intent_count,
            "command_count": self.command_count,
            "rejection_count": self.rejection_count,
            "error_count": self.error_count,
            "status": self.status,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }


@dataclass(frozen=True, kw_only=True)
class LiveSimLifecycleRunResult:
    run_id: str
    run_type: str
    evaluated_count: int = 0
    signal_count: int = 0
    intent_count: int = 0
    command_count: int = 0
    skipped_count: int = 0
    rejection_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    details: Mapping[str, Any] | None = None
    live_sim_only: bool = True
    live_real_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "evaluated_count": self.evaluated_count,
            "signal_count": self.signal_count,
            "intent_count": self.intent_count,
            "command_count": self.command_count,
            "skipped_count": self.skipped_count,
            "rejection_count": self.rejection_count,
            "error_count": self.error_count,
            "status": self.status,
            "details": normalize_value(dict(self.details or {})),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }


def evaluate_live_sim_eligibility(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
) -> LiveSimEligibility:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    dry_run_evidence = _latest_dry_run_evidence(connection, normalized_id)
    admission = evaluate_trade_admission(
        connection,
        normalized_id,
        AdmissionPolicy(
            name="live_sim_intent",
            require_candidate_context_ready=(
                resolved_settings.live_sim_require_candidate_context_ready
            ),
            require_strategy_matched=resolved_settings.live_sim_require_strategy_matched,
            require_risk_observe_pass=resolved_settings.live_sim_require_risk_observe_pass,
            require_fresh_tick=resolved_settings.live_sim_require_fresh_tick,
            stale_tick_sec=resolved_settings.live_sim_stale_tick_sec,
            require_dry_run_evidence=(
                resolved_settings.live_sim_require_dry_run_evidence
            ),
        ),
        dry_run_evidence=dry_run_evidence,
    )
    safety_gate = check_live_sim_safety_gate(
        connection,
        resolved_settings,
        purpose="NEW_BUY",
        enforce_daily_loss_limit=True,
        enforce_entry_window=True,
        trade_date=admission.trade_date,
    )

    reason_codes: list[str] = _map_admission_reasons(
        admission.reason_codes,
        LIVE_SIM_ADMISSION_REASON_MAP,
    )
    evidence: dict[str, Any] = {
        "candidate_instance_id": normalized_id,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "ai_artifacts_used": False,
    }
    evidence.update(admission.to_evidence())
    safety_gate_evidence = safety_gate.to_dict()
    evidence["safety_gate"] = safety_gate_evidence
    evidence["entry_window"] = safety_gate_evidence.get("entry_window", {})
    if not safety_gate.passed:
        reason_codes.extend(safety_gate.reason_codes)
    trade_date = admission.trade_date
    code = admission.code
    name = admission.name
    price = 0.0
    if admission.latest_tick_evidence:
        price = float(admission.latest_tick_evidence["price"])
    limit_price = _live_sim_buy_limit_price(price, resolved_settings) if price > 0 else 0.0

    side = LiveSimSide.BUY
    order_type = LiveSimOrderType(resolved_settings.live_sim_default_order_type)
    if side is LiveSimSide.BUY and not resolved_settings.live_sim_allow_buy:
        reason_codes.append(LiveSimReasonCode.SELL_NOT_ALLOWED.value)
    if side is LiveSimSide.SELL and not resolved_settings.live_sim_allow_sell:
        reason_codes.append(LiveSimReasonCode.SELL_NOT_ALLOWED.value)
    if order_type is LiveSimOrderType.MARKET and not resolved_settings.live_sim_allow_market_order:
        reason_codes.append(LiveSimReasonCode.MARKET_ORDER_NOT_ALLOWED.value)
    if order_type is LiveSimOrderType.LIMIT and not resolved_settings.live_sim_allow_limit_order:
        reason_codes.append(LiveSimReasonCode.MARKET_ORDER_NOT_ALLOWED.value)

    quantity, notional = _calculate_live_sim_quantity_and_notional(
        limit_price,
        resolved_settings,
        dry_run_evidence=dry_run_evidence,
    )
    evidence["sizing"] = {
        "max_order_notional": resolved_settings.live_sim_max_order_notional,
        "dry_run_notional": dry_run_evidence.get("notional") if dry_run_evidence else None,
        "current_price": price,
        "limit_price": limit_price,
        "buy_price_offset_ticks": resolved_settings.live_sim_buy_price_offset_ticks,
        "quantity": quantity,
        "notional": notional,
    }
    if quantity < 1:
        reason_codes.append(LiveSimReasonCode.INVALID_QUANTITY.value)
    if notional <= 0 or notional > resolved_settings.live_sim_max_order_notional:
        reason_codes.append(LiveSimReasonCode.MAX_ORDER_NOTIONAL_EXCEEDED.value)

    if code is not None and trade_date is not None:
        if _recent_active_live_sim_count_for_code(connection, code, resolved_settings) > 0:
            reason_codes.append(LiveSimReasonCode.DUPLICATE_LIVE_SIM_ORDER.value)
        if _latest_reconcile_blocks_new_buy(connection, resolved_settings):
            reason_codes.append(LiveSimReasonCode.LIVE_SIM_RECONCILE_MISMATCH_BLOCK.value)
        if (
            not resolved_settings.live_sim_position_allow_scale_in
            and _open_position_count_for_code(connection, code) > 0
        ):
            reason_codes.append(LiveSimReasonCode.LIVE_SIM_OPEN_POSITION_EXISTS.value)
        if _active_exit_count_for_code(connection, code) > 0:
            reason_codes.append(LiveSimReasonCode.LIVE_SIM_ACTIVE_EXIT_EXISTS.value)
        if _active_cancel_count_for_code(connection, code) > 0:
            reason_codes.append(LiveSimReasonCode.LIVE_SIM_ACTIVE_CANCEL_EXISTS.value)
        if _unresolved_lifecycle_error_count(connection, code=code) > 0:
            reason_codes.append(LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value)
        if (
            _daily_order_count(connection, trade_date)
            >= resolved_settings.live_sim_max_daily_order_count
        ):
            reason_codes.append(LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value)
        daily_notional = _daily_order_notional(connection, trade_date)
        if daily_notional + notional > resolved_settings.live_sim_max_daily_notional:
            reason_codes.append(LiveSimReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value)
    if _active_order_count(connection) >= resolved_settings.live_sim_max_active_orders:
        reason_codes.append(LiveSimReasonCode.ACTIVE_ORDER_LIMIT_EXCEEDED.value)
    if _active_position_count(connection) >= resolved_settings.live_sim_max_active_positions:
        reason_codes.append(LiveSimReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value)

    reason_codes = _merge_reasons(reason_codes)
    eligibility = LiveSimEligibility(
        eligible=not reason_codes,
        candidate_instance_id=normalized_id,
        strategy_observation_id=admission.strategy_observation_id,
        risk_observation_id=admission.risk_observation_id,
        status="ELIGIBLE" if not reason_codes else "INELIGIBLE",
        reason_codes=reason_codes,
        evidence_json=evidence
        | {
            "trade_date": trade_date,
            "code": code,
            "name": name,
            "account_id": resolved_settings.live_sim_account_id,
            "order_type": order_type.value,
            "side": side.value,
        },
        safety_gate_result=safety_gate.to_dict(),
        computed_at=datetime_to_wire(utc_now()),
    )
    return eligibility


def create_live_sim_intent(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
    source: str = "manual",
    idempotency_suffix: str | None = None,
    extra_evidence: Mapping[str, Any] | None = None,
) -> LiveSimIntent:
    resolved_settings = settings or load_settings()
    eligibility = evaluate_live_sim_eligibility(
        connection, candidate_instance_id, resolved_settings
    )
    evidence = dict(eligibility.evidence_json) | {
        "source": source,
        **dict(extra_evidence or {}),
    }
    candidate = _candidate_row(connection, candidate_instance_id)
    strategy = _strategy_latest_row(connection, candidate_instance_id)
    risk = _risk_latest_row(connection, candidate_instance_id)

    if not eligibility.eligible:
        _save_rejection(
            connection,
            candidate_instance_id=candidate_instance_id,
            strategy_observation_id=eligibility.strategy_observation_id,
            risk_observation_id=eligibility.risk_observation_id,
            trade_date=evidence.get("trade_date"),
            account_id=resolved_settings.live_sim_account_id,
            code=evidence.get("code"),
            reason_codes=eligibility.reason_codes,
            evidence=evidence,
        )
        connection.commit()
        return LiveSimIntent(
            live_sim_intent_id=new_message_id("live_sim_intent_rejected"),
            candidate_instance_id=candidate_instance_id,
            strategy_observation_id=eligibility.strategy_observation_id,
            risk_observation_id=eligibility.risk_observation_id,
            dry_run_intent_id=_json_object(evidence.get("dry_run")).get("dry_run_intent_id"),
            dry_run_order_id=_json_object(evidence.get("dry_run")).get("dry_run_order_id"),
            trade_date=str(evidence.get("trade_date") or "UNKNOWN"),
            account_id=resolved_settings.live_sim_account_id or "SIMULATION_ACCOUNT_REQUIRED",
            code=str(evidence.get("code") or "000000"),
            name=str(evidence.get("name") or "UNKNOWN"),
            side=evidence.get("side", LiveSimSide.BUY.value),
            order_type=evidence.get("order_type", LiveSimOrderType.LIMIT.value),
            quantity=0,
            limit_price=None,
            notional=0,
            status=LiveSimIntentStatus.REJECTED,
            reason_codes=eligibility.reason_codes,
            evidence_json=evidence,
            idempotency_key=_idempotency_key(
                str(evidence.get("trade_date") or "UNKNOWN"),
                str(evidence.get("code") or "000000"),
                candidate_instance_id,
                eligibility.strategy_observation_id,
                eligibility.risk_observation_id,
                suffix=idempotency_suffix,
            ),
            created_at=datetime_to_wire(utc_now()),
        )

    if candidate is None or strategy is None or risk is None:
        raise ValueError("eligible LIVE_SIM intent requires candidate, strategy, and risk rows")
    tick = _latest_tick_row(connection, candidate["code"])
    if tick is None:
        raise ValueError("eligible LIVE_SIM intent requires latest tick")

    price = float(tick["price"])
    limit_price = _live_sim_buy_limit_price(price, resolved_settings)
    quantity, notional = _calculate_live_sim_quantity_and_notional(
        limit_price,
        resolved_settings,
        dry_run_evidence=_json_object(evidence.get("dry_run")),
    )
    now = utc_now()
    dry_run_evidence = _json_object(evidence.get("dry_run"))
    intent = LiveSimIntent(
        live_sim_intent_id=new_message_id("live_sim_intent"),
        candidate_instance_id=str(candidate["candidate_instance_id"]),
        strategy_observation_id=str(strategy["strategy_observation_id"]),
        risk_observation_id=str(risk["risk_observation_id"]),
        dry_run_intent_id=dry_run_evidence.get("dry_run_intent_id"),
        dry_run_order_id=dry_run_evidence.get("dry_run_order_id"),
        trade_date=str(candidate["trade_date"]),
        account_id=resolved_settings.live_sim_account_id,
        code=str(candidate["code"]),
        name=str(candidate["name"]),
        side=LiveSimSide.BUY,
        order_type=LiveSimOrderType(resolved_settings.live_sim_default_order_type),
        quantity=quantity,
        limit_price=limit_price,
        notional=notional,
        status=LiveSimIntentStatus.CREATED,
        reason_codes=[LiveSimReasonCode.OBSERVE_ONLY_AI_ARTIFACT_IGNORED.value],
        evidence_json=evidence
        | {
            "latest_tick_event_ts": tick["event_ts"],
            "current_price": price,
            "quantity": quantity,
            "notional": notional,
            "limit_price": limit_price,
            "price_policy": _live_sim_buy_price_policy_evidence(
                price,
                limit_price,
                resolved_settings,
            ),
        },
        idempotency_key=_idempotency_key(
            str(candidate["trade_date"]),
            str(candidate["code"]),
            str(candidate["candidate_instance_id"]),
            str(strategy["strategy_observation_id"]),
            str(risk["risk_observation_id"]),
            suffix=idempotency_suffix,
        ),
        created_at=now,
        expires_at=now + timedelta(seconds=resolved_settings.live_sim_order_ttl_sec),
    )
    _insert_intent(connection, intent)
    connection.commit()
    return intent


def queue_live_sim_order_command(
    connection: sqlite3.Connection,
    live_sim_intent_id: str,
    settings: Settings | None = None,
) -> LiveSimOrderRecord:
    resolved_settings = settings or load_settings()
    intent_row = get_live_sim_intent(connection, live_sim_intent_id)
    if intent_row is None:
        raise ValueError(f"LIVE_SIM intent not found: {live_sim_intent_id}")
    if intent_row["status"] != LiveSimIntentStatus.CREATED.value:
        _record_error(
            connection,
            live_sim_intent_id=live_sim_intent_id,
            live_sim_order_id=None,
            code=intent_row.get("code"),
            error_message=LiveSimReasonCode.INVALID_INTENT_STATUS.value,
            payload={"intent": intent_row},
        )
        connection.commit()
        raise ValueError(f"LIVE_SIM intent cannot be queued from status: {intent_row['status']}")

    safety_gate = check_live_sim_safety_gate(
        connection,
        resolved_settings,
        purpose="NEW_BUY",
        enforce_daily_loss_limit=intent_row["side"] == LiveSimSide.BUY.value,
        enforce_entry_window=intent_row["side"] == LiveSimSide.BUY.value,
        trade_date=intent_row["trade_date"],
    )
    queue_reasons = list(safety_gate.reason_codes)
    if intent_row["side"] == LiveSimSide.SELL.value and not resolved_settings.live_sim_allow_sell:
        queue_reasons.append(LiveSimReasonCode.SELL_NOT_ALLOWED.value)
    if intent_row["order_type"] == LiveSimOrderType.MARKET.value:
        queue_reasons.append(LiveSimReasonCode.MARKET_ORDER_NOT_ALLOWED.value)
    if _active_order_count(connection) >= resolved_settings.live_sim_max_active_orders:
        queue_reasons.append(LiveSimReasonCode.ACTIVE_ORDER_LIMIT_EXCEEDED.value)
    if (
        _recent_active_live_sim_count_for_code(
            connection,
            intent_row["code"],
            resolved_settings,
            exclude_intent_id=live_sim_intent_id,
        )
        > 0
    ):
        queue_reasons.append(LiveSimReasonCode.DUPLICATE_LIVE_SIM_ORDER.value)

    queue_reasons = _merge_reasons(queue_reasons)
    if queue_reasons:
        _save_rejection(
            connection,
            candidate_instance_id=intent_row["candidate_instance_id"],
            strategy_observation_id=intent_row["strategy_observation_id"],
            risk_observation_id=intent_row["risk_observation_id"],
            trade_date=intent_row["trade_date"],
            account_id=intent_row["account_id"],
            code=intent_row["code"],
            reason_codes=queue_reasons,
            evidence={"intent": intent_row, "safety_gate": safety_gate.to_dict()},
        )
        connection.commit()
        raise ValueError(",".join(queue_reasons))

    payload = _build_gateway_send_order_payload(intent_row, resolved_settings)
    command = GatewayCommand(
        command_type="send_order",
        source="live_sim",
        payload=payload,
        idempotency_key=intent_row["idempotency_key"],
    )
    enqueue_result = enqueue_command(
        connection,
        command,
        expires_at=intent_row.get("expires_at"),
    )
    if not enqueue_result.accepted:
        _save_rejection(
            connection,
            candidate_instance_id=intent_row["candidate_instance_id"],
            strategy_observation_id=intent_row["strategy_observation_id"],
            risk_observation_id=intent_row["risk_observation_id"],
            trade_date=intent_row["trade_date"],
            account_id=intent_row["account_id"],
            code=intent_row["code"],
            reason_codes=[LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value],
            evidence={
                "intent": intent_row,
                "enqueue_error": enqueue_result.error_message,
                "payload_hash": enqueue_result.payload_hash,
            },
        )
        connection.commit()
        raise ValueError(
            enqueue_result.error_message or LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value
        )

    now = datetime_to_wire(utc_now())
    order = LiveSimOrderRecord(
        live_sim_order_id=new_message_id("live_sim_order"),
        live_sim_intent_id=live_sim_intent_id,
        gateway_command_id=command.command_id,
        account_id=intent_row["account_id"],
        code=intent_row["code"],
        name=intent_row["name"],
        side=intent_row["side"],
        order_type=intent_row["order_type"],
        quantity=int(intent_row["quantity"]),
        limit_price=intent_row["limit_price"],
        notional=float(intent_row["notional"]),
        status=LiveSimOrderStatus.COMMAND_QUEUED,
        filled_quantity=0,
        remaining_quantity=int(intent_row["quantity"]),
        idempotency_key=intent_row["idempotency_key"],
        created_at=now,
        command_queued_at=now,
    )
    _insert_order(connection, order, trade_date=str(intent_row["trade_date"]))
    connection.execute(
        """
        UPDATE live_sim_intents
        SET status = ?,
            gateway_command_id = ?
        WHERE live_sim_intent_id = ?
        """,
        (LiveSimIntentStatus.COMMAND_QUEUED.value, command.command_id, live_sim_intent_id),
    )
    connection.commit()
    return order


def handle_live_sim_gateway_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    event_type = event.event_type.strip().lower()
    if event_type in {"command_started", "command_ack", "command_failed"}:
        return _handle_live_sim_command_event(connection, event)
    if event_type == "execution_event":
        return _handle_live_sim_execution_event(connection, event, resolved_settings)
    if event_type in {"order_rejected", "cancel_ack", "cancel_rejected"}:
        return _handle_live_sim_order_lifecycle_event(connection, event)
    if event_type in {"balance_snapshot", "account_snapshot", "kiwoom_balance_chejan"}:
        return apply_live_sim_broker_snapshot(connection, event, resolved_settings)
    if event_type in {"kiwoom_order_chejan"}:
        return _handle_live_sim_chejan_event(connection, event, resolved_settings)
    if _looks_live_sim_event(event):
        _record_error(
            connection,
            live_sim_intent_id=_json_object(event.payload.get("metadata")).get(
                "live_sim_intent_id"
            ),
            live_sim_order_id=_json_object(event.payload.get("metadata")).get(
                "live_sim_order_id"
            ),
            code=event.payload.get("code"),
            error_message="UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
            payload=event.to_dict(),
        )
        connection.commit()
        return {"handled": False, "reason": "unknown_live_sim_event"}
    return {"handled": False, "reason": "event_type_not_live_sim"}


def reconcile_live_sim(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> LiveSimReconcileSnapshot:
    return reconcile_live_sim_orders_and_positions(connection, settings=settings)


def reconcile_live_sim_orders_and_positions(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> LiveSimReconcileSnapshot:
    resolved_settings = settings or load_settings()
    trade_date = _today_trade_date()
    open_orders = list_live_sim_orders(
        connection,
        trade_date=trade_date,
        status=None,
        limit=500,
        open_only=True,
    )
    positions = list_live_sim_positions(connection, status=None, limit=500, open_only=True)
    mismatch_count = 0
    mismatches: list[dict[str, Any]] = []
    for order in open_orders:
        if order.get("gateway_command_id") is None:
            mismatch_count += 1
            mismatches.append({"order": order["live_sim_order_id"], "reason": "missing_command"})
        command = _gateway_command_row(connection, order.get("gateway_command_id"))
        if command is None:
            mismatch_count += 1
            mismatches.append({"order": order["live_sim_order_id"], "reason": "command_missing"})
        elif command["status"] in {"FAILED", "REJECTED", "EXPIRED"} and order["status"] not in {
            LiveSimOrderStatus.FAILED.value,
            LiveSimOrderStatus.EXPIRED.value,
            LiveSimOrderStatus.BROKER_REJECTED.value,
        }:
            mismatch_count += 1
            mismatches.append(
                {
                    "order": order["live_sim_order_id"],
                    "reason": "command_terminal_mismatch",
                    "command_status": command["status"],
                }
            )
        execution_sum = _execution_quantity_sum(connection, order["live_sim_order_id"])
        if execution_sum != int(order["filled_quantity"]):
            mismatch_count += 1
            mismatches.append(
                {
                    "order": order["live_sim_order_id"],
                    "reason": "filled_quantity_execution_sum_mismatch",
                    "filled_quantity": order["filled_quantity"],
                    "execution_sum": execution_sum,
                }
            )
        if int(order["remaining_quantity"]) < 0:
            mismatch_count += 1
            mismatches.append(
                {
                    "order": order["live_sim_order_id"],
                    "reason": "negative_remaining_quantity",
                    "remaining_quantity": order["remaining_quantity"],
                }
            )
        if int(order["filled_quantity"]) + int(order["remaining_quantity"]) > int(
            order["quantity"]
        ):
            mismatch_count += 1
            mismatches.append(
                {
                    "order": order["live_sim_order_id"],
                    "reason": "order_quantity_inconsistent",
                    "quantity": order["quantity"],
                    "filled_quantity": order["filled_quantity"],
                    "remaining_quantity": order["remaining_quantity"],
                }
            )

    for position in positions:
        fill_quantity = _net_position_quantity_from_fills(
            connection,
            str(position["account_id"]),
            str(position["code"]),
        )
        if fill_quantity != int(position["quantity"]):
            mismatch_count += 1
            mismatches.append(
                {
                    "position": position["position_id"],
                    "reason": "position_quantity_fill_sum_mismatch",
                    "position_quantity": position["quantity"],
                    "fill_quantity": fill_quantity,
                }
            )
        if int(position["quantity"]) < 0 or int(position["available_quantity"]) < 0:
            mismatch_count += 1
            mismatches.append(
                {
                    "position": position["position_id"],
                    "reason": "negative_position_quantity",
                }
            )
        expected_entry_notional = float(position["avg_entry_price"]) * int(position["quantity"])
        entry_notional_delta = abs(
            float(position["total_entry_notional"]) - expected_entry_notional
        )
        if entry_notional_delta > resolved_settings.live_sim_reconcile_notional_tolerance:
            mismatch_count += 1
            mismatches.append(
                {
                    "position": position["position_id"],
                    "reason": "position_entry_notional_mismatch",
                    "total_entry_notional": position["total_entry_notional"],
                    "expected_entry_notional": expected_entry_notional,
                    "delta": entry_notional_delta,
                    "tolerance": resolved_settings.live_sim_reconcile_notional_tolerance,
                }
            )
        if _active_exit_count_for_code(connection, str(position["code"])) > 0 and str(
            position["status"]
        ).upper() == "OPEN":
            mismatches.append(
                {
                    "position": position["position_id"],
                    "reason": "active_exit_exists_for_open_position",
                }
            )

    stale_orders = _stale_open_order_count(connection, resolved_settings)
    if stale_orders > 0:
        mismatch_count += stale_orders
        mismatches.append({"reason": "stale_open_orders", "count": stale_orders})

    broker_snapshot_available = False
    blocking_new_buy = bool(
        mismatch_count > 0 and resolved_settings.live_sim_reconcile_block_new_buy_on_mismatch
    )
    allow_exit = bool(resolved_settings.live_sim_reconcile_allow_exit_on_mismatch)
    status = (
        "RECONCILE_MISMATCH"
        if mismatch_count
        else "LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT"
        if not broker_snapshot_available
        else "OK"
    )

    snapshot = LiveSimReconcileSnapshot(
        reconcile_id=new_message_id("live_sim_reconcile"),
        account_id=resolved_settings.live_sim_account_id or "SIMULATION_ACCOUNT_REQUIRED",
        trade_date=trade_date,
        broker_open_order_count=0,
        broker_position_count=0,
        local_open_order_count=len(open_orders),
        local_position_count=len(positions),
        mismatch_count=mismatch_count,
        status=status,
        snapshot_json={
            "broker_snapshot_available": broker_snapshot_available,
            "broker_snapshot_status": "BROKER_SNAPSHOT_UNAVAILABLE",
            "open_orders": open_orders,
            "positions": positions,
            "mismatches": mismatches,
            "notional_tolerance": resolved_settings.live_sim_reconcile_notional_tolerance,
            "blocking_new_buy": blocking_new_buy,
            "allow_exit": allow_exit,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
        },
        created_at=datetime_to_wire(utc_now()),
    )
    _insert_reconcile_snapshot(connection, snapshot)
    connection.execute(
        """
        UPDATE live_sim_reconcile_snapshots
        SET blocking_new_buy = ?,
            allow_exit = ?
        WHERE reconcile_id = ?
        """,
        (1 if blocking_new_buy else 0, 1 if allow_exit else 0, snapshot.reconcile_id),
    )
    if mismatch_count:
        _record_lifecycle_event(
            connection,
            event_type="RECONCILE_MISMATCH",
            entity_type="RECONCILE",
            entity_id=snapshot.reconcile_id,
            status=snapshot.status,
            reason="RECONCILE_MISMATCH",
            evidence={"mismatches": mismatches, "blocking_new_buy": blocking_new_buy},
        )
    connection.commit()
    return snapshot


def get_live_sim_status(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    safety_gate = check_live_sim_safety_gate(connection, resolved_settings)
    return {
        "enabled": resolved_settings.live_sim_enabled,
        "order_routing_enabled": resolved_settings.live_sim_order_routing_enabled,
        "gateway_command_enabled": resolved_settings.live_sim_gateway_command_enabled,
        "kill_switch": resolved_settings.live_sim_kill_switch,
        "live_real_allowed": False,
        "account_id_configured": bool(resolved_settings.live_sim_account_id.strip()),
        "account_mode": resolved_settings.live_sim_account_mode,
        "broker_env": resolved_settings.live_sim_broker_env,
        "server_mode": resolved_settings.live_sim_server_mode,
        "safety_gate": safety_gate.to_dict(),
        "intent_count": _count_rows(connection, "live_sim_intents"),
        "order_count": _count_rows(connection, "live_sim_orders"),
        "execution_count": _count_rows(connection, "live_sim_executions"),
        "position_count": _count_rows(connection, "live_sim_positions"),
        "open_position_count": _active_position_count(connection),
        "cancel_pending_count": _active_cancel_count(connection),
        "active_exit_signal_count": _active_exit_signal_count(connection),
        "rejection_count": _count_rows(connection, "live_sim_rejections"),
        "open_order_count": _active_order_count(connection),
        "max_order_notional": resolved_settings.live_sim_max_order_notional,
        "max_daily_order_count": resolved_settings.live_sim_max_daily_order_count,
        "max_daily_notional": resolved_settings.live_sim_max_daily_notional,
        "allow_buy": resolved_settings.live_sim_allow_buy,
        "allow_sell": resolved_settings.live_sim_allow_sell,
        "allow_exit_sell_close_only": resolved_settings.live_sim_exit_allow_sell_close_only,
        "cancel_enabled": resolved_settings.live_sim_cancel_enabled,
        "exit_engine_enabled": resolved_settings.live_sim_exit_engine_enabled,
        "reconcile_enabled": resolved_settings.live_sim_reconcile_enabled,
        "allow_market_order": resolved_settings.live_sim_allow_market_order,
        "allow_limit_order": resolved_settings.live_sim_allow_limit_order,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "warnings": [
            "LIVE_SIM is simulation-account only.",
            "LIVE_REAL remains disabled.",
            "Dashboard order buttons are not available.",
        ],
    }


def list_live_sim_intents(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: object | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    return _list_rows(
        connection,
        "live_sim_intents",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, live_sim_intent_id DESC",
        limit=limit,
        mapper=_intent_row_to_dict,
    )


def get_live_sim_intent(
    connection: sqlite3.Connection,
    live_sim_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_intents WHERE live_sim_intent_id = ?",
        (require_non_empty_str(live_sim_intent_id, "live_sim_intent_id"),),
    ).fetchone()
    return None if row is None else _intent_row_to_dict(row)


def list_live_sim_orders(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: object | None = None,
    code: str | None = None,
    limit: int = 100,
    open_only: bool = False,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    if open_only:
        clauses.append(f"status IN ({_placeholders(ACTIVE_LIVE_SIM_ORDER_STATUSES)})")
        params.extend(sorted(ACTIVE_LIVE_SIM_ORDER_STATUSES))
    return _list_rows(
        connection,
        "live_sim_orders",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, live_sim_order_id DESC",
        limit=limit,
        mapper=_order_row_to_dict,
    )


def get_live_sim_order(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_orders WHERE live_sim_order_id = ?",
        (require_non_empty_str(live_sim_order_id, "live_sim_order_id"),),
    ).fetchone()
    return None if row is None else _order_row_to_dict(row)


def list_live_sim_executions(
    connection: sqlite3.Connection,
    *,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    return _list_rows(
        connection,
        "live_sim_executions",
        clauses=clauses,
        params=params,
        order_by="executed_at DESC, live_sim_execution_id DESC",
        limit=limit,
        mapper=_execution_row_to_dict,
    )


def list_live_sim_rejections(
    connection: sqlite3.Connection,
    *,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    return _list_rows(
        connection,
        "live_sim_rejections",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, rejection_id DESC",
        limit=limit,
        mapper=_rejection_row_to_dict,
    )


def list_live_sim_reconcile_snapshots(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _list_rows(
        connection,
        "live_sim_reconcile_snapshots",
        clauses=[],
        params=[],
        order_by="created_at DESC, reconcile_id DESC",
        limit=limit,
        mapper=_reconcile_row_to_dict,
    )


def list_live_sim_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _list_rows(
        connection,
        "live_sim_errors",
        clauses=[],
        params=[],
        order_by="created_at DESC, id DESC",
        limit=limit,
        mapper=_error_row_to_dict,
    )


def list_live_sim_positions(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: object | None = None,
    code: str | None = None,
    limit: int = 100,
    open_only: bool = False,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    if open_only:
        clauses.append(f"status IN ({_placeholders(ACTIVE_LIVE_SIM_POSITION_STATUSES)})")
        params.extend(sorted(ACTIVE_LIVE_SIM_POSITION_STATUSES))
    return _list_rows(
        connection,
        "live_sim_positions",
        clauses=clauses,
        params=params,
        order_by="updated_at DESC, position_id DESC",
        limit=limit,
        mapper=_position_row_to_dict,
    )


def get_live_sim_position(
    connection: sqlite3.Connection,
    position_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_positions WHERE position_id = ?",
        (require_non_empty_str(position_id, "position_id"),),
    ).fetchone()
    return None if row is None else _position_row_to_dict(row)


def list_live_sim_exit_signals(
    connection: sqlite3.Connection,
    *,
    code: str | None = None,
    status: object | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    if status is not None:
        clauses.append("status = ?")
        params.append(str(getattr(status, "value", status)).upper())
    return _list_rows(
        connection,
        "live_sim_exit_signals",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, exit_signal_id DESC",
        limit=limit,
        mapper=_exit_signal_row_to_dict,
    )


def list_live_sim_cancel_intents(
    connection: sqlite3.Connection,
    *,
    code: str | None = None,
    status: object | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    if status is not None:
        clauses.append("status = ?")
        params.append(str(getattr(status, "value", status)).upper())
    return _list_rows(
        connection,
        "live_sim_cancel_intents",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, cancel_intent_id DESC",
        limit=limit,
        mapper=_cancel_intent_row_to_dict,
    )


def list_live_sim_lifecycle_events(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    code: str | None = None,
    position_id: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if position_id is not None:
        clauses.append("position_id = ?")
        params.append(require_non_empty_str(position_id, "position_id"))
    if code is not None:
        clauses.append("evidence_json LIKE ?")
        params.append(f'%"{validate_stock_code(code)}"%')
    return _list_rows(
        connection,
        "live_sim_lifecycle_events",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, lifecycle_event_id DESC",
        limit=limit,
        mapper=_lifecycle_event_row_to_dict,
    )


def get_latest_live_sim_reconcile(
    connection: sqlite3.Connection,
) -> dict[str, Any] | None:
    rows = list_live_sim_reconcile_snapshots(connection, limit=1)
    return rows[0] if rows else None


def get_live_sim_cancel_intent(
    connection: sqlite3.Connection,
    cancel_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_cancel_intents WHERE cancel_intent_id = ?",
        (require_non_empty_str(cancel_intent_id, "cancel_intent_id"),),
    ).fetchone()
    return None if row is None else _cancel_intent_row_to_dict(row)


def get_live_sim_exit_signal(
    connection: sqlite3.Connection,
    exit_signal_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_exit_signals WHERE exit_signal_id = ?",
        (require_non_empty_str(exit_signal_id, "exit_signal_id"),),
    ).fetchone()
    return None if row is None else _exit_signal_row_to_dict(row)


def get_live_sim_exit_intent(
    connection: sqlite3.Connection,
    exit_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_exit_intents WHERE exit_intent_id = ?",
        (require_non_empty_str(exit_intent_id, "exit_intent_id"),),
    ).fetchone()
    return None if row is None else _exit_intent_row_to_dict(row)


def evaluate_live_sim_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> LiveSimRunResult:
    resolved_settings = settings or load_settings()
    targets = _live_sim_candidate_targets(
        connection,
        trade_date=trade_date,
        limit=_bounded_limit(limit or 100),
    )
    run_id = new_message_id("live_sim_run")
    started_at = datetime_to_wire(utc_now())
    _insert_run(connection, run_id=run_id, trade_date=trade_date, started_at=started_at)
    evaluated_count = 0
    eligible_count = 0
    error_count = 0
    try:
        for target in targets:
            try:
                eligibility = evaluate_live_sim_eligibility(
                    connection,
                    target["candidate_instance_id"],
                    resolved_settings,
                )
                evaluated_count += 1
                if eligibility.eligible:
                    eligible_count += 1
            except Exception as exc:
                error_count += 1
                _record_error(
                    connection,
                    live_sim_intent_id=None,
                    live_sim_order_id=None,
                    code=target.get("code"),
                    error_message=str(exc),
                    payload=target,
                    run_id=run_id,
                )
        status = "COMPLETED" if error_count == 0 else "COMPLETED_WITH_ERRORS"
        _complete_run(
            connection,
            run_id=run_id,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            status=status,
            error_count=error_count,
        )
        connection.commit()
        return LiveSimRunResult(
            run_id=run_id,
            trade_date=trade_date,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            error_count=error_count,
            status=status,
        )
    except Exception as exc:
        _complete_run(
            connection,
            run_id=run_id,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            status="FAILED",
            error_count=error_count + 1,
            error_message=str(exc),
        )
        connection.commit()
        raise


def create_live_sim_intent_for_candidate(*args: Any, **kwargs: Any) -> LiveSimIntent:
    return create_live_sim_intent(*args, **kwargs)


def queue_live_sim_order_for_intent(*args: Any, **kwargs: Any) -> LiveSimOrderRecord:
    return queue_live_sim_order_command(*args, **kwargs)


def evaluate_live_sim_cancel_candidates(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    resolved_settings = settings or load_settings()
    rows = connection.execute(
        f"""
        SELECT *
        FROM live_sim_orders
        WHERE side = 'BUY'
            AND status IN ({_placeholders(ACTIVE_LIVE_SIM_ORDER_STATUSES)})
            AND remaining_quantity > 0
        ORDER BY created_at ASC, live_sim_order_id ASC
        LIMIT ?
        """,
        (*sorted(ACTIVE_LIVE_SIM_ORDER_STATUSES), _bounded_limit(limit or 500)),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        order = _order_row_to_dict(row)
        age_sec = _age_seconds_from_wire(order.get("created_at"))
        reasons: list[str] = []
        if age_sec < resolved_settings.live_sim_cancel_order_ttl_sec:
            reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_TTL_NOT_EXPIRED.value)
        if int(order.get("remaining_quantity") or 0) <= 0:
            reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_NO_REMAINING_QUANTITY.value)
        if str(order.get("side", "")).upper() != LiveSimSide.BUY.value:
            reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_NOT_BUY.value)
        if (
            resolved_settings.live_sim_cancel_require_broker_order_no
            and not resolved_settings.live_sim_cancel_allow_without_broker_order_no
            and not order.get("broker_order_no")
        ):
            reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_BROKER_ORDER_NO_REQUIRED.value)
        if _active_cancel_for_order(connection, order["live_sim_order_id"]):
            reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_DUPLICATE.value)
        if reasons == [LiveSimReasonCode.LIVE_SIM_CANCEL_TTL_NOT_EXPIRED.value]:
            continue
        item = {
            "order": order,
            "age_sec": age_sec,
            "eligible": not reasons,
            "reason_codes": _merge_reasons(reasons),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
        }
        candidates.append(item)
        if reasons and age_sec >= resolved_settings.live_sim_cancel_order_ttl_sec:
            _record_lifecycle_event(
                connection,
                event_type="CANCEL_CANDIDATE_REJECTED",
                entity_type="LIVE_SIM_ORDER",
                entity_id=order["live_sim_order_id"],
                live_sim_order_id=order["live_sim_order_id"],
                status=order["status"],
                reason=",".join(item["reason_codes"]),
                evidence=item,
            )
    connection.commit()
    return candidates


def create_live_sim_cancel_intent(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
    reason: str = "TTL_EXPIRED",
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    order = get_live_sim_order(connection, live_sim_order_id)
    if order is None:
        raise ValueError(f"LIVE_SIM order not found: {live_sim_order_id}")
    if _active_cancel_for_order(connection, live_sim_order_id):
        raise ValueError(LiveSimReasonCode.LIVE_SIM_CANCEL_DUPLICATE.value)
    cancel_quantity = int(order["remaining_quantity"])
    if cancel_quantity <= 0:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_CANCEL_NO_REMAINING_QUANTITY.value)
    idempotency_key = f"live_sim_cancel:{live_sim_order_id}:{reason.upper()}"
    cancel_intent_id = new_message_id("live_sim_cancel")
    evidence = {
        "order": order,
        "reason": reason.upper(),
        "cancel_quantity": cancel_quantity,
        "ttl_sec": resolved_settings.live_sim_cancel_order_ttl_sec,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }
    connection.execute(
        """
        INSERT INTO live_sim_cancel_intents (
            cancel_intent_id,
            live_sim_order_id,
            code,
            original_order_no,
            cancel_quantity,
            reason,
            status,
            evidence_json,
            idempotency_key,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, 'CREATED', ?, ?, 1, 0)
        """,
        (
            cancel_intent_id,
            live_sim_order_id,
            order["code"],
            order.get("broker_order_no"),
            cancel_quantity,
            reason.upper(),
            _json_dumps(evidence),
            idempotency_key,
        ),
    )
    connection.execute(
        """
        UPDATE live_sim_orders
        SET status = ?,
            last_event_at = ?
        WHERE live_sim_order_id = ?
        """,
        (
            LiveSimOrderStatus.CANCEL_REQUESTED.value,
            datetime_to_wire(utc_now()),
            live_sim_order_id,
        ),
    )
    _record_lifecycle_event(
        connection,
        event_type="CANCEL_INTENT_CREATED",
        entity_type="LIVE_SIM_CANCEL_INTENT",
        entity_id=cancel_intent_id,
        live_sim_order_id=live_sim_order_id,
        status="CREATED",
        reason=reason.upper(),
        evidence=evidence,
    )
    connection.commit()
    return get_live_sim_cancel_intent(connection, cancel_intent_id) or {}


def queue_live_sim_cancel_command(
    connection: sqlite3.Connection,
    cancel_intent_id: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    cancel_intent = get_live_sim_cancel_intent(connection, cancel_intent_id)
    if cancel_intent is None:
        raise ValueError(f"LIVE_SIM cancel intent not found: {cancel_intent_id}")
    order = get_live_sim_order(connection, cancel_intent["live_sim_order_id"])
    if order is None:
        raise ValueError(f"LIVE_SIM order not found: {cancel_intent['live_sim_order_id']}")
    reasons = _cancel_safety_reasons(connection, order, cancel_intent, resolved_settings)
    if reasons:
        _mark_cancel_intent_rejected(connection, cancel_intent, reasons)
        connection.commit()
        raise ValueError(",".join(reasons))

    payload = _build_gateway_cancel_order_payload(order, cancel_intent, resolved_settings)
    command = GatewayCommand(
        command_type="cancel_order",
        source="live_sim",
        payload=payload,
        idempotency_key=cancel_intent["idempotency_key"],
    )
    enqueue_result = enqueue_command(connection, command)
    if not enqueue_result.accepted:
        _mark_cancel_intent_rejected(
            connection,
            cancel_intent,
            [LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value],
            extra={"enqueue_error": enqueue_result.error_message},
        )
        connection.commit()
        raise ValueError(
            enqueue_result.error_message or LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value
        )
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        UPDATE live_sim_cancel_intents
        SET status = 'COMMAND_QUEUED',
            gateway_command_id = ?
        WHERE cancel_intent_id = ?
        """,
        (command.command_id, cancel_intent_id),
    )
    connection.execute(
        """
        UPDATE live_sim_orders
        SET status = ?,
            last_event_at = ?
        WHERE live_sim_order_id = ?
        """,
        (
            LiveSimOrderStatus.CANCEL_COMMAND_QUEUED.value,
            now,
            order["live_sim_order_id"],
        ),
    )
    _record_lifecycle_event(
        connection,
        event_type="CANCEL_COMMAND_QUEUED",
        entity_type="LIVE_SIM_CANCEL_INTENT",
        entity_id=cancel_intent_id,
        live_sim_order_id=order["live_sim_order_id"],
        status="COMMAND_QUEUED",
        reason=cancel_intent["reason"],
        evidence={"gateway_command_id": command.command_id, "payload": payload},
    )
    connection.commit()
    return get_live_sim_cancel_intent(connection, cancel_intent_id) or {}


def run_live_sim_cancel_unfilled_once(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    dry_run: bool = False,
    queue_commands: bool = False,
    limit: int | None = None,
) -> LiveSimLifecycleRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("live_sim_cancel_run")
    candidates = evaluate_live_sim_cancel_candidates(
        connection,
        resolved_settings,
        limit=limit or resolved_settings.live_sim_cancel_max_commands_per_run,
    )
    created: list[dict[str, Any]] = []
    queued: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for candidate in candidates:
        if not candidate["eligible"]:
            continue
        if len(created) >= resolved_settings.live_sim_cancel_max_commands_per_run:
            break
        order = candidate["order"]
        if dry_run:
            continue
        try:
            intent = create_live_sim_cancel_intent(
                connection,
                order["live_sim_order_id"],
                reason="TTL_EXPIRED",
                settings=resolved_settings,
            )
            created.append(intent)
            if queue_commands:
                queued.append(
                    queue_live_sim_cancel_command(
                        connection,
                        intent["cancel_intent_id"],
                        settings=resolved_settings,
                    )
                )
        except Exception as exc:
            errors.append({"order": order["live_sim_order_id"], "error": str(exc)})
            _record_error(
                connection,
                live_sim_intent_id=order.get("live_sim_intent_id"),
                live_sim_order_id=order["live_sim_order_id"],
                code=order.get("code"),
                error_message=str(exc),
                payload=candidate,
                run_id=run_id,
            )
            connection.commit()
    return LiveSimLifecycleRunResult(
        run_id=run_id,
        run_type="CANCEL_UNFILLED_ONCE",
        evaluated_count=len(candidates),
        intent_count=len(created),
        command_count=len(queued),
        skipped_count=len([item for item in candidates if not item["eligible"]]),
        error_count=len(errors),
        status="COMPLETED_WITH_ERRORS" if errors else "DRY_RUN" if dry_run else "COMPLETED",
        details={"candidates": candidates, "created": created, "queued": queued, "errors": errors},
    )


def evaluate_live_sim_reprice_candidates(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    resolved_settings = settings or load_settings()
    if not resolved_settings.live_sim_reprice_enabled:
        return []
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_orders
        WHERE side = 'BUY'
            AND status IN ('CANCELLED', 'CANCEL_ACKED')
            AND filled_quantity = 0
            AND remaining_quantity > 0
        ORDER BY last_event_at DESC, created_at DESC, live_sim_order_id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit or 500),),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        order = _order_row_to_dict(row)
        intent = get_live_sim_intent(connection, str(order["live_sim_intent_id"]))
        cancel_intent = _latest_cancel_intent_for_order(
            connection,
            str(order["live_sim_order_id"]),
        )
        reasons: list[str] = []
        if intent is None:
            reasons.append("LIVE_SIM_REPRICE_ORIGINAL_INTENT_MISSING")
        if not _is_ttl_cancel_ack(cancel_intent):
            reasons.append("LIVE_SIM_REPRICE_REQUIRES_TTL_CANCEL_ACK")
        root_order_id = _reprice_root_order_id(order, intent)
        attempt_count = _reprice_attempt_count_for_root_order(connection, root_order_id)
        if attempt_count >= resolved_settings.live_sim_reprice_max_attempts:
            reasons.append("LIVE_SIM_REPRICE_MAX_ATTEMPTS_EXCEEDED")
        tick = _latest_tick_row(connection, str(order["code"]))
        if tick is None:
            reasons.append(LiveSimReasonCode.LATEST_TICK_MISSING.value)
        item = {
            "order": order,
            "original_intent": intent,
            "cancel_intent": cancel_intent,
            "root_live_sim_order_id": root_order_id,
            "attempt_count": attempt_count,
            "next_attempt": attempt_count + 1,
            "max_attempts": resolved_settings.live_sim_reprice_max_attempts,
            "eligible": not reasons,
            "reason_codes": _merge_reasons(reasons),
            "latest_tick": _row_to_dict(tick) if tick is not None else None,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
        }
        candidates.append(item)
    return candidates


def create_live_sim_reprice_intent(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
    settings: Settings | None = None,
) -> LiveSimIntent:
    resolved_settings = settings or load_settings()
    order = get_live_sim_order(connection, live_sim_order_id)
    if order is None:
        raise ValueError(f"LIVE_SIM order not found: {live_sim_order_id}")
    original_intent = get_live_sim_intent(connection, str(order["live_sim_intent_id"]))
    if original_intent is None:
        raise ValueError("LIVE_SIM_REPRICE_ORIGINAL_INTENT_MISSING")
    cancel_intent = _latest_cancel_intent_for_order(connection, live_sim_order_id)
    if not _is_ttl_cancel_ack(cancel_intent):
        raise ValueError("LIVE_SIM_REPRICE_REQUIRES_TTL_CANCEL_ACK")
    if int(order.get("filled_quantity") or 0) != 0:
        raise ValueError("LIVE_SIM_REPRICE_REQUIRES_UNFILLED_BUY")
    if str(order.get("side", "")).upper() != LiveSimSide.BUY.value:
        raise ValueError("LIVE_SIM_REPRICE_REQUIRES_BUY")

    root_order_id = _reprice_root_order_id(order, original_intent)
    attempt = _reprice_attempt_count_for_root_order(connection, root_order_id) + 1
    if attempt > resolved_settings.live_sim_reprice_max_attempts:
        raise ValueError("LIVE_SIM_REPRICE_MAX_ATTEMPTS_EXCEEDED")
    reprice_evidence = {
        "reprice": {
            "enabled": True,
            "attempt": attempt,
            "max_attempts": resolved_settings.live_sim_reprice_max_attempts,
            "original_live_sim_order_id": live_sim_order_id,
            "original_live_sim_intent_id": order["live_sim_intent_id"],
            "root_live_sim_order_id": root_order_id,
            "cancel_intent_id": (
                None if cancel_intent is None else cancel_intent["cancel_intent_id"]
            ),
            "cancel_reason": None if cancel_intent is None else cancel_intent["reason"],
            "price_source": "LATEST_TICK",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
        }
    }
    intent = create_live_sim_intent(
        connection,
        str(original_intent["candidate_instance_id"]),
        settings=resolved_settings,
        source="live_sim_reprice",
        idempotency_suffix=f"reprice:{root_order_id}:{attempt}",
        extra_evidence=reprice_evidence,
    )
    if intent.status is LiveSimIntentStatus.CREATED:
        _record_lifecycle_event(
            connection,
            event_type="REPRICE_INTENT_CREATED",
            entity_type="LIVE_SIM_INTENT",
            entity_id=intent.live_sim_intent_id,
            live_sim_order_id=live_sim_order_id,
            status=intent.status.value,
            reason="TTL_CANCEL_REPRICE",
            evidence=intent.to_dict(),
        )
        connection.commit()
    return intent


def run_live_sim_reprice_once(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    dry_run: bool = False,
    queue_commands: bool = False,
    limit: int | None = None,
) -> LiveSimLifecycleRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("live_sim_reprice_run")
    if not resolved_settings.live_sim_reprice_enabled:
        return LiveSimLifecycleRunResult(
            run_id=run_id,
            run_type="REPRICE_BUY_ONCE",
            status="SKIPPED",
            details={"reason": "LIVE_SIM_REPRICE_ENABLED=false"},
        )
    candidates = evaluate_live_sim_reprice_candidates(
        connection,
        resolved_settings,
        limit=limit or resolved_settings.live_sim_operating_max_buy_commands_per_cycle,
    )
    created: list[dict[str, Any]] = []
    queued: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    command_limit = _bounded_limit(
        limit or resolved_settings.live_sim_operating_max_buy_commands_per_cycle
    )
    for candidate in candidates:
        if not candidate["eligible"]:
            continue
        if len(created) >= command_limit:
            break
        order = candidate["order"]
        if dry_run:
            continue
        try:
            intent = create_live_sim_reprice_intent(
                connection,
                order["live_sim_order_id"],
                settings=resolved_settings,
            )
            created.append(intent.to_dict())
            if intent.status is not LiveSimIntentStatus.CREATED:
                continue
            if queue_commands:
                queued.append(
                    queue_live_sim_order_command(
                        connection,
                        intent.live_sim_intent_id,
                        settings=resolved_settings,
                    ).to_dict()
                )
        except Exception as exc:
            errors.append({"order": order["live_sim_order_id"], "error": str(exc)})
            _record_error(
                connection,
                live_sim_intent_id=order.get("live_sim_intent_id"),
                live_sim_order_id=order["live_sim_order_id"],
                code=order.get("code"),
                error_message=str(exc),
                payload=candidate,
                run_id=run_id,
            )
            connection.commit()
    return LiveSimLifecycleRunResult(
        run_id=run_id,
        run_type="REPRICE_BUY_ONCE",
        evaluated_count=len(candidates),
        intent_count=len(created),
        command_count=len(queued),
        skipped_count=len([item for item in candidates if not item["eligible"]]),
        error_count=len(errors),
        status="COMPLETED_WITH_ERRORS" if errors else "DRY_RUN" if dry_run else "COMPLETED",
        details={"candidates": candidates, "created": created, "queued": queued, "errors": errors},
    )


def evaluate_live_sim_exit_signals(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    position_id: str | None = None,
    code: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    resolved_settings = settings or load_settings()
    clauses = ["status = 'OPEN'", "quantity > 0", "available_quantity > 0"]
    params: list[Any] = []
    if position_id is not None:
        clauses.append("position_id = ?")
        params.append(require_non_empty_str(position_id, "position_id"))
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    rows = connection.execute(
        f"""
        SELECT *
        FROM live_sim_positions
        WHERE {" AND ".join(clauses)}
        ORDER BY opened_at ASC, position_id ASC
        LIMIT ?
        """,
        (*params, _bounded_limit(limit or 500)),
    ).fetchall()
    signals: list[dict[str, Any]] = []
    for row in rows:
        position = _position_row_to_dict(row)
        tick = _latest_tick_row(connection, position["code"])
        if tick is None:
            _record_lifecycle_event(
                connection,
                event_type="EXIT_EVALUATION_DATA_WAIT",
                entity_type="LIVE_SIM_POSITION",
                entity_id=position["position_id"],
                position_id=position["position_id"],
                status=position["status"],
                reason=LiveSimReasonCode.LATEST_TICK_MISSING.value,
                evidence={"position": position},
            )
            continue
        last_price = float(tick["price"])
        tick_age = tick_age_seconds(tick["event_ts"])
        metrics = _mark_position_to_market(
            connection,
            position,
            last_price,
            tick["event_ts"],
            resolved_settings,
        )
        position = get_live_sim_position(connection, position["position_id"]) or position
        if tick_age > resolved_settings.live_sim_stale_tick_sec:
            _record_lifecycle_event(
                connection,
                event_type="EXIT_EVALUATION_DATA_WAIT",
                entity_type="LIVE_SIM_POSITION",
                entity_id=position["position_id"],
                position_id=position["position_id"],
                status=position["status"],
                reason=LiveSimReasonCode.LIVE_SIM_EXIT_LATEST_TICK_STALE.value,
                evidence={"position": position, "tick_age_sec": tick_age},
            )
            continue

        avg_price = float(position["avg_entry_price"])
        highest_price = float(position.get("highest_price") or last_price)
        hold_sec = _age_seconds_from_wire(position.get("opened_at"))
        candidates: list[dict[str, Any]] = []
        if avg_price > 0 and last_price <= avg_price * (
            1 - resolved_settings.live_sim_exit_stop_loss_pct / 100
        ):
            candidates.append(
                _exit_signal_candidate(
                    position,
                    "STOP_LOSS",
                    avg_price * (1 - resolved_settings.live_sim_exit_stop_loss_pct / 100),
                    last_price,
                    {"unrealized_pnl": metrics["unrealized_pnl"]},
                )
            )
        if avg_price > 0 and last_price >= avg_price * (
            1 + resolved_settings.live_sim_exit_take_profit_pct / 100
        ):
            candidates.append(
                _exit_signal_candidate(
                    position,
                    "TAKE_PROFIT",
                    avg_price * (1 + resolved_settings.live_sim_exit_take_profit_pct / 100),
                    last_price,
                    {"unrealized_pnl": metrics["unrealized_pnl"]},
                )
            )
        trailing_activated = highest_price >= avg_price * (
            1 + resolved_settings.live_sim_exit_trailing_activation_pct / 100
        )
        trailing_stop_price = highest_price * (
            1 - resolved_settings.live_sim_exit_trailing_stop_pct / 100
        )
        if trailing_activated and last_price <= trailing_stop_price:
            candidates.append(
                _exit_signal_candidate(
                    position,
                    "TRAILING_STOP",
                    trailing_stop_price,
                    last_price,
                    {"highest_price": highest_price, "trailing_stop_price": trailing_stop_price},
                )
            )
        if (
            hold_sec >= resolved_settings.live_sim_exit_max_hold_sec
            and hold_sec >= resolved_settings.live_sim_exit_min_hold_sec
        ):
            candidates.append(
                _exit_signal_candidate(
                    position,
                    "MAX_HOLD",
                    None,
                    last_price,
                    {"hold_sec": hold_sec},
                )
            )
        if resolved_settings.live_sim_exit_eod_flatten_enabled and _is_eod_flatten_time(
            resolved_settings
        ):
            candidates.append(
                _exit_signal_candidate(
                    position,
                    "EOD_FLATTEN",
                    None,
                    last_price,
                    {"eod_flatten_time": resolved_settings.live_sim_exit_eod_flatten_time},
                )
            )
        for signal in candidates:
            if _active_exit_for_position(connection, position["position_id"]):
                signal["eligible"] = False
                signal["reason_codes"] = [LiveSimReasonCode.LIVE_SIM_EXIT_DUPLICATE.value]
            else:
                signal["eligible"] = True
                signal["reason_codes"] = []
            signals.append(signal)
    connection.commit()
    return signals


def create_live_sim_exit_intent(
    connection: sqlite3.Connection,
    position_id: str,
    exit_signal_id: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    position = get_live_sim_position(connection, position_id)
    if position is None:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_POSITION_NOT_FOUND.value)
    if not resolved_settings.live_sim_exit_engine_enabled:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_DISABLED.value)
    if not resolved_settings.live_sim_exit_order_creation_enabled:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_ORDER_CREATION_DISABLED.value)
    if _active_exit_intent_for_position(connection, position_id):
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_DUPLICATE.value)
    if str(position["status"]).upper() not in {"OPEN", "RECONCILE_MISMATCH"}:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_POSITION_NOT_OPEN.value)
    quantity = int(position["available_quantity"])
    if quantity <= 0 or quantity > int(position["quantity"]):
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_QUANTITY_INVALID.value)
    signal = (
        get_live_sim_exit_signal(connection, exit_signal_id)
        if exit_signal_id is not None
        else _latest_exit_signal_for_position(connection, position_id)
    )
    if signal is None:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_SIGNAL_REQUIRED.value)
    idempotency_key = f"live_sim_exit:{position_id}:{signal['exit_signal_id']}:{signal['reason']}"
    exit_intent_id = new_message_id("live_sim_exit")
    evidence = {
        "position": position,
        "signal": signal,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "close_only": True,
        "allow_short": False,
    }
    connection.execute(
        """
        INSERT INTO live_sim_exit_intents (
            exit_intent_id,
            position_id,
            exit_signal_id,
            code,
            quantity,
            order_type,
            limit_price,
            reason,
            status,
            evidence_json,
            idempotency_key,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'CREATED', ?, ?, 1, 0)
        """,
        (
            exit_intent_id,
            position_id,
            signal["exit_signal_id"],
            position["code"],
            quantity,
            resolved_settings.live_sim_exit_default_order_type,
            signal.get("last_price") or position.get("last_price"),
            signal["reason"],
            _json_dumps(evidence),
            idempotency_key,
        ),
    )
    connection.execute(
        """
        UPDATE live_sim_exit_signals
        SET status = 'EXIT_INTENT_CREATED'
        WHERE exit_signal_id = ?
        """,
        (signal["exit_signal_id"],),
    )
    _record_lifecycle_event(
        connection,
        event_type="EXIT_INTENT_CREATED",
        entity_type="LIVE_SIM_EXIT_INTENT",
        entity_id=exit_intent_id,
        position_id=position_id,
        status="CREATED",
        reason=signal["reason"],
        evidence=evidence,
    )
    connection.commit()
    return get_live_sim_exit_intent(connection, exit_intent_id) or {}


def queue_live_sim_exit_order_command(
    connection: sqlite3.Connection,
    exit_intent_id: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    exit_intent = get_live_sim_exit_intent(connection, exit_intent_id)
    if exit_intent is None:
        raise ValueError(f"LIVE_SIM exit intent not found: {exit_intent_id}")
    position = get_live_sim_position(connection, exit_intent["position_id"])
    if position is None:
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_POSITION_NOT_FOUND.value)
    reasons = _exit_safety_reasons(connection, position, exit_intent, resolved_settings)
    if reasons:
        _mark_exit_intent_rejected(connection, exit_intent, reasons)
        connection.commit()
        raise ValueError(",".join(reasons))

    payload = _build_gateway_exit_order_payload(position, exit_intent, resolved_settings)
    command = GatewayCommand(
        command_type="send_order",
        source="live_sim",
        payload=payload,
        idempotency_key=exit_intent["idempotency_key"],
    )
    enqueue_result = enqueue_command(connection, command)
    if not enqueue_result.accepted:
        _mark_exit_intent_rejected(
            connection,
            exit_intent,
            [LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value],
            extra={"enqueue_error": enqueue_result.error_message},
        )
        connection.commit()
        raise ValueError(
            enqueue_result.error_message or LiveSimReasonCode.COMMAND_QUEUE_REJECTED.value
        )

    now = datetime_to_wire(utc_now())
    order = LiveSimOrderRecord(
        live_sim_order_id=new_message_id("live_sim_exit_order"),
        live_sim_intent_id=exit_intent_id,
        gateway_command_id=command.command_id,
        account_id=position["account_id"],
        code=position["code"],
        name=position["name"],
        side=LiveSimSide.SELL,
        order_type=exit_intent["order_type"],
        quantity=int(exit_intent["quantity"]),
        limit_price=exit_intent["limit_price"],
        notional=float(exit_intent["quantity"]) * float(exit_intent["limit_price"] or 0),
        status=LiveSimOrderStatus.EXIT_COMMAND_QUEUED,
        filled_quantity=0,
        remaining_quantity=int(exit_intent["quantity"]),
        idempotency_key=exit_intent["idempotency_key"],
        created_at=now,
        command_queued_at=now,
    )
    _insert_order(connection, order, trade_date=str(position["trade_date"]))
    connection.execute(
        """
        UPDATE live_sim_exit_intents
        SET status = 'COMMAND_QUEUED',
            gateway_command_id = ?,
            live_sim_order_id = ?
        WHERE exit_intent_id = ?
        """,
        (command.command_id, order.live_sim_order_id, exit_intent_id),
    )
    connection.execute(
        """
        UPDATE live_sim_exit_signals
        SET status = 'COMMAND_QUEUED'
        WHERE exit_signal_id = ?
        """,
        (exit_intent["exit_signal_id"],),
    )
    connection.execute(
        """
        UPDATE live_sim_positions
        SET status = 'CLOSING',
            updated_at = ?
        WHERE position_id = ?
        """,
        (now, position["position_id"]),
    )
    _record_lifecycle_event(
        connection,
        event_type="EXIT_COMMAND_QUEUED",
        entity_type="LIVE_SIM_EXIT_INTENT",
        entity_id=exit_intent_id,
        live_sim_order_id=order.live_sim_order_id,
        position_id=position["position_id"],
        status="COMMAND_QUEUED",
        reason=exit_intent["reason"],
        evidence={"gateway_command_id": command.command_id, "payload": payload},
    )
    connection.commit()
    return get_live_sim_exit_intent(connection, exit_intent_id) or {}


def run_live_sim_exit_once(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    *,
    dry_run: bool = False,
    queue_commands: bool = False,
    position_id: str | None = None,
    code: str | None = None,
    limit: int | None = None,
) -> LiveSimLifecycleRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("live_sim_exit_run")
    signals = evaluate_live_sim_exit_signals(
        connection,
        resolved_settings,
        position_id=position_id,
        code=code,
        limit=limit or resolved_settings.live_sim_exit_max_commands_per_run,
    )
    inserted_signals: list[dict[str, Any]] = []
    intents: list[dict[str, Any]] = []
    queued: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for signal in signals:
        if not signal["eligible"]:
            continue
        if len(intents) >= resolved_settings.live_sim_exit_max_commands_per_run:
            break
        if dry_run:
            continue
        try:
            inserted = _insert_exit_signal_from_candidate(connection, signal)
            inserted_signals.append(inserted)
            intent = create_live_sim_exit_intent(
                connection,
                signal["position_id"],
                inserted["exit_signal_id"],
                settings=resolved_settings,
            )
            intents.append(intent)
            if queue_commands:
                queued.append(
                    queue_live_sim_exit_order_command(
                        connection,
                        intent["exit_intent_id"],
                        settings=resolved_settings,
                    )
                )
        except Exception as exc:
            errors.append({"position": signal.get("position_id"), "error": str(exc)})
            _record_error(
                connection,
                live_sim_intent_id=None,
                live_sim_order_id=None,
                code=signal.get("code"),
                error_message=str(exc),
                payload=signal,
                run_id=run_id,
            )
            connection.commit()
    return LiveSimLifecycleRunResult(
        run_id=run_id,
        run_type="EXIT_ONCE",
        evaluated_count=len(signals),
        signal_count=len(inserted_signals),
        intent_count=len(intents),
        command_count=len(queued),
        skipped_count=len([item for item in signals if not item["eligible"]]),
        error_count=len(errors),
        status="COMPLETED_WITH_ERRORS" if errors else "DRY_RUN" if dry_run else "COMPLETED",
        details={"signals": signals, "created": intents, "queued": queued, "errors": errors},
    )


def apply_live_sim_broker_snapshot(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings | None = None,
) -> dict[str, Any]:
    payload = dict(event.payload)
    _record_lifecycle_event(
        connection,
        event_type="BROKER_SNAPSHOT_OBSERVED",
        entity_type="BROKER_SNAPSHOT",
        entity_id=event.event_id,
        status="LOCAL_ONLY_WITH_BROKER_SNAPSHOT",
        reason=str(event.event_type).upper(),
        evidence={"payload": payload, "event_id": event.event_id},
    )
    connection.commit()
    return {"handled": True, "snapshot_event_id": event.event_id}


def _handle_live_sim_command_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> dict[str, Any]:
    if event.command_id is None:
        return {"handled": False, "reason": "missing_command_id"}
    order = _order_by_command_id(connection, event.command_id)
    cancel_intent = _cancel_intent_by_command_id(connection, event.command_id)
    if order is None:
        if cancel_intent is not None:
            return _handle_cancel_command_event(connection, event, cancel_intent)
        return {"handled": False, "reason": "command_not_live_sim"}
    now = datetime_to_wire(utc_now())
    payload = dict(event.payload)
    event_type = event.event_type.strip().lower()
    if event_type == "command_started":
        next_status = (
            LiveSimOrderStatus.EXIT_COMMAND_QUEUED.value
            if order["side"] == LiveSimSide.SELL.value
            else LiveSimOrderStatus.COMMAND_DISPATCHED.value
        )
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                command_dispatched_at = COALESCE(command_dispatched_at, ?),
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                next_status,
                now,
                now,
                order["live_sim_order_id"],
            ),
        )
    elif event_type == "command_ack":
        details = _json_object(payload.get("details"))
        accepted = bool(details.get("accepted", True))
        broker_order_no = details.get("broker_order_no") or details.get("broker_order_id")
        status = (
            LiveSimOrderStatus.BROKER_ACKED.value
            if accepted
            else LiveSimOrderStatus.BROKER_REJECTED.value
        )
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                broker_order_no = COALESCE(?, broker_order_no),
                broker_result_code = COALESCE(?, broker_result_code),
                broker_message = COALESCE(?, broker_message),
                broker_acked_at = ?,
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                status,
                broker_order_no,
                details.get("broker_result_code"),
                payload.get("message"),
                now,
                now,
                order["live_sim_order_id"],
            ),
        )
        if accepted:
            connection.execute(
                """
                UPDATE live_sim_intents
                SET broker_order_sent = 1
                WHERE live_sim_intent_id = ?
                """,
                (order["live_sim_intent_id"],),
            )
        _record_lifecycle_event(
            connection,
            event_type="ORDER_BROKER_ACK" if accepted else "ORDER_BROKER_REJECT",
            entity_type="LIVE_SIM_ORDER",
            entity_id=order["live_sim_order_id"],
            live_sim_order_id=order["live_sim_order_id"],
            status=status,
            reason=str(details.get("broker_result_code") or ""),
            evidence={"payload": payload, "details": details},
        )
    elif event_type == "command_failed":
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                broker_message = COALESCE(?, broker_message),
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                LiveSimOrderStatus.FAILED.value,
                payload.get("error_message"),
                now,
                order["live_sim_order_id"],
            ),
        )
        if order["side"] == LiveSimSide.SELL.value:
            _mark_exit_intent_by_order_rejected(
                connection,
                order["live_sim_order_id"],
                payload.get("error_message") or "COMMAND_FAILED",
            )
        _record_lifecycle_event(
            connection,
            event_type="ORDER_COMMAND_FAILED",
            entity_type="LIVE_SIM_ORDER",
            entity_id=order["live_sim_order_id"],
            live_sim_order_id=order["live_sim_order_id"],
            status=LiveSimOrderStatus.FAILED.value,
            reason=str(payload.get("error_message") or "COMMAND_FAILED"),
            evidence={"payload": payload},
        )
    connection.commit()
    return {"handled": True, "live_sim_order_id": order["live_sim_order_id"]}


def _handle_live_sim_execution_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> dict[str, Any]:
    payload = dict(event.payload)
    metadata = _json_object(payload.get("metadata"))
    broker_env = str(
        metadata.get("broker_env") or payload.get("broker_env") or settings.live_sim_broker_env
    )
    account_mode = str(
        metadata.get("account_mode")
        or payload.get("account_mode")
        or settings.live_sim_account_mode
    )
    server_mode = str(
        metadata.get("server_mode") or payload.get("server_mode") or settings.live_sim_server_mode
    )
    if not (
        is_simulation_like(broker_env)
        and is_simulation_like(account_mode)
        and is_simulation_like(server_mode)
    ):
        _record_error(
            connection,
            live_sim_intent_id=metadata.get("live_sim_intent_id"),
            live_sim_order_id=metadata.get("live_sim_order_id"),
            code=payload.get("code"),
            error_message=LiveSimReasonCode.ACCOUNT_NOT_SIMULATION.value,
            payload=payload,
        )
        connection.commit()
        return {"handled": False, "reason": LiveSimReasonCode.ACCOUNT_NOT_SIMULATION.value}

    broker_order_no = payload.get("broker_order_id") or payload.get("broker_order_no")
    command_id = event.command_id or metadata.get("gateway_command_id")
    order = None
    if broker_order_no:
        order = _order_by_broker_order_no(connection, str(broker_order_no))
    if order is None and command_id:
        order = _order_by_command_id(connection, str(command_id))
    if order is None:
        _record_error(
            connection,
            live_sim_intent_id=metadata.get("live_sim_intent_id"),
            live_sim_order_id=None,
            code=payload.get("code"),
            error_message=LiveSimReasonCode.RECONCILE_REQUIRED.value,
            payload=payload,
        )
        connection.commit()
        return {"handled": False, "reason": LiveSimReasonCode.RECONCILE_REQUIRED.value}

    execution_key = _execution_key(order, payload, event)
    if _execution_already_applied(connection, execution_key):
        _record_lifecycle_event(
            connection,
            event_type="DUPLICATE_EXECUTION_EVENT_IGNORED",
            entity_type="LIVE_SIM_ORDER",
            entity_id=order["live_sim_order_id"],
            live_sim_order_id=order["live_sim_order_id"],
            status=order["status"],
            reason="DUPLICATE_EXECUTION_EVENT",
            evidence={"execution_key": execution_key, "payload": payload},
        )
        connection.commit()
        return {
            "handled": True,
            "duplicate": True,
            "live_sim_order_id": order["live_sim_order_id"],
        }

    quantity = int(payload["quantity"])
    price = float(payload["price"])
    remaining = payload.get("remaining_quantity")
    remaining_quantity = (
        int(remaining)
        if remaining is not None
        else max(
            int(order["remaining_quantity"]) - quantity,
            0,
        )
    )
    filled_quantity = int(order["filled_quantity"]) + quantity
    status = (
        LiveSimOrderStatus.FILLED.value
        if remaining_quantity == 0
        else LiveSimOrderStatus.PARTIALLY_FILLED.value
    )
    execution = LiveSimExecutionRecord(
        live_sim_execution_id=new_message_id("live_sim_execution"),
        live_sim_order_id=order["live_sim_order_id"],
        live_sim_intent_id=order["live_sim_intent_id"],
        broker_order_no=str(broker_order_no) if broker_order_no else order.get("broker_order_no"),
        account_id=str(payload.get("account_id") or order["account_id"]),
        code=str(payload["code"]),
        side=str(payload["side"]),
        quantity=quantity,
        price=price,
        notional=price * quantity,
        executed_at=payload["executed_at"],
        raw_event_json=payload,
    )
    _insert_execution(connection, execution)
    connection.execute(
        """
        UPDATE live_sim_executions
        SET broker_execution_id = ?,
            execution_key = ?
        WHERE live_sim_execution_id = ?
        """,
        (
            str(payload.get("execution_id") or payload.get("broker_execution_id") or ""),
            execution_key,
            execution.live_sim_execution_id,
        ),
    )
    avg_fill_price = _average_fill_price(connection, order["live_sim_order_id"])
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        UPDATE live_sim_orders
        SET status = ?,
            filled_quantity = ?,
            remaining_quantity = ?,
            avg_fill_price = ?,
            last_event_at = ?
        WHERE live_sim_order_id = ?
        """,
        (
            status,
            filled_quantity,
            remaining_quantity,
            avg_fill_price,
            now,
            order["live_sim_order_id"],
        ),
    )
    if str(order["side"]).upper() == LiveSimSide.BUY.value:
        position_result = _apply_buy_fill(
            connection,
            order,
            execution_id=execution.live_sim_execution_id,
            quantity=quantity,
            price=price,
            executed_at=str(payload["executed_at"]),
            settings=settings,
        )
    else:
        position_result = _apply_sell_fill(
            connection,
            order,
            execution_id=execution.live_sim_execution_id,
            quantity=quantity,
            price=price,
            executed_at=str(payload["executed_at"]),
            settings=settings,
            metadata=metadata,
        )
        _mark_exit_after_sell_fill(
            connection,
            order,
            position_result=position_result,
            order_status=status,
        )
    _record_lifecycle_event(
        connection,
        event_type="EXECUTION_EVENT_APPLIED",
        entity_type="LIVE_SIM_ORDER",
        entity_id=order["live_sim_order_id"],
        live_sim_order_id=order["live_sim_order_id"],
        position_id=position_result.get("position_id"),
        status=status,
        reason="FILL_EVENT_RECEIVED",
        evidence={
            "execution_id": execution.live_sim_execution_id,
            "execution_key": execution_key,
            "position_result": position_result,
            "payload": payload,
        },
    )
    connection.commit()
    return {
        "handled": True,
        "live_sim_order_id": order["live_sim_order_id"],
        "live_sim_execution_id": execution.live_sim_execution_id,
        "position": position_result,
    }


def _handle_cancel_command_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    cancel_intent: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(event.payload)
    event_type = event.event_type.strip().lower()
    now = datetime_to_wire(utc_now())
    order = get_live_sim_order(connection, str(cancel_intent["live_sim_order_id"]))
    if order is None:
        return {"handled": False, "reason": "cancel_original_order_missing"}
    if event_type == "command_started":
        _record_lifecycle_event(
            connection,
            event_type="CANCEL_COMMAND_STARTED",
            entity_type="LIVE_SIM_CANCEL_INTENT",
            entity_id=str(cancel_intent["cancel_intent_id"]),
            live_sim_order_id=str(order["live_sim_order_id"]),
            status="COMMAND_DISPATCHED",
            reason=str(cancel_intent["reason"]),
            evidence={"payload": payload},
        )
    elif event_type == "command_ack":
        details = _json_object(payload.get("details"))
        accepted = bool(details.get("accepted", True))
        intent_status = "ACKED" if accepted else "REJECTED"
        order_status = (
            LiveSimOrderStatus.CANCELLED.value
            if accepted
            else LiveSimOrderStatus.CANCEL_REJECTED.value
        )
        connection.execute(
            """
            UPDATE live_sim_cancel_intents
            SET status = ?
            WHERE cancel_intent_id = ?
            """,
            (intent_status, cancel_intent["cancel_intent_id"]),
        )
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                broker_result_code = COALESCE(?, broker_result_code),
                broker_message = COALESCE(?, broker_message),
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                order_status,
                details.get("broker_result_code"),
                payload.get("message"),
                now,
                order["live_sim_order_id"],
            ),
        )
        if accepted and int(order.get("filled_quantity") or 0) == 0:
            connection.execute(
                """
                UPDATE live_sim_intents
                SET status = ?
                WHERE live_sim_intent_id = ?
                """,
                (LiveSimIntentStatus.CANCELLED.value, order["live_sim_intent_id"]),
            )
        _record_lifecycle_event(
            connection,
            event_type="CANCEL_ACK" if accepted else "CANCEL_REJECTED",
            entity_type="LIVE_SIM_CANCEL_INTENT",
            entity_id=str(cancel_intent["cancel_intent_id"]),
            live_sim_order_id=str(order["live_sim_order_id"]),
            status=intent_status,
            reason=str(cancel_intent["reason"]),
            evidence={"payload": payload, "details": details},
        )
    elif event_type == "command_failed":
        connection.execute(
            """
            UPDATE live_sim_cancel_intents
            SET status = 'REJECTED'
            WHERE cancel_intent_id = ?
            """,
            (cancel_intent["cancel_intent_id"],),
        )
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                broker_message = COALESCE(?, broker_message),
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                LiveSimOrderStatus.CANCEL_REJECTED.value,
                payload.get("error_message"),
                now,
                order["live_sim_order_id"],
            ),
        )
        _record_lifecycle_event(
            connection,
            event_type="CANCEL_COMMAND_FAILED",
            entity_type="LIVE_SIM_CANCEL_INTENT",
            entity_id=str(cancel_intent["cancel_intent_id"]),
            live_sim_order_id=str(order["live_sim_order_id"]),
            status="REJECTED",
            reason=str(payload.get("error_message") or "COMMAND_FAILED"),
            evidence={"payload": payload},
        )
    connection.commit()
    return {
        "handled": True,
        "cancel_intent_id": cancel_intent["cancel_intent_id"],
        "live_sim_order_id": order["live_sim_order_id"],
    }


def _handle_live_sim_order_lifecycle_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> dict[str, Any]:
    payload = dict(event.payload)
    metadata = _json_object(payload.get("metadata"))
    event_type = event.event_type.strip().lower()
    order = None
    if event.command_id:
        order = _order_by_command_id(connection, event.command_id)
    broker_order_no = payload.get("broker_order_no") or payload.get("broker_order_id")
    if order is None and broker_order_no:
        order = _order_by_broker_order_no(connection, str(broker_order_no))
    cancel_intent = (
        _cancel_intent_by_command_id(connection, event.command_id)
        if event.command_id is not None
        else None
    )
    if event_type in {"cancel_ack", "cancel_rejected"} and cancel_intent is not None:
        synthetic = GatewayEvent(
            event_type="command_ack" if event_type == "cancel_ack" else "command_failed",
            source=event.source,
            command_id=event.command_id,
            idempotency_key=event.idempotency_key,
            payload=payload,
        )
        return _handle_cancel_command_event(connection, synthetic, cancel_intent)
    if order is None:
        _record_error(
            connection,
            live_sim_intent_id=metadata.get("live_sim_intent_id"),
            live_sim_order_id=metadata.get("live_sim_order_id"),
            code=payload.get("code"),
            error_message=LiveSimReasonCode.RECONCILE_REQUIRED.value,
            payload=payload,
        )
        connection.commit()
        return {"handled": False, "reason": LiveSimReasonCode.RECONCILE_REQUIRED.value}
    status = (
        LiveSimOrderStatus.BROKER_REJECTED.value
        if event_type in {"order_rejected", "cancel_rejected"}
        else LiveSimOrderStatus.CANCELLED.value
    )
    connection.execute(
        """
        UPDATE live_sim_orders
        SET status = ?,
            broker_result_code = COALESCE(?, broker_result_code),
            broker_message = COALESCE(?, broker_message),
            last_event_at = ?
        WHERE live_sim_order_id = ?
        """,
        (
            status,
            payload.get("broker_result_code"),
            payload.get("broker_message") or payload.get("message"),
            datetime_to_wire(utc_now()),
            order["live_sim_order_id"],
        ),
    )
    _record_lifecycle_event(
        connection,
        event_type=event_type.upper(),
        entity_type="LIVE_SIM_ORDER",
        entity_id=order["live_sim_order_id"],
        live_sim_order_id=order["live_sim_order_id"],
        status=status,
        reason=str(payload.get("broker_message") or payload.get("message") or event_type),
        evidence={"payload": payload},
    )
    connection.commit()
    return {"handled": True, "live_sim_order_id": order["live_sim_order_id"]}


def _handle_live_sim_chejan_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> dict[str, Any]:
    payload = dict(event.payload)
    if int(payload.get("quantity") or payload.get("execution_quantity") or 0) > 0 and (
        payload.get("price") or payload.get("execution_price")
    ):
        translated = GatewayEvent(
            event_type="execution_event",
            source=event.source,
            command_id=event.command_id,
            idempotency_key=event.idempotency_key,
            payload={
                **payload,
                "execution_id": payload.get("execution_id")
                or payload.get("broker_execution_id")
                or payload.get("chejan_execution_id")
                or event.event_id,
                "quantity": int(payload.get("quantity") or payload.get("execution_quantity")),
                "price": float(payload.get("price") or payload.get("execution_price")),
                "executed_at": payload.get("executed_at")
                or payload.get("event_ts")
                or datetime_to_wire(event.ts),
            },
        )
        return _handle_live_sim_execution_event(connection, translated, settings)
    _record_lifecycle_event(
        connection,
        event_type="KIWOOM_ORDER_CHEJAN_OBSERVED",
        entity_type="CHEJAN",
        entity_id=event.event_id,
        status=str(payload.get("order_status") or "OBSERVED").upper(),
        reason="CHEJAN_NON_FILL",
        evidence={"payload": payload},
    )
    connection.commit()
    return {"handled": True, "chejan_event_id": event.event_id}


def _looks_live_sim_event(event: GatewayEvent) -> bool:
    payload = dict(event.payload)
    metadata = _json_object(payload.get("metadata"))
    return (
        payload.get("live_sim_only") is True
        or metadata.get("live_sim_only") is True
        or str(payload.get("mode", "")).upper() == "LIVE_SIM"
        or str(payload.get("live_mode", "")).upper() == "LIVE_SIM"
    )


def _execution_key(
    order: Mapping[str, Any],
    payload: Mapping[str, Any],
    event: GatewayEvent,
) -> str:
    explicit = (
        payload.get("execution_id")
        or payload.get("broker_execution_id")
        or payload.get("chejan_execution_id")
    )
    if explicit:
        return f"broker-exec:{order['live_sim_order_id']}:{explicit}"
    return ":".join(
        [
            "broker-exec",
            str(order["live_sim_order_id"]),
            str(payload.get("broker_order_no") or payload.get("broker_order_id") or ""),
            str(payload.get("side") or ""),
            str(payload.get("quantity") or ""),
            str(payload.get("price") or ""),
            str(payload.get("executed_at") or datetime_to_wire(event.ts)),
        ]
    )


def _execution_already_applied(connection: sqlite3.Connection, execution_key: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM live_sim_executions WHERE execution_key = ?",
        (require_non_empty_str(execution_key, "execution_key"),),
    ).fetchone()
    return row is not None


def _apply_buy_fill(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    *,
    execution_id: str,
    quantity: int,
    price: float,
    executed_at: str,
    settings: Settings,
) -> dict[str, Any]:
    position = _position_by_account_code(connection, str(order["account_id"]), str(order["code"]))
    fill_notional = quantity * price
    if position is None:
        position_id = new_message_id("live_sim_position")
        fee = fill_notional * settings.live_sim_fee_rate
        entry_cost = fill_notional + fee
        connection.execute(
            """
            INSERT INTO live_sim_positions (
                position_id,
                account_id,
                trade_date,
                code,
                name,
                side,
                quantity,
                available_quantity,
                avg_entry_price,
                total_entry_notional,
                highest_price,
                lowest_price,
                opened_at,
                last_price,
                last_price_at,
                status,
                source_live_sim_order_id,
                source_live_sim_intent_id,
                live_sim_only,
                live_real_allowed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'LONG', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, 1, 0, ?, ?)
            """,
            (
                position_id,
                order["account_id"],
                order["trade_date"],
                order["code"],
                order["name"],
                quantity,
                quantity,
                entry_cost / quantity,
                entry_cost,
                price,
                price,
                executed_at,
                price,
                executed_at,
                order["live_sim_order_id"],
                order["live_sim_intent_id"],
                executed_at,
                executed_at,
            ),
        )
        status = "POSITION_OPENED"
    else:
        position_id = str(position["position_id"])
        old_quantity = int(position["quantity"])
        new_quantity = old_quantity + quantity
        old_notional = float(position["total_entry_notional"] or 0)
        fee = fill_notional * settings.live_sim_fee_rate
        entry_cost = fill_notional + fee
        new_notional = old_notional + entry_cost
        avg_entry_price = new_notional / new_quantity if new_quantity > 0 else 0.0
        highest_price = max(float(position["highest_price"] or price), price)
        lowest_price = min(float(position["lowest_price"] or price), price)
        connection.execute(
            """
            UPDATE live_sim_positions
            SET quantity = ?,
                available_quantity = ?,
                avg_entry_price = ?,
                total_entry_notional = ?,
                highest_price = ?,
                lowest_price = ?,
                last_price = ?,
                last_price_at = ?,
                status = 'OPEN',
                updated_at = ?
            WHERE position_id = ?
            """,
            (
                new_quantity,
                int(position["available_quantity"]) + quantity,
                avg_entry_price,
                new_notional,
                highest_price,
                lowest_price,
                price,
                executed_at,
                executed_at,
                position_id,
            ),
        )
        status = "POSITION_INCREASED"
    _insert_position_event(
        connection,
        position_id=position_id,
        event_type=status,
        live_sim_order_id=str(order["live_sim_order_id"]),
        live_sim_execution_id=execution_id,
        code=str(order["code"]),
        quantity_delta=quantity,
        price=price,
        realized_pnl=0.0,
        evidence={
            "order": order,
            "gross_entry_notional": fill_notional,
            "buy_fee": fill_notional * settings.live_sim_fee_rate,
            "net_entry_cost": fill_notional * (1 + settings.live_sim_fee_rate),
            "fee_rate": settings.live_sim_fee_rate,
        },
    )
    return {"position_id": position_id, "event_type": status, "quantity_delta": quantity}


def _apply_sell_fill(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    *,
    execution_id: str,
    quantity: int,
    price: float,
    executed_at: str,
    settings: Settings,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    position = None
    if metadata.get("position_id"):
        position = connection.execute(
            "SELECT * FROM live_sim_positions WHERE position_id = ?",
            (str(metadata["position_id"]),),
        ).fetchone()
    if position is None:
        position = _position_by_account_code(
            connection,
            str(order["account_id"]),
            str(order["code"]),
            include_closing=True,
        )
    if position is None:
        _record_error(
            connection,
            live_sim_intent_id=order.get("live_sim_intent_id"),
            live_sim_order_id=order.get("live_sim_order_id"),
            code=order.get("code"),
            error_message="SELL_FILL_POSITION_MISSING",
            payload={"order": order, "metadata": metadata},
        )
        return {"event_type": "RECONCILE_MISMATCH", "position_id": None}
    available = int(position["available_quantity"])
    if quantity > available:
        _record_error(
            connection,
            live_sim_intent_id=order.get("live_sim_intent_id"),
            live_sim_order_id=order.get("live_sim_order_id"),
            code=order.get("code"),
            error_message="SELL_FILL_EXCEEDS_AVAILABLE_QUANTITY",
            payload={"order": order, "position": _position_row_to_dict(position)},
        )
        connection.execute(
            """
            UPDATE live_sim_positions
            SET status = 'RECONCILE_MISMATCH',
                updated_at = ?
            WHERE position_id = ?
            """,
            (executed_at, position["position_id"]),
        )
        return {
            "event_type": "RECONCILE_MISMATCH",
            "position_id": position["position_id"],
        }
    avg_price = float(position["avg_entry_price"])
    gross_realized = (price - avg_price) * quantity
    fee = price * quantity * settings.live_sim_fee_rate
    tax = price * quantity * settings.live_sim_tax_rate
    realized_pnl = gross_realized - fee - tax
    new_quantity = int(position["quantity"]) - quantity
    new_available = available - quantity
    new_entry_notional = avg_price * new_quantity
    highest_price = max(float(position["highest_price"] or price), price)
    lowest_price = min(float(position["lowest_price"] or price), price)
    unrealized_pnl = _net_unrealized_pnl(
        quantity=new_quantity,
        avg_entry_price=avg_price,
        last_price=price,
        settings=settings,
    )
    cumulative_realized = float(position["realized_pnl"] or 0) + realized_pnl
    status = "CLOSED" if new_quantity == 0 else "CLOSING"
    connection.execute(
        """
        UPDATE live_sim_positions
        SET quantity = ?,
            available_quantity = ?,
            total_entry_notional = ?,
            realized_pnl = ?,
            unrealized_pnl = ?,
            highest_price = ?,
            lowest_price = ?,
            last_price = ?,
            last_price_at = ?,
            status = ?,
            closed_at = COALESCE(?, closed_at),
            updated_at = ?
        WHERE position_id = ?
        """,
        (
            new_quantity,
            new_available,
            new_entry_notional,
            cumulative_realized,
            unrealized_pnl,
            highest_price,
            lowest_price,
            price,
            executed_at,
            status,
            executed_at if new_quantity == 0 else None,
            executed_at,
            position["position_id"],
        ),
    )
    event_type = "POSITION_CLOSED" if new_quantity == 0 else "POSITION_REDUCED"
    excursion = _position_excursion_metrics(
        entry_price=avg_price,
        highest_price=highest_price,
        lowest_price=lowest_price,
    )
    close_evidence = excursion if new_quantity == 0 else {}
    _insert_position_event(
        connection,
        position_id=str(position["position_id"]),
        event_type=event_type,
        live_sim_order_id=str(order["live_sim_order_id"]),
        live_sim_execution_id=execution_id,
        code=str(order["code"]),
        quantity_delta=-quantity,
        price=price,
        realized_pnl=realized_pnl,
        evidence={
            "avg_entry_price": avg_price,
            "gross_realized_pnl": gross_realized,
            "fee": fee,
            "tax": tax,
            "remaining_quantity": new_quantity,
            **close_evidence,
        },
    )
    return {
        "event_type": event_type,
        "position_id": position["position_id"],
        "realized_pnl": realized_pnl,
        "remaining_quantity": new_quantity,
        "status": status,
        **close_evidence,
    }


def _mark_exit_after_sell_fill(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    *,
    position_result: Mapping[str, Any],
    order_status: str,
) -> None:
    exit_intent = _exit_intent_by_order_id(connection, str(order["live_sim_order_id"]))
    if exit_intent is None:
        return
    closed = int(position_result.get("remaining_quantity") or 0) == 0
    intent_status = "CLOSED" if closed else "COMMAND_QUEUED"
    signal_status = "CLOSED" if closed else "COMMAND_QUEUED"
    order_next_status = LiveSimOrderStatus.EXIT_FILLED.value if closed else order_status
    connection.execute(
        "UPDATE live_sim_exit_intents SET status = ? WHERE exit_intent_id = ?",
        (intent_status, exit_intent["exit_intent_id"]),
    )
    connection.execute(
        "UPDATE live_sim_exit_signals SET status = ? WHERE exit_signal_id = ?",
        (signal_status, exit_intent["exit_signal_id"]),
    )
    connection.execute(
        "UPDATE live_sim_orders SET status = ? WHERE live_sim_order_id = ?",
        (order_next_status, order["live_sim_order_id"]),
    )


def _position_by_account_code(
    connection: sqlite3.Connection,
    account_id: str,
    code: str,
    *,
    include_closing: bool = False,
) -> sqlite3.Row | None:
    statuses = {"OPEN", "RECONCILE_MISMATCH"}
    if include_closing:
        statuses.add("CLOSING")
    return connection.execute(
        f"""
        SELECT *
        FROM live_sim_positions
        WHERE account_id = ?
            AND code = ?
            AND status IN ({_placeholders(statuses)})
        ORDER BY opened_at ASC, position_id ASC
        LIMIT 1
        """,
        (account_id, validate_stock_code(code), *sorted(statuses)),
    ).fetchone()


def _insert_position_event(
    connection: sqlite3.Connection,
    *,
    position_id: str | None,
    event_type: str,
    live_sim_order_id: str | None,
    live_sim_execution_id: str | None,
    code: str | None,
    quantity_delta: int,
    price: float | None,
    realized_pnl: float,
    evidence: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_position_events (
            event_id,
            position_id,
            event_type,
            live_sim_order_id,
            live_sim_execution_id,
            code,
            quantity_delta,
            price,
            realized_pnl,
            evidence_json,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            new_message_id("live_sim_position_event"),
            position_id,
            event_type,
            live_sim_order_id,
            live_sim_execution_id,
            validate_stock_code(code) if code else None,
            quantity_delta,
            price,
            realized_pnl,
            _json_dumps(evidence),
        ),
    )


def _cancel_safety_reasons(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    cancel_intent: Mapping[str, Any],
    settings: Settings,
) -> list[str]:
    reasons: list[str] = []
    safety_gate = check_live_sim_safety_gate(connection, settings, purpose="LIFECYCLE")
    if not safety_gate.passed:
        reasons.extend(safety_gate.reason_codes)
    if not settings.live_sim_cancel_enabled:
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_DISABLED.value)
    if not settings.live_sim_cancel_unfilled_enabled:
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_UNFILLED_DISABLED.value)
    if settings.live_sim_cancel_kill_switch or settings.live_sim_kill_switch:
        reasons.append(LiveSimReasonCode.LIVE_SIM_KILL_SWITCH_ACTIVE.value)
    if str(order["side"]).upper() != LiveSimSide.BUY.value:
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_NOT_BUY.value)
    if int(order["remaining_quantity"]) <= 0:
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_NO_REMAINING_QUANTITY.value)
    if _age_seconds_from_wire(order["created_at"]) < settings.live_sim_cancel_order_ttl_sec:
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_TTL_NOT_EXPIRED.value)
    if (
        settings.live_sim_cancel_require_broker_order_no
        and not settings.live_sim_cancel_allow_without_broker_order_no
        and not order.get("broker_order_no")
    ):
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_BROKER_ORDER_NO_REQUIRED.value)
    if cancel_intent.get("gateway_command_id"):
        reasons.append(LiveSimReasonCode.LIVE_SIM_CANCEL_DUPLICATE.value)
    return _merge_reasons(reasons)


def _exit_safety_reasons(
    connection: sqlite3.Connection,
    position: Mapping[str, Any],
    exit_intent: Mapping[str, Any],
    settings: Settings,
) -> list[str]:
    reasons: list[str] = []
    safety_gate = check_live_sim_safety_gate(connection, settings, purpose="LIFECYCLE")
    if not safety_gate.passed:
        reasons.extend(safety_gate.reason_codes)
    if not settings.live_sim_exit_engine_enabled:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_DISABLED.value)
    if not settings.live_sim_exit_order_creation_enabled:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_ORDER_CREATION_DISABLED.value)
    if not settings.live_sim_exit_gateway_command_enabled:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_GATEWAY_COMMAND_DISABLED.value)
    if settings.live_sim_kill_switch:
        reasons.append(LiveSimReasonCode.LIVE_SIM_KILL_SWITCH_ACTIVE.value)
    if not settings.live_sim_exit_allow_sell_close_only:
        reasons.append(LiveSimReasonCode.SELL_NOT_ALLOWED.value)
    if settings.live_sim_exit_allow_short:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_SHORT_NOT_ALLOWED.value)
    if str(exit_intent["order_type"]).upper() == LiveSimOrderType.MARKET.value:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_MARKET_ORDER_NOT_ALLOWED.value)
    if str(position["status"]).upper() not in {"OPEN", "CLOSING", "RECONCILE_MISMATCH"}:
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_POSITION_NOT_OPEN.value)
    if int(exit_intent["quantity"]) <= 0 or int(exit_intent["quantity"]) > int(
        position["available_quantity"]
    ):
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_QUANTITY_INVALID.value)
    if exit_intent.get("gateway_command_id"):
        reasons.append(LiveSimReasonCode.LIVE_SIM_EXIT_DUPLICATE.value)
    return _merge_reasons(reasons)


def _build_gateway_cancel_order_payload(
    order: Mapping[str, Any],
    cancel_intent: Mapping[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    idempotency_key = str(cancel_intent["idempotency_key"])
    return {
        "account_id": order["account_id"],
        "account_mode": settings.live_sim_account_mode,
        "broker_env": settings.live_sim_broker_env,
        "server_mode": settings.live_sim_server_mode,
        "code": order["code"],
        "name": order["name"],
        "side": "BUY_CANCEL",
        "quantity": int(cancel_intent["cancel_quantity"]),
        "cancel_quantity": int(cancel_intent["cancel_quantity"]),
        "original_order_no": cancel_intent.get("original_order_no") or order.get("broker_order_no"),
        "original_order_id": order["live_sim_order_id"],
        "live_sim_order_id": order["live_sim_order_id"],
        "reason": cancel_intent["reason"],
        "mode": "LIVE_SIM",
        "live_mode": "LIVE_SIM",
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "idempotency_key": idempotency_key,
        "metadata": {
            "source": "live_sim",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "cancel_intent_id": cancel_intent["cancel_intent_id"],
            "original_live_sim_order_id": order["live_sim_order_id"],
            "live_sim_intent_id": order["live_sim_intent_id"],
            "idempotency_key": idempotency_key,
        },
    }


def _build_gateway_exit_order_payload(
    position: Mapping[str, Any],
    exit_intent: Mapping[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    idempotency_key = str(exit_intent["idempotency_key"])
    limit_price = float(exit_intent["limit_price"] or position.get("last_price") or 0)
    limit_price += _tick_offset_value(settings.live_sim_exit_price_offset_ticks)
    return {
        "account_id": position["account_id"],
        "account_mode": settings.live_sim_account_mode,
        "broker_env": settings.live_sim_broker_env,
        "server_mode": settings.live_sim_server_mode,
        "code": position["code"],
        "name": position["name"],
        "side": "SELL",
        "quantity": int(exit_intent["quantity"]),
        "price": limit_price,
        "limit_price": limit_price,
        "order_type": exit_intent["order_type"],
        "hoga": settings.live_sim_default_hoga,
        "mode": "LIVE_SIM",
        "live_mode": "LIVE_SIM",
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "close_only": True,
        "allow_short": False,
        "position_id": position["position_id"],
        "exit_signal_id": exit_intent["exit_signal_id"],
        "exit_intent_id": exit_intent["exit_intent_id"],
        "live_sim_intent_id": exit_intent["exit_intent_id"],
        "idempotency_key": idempotency_key,
        "metadata": {
            "source": "live_sim",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "close_only": True,
            "allow_short": False,
            "position_id": position["position_id"],
            "exit_signal_id": exit_intent["exit_signal_id"],
            "exit_intent_id": exit_intent["exit_intent_id"],
            "live_sim_intent_id": exit_intent["exit_intent_id"],
            "idempotency_key": idempotency_key,
        },
    }


def _mark_cancel_intent_rejected(
    connection: sqlite3.Connection,
    cancel_intent: Mapping[str, Any],
    reasons: list[str],
    *,
    extra: Mapping[str, Any] | None = None,
) -> None:
    evidence = {"cancel_intent": cancel_intent, "reason_codes": reasons, **dict(extra or {})}
    connection.execute(
        """
        UPDATE live_sim_cancel_intents
        SET status = 'REJECTED',
            evidence_json = ?
        WHERE cancel_intent_id = ?
        """,
        (_json_dumps(evidence), cancel_intent["cancel_intent_id"]),
    )
    _record_lifecycle_event(
        connection,
        event_type="CANCEL_INTENT_REJECTED",
        entity_type="LIVE_SIM_CANCEL_INTENT",
        entity_id=str(cancel_intent["cancel_intent_id"]),
        live_sim_order_id=str(cancel_intent["live_sim_order_id"]),
        status="REJECTED",
        reason=",".join(reasons),
        evidence=evidence,
    )


def _mark_exit_intent_rejected(
    connection: sqlite3.Connection,
    exit_intent: Mapping[str, Any],
    reasons: list[str],
    *,
    extra: Mapping[str, Any] | None = None,
) -> None:
    evidence = {"exit_intent": exit_intent, "reason_codes": reasons, **dict(extra or {})}
    connection.execute(
        """
        UPDATE live_sim_exit_intents
        SET status = 'REJECTED',
            evidence_json = ?
        WHERE exit_intent_id = ?
        """,
        (_json_dumps(evidence), exit_intent["exit_intent_id"]),
    )
    _record_lifecycle_event(
        connection,
        event_type="EXIT_INTENT_REJECTED",
        entity_type="LIVE_SIM_EXIT_INTENT",
        entity_id=str(exit_intent["exit_intent_id"]),
        position_id=str(exit_intent["position_id"]),
        status="REJECTED",
        reason=",".join(reasons),
        evidence=evidence,
    )


def _mark_exit_intent_by_order_rejected(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
    reason: str,
) -> None:
    exit_intent = _exit_intent_by_order_id(connection, live_sim_order_id)
    if exit_intent is None:
        return
    _mark_exit_intent_rejected(connection, exit_intent, [reason])


def _exit_signal_candidate(
    position: Mapping[str, Any],
    reason: str,
    trigger_price: float | None,
    last_price: float,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "position_id": position["position_id"],
        "code": position["code"],
        "reason": reason,
        "trigger_price": trigger_price,
        "last_price": last_price,
        "quantity": int(position["available_quantity"]),
        "status": "SIGNALLED",
        "evidence_json": {
            "position": position,
            **dict(evidence),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
        },
    }


def _insert_exit_signal_from_candidate(
    connection: sqlite3.Connection,
    signal: Mapping[str, Any],
) -> dict[str, Any]:
    if _active_exit_for_position(connection, str(signal["position_id"])):
        raise ValueError(LiveSimReasonCode.LIVE_SIM_EXIT_DUPLICATE.value)
    exit_signal_id = new_message_id("live_sim_exit_signal")
    connection.execute(
        """
        INSERT INTO live_sim_exit_signals (
            exit_signal_id,
            position_id,
            code,
            reason,
            trigger_price,
            last_price,
            quantity,
            status,
            evidence_json,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            exit_signal_id,
            signal["position_id"],
            signal["code"],
            signal["reason"],
            signal.get("trigger_price"),
            signal.get("last_price"),
            signal["quantity"],
            signal["status"],
            _json_dumps(signal["evidence_json"]),
        ),
    )
    _record_lifecycle_event(
        connection,
        event_type="EXIT_SIGNAL_CREATED",
        entity_type="LIVE_SIM_EXIT_SIGNAL",
        entity_id=exit_signal_id,
        position_id=str(signal["position_id"]),
        status=str(signal["status"]),
        reason=str(signal["reason"]),
        evidence=dict(signal),
    )
    connection.commit()
    return get_live_sim_exit_signal(connection, exit_signal_id) or {}


def _mark_position_to_market(
    connection: sqlite3.Connection,
    position: Mapping[str, Any],
    last_price: float,
    last_price_at: object,
    settings: Settings,
) -> dict[str, Any]:
    quantity = int(position["quantity"])
    avg_entry_price = float(position["avg_entry_price"])
    unrealized_pnl = _net_unrealized_pnl(
        quantity=quantity,
        avg_entry_price=avg_entry_price,
        last_price=last_price,
        settings=settings,
    )
    highest_price = max(float(position.get("highest_price") or last_price), last_price)
    lowest_price = min(float(position.get("lowest_price") or last_price), last_price)
    activated = highest_price >= avg_entry_price * (
        1 + settings.live_sim_exit_trailing_activation_pct / 100
    )
    trailing_stop_price = (
        highest_price * (1 - settings.live_sim_exit_trailing_stop_pct / 100)
        if activated
        else position.get("trailing_stop_price")
    )
    connection.execute(
        """
        UPDATE live_sim_positions
        SET last_price = ?,
            last_price_at = ?,
            unrealized_pnl = ?,
            highest_price = ?,
            lowest_price = ?,
            trailing_stop_price = ?,
            updated_at = ?
        WHERE position_id = ?
        """,
        (
            last_price,
            str(last_price_at),
            unrealized_pnl,
            highest_price,
            lowest_price,
            trailing_stop_price,
            datetime_to_wire(utc_now()),
            position["position_id"],
        ),
    )
    return {
        "unrealized_pnl": unrealized_pnl,
        "highest_price": highest_price,
        "lowest_price": lowest_price,
        "trailing_stop_price": trailing_stop_price,
    }


def _net_unrealized_pnl(
    *,
    quantity: int,
    avg_entry_price: float,
    last_price: float,
    settings: Settings,
) -> float:
    if quantity <= 0:
        return 0.0
    gross = (last_price - avg_entry_price) * quantity
    exit_notional = last_price * quantity
    estimated_sell_fee = exit_notional * settings.live_sim_fee_rate
    estimated_sell_tax = exit_notional * settings.live_sim_tax_rate
    return gross - estimated_sell_fee - estimated_sell_tax


def _position_excursion_metrics(
    *,
    entry_price: float,
    highest_price: float,
    lowest_price: float,
) -> dict[str, float]:
    if entry_price <= 0:
        return {
            "entry_price": entry_price,
            "highest_price": highest_price,
            "lowest_price": lowest_price,
            "mfe": 0.0,
            "mae": 0.0,
            "mfe_pct": 0.0,
            "mae_pct": 0.0,
        }
    mfe = (highest_price - entry_price) / entry_price
    mae = (lowest_price - entry_price) / entry_price
    return {
        "entry_price": entry_price,
        "highest_price": highest_price,
        "lowest_price": lowest_price,
        "mfe": mfe,
        "mae": mae,
        "mfe_pct": mfe * 100,
        "mae_pct": mae * 100,
    }


def _is_eod_flatten_time(settings: Settings) -> bool:
    return market_time_str() >= settings.live_sim_exit_eod_flatten_time


def _build_gateway_send_order_payload(
    intent: Mapping[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    idempotency_key = str(intent["idempotency_key"])
    return {
        "account_id": intent["account_id"],
        "account_mode": settings.live_sim_account_mode,
        "broker_env": settings.live_sim_broker_env,
        "server_mode": settings.live_sim_server_mode,
        "code": intent["code"],
        "name": intent["name"],
        "side": intent["side"],
        "quantity": int(intent["quantity"]),
        "price": intent["limit_price"],
        "limit_price": intent["limit_price"],
        "order_type": intent["order_type"],
        "hoga": settings.live_sim_default_hoga,
        "mode": "LIVE_SIM",
        "live_mode": "LIVE_SIM",
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "live_sim_intent_id": intent["live_sim_intent_id"],
        "idempotency_key": idempotency_key,
        "metadata": {
            "source": "live_sim",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "candidate_instance_id": intent["candidate_instance_id"],
            "strategy_observation_id": intent["strategy_observation_id"],
            "risk_observation_id": intent["risk_observation_id"],
            "dry_run_intent_id": intent.get("dry_run_intent_id"),
            "dry_run_order_id": intent.get("dry_run_order_id"),
            "live_sim_intent_id": intent["live_sim_intent_id"],
            "idempotency_key": idempotency_key,
        },
    }


def _insert_intent(connection: sqlite3.Connection, intent: LiveSimIntent) -> None:
    data = intent.to_dict()
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            dry_run_intent_id,
            dry_run_order_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            reason_codes_json,
            evidence_json,
            idempotency_key,
            gateway_command_id,
            live_sim_only,
            live_real_allowed,
            broker_order_sent,
            created_at,
            expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)
        """,
        (
            data["live_sim_intent_id"],
            data["candidate_instance_id"],
            data["strategy_observation_id"],
            data["risk_observation_id"],
            data["dry_run_intent_id"],
            data["dry_run_order_id"],
            data["trade_date"],
            data["account_id"],
            data["code"],
            data["name"],
            data["side"],
            data["order_type"],
            data["quantity"],
            data["limit_price"],
            data["notional"],
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["idempotency_key"],
            data["gateway_command_id"],
            1 if data["broker_order_sent"] else 0,
            data["created_at"],
            data["expires_at"],
        ),
    )


def _insert_order(
    connection: sqlite3.Connection,
    order: LiveSimOrderRecord,
    *,
    trade_date: str,
) -> None:
    data = order.to_dict()
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            gateway_command_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            broker_order_no,
            broker_result_code,
            broker_message,
            filled_quantity,
            remaining_quantity,
            avg_fill_price,
            idempotency_key,
            live_sim_only,
            live_real_allowed,
            created_at,
            command_queued_at,
            command_dispatched_at,
            broker_acked_at,
            last_event_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?)
        """,
        (
            data["live_sim_order_id"],
            data["live_sim_intent_id"],
            data["gateway_command_id"],
            trade_date,
            data["account_id"],
            data["code"],
            data["name"],
            data["side"],
            data["order_type"],
            data["quantity"],
            data["limit_price"],
            data["notional"],
            data["status"],
            data["broker_order_no"],
            data["broker_result_code"],
            data["broker_message"],
            data["filled_quantity"],
            data["remaining_quantity"],
            data["avg_fill_price"],
            data["idempotency_key"],
            data["created_at"],
            data["command_queued_at"],
            data["command_dispatched_at"],
            data["broker_acked_at"],
            data["last_event_at"],
        ),
    )


def _insert_execution(
    connection: sqlite3.Connection,
    execution: LiveSimExecutionRecord,
) -> None:
    data = execution.to_dict()
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id,
            live_sim_order_id,
            live_sim_intent_id,
            broker_order_no,
            account_id,
            code,
            side,
            quantity,
            price,
            notional,
            executed_at,
            raw_event_json,
            live_sim_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            data["live_sim_execution_id"],
            data["live_sim_order_id"],
            data["live_sim_intent_id"],
            data["broker_order_no"],
            data["account_id"],
            data["code"],
            data["side"],
            data["quantity"],
            data["price"],
            data["notional"],
            data["executed_at"],
            _json_dumps(data["raw_event_json"]),
        ),
    )


def _insert_reconcile_snapshot(
    connection: sqlite3.Connection,
    snapshot: LiveSimReconcileSnapshot,
) -> None:
    data = snapshot.to_dict()
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id,
            account_id,
            trade_date,
            code,
            broker_open_order_count,
            broker_position_count,
            local_open_order_count,
            local_position_count,
            mismatch_count,
            status,
            snapshot_json,
            created_at,
            live_sim_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            data["reconcile_id"],
            data["account_id"],
            data["trade_date"],
            data["code"],
            data["broker_open_order_count"],
            data["broker_position_count"],
            data["local_open_order_count"],
            data["local_position_count"],
            data["mismatch_count"],
            data["status"],
            _json_dumps(data["snapshot_json"]),
            data["created_at"],
        ),
    )


def _save_rejection(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None,
    strategy_observation_id: str | None,
    risk_observation_id: str | None,
    trade_date: object,
    account_id: object,
    code: object,
    reason_codes: list[str] | tuple[str, ...],
    evidence: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_rejections (
            rejection_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            trade_date,
            account_id,
            code,
            reason_codes_json,
            evidence_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_message_id("live_sim_rejection"),
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
            None if trade_date is None else str(trade_date),
            None if account_id is None else str(account_id),
            None if code is None else str(code),
            _json_dumps(reason_codes),
            _json_dumps(evidence),
        ),
    )


def _record_error(
    connection: sqlite3.Connection,
    *,
    live_sim_intent_id: str | None,
    live_sim_order_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
    run_id: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_errors (
            run_id,
            live_sim_intent_id,
            live_sim_order_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            live_sim_intent_id,
            live_sim_order_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            _json_dumps(payload),
        ),
    )
    _record_lifecycle_event(
        connection,
        event_type="LIFECYCLE_ERROR",
        entity_type="LIVE_SIM_ERROR",
        entity_id=None,
        live_sim_order_id=live_sim_order_id,
        status="ERROR",
        reason=error_message,
        evidence={"run_id": run_id, "code": code, "payload": payload},
    )


def _record_lifecycle_event(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    entity_type: str,
    entity_id: str | None = None,
    live_sim_order_id: str | None = None,
    position_id: str | None = None,
    status: str | None = None,
    reason: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id,
            event_type,
            entity_type,
            entity_id,
            live_sim_order_id,
            position_id,
            status,
            reason,
            evidence_json,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            new_message_id("live_sim_lifecycle"),
            require_non_empty_str(event_type, "event_type").upper(),
            require_non_empty_str(entity_type, "entity_type").upper(),
            entity_id,
            live_sim_order_id,
            position_id,
            None if status is None else str(status).upper(),
            reason,
            _json_dumps(evidence or {}),
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
        INSERT INTO live_sim_runs (run_id, trade_date, started_at, status)
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
    command_count: int = 0,
    rejection_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE live_sim_runs
        SET completed_at = ?,
            evaluated_count = ?,
            eligible_count = ?,
            intent_count = ?,
            command_count = ?,
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
            command_count,
            rejection_count,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )


def _candidate_row(
    connection: sqlite3.Connection, candidate_instance_id: str
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
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
        "SELECT * FROM market_ticks_latest WHERE code = ?",
        (validate_stock_code(code),),
    ).fetchone()


def _latest_dry_run_evidence(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT
            i.dry_run_intent_id,
            i.status AS intent_status,
            i.notional,
            i.quantity,
            i.intended_price,
            o.dry_run_order_id,
            o.status AS order_status
        FROM dry_run_intents i
        LEFT JOIN dry_run_orders o ON o.dry_run_intent_id = i.dry_run_intent_id
        WHERE i.candidate_instance_id = ?
        ORDER BY i.created_at DESC, i.dry_run_intent_id DESC
        LIMIT 1
        """,
        (candidate_instance_id,),
    ).fetchone()
    if row is None:
        return {}
    return {
        "dry_run_intent_id": row["dry_run_intent_id"],
        "dry_run_order_id": row["dry_run_order_id"],
        "intent_status": row["intent_status"],
        "order_status": row["order_status"],
        "notional": row["notional"],
        "quantity": row["quantity"],
        "intended_price": row["intended_price"],
        "dry_run_only": True,
    }


def _calculate_live_sim_quantity_and_notional(
    price: float,
    settings: Settings,
    *,
    dry_run_evidence: Mapping[str, Any],
) -> tuple[int, float]:
    if price <= 0:
        return 0, 0.0
    dry_run_notional = dry_run_evidence.get("notional")
    target_notional = settings.live_sim_max_order_notional
    if dry_run_notional is not None:
        target_notional = min(target_notional, float(dry_run_notional))
    quantity = math.floor(target_notional / price)
    if quantity < 1:
        return quantity, price * quantity
    return quantity, price * quantity


def _live_sim_buy_limit_price(price: float, settings: Settings) -> int:
    return add_ticks(price, settings.live_sim_buy_price_offset_ticks)


def _live_sim_buy_price_policy_evidence(
    current_price: float,
    limit_price: float,
    settings: Settings,
) -> dict[str, Any]:
    return {
        "source": "CURRENT_PRICE_PLUS_KRX_TICKS",
        "current_price": current_price,
        "limit_price": limit_price,
        "buy_price_offset_ticks": settings.live_sim_buy_price_offset_ticks,
        "legacy_price_offset_ticks": settings.live_sim_price_offset_ticks,
    }


def _tick_offset_value(offset_ticks: int) -> float:
    return float(offset_ticks)


def _recent_active_live_sim_count_for_code(
    connection: sqlite3.Connection,
    code: str,
    settings: Settings,
    *,
    exclude_intent_id: str | None = None,
) -> int:
    cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=settings.live_sim_duplicate_cooldown_sec)
    )
    params: list[Any] = [
        validate_stock_code(code),
        *sorted(ACTIVE_LIVE_SIM_INTENT_STATUSES),
        cutoff,
    ]
    extra = ""
    if exclude_intent_id is not None:
        extra = "AND live_sim_intent_id != ?"
        params.append(exclude_intent_id)
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_intents
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_LIVE_SIM_INTENT_STATUSES)})
            AND created_at >= ?
            {extra}
        """,
        tuple(params),
    ).fetchone()
    intent_count = int(row["count"])
    params = [validate_stock_code(code), *sorted(ACTIVE_LIVE_SIM_ORDER_STATUSES), cutoff]
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_orders
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_LIVE_SIM_ORDER_STATUSES)})
            AND created_at >= ?
        """,
        tuple(params),
    ).fetchone()
    return intent_count + int(row["count"])


def _daily_order_count(connection: sqlite3.Connection, trade_date: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM live_sim_orders
        WHERE trade_date = ?
            AND UPPER(side) = 'BUY'
        """,
        (trade_date,),
    ).fetchone()
    return int(row["count"])


def _daily_order_notional(connection: sqlite3.Connection, trade_date: str) -> float:
    row = connection.execute(
        "SELECT COALESCE(SUM(notional), 0) AS total FROM live_sim_orders WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()
    return float(row["total"])


def _active_order_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_orders
        WHERE status IN ({_placeholders(ACTIVE_LIVE_SIM_ORDER_STATUSES)})
        """,
        tuple(sorted(ACTIVE_LIVE_SIM_ORDER_STATUSES)),
    ).fetchone()
    return int(row["count"])


def _active_position_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(DISTINCT code) AS count
        FROM (
            SELECT code
            FROM live_sim_positions
            WHERE status IN ('OPEN', 'CLOSING', 'RECONCILE_MISMATCH')
                AND quantity > 0
            UNION
            SELECT code
            FROM live_sim_orders
            WHERE side = 'BUY'
                AND status IN ('PARTIALLY_FILLED', 'FILLED')
        )
        """
    ).fetchone()
    return int(row["count"])


def _open_position_count_for_code(connection: sqlite3.Connection, code: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT position_id AS id
            FROM live_sim_positions
            WHERE code = ?
                AND status IN ('OPEN', 'CLOSING', 'RECONCILE_MISMATCH')
                AND quantity > 0
            UNION
            SELECT live_sim_order_id AS id
            FROM live_sim_orders
            WHERE code = ?
                AND side = 'BUY'
                AND status IN ('PARTIALLY_FILLED', 'FILLED')
        )
        """,
        (validate_stock_code(code), validate_stock_code(code)),
    ).fetchone()
    return int(row["count"] or 0)


def _active_exit_count_for_code(connection: sqlite3.Connection, code: str) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_exit_intents
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_EXIT_INTENT_STATUSES)})
        """,
        (validate_stock_code(code), *sorted(ACTIVE_EXIT_INTENT_STATUSES)),
    ).fetchone()
    return int(row["count"] or 0)


def _active_cancel_count_for_code(connection: sqlite3.Connection, code: str) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_cancel_intents
        WHERE code = ?
            AND status IN ({_placeholders(ACTIVE_CANCEL_INTENT_STATUSES)})
        """,
        (validate_stock_code(code), *sorted(ACTIVE_CANCEL_INTENT_STATUSES)),
    ).fetchone()
    return int(row["count"] or 0)


def _active_cancel_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_cancel_intents
        WHERE status IN ({_placeholders(ACTIVE_CANCEL_INTENT_STATUSES)})
        """,
        tuple(sorted(ACTIVE_CANCEL_INTENT_STATUSES)),
    ).fetchone()
    return int(row["count"] or 0)


def _active_exit_signal_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_exit_signals
        WHERE status IN ({_placeholders(ACTIVE_EXIT_SIGNAL_STATUSES)})
        """,
        tuple(sorted(ACTIVE_EXIT_SIGNAL_STATUSES)),
    ).fetchone()
    return int(row["count"] or 0)


def _latest_reconcile_blocks_new_buy(
    connection: sqlite3.Connection,
    settings: Settings,
) -> bool:
    if not settings.live_sim_reconcile_block_new_buy_on_mismatch:
        return False
    row = connection.execute(
        """
        SELECT status, mismatch_count, blocking_new_buy
        FROM live_sim_reconcile_snapshots
        ORDER BY created_at DESC, reconcile_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return False
    return bool(row["blocking_new_buy"]) or (
        int(row["mismatch_count"] or 0) > 0 and str(row["status"]).upper() == "RECONCILE_MISMATCH"
    )


def _unresolved_lifecycle_error_count(
    connection: sqlite3.Connection,
    *,
    code: str | None = None,
) -> int:
    clauses = [
        "event_type IN ('RECONCILE_MISMATCH', 'LIFECYCLE_ERROR')",
    ]
    params: list[Any] = []
    if code is not None:
        clauses.append("evidence_json LIKE ?")
        params.append(f'%"{validate_stock_code(code)}"%')
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_lifecycle_events
        WHERE {" AND ".join(clauses)}
        """,
        tuple(params),
    ).fetchone()
    return int(row["count"] or 0)


def _live_sim_candidate_targets(
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


def _gateway_command_row(
    connection: sqlite3.Connection,
    command_id: object,
) -> dict[str, Any] | None:
    if command_id is None:
        return None
    row = connection.execute(
        "SELECT * FROM gateway_commands WHERE command_id = ?",
        (str(command_id),),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


def _order_by_command_id(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_orders WHERE gateway_command_id = ?",
        (require_non_empty_str(command_id, "command_id"),),
    ).fetchone()
    return None if row is None else _order_row_to_dict(row)


def _order_by_broker_order_no(
    connection: sqlite3.Connection,
    broker_order_no: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_orders WHERE broker_order_no = ?",
        (require_non_empty_str(broker_order_no, "broker_order_no"),),
    ).fetchone()
    return None if row is None else _order_row_to_dict(row)


def _cancel_intent_by_command_id(
    connection: sqlite3.Connection,
    command_id: str | None,
) -> dict[str, Any] | None:
    if command_id is None:
        return None
    row = connection.execute(
        "SELECT * FROM live_sim_cancel_intents WHERE gateway_command_id = ?",
        (require_non_empty_str(command_id, "command_id"),),
    ).fetchone()
    return None if row is None else _cancel_intent_row_to_dict(row)


def _latest_cancel_intent_for_order(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_cancel_intents
        WHERE live_sim_order_id = ?
        ORDER BY created_at DESC, cancel_intent_id DESC
        LIMIT 1
        """,
        (require_non_empty_str(live_sim_order_id, "live_sim_order_id"),),
    ).fetchone()
    return None if row is None else _cancel_intent_row_to_dict(row)


def _is_ttl_cancel_ack(cancel_intent: Mapping[str, Any] | None) -> bool:
    if cancel_intent is None:
        return False
    return (
        str(cancel_intent.get("reason") or "").upper() == "TTL_EXPIRED"
        and str(cancel_intent.get("status") or "").upper() == "ACKED"
    )


def _reprice_root_order_id(
    order: Mapping[str, Any],
    intent: Mapping[str, Any] | None,
) -> str:
    evidence = _json_object(None if intent is None else intent.get("evidence_json"))
    reprice = _json_object(evidence.get("reprice"))
    return str(
        reprice.get("root_live_sim_order_id")
        or reprice.get("original_live_sim_order_id")
        or order["live_sim_order_id"]
    )


def _reprice_attempt_count_for_root_order(
    connection: sqlite3.Connection,
    root_live_sim_order_id: str,
) -> int:
    rows = connection.execute(
        """
        SELECT evidence_json
        FROM live_sim_intents
        ORDER BY created_at DESC, live_sim_intent_id DESC
        LIMIT 1000
        """
    ).fetchall()
    count = 0
    for row in rows:
        evidence = _json_object(row["evidence_json"])
        reprice = _json_object(evidence.get("reprice"))
        if str(reprice.get("root_live_sim_order_id") or "") == root_live_sim_order_id:
            count += 1
    return count


def _exit_intent_by_order_id(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_exit_intents WHERE live_sim_order_id = ?",
        (require_non_empty_str(live_sim_order_id, "live_sim_order_id"),),
    ).fetchone()
    return None if row is None else _exit_intent_row_to_dict(row)


def _latest_exit_signal_for_position(
    connection: sqlite3.Connection,
    position_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_exit_signals
        WHERE position_id = ?
            AND status IN ('SIGNALLED', 'EXIT_INTENT_CREATED', 'COMMAND_QUEUED')
        ORDER BY created_at DESC, exit_signal_id DESC
        LIMIT 1
        """,
        (require_non_empty_str(position_id, "position_id"),),
    ).fetchone()
    return None if row is None else _exit_signal_row_to_dict(row)


def _active_cancel_for_order(connection: sqlite3.Connection, live_sim_order_id: str) -> bool:
    row = connection.execute(
        f"""
        SELECT 1
        FROM live_sim_cancel_intents
        WHERE live_sim_order_id = ?
            AND status IN ({_placeholders(ACTIVE_CANCEL_INTENT_STATUSES)})
        LIMIT 1
        """,
        (
            require_non_empty_str(live_sim_order_id, "live_sim_order_id"),
            *sorted(ACTIVE_CANCEL_INTENT_STATUSES),
        ),
    ).fetchone()
    return row is not None


def _active_exit_for_position(connection: sqlite3.Connection, position_id: str) -> bool:
    if _active_exit_intent_for_position(connection, position_id):
        return True
    signal = connection.execute(
        f"""
        SELECT 1
        FROM live_sim_exit_signals
        WHERE position_id = ?
            AND status IN ({_placeholders(ACTIVE_EXIT_SIGNAL_STATUSES)})
        LIMIT 1
        """,
        (position_id, *sorted(ACTIVE_EXIT_SIGNAL_STATUSES)),
    ).fetchone()
    return signal is not None


def _active_exit_intent_for_position(connection: sqlite3.Connection, position_id: str) -> bool:
    row = connection.execute(
        f"""
        SELECT 1
        FROM live_sim_exit_intents
        WHERE position_id = ?
            AND status IN ({_placeholders(ACTIVE_EXIT_INTENT_STATUSES)})
        LIMIT 1
        """,
        (require_non_empty_str(position_id, "position_id"), *sorted(ACTIVE_EXIT_INTENT_STATUSES)),
    ).fetchone()
    return row is not None


def _average_fill_price(connection: sqlite3.Connection, live_sim_order_id: str) -> float:
    row = connection.execute(
        """
        SELECT SUM(price * quantity) AS notional, SUM(quantity) AS quantity
        FROM live_sim_executions
        WHERE live_sim_order_id = ?
        """,
        (live_sim_order_id,),
    ).fetchone()
    quantity = int(row["quantity"] or 0)
    if quantity == 0:
        return 0.0
    return float(row["notional"]) / quantity


def _execution_quantity_sum(connection: sqlite3.Connection, live_sim_order_id: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(SUM(quantity), 0) AS quantity
        FROM live_sim_executions
        WHERE live_sim_order_id = ?
        """,
        (live_sim_order_id,),
    ).fetchone()
    return int(row["quantity"] or 0)


def _net_position_quantity_from_fills(
    connection: sqlite3.Connection,
    account_id: str,
    code: str,
) -> int:
    row = connection.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END), 0)
            AS quantity
        FROM live_sim_executions
        WHERE account_id = ?
            AND code = ?
        """,
        (account_id, validate_stock_code(code)),
    ).fetchone()
    return int(row["quantity"] or 0)


def _stale_open_order_count(connection: sqlite3.Connection, settings: Settings) -> int:
    cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=settings.live_sim_reconcile_stale_order_sec)
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_orders
        WHERE status IN ({_placeholders(ACTIVE_LIVE_SIM_ORDER_STATUSES)})
            AND created_at <= ?
        """,
        (*sorted(ACTIVE_LIVE_SIM_ORDER_STATUSES), cutoff),
    ).fetchone()
    return int(row["count"] or 0)


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
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_sent"] = bool(item["broker_order_sent"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _order_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _execution_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["raw_event_json"] = _json_object(item.pop("raw_event_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = False
    return item


def _rejection_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = True
    item["live_real_allowed"] = False
    return item


def _reconcile_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["snapshot_json"] = _json_object(item.pop("snapshot_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = False
    if "blocking_new_buy" in item:
        item["blocking_new_buy"] = bool(item["blocking_new_buy"])
    else:
        item["blocking_new_buy"] = bool(item["snapshot_json"].get("blocking_new_buy"))
    if "allow_exit" in item:
        item["allow_exit"] = bool(item["allow_exit"])
    else:
        item["allow_exit"] = bool(item["snapshot_json"].get("allow_exit", True))
    return item


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["payload"] = _json_object(item.pop("payload_json"))
    return item


def _position_row_to_dict(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    item = _row_to_dict(row) if isinstance(row, sqlite3.Row) else dict(row)
    item["live_sim_only"] = bool(item.get("live_sim_only", True))
    item["live_real_allowed"] = bool(item.get("live_real_allowed", False))
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _exit_signal_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _exit_intent_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _cancel_intent_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


def _lifecycle_event_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
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


def _idempotency_key(
    trade_date: str,
    code: str,
    candidate_instance_id: str,
    strategy_observation_id: str | None,
    risk_observation_id: str | None,
    *,
    suffix: str | None = None,
) -> str:
    parts = [
        "live_sim",
        require_non_empty_str(trade_date, "trade_date"),
        validate_stock_code(code),
        require_non_empty_str(candidate_instance_id, "candidate_instance_id"),
        require_non_empty_str(strategy_observation_id or "missing_strategy", "strategy"),
        require_non_empty_str(risk_observation_id or "missing_risk", "risk"),
    ]
    if suffix is not None:
        parts.append(require_non_empty_str(suffix, "idempotency_suffix"))
    return ":".join(parts)


def _today_trade_date() -> str:
    return market_today()


def _age_seconds_from_wire(value: object) -> float:
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except ValueError:
        return float("inf")


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
