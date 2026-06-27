from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_value,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
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
from domain.risk.status import RiskObservationStatus
from domain.strategy.status import StrategyObservationStatus
from storage.gateway_command_store import enqueue_command

from services.config import Settings, load_settings
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


def evaluate_live_sim_eligibility(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
) -> LiveSimEligibility:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    safety_gate = check_live_sim_safety_gate(connection, resolved_settings)
    candidate = _candidate_row(connection, normalized_id)
    strategy = _strategy_latest_row(connection, normalized_id)
    risk = _risk_latest_row(connection, normalized_id)

    reason_codes: list[str] = []
    evidence: dict[str, Any] = {
        "candidate_instance_id": normalized_id,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "ai_artifacts_used": False,
    }
    if not safety_gate.passed:
        reason_codes.extend(safety_gate.reason_codes)
    if candidate is not None:
        evidence["candidate"] = _candidate_evidence(candidate)
    if strategy is not None:
        evidence["strategy"] = _strategy_evidence(strategy)
    if risk is not None:
        evidence["risk"] = _risk_evidence(risk)

    if candidate is None:
        reason_codes.append(LiveSimReasonCode.CANDIDATE_NOT_FOUND.value)
        trade_date = None
        code = None
        name = "UNKNOWN"
    else:
        trade_date = str(candidate["trade_date"])
        code = validate_stock_code(candidate["code"])
        name = str(candidate["name"])
        if (
            resolved_settings.live_sim_require_candidate_context_ready
            and str(candidate["state"]).upper() != CandidateState.CONTEXT_READY.value
        ):
            reason_codes.append(LiveSimReasonCode.CANDIDATE_NOT_CONTEXT_READY.value)

    if strategy is None:
        reason_codes.append(LiveSimReasonCode.STRATEGY_OBSERVATION_MISSING.value)
    elif (
        resolved_settings.live_sim_require_strategy_matched
        and str(strategy["overall_status"]).upper()
        != StrategyObservationStatus.MATCHED_OBSERVATION.value
    ):
        reason_codes.append(LiveSimReasonCode.STRATEGY_NOT_MATCHED.value)

    if risk is None:
        reason_codes.append(LiveSimReasonCode.RISK_OBSERVATION_MISSING.value)
    elif (
        resolved_settings.live_sim_require_risk_observe_pass
        and str(risk["overall_status"]).upper() != RiskObservationStatus.OBSERVE_PASS.value
    ):
        reason_codes.append(LiveSimReasonCode.RISK_NOT_OBSERVE_PASS.value)

    tick = _latest_tick_row(connection, code) if code is not None else None
    price = 0.0
    if tick is None:
        reason_codes.append(LiveSimReasonCode.LATEST_TICK_MISSING.value)
    else:
        tick_age = tick_age_seconds(tick["event_ts"])
        price = float(tick["price"])
        evidence["latest_tick"] = _tick_evidence(tick, tick_age)
        if resolved_settings.live_sim_require_fresh_tick and (
            tick_age > resolved_settings.live_sim_stale_tick_sec
        ):
            reason_codes.append(LiveSimReasonCode.LATEST_TICK_STALE.value)

    dry_run_evidence = _latest_dry_run_evidence(connection, normalized_id)
    if dry_run_evidence:
        evidence["dry_run"] = dry_run_evidence
    elif resolved_settings.live_sim_require_dry_run_evidence:
        reason_codes.append(LiveSimReasonCode.DRY_RUN_EVIDENCE_MISSING.value)

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
        price,
        resolved_settings,
        dry_run_evidence=dry_run_evidence,
    )
    evidence["sizing"] = {
        "max_order_notional": resolved_settings.live_sim_max_order_notional,
        "dry_run_notional": dry_run_evidence.get("notional") if dry_run_evidence else None,
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
        strategy_observation_id=(
            None if strategy is None else str(strategy["strategy_observation_id"])
        ),
        risk_observation_id=None if risk is None else str(risk["risk_observation_id"]),
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
) -> LiveSimIntent:
    resolved_settings = settings or load_settings()
    eligibility = evaluate_live_sim_eligibility(
        connection, candidate_instance_id, resolved_settings
    )
    evidence = dict(eligibility.evidence_json) | {"source": source}
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
            ),
            created_at=datetime_to_wire(utc_now()),
        )

    if candidate is None or strategy is None or risk is None:
        raise ValueError("eligible LIVE_SIM intent requires candidate, strategy, and risk rows")
    tick = _latest_tick_row(connection, candidate["code"])
    if tick is None:
        raise ValueError("eligible LIVE_SIM intent requires latest tick")

    price = float(tick["price"])
    quantity, notional = _calculate_live_sim_quantity_and_notional(
        price,
        resolved_settings,
        dry_run_evidence=_json_object(evidence.get("dry_run")),
    )
    limit_price = price + _tick_offset_value(resolved_settings.live_sim_price_offset_ticks)
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
            "quantity": quantity,
            "notional": notional,
            "limit_price": limit_price,
        },
        idempotency_key=_idempotency_key(
            str(candidate["trade_date"]),
            str(candidate["code"]),
            str(candidate["candidate_instance_id"]),
            str(strategy["strategy_observation_id"]),
            str(risk["risk_observation_id"]),
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

    safety_gate = check_live_sim_safety_gate(connection, resolved_settings)
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
    return {"handled": False, "reason": "event_type_not_live_sim"}


def reconcile_live_sim(
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

    snapshot = LiveSimReconcileSnapshot(
        reconcile_id=new_message_id("live_sim_reconcile"),
        account_id=resolved_settings.live_sim_account_id or "SIMULATION_ACCOUNT_REQUIRED",
        trade_date=trade_date,
        broker_open_order_count=0,
        broker_position_count=0,
        local_open_order_count=len(open_orders),
        local_position_count=_active_position_count(connection),
        mismatch_count=mismatch_count,
        status="LOCAL_ONLY" if mismatch_count == 0 else "RECONCILE_MISMATCH",
        snapshot_json={
            "broker_snapshot_available": False,
            "broker_snapshot_status": "BROKER_SNAPSHOT_UNAVAILABLE",
            "open_orders": open_orders,
            "mismatches": mismatches,
        },
        created_at=datetime_to_wire(utc_now()),
    )
    _insert_reconcile_snapshot(connection, snapshot)
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
        "rejection_count": _count_rows(connection, "live_sim_rejections"),
        "open_order_count": _active_order_count(connection),
        "max_order_notional": resolved_settings.live_sim_max_order_notional,
        "max_daily_order_count": resolved_settings.live_sim_max_daily_order_count,
        "max_daily_notional": resolved_settings.live_sim_max_daily_notional,
        "allow_buy": resolved_settings.live_sim_allow_buy,
        "allow_sell": resolved_settings.live_sim_allow_sell,
        "allow_market_order": resolved_settings.live_sim_allow_market_order,
        "allow_limit_order": resolved_settings.live_sim_allow_limit_order,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
        "warnings": [
            "LIVE_SIM is simulation-account only.",
            "LIVE_REAL remains disabled in PR12.",
            "Dashboard order buttons are not available in PR12.",
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


def _handle_live_sim_command_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> dict[str, Any]:
    if event.command_id is None:
        return {"handled": False, "reason": "missing_command_id"}
    order = _order_by_command_id(connection, event.command_id)
    if order is None:
        return {"handled": False, "reason": "command_not_live_sim"}
    now = datetime_to_wire(utc_now())
    payload = dict(event.payload)
    event_type = event.event_type.strip().lower()
    if event_type == "command_started":
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                command_dispatched_at = COALESCE(command_dispatched_at, ?),
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                LiveSimOrderStatus.COMMAND_DISPATCHED.value,
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
    connection.commit()
    return {
        "handled": True,
        "live_sim_order_id": order["live_sim_order_id"],
        "live_sim_execution_id": execution.live_sim_execution_id,
    }


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
        "SELECT COUNT(*) AS count FROM live_sim_orders WHERE trade_date = ?",
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
        FROM live_sim_orders
        WHERE side = 'BUY'
            AND status IN ('PARTIALLY_FILLED', 'FILLED')
        """
    ).fetchone()
    return int(row["count"])


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
) -> str:
    return ":".join(
        [
            "live_sim",
            require_non_empty_str(trade_date, "trade_date"),
            validate_stock_code(code),
            require_non_empty_str(candidate_instance_id, "candidate_instance_id"),
            require_non_empty_str(strategy_observation_id or "missing_strategy", "strategy"),
            require_non_empty_str(risk_observation_id or "missing_risk", "risk"),
        ]
    )


def _today_trade_date() -> str:
    return datetime_to_wire(utc_now())[:10]


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _placeholders(values: set[str]) -> str:
    return ",".join("?" for _ in values)


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


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
