from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, utc_now, validate_stock_code
from domain.live_sim.models import LiveSimIntent, LiveSimOrderRecord
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderType, LiveSimSide

from services.config import Settings, load_settings
from services.live_sim.live_sim_service import (
    _insert_intent,
    _save_rejection,
    queue_live_sim_order_command,
)
from services.live_sim.order_plan_eligibility import (
    LiveSimOrderPlanEligibility,
    evaluate_live_sim_order_plan_eligibility,
    find_live_sim_intent_by_order_plan,
    find_live_sim_order_by_intent,
    record_live_sim_order_plan_rejection,
)

ORDER_PLAN_INTENT_SOURCE = "order_plan_pipeline"


def create_live_sim_intent_from_order_plan(
    connection: sqlite3.Connection,
    order_plan_id: str,
    settings: Settings | None = None,
    source: str = ORDER_PLAN_INTENT_SOURCE,
) -> LiveSimIntent:
    resolved_settings = settings or load_settings()
    existing = find_live_sim_intent_by_order_plan(connection, order_plan_id)
    if existing is not None and existing["status"] in {
        LiveSimIntentStatus.CREATED.value,
        LiveSimIntentStatus.COMMAND_QUEUED.value,
    }:
        eligibility = evaluate_live_sim_order_plan_eligibility(
            connection,
            order_plan_id,
            settings=resolved_settings,
        )
        if LiveSimReasonCode.ORDER_PLAN_DUPLICATE_INTENT.value in eligibility.reason_codes:
            record_live_sim_order_plan_rejection(
                connection,
                eligibility,
                account_id=resolved_settings.live_sim_account_id,
                source=source,
            )
            connection.commit()
        return LiveSimIntent.from_dict(existing)

    eligibility = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=resolved_settings,
    )
    if not eligibility.eligible:
        record_live_sim_order_plan_rejection(
            connection,
            eligibility,
            account_id=resolved_settings.live_sim_account_id,
            source=source,
        )
        connection.commit()
        return _rejected_intent(eligibility, resolved_settings, source=source)

    intent = _build_intent_from_eligibility(eligibility, resolved_settings, source=source)
    duplicate_by_key = _intent_by_idempotency_key(connection, intent.idempotency_key)
    if duplicate_by_key is not None:
        _save_rejection(
            connection,
            candidate_instance_id=eligibility.candidate_instance_id,
            strategy_observation_id=eligibility.strategy_observation_id,
            risk_observation_id=eligibility.risk_observation_id,
            trade_date=eligibility.order_plan.get("trade_date"),
            account_id=resolved_settings.live_sim_account_id,
            code=eligibility.code,
            reason_codes=[LiveSimReasonCode.ORDER_PLAN_DUPLICATE_INTENT.value],
            evidence={
                "source": source,
                "order_plan_id": order_plan_id,
                "duplicate_intent": duplicate_by_key,
                "eligibility": eligibility.to_dict(),
            },
        )
        connection.commit()
        return LiveSimIntent.from_dict(duplicate_by_key)

    _insert_intent(connection, intent)
    connection.commit()
    return intent


def queue_live_sim_order_command_from_order_plan(
    connection: sqlite3.Connection,
    order_plan_id: str,
    settings: Settings | None = None,
    source: str = ORDER_PLAN_INTENT_SOURCE,
) -> LiveSimOrderRecord:
    resolved_settings = settings or load_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=resolved_settings,
        source=source,
    )
    if intent.status is LiveSimIntentStatus.COMMAND_QUEUED:
        existing_order = find_live_sim_order_by_intent(connection, intent.live_sim_intent_id)
        if existing_order is not None:
            return _order_record_from_dict(existing_order)
    if intent.status is not LiveSimIntentStatus.CREATED:
        raise ValueError(",".join(intent.reason_codes) or intent.status.value)
    disabled_reasons: list[str] = []
    if not resolved_settings.live_sim_pilot_pipeline_enabled:
        disabled_reasons.append(LiveSimReasonCode.PILOT_PIPELINE_DISABLED.value)
    if not resolved_settings.live_sim_pilot_auto_queue_command:
        disabled_reasons.append(LiveSimReasonCode.PILOT_AUTO_QUEUE_DISABLED.value)
    if disabled_reasons:
        _save_rejection(
            connection,
            candidate_instance_id=intent.candidate_instance_id,
            strategy_observation_id=intent.strategy_observation_id,
            risk_observation_id=intent.risk_observation_id,
            trade_date=intent.trade_date,
            account_id=intent.account_id,
            code=intent.code,
            reason_codes=disabled_reasons,
            evidence={
                "source": source,
                "order_plan_id": order_plan_id,
                "intent": intent.to_dict(),
                "live_sim_only": True,
                "live_real_allowed": False,
            },
        )
        connection.commit()
        raise ValueError(",".join(disabled_reasons))
    return queue_live_sim_order_command(
        connection,
        intent.live_sim_intent_id,
        settings=resolved_settings,
    )


def _build_intent_from_eligibility(
    eligibility: LiveSimOrderPlanEligibility,
    settings: Settings,
    *,
    source: str,
) -> LiveSimIntent:
    order_plan = dict(eligibility.order_plan)
    sizing = dict(eligibility.sizing)
    candidate_id = str(eligibility.candidate_instance_id)
    code = validate_stock_code(str(eligibility.code))
    trade_date = str(order_plan["trade_date"])
    limit_price = float(sizing["limit_price"])
    max_notional = min(
        settings.live_sim_order_plan_max_notional,
        settings.live_sim_max_order_notional,
    )
    planned_quantity = int(sizing["quantity"])
    quantity = min(planned_quantity, math.floor(max_notional / limit_price))
    notional = float(quantity * limit_price)
    now = utc_now()
    expires_at = min(
        now + timedelta(seconds=settings.live_sim_order_ttl_sec),
        _parse_expiry(order_plan["expires_at"]),
    )
    idempotency_key = make_live_sim_order_plan_idempotency_key(
        trade_date=trade_date,
        account_id=settings.live_sim_account_id,
        order_plan_id=str(order_plan["order_plan_id"]),
        code=code,
        side=LiveSimSide.BUY.value,
        limit_price=limit_price,
        quantity=quantity,
    )
    dry_run = dict(eligibility.dry_run_evidence)
    evidence = dict(eligibility.evidence_json) | {
        "source": source,
        "order_plan_id": order_plan["order_plan_id"],
        "order_plan_draft": order_plan,
        "entry_timing_evidence": _json_object(order_plan.get("evidence_json")),
        "entry_timing_state": order_plan["entry_timing_state"],
        "setup_type": order_plan["setup_type"],
        "theme_state": order_plan.get("theme_state"),
        "stock_role": order_plan.get("stock_role"),
        "not_order_intent_source": True,
        "converted_to_live_sim_intent": True,
        "quantity": quantity,
        "notional": notional,
        "limit_price": limit_price,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
    }
    return LiveSimIntent(
        live_sim_intent_id=new_message_id("live_sim_intent"),
        candidate_instance_id=candidate_id,
        strategy_observation_id=eligibility.strategy_observation_id,
        risk_observation_id=eligibility.risk_observation_id,
        dry_run_intent_id=dry_run.get("dry_run_intent_id"),
        dry_run_order_id=dry_run.get("dry_run_order_id"),
        trade_date=trade_date,
        account_id=settings.live_sim_account_id,
        code=code,
        name=str(eligibility.name),
        side=LiveSimSide.BUY,
        order_type=LiveSimOrderType.LIMIT,
        quantity=quantity,
        limit_price=limit_price,
        notional=notional,
        status=LiveSimIntentStatus.CREATED,
        reason_codes=[LiveSimReasonCode.OBSERVE_ONLY_AI_ARTIFACT_IGNORED.value],
        evidence_json=evidence,
        idempotency_key=idempotency_key,
        created_at=now,
        expires_at=expires_at,
    )


def _rejected_intent(
    eligibility: LiveSimOrderPlanEligibility,
    settings: Settings,
    *,
    source: str,
) -> LiveSimIntent:
    order_plan = dict(eligibility.order_plan)
    evidence = eligibility.to_dict() | {"source": source}
    trade_date = str(order_plan.get("trade_date") or "UNKNOWN")
    code = str(eligibility.code or order_plan.get("code") or "000000")
    candidate_id = str(
        eligibility.candidate_instance_id
        or order_plan.get("candidate_instance_id")
        or "ORDER_PLAN_MISSING"
    )
    return LiveSimIntent(
        live_sim_intent_id=new_message_id("live_sim_intent_rejected"),
        candidate_instance_id=candidate_id,
        strategy_observation_id=eligibility.strategy_observation_id,
        risk_observation_id=eligibility.risk_observation_id,
        dry_run_intent_id=None,
        dry_run_order_id=None,
        trade_date=trade_date,
        account_id=settings.live_sim_account_id or "SIMULATION_ACCOUNT_REQUIRED",
        code=code,
        name=str(eligibility.name or order_plan.get("name") or "UNKNOWN"),
        side=LiveSimSide.BUY,
        order_type=LiveSimOrderType.LIMIT,
        quantity=0,
        limit_price=None,
        notional=0,
        status=LiveSimIntentStatus.REJECTED,
        reason_codes=eligibility.reason_codes,
        evidence_json=evidence,
        idempotency_key=make_live_sim_order_plan_idempotency_key(
            trade_date=trade_date,
            account_id=settings.live_sim_account_id or "SIMULATION_ACCOUNT_REQUIRED",
            order_plan_id=eligibility.order_plan_id,
            code=code,
            side=LiveSimSide.BUY.value,
            limit_price=0,
            quantity=0,
        ),
        created_at=datetime_to_wire(utc_now()),
    )


def make_live_sim_order_plan_idempotency_key(
    *,
    trade_date: str,
    account_id: str,
    order_plan_id: str,
    code: str,
    side: str,
    limit_price: float,
    quantity: int,
) -> str:
    return ":".join(
        [
            "live_sim",
            "order_plan",
            str(trade_date),
            str(account_id),
            str(order_plan_id),
            validate_stock_code(code),
            str(side).upper(),
            str(int(limit_price) if float(limit_price).is_integer() else limit_price),
            str(int(quantity)),
        ]
    )


def _intent_by_idempotency_key(
    connection: sqlite3.Connection,
    idempotency_key: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM live_sim_intents WHERE idempotency_key = ?",
        (idempotency_key,),
    ).fetchone()
    if row is None:
        return None
    item = {key: row[key] for key in row.keys()}
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_sent"] = bool(item["broker_order_sent"])
    return item


def _order_record_from_dict(data: Mapping[str, Any]) -> LiveSimOrderRecord:
    return LiveSimOrderRecord(
        live_sim_order_id=str(data["live_sim_order_id"]),
        live_sim_intent_id=str(data["live_sim_intent_id"]),
        gateway_command_id=data.get("gateway_command_id"),
        account_id=str(data["account_id"]),
        code=str(data["code"]),
        name=str(data["name"]),
        side=str(data["side"]),
        order_type=str(data["order_type"]),
        quantity=int(data["quantity"]),
        limit_price=data.get("limit_price"),
        notional=float(data["notional"]),
        status=str(data["status"]),
        broker_order_no=data.get("broker_order_no"),
        broker_result_code=data.get("broker_result_code"),
        broker_message=data.get("broker_message"),
        filled_quantity=int(data.get("filled_quantity") or 0),
        remaining_quantity=int(data.get("remaining_quantity") or 0),
        avg_fill_price=data.get("avg_fill_price"),
        idempotency_key=str(data["idempotency_key"]),
        created_at=data.get("created_at"),
        command_queued_at=data.get("command_queued_at"),
        command_dispatched_at=data.get("command_dispatched_at"),
        broker_acked_at=data.get("broker_acked_at"),
        last_event_at=data.get("last_event_at"),
    )


def _parse_expiry(value: object):
    from domain.broker.utils import parse_timestamp

    return parse_timestamp(value, "expires_at")


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return [value.upper()] if value.strip() else []
    else:
        loaded = value
    if not isinstance(loaded, list):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]
