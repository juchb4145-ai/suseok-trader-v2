from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_value,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.live_sim.reasons import LiveSimReasonCode

from services.admission import AdmissionPolicy, AdmissionReason, evaluate_trade_admission
from services.config import Settings, load_settings
from services.entry_timing.models import EntryTimingState, OrderPlanStatus, SetupType
from services.entry_timing.tick_size import add_ticks
from services.live_sim.live_sim_service import (
    _active_cancel_count_for_code,
    _active_exit_count_for_code,
    _active_order_count,
    _active_position_count,
    _daily_order_count,
    _daily_order_notional,
    _latest_dry_run_evidence,
    _latest_reconcile_blocks_new_buy,
    _open_position_count_for_code,
    _recent_active_live_sim_count_for_code,
    _save_rejection,
    _unresolved_lifecycle_error_count,
)
from services.live_sim.safety_gate import check_live_sim_safety_gate
from services.pipeline_coherency import assess_order_plan_lineage

READY_ENTRY_TIMING_STATES = {
    EntryTimingState.GOOD_PULLBACK.value,
    EntryTimingState.PULLBACK_RECLAIM.value,
    EntryTimingState.VWAP_RECLAIM.value,
    EntryTimingState.MOMENTUM_CONTINUATION.value,
}

BLOCKED_ORDER_PLAN_REASON_TOKENS = ("BLOCKED", "CHASE", "OVERHEAT", "STALE")

ORDER_PLAN_ADMISSION_REASON_MAP = {
    AdmissionReason.CANDIDATE_NOT_FOUND.value: LiveSimReasonCode.CANDIDATE_NOT_FOUND.value,
    AdmissionReason.CANDIDATE_NOT_CONTEXT_READY.value: (
        LiveSimReasonCode.ORDER_PLAN_CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.CANDIDATE_CONTEXT_MISSING.value: (
        LiveSimReasonCode.ORDER_PLAN_CANDIDATE_NOT_CONTEXT_READY.value
    ),
    AdmissionReason.STRATEGY_OBSERVATION_MISSING.value: (
        LiveSimReasonCode.ORDER_PLAN_STRATEGY_NOT_MATCHED.value
    ),
    AdmissionReason.STRATEGY_NOT_MATCHED.value: (
        LiveSimReasonCode.ORDER_PLAN_STRATEGY_NOT_MATCHED.value
    ),
    AdmissionReason.STRATEGY_OBSERVE_ONLY_MISMATCH.value: (
        LiveSimReasonCode.ORDER_PLAN_STRATEGY_NOT_MATCHED.value
    ),
    AdmissionReason.RISK_OBSERVATION_MISSING.value: (
        LiveSimReasonCode.ORDER_PLAN_RISK_NOT_PASS.value
    ),
    AdmissionReason.RISK_NOT_OBSERVE_PASS.value: LiveSimReasonCode.ORDER_PLAN_RISK_NOT_PASS.value,
    AdmissionReason.RISK_OBSERVE_ONLY_MISMATCH.value: (
        LiveSimReasonCode.ORDER_PLAN_RISK_NOT_PASS.value
    ),
    AdmissionReason.LATEST_TICK_MISSING.value: (
        LiveSimReasonCode.ORDER_PLAN_LATEST_TICK_MISSING.value
    ),
    AdmissionReason.LATEST_TICK_STALE.value: (
        LiveSimReasonCode.ORDER_PLAN_LATEST_TICK_STALE.value
    ),
    AdmissionReason.DRY_RUN_EVIDENCE_MISSING.value: (
        LiveSimReasonCode.ORDER_PLAN_DRY_RUN_EVIDENCE_MISSING.value
    ),
}


@dataclass(frozen=True, kw_only=True)
class LiveSimOrderPlanEligibility:
    eligible: bool
    status: str
    order_plan_id: str
    candidate_instance_id: str | None = None
    code: str | None = None
    name: str | None = None
    strategy_observation_id: str | None = None
    risk_observation_id: str | None = None
    entry_timing_evaluation_id: str | None = None
    reason_codes: Sequence[str] = field(default_factory=tuple)
    safety_gate_result: Mapping[str, Any] = field(default_factory=dict)
    order_plan: Mapping[str, Any] = field(default_factory=dict)
    candidate_evidence: Mapping[str, Any] = field(default_factory=dict)
    strategy_evidence: Mapping[str, Any] = field(default_factory=dict)
    risk_evidence: Mapping[str, Any] = field(default_factory=dict)
    latest_tick_evidence: Mapping[str, Any] = field(default_factory=dict)
    sizing: Mapping[str, Any] = field(default_factory=dict)
    dry_run_evidence: Mapping[str, Any] = field(default_factory=dict)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    computed_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    live_sim_only: bool = True
    live_real_allowed: bool = False
    broker_order_path: str = "LIVE_SIM_ONLY"
    real_order_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        reason_codes = [str(reason).upper() for reason in self.reason_codes]
        return {
            "eligible": bool(self.eligible),
            "status": self.status,
            "order_plan_id": self.order_plan_id,
            "candidate_instance_id": self.candidate_instance_id,
            "code": self.code,
            "name": self.name,
            "strategy_observation_id": self.strategy_observation_id,
            "risk_observation_id": self.risk_observation_id,
            "entry_timing_evaluation_id": self.entry_timing_evaluation_id,
            "reason_codes": reason_codes,
            "reason_categories": {
                reason: _reason_category(reason) for reason in reason_codes
            },
            "safety_gate_result": normalize_value(dict(self.safety_gate_result)),
            "order_plan": normalize_value(dict(self.order_plan)),
            "candidate_evidence": normalize_value(dict(self.candidate_evidence)),
            "strategy_evidence": normalize_value(dict(self.strategy_evidence)),
            "risk_evidence": normalize_value(dict(self.risk_evidence)),
            "latest_tick_evidence": normalize_value(dict(self.latest_tick_evidence)),
            "sizing": normalize_value(dict(self.sizing)),
            "dry_run_evidence": normalize_value(dict(self.dry_run_evidence)),
            "evidence_json": normalize_value(dict(self.evidence_json)),
            "computed_at": self.computed_at,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }


def evaluate_live_sim_order_plan_eligibility(
    connection: sqlite3.Connection,
    order_plan_id: str,
    settings: Settings | None = None,
    *,
    ignore_live_sim_intent_id: str | None = None,
) -> LiveSimOrderPlanEligibility:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(order_plan_id, "order_plan_id")
    safety_gate = check_live_sim_safety_gate(
        connection,
        resolved_settings,
        purpose="NEW_BUY",
    )
    order_plan = _order_plan_row(connection, normalized_id)
    latest_plan = _latest_order_plan_row(connection, normalized_id)
    evaluation = _entry_timing_evaluation_row(connection, normalized_id)

    reason_codes: list[str] = []
    if order_plan is None:
        if not safety_gate.passed:
            reason_codes.extend(safety_gate.reason_codes)
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_SAFETY_GATE_FAILED.value)
        if not resolved_settings.trading_capabilities.live_sim_order_plan_allowed:
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ROUTING_DISABLED.value)
        if not resolved_settings.live_sim_order_plan_routing_enabled:
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ROUTING_DISABLED.value)
        return _eligibility_result(
            eligible=False,
            status="INELIGIBLE",
            order_plan_id=normalized_id,
            reason_codes=[
                *reason_codes,
                LiveSimReasonCode.ORDER_PLAN_NOT_FOUND.value,
            ],
            safety_gate=safety_gate.to_dict(),
            evidence={"order_plan_id": normalized_id},
        )

    safety_gate = check_live_sim_safety_gate(
        connection,
        resolved_settings,
        purpose="NEW_BUY",
        enforce_daily_loss_limit=True,
        enforce_entry_window=str(order_plan["side"]).upper() == "BUY",
        trade_date=str(order_plan["trade_date"]),
    )
    if not safety_gate.passed:
        reason_codes.extend(safety_gate.reason_codes)
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_SAFETY_GATE_FAILED.value)
    if not resolved_settings.trading_capabilities.live_sim_order_plan_allowed:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ROUTING_DISABLED.value)
    if not resolved_settings.live_sim_order_plan_routing_enabled:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ROUTING_DISABLED.value)

    candidate_id = str(order_plan["candidate_instance_id"])
    code = validate_stock_code(order_plan["code"])
    name = str(order_plan["name"])
    side = str(order_plan["side"]).upper()
    order_plan_reasons = _json_array(order_plan.get("reason_codes"))
    evidence_json = _json_object(order_plan.get("evidence_json"))
    order_type = str(evidence_json.get("order_type", "LIMIT")).upper()

    if latest_plan is None or latest_plan["order_plan_id"] != order_plan["order_plan_id"]:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_NOT_LATEST.value)
    if order_plan["status"] != OrderPlanStatus.PLAN_READY.value:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_NOT_READY.value)
    if not bool(order_plan.get("observe_only")) or not bool(order_plan.get("not_order_intent")):
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_NOT_READY.value)
    if _is_expired(order_plan["expires_at"]):
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_EXPIRED.value)
    if side != "BUY":
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_NOT_BUY.value)
    if order_type == "MARKET" and not resolved_settings.live_sim_order_plan_allow_market_order:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_MARKET_ORDER_NOT_ALLOWED.value)
    elif order_type not in {"LIMIT", "MARKET"}:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_MARKET_ORDER_NOT_ALLOWED.value)
    if _float(order_plan["current_price"]) <= 0 or _float(order_plan["limit_price"]) <= 0:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_INVALID_PRICE.value)
    if _int(order_plan["suggested_quantity"]) < 1:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_INVALID_QUANTITY.value)
    if _float(order_plan["suggested_notional"]) <= 0:
        reason_codes.append(LiveSimReasonCode.INVALID_NOTIONAL.value)
    if (
        _float(order_plan["suggested_notional"])
        < resolved_settings.live_sim_order_plan_min_notional
    ):
        reason_codes.append(LiveSimReasonCode.INVALID_NOTIONAL.value)
    max_order_plan_notional = min(
        resolved_settings.live_sim_order_plan_max_notional,
        resolved_settings.live_sim_max_order_notional,
    )
    if _float(order_plan["suggested_notional"]) > max_order_plan_notional:
        reason_codes.append(LiveSimReasonCode.MAX_ORDER_NOTIONAL_EXCEEDED.value)
    if str(order_plan["setup_type"]).upper() == SetupType.NO_SETUP.value:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ENTRY_TIMING_NOT_ALLOWED.value)
    if str(order_plan["entry_timing_state"]).upper() not in READY_ENTRY_TIMING_STATES:
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_ENTRY_TIMING_NOT_ALLOWED.value)
    if _has_blocked_order_plan_reason(order_plan_reasons):
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_BLOCKED_REASON.value)

    dry_run_evidence = _latest_dry_run_evidence(connection, candidate_id)
    admission = evaluate_trade_admission(
        connection,
        candidate_id,
        AdmissionPolicy(
            name="live_sim_order_plan",
            require_candidate_context_ready=(
                resolved_settings.live_sim_order_plan_require_candidate_context_ready
            ),
            require_candidate_context_row=(
                resolved_settings.live_sim_order_plan_require_candidate_context_ready
            ),
            require_strategy_matched=(
                resolved_settings.live_sim_order_plan_require_strategy_matched
            ),
            require_risk_observe_pass=(
                resolved_settings.live_sim_order_plan_require_risk_observe_pass
            ),
            require_fresh_tick=resolved_settings.live_sim_order_plan_require_fresh_tick,
            stale_tick_sec=resolved_settings.live_sim_order_plan_stale_sec,
            require_dry_run_evidence=(
                resolved_settings.live_sim_order_plan_require_dry_run_evidence
            ),
            require_strategy_observe_only=True,
            require_risk_observe_only=True,
        ),
        fallback_trade_date=str(order_plan["trade_date"]),
        fallback_code=code,
        fallback_name=name,
        dry_run_evidence=dry_run_evidence,
    )
    reason_codes.extend(
        _map_admission_reasons(
            admission.reason_codes,
            ORDER_PLAN_ADMISSION_REASON_MAP,
        )
    )

    strategy_id = admission.strategy_observation_id
    risk_id = admission.risk_observation_id
    latest_tick_evidence = dict(admission.latest_tick_evidence)
    latest_price = 0.0
    if latest_tick_evidence:
        latest_price = _float(latest_tick_evidence.get("price"))
        if latest_price <= 0:
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_INVALID_PRICE.value)
        draft_price = _float(order_plan["current_price"])
        if draft_price > 0:
            drift_pct = abs(latest_price - draft_price) / draft_price * 100
            latest_tick_evidence["draft_price_drift_pct"] = drift_pct
            if drift_pct > resolved_settings.live_sim_order_plan_max_price_drift_pct:
                reason_codes.append(LiveSimReasonCode.ORDER_PLAN_PRICE_DRIFT_EXCEEDED.value)
        limit_price = _live_sim_buy_limit_price_for_plan(
            order_plan,
            resolved_settings,
            latest_price=latest_price,
        )
        if limit_price > 0 and latest_price > limit_price * (
            1 + resolved_settings.live_sim_order_plan_max_price_drift_pct / 100
        ):
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_PRICE_DRIFT_EXCEEDED.value)
        if (
            _int(latest_tick_evidence.get("spread_ticks"))
            > resolved_settings.entry_timing_max_spread_ticks
        ):
            reason_codes.append(LiveSimReasonCode.ORDER_PLAN_BLOCKED_REASON.value)

    lineage_guard = assess_order_plan_lineage(
        connection,
        order_plan,
        evaluation,
        max_age_sec=resolved_settings.live_sim_order_plan_stale_sec,
    )
    if lineage_guard["status"] != "PASS":
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_LINEAGE_INVALID.value)

    existing_intent = find_live_sim_intent_by_order_plan(connection, normalized_id)
    if (
        existing_intent is not None
        and existing_intent.get("live_sim_intent_id") != ignore_live_sim_intent_id
    ):
        reason_codes.append(LiveSimReasonCode.ORDER_PLAN_DUPLICATE_INTENT.value)
    if (
        _recent_active_live_sim_count_for_code(
            connection,
            code,
            resolved_settings,
            exclude_intent_id=ignore_live_sim_intent_id,
        )
        > 0
    ):
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
    if _unresolved_lifecycle_error_count(connection) > 0:
        reason_codes.append(LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value)
    if _daily_order_count(connection, str(order_plan["trade_date"])) >= (
        resolved_settings.live_sim_max_daily_order_count
    ):
        reason_codes.append(LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value)
    daily_notional = _daily_order_notional(connection, str(order_plan["trade_date"]))
    sizing = _sizing(
        order_plan,
        resolved_settings,
        latest_price=latest_price if latest_price > 0 else None,
    )
    if daily_notional + float(sizing["notional"]) > resolved_settings.live_sim_max_daily_notional:
        reason_codes.append(LiveSimReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value)
    if _active_order_count(connection) >= resolved_settings.live_sim_max_active_orders:
        reason_codes.append(LiveSimReasonCode.ACTIVE_ORDER_LIMIT_EXCEEDED.value)
    if _active_position_count(connection) >= resolved_settings.live_sim_max_active_positions:
        reason_codes.append(LiveSimReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value)

    reason_codes = _merge_reasons(reason_codes)
    admission_evidence = admission.to_evidence()
    candidate_evidence = dict(admission.candidate_evidence)
    if admission.candidate_context_evidence:
        candidate_evidence["candidate_context"] = dict(admission.candidate_context_evidence)
    strategy_evidence = dict(admission.strategy_evidence)
    risk_evidence = dict(admission.risk_evidence)
    evidence = {
        "order_plan_id": normalized_id,
        "trade_date": order_plan["trade_date"],
        "code": code,
        "name": name,
        "account_id": resolved_settings.live_sim_account_id,
        "source": "order_plan_pipeline",
        "order_plan_reason_codes": order_plan_reasons,
        "order_type": order_type,
        "latest_plan_order_plan_id": latest_plan["order_plan_id"] if latest_plan else None,
        "entry_timing_evaluation": (
            _evaluation_evidence(evaluation) if evaluation is not None else {}
        ),
        "candidate": candidate_evidence,
        "strategy": strategy_evidence,
        "risk": risk_evidence,
        "latest_tick": latest_tick_evidence,
        "dry_run": dry_run_evidence,
        "sizing": sizing,
        "entry_window": safety_gate.to_dict().get("entry_window", {}),
        "reason_categories": {
            reason: _reason_category(reason) for reason in reason_codes
        },
        "admission_trace": admission_evidence["admission_trace"],
        "pipeline_lineage_guard": lineage_guard,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
    }
    return _eligibility_result(
        eligible=not reason_codes,
        status="ELIGIBLE" if not reason_codes else "INELIGIBLE",
        order_plan_id=normalized_id,
        candidate_instance_id=candidate_id,
        code=code,
        name=name,
        strategy_observation_id=strategy_id,
        risk_observation_id=risk_id,
        entry_timing_evaluation_id=(
            None if evaluation is None else str(evaluation["entry_timing_evaluation_id"])
        ),
        reason_codes=reason_codes,
        safety_gate=safety_gate.to_dict(),
        order_plan=order_plan,
        candidate_evidence=candidate_evidence,
        strategy_evidence=strategy_evidence,
        risk_evidence=risk_evidence,
        latest_tick_evidence=latest_tick_evidence,
        sizing=sizing,
        dry_run_evidence=dry_run_evidence,
        evidence=evidence,
    )


def evaluate_live_sim_order_plan_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> list[LiveSimOrderPlanEligibility]:
    resolved_settings = settings or load_settings()
    rows = select_live_sim_order_plan_candidates(
        connection,
        trade_date=trade_date,
        limit=limit or resolved_settings.live_sim_order_plan_max_plans_per_run,
    )
    return [
        evaluate_live_sim_order_plan_eligibility(
            connection,
            row["order_plan_id"],
            settings=resolved_settings,
        )
        for row in rows
    ]


def select_live_sim_order_plan_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    clauses = ["status = ?", "expires_at > ?"]
    params: list[Any] = [OrderPlanStatus.PLAN_READY.value, datetime_to_wire(utc_now())]
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM order_plan_drafts_latest
        WHERE {" AND ".join(clauses)}
        ORDER BY
            COALESCE(priority_score, 0) DESC,
            created_at DESC,
            order_plan_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_order_plan_dict(row) for row in rows]


def record_live_sim_order_plan_rejection(
    connection: sqlite3.Connection,
    eligibility: LiveSimOrderPlanEligibility,
    *,
    account_id: str | None,
    source: str = "order_plan_pipeline",
) -> None:
    evidence = eligibility.to_dict()
    evidence["source"] = source
    _save_rejection(
        connection,
        candidate_instance_id=eligibility.candidate_instance_id,
        strategy_observation_id=eligibility.strategy_observation_id,
        risk_observation_id=eligibility.risk_observation_id,
        trade_date=eligibility.order_plan.get("trade_date"),
        account_id=account_id,
        code=eligibility.code,
        reason_codes=list(eligibility.reason_codes),
        evidence=evidence,
    )


def find_live_sim_intent_by_order_plan(
    connection: sqlite3.Connection,
    order_plan_id: str,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(order_plan_id, "order_plan_id")
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_intents
        WHERE order_plan_id = ?
        LIMIT 1
        """,
        (normalized_id,),
    ).fetchone()
    return None if row is None else _intent_row_to_dict(row)


def find_live_sim_order_by_intent(
    connection: sqlite3.Connection,
    live_sim_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_orders
        WHERE live_sim_intent_id = ?
        ORDER BY created_at DESC, live_sim_order_id DESC
        LIMIT 1
        """,
        (require_non_empty_str(live_sim_intent_id, "live_sim_intent_id"),),
    ).fetchone()
    if row is None:
        return None
    return _order_row_to_dict(row)


def _eligibility_result(
    *,
    eligible: bool,
    status: str,
    order_plan_id: str,
    reason_codes: Sequence[str],
    safety_gate: Mapping[str, Any],
    candidate_instance_id: str | None = None,
    code: str | None = None,
    name: str | None = None,
    strategy_observation_id: str | None = None,
    risk_observation_id: str | None = None,
    entry_timing_evaluation_id: str | None = None,
    order_plan: Mapping[str, Any] | None = None,
    candidate_evidence: Mapping[str, Any] | None = None,
    strategy_evidence: Mapping[str, Any] | None = None,
    risk_evidence: Mapping[str, Any] | None = None,
    latest_tick_evidence: Mapping[str, Any] | None = None,
    sizing: Mapping[str, Any] | None = None,
    dry_run_evidence: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> LiveSimOrderPlanEligibility:
    merged = _merge_reasons(list(reason_codes))
    return LiveSimOrderPlanEligibility(
        eligible=eligible,
        status=status,
        order_plan_id=order_plan_id,
        candidate_instance_id=candidate_instance_id,
        code=code,
        name=name,
        strategy_observation_id=strategy_observation_id,
        risk_observation_id=risk_observation_id,
        entry_timing_evaluation_id=entry_timing_evaluation_id,
        reason_codes=merged,
        safety_gate_result=safety_gate,
        order_plan=order_plan or {},
        candidate_evidence=candidate_evidence or {},
        strategy_evidence=strategy_evidence or {},
        risk_evidence=risk_evidence or {},
        latest_tick_evidence=latest_tick_evidence or {},
        sizing=sizing or {},
        dry_run_evidence=dry_run_evidence or {},
        evidence_json=evidence or {},
        computed_at=datetime_to_wire(utc_now()),
    )


def _order_plan_row(connection: sqlite3.Connection, order_plan_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    return None if row is None else _order_plan_dict(row)


def _latest_order_plan_row(
    connection: sqlite3.Connection,
    order_plan_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM order_plan_drafts_latest WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    return None if row is None else _order_plan_dict(row)


def _entry_timing_evaluation_row(
    connection: sqlite3.Connection,
    order_plan_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM entry_timing_evaluations
        WHERE order_plan_id = ?
        ORDER BY evaluated_at DESC, entry_timing_evaluation_id DESC
        LIMIT 1
        """,
        (order_plan_id,),
    ).fetchone()


def _order_plan_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["observe_only"] = bool(item["observe_only"])
    item["not_order_intent"] = bool(item["not_order_intent"])
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    return item


def _intent_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_sent"] = bool(item["broker_order_sent"])
    return item


def _order_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    return item


def _evaluation_evidence(evaluation: sqlite3.Row) -> dict[str, Any]:
    return {
        "entry_timing_evaluation_id": evaluation["entry_timing_evaluation_id"],
        "setup_type": evaluation["setup_type"],
        "entry_timing_state": evaluation["entry_timing_state"],
        "price_location_state": evaluation["price_location_state"],
        "status": evaluation["status"],
        "reason_codes": _json_array(evaluation["reason_codes_json"]),
        "evidence_json": _json_object(evaluation["evidence_json"]),
        "observe_only": bool(evaluation["observe_only"]),
        "not_order_intent": bool(evaluation["not_order_intent"]),
    }


def _sizing(
    order_plan: Mapping[str, Any],
    settings: Settings,
    *,
    latest_price: float | None = None,
) -> dict[str, Any]:
    limit_price = _live_sim_buy_limit_price_for_plan(
        order_plan,
        settings,
        latest_price=latest_price,
    )
    order_plan_limit_price = _float(order_plan["limit_price"])
    max_notional = min(
        settings.live_sim_order_plan_max_notional,
        settings.live_sim_max_order_notional,
    )
    planned_quantity = _int(order_plan["suggested_quantity"])
    planned_notional = _float(order_plan["suggested_notional"])
    max_quantity = int(max_notional // limit_price) if limit_price > 0 else 0
    quantity = max(min(planned_quantity, max_quantity), 0)
    notional = float(quantity * limit_price)
    return {
        "quantity": quantity,
        "notional": notional,
        "limit_price": limit_price,
        "order_plan_limit_price": order_plan_limit_price,
        "buy_price_offset_ticks": settings.live_sim_buy_price_offset_ticks,
        "price_policy": "LATEST_PRICE_PLUS_KRX_TICKS",
        "planned_quantity": planned_quantity,
        "planned_notional": planned_notional,
        "min_notional": settings.live_sim_order_plan_min_notional,
        "default_notional": settings.live_sim_order_plan_default_notional,
        "max_order_plan_notional": settings.live_sim_order_plan_max_notional,
        "max_live_sim_order_notional": settings.live_sim_max_order_notional,
    }


def _live_sim_buy_limit_price_for_plan(
    order_plan: Mapping[str, Any],
    settings: Settings,
    *,
    latest_price: float | None,
) -> float:
    base_price = latest_price if latest_price is not None and latest_price > 0 else _float(
        order_plan["limit_price"]
    )
    if base_price <= 0:
        return 0.0
    try:
        return float(add_ticks(base_price, settings.live_sim_buy_price_offset_ticks))
    except ValueError:
        return 0.0


def _has_blocked_order_plan_reason(reasons: Sequence[str]) -> bool:
    for reason in reasons:
        normalized = str(reason).upper()
        if normalized.startswith("RISK_"):
            return True
        if any(token in normalized for token in BLOCKED_ORDER_PLAN_REASON_TOKENS):
            return True
    return False


def _is_expired(value: object) -> bool:
    try:
        return parse_timestamp(value, "expires_at") <= utc_now()
    except ValueError:
        return True


def _reason_category(reason: str) -> str:
    normalized = str(reason).upper()
    if "DUPLICATE" in normalized:
        return "DUPLICATE"
    if "LIMIT" in normalized or "NOTIONAL" in normalized or "QUANTITY" in normalized:
        return "LIMIT"
    if normalized.startswith("ORDER_PLAN_STRATEGY") or "STRATEGY" in normalized:
        return "STRATEGY"
    if normalized.startswith("ORDER_PLAN_RISK") or "RISK" in normalized:
        return "RISK"
    if "TICK" in normalized or "STALE" in normalized or "DATA" in normalized:
        return "DATA"
    if "WINDOW" in normalized or "SESSION" in normalized:
        return "SESSION"
    if "DISABLED" in normalized or "ROUTING" in normalized or "AUTO_QUEUE" in normalized:
        return "CONFIG"
    if (
        "LIVE_SIM" in normalized
        or "LIVE_REAL" in normalized
        or "ACCOUNT" in normalized
        or "GATEWAY" in normalized
        or "BROKER" in normalized
        or "SERVER" in normalized
        or "KILL_SWITCH" in normalized
    ):
        return "SAFETY"
    return "ORDER_PLAN"


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


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
    if not isinstance(loaded, Sequence) or isinstance(loaded, str):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


def _map_admission_reasons(
    reasons: Sequence[str],
    reason_map: Mapping[str, str],
) -> list[str]:
    return [reason_map.get(str(reason).upper(), str(reason).upper()) for reason in reasons]


def _float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
