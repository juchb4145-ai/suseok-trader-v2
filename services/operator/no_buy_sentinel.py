from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_value,
    parse_timestamp,
    utc_now,
)
from storage.event_store import get_gateway_status_values

from services.ai_advisory.storage import build_status as build_ai_advisory_status
from services.candidate_service import list_candidates
from services.config import Settings, TradingProfile, candidate_timezone, load_settings
from services.entry_timing.service import (
    list_entry_timing_evaluations,
    list_latest_order_plan_drafts,
)
from services.live_sim.live_sim_service import (
    get_latest_live_sim_reconcile,
    get_live_sim_status,
)
from services.live_sim.order_plan_eligibility import evaluate_live_sim_order_plan_eligibility
from services.operator.models import (
    BlockType,
    NoBuySentinelSnapshot,
    NoBuyStatus,
    StageCategory,
)
from services.operator.reason_classifier import (
    aggregate_reason_summary,
    primary_classification,
    summarize_classifications,
)
from services.theme_leadership import rebuild_theme_leadership

SYSTEM_BLOCK_STATUSES = {
    NoBuyStatus.LIVE_SIM_SAFETY_BLOCK,
    NoBuyStatus.RECONCILE_BLOCK,
    NoBuyStatus.DUPLICATE_OR_POSITION_BLOCK,
    NoBuyStatus.CONFIG_DISABLED,
    NoBuyStatus.GATEWAY_UNAVAILABLE,
    NoBuyStatus.MIXED_BLOCKS,
}


def build_no_buy_sentinel_snapshot(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    manual: bool = False,
    limit: int | None = None,
    include_ai: bool | None = None,
    include_debug: bool = False,
    write_snapshot: bool | None = None,
) -> NoBuySentinelSnapshot:
    resolved_settings = settings or load_settings()
    resolved_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    evaluated_at = datetime_to_wire(utc_now())
    bounded_limit = _bounded_limit(
        limit if limit is not None else resolved_settings.no_buy_sentinel_top_near_miss_limit
    )
    should_include_ai = (
        resolved_settings.no_buy_sentinel_include_ai if include_ai is None else include_ai
    )
    market_session = _market_session(resolved_settings, manual=manual)

    theme_result = rebuild_theme_leadership(
        connection,
        trade_date=resolved_trade_date,
        write_candidate_sources=False,
        settings=resolved_settings,
    )
    watchset_items = [item.to_dict() for item in theme_result.watchset.items]
    theme_snapshots = [item.to_dict(include_members=False) for item in theme_result.snapshots]
    candidates = list_candidates(
        connection,
        trade_date=resolved_trade_date,
        active_only=True,
        limit=500,
    )
    order_plans = list_latest_order_plan_drafts(
        connection,
        trade_date=resolved_trade_date,
        limit=500,
    )
    entry_evaluations = list_entry_timing_evaluations(
        connection,
        trade_date=resolved_trade_date,
        limit=500,
    )

    intent_count = _count_live_sim_intents(connection, resolved_trade_date)
    order_count = _count_live_sim_orders(connection, resolved_trade_date)
    command_count = _count_live_sim_commands(
        connection,
        lookback_minutes=resolved_settings.no_buy_sentinel_lookback_minutes,
    )
    live_sim_status = get_live_sim_status(connection, resolved_settings)
    latest_reconcile = (
        get_latest_live_sim_reconcile(connection)
        if resolved_settings.no_buy_sentinel_include_reconcile
        else None
    )
    gateway_summary = _gateway_summary(connection, resolved_settings)
    config_summary = (
        _config_summary(resolved_settings)
        if resolved_settings.no_buy_sentinel_include_config
        else _config_summary_disabled(resolved_settings)
    )
    ai_summary = (
        _ai_summary(connection, resolved_settings, resolved_trade_date)
        if should_include_ai
        else _empty_ai_summary()
    )

    eligibility_by_order_plan: dict[str, dict[str, Any]] = {}
    eligibility_reason_codes: list[str] = []
    buy_eligible_count = 0
    for plan in order_plans:
        order_plan_id = str(plan.get("order_plan_id") or "")
        if not order_plan_id:
            continue
        try:
            eligibility = evaluate_live_sim_order_plan_eligibility(
                connection,
                order_plan_id,
                settings=resolved_settings,
            ).to_dict()
        except (ValueError, sqlite3.Error) as exc:
            eligibility = {
                "eligible": False,
                "status": "ERROR",
                "order_plan_id": order_plan_id,
                "reason_codes": ["LIVE_SIM_ELIGIBILITY_ERROR"],
                "error_message": str(exc),
            }
        eligibility_by_order_plan[order_plan_id] = eligibility
        if eligibility.get("eligible"):
            buy_eligible_count += 1
        eligibility_reason_codes.extend(_as_reason_list(eligibility.get("reason_codes")))

    plan_ready_count = _count_status(order_plans, "PLAN_READY")
    wait_retry_count = _count_status(order_plans, "WAIT_RETRY")
    data_wait_count = _count_status(order_plans, "DATA_WAIT")
    ai_selected_codes = set(str(code) for code in ai_summary.get("selected_codes", []))
    top_near_miss = _top_near_misses(
        order_plans=order_plans,
        candidates=candidates,
        watchset_items=watchset_items,
        eligibility_by_order_plan=eligibility_by_order_plan,
        ai_scores=_ai_scores_by_identity(ai_summary),
        ai_selected_codes=ai_selected_codes,
        limit=bounded_limit,
    )

    all_reason_codes: list[str] = [
        *eligibility_reason_codes,
        *[reason for item in top_near_miss for reason in _as_reason_list(item.get("reason_codes"))],
    ]
    all_reason_codes.extend(_system_reason_codes(gateway_summary, config_summary, latest_reconcile))
    if ai_summary.get("classification") in {"AI_NO_TRADE", "AI_UNAVAILABLE"}:
        all_reason_codes.append(str(ai_summary["classification"]))

    stage_summary = _stage_summary(
        theme_result_status=theme_result.status,
        theme_snapshots=theme_snapshots,
        watchset_count=len(watchset_items),
        candidates=candidates,
        order_plans=order_plans,
        entry_evaluations=entry_evaluations,
        live_sim_status=live_sim_status,
        latest_reconcile=latest_reconcile,
        top_near_miss=top_near_miss,
        plan_ready_count=plan_ready_count,
        wait_retry_count=wait_retry_count,
        data_wait_count=data_wait_count,
        buy_eligible_count=buy_eligible_count,
    )
    reason_summary = aggregate_reason_summary(all_reason_codes)
    status = _resolve_status(
        intent_count=_int(intent_count),
        order_count=_int(order_count),
        command_count=_int(command_count),
        plan_ready_count=plan_ready_count,
        buy_eligible_count=buy_eligible_count,
        wait_retry_count=wait_retry_count,
        data_wait_count=data_wait_count,
        candidate_count=len(candidates),
        watchset_count=len(watchset_items),
        theme_snapshots=theme_snapshots,
        top_near_miss=top_near_miss,
        gateway_summary=gateway_summary,
        config_summary=config_summary,
        latest_reconcile=latest_reconcile,
        ai_summary=ai_summary,
    )
    if status in SYSTEM_BLOCK_STATUSES and ai_summary.get("selected_count", 0) > 0:
        ai_summary["classification"] = "SYSTEM_BLOCK_WITH_AI_INTEREST"
    if status is NoBuyStatus.CONFIG_DISABLED and ai_summary.get("selected_count", 0) > 0:
        ai_summary["classification"] = "CONFIG_DISABLED_WITH_AI_INTEREST"

    no_buy_detected = status is not NoBuyStatus.OK_TRADING_ACTIVITY
    system_summary = {
        "gateway": gateway_summary,
        "config": config_summary,
        "live_sim": _compact_live_sim_status(live_sim_status),
        "reconcile": latest_reconcile,
        "market_session": market_session,
        "manual": manual,
        "diagnostic_ready": manual or market_session in {"OPEN_DIAGNOSTIC_READY", "CLOSED"},
    }
    if include_debug:
        system_summary["debug"] = {
            "theme_snapshot_count": len(theme_snapshots),
            "entry_evaluation_count": len(entry_evaluations),
            "eligibility_count": len(eligibility_by_order_plan),
        }

    snapshot = NoBuySentinelSnapshot(
        snapshot_id=new_message_id("no_buy_sentinel"),
        trade_date=resolved_trade_date,
        evaluated_at=evaluated_at,
        market_session=market_session,
        status=status,
        no_buy_detected=no_buy_detected,
        intent_count=intent_count,
        order_count=order_count,
        command_count=command_count,
        plan_ready_count=plan_ready_count,
        buy_eligible_count=buy_eligible_count,
        ai_selected_count=int(ai_summary.get("selected_count", 0)),
        top_near_miss=top_near_miss,
        stage_summary=stage_summary,
        reason_summary=reason_summary,
        ai_summary=ai_summary,
        system_summary=system_summary,
        operator_checklist=_operator_checklist(status, system_summary, ai_summary),
        created_at=evaluated_at,
    )
    should_write = (
        resolved_settings.no_buy_sentinel_write_snapshots
        if write_snapshot is None
        else write_snapshot
    )
    if should_write:
        _save_snapshot(connection, snapshot)
    return snapshot


def rebuild_no_buy_sentinel_snapshot(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    limit: int | None = None,
    include_ai: bool | None = None,
    include_debug: bool = False,
) -> NoBuySentinelSnapshot:
    return build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        trade_date=trade_date,
        manual=True,
        limit=limit,
        include_ai=include_ai,
        include_debug=include_debug,
        write_snapshot=True,
    )


def get_latest_no_buy_sentinel_snapshot(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
) -> dict[str, Any] | None:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(str(trade_date))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    row = connection.execute(
        f"""
        SELECT *
        FROM no_buy_sentinel_snapshots
        {where_sql}
        ORDER BY created_at DESC, snapshot_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return None if row is None else _snapshot_row_to_dict(row)


def list_no_buy_sentinel_snapshots(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(str(trade_date))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    rows = connection.execute(
        f"""
        SELECT *
        FROM no_buy_sentinel_snapshots
        {where_sql}
        ORDER BY created_at DESC, snapshot_id DESC
        LIMIT ?
        """,
        (*params, _bounded_limit(limit)),
    ).fetchall()
    return [_snapshot_row_to_dict(row) for row in rows]


def _top_near_misses(
    *,
    order_plans: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    watchset_items: Sequence[Mapping[str, Any]],
    eligibility_by_order_plan: Mapping[str, Mapping[str, Any]],
    ai_scores: Mapping[str, Mapping[str, Any]],
    ai_selected_codes: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    planned_candidate_ids = set()
    planned_codes = set()
    for plan in order_plans:
        order_plan_id = str(plan.get("order_plan_id") or "")
        candidate_id = str(plan.get("candidate_instance_id") or "")
        code = str(plan.get("code") or "")
        planned_candidate_ids.add(candidate_id)
        planned_codes.add(code)
        eligibility = dict(eligibility_by_order_plan.get(order_plan_id, {}))
        reason_codes = _as_reason_list(plan.get("reason_codes"))
        reason_codes.extend(_as_reason_list(eligibility.get("reason_codes")))
        ai_score = _ai_score_for(plan, ai_scores)
        ai_selected = code in ai_selected_codes or bool(ai_score.get("selected"))
        if ai_selected and eligibility and not eligibility.get("eligible", False):
            reason_codes.append("SYSTEM_BLOCK_WITH_AI_INTEREST")
        classification = primary_classification(reason_codes or [plan.get("status")])
        item = {
            "code": code,
            "name": plan.get("name"),
            "candidate_instance_id": candidate_id,
            "order_plan_id": order_plan_id,
            "theme_name": plan.get("theme_name"),
            "theme_state": plan.get("theme_state"),
            "stock_role": plan.get("stock_role"),
            "entry_timing_state": plan.get("entry_timing_state"),
            "order_plan_status": plan.get("status"),
            "live_sim_eligibility_status": eligibility.get("status"),
            "ai_score": ai_score.get("score"),
            "ai_confidence": ai_score.get("confidence"),
            "ai_selected": bool(ai_selected),
            "primary_block_stage": classification.stage.value,
            "primary_block_type": classification.block_type.value,
            "reason_codes": _dedupe(reason_codes or [str(plan.get("status") or "UNKNOWN")]),
            "operator_hint": classification.operator_hint,
            "not_buy_recommendation": True,
        }
        rows.append(item)

    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_instance_id") or "")
        code = str(candidate.get("code") or "")
        if candidate_id in planned_candidate_ids:
            continue
        reason_codes = _dedupe(
            [
                "CANDIDATE_WITHOUT_ORDER_PLAN",
                *(_as_reason_list(candidate.get("reason_codes"))),
            ]
        )
        classification = primary_classification(reason_codes)
        rows.append(
            {
                "code": code,
                "name": candidate.get("name"),
                "candidate_instance_id": candidate_id,
                "order_plan_id": None,
                "theme_name": candidate.get("theme_name"),
                "theme_state": candidate.get("theme_state"),
                "stock_role": candidate.get("theme_role"),
                "entry_timing_state": None,
                "order_plan_status": None,
                "live_sim_eligibility_status": None,
                "ai_score": _ai_score_for(candidate, ai_scores).get("score"),
                "ai_confidence": _ai_score_for(candidate, ai_scores).get("confidence"),
                "ai_selected": code in ai_selected_codes,
                "primary_block_stage": classification.stage.value,
                "primary_block_type": classification.block_type.value,
                "reason_codes": reason_codes,
                "operator_hint": classification.operator_hint,
                "not_buy_recommendation": True,
            }
        )

    for watch in watchset_items:
        code = str(watch.get("code") or "")
        if code in planned_codes:
            continue
        reason_codes = _dedupe(
            [
                "WATCHSET_WITHOUT_ORDER_PLAN",
                *_as_reason_list(watch.get("reason_codes")),
            ]
        )
        classification = primary_classification(reason_codes)
        rows.append(
            {
                "code": code,
                "name": watch.get("name"),
                "candidate_instance_id": None,
                "order_plan_id": None,
                "theme_name": watch.get("theme_name"),
                "theme_state": watch.get("theme_state"),
                "stock_role": watch.get("stock_role"),
                "entry_timing_state": None,
                "order_plan_status": None,
                "live_sim_eligibility_status": None,
                "ai_score": _ai_score_for(watch, ai_scores).get("score"),
                "ai_confidence": _ai_score_for(watch, ai_scores).get("confidence"),
                "ai_selected": code in ai_selected_codes,
                "primary_block_stage": classification.stage.value,
                "primary_block_type": classification.block_type.value,
                "reason_codes": reason_codes,
                "operator_hint": classification.operator_hint,
                "not_buy_recommendation": True,
            }
        )

    rows.sort(key=_near_miss_sort_key)
    return rows[: _bounded_limit(limit)]


def _resolve_status(
    *,
    intent_count: int,
    order_count: int,
    command_count: int,
    plan_ready_count: int,
    buy_eligible_count: int,
    wait_retry_count: int,
    data_wait_count: int,
    candidate_count: int,
    watchset_count: int,
    theme_snapshots: Sequence[Mapping[str, Any]],
    top_near_miss: Sequence[Mapping[str, Any]],
    gateway_summary: Mapping[str, Any],
    config_summary: Mapping[str, Any],
    latest_reconcile: Mapping[str, Any] | None,
    ai_summary: Mapping[str, Any],
) -> NoBuyStatus:
    if intent_count > 0 or order_count > 0 or command_count > 0:
        return NoBuyStatus.OK_TRADING_ACTIVITY
    if plan_ready_count > 0 and bool(gateway_summary.get("unavailable")):
        return NoBuyStatus.GATEWAY_UNAVAILABLE
    if plan_ready_count > 0 and bool(config_summary.get("critical_disabled")):
        return NoBuyStatus.CONFIG_DISABLED
    if plan_ready_count > 0 and _reconcile_blocks(latest_reconcile):
        return NoBuyStatus.RECONCILE_BLOCK
    if plan_ready_count > 0 and (
        _near_miss_has_stage(top_near_miss, StageCategory.DUPLICATE_POSITION)
        or _near_miss_has_reason_token(
            top_near_miss,
            (
                "DUPLICATE",
                "OPEN_POSITION",
                "POSITION_EXISTS",
                "ACTIVE_POSITION_LIMIT",
            ),
        )
    ):
        return NoBuyStatus.DUPLICATE_OR_POSITION_BLOCK
    if plan_ready_count > 0 and _near_miss_has_stage(top_near_miss, StageCategory.LIVE_SIM_SAFETY):
        return NoBuyStatus.LIVE_SIM_SAFETY_BLOCK
    if plan_ready_count > 0 and _near_miss_has_stage(top_near_miss, StageCategory.LIMIT):
        return NoBuyStatus.LIVE_SIM_SAFETY_BLOCK
    if plan_ready_count > 0 and buy_eligible_count == 0:
        stages = {
            str(item.get("primary_block_stage") or "")
            for item in top_near_miss
            if item.get("order_plan_status") == "PLAN_READY"
        }
        return NoBuyStatus.MIXED_BLOCKS if len(stages) > 1 else NoBuyStatus.ORDER_PLAN_NOT_READY
    if plan_ready_count > 0 and ai_summary.get("classification") == "AI_NO_TRADE":
        return NoBuyStatus.AI_NO_TRADE
    if plan_ready_count > 0 and buy_eligible_count > 0:
        if ai_summary.get("classification") == "AI_NO_TRADE":
            return NoBuyStatus.AI_NO_TRADE
        return NoBuyStatus.UNKNOWN
    if wait_retry_count > 0 or data_wait_count > 0:
        return NoBuyStatus.ENTRY_TIMING_WAIT
    if candidate_count > 0 or watchset_count > 0:
        return NoBuyStatus.ORDER_PLAN_NOT_READY
    if theme_snapshots and all(str(item.get("state")) == "DATA_WAIT" for item in theme_snapshots):
        return NoBuyStatus.THEME_DATA_WAIT
    if ai_summary.get("classification") == "AI_NO_TRADE":
        return NoBuyStatus.AI_NO_TRADE
    if candidate_count == 0 and watchset_count == 0:
        return NoBuyStatus.NO_CANDIDATE
    return NoBuyStatus.UNKNOWN


def _stage_summary(
    *,
    theme_result_status: str,
    theme_snapshots: Sequence[Mapping[str, Any]],
    watchset_count: int,
    candidates: Sequence[Mapping[str, Any]],
    order_plans: Sequence[Mapping[str, Any]],
    entry_evaluations: Sequence[Mapping[str, Any]],
    live_sim_status: Mapping[str, Any],
    latest_reconcile: Mapping[str, Any] | None,
    top_near_miss: Sequence[Mapping[str, Any]],
    plan_ready_count: int,
    wait_retry_count: int,
    data_wait_count: int,
    buy_eligible_count: int,
) -> dict[str, Any]:
    theme_state_counts = Counter(str(item.get("state") or "UNKNOWN") for item in theme_snapshots)
    order_plan_status_counts = Counter(str(item.get("status") or "UNKNOWN") for item in order_plans)
    entry_timing_state_counts = Counter(
        str(item.get("entry_timing_state") or "UNKNOWN") for item in entry_evaluations
    )
    return {
        "theme": {
            "status": theme_result_status,
            "snapshot_count": len(theme_snapshots),
            "state_counts": dict(theme_state_counts),
            "data_wait_count": int(theme_state_counts.get("DATA_WAIT", 0)),
            "watchset_count": watchset_count,
        },
        "candidate": {
            "active_count": len(candidates),
            "state_counts": dict(
                Counter(str(item.get("state") or "UNKNOWN") for item in candidates)
            ),
        },
        "entry_timing": {
            "evaluation_count": len(entry_evaluations),
            "entry_timing_state_counts": dict(entry_timing_state_counts),
            "plan_ready_count": plan_ready_count,
            "wait_retry_count": wait_retry_count,
            "data_wait_count": data_wait_count,
        },
        "order_plan": {
            "latest_count": len(order_plans),
            "status_counts": dict(order_plan_status_counts),
            "buy_eligible_count": buy_eligible_count,
        },
        "live_sim_safety": {
            "safety_gate": live_sim_status.get("safety_gate", {}),
            "kill_switch": live_sim_status.get("kill_switch"),
            "rejection_count": live_sim_status.get("rejection_count", 0),
        },
        "reconcile": {
            "status": None if latest_reconcile is None else latest_reconcile.get("status"),
            "blocking_new_buy": bool(latest_reconcile and latest_reconcile.get("blocking_new_buy")),
            "mismatch_count": (
                0 if latest_reconcile is None else latest_reconcile.get("mismatch_count", 0)
            ),
        },
        "near_miss": summarize_classifications(top_near_miss),
    }


def _ai_summary(
    connection: sqlite3.Connection,
    settings: Settings,
    trade_date: str,
) -> dict[str, Any]:
    latest = _latest_ai_run(connection, trade_date)
    status = build_ai_advisory_status(connection, settings=settings)
    invalid_error_count = int(status.get("invalid_schema_error_count", 0))
    error_count = int(status.get("error_count", 0))
    if latest is None:
        return _empty_ai_summary() | {
            "provider": status.get("provider"),
            "model": status.get("model"),
            "invalid_schema_error_count": invalid_error_count,
            "error_count": error_count,
        }
    scores = _ai_scores_for_run(connection, str(latest["run_id"]))
    selected_codes = [str(item["code"]) for item in scores if item.get("selected")]
    top_score = max(
        (item.get("score") for item in scores if item.get("score") is not None),
        default=None,
    )
    top_confidence = max(
        (item.get("confidence") for item in scores if item.get("confidence") is not None),
        default=None,
    )
    run_status = str(latest.get("status") or "UNKNOWN").upper()
    classification = "NONE"
    if run_status in {"TIMEOUT", "INVALID_SCHEMA", "FAILED", "ERROR"}:
        classification = "AI_UNAVAILABLE"
    elif int(latest.get("selected_count") or 0) == 0 and latest.get("no_trade_reason"):
        classification = "AI_NO_TRADE"
    return {
        "latest_run_status": run_status,
        "run_id": latest.get("run_id"),
        "provider": latest.get("provider"),
        "model": latest.get("model"),
        "selected_count": int(latest.get("selected_count") or 0),
        "selected_codes": selected_codes,
        "no_trade_reason": latest.get("no_trade_reason"),
        "summary": latest.get("summary"),
        "top_score": top_score,
        "top_confidence": top_confidence,
        "invalid_schema_error_count": invalid_error_count,
        "error_count": error_count,
        "fallback_used": bool(latest.get("fallback_provider")),
        "fallback_provider": latest.get("fallback_provider"),
        "external_call_attempted": bool(latest.get("external_call_attempted")),
        "latency_ms": latest.get("latency_ms"),
        "classification": classification,
        "scores": scores,
        "advisory_only": True,
        "no_order_side_effects": True,
    }


def _empty_ai_summary() -> dict[str, Any]:
    return {
        "latest_run_status": None,
        "provider": None,
        "model": None,
        "selected_count": 0,
        "selected_codes": [],
        "no_trade_reason": None,
        "summary": None,
        "top_score": None,
        "top_confidence": None,
        "invalid_schema_error_count": 0,
        "error_count": 0,
        "fallback_used": False,
        "external_call_attempted": False,
        "latency_ms": None,
        "classification": "NONE",
        "scores": [],
        "advisory_only": True,
        "no_order_side_effects": True,
    }


def _gateway_summary(connection: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    values = get_gateway_status_values(connection)
    heartbeat = values.get("last_heartbeat_at")
    heartbeat_age_sec = _age_seconds(heartbeat)
    max_age = max(settings.no_buy_sentinel_lookback_minutes * 60, 1)
    orderable = str(values.get("gateway_orderable", "")).lower() == "true"
    queue_healthy = str(values.get("command_queue_healthy", "")).lower() == "true"
    stale = heartbeat is None or heartbeat_age_sec > max_age
    unavailable = stale or not orderable or not queue_healthy
    return {
        "last_heartbeat_at": heartbeat,
        "heartbeat_age_sec": None if heartbeat is None else heartbeat_age_sec,
        "heartbeat_stale": stale,
        "gateway_orderable": orderable,
        "command_queue_healthy": queue_healthy,
        "account_mode": values.get("account_mode"),
        "broker_env": values.get("broker_env"),
        "server_mode": values.get("server_mode"),
        "unavailable": unavailable,
    }


def _config_summary(settings: Settings) -> dict[str, Any]:
    disabled_flags = []
    if not settings.no_buy_sentinel_enabled:
        disabled_flags.append("NO_BUY_SENTINEL_ENABLED")
    if settings.trading_profile is not TradingProfile.LIVE_SIM_PILOT:
        disabled_flags.append("TRADING_PROFILE_LIVE_SIM_PILOT")
    if not settings.live_sim_enabled:
        disabled_flags.append("LIVE_SIM_ENABLED")
    if not settings.live_sim_pilot_pipeline_enabled:
        disabled_flags.append("LIVE_SIM_PILOT_PIPELINE_ENABLED")
    if not settings.live_sim_order_plan_routing_enabled:
        disabled_flags.append("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED")
    if not settings.live_sim_gateway_command_enabled:
        disabled_flags.append("LIVE_SIM_GATEWAY_COMMAND_ENABLED")
    return {
        "no_buy_sentinel_enabled": settings.no_buy_sentinel_enabled,
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "live_sim_enabled": settings.live_sim_enabled,
        "live_sim_pilot_pipeline_enabled": settings.live_sim_pilot_pipeline_enabled,
        "live_sim_order_plan_routing_enabled": settings.live_sim_order_plan_routing_enabled,
        "live_sim_gateway_command_enabled": settings.live_sim_gateway_command_enabled,
        "live_sim_kill_switch": settings.live_sim_kill_switch,
        "live_real_allowed": False,
        "disabled_flags": disabled_flags,
        "critical_disabled": bool(disabled_flags),
    }


def _config_summary_disabled(settings: Settings) -> dict[str, Any]:
    return {
        "no_buy_sentinel_enabled": settings.no_buy_sentinel_enabled,
        "include_config": False,
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "live_real_allowed": False,
        "disabled_flags": [],
        "critical_disabled": False,
    }


def _operator_checklist(
    status: NoBuyStatus,
    system_summary: Mapping[str, Any],
    ai_summary: Mapping[str, Any],
) -> list[str]:
    base = ["이 화면은 진단 전용입니다. 매수/매도/취소 실행 버튼이 없습니다."]
    match status:
        case NoBuyStatus.OK_TRADING_ACTIVITY:
            base.append("오늘 LIVE_SIM intent/order/command 활동이 있어 무매수 상태가 아닙니다.")
        case NoBuyStatus.THEME_DATA_WAIT:
            base.append("ThemeLeadership DATA_WAIT 원인을 확인합니다.")
        case NoBuyStatus.ENTRY_TIMING_WAIT:
            base.append("EntryTiming WAIT_RETRY/DATA_WAIT 후보의 reason code를 확인합니다.")
        case NoBuyStatus.LIVE_SIM_SAFETY_BLOCK:
            base.append(
                "LIVE_SIM safety gate, kill switch, account/server/broker mode를 확인합니다."
            )
        case NoBuyStatus.RECONCILE_BLOCK:
            base.append("최신 reconcile snapshot의 mismatch와 blocking_new_buy 값을 확인합니다.")
        case NoBuyStatus.DUPLICATE_OR_POSITION_BLOCK:
            base.append("동일 종목 open order/position/duplicate cooldown 상태를 확인합니다.")
        case NoBuyStatus.CONFIG_DISABLED:
            flags = ", ".join(system_summary.get("config", {}).get("disabled_flags", []))
            base.append(f"설정 flag 비활성 항목을 확인합니다: {flags or '-'}")
        case NoBuyStatus.GATEWAY_UNAVAILABLE:
            base.append("Gateway heartbeat/orderable/command queue 상태를 확인합니다.")
        case NoBuyStatus.AI_NO_TRADE:
            base.append("AI 관망 사유를 확인하되 시스템 안전 판단과 분리해서 봅니다.")
        case NoBuyStatus.NO_CANDIDATE:
            base.append("watchset과 candidate source 입력이 비어 있는지 확인합니다.")
        case _:
            base.append("top near-miss의 primary block stage와 원본 reason code를 확인합니다.")
    if ai_summary.get("classification") == "SYSTEM_BLOCK_WITH_AI_INTEREST":
        base.append("AI selected가 있어도 시스템 safety/config/reconcile block이 우선입니다.")
    if ai_summary.get("classification") == "AI_UNAVAILABLE":
        base.append("AI 실패/timeout/invalid schema는 시스템 무매수 원인으로 보지 않습니다.")
    return base


def _save_snapshot(connection: sqlite3.Connection, snapshot: NoBuySentinelSnapshot) -> None:
    payload = snapshot.to_dict()
    try:
        connection.execute(
            """
            INSERT INTO no_buy_sentinel_snapshots (
                snapshot_id,
                trade_date,
                status,
                no_buy_detected,
                intent_count,
                order_count,
                command_count,
                plan_ready_count,
                ai_selected_count,
                primary_reason,
                stage_summary_json,
                reason_summary_json,
                top_near_miss_json,
                operator_checklist_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["snapshot_id"],
                payload["trade_date"],
                payload["status"],
                1 if payload["no_buy_detected"] else 0,
                payload["intent_count"],
                payload["order_count"],
                payload["command_count"],
                payload["plan_ready_count"],
                payload["ai_selected_count"],
                _primary_reason(payload),
                _json_dumps(payload["stage_summary"]),
                _json_dumps(payload["reason_summary"]),
                _json_dumps(payload["top_near_miss"]),
                _json_dumps(payload["operator_checklist"]),
                payload["created_at"],
            ),
        )
        connection.commit()
    except sqlite3.Error:
        connection.rollback()


def _snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["no_buy_detected"] = bool(item["no_buy_detected"])
    item["stage_summary"] = _json_object(item.pop("stage_summary_json"))
    item["reason_summary"] = _json_object(item.pop("reason_summary_json"))
    item["top_near_miss"] = _json_array(item.pop("top_near_miss_json"))
    item["operator_checklist"] = _json_array(item.pop("operator_checklist_json"))
    item["read_only"] = True
    item["no_order_side_effects"] = True
    return item


def _latest_ai_run(connection: sqlite3.Connection, trade_date: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_candidate_scoring_runs
        WHERE trade_date = ?
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """,
        (trade_date,),
    ).fetchone()
    if row is None:
        row = connection.execute(
            """
            SELECT *
            FROM ai_candidate_scoring_runs
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    item = {key: row[key] for key in row.keys()}
    for key in (
        "external_call_enabled",
        "external_call_attempted",
        "raw_response_stored",
        "prompt_redacted",
        "prompt_truncated",
        "live_sim_only",
        "advisory_only",
        "no_order_side_effects",
    ):
        if key in item:
            item[key] = bool(item[key])
    return item


def _ai_scores_for_run(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_candidate_scores
        WHERE run_id = ?
        ORDER BY selected DESC, score DESC, confidence DESC, code ASC
        LIMIT 500
        """,
        (run_id,),
    ).fetchall()
    result = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["selected"] = bool(item["selected"])
        item["flags"] = _json_array(item.pop("flags_json", "[]"))
        result.append(item)
    return result


def _ai_scores_by_identity(ai_summary: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    by_key: dict[str, Mapping[str, Any]] = {}
    for score in ai_summary.get("scores", []):
        for key in (
            str(score.get("code") or ""),
            str(score.get("candidate_instance_id") or ""),
            str(score.get("order_plan_id") or ""),
        ):
            if key:
                by_key[key] = score
    return by_key


def _ai_score_for(
    row: Mapping[str, Any],
    ai_scores: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    for key in (
        str(row.get("order_plan_id") or ""),
        str(row.get("candidate_instance_id") or ""),
        str(row.get("code") or ""),
    ):
        if key and key in ai_scores:
            return ai_scores[key]
    return {}


def _near_miss_sort_key(item: Mapping[str, Any]) -> tuple[int, float, float, str]:
    status = str(item.get("order_plan_status") or "")
    stage = str(item.get("primary_block_stage") or "")
    block_type = str(item.get("primary_block_type") or "")
    ai_score = _float(item.get("ai_score"))
    ai_confidence = _float(item.get("ai_confidence"))
    if status == "PLAN_READY" and stage in {
        StageCategory.LIVE_SIM_SAFETY.value,
        StageCategory.CONFIG.value,
        StageCategory.RECONCILE.value,
        StageCategory.DUPLICATE_POSITION.value,
        StageCategory.GATEWAY.value,
    }:
        rank = 0
    elif bool(item.get("ai_selected")) or ai_score > 0 or ai_confidence > 0:
        rank = 1
    elif status == "WAIT_RETRY":
        rank = 2
    elif block_type == BlockType.DATA_WAIT.value or status == "DATA_WAIT":
        rank = 3
    else:
        rank = 4
    return (rank, -ai_score, -ai_confidence, str(item.get("code") or ""))


def _system_reason_codes(
    gateway_summary: Mapping[str, Any],
    config_summary: Mapping[str, Any],
    latest_reconcile: Mapping[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    if gateway_summary.get("heartbeat_stale"):
        reasons.append("GATEWAY_HEARTBEAT_STALE")
    if not gateway_summary.get("gateway_orderable", True):
        reasons.append("GATEWAY_NOT_ORDERABLE")
    if not gateway_summary.get("command_queue_healthy", True):
        reasons.append("GATEWAY_COMMAND_QUEUE_UNHEALTHY")
    reasons.extend(config_summary.get("disabled_flags", []))
    if _reconcile_blocks(latest_reconcile):
        reasons.append("LIVE_SIM_RECONCILE_MISMATCH_BLOCK")
    return reasons


def _compact_live_sim_status(status: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "enabled",
        "order_routing_enabled",
        "gateway_command_enabled",
        "kill_switch",
        "account_id_configured",
        "account_mode",
        "broker_env",
        "server_mode",
        "intent_count",
        "order_count",
        "open_order_count",
        "open_position_count",
        "rejection_count",
        "allow_buy",
        "reconcile_enabled",
    )
    return {key: status.get(key) for key in keys}


def _count_live_sim_intents(connection: sqlite3.Connection, trade_date: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()
    return int(row["count"] or 0)


def _count_live_sim_orders(connection: sqlite3.Connection, trade_date: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_orders WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()
    return int(row["count"] or 0)


def _count_live_sim_commands(connection: sqlite3.Connection, *, lookback_minutes: int) -> int:
    cutoff = datetime_to_wire(utc_now() - timedelta(minutes=max(int(lookback_minutes), 1)))
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE created_at >= ?
            AND (
                source = 'live_sim'
                OR payload_json LIKE '%"live_sim_only":true%'
                OR payload_json LIKE '%"live_sim_only": true%'
            )
        """,
        (cutoff,),
    ).fetchone()
    return int(row["count"] or 0)


def _market_session(settings: Settings, *, manual: bool) -> str:
    if manual:
        return "MANUAL_DIAGNOSTIC"
    tz = candidate_timezone(settings.candidate_trade_date_timezone)
    now = datetime.now(tz)
    try:
        open_time = time.fromisoformat(settings.no_buy_sentinel_market_open_time)
    except ValueError:
        open_time = time(9, 0, 0)
    open_at = datetime.combine(now.date(), open_time, tzinfo=tz)
    ready_at = open_at + timedelta(minutes=settings.no_buy_sentinel_minutes_after_open)
    close_at = datetime.combine(now.date(), time(15, 30, 0), tzinfo=tz)
    if now < open_at:
        return "BEFORE_OPEN"
    if now < ready_at:
        return "OPEN_WARMUP"
    if now <= close_at:
        return "OPEN_DIAGNOSTIC_READY"
    return "CLOSED"


def _resolve_trade_date(trade_date: str | None, settings: Settings) -> str:
    if trade_date is not None:
        return str(trade_date).strip()
    return (
        datetime.now(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _reconcile_blocks(reconcile: Mapping[str, Any] | None) -> bool:
    if not reconcile:
        return False
    return bool(reconcile.get("blocking_new_buy")) or (
        str(reconcile.get("status") or "").upper() == "RECONCILE_MISMATCH"
        and int(reconcile.get("mismatch_count") or 0) > 0
    )


def _near_miss_has_stage(items: Sequence[Mapping[str, Any]], stage: StageCategory) -> bool:
    return any(str(item.get("primary_block_stage")) == stage.value for item in items)


def _near_miss_has_reason_token(
    items: Sequence[Mapping[str, Any]],
    tokens: Sequence[str],
) -> bool:
    normalized_tokens = tuple(str(token).upper() for token in tokens)
    return any(
        any(token in str(reason).upper() for token in normalized_tokens)
        for item in items
        for reason in _as_reason_list(item.get("reason_codes"))
    )


def _count_status(rows: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(1 for row in rows if str(row.get("status") or "").upper() == status)


def _as_reason_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return [value.upper()] if value.strip() else []
    else:
        loaded = value
    if not isinstance(loaded, Sequence) or isinstance(loaded, str | bytes):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value).upper() for value in values if str(value).strip())]


def _age_seconds(value: object) -> float:
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except ValueError:
        return float("inf")


def _primary_reason(payload: Mapping[str, Any]) -> str:
    reason_counts = payload.get("reason_summary", {}).get("reason_counts", {})
    if isinstance(reason_counts, Mapping) and reason_counts:
        return str(max(reason_counts.items(), key=lambda item: int(item[1]))[0])
    return str(payload.get("status") or "UNKNOWN")


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return loaded if isinstance(loaded, list) else []


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
