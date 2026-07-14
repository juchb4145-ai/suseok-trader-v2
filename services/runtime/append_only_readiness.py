from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now

from services.config import (
    Settings,
    TradingMode,
    TradingProfile,
    candidate_timezone,
    load_settings,
)
from services.runtime.gateway_live_sim_lifecycle_routing import (
    build_live_sim_lifecycle_cutover_status,
)

APPEND_ONLY_READINESS_FORMAT = "append-only-readiness/v1"
REQUIRED_TRADING_DAYS = 10
REQUIRED_COMPONENTS = (
    "market_data",
    "market_reference",
    "market_index",
    "market_regime",
    "market_scan",
    "live_sim_lifecycle",
)
_MAX_RUN_ROWS_PER_COMPONENT = 2000
_MAX_DAILY_ROWS_IN_RESPONSE = 15

_PROJECTION_SPECS: dict[str, dict[str, Any]] = {
    "market_data": {
        "table": "market_data_projection_reconcile_runs",
        "event_ts": "evidence_event_ts",
        "zero_fields": (
            "outbox_pending_count",
            "outbox_processing_count",
            "outbox_skipped_count",
            "outbox_error_count",
            "outbox_dead_letter_count",
            "missing_projection_count",
            "inline_projection_error_count",
            "outbox_error_issue_count",
            "duplicate_or_conflict_count",
            "synthetic_child_event_issue_count",
            "watermark_risk_count",
            "tr_response_pending_within_sla_count",
            "tr_response_deferred_quote_refresh_error_count",
            "condition_event_pending_within_sla_count",
            "condition_event_deferred_fusion_refresh_error_count",
            "condition_event_side_effect_duplicate_count",
            "condition_event_candidate_ingest_in_worker_count",
            "condition_event_artifact_missing_after_worker_count",
            "invalid_effective_skip_count",
        ),
        "true_fields": (),
    },
    "market_reference": {
        "table": "market_reference_projection_reconcile_runs",
        "event_ts": "latest_event_ts",
        "zero_fields": (
            "outbox_pending_count",
            "outbox_processing_count",
            "outbox_skipped_count",
            "outbox_error_count",
            "outbox_dead_letter_count",
            "missing_membership_count",
        ),
        "true_fields": (),
    },
    "market_index": {
        "table": "market_index_projection_reconcile_runs",
        "event_ts": "latest_event_ts",
        "zero_fields": (
            "outbox_pending_count",
            "outbox_processing_count",
            "outbox_skipped_count",
            "outbox_error_count",
            "outbox_dead_letter_count",
            "missing_sample_count",
            "projection_error_count",
            "parser_unverified_count",
            "data_unusable_count",
            "tr_bootstrap_source_count",
            "unknown_source_count",
        ),
        "true_fields": ("data_usability_ready", "parser_confidence_ready"),
    },
    "market_regime": {
        "table": "market_regime_projection_reconcile_runs",
        "event_ts": "latest_event_ts",
        "zero_fields": (
            "outbox_pending_count",
            "outbox_processing_count",
            "outbox_skipped_count",
            "outbox_error_count",
            "outbox_dead_letter_count",
        ),
        "true_fields": ("latest_event_covered", "context_ready"),
    },
    "market_scan": {
        "table": "market_scan_projection_reconcile_runs",
        "event_ts": "latest_event_ts",
        "zero_fields": (
            "outbox_pending_count",
            "outbox_processing_count",
            "outbox_skipped_count",
            "outbox_error_count",
            "outbox_dead_letter_count",
            "projection_error_row_count",
        ),
        "true_fields": ("latest_event_covered",),
    },
}


def build_append_only_readiness_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    timezone_name = resolved_settings.candidate_trade_date_timezone
    schema_availability = {
        component: _table_exists(connection, str(spec["table"]))
        for component, spec in _PROJECTION_SPECS.items()
    }
    schema_availability["live_sim_lifecycle"] = all(
        _table_exists(connection, table_name)
        for table_name in (
            "live_sim_lifecycle_consumer_runs",
            "live_sim_lifecycle_inbox",
        )
    )
    component_daily_evidence = {
        component: (
            _projection_daily_evidence(
                connection,
                component=component,
                timezone_name=timezone_name,
            )
            if schema_availability[component]
            else {}
        )
        for component in _PROJECTION_SPECS
    }
    component_daily_evidence["live_sim_lifecycle"] = (
        _lifecycle_daily_evidence(
            connection,
            timezone_name=timezone_name,
        )
        if schema_availability["live_sim_lifecycle"]
        else {}
    )
    configuration = _configuration_readiness(resolved_settings)
    current_health = _current_consumer_health(
        connection,
        settings=resolved_settings,
    )
    return evaluate_append_only_readiness(
        component_daily_evidence=component_daily_evidence,
        configuration=configuration,
        current_health=current_health,
        schema_availability=schema_availability,
        required_trading_days=REQUIRED_TRADING_DAYS,
    )


def evaluate_append_only_readiness(
    *,
    component_daily_evidence: Mapping[str, Mapping[str, Mapping[str, Any]]],
    configuration: Mapping[str, Any],
    current_health: Mapping[str, Any],
    schema_availability: Mapping[str, bool] | None = None,
    required_trading_days: int = REQUIRED_TRADING_DAYS,
) -> dict[str, Any]:
    required_days = max(int(required_trading_days), 1)
    normalized = {
        component: dict(component_daily_evidence.get(component) or {})
        for component in REQUIRED_COMPONENTS
    }
    anchor_dates = sorted(normalized["market_index"], reverse=True)
    qualified_dates = [
        trade_date
        for trade_date in anchor_dates
        if all(
            bool((normalized[component].get(trade_date) or {}).get("passed"))
            for component in REQUIRED_COMPONENTS
        )
    ]
    consecutive_dates: list[str] = []
    for trade_date in anchor_dates:
        if trade_date not in qualified_dates:
            break
        consecutive_dates.append(trade_date)

    config_ready = bool(configuration.get("ready"))
    health_ready = bool(current_health.get("ready"))
    resolved_schema_availability = {
        component: bool(
            True
            if schema_availability is None
            else schema_availability.get(component, False)
        )
        for component in REQUIRED_COMPONENTS
    }
    schema_ready = all(resolved_schema_availability.values())
    evidence_ready = len(consecutive_dates) >= required_days
    observe_safe = bool(
        (configuration.get("gates") or {}).get("observe_safe")
    )
    reason_codes: list[str] = []
    if not observe_safe:
        reason_codes.append("APPEND_ONLY_OBSERVE_SAFETY_NOT_SATISFIED")
    if not config_ready:
        reason_codes.append("APPEND_ONLY_CONFIGURATION_NOT_ARMED")
    if not health_ready:
        reason_codes.append("APPEND_ONLY_CURRENT_CONSUMER_HEALTH_NOT_READY")
    if not schema_ready:
        reason_codes.append("APPEND_ONLY_EVIDENCE_SCHEMA_INCOMPLETE")
    if not evidence_ready:
        reason_codes.append("APPEND_ONLY_TEN_TRADING_DAY_EVIDENCE_INCOMPLETE")

    if not observe_safe:
        status = "FAIL_SAFETY"
    elif not schema_ready:
        status = "BLOCKED_SCHEMA"
    elif not config_ready:
        status = "BLOCKED_CONFIG"
    elif not health_ready:
        status = "BLOCKED_HEALTH"
    elif not evidence_ready:
        status = "BLOCKED_EVIDENCE"
    else:
        status = "READY_FOR_OPERATOR_REVIEW"

    component_statuses = {
        component: _component_status(
            normalized[component],
            anchor_dates=anchor_dates,
            schema_available=resolved_schema_availability[component],
        )
        for component in REQUIRED_COMPONENTS
    }
    return {
        "format": APPEND_ONLY_READINESS_FORMAT,
        "status": status,
        "reason_codes": reason_codes,
        "required_trading_days": required_days,
        "qualified_trading_day_count": len(qualified_dates),
        "consecutive_qualified_trading_day_count": len(consecutive_dates),
        "qualified_trade_dates": qualified_dates[:required_days],
        "consecutive_qualified_trade_dates": consecutive_dates[:required_days],
        "latest_anchor_trade_date": anchor_dates[0] if anchor_dates else None,
        "anchor_source": "MARKET_INDEX_REALTIME_RECONCILE_EVIDENCE",
        "component_statuses": component_statuses,
        "schema_ready": schema_ready,
        "schema_availability": resolved_schema_availability,
        "configuration": dict(configuration),
        "current_health": dict(current_health),
        "evidence_ready": evidence_ready,
        "ready_for_operator_review": status == "READY_FOR_OPERATOR_REVIEW",
        "official_krx_calendar_confirmation_required": True,
        "koa_studio_confirmation_required": True,
        "automatic_cutover_allowed": False,
        "flag_cleanup_allowed": False,
        "raw_append_enqueue_only_enabled": False,
        "request_path_removal_performed": False,
        "emergency_inline_fallback_retained": True,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "generated_at": datetime_to_wire(utc_now()),
    }


def _projection_daily_evidence(
    connection: sqlite3.Connection,
    *,
    component: str,
    timezone_name: str,
) -> dict[str, dict[str, Any]]:
    spec = _PROJECTION_SPECS[component]
    if component == "market_data":
        rows = connection.execute(
            """
            SELECT runs.*, events.event_ts AS evidence_event_ts
            FROM market_data_projection_reconcile_runs AS runs
            LEFT JOIN gateway_events AS events
              ON events.rowid = runs.event_rowid_max
            ORDER BY runs.created_at DESC, runs.run_id DESC
            LIMIT ?
            """,
            (_MAX_RUN_ROWS_PER_COMPONENT,),
        ).fetchall()
    else:
        table_name = str(spec["table"])
        rows = connection.execute(
            f"SELECT * FROM {table_name} "
            "ORDER BY created_at DESC, run_id DESC LIMIT ?",
            (_MAX_RUN_ROWS_PER_COMPONENT,),
        ).fetchall()

    daily: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row)
        run_date = _local_date(payload.get("created_at"), timezone_name=timezone_name)
        if run_date is None or run_date in daily:
            continue
        event_date = _local_date(
            payload.get(str(spec["event_ts"])),
            timezone_name=timezone_name,
        )
        reasons = _projection_failure_reasons(
            component,
            payload,
            run_date=run_date,
            event_date=event_date,
            zero_fields=spec["zero_fields"],
            true_fields=spec["true_fields"],
        )
        daily[run_date] = {
            "passed": not reasons,
            "run_id": payload.get("run_id"),
            "status": payload.get("status"),
            "run_date": run_date,
            "event_date": event_date,
            "checked_event_count": int(payload.get("checked_event_count") or 0),
            "append_only_ready": bool(payload.get("append_only_ready")),
            "reason_codes": reasons,
            "reconcile_reason_codes": _json_list(payload.get("reason_codes_json")),
            "created_at": payload.get("created_at"),
        }
    return daily


def _projection_failure_reasons(
    component: str,
    payload: Mapping[str, Any],
    *,
    run_date: str,
    event_date: str | None,
    zero_fields: Sequence[str],
    true_fields: Sequence[str],
) -> list[str]:
    reasons: list[str] = []
    if str(payload.get("status") or "").upper() != "PASS":
        reasons.append("RECONCILE_NOT_PASS")
    checked_event_count = int(payload.get("checked_event_count") or 0)
    if checked_event_count <= 0:
        reasons.append("CHECKED_EVENT_MISSING")
    if not bool(payload.get("append_only_ready")):
        reasons.append("APPEND_ONLY_READY_FALSE")
    if not bool(payload.get("no_trading_side_effects")):
        reasons.append("TRADING_SIDE_EFFECT_GUARD_MISSING")
    if event_date is None:
        reasons.append("EVIDENCE_EVENT_DATE_MISSING")
    elif event_date != run_date:
        reasons.append("EVIDENCE_EVENT_DATE_MISMATCH")
    for field_name in zero_fields:
        if int(payload.get(field_name) or 0) != 0:
            reasons.append(f"{field_name.upper()}_NONZERO")
    for field_name in true_fields:
        if not bool(payload.get(field_name)):
            reasons.append(f"{field_name.upper()}_FALSE")
    if component == "market_index" and int(
        payload.get("realtime_source_count") or 0
    ) != checked_event_count:
        reasons.append("MARKET_INDEX_REALTIME_COVERAGE_INCOMPLETE")
    return sorted(set(reasons))


def _lifecycle_daily_evidence(
    connection: sqlite3.Connection,
    *,
    timezone_name: str,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_lifecycle_consumer_runs
        ORDER BY completed_at DESC, run_id DESC
        LIMIT ?
        """,
        (_MAX_RUN_ROWS_PER_COMPONENT,),
    ).fetchall()
    daily: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row)
        run_date = _local_date(payload.get("completed_at"), timezone_name=timezone_name)
        if run_date is None or run_date in daily:
            continue
        reasons: list[str] = []
        if str(payload.get("status") or "").upper() not in {"IDLE", "COMPLETED"}:
            reasons.append("LIFECYCLE_WORKER_RUN_NOT_HEALTHY")
        for field_name in ("error_count", "dead_letter_count", "stale_reset_count"):
            if int(payload.get(field_name) or 0) != 0:
                reasons.append(f"{field_name.upper()}_NONZERO")
        daily[run_date] = {
            "passed": not reasons,
            "run_id": payload.get("run_id"),
            "status": payload.get("status"),
            "run_date": run_date,
            "event_date": run_date,
            "checked_event_count": int(payload.get("claimed_count") or 0),
            "append_only_ready": not reasons,
            "reason_codes": sorted(set(reasons)),
            "reconcile_reason_codes": [],
            "created_at": payload.get("completed_at"),
        }
    return daily


def _configuration_readiness(settings: Settings) -> dict[str, Any]:
    candidate_ingest_allowed = (
        settings.gateway_market_data_append_only_condition_event_allow_candidate_ingest_in_worker
    )
    gates = {
        "observe_safe": bool(
            settings.trading_profile is TradingProfile.OBSERVE
            and settings.trading_mode is TradingMode.OBSERVE
            and not settings.trading_allow_live_sim
            and not settings.trading_allow_live_real
        ),
        "projection_worker_apply_all": bool(
            settings.projection_outbox_worker_enabled
            and settings.projection_outbox_apply_projection_enabled
            and settings.projection_outbox_market_data_apply_enabled
            and settings.projection_outbox_market_reference_apply_enabled
            and settings.projection_outbox_market_index_apply_enabled
            and settings.projection_outbox_market_regime_apply_enabled
            and settings.projection_outbox_market_scan_apply_enabled
        ),
        "market_data_full_guarded": bool(
            settings.gateway_market_data_append_only_operating_mode
            == "MARKET_DATA_FULL_GUARDED"
            and settings.gateway_market_data_append_only_dry_run_enabled
            and settings.gateway_market_data_append_only_cutover_enabled
            and settings.gateway_market_data_append_only_price_tick_cutover_enabled
            and settings.gateway_market_data_append_only_tr_response_dry_run_enabled
            and settings.gateway_market_data_append_only_tr_response_cutover_enabled
            and settings.gateway_market_data_append_only_condition_event_dry_run_enabled
            and settings.gateway_market_data_append_only_condition_event_cutover_enabled
            and not settings.gateway_market_data_append_only_global_kill_switch
            and settings.gateway_market_data_append_only_global_max_skip_per_minute > 0
            and settings.gateway_market_data_append_only_price_tick_max_skip_per_minute > 0
            and settings.gateway_market_data_append_only_tr_response_max_skip_per_minute > 0
            and settings.gateway_market_data_append_only_condition_event_max_skip_per_minute
            > 0
            and settings.gateway_market_data_append_only_auto_rollback_enabled
            and not candidate_ingest_allowed
        ),
        "market_reference_guarded": bool(
            settings.gateway_market_reference_append_only_dry_run_enabled
            and settings.gateway_market_reference_append_only_cutover_enabled
            and not settings.gateway_market_reference_append_only_global_kill_switch
            and settings.gateway_market_reference_append_only_max_skip_per_minute > 0
            and not settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        ),
        "market_index_guarded": bool(
            settings.gateway_market_index_append_only_dry_run_enabled
            and settings.gateway_market_index_append_only_cutover_enabled
            and not settings.gateway_market_index_append_only_global_kill_switch
            and settings.gateway_market_index_append_only_max_skip_per_minute > 0
            and not settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        ),
        "market_regime_guarded": bool(
            settings.gateway_market_regime_append_only_dry_run_enabled
            and settings.gateway_market_regime_append_only_cutover_enabled
            and not settings.gateway_market_regime_append_only_global_kill_switch
            and settings.gateway_market_regime_append_only_max_skip_per_minute > 0
            and not settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
        ),
        "market_scan_guarded": bool(
            settings.gateway_market_scan_append_only_dry_run_enabled
            and settings.gateway_market_scan_append_only_cutover_enabled
            and not settings.gateway_market_scan_append_only_global_kill_switch
            and settings.gateway_market_scan_append_only_max_skip_per_minute > 0
            and not settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        ),
        "lifecycle_guarded": bool(
            settings.live_sim_lifecycle_consumer_enabled
            and settings.live_sim_lifecycle_worker_enabled
            and settings.live_sim_lifecycle_cutover_dry_run_enabled
            and settings.live_sim_lifecycle_cutover_enabled
            and not settings.live_sim_lifecycle_global_kill_switch
            and settings.live_sim_lifecycle_inline_fallback_enabled
        ),
    }
    blocked_gates = sorted(key for key, value in gates.items() if not value)
    return {
        "ready": not blocked_gates,
        "gates": gates,
        "blocked_gates": blocked_gates,
        "defaults_remain_fail_closed": True,
    }


def _current_consumer_health(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    statuses = ("PENDING", "PROCESSING", "ERROR", "DEAD_LETTER")
    outbox_counts = {status: 0 for status in statuses}
    outbox_schema_available = _table_exists(connection, "projection_outbox")
    if outbox_schema_available:
        for row in connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM projection_outbox
            WHERE status IN ('PENDING', 'PROCESSING', 'ERROR', 'DEAD_LETTER')
            GROUP BY status
            """
        ):
            outbox_counts[str(row["status"])] = int(row["count"])
    lifecycle_schema_available = all(
        _table_exists(connection, table_name)
        for table_name in (
            "live_sim_lifecycle_consumer_runs",
            "live_sim_lifecycle_inbox",
        )
    )
    lifecycle = (
        build_live_sim_lifecycle_cutover_status(
            connection,
            settings=settings,
        )
        if lifecycle_schema_available
        else {"status": "SCHEMA_MISSING", "reason_codes": ["SCHEMA_MISSING"]}
    )
    lifecycle_ready = bool(
        lifecycle_schema_available
        and lifecycle.get("status") == "PASS"
        and int(lifecycle.get("pending_count") or 0) == 0
        and int(lifecycle.get("processing_count") or 0) == 0
        and int(lifecycle.get("dead_letter_count") or 0) == 0
        and int(lifecycle.get("stale_processing_count") or 0) == 0
        and int(lifecycle.get("missing_inbox_count") or 0) == 0
        and int(lifecycle.get("applied_without_result_count") or 0) == 0
    )
    outbox_ready = bool(
        outbox_schema_available and all(value == 0 for value in outbox_counts.values())
    )
    return {
        "ready": outbox_ready and lifecycle_ready,
        "projection_outbox_counts": outbox_counts,
        "projection_outbox_ready": outbox_ready,
        "projection_outbox_schema_available": outbox_schema_available,
        "lifecycle_ready": lifecycle_ready,
        "lifecycle_schema_available": lifecycle_schema_available,
        "lifecycle_status": lifecycle.get("status"),
        "lifecycle_reason_codes": list(lifecycle.get("reason_codes") or []),
    }


def _component_status(
    daily: Mapping[str, Mapping[str, Any]],
    *,
    anchor_dates: Sequence[str],
    schema_available: bool,
) -> dict[str, Any]:
    ordered_dates = sorted(daily, reverse=True)
    latest_date = ordered_dates[0] if ordered_dates else None
    latest = None if latest_date is None else dict(daily[latest_date])
    return {
        "schema_available": schema_available,
        "latest_trade_date": latest_date,
        "latest": latest,
        "pass_day_count": sum(bool(item.get("passed")) for item in daily.values()),
        "failed_day_count": sum(not bool(item.get("passed")) for item in daily.values()),
        "missing_anchor_dates": [
            trade_date
            for trade_date in anchor_dates[:REQUIRED_TRADING_DAYS]
            if trade_date not in daily
        ],
        "daily_evidence": [
            dict(daily[trade_date])
            for trade_date in ordered_dates[:_MAX_DAILY_ROWS_IN_RESPONSE]
        ],
    }


def _local_date(value: Any, *, timezone_name: str) -> str | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(value, "append_only_evidence_timestamp")
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(candidate_timezone(timezone_name)).date().isoformat()


def _json_list(value: Any) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return ["INVALID_REASON_CODES_JSON"]
    if not isinstance(parsed, list):
        return ["INVALID_REASON_CODES_JSON"]
    return [str(item) for item in parsed]


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None
