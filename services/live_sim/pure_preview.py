from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from domain.broker.utils import (
    MARKET_TIMEZONE,
    datetime_to_wire,
    market_today,
    utc_now,
)
from domain.live_sim.reasons import LiveSimReasonCode

from services.config import Settings
from services.live_sim.live_sim_service import get_live_sim_status
from services.live_sim.order_plan_eligibility import (
    LiveSimOrderPlanEligibility,
    evaluate_live_sim_order_plan_eligibility,
    select_live_sim_order_plan_candidates,
)

FAST1_PREVIEW_CONTRACT = "fast1-pure-live-sim-preview.v2"
FAST0_TRANSITION_STATUS = "RETIRED_HISTORICAL"
_DEFAULT_LIMIT = 5
_MAX_LIMIT = 20
_FORBIDDEN_SIDE_EFFECT_TABLES = (
    "entry_timing_evaluations",
    "order_plan_drafts",
    "order_plan_drafts_latest",
    "live_sim_intents",
    "live_sim_orders",
    "live_sim_runs",
    "live_sim_operating_runs",
    "live_sim_rejections",
    "gateway_commands",
    "gateway_command_events",
    "gateway_command_dedupe_keys",
)


class Fast1PreviewError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def open_fast1_preview_connection(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise Fast1PreviewError("DATABASE_NOT_FOUND", "FAST-1 preview database is missing")

    wal_exists = Path(f"{path}-wal").exists()
    shm_exists = Path(f"{path}-shm").exists()
    journal_exists = Path(f"{path}-journal").exists()
    if wal_exists != shm_exists or journal_exists:
        raise Fast1PreviewError(
            "DATABASE_SIDECAR_STATE_UNSTABLE",
            "FAST-1 preview requires a stable SQLite sidecar state",
        )

    connection = sqlite3.connect(
        f"{path.as_uri()}?mode=ro",
        uri=True,
        timeout=15.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def build_fast1_pure_preview(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    trade_date: str | None = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    if connection.in_transaction:
        raise Fast1PreviewError(
            "PREVIEW_CONNECTION_ALREADY_IN_TRANSACTION",
            "FAST-1 preview requires a fresh read-only snapshot",
        )
    if not _query_only_enabled(connection):
        raise Fast1PreviewError(
            "PREVIEW_QUERY_ONLY_REQUIRED",
            "FAST-1 preview requires PRAGMA query_only=ON",
        )

    current_trade_date = market_today()
    selected_trade_date = str(trade_date or current_trade_date).strip()
    if selected_trade_date != current_trade_date:
        raise Fast1PreviewError(
            "FAST1_CURRENT_TRADE_DATE_ONLY",
            "FAST-1 preview is restricted to the current KRX trade date",
        )
    bounded_limit = min(max(int(limit), 1), _MAX_LIMIT)

    connection.execute("BEGIN DEFERRED")
    try:
        before_counts = _side_effect_table_counts(connection)
        rows = select_live_sim_order_plan_candidates(
            connection,
            trade_date=selected_trade_date,
            limit=bounded_limit,
        )
        evaluations = [
            evaluate_live_sim_order_plan_eligibility(
                connection,
                str(row["order_plan_id"]),
                settings=settings,
            )
            for row in rows
        ]
        status = get_live_sim_status(connection, settings=settings)
        reconcile = _latest_reconcile_state(connection)
        current_market = _current_market_state(
            connection,
            trade_date=selected_trade_date,
            settings=settings,
        )
        plans = [
            _preview_plan(rank=index, source=row, eligibility=eligibility)
            for index, (row, eligibility) in enumerate(
                zip(rows, evaluations, strict=True), start=1
            )
        ]
        selectable = [plan for plan in plans if plan["selectable"]]
        top_candidate = selectable[0] if selectable else None
        after_counts = _side_effect_table_counts(connection)
        deltas = {
            table: int(after_counts.get(table, 0)) - int(before_counts.get(table, 0))
            for table in sorted(set(before_counts) | set(after_counts))
        }
        if any(deltas.values()):
            raise Fast1PreviewError(
                "PREVIEW_SIDE_EFFECT_DETECTED",
                "FAST-1 preview changed a forbidden table",
            )

        blocker_reason_codes = _blocker_reason_codes(
            plans,
            reconcile=reconcile,
            current_market=current_market,
        )
        canary_ready = top_candidate is not None and not blocker_reason_codes
        return {
            "contract": FAST1_PREVIEW_CONTRACT,
            "generated_at": datetime_to_wire(utc_now()),
            "trade_date": selected_trade_date,
            "qualification_transition": {
                "fast0_status": FAST0_TRANSITION_STATUS,
                "historical_blockers_resolved": False,
                "historical_data_mutated": False,
                "historical_qualification_required": False,
                "scope": "CURRENT_TRADE_DATE_ONLY",
                "current_gate_dependency": "CURRENT_MARKET_AND_CANARY",
                "operational_activation_authorized": False,
            },
            "selection": {
                "candidate_count": len(plans),
                "selectable_count": len(selectable),
                "blocked_count": len(plans) - len(selectable),
                "deterministic_order": [
                    "priority_score DESC",
                    "created_at DESC",
                    "order_plan_id ASC",
                ],
                "top_candidate": _top_candidate(top_candidate),
                "canary_ready": canary_ready,
                "blocker_reason_codes": blocker_reason_codes,
            },
            "plans": plans,
            "runtime_state": {
                "active_order_count": int(status.get("open_order_count") or 0),
                "active_position_count": int(status.get("open_position_count") or 0),
                "reconcile": reconcile,
            },
            "current_market": current_market,
            "side_effect_guard": {
                "opened_read_only": True,
                "query_only": True,
                "snapshot_transaction": True,
                "forbidden_table_counts_before": before_counts,
                "forbidden_table_counts_after": after_counts,
                "forbidden_table_deltas": deltas,
                "total_absolute_delta": sum(abs(value) for value in deltas.values()),
            },
            "preview_only": True,
            "read_only": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
            "no_broker_calls": True,
            "no_gateway_commands": True,
            "live_sim_only": True,
            "live_real_allowed": False,
            "real_order_allowed": False,
        }
    finally:
        if connection.in_transaction:
            connection.rollback()


def _preview_plan(
    *,
    rank: int,
    source: Mapping[str, Any],
    eligibility: LiveSimOrderPlanEligibility,
) -> dict[str, Any]:
    evidence = dict(eligibility.evidence_json)
    safety_gate = dict(eligibility.safety_gate_result)
    boundary = _mapping(safety_gate.get("order_broker_boundary"))
    lineage = _mapping(evidence.get("pipeline_lineage_guard"))
    reasons = [str(reason).upper() for reason in eligibility.reason_codes]
    return {
        "rank": rank,
        "order_plan_id": eligibility.order_plan_id,
        "candidate_instance_id": eligibility.candidate_instance_id,
        "code": eligibility.code,
        "name": eligibility.name,
        "priority_score": source.get("priority_score"),
        "created_at": source.get("created_at"),
        "eligible": bool(eligibility.eligible),
        "selectable": bool(eligibility.eligible),
        "status": eligibility.status,
        "reason_codes": reasons,
        "reason_categories": {
            reason: _mapping(eligibility.to_dict().get("reason_categories")).get(reason)
            for reason in reasons
        },
        "pipeline_lineage": lineage,
        "safety_gate": {
            "passed": safety_gate.get("passed") is True,
            "status": safety_gate.get("status"),
            "reason_codes": list(safety_gate.get("reason_codes") or []),
            "live_real_disabled": safety_gate.get("live_real_disabled") is True,
            "simulation_account_confirmed": (
                safety_gate.get("simulation_account_confirmed") is True
            ),
            "simulation_server_confirmed": (
                safety_gate.get("simulation_server_confirmed") is True
            ),
        },
        "broker_boundary": {
            "status": boundary.get("status"),
            "effective_status": boundary.get("effective_status"),
            "effective_unconfirmed_count": int(
                boundary.get("effective_unconfirmed_count") or 0
            ),
            "blocked": (
                LiveSimReasonCode.ORDER_BROKER_BOUNDARY_BLOCKED.value in reasons
            ),
        },
        "duplicate_intent": (
            LiveSimReasonCode.ORDER_PLAN_DUPLICATE_INTENT.value in reasons
        ),
        "reconcile_blocked": (
            LiveSimReasonCode.LIVE_SIM_RECONCILE_MISMATCH_BLOCK.value in reasons
        ),
        "sizing": dict(eligibility.sizing),
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def _top_candidate(plan: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if plan is None:
        return None
    return {
        "rank": plan.get("rank"),
        "order_plan_id": plan.get("order_plan_id"),
        "candidate_instance_id": plan.get("candidate_instance_id"),
        "code": plan.get("code"),
        "name": plan.get("name"),
        "priority_score": plan.get("priority_score"),
    }


def _blocker_reason_codes(
    plans: Sequence[Mapping[str, Any]],
    *,
    reconcile: Mapping[str, Any],
    current_market: Mapping[str, Any],
) -> list[str]:
    reasons = [str(value).upper() for value in current_market.get("reason_codes") or []]
    selectable = [plan for plan in plans if plan.get("selectable") is True]
    if not plans:
        reasons.append("NO_CURRENT_PLAN_READY")
    if plans and not selectable:
        reasons.append("NO_SELECTABLE_CURRENT_PLAN")
    if not selectable:
        for plan in plans:
            reasons.extend(str(value).upper() for value in plan.get("reason_codes") or [])
    if reconcile.get("blocking_new_buy") is True:
        reasons.append(LiveSimReasonCode.LIVE_SIM_RECONCILE_MISMATCH_BLOCK.value)
    return list(dict.fromkeys(reason for reason in reasons if reason))


def _current_market_state(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    settings: Settings,
) -> dict[str, Any]:
    start_at, end_at = _market_day_utc_bounds(trade_date)
    now = utc_now()
    now_at = datetime_to_wire(now)
    stock_fresh_at = datetime_to_wire(
        now - timedelta(seconds=settings.market_data_tick_stale_sec)
    )
    index_fresh_at = datetime_to_wire(
        now - timedelta(seconds=settings.market_index_stale_sec)
    )
    context_fresh_at = datetime_to_wire(
        now - timedelta(seconds=settings.market_context_snapshot_stale_sec)
    )
    stock_tick = connection.execute(
        """
        SELECT
            SUM(CASE WHEN event_ts >= ? AND event_ts < ? THEN 1 ELSE 0 END)
                AS count,
            SUM(CASE WHEN event_ts >= ? AND event_ts <= ? THEN 1 ELSE 0 END)
                AS fresh_count,
            MAX(CASE WHEN event_ts >= ? AND event_ts < ? THEN event_ts END)
                AS latest_current_at,
            MAX(event_ts) AS latest_available_at,
            MAX(received_at) AS latest_received_at
        FROM market_ticks_latest
        WHERE exchange = 'KRX'
        """,
        (start_at, end_at, stock_fresh_at, now_at, start_at, end_at),
    ).fetchone()
    index_tick = connection.execute(
        """
        SELECT
            SUM(CASE WHEN event_ts >= ? AND event_ts < ? THEN 1 ELSE 0 END)
                AS count,
            SUM(CASE WHEN event_ts >= ? AND event_ts <= ? THEN 1 ELSE 0 END)
                AS fresh_count,
            MAX(CASE WHEN event_ts >= ? AND event_ts < ? THEN event_ts END)
                AS latest_current_at,
            MAX(event_ts) AS latest_available_at,
            MAX(received_at) AS latest_received_at
        FROM market_index_ticks_latest
        WHERE index_code IN ('KOSPI', 'KOSDAQ')
        """,
        (start_at, end_at, index_fresh_at, now_at, start_at, end_at),
    ).fetchone()
    market_context = connection.execute(
        """
        SELECT
            SUM(CASE WHEN trade_date = ? THEN 1 ELSE 0 END) AS count,
            SUM(CASE WHEN trade_date = ? AND snapshot_at >= ? AND snapshot_at <= ?
                THEN 1 ELSE 0 END) AS fresh_count,
            MAX(CASE WHEN trade_date = ? THEN snapshot_at END) AS latest_current_at,
            MAX(snapshot_at) AS latest_available_at
        FROM market_context_latest
        WHERE market IN ('KOSPI', 'KOSDAQ')
        """,
        (trade_date, trade_date, context_fresh_at, now_at, trade_date),
    ).fetchone()
    market_scan = connection.execute(
        """
        SELECT
            SUM(CASE WHEN scanned_at >= ? AND scanned_at < ? THEN 1 ELSE 0 END)
                AS count,
            MAX(CASE WHEN scanned_at >= ? AND scanned_at < ? THEN scanned_at END)
                AS latest_current_at,
            MAX(scanned_at) AS latest_available_at
        FROM market_scan_latest
        """,
        (start_at, end_at, start_at, end_at),
    ).fetchone()

    stock_tick_count = int(stock_tick["count"] or 0)
    fresh_stock_tick_count = int(stock_tick["fresh_count"] or 0)
    index_tick_count = int(index_tick["count"] or 0)
    fresh_index_tick_count = int(index_tick["fresh_count"] or 0)
    market_context_count = int(market_context["count"] or 0)
    fresh_market_context_count = int(market_context["fresh_count"] or 0)
    reason_codes: list[str] = []
    if stock_tick_count == 0:
        reason_codes.append("NO_CURRENT_MARKET_TICK")
    elif fresh_stock_tick_count == 0:
        reason_codes.append("CURRENT_MARKET_TICK_STALE")
    if index_tick_count < 2:
        reason_codes.append("NO_CURRENT_MARKET_INDEX")
    elif fresh_index_tick_count < 2:
        reason_codes.append("CURRENT_MARKET_INDEX_STALE")
    if market_context_count < 2:
        reason_codes.append("NO_CURRENT_MARKET_CONTEXT")
    elif fresh_market_context_count < 2:
        reason_codes.append("CURRENT_MARKET_CONTEXT_STALE")

    return {
        "status": "READY" if not reason_codes else "BLOCKED",
        "reason_codes": reason_codes,
        "trade_date": trade_date,
        "utc_window": {"start_at": start_at, "end_at": end_at},
        "evaluated_at": now_at,
        "stock_ticks": {
            "count": stock_tick_count,
            "fresh_count": fresh_stock_tick_count,
            "stale_after_sec": settings.market_data_tick_stale_sec,
            "latest_current_at": stock_tick["latest_current_at"],
            "latest_available_at": stock_tick["latest_available_at"],
            "latest_received_at": stock_tick["latest_received_at"],
        },
        "market_indexes": {
            "required": ["KOSPI", "KOSDAQ"],
            "count": index_tick_count,
            "fresh_count": fresh_index_tick_count,
            "stale_after_sec": settings.market_index_stale_sec,
            "latest_current_at": index_tick["latest_current_at"],
            "latest_available_at": index_tick["latest_available_at"],
            "latest_received_at": index_tick["latest_received_at"],
        },
        "market_contexts": {
            "required": ["KOSPI", "KOSDAQ"],
            "count": market_context_count,
            "fresh_count": fresh_market_context_count,
            "stale_after_sec": settings.market_context_snapshot_stale_sec,
            "latest_current_at": market_context["latest_current_at"],
            "latest_available_at": market_context["latest_available_at"],
        },
        "market_scan": {
            "required": False,
            "count": int(market_scan["count"] or 0),
            "latest_current_at": market_scan["latest_current_at"],
            "latest_available_at": market_scan["latest_available_at"],
        },
        "pipeline": _current_pipeline_counts(connection, trade_date=trade_date),
    }


def _current_pipeline_counts(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
) -> dict[str, int]:
    tables = (
        "candidates",
        "candidate_sources_latest",
        "strategy_observations_latest",
        "risk_observations_latest",
        "entry_timing_evaluations",
        "order_plan_drafts_latest",
    )
    return {
        table: int(
            connection.execute(
                f'SELECT COUNT(*) AS count FROM "{table}" WHERE trade_date = ?',
                (trade_date,),
            ).fetchone()["count"]
        )
        for table in tables
    }


def _market_day_utc_bounds(trade_date: str) -> tuple[str, str]:
    parsed_date = date.fromisoformat(trade_date)
    start = datetime.combine(parsed_date, time.min, tzinfo=MARKET_TIMEZONE)
    return datetime_to_wire(start), datetime_to_wire(start + timedelta(days=1))


def _latest_reconcile_state(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT status, mismatch_count, blocking_new_buy, created_at
        FROM live_sim_reconcile_snapshots
        ORDER BY created_at DESC, reconcile_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "present": False,
            "status": "NOT_AVAILABLE",
            "mismatch_count": 0,
            "blocking_new_buy": False,
            "created_at": None,
        }
    return {
        "present": True,
        "status": str(row["status"]),
        "mismatch_count": int(row["mismatch_count"] or 0),
        "blocking_new_buy": bool(row["blocking_new_buy"]),
        "created_at": row["created_at"],
    }


def _side_effect_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    existing = {str(row["name"]) for row in rows}
    protected = sorted(
        table
        for table in existing
        if table in _FORBIDDEN_SIDE_EFFECT_TABLES or table.startswith("dry_run_")
    )
    return {
        table: int(
            connection.execute(
                f'SELECT COUNT(*) AS count FROM "{table.replace(chr(34), chr(34) * 2)}"'
            ).fetchone()["count"]
        )
        for table in protected
    }


def _query_only_enabled(connection: sqlite3.Connection) -> bool:
    row = connection.execute("PRAGMA query_only").fetchone()
    return row is not None and int(row[0]) == 1


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}
