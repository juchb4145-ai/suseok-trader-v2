from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from services.config import Settings, load_settings
from services.runtime.market_reference_projection_reconcile import (
    get_latest_market_reference_projection_reconcile,
)
from storage.gateway_command_store import canonical_json

PROJECTION_NAME_MARKET_REFERENCE = "market_reference"
MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_IN_PR13"
)


@dataclass(frozen=True, kw_only=True)
class MarketReferenceAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_job_present: bool
    outbox_status: str | None
    would_skip_inline: bool
    effective_skip_inline: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    reconcile_required: bool = True
    worker_apply_enabled: bool = False
    membership_count: int = 0
    min_membership_count: int = 0
    projection_name: str = PROJECTION_NAME_MARKET_REFERENCE
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
            "dry_run_enabled": self.dry_run_enabled,
            "cutover_enabled": self.cutover_enabled,
            "reconcile_required": self.reconcile_required,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_job_present": self.outbox_job_present,
            "outbox_status": self.outbox_status,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "worker_apply_enabled": self.worker_apply_enabled,
            "membership_count": self.membership_count,
            "min_membership_count": self.min_membership_count,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def decide_market_reference_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketReferenceAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    dry_run_enabled = bool(settings.gateway_market_reference_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_reference_append_only_cutover_enabled)
    reconcile_required = bool(
        settings.gateway_market_reference_append_only_require_reconcile_pass
    )
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    latest_status = _string_or_none(
        latest_run.get("status") if isinstance(latest_run, Mapping) else None
    )
    latest_run_id = _string_or_none(
        latest_run.get("run_id") if isinstance(latest_run, Mapping) else None
    )
    latest_created_at = _string_or_none(
        latest_run.get("created_at") if isinstance(latest_run, Mapping) else None
    )
    latest_age_sec = _age_seconds(latest_created_at) if latest_created_at else None
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    membership_count = _membership_count(connection)
    min_membership_count = int(
        settings.gateway_market_reference_append_only_min_membership_count
    )
    outbox_job = _market_reference_outbox_job(connection, event.event_id)
    outbox_job_present = outbox_job is not None
    normalized_outbox_status = _normalize_outbox_status(
        outbox_status or (outbox_job["status"] if outbox_job is not None else None)
    )
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_reference_apply_enabled
    )
    source_event_status = _gateway_event_status(connection, event.event_id)

    reason_codes: list[str] = []
    would_skip_inline = False
    if event_type != "market_symbols":
        reason_codes.append("NOT_MARKET_SYMBOLS_EVENT")
    elif not dry_run_enabled:
        reason_codes.append("DRY_RUN_DISABLED")
    elif not _outbox_is_ready(
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
    ):
        reason_codes.append("MARKET_REFERENCE_OUTBOX_JOB_MISSING_OR_NOT_READY")
    elif reconcile_required and latest_run is None:
        reason_codes.append("MARKET_REFERENCE_RECONCILE_MISSING")
    elif reconcile_required and (latest_status != "PASS" or not append_only_ready):
        reason_codes.append("MARKET_REFERENCE_RECONCILE_NOT_PASS")
    elif (
        reconcile_required
        and latest_age_sec is not None
        and latest_age_sec
        > settings.gateway_market_reference_append_only_reconcile_max_age_sec
    ):
        reason_codes.append("MARKET_REFERENCE_RECONCILE_STALE")
    elif source_event_status != "ACCEPTED":
        reason_codes.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    elif membership_count < min_membership_count:
        reason_codes.append("MARKET_REFERENCE_MEMBERSHIP_COUNT_BELOW_MIN")
    else:
        would_skip_inline = True
        reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    if cutover_enabled or would_skip_inline:
        reason_codes.append(MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON)

    evidence = {
        "pr": "PR-13",
        "cutover_status": "DRY_RUN_ONLY",
        "inline_projection_remains_enabled": True,
        "effective_skip_disabled_in_pr13": bool(
            settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        ),
        "fallback_inline_projection_expected": True,
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "outbox_job_status": None if outbox_job is None else outbox_job["status"],
        "source_event_status": source_event_status,
        "latest_reconcile": (
            None
            if not isinstance(latest_run, Mapping)
            else {
                "run_id": latest_run.get("run_id"),
                "status": latest_run.get("status"),
                "append_only_ready": bool(latest_run.get("append_only_ready")),
                "created_at": latest_run.get("created_at"),
            }
        ),
        "worker_apply_enabled": worker_apply_enabled,
        "membership_count": membership_count,
        "min_membership_count": min_membership_count,
        "market_data_controller_unaffected": True,
        "live_real_order_behavior_unchanged": True,
        "no_trading_side_effects": True,
    }
    decision = MarketReferenceAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        reconcile_required=reconcile_required,
        latest_reconcile_status=latest_status,
        latest_reconcile_run_id=latest_run_id,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_job_present=outbox_job_present,
        outbox_status=normalized_outbox_status,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        worker_apply_enabled=worker_apply_enabled,
        membership_count=membership_count,
        min_membership_count=min_membership_count,
        blocked_reason_codes=tuple(reason_codes),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_market_reference_routing_decision(connection, decision)
    return decision


def get_latest_market_reference_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_reference_append_only_routing_decisions(connection, limit=500)
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    effective_skip_count = _count_decisions(connection, "effective_skip_inline")
    would_skip_count = _count_decisions(connection, "would_skip_inline")
    failures: list[str] = []
    warnings = [
        "PR-13 dry-run only; market_reference inline projection remains enabled",
        "market_data controller unaffected",
        "LIVE_REAL/order behavior unchanged",
    ]
    if effective_skip_count > 0:
        failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_FORBIDDEN_IN_PR13")
    if isinstance(latest_run, Mapping) and latest_run.get("status") == "FAIL":
        failures.append("MARKET_REFERENCE_RECONCILE_FAIL")
    if not bool(resolved_settings.gateway_market_reference_append_only_dry_run_enabled):
        warnings.append("market_reference append-only dry-run disabled")
    if bool(resolved_settings.gateway_market_reference_append_only_cutover_enabled):
        warnings.append("cutover flag ignored in PR-13; effective skip remains false")
    blocked_reason_counts = _blocked_reason_code_counts(rows)
    membership_count = _membership_count(connection)
    min_membership_count = int(
        resolved_settings.gateway_market_reference_append_only_min_membership_count
    )
    return {
        "pr": "PR-13",
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_reference_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_reference_append_only_cutover_enabled
        ),
        "effective_skip_disabled_in_pr13": True,
        "reconcile_required": bool(
            resolved_settings.gateway_market_reference_append_only_require_reconcile_pass
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_reference_append_only_reconcile_max_age_sec
        ),
        "total_decision_count": _count_all_decisions(connection),
        "would_skip_inline_count": would_skip_count,
        "effective_skip_inline_count": effective_skip_count,
        "blocked_reason_code_counts": blocked_reason_counts,
        "latest_decision": rows[0] if rows else None,
        "latest_reconcile": latest_reconcile,
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "append_only_ready": bool(
            latest_run.get("append_only_ready")
            if isinstance(latest_run, Mapping)
            else False
        ),
        "worker_apply_enabled": bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_reference_apply_enabled
        ),
        "membership_count": membership_count,
        "min_membership_count": min_membership_count,
        "membership_count_ready": membership_count >= min_membership_count,
        "failures": failures,
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_reference_append_only_routing_decisions(
    connection: sqlite3.Connection,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM market_reference_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_routing_row_to_dict(row) for row in rows]


def build_market_reference_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest_event = connection.execute(
        """
        SELECT event_id, event_ts, received_at
        FROM gateway_events
        WHERE status = 'ACCEPTED' AND event_type = 'market_symbols'
        ORDER BY rowid DESC
        LIMIT 1
        """
    ).fetchone()
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    routing_status = get_latest_market_reference_append_only_routing_status(
        connection,
        settings=resolved_settings,
    )
    outbox_counts = _market_reference_outbox_counts(connection)
    membership_count = _membership_count(connection)
    missing_membership_count = int(
        latest_run.get("missing_membership_count") or 0
        if isinstance(latest_run, Mapping)
        else 0
    )
    return {
        "pr": "PR-13",
        "membership_count": membership_count,
        "latest_market_symbols_event_id": (
            None if latest_event is None else latest_event["event_id"]
        ),
        "latest_market_symbols_event_ts": (
            None if latest_event is None else latest_event["event_ts"]
        ),
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "latest_reconcile_run_id": (
            latest_run.get("run_id") if isinstance(latest_run, Mapping) else None
        ),
        "append_only_ready": bool(
            latest_run.get("append_only_ready")
            if isinstance(latest_run, Mapping)
            else False
        ),
        "missing_membership_count": missing_membership_count,
        "outbox": outbox_counts,
        "append_only_routing": routing_status,
        "append_only_dry_run_would_skip_count": int(
            routing_status.get("would_skip_inline_count") or 0
        ),
        "effective_skip_inline_count": int(
            routing_status.get("effective_skip_inline_count") or 0
        ),
        "warnings": [
            "PR-13 dry-run only; market_reference inline projection remains enabled",
            "market_data controller unaffected",
            "LIVE_REAL/order behavior unchanged",
        ],
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _persist_market_reference_routing_decision(
    connection: sqlite3.Connection,
    decision: MarketReferenceAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_reference_projection_routing_decisions (
            event_id,
            event_type,
            projection_name,
            dry_run_enabled,
            cutover_enabled,
            reconcile_required,
            latest_reconcile_run_id,
            latest_reconcile_status,
            latest_reconcile_created_at,
            latest_reconcile_age_sec,
            append_only_ready,
            outbox_status,
            outbox_job_present,
            would_skip_inline,
            effective_skip_inline,
            worker_apply_enabled,
            membership_count,
            min_membership_count,
            blocked_reason_codes_json,
            evidence_json,
            decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            event_type = excluded.event_type,
            dry_run_enabled = excluded.dry_run_enabled,
            cutover_enabled = excluded.cutover_enabled,
            reconcile_required = excluded.reconcile_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            worker_apply_enabled = excluded.worker_apply_enabled,
            membership_count = excluded.membership_count,
            min_membership_count = excluded.min_membership_count,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            decision.event_id,
            decision.event_type,
            decision.projection_name,
            int(decision.dry_run_enabled),
            int(decision.cutover_enabled),
            int(decision.reconcile_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            int(decision.would_skip_inline),
            int(decision.effective_skip_inline),
            int(decision.worker_apply_enabled),
            decision.membership_count,
            decision.min_membership_count,
            json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=False,
                sort_keys=True,
            ),
            canonical_json(decision.evidence),
            decision.decided_at,
        ),
    )
    connection.commit()


def _market_reference_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, updated_at, created_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_reference' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _market_reference_outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "job_count": 0,
        "pending_count": 0,
        "processing_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
    }
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name = 'market_reference'
        GROUP BY status
        """
    ).fetchall()
    for row in rows:
        status = str(row["status"]).lower()
        count = int(row["count"])
        counts["job_count"] += count
        key = f"{status}_count"
        if key in counts:
            counts[key] = count
    return counts


def _outbox_is_ready(
    *,
    outbox_status: str | None,
    outbox_job_present: bool,
) -> bool:
    if not outbox_job_present:
        return False
    normalized = _normalize_outbox_status(outbox_status)
    return normalized in {
        None,
        "ENQUEUED",
        "DUPLICATE",
        "PENDING",
        "PROCESSING",
        "APPLIED",
        "SKIPPED",
    }


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_reference_projection_routing_decisions"
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _count_decisions(connection: sqlite3.Connection, column_name: str) -> int:
    if column_name not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError(f"unsupported routing decision count column: {column_name}")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM market_reference_projection_routing_decisions
        WHERE {column_name} = 1
        """
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _blocked_reason_code_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for code in row.get("blocked_reason_codes") or []:
            counter[str(code)] += 1
    return dict(sorted(counter.items()))


def _membership_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_symbol_memberships"
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _gateway_event_status(
    connection: sqlite3.Connection,
    event_id: str,
) -> str | None:
    row = connection.execute(
        """
        SELECT status
        FROM gateway_events
        WHERE event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else _string_or_none(row["status"])


def _routing_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "would_skip_inline",
        "effective_skip_inline",
        "worker_apply_enabled",
    ):
        payload[key] = bool(payload[key])
    payload["blocked_reason_codes"] = _json_array(
        payload.pop("blocked_reason_codes_json", "[]")
    )
    payload["evidence"] = _json_object(payload.pop("evidence_json", "{}"))
    payload["read_only"] = True
    payload["no_trading_side_effects"] = True
    return payload


def _normalize_outbox_status(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip().upper()


def _string_or_none(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _age_seconds(value: str) -> float | None:
    try:
        parsed = parse_timestamp(value, "created_at")
    except Exception:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_array(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []
