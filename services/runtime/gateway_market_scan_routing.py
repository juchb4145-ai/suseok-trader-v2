from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, TradingMode, TradingProfile
from services.market_scan_service import inspect_market_scan_event
from services.runtime.market_scan_projection_reconcile import (
    get_latest_market_scan_projection_reconcile,
)

PROJECTION_NAME_MARKET_SCAN = "market_scan"
MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_IN_PR20"
)


@dataclass(frozen=True, kw_only=True)
class MarketScanAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    reconcile_required: bool
    latest_reconcile_run_id: str | None
    latest_reconcile_status: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_status: str | None
    outbox_job_present: bool
    parser_verified: bool
    data_usable: bool
    market_data_dependency_ready: bool
    worker_apply_enabled: bool
    observe_safe: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    effective_skip_disabled_in_pr20: bool
    blocked_reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence: Mapping[str, Any] = field(default_factory=dict)
    decided_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": PROJECTION_NAME_MARKET_SCAN,
            "dry_run_enabled": self.dry_run_enabled,
            "reconcile_required": self.reconcile_required,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_status": self.outbox_status,
            "outbox_job_present": self.outbox_job_present,
            "parser_verified": self.parser_verified,
            "data_usable": self.data_usable,
            "market_data_dependency_ready": self.market_data_dependency_ready,
            "worker_apply_enabled": self.worker_apply_enabled,
            "observe_safe": self.observe_safe,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "effective_skip_disabled_in_pr20": (
                self.effective_skip_disabled_in_pr20
            ),
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
        }


def decide_market_scan_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketScanAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    readiness = inspect_market_scan_event(event, settings=settings)
    latest = get_latest_market_scan_projection_reconcile(connection)
    latest_run = _mapping(latest.get("latest_run"))
    latest_status = _optional_text(latest_run.get("status"))
    latest_created_at = _optional_text(latest_run.get("created_at"))
    latest_age_sec = _age_seconds(latest_created_at)
    append_only_ready = bool(latest_run.get("append_only_ready"))
    current_outbox = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _optional_text(
        (current_outbox or {}).get("status") if current_outbox else outbox_status
    )
    outbox_counts = _outbox_counts(connection)
    observe_safe = bool(
        settings.trading_profile is TradingProfile.OBSERVE
        and settings.trading_mode is TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
    )
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_scan_apply_enabled
    )
    dependency = _market_data_dependency_status(
        connection,
        event_id=event.event_id,
        expected_rows=readiness.row_count,
        settings=settings,
    )
    prior_event_id = _prior_scan_event_id(connection, event.event_id)
    reconcile_covers_prior_event = bool(
        prior_event_id
        and latest_run.get("latest_event_id") == prior_event_id
        and latest_status == "PASS"
        and append_only_ready
    )

    reasons: list[str] = []
    if not settings.gateway_market_scan_append_only_dry_run_enabled:
        reasons.append("MARKET_SCAN_DRY_RUN_DISABLED")
    if event_type != "tr_response" or not readiness.recognized:
        reasons.append("MARKET_SCAN_EVENT_NOT_ELIGIBLE")
    if not settings.market_scan_enabled:
        reasons.append("MARKET_SCAN_SERVICE_DISABLED")
    if not observe_safe:
        reasons.append("MARKET_SCAN_OBSERVE_SAFE_REQUIRED")
    if not worker_apply_enabled:
        reasons.append("MARKET_SCAN_WORKER_APPLY_DISABLED")
    if not settings.gateway_market_scan_append_only_require_reconcile_pass:
        reasons.append("MARKET_SCAN_RECONCILE_GUARD_DISABLED")
    if latest_status != "PASS" or not append_only_ready:
        reasons.append("MARKET_SCAN_RECONCILE_NOT_READY")
    if latest_age_sec is None or (
        latest_age_sec > settings.gateway_market_scan_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_SCAN_RECONCILE_STALE")
    if not reconcile_covers_prior_event:
        reasons.append("MARKET_SCAN_PRIOR_EVENT_NOT_RECONCILED")
    if not settings.gateway_market_scan_append_only_require_parser_verified:
        reasons.append("MARKET_SCAN_PARSER_GUARD_DISABLED")
    if not readiness.parser_verified:
        reasons.append("MARKET_SCAN_PARSER_UNVERIFIED")
    if not readiness.data_usable:
        reasons.append("MARKET_SCAN_DATA_UNUSABLE")
    if not settings.gateway_market_scan_append_only_require_market_data_dependency:
        reasons.append("MARKET_SCAN_MARKET_DATA_DEPENDENCY_GUARD_DISABLED")
    if not dependency["ready"]:
        reasons.append("MARKET_SCAN_MARKET_DATA_DEPENDENCY_NOT_READY")
    if current_outbox is None:
        reasons.append("MARKET_SCAN_OUTBOX_JOB_MISSING")
    elif str(current_outbox["status"]).upper() not in {"PENDING", "PROCESSING"}:
        reasons.append("MARKET_SCAN_OUTBOX_NOT_ENQUEUED")
    if outbox_counts["error_count"] > 0:
        reasons.append("MARKET_SCAN_OUTBOX_ERROR_PRESENT")
    if outbox_counts["dead_letter_count"] > 0:
        reasons.append("MARKET_SCAN_OUTBOX_DEAD_LETTER_PRESENT")
    if (
        outbox_counts["pending_count"] + outbox_counts["processing_count"]
        > settings.gateway_market_scan_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_SCAN_OUTBOX_BACKLOG_NOT_READY")

    would_skip_inline = not reasons
    blocked_reason_codes = list(reasons)
    if would_skip_inline:
        blocked_reason_codes.extend(
            (
                "DRY_RUN_WOULD_SKIP_INLINE",
                MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON,
            )
        )
    elif settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20:
        blocked_reason_codes.append(MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON)
    decision = MarketScanAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=settings.gateway_market_scan_append_only_dry_run_enabled,
        reconcile_required=(
            settings.gateway_market_scan_append_only_require_reconcile_pass
        ),
        latest_reconcile_run_id=_optional_text(latest_run.get("run_id")),
        latest_reconcile_status=latest_status,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_status=normalized_outbox_status,
        outbox_job_present=current_outbox is not None,
        parser_verified=readiness.parser_verified,
        data_usable=readiness.data_usable,
        market_data_dependency_ready=bool(dependency["ready"]),
        worker_apply_enabled=worker_apply_enabled,
        observe_safe=observe_safe,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=False,
        effective_skip_disabled_in_pr20=(
            settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        ),
        blocked_reason_codes=tuple(dict.fromkeys(blocked_reason_codes)),
        evidence={
            "pr": "PR-20",
            "readiness": readiness.to_dict(),
            "market_data_dependency": dependency,
            "prior_event_id": prior_event_id,
            "reconcile_covers_prior_event": reconcile_covers_prior_event,
            "outbox_counts": outbox_counts,
            "inline_market_scan_path_retained": True,
            "nxt_may_validate_only_explicit_venue_neutral_scan_inputs": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        },
        decided_at=decided_at,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_scan_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    decisions = list_market_scan_append_only_routing_decisions(connection, limit=100)
    latest = decisions[0] if decisions else None
    reconcile = get_latest_market_scan_projection_reconcile(connection)
    outbox = _outbox_counts(connection)
    return {
        "status": "PASS" if latest and latest.get("would_skip_inline") else "WARN",
        "dry_run_enabled": settings.gateway_market_scan_append_only_dry_run_enabled,
        "effective_skip_disabled_in_pr20": (
            settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        ),
        "worker_apply_enabled": bool(
            settings.projection_outbox_apply_projection_enabled
            and settings.projection_outbox_market_scan_apply_enabled
        ),
        "decision_count": _count_decisions(connection),
        "would_skip_inline_count": _count_decisions(
            connection, field_name="would_skip_inline"
        ),
        "effective_skip_inline_count": _count_decisions(
            connection, field_name="effective_skip_inline"
        ),
        "latest_decision": latest,
        "latest_reconcile": reconcile,
        "outbox": outbox,
        "inline_market_scan_path_retained": True,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_scan_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT * FROM market_scan_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_decision_row(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketScanAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_scan_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            reconcile_required, latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec, append_only_ready,
            outbox_status, outbox_job_present, parser_verified, data_usable,
            market_data_dependency_ready, worker_apply_enabled, observe_safe,
            would_skip_inline, effective_skip_inline,
            effective_skip_disabled_in_pr20, blocked_reason_codes_json,
            evidence_json, decided_at
        ) VALUES (?, ?, 'market_scan', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            dry_run_enabled = excluded.dry_run_enabled,
            reconcile_required = excluded.reconcile_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            parser_verified = excluded.parser_verified,
            data_usable = excluded.data_usable,
            market_data_dependency_ready = excluded.market_data_dependency_ready,
            worker_apply_enabled = excluded.worker_apply_enabled,
            observe_safe = excluded.observe_safe,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = 0,
            effective_skip_disabled_in_pr20 = excluded.effective_skip_disabled_in_pr20,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            decision.event_id,
            decision.event_type,
            int(decision.dry_run_enabled),
            int(decision.reconcile_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            int(decision.parser_verified),
            int(decision.data_usable),
            int(decision.market_data_dependency_ready),
            int(decision.worker_apply_enabled),
            int(decision.observe_safe),
            int(decision.would_skip_inline),
            0,
            int(decision.effective_skip_disabled_in_pr20),
            json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            canonical_json(dict(decision.evidence)),
            decision.decided_at,
        ),
    )
    connection.commit()


def _market_data_dependency_status(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    expected_rows: int,
    settings: Settings,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    artifact_count = int(row["count"] if row else 0)
    sibling = connection.execute(
        """
        SELECT status FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    sibling_status = None if sibling is None else str(sibling["status"]).upper()
    artifact_ready = expected_rows > 0 and artifact_count >= expected_rows
    ordered_worker_ready = bool(
        sibling_status in {"PENDING", "PROCESSING", "APPLIED"}
        and settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_data_apply_enabled
    )
    return {
        "ready": artifact_ready or ordered_worker_ready,
        "artifact_count": artifact_count,
        "expected_rows": expected_rows,
        "sibling_outbox_status": sibling_status,
        "ordered_worker_ready": ordered_worker_ready,
    }


def _outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT * FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else {key: row[key] for key in row.keys()}


def _outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
    result = {
        "pending_count": 0,
        "processing_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
    }
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count FROM projection_outbox
        WHERE projection_name = 'market_scan' GROUP BY status
        """
    ).fetchall()
    for row in rows:
        key = f"{str(row['status']).lower()}_count"
        if key in result:
            result[key] = int(row["count"])
    return result


def _prior_scan_event_id(connection: sqlite3.Connection, event_id: str) -> str | None:
    current = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if current is None:
        return None
    row = connection.execute(
        """
        SELECT event_id FROM gateway_events
        WHERE status = 'ACCEPTED' AND lower(event_type) = 'tr_response'
          AND rowid < ?
          AND (
                lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
                    LIKE 'market_scan:%'
             OR lower(COALESCE(json_extract(payload_json, '$.request_name'), ''))
                    LIKE 'market_scan_%'
          )
        ORDER BY rowid DESC LIMIT 1
        """,
        (int(current["event_rowid"]),),
    ).fetchone()
    return None if row is None else str(row["event_id"])


def _decision_row(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    for key in (
        "dry_run_enabled",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "parser_verified",
        "data_usable",
        "market_data_dependency_ready",
        "worker_apply_enabled",
        "observe_safe",
        "would_skip_inline",
        "effective_skip_inline",
        "effective_skip_disabled_in_pr20",
    ):
        data[key] = bool(data[key])
    data["blocked_reason_codes"] = json.loads(data.pop("blocked_reason_codes_json"))
    data["evidence"] = json.loads(data.pop("evidence_json"))
    return data


def _count_decisions(
    connection: sqlite3.Connection,
    *,
    field_name: str | None = None,
) -> int:
    if field_name is None:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM market_scan_projection_routing_decisions"
        ).fetchone()
    elif field_name in {"would_skip_inline", "effective_skip_inline"}:
        row = connection.execute(
            f"SELECT COUNT(*) AS count FROM market_scan_projection_routing_decisions "
            f"WHERE {field_name} = 1"
        ).fetchone()
    else:
        raise ValueError("unsupported field_name")
    return int(row["count"] if row else 0)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except Exception:
        return None
