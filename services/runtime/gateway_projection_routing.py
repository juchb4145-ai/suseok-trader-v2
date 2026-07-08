from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_data_service import MARKET_DATA_EVENT_TYPES
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
)

PROJECTION_NAME_MARKET_DATA = "market_data"
PR6_EFFECTIVE_SKIP_DISABLED_REASON = "EFFECTIVE_SKIP_DISABLED_IN_PR6"


@dataclass(frozen=True, kw_only=True)
class MarketDataAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    reconcile_required: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_status: str | None
    outbox_job_present: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    projection_name: str = PROJECTION_NAME_MARKET_DATA
    evidence: Mapping[str, Any] = field(default_factory=dict)

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
            "outbox_status": self.outbox_status,
            "outbox_job_present": self.outbox_job_present,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": True,
        }


def decide_market_data_projection_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketDataAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    dry_run_enabled = bool(settings.gateway_market_data_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_data_append_only_cutover_enabled)
    reconcile_required = bool(
        settings.gateway_market_data_append_only_require_reconcile_pass
    )
    decided_at = datetime_to_wire(utc_now())
    latest_reconcile = get_latest_market_data_projection_reconcile(connection)
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
    outbox_job = _market_data_outbox_job(connection, event.event_id)
    outbox_job_present = outbox_job is not None
    normalized_outbox_status = _normalize_outbox_status(
        outbox_status or (outbox_job["status"] if outbox_job is not None else None)
    )
    allowed_event_types = {
        str(value).strip().lower()
        for value in settings.gateway_market_data_append_only_event_types
    }

    reason_codes: list[str] = []
    would_skip_inline = False
    if event_type not in MARKET_DATA_EVENT_TYPES:
        reason_codes.append("NOT_MARKET_DATA_EVENT")
    elif not dry_run_enabled:
        reason_codes.append("DRY_RUN_DISABLED")
    elif event_type not in allowed_event_types:
        reason_codes.append("EVENT_TYPE_NOT_ALLOWED_FOR_APPEND_ONLY")
    elif not _outbox_is_ready(
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
        min_outbox_status=settings.gateway_market_data_append_only_min_outbox_status,
    ):
        reason_codes.append("MARKET_DATA_OUTBOX_JOB_MISSING_OR_NOT_ENQUEUED")
    elif reconcile_required and latest_run is None:
        reason_codes.append("MARKET_DATA_RECONCILE_MISSING")
    elif reconcile_required and (latest_status != "PASS" or not append_only_ready):
        reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
    elif (
        reconcile_required
        and latest_age_sec is not None
        and latest_age_sec > settings.gateway_market_data_append_only_reconcile_max_age_sec
    ):
        reason_codes.append("MARKET_DATA_RECONCILE_STALE")
    else:
        would_skip_inline = True
        reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    if cutover_enabled:
        reason_codes.append(PR6_EFFECTIVE_SKIP_DISABLED_REASON)

    evidence = {
        "pr": "PR-6",
        "dry_run_only": True,
        "inline_projection_remains_enabled": True,
        "effective_skip_disabled_in_pr6": True,
        "allowed_event_types": sorted(allowed_event_types),
        "configured_min_outbox_status": (
            settings.gateway_market_data_append_only_min_outbox_status
        ),
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "outbox_job_status": None if outbox_job is None else outbox_job["status"],
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
        "no_trading_side_effects": True,
    }
    decision = MarketDataAppendOnlyRoutingDecision(
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
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        blocked_reason_codes=tuple(reason_codes),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_market_data_projection_routing_decision(connection, decision)
    return decision


def get_latest_market_data_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_data_append_only_routing_decisions(connection, limit=500)
    total_count = _count_all_decisions(connection)
    would_skip_count = _count_decisions(connection, "would_skip_inline")
    effective_skip_count = _count_decisions(connection, "effective_skip_inline")
    blocked_reason_counts = _blocked_reason_code_counts(rows)
    blocked_count = sum(
        1
        for row in rows
        if not row["would_skip_inline"]
        and "DRY_RUN_DISABLED" not in set(row["blocked_reason_codes"])
    )
    latest_reconcile = get_latest_market_data_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    warnings = ["PR-6 dry-run only; inline projection remains enabled"]
    if effective_skip_count:
        warnings.append("PR-6 violation: effective_skip_inline_count must remain 0")
    return {
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_data_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_data_append_only_cutover_enabled
        ),
        "reconcile_required": bool(
            resolved_settings.gateway_market_data_append_only_require_reconcile_pass
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_data_append_only_reconcile_max_age_sec
        ),
        "event_types": list(
            resolved_settings.gateway_market_data_append_only_event_types
        ),
        "min_outbox_status": (
            resolved_settings.gateway_market_data_append_only_min_outbox_status
        ),
        "total_decision_count": total_count,
        "would_skip_inline_count": would_skip_count,
        "effective_skip_inline_count": effective_skip_count,
        "blocked_count": blocked_count,
        "blocked_reason_code_counts": blocked_reason_counts,
        "latest_decision": rows[0] if rows else None,
        "latest_reconcile": latest_reconcile,
        "append_only_ready": append_only_ready,
        "effective_skip_disabled_in_pr6": True,
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_data_append_only_routing_decisions(
    connection: sqlite3.Connection,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM market_data_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_routing_row_to_dict(row) for row in rows]


def _persist_market_data_projection_routing_decision(
    connection: sqlite3.Connection,
    decision: MarketDataAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
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
            blocked_reason_codes_json,
            evidence_json,
            decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def _market_data_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, updated_at, created_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _outbox_is_ready(
    *,
    outbox_status: str | None,
    outbox_job_present: bool,
    min_outbox_status: str,
) -> bool:
    if not outbox_job_present:
        return False
    normalized = _normalize_outbox_status(outbox_status)
    if normalized in {"ERROR", "NOOP"}:
        return False
    required = _normalize_outbox_status(min_outbox_status)
    if required == "ENQUEUED":
        return normalized in {
            None,
            "ENQUEUED",
            "DUPLICATE",
            "PENDING",
            "PROCESSING",
            "APPLIED",
            "SKIPPED",
        }
    return normalized == required


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_data_projection_routing_decisions"
    ).fetchone()
    return int(row["count"])


def _count_decisions(connection: sqlite3.Connection, column: str) -> int:
    if column not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError(f"unsupported routing decision count column: {column}")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE {column} = 1
        """
    ).fetchone()
    return int(row["count"])


def _blocked_reason_code_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("blocked_reason_codes", []):
            if reason != "DRY_RUN_WOULD_SKIP_INLINE":
                counter[str(reason)] += 1
    return dict(sorted(counter.items()))


def _routing_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["dry_run_enabled"] = bool(payload["dry_run_enabled"])
    payload["cutover_enabled"] = bool(payload["cutover_enabled"])
    payload["reconcile_required"] = bool(payload["reconcile_required"])
    payload["append_only_ready"] = bool(payload["append_only_ready"])
    payload["outbox_job_present"] = bool(payload["outbox_job_present"])
    payload["would_skip_inline"] = bool(payload["would_skip_inline"])
    payload["effective_skip_inline"] = bool(payload["effective_skip_inline"])
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
