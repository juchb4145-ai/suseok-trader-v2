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
from services.market_index_service import (
    MARKET_INDEX_SOURCE_REALTIME,
    classify_market_index_data_source,
    market_index_parser_status,
    market_index_parser_verified,
    market_index_payload_usability,
)
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
)

PROJECTION_NAME_MARKET_INDEX = "market_index"
MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_IN_PR15"
)


@dataclass(frozen=True, kw_only=True)
class MarketIndexAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    reconcile_required: bool
    data_usable_required: bool
    parser_verified_required: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_job_present: bool
    outbox_status: str | None
    parser_status: str
    parser_verified: bool
    data_source: str
    data_usable: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    worker_apply_enabled: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    projection_name: str = PROJECTION_NAME_MARKET_INDEX
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
            "dry_run_enabled": self.dry_run_enabled,
            "cutover_enabled": self.cutover_enabled,
            "reconcile_required": self.reconcile_required,
            "data_usable_required": self.data_usable_required,
            "parser_verified_required": self.parser_verified_required,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_job_present": self.outbox_job_present,
            "outbox_status": self.outbox_status,
            "parser_status": self.parser_status,
            "parser_verified": self.parser_verified,
            "data_source": self.data_source,
            "data_usable": self.data_usable,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "worker_apply_enabled": self.worker_apply_enabled,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def decide_market_index_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketIndexAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    latest = get_latest_market_index_projection_reconcile(connection)
    latest_run = latest.get("latest_run")
    latest_status = _mapping_string(latest_run, "status")
    latest_run_id = _mapping_string(latest_run, "run_id")
    latest_created_at = _mapping_string(latest_run, "created_at")
    latest_age_sec = _age_seconds(latest_created_at)
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    outbox_job = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _normalize_outbox_status(
        outbox_status or (outbox_job.get("status") if outbox_job else None)
    )
    parser_status = market_index_parser_status(event.payload)
    parser_verified = market_index_parser_verified(event.payload)
    data_source = classify_market_index_data_source(event.payload)
    usability = market_index_payload_usability(event.payload)
    data_usable = bool(usability.get("data_usable"))
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_index_apply_enabled
    )
    reconcile_required = bool(
        settings.gateway_market_index_append_only_require_reconcile_pass
    )
    data_usable_required = bool(
        settings.gateway_market_index_append_only_require_data_usable
    )
    parser_verified_required = bool(
        settings.gateway_market_index_append_only_require_parser_verified
    )
    dry_run_enabled = bool(settings.gateway_market_index_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_index_append_only_cutover_enabled)
    source_status = _gateway_event_status(connection, event.event_id)

    reasons: list[str] = []
    would_skip_inline = False
    if event_type != "market_index_tick":
        reasons.append("NOT_MARKET_INDEX_TICK")
    elif not dry_run_enabled:
        reasons.append("DRY_RUN_DISABLED")
    elif source_status != "ACCEPTED":
        reasons.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    elif outbox_job is None or normalized_outbox_status not in {
        "ENQUEUED",
        "PENDING",
        "PROCESSING",
        "APPLIED",
    }:
        reasons.append("MARKET_INDEX_OUTBOX_JOB_MISSING_OR_NOT_READY")
    elif not worker_apply_enabled:
        reasons.append("MARKET_INDEX_WORKER_APPLY_DISABLED")
    elif reconcile_required and latest_run is None:
        reasons.append("MARKET_INDEX_RECONCILE_MISSING")
    elif reconcile_required and (latest_status != "PASS" or not append_only_ready):
        reasons.append("MARKET_INDEX_RECONCILE_NOT_PASS")
    elif (
        reconcile_required
        and latest_age_sec is not None
        and latest_age_sec
        > settings.gateway_market_index_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_INDEX_RECONCILE_STALE")
    elif data_usable_required and not data_usable:
        reasons.append("MARKET_INDEX_DATA_NOT_USABLE")
    elif parser_verified_required and not parser_verified:
        reasons.append("MARKET_INDEX_PARSER_NOT_VERIFIED")
    elif data_source != MARKET_INDEX_SOURCE_REALTIME:
        reasons.append("MARKET_INDEX_SOURCE_NOT_SUPPORTED_FOR_CUTOVER")
    else:
        would_skip_inline = True
        reasons.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    if cutover_enabled or would_skip_inline:
        reasons.append(MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON)
    evidence = {
        "pr": "PR-15",
        "cutover_status": "DRY_RUN_ONLY",
        "inline_market_index_projection_remains_enabled": True,
        "inline_market_regime_projection_remains_enabled": True,
        "effective_skip_disabled_in_pr15": bool(
            settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        ),
        "payload_usability": usability,
        "source_gateway_event_status": source_status,
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "latest_reconcile": latest_run,
        "parser_confidence_separate_from_data_usability": True,
        "tr_bootstrap_adapter_status": "NOT_IMPLEMENTED",
        "nxt_is_not_valid_market_index_evidence": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    decision = MarketIndexAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        reconcile_required=reconcile_required,
        data_usable_required=data_usable_required,
        parser_verified_required=parser_verified_required,
        latest_reconcile_status=latest_status,
        latest_reconcile_run_id=latest_run_id,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_job_present=outbox_job is not None,
        outbox_status=normalized_outbox_status,
        parser_status=parser_status,
        parser_verified=parser_verified,
        data_source=data_source,
        data_usable=data_usable,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        worker_apply_enabled=worker_apply_enabled,
        blocked_reason_codes=tuple(reasons),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_index_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_index_append_only_routing_decisions(connection, limit=500)
    latest_reconcile = get_latest_market_index_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    effective_count = _count_decisions(connection, "effective_skip_inline")
    would_count = _count_decisions(connection, "would_skip_inline")
    failures: list[str] = []
    if effective_count > 0:
        failures.append("MARKET_INDEX_EFFECTIVE_SKIP_FORBIDDEN_IN_PR15")
    if isinstance(latest_run, Mapping) and latest_run.get("status") == "FAIL":
        failures.append("MARKET_INDEX_RECONCILE_FAIL")
    warnings = [
        "PR-15 dry-run only; market_index and market_regime inline paths remain enabled",
        "TR bootstrap adapter is not implemented",
        "NXT is not accepted as KRX market index evidence",
    ]
    return {
        "pr": "PR-15",
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_index_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_index_append_only_cutover_enabled
        ),
        "effective_skip_disabled_in_pr15": True,
        "reconcile_required": bool(
            resolved_settings.gateway_market_index_append_only_require_reconcile_pass
        ),
        "data_usable_required": bool(
            resolved_settings.gateway_market_index_append_only_require_data_usable
        ),
        "parser_verified_required": bool(
            resolved_settings.gateway_market_index_append_only_require_parser_verified
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_index_append_only_reconcile_max_age_sec
        ),
        "worker_apply_enabled": bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_index_apply_enabled
        ),
        "total_decision_count": _count_all_decisions(connection),
        "would_skip_inline_count": would_count,
        "effective_skip_inline_count": effective_count,
        "blocked_reason_code_counts": _blocked_reason_counts(rows),
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
        "parser_confidence_separate_from_data_usability": True,
        "tr_bootstrap_adapter_status": "NOT_IMPLEMENTED",
        "failures": failures,
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_index_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_index_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketIndexAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_index_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled, cutover_enabled,
            reconcile_required, data_usable_required, parser_verified_required,
            latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec, append_only_ready,
            outbox_status, outbox_job_present, parser_status, parser_verified,
            data_source, data_usable, would_skip_inline, effective_skip_inline,
            worker_apply_enabled, blocked_reason_codes_json, evidence_json, decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            event_type = excluded.event_type,
            dry_run_enabled = excluded.dry_run_enabled,
            cutover_enabled = excluded.cutover_enabled,
            reconcile_required = excluded.reconcile_required,
            data_usable_required = excluded.data_usable_required,
            parser_verified_required = excluded.parser_verified_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            parser_status = excluded.parser_status,
            parser_verified = excluded.parser_verified,
            data_source = excluded.data_source,
            data_usable = excluded.data_usable,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            worker_apply_enabled = excluded.worker_apply_enabled,
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
            int(decision.data_usable_required),
            int(decision.parser_verified_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            decision.parser_status,
            int(decision.parser_verified),
            decision.data_source,
            int(decision.data_usable),
            int(decision.would_skip_inline),
            int(decision.effective_skip_inline),
            int(decision.worker_apply_enabled),
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


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, created_at, updated_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _gateway_event_status(connection: sqlite3.Connection, event_id: str) -> str | None:
    row = connection.execute(
        "SELECT status FROM gateway_events WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    return None if row is None else str(row["status"])


def _normalize_outbox_status(value: object) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _mapping_string(value: object, key: str) -> str | None:
    if not isinstance(value, Mapping):
        return None
    text = str(value.get(key) or "").strip()
    return text or None


def _age_seconds(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(str(value), "timestamp")
    except ValueError:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS count FROM market_index_projection_routing_decisions"
        ).fetchone()["count"]
    )


def _count_decisions(connection: sqlite3.Connection, field_name: str) -> int:
    if field_name not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError("unsupported decision field")
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM market_index_projection_routing_decisions "
            f"WHERE {field_name} = 1"
        ).fetchone()["count"]
    )


def _blocked_reason_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("blocked_reason_codes") or []:
            counts[str(reason)] += 1
    return dict(sorted(counts.items()))


def _row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "reconcile_required",
        "data_usable_required",
        "parser_verified_required",
        "append_only_ready",
        "outbox_job_present",
        "parser_verified",
        "data_usable",
        "would_skip_inline",
        "effective_skip_inline",
        "worker_apply_enabled",
    ):
        payload[key] = bool(payload[key])
    payload["blocked_reason_codes"] = _json_array(
        payload.pop("blocked_reason_codes_json", "[]")
    )
    payload["evidence"] = _json_object(payload.pop("evidence_json", "{}"))
    return payload


def _json_array(value: object) -> list[Any]:
    try:
        loaded = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _json_object(value: object) -> dict[str, Any]:
    try:
        loaded = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}
