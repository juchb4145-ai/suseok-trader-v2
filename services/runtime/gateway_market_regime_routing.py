from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, TradingMode, TradingProfile
from services.market_context_service import get_market_context_status
from services.runtime.market_regime_projection_reconcile import (
    get_latest_market_regime_projection_reconcile,
)

PROJECTION_NAME_MARKET_REGIME = "market_regime"


@dataclass(frozen=True, kw_only=True)
class MarketRegimeAppendOnlyRoutingDecision:
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
    index_artifact_present: bool
    context_ready: bool
    worker_apply_enabled: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    effective_skip_disabled_in_pr18: bool
    blocked_reason_codes: tuple[str, ...]
    evidence: Mapping[str, Any] = field(default_factory=dict)
    decided_at: str = ""
    projection_name: str = PROJECTION_NAME_MARKET_REGIME
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
            "dry_run_enabled": self.dry_run_enabled,
            "reconcile_required": self.reconcile_required,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_status": self.outbox_status,
            "outbox_job_present": self.outbox_job_present,
            "index_artifact_present": self.index_artifact_present,
            "context_ready": self.context_ready,
            "worker_apply_enabled": self.worker_apply_enabled,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "effective_skip_disabled_in_pr18": self.effective_skip_disabled_in_pr18,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def decide_market_regime_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketRegimeAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    latest = get_latest_market_regime_projection_reconcile(connection)
    latest_run = latest.get("latest_run")
    latest_run = dict(latest_run) if isinstance(latest_run, Mapping) else {}
    latest_created_at = _optional_text(latest_run.get("created_at"))
    latest_age_sec = _age_seconds(latest_created_at)
    outbox = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _optional_text(
        (outbox.get("status") if outbox else None) or outbox_status
    )
    context_status = get_market_context_status(connection, settings=settings)
    context_ready = bool(context_status.get("status") == "PASS")
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_regime_apply_enabled
    )
    reconcile_required = bool(
        settings.gateway_market_regime_append_only_require_reconcile_pass
    )
    effective_guard = bool(
        settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
    )
    index_artifact_present = _market_index_sample_exists(connection, event.event_id)
    source_status = _gateway_event_status(connection, event.event_id)
    reasons: list[str] = []
    if event_type != "market_index_tick":
        reasons.append("NOT_MARKET_INDEX_TICK")
    if not _is_observe_safe(settings):
        reasons.append("MARKET_REGIME_CORE_NOT_OBSERVE_SAFE")
    if not settings.gateway_market_regime_append_only_dry_run_enabled:
        reasons.append("DRY_RUN_DISABLED")
    if source_status != "ACCEPTED":
        reasons.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    if outbox is None or normalized_outbox_status not in {
        "ENQUEUED",
        "PENDING",
        "PROCESSING",
        "APPLIED",
    }:
        reasons.append("MARKET_REGIME_OUTBOX_JOB_MISSING_OR_NOT_READY")
    if not index_artifact_present:
        reasons.append("MARKET_REGIME_INDEX_DEPENDENCY_MISSING")
    if not worker_apply_enabled:
        reasons.append("MARKET_REGIME_WORKER_APPLY_DISABLED")
    if not reconcile_required:
        reasons.append("MARKET_REGIME_RECONCILE_GUARD_DISABLED")
    if str(latest_run.get("status") or "") != "PASS":
        reasons.append("MARKET_REGIME_RECONCILE_NOT_PASS")
    if latest_age_sec is None or (
        latest_age_sec
        > settings.gateway_market_regime_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_REGIME_RECONCILE_STALE")
    if not bool(latest_run.get("append_only_ready")):
        reasons.append("MARKET_REGIME_APPEND_ONLY_NOT_READY")
    if not context_ready:
        reasons.append("MARKET_REGIME_CONTEXT_NOT_READY")
    if not effective_guard:
        reasons.append("PR18_EFFECTIVE_SKIP_GUARD_DISABLED")
    would_skip_inline = not reasons
    blocked_reason_codes = [*reasons]
    if would_skip_inline:
        blocked_reason_codes.append("EFFECTIVE_SKIP_DISABLED_IN_PR18")
    decision = MarketRegimeAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=settings.gateway_market_regime_append_only_dry_run_enabled,
        reconcile_required=reconcile_required,
        latest_reconcile_run_id=_optional_text(latest_run.get("run_id")),
        latest_reconcile_status=_optional_text(latest_run.get("status")),
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=bool(latest_run.get("append_only_ready")),
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox is not None,
        index_artifact_present=index_artifact_present,
        context_ready=context_ready,
        worker_apply_enabled=worker_apply_enabled,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=False,
        effective_skip_disabled_in_pr18=effective_guard,
        blocked_reason_codes=tuple(dict.fromkeys(blocked_reason_codes)),
        evidence={
            "source_status": source_status,
            "context_status": context_status,
            "outbox": outbox,
            "pr18_preparation_only": True,
            "inline_market_regime_path_retained": True,
            "no_order_side_effects": True,
        },
        decided_at=decided_at,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_regime_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    latest_row = connection.execute(
        """
        SELECT * FROM market_regime_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    counts = connection.execute(
        """
        SELECT
            COUNT(*) AS decision_count,
            SUM(would_skip_inline) AS would_skip_inline_count,
            SUM(effective_skip_inline) AS effective_skip_inline_count
        FROM market_regime_projection_routing_decisions
        """
    ).fetchone()
    latest = None if latest_row is None else _decision_row_to_dict(latest_row)
    return {
        "status": (
            "FAIL"
            if latest is not None and latest["effective_skip_inline"]
            else "PASS"
            if latest is not None and latest["would_skip_inline"]
            else "WARN"
        ),
        "dry_run_enabled": settings.gateway_market_regime_append_only_dry_run_enabled,
        "effective_skip_disabled_in_pr18": (
            settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
        ),
        "worker_apply_enabled": bool(
            settings.projection_outbox_apply_projection_enabled
            and settings.projection_outbox_market_regime_apply_enabled
        ),
        "decision_count": int(counts["decision_count"] or 0),
        "would_skip_inline_count": int(counts["would_skip_inline_count"] or 0),
        "effective_skip_inline_count": int(counts["effective_skip_inline_count"] or 0),
        "latest_decision": latest,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_regime_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT * FROM market_regime_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_decision_row_to_dict(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketRegimeAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_regime_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            reconcile_required, latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec, append_only_ready,
            outbox_status, outbox_job_present, index_artifact_present, context_ready,
            worker_apply_enabled, would_skip_inline, effective_skip_inline,
            effective_skip_disabled_in_pr18, blocked_reason_codes_json,
            evidence_json, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            index_artifact_present = excluded.index_artifact_present,
            context_ready = excluded.context_ready,
            worker_apply_enabled = excluded.worker_apply_enabled,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = 0,
            effective_skip_disabled_in_pr18 = excluded.effective_skip_disabled_in_pr18,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            decision.event_id,
            decision.event_type,
            decision.projection_name,
            int(decision.dry_run_enabled),
            int(decision.reconcile_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            int(decision.index_artifact_present),
            int(decision.context_ready),
            int(decision.worker_apply_enabled),
            int(decision.would_skip_inline),
            0,
            int(decision.effective_skip_disabled_in_pr18),
            json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            canonical_json(decision.evidence),
            decision.decided_at,
        ),
    )
    connection.commit()


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, attempts, updated_at
        FROM projection_outbox
        WHERE projection_name = ? AND event_id = ?
        LIMIT 1
        """,
        (PROJECTION_NAME_MARKET_REGIME, event_id),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


def _market_index_sample_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM market_index_tick_samples WHERE event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
        is not None
    )


def _gateway_event_status(connection: sqlite3.Connection, event_id: str) -> str | None:
    row = connection.execute(
        "SELECT status FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return None if row is None else str(row["status"])


def _is_observe_safe(settings: Settings) -> bool:
    return bool(
        settings.trading_profile is TradingProfile.OBSERVE
        and settings.trading_mode is TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
    )


def _age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(
            (utc_now() - parse_timestamp(value, "created_at")).total_seconds(), 0.0
        )
    except ValueError:
        return None


def _decision_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["blocked_reason_codes"] = json.loads(data.pop("blocked_reason_codes_json"))
    data["evidence"] = json.loads(data.pop("evidence_json"))
    for key in (
        "dry_run_enabled",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "index_artifact_present",
        "context_ready",
        "worker_apply_enabled",
        "would_skip_inline",
        "effective_skip_inline",
        "effective_skip_disabled_in_pr18",
    ):
        data[key] = bool(data[key])
    return data


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
