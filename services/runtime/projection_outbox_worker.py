from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from storage.projection_outbox import (
    claim_projection_outbox_jobs,
    get_projection_outbox_status,
    mark_projection_outbox_applied,
    mark_projection_outbox_error,
    mark_projection_outbox_skipped,
)

from services.config import Settings, load_settings
from services.market_data_service import (
    MARKET_DATA_EVENT_TYPES,
    QUOTE_ONLY_REAL_TYPES,
    normalize_market_data_exchange,
    process_gateway_event,
)
from services.runtime.gateway_projection_routing import (
    record_market_data_post_apply_deferred_side_effects,
)
from services.runtime.incremental_evaluation import enqueue_incremental_evaluation_for_event

APPLY_MODE_SHADOW_VERIFY_ONLY = "SHADOW_VERIFY_ONLY"
APPLY_MODE_MARKET_DATA_APPLY = "MARKET_DATA_APPLY"


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxVerificationResult:
    status: str
    reason: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "evidence": dict(self.evidence),
            "error_message": self.error_message,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxBatchResult:
    run_id: str
    status: str
    claimed_count: int
    applied_count: int
    skipped_count: int
    error_count: int
    dead_letter_count: int
    remaining_pending_count: int
    shadow_mode: bool = True
    apply_projection: bool = False
    apply_projection_requested: bool = False
    apply_projection_effective: bool = False
    market_data_apply_enabled: bool = False
    applied_by_verify_count: int = 0
    applied_by_worker_count: int = 0
    skipped_apply_disabled_count: int = 0
    projection_apply_error_count: int = 0
    mutated_projection_names: tuple[str, ...] = ()
    no_trading_side_effects: bool = True
    projection_side_effects_allowed: bool = False
    errors: tuple[dict[str, Any], ...] = ()
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "claimed_count": self.claimed_count,
            "applied_count": self.applied_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
            "dead_letter_count": self.dead_letter_count,
            "remaining_pending_count": self.remaining_pending_count,
            "shadow_mode": self.shadow_mode,
            "apply_projection": self.apply_projection,
            "apply_projection_requested": self.apply_projection_requested,
            "apply_projection_effective": self.apply_projection_effective,
            "market_data_apply_enabled": self.market_data_apply_enabled,
            "applied_by_verify_count": self.applied_by_verify_count,
            "applied_by_worker_count": self.applied_by_worker_count,
            "skipped_apply_disabled_count": self.skipped_apply_disabled_count,
            "projection_apply_error_count": self.projection_apply_error_count,
            "mutated_projection_names": list(self.mutated_projection_names),
            "no_trading_side_effects": self.no_trading_side_effects,
            "projection_side_effects_allowed": self.projection_side_effects_allowed,
            "errors": list(self.errors),
            "created_at": self.created_at,
        }


def process_projection_outbox_batch(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int | None = None,
    owner_id: str | None = None,
    apply_projection: bool | None = None,
) -> ProjectionOutboxBatchResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("projection_outbox_shadow")
    resolved_owner_id = owner_id or run_id
    apply_requested = (
        bool(resolved_settings.projection_outbox_apply_projection_enabled)
        if apply_projection is None
        else bool(apply_projection)
    )
    global_apply_enabled = bool(resolved_settings.projection_outbox_apply_projection_enabled)
    market_data_apply_enabled = bool(
        resolved_settings.projection_outbox_market_data_apply_enabled
    )
    apply_effective = apply_requested and global_apply_enabled and market_data_apply_enabled
    apply_mode = (
        APPLY_MODE_MARKET_DATA_APPLY if apply_effective else APPLY_MODE_SHADOW_VERIFY_ONLY
    )
    bounded_limit = limit or (
        resolved_settings.projection_outbox_apply_batch_size
        if apply_effective
        else resolved_settings.projection_outbox_batch_size
    )
    min_age_sec = (
        resolved_settings.projection_outbox_apply_min_age_sec
        if apply_effective
        else resolved_settings.projection_outbox_shadow_min_age_sec
    )
    created_at = datetime_to_wire(utc_now())
    claimed_jobs = claim_projection_outbox_jobs(
        connection,
        owner_id=resolved_owner_id,
        limit=bounded_limit,
        processing_ttl_sec=resolved_settings.projection_outbox_processing_ttl_sec,
        min_age_sec=min_age_sec,
    )
    applied_count = 0
    skipped_count = 0
    error_count = 0
    dead_letter_count = 0
    applied_by_verify_count = 0
    applied_by_worker_count = 0
    skipped_apply_disabled_count = 0
    projection_apply_error_count = 0
    mutated_projection_names: set[str] = set()
    errors: list[dict[str, Any]] = []

    for job in claimed_jobs:
        if apply_requested and not apply_effective:
            verification = _verification_skipped(
                "APPLY_DISABLED_BY_SETTINGS",
                projection_name=job.get("projection_name"),
                event_id=job.get("event_id"),
                apply_projection_enabled=global_apply_enabled,
                market_data_apply_enabled=market_data_apply_enabled,
            )
        elif apply_effective:
            verification = apply_projection_outbox_job(
                connection,
                job,
                settings=resolved_settings,
                worker_run_id=run_id,
                owner_id=resolved_owner_id,
            )
        else:
            verification = verify_projection_outbox_job(
                connection,
                job,
                settings=resolved_settings,
            )
        evidence = {
            **dict(verification.evidence),
            "verification_reason": verification.reason,
            "worker_run_id": run_id,
            "shadow_mode": True,
            "projection_name": job.get("projection_name"),
            "event_id": job.get("event_id"),
            "event_type": job.get("event_type"),
            "apply_mode": apply_mode,
            "apply_projection": apply_effective,
            "apply_projection_requested": apply_requested,
            "apply_projection_enabled": global_apply_enabled,
            "market_data_apply_enabled": market_data_apply_enabled,
            "no_trading_side_effects": True,
            "projection_side_effects_allowed": apply_effective,
        }
        outbox_id = str(job["outbox_id"])
        if verification.status == "APPLIED":
            mark_projection_outbox_applied(
                connection,
                outbox_id,
                owner_id=resolved_owner_id,
                evidence=evidence,
            )
            applied_count += 1
            if evidence.get("apply_result") == "APPLIED_BY_WORKER":
                applied_by_worker_count += 1
            else:
                applied_by_verify_count += 1
            mutated_projection_name = evidence.get("mutated_projection_name")
            if mutated_projection_name:
                mutated_projection_names.add(str(mutated_projection_name))
        elif verification.status == "SKIPPED":
            mark_projection_outbox_skipped(
                connection,
                outbox_id,
                owner_id=resolved_owner_id,
                reason=verification.reason,
                evidence=evidence,
            )
            skipped_count += 1
            if verification.reason in {
                "APPLY_DISABLED_BY_SETTINGS",
                "APPLY_NOT_ENABLED_FOR_PROJECTION",
            }:
                skipped_apply_disabled_count += 1
        else:
            message = verification.error_message or verification.reason
            if evidence.get("apply_result") == "APPLY_ERROR":
                projection_apply_error_count += 1
            will_dead_letter = (
                int(job.get("attempts") or 0) + 1
                >= resolved_settings.projection_outbox_retry_limit
            )
            mark_projection_outbox_error(
                connection,
                outbox_id,
                owner_id=resolved_owner_id,
                error_message=message,
                retry_limit=resolved_settings.projection_outbox_retry_limit,
                evidence=evidence,
            )
            errors.append(
                {
                    "outbox_id": outbox_id,
                    "projection_name": job.get("projection_name"),
                    "event_id": job.get("event_id"),
                    "reason": verification.reason,
                    "error_message": message,
                    "dead_letter": will_dead_letter,
                    "apply_mode": apply_mode,
                }
            )
            if will_dead_letter:
                dead_letter_count += 1
            else:
                error_count += 1

    status = "NOOP"
    if claimed_jobs:
        status = "COMPLETED_WITH_ERRORS" if errors else "COMPLETED"
    outbox_status = get_projection_outbox_status(connection, settings=resolved_settings)
    return ProjectionOutboxBatchResult(
        run_id=run_id,
        status=status,
        claimed_count=len(claimed_jobs),
        applied_count=applied_count,
        skipped_count=skipped_count,
        error_count=error_count,
        dead_letter_count=dead_letter_count,
        remaining_pending_count=int(outbox_status["pending_count"]),
        apply_projection=apply_effective,
        apply_projection_requested=apply_requested,
        apply_projection_effective=apply_effective,
        market_data_apply_enabled=market_data_apply_enabled,
        applied_by_verify_count=applied_by_verify_count,
        applied_by_worker_count=applied_by_worker_count,
        skipped_apply_disabled_count=skipped_apply_disabled_count,
        projection_apply_error_count=projection_apply_error_count,
        mutated_projection_names=tuple(sorted(mutated_projection_names)),
        projection_side_effects_allowed=apply_effective,
        errors=tuple(errors),
        created_at=created_at,
    )


def apply_projection_outbox_job(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    *,
    settings: Settings,
    worker_run_id: str,
    owner_id: str,
) -> ProjectionOutboxVerificationResult:
    del owner_id
    projection_name = str(job.get("projection_name") or "").strip()
    event_id = str(job.get("event_id") or "").strip()
    event_type = str(job.get("event_type") or "").strip().lower()
    if projection_name != "market_data":
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    source_event = _gateway_event_row(connection, event_id)
    if source_event is None:
        return _verification_error(
            "SOURCE_GATEWAY_EVENT_MISSING",
            f"gateway_event not found: {event_id}",
            apply_result="APPLY_ERROR",
        )
    if str(source_event["status"]) != "ACCEPTED":
        return _verification_skipped(
            "SKIPPED_SOURCE_NOT_ACCEPTED",
            event_id=event_id,
            source_status=source_event["status"],
        )
    if event_type not in MARKET_DATA_EVENT_TYPES:
        return _verification_skipped(
            "MARKET_DATA_APPLY_EVENT_TYPE_UNSUPPORTED",
            event_id=event_id,
            event_type=event_type,
        )
    return _apply_market_data_projection(
        connection,
        job,
        source_event,
        settings=settings,
        worker_run_id=worker_run_id,
    )


def _apply_market_data_projection(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    source_event: Mapping[str, Any],
    *,
    settings: Settings,
    worker_run_id: str,
) -> ProjectionOutboxVerificationResult:
    event_id = str(job.get("event_id") or "")
    event_type = str(job.get("event_type") or "").lower()
    verification_before = verify_projection_outbox_job(connection, job, settings=settings)
    verification_before_payload = verification_before.to_dict()
    effective_gateway_skip = (
        event_type == "price_tick"
        and _routing_decision_effective_skip_inline(connection, event_id)
    )
    if verification_before.status == "APPLIED":
        return _verification_applied(
            "MARKET_DATA_ALREADY_APPLIED_BY_INLINE",
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLIED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
            projection_result_status=None,
        )
    if verification_before.status == "SKIPPED" and not (
        effective_gateway_skip
        and verification_before.reason == "MARKET_DATA_PRICE_TICK_OLDER_THAN_LATEST"
    ):
        return _verification_skipped(
            verification_before.reason,
            **dict(verification_before.evidence),
            apply_result="SKIPPED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
        )

    try:
        event = _gateway_event_from_row(source_event)
        projection_result = process_gateway_event(connection, event, settings=settings)
    except Exception as exc:
        return _verification_error(
            "MARKET_DATA_APPLY_EXCEPTION",
            str(exc),
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )

    projection_result_status = projection_result.status
    projection_result_payload = {
        "event_id": projection_result.event_id,
        "event_type": projection_result.event_type,
        "status": projection_result.status,
        "applied_count": projection_result.applied_count,
        "ignored_count": projection_result.ignored_count,
        "error_count": projection_result.error_count,
        "error_message": projection_result.error_message,
    }
    if projection_result.status == "ERROR":
        return _verification_error(
            "MARKET_DATA_APPLY_FAILED",
            projection_result.error_message or projection_result.status,
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
            projection_result_status=projection_result_status,
            projection_result=projection_result_payload,
        )

    post_apply_side_effects: dict[str, Any] = {}
    if event_type == "price_tick" and projection_result.applied_count > 0:
        post_apply_side_effects = _enqueue_deferred_incremental_evaluation_for_price_tick(
            connection,
            event,
            settings=settings,
        )
        record_market_data_post_apply_deferred_side_effects(
            connection,
            event_id,
            post_apply_side_effects,
        )

    verification_after = verify_projection_outbox_job(connection, job, settings=settings)
    evidence = {
        "event_id": event_id,
        "event_type": event_type,
        "verification_before_apply": verification_before_payload,
        "verification_after_apply": verification_after.to_dict(),
        "projection_result_status": projection_result_status,
        "projection_result": projection_result_payload,
        "post_apply_side_effects": post_apply_side_effects,
    }
    if verification_after.status == "APPLIED":
        if projection_result.applied_count > 0:
            evidence["apply_result"] = "APPLIED_BY_WORKER"
            evidence["mutated_projection_name"] = "market_data"
        else:
            evidence["apply_result"] = "APPLIED_BY_VERIFY"
        return _verification_applied(
            "MARKET_DATA_APPLIED_BY_WORKER"
            if projection_result.applied_count > 0
            else "MARKET_DATA_ALREADY_APPLIED_BY_INLINE",
            **evidence,
        )
    if verification_after.status == "SKIPPED":
        return _verification_skipped(
            verification_after.reason,
            **dict(verification_after.evidence),
            **evidence,
            apply_result="SKIPPED_AFTER_APPLY",
        )
    return _verification_error(
        verification_after.reason,
        verification_after.error_message or verification_after.reason,
        **evidence,
        apply_result="APPLY_ERROR",
    )


def verify_projection_outbox_job(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    *,
    settings: Settings | None = None,
) -> ProjectionOutboxVerificationResult:
    resolved_settings = settings or load_settings()
    event_id = str(job.get("event_id") or "").strip()
    event_type = str(job.get("event_type") or "").strip().lower()
    projection_name = str(job.get("projection_name") or "").strip()
    if not event_id or not event_type or not projection_name:
        return _verification_error("INVALID_OUTBOX_JOB", "outbox job is missing keys")

    source_event = _gateway_event_row(connection, event_id)
    if source_event is None:
        return _verification_error(
            "SOURCE_GATEWAY_EVENT_MISSING",
            f"gateway_event not found: {event_id}",
        )
    if str(source_event["status"]) != "ACCEPTED":
        return _verification_skipped(
            "SKIPPED_SOURCE_NOT_ACCEPTED",
            event_id=event_id,
            source_status=source_event["status"],
        )

    payload = _json_object(source_event["payload_json"])
    if projection_name == "market_data":
        return _verify_market_data(connection, event_id, event_type, payload, source_event)
    if projection_name == "condition_fusion":
        return _verify_condition_fusion(
            connection,
            event_id,
            payload,
            settings=resolved_settings,
        )
    if projection_name == "market_reference":
        return _verify_market_reference(connection, event_id, payload)
    if projection_name == "market_index":
        return _verify_market_index(connection, event_id)
    if projection_name == "market_regime":
        return _verify_market_regime(connection, event_id)
    if projection_name == "market_scan":
        return _verify_market_scan(connection, event_id, payload)
    return _verification_skipped(
        "SHADOW_VERIFY_NOT_SUPPORTED",
        projection_name=projection_name,
        event_id=event_id,
    )


def _verify_market_data(
    connection: sqlite3.Connection,
    event_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    source_event: Mapping[str, Any],
) -> ProjectionOutboxVerificationResult:
    inline_error = _market_data_inline_error(connection, event_id)
    if inline_error is not None:
        return _verification_applied(
            "APPLIED_WITH_INLINE_ERROR",
            event_id=event_id,
            inline_projection_status="ERROR",
            inline_error=inline_error,
        )
    if event_type == "price_tick":
        if _event_id_exists(connection, "market_tick_samples", event_id):
            return _verification_applied(
                "MARKET_DATA_PRICE_TICK_SAMPLE_OBSERVED",
                event_id=event_id,
                table="market_tick_samples",
            )
        skipped = _classify_missing_price_tick_sample(
            connection,
            event_id,
            payload,
            source_event,
        )
        if skipped is not None:
            return skipped
        return _verification_error(
            "MARKET_DATA_PRICE_TICK_SAMPLE_MISSING",
            f"market_tick_samples missing for event_id={event_id}",
        )
    if event_type == "condition_event":
        return _verify_event_id_table(
            connection,
            "market_condition_signals",
            event_id,
            success_reason="MARKET_DATA_CONDITION_SIGNAL_OBSERVED",
            error_reason="MARKET_DATA_CONDITION_SIGNAL_MISSING",
        )
    if event_type == "tr_response":
        if not _rows_payload_has_rows(payload):
            return _verification_skipped("MARKET_DATA_TR_RESPONSE_NO_ROWS", event_id=event_id)
        return _verify_event_id_table(
            connection,
            "market_tr_snapshots",
            event_id,
            success_reason="MARKET_DATA_TR_SNAPSHOT_OBSERVED",
            error_reason="MARKET_DATA_TR_SNAPSHOT_MISSING",
        )
    return _verification_skipped(
        "MARKET_DATA_SHADOW_VERIFY_NOT_SUPPORTED",
        event_id=event_id,
        event_type=event_type,
    )


def _verify_condition_fusion(
    connection: sqlite3.Connection,
    event_id: str,
    payload: Mapping[str, Any],
    *,
    settings: Settings,
) -> ProjectionOutboxVerificationResult:
    if not settings.condition_fusion_event_incremental_enabled:
        return _verification_skipped(
            "CONDITION_FUSION_INCREMENTAL_DISABLED",
            event_id=event_id,
        )
    try:
        condition = BrokerConditionEvent.from_dict(payload)
    except Exception as exc:
        return _verification_error(
            "CONDITION_FUSION_PAYLOAD_INVALID",
            str(exc),
        )
    row = connection.execute(
        """
        SELECT latest_event_id
        FROM candidate_condition_fusion
        WHERE code = ?
        LIMIT 1
        """,
        (condition.code,),
    ).fetchone()
    if row is None:
        return _verification_skipped(
            "CONDITION_FUSION_SHADOW_VERIFY_NOT_SUPPORTED",
            event_id=event_id,
            code=condition.code,
        )
    return _verification_applied(
        "CONDITION_FUSION_ROW_OBSERVED",
        event_id=event_id,
        code=condition.code,
        latest_event_id=row["latest_event_id"],
    )


def _verify_market_reference(
    connection: sqlite3.Connection,
    event_id: str,
    payload: Mapping[str, Any],
) -> ProjectionOutboxVerificationResult:
    if _event_id_exists(connection, "market_symbol_memberships", event_id):
        return _verification_applied(
            "MARKET_REFERENCE_SYMBOL_MEMBERSHIP_OBSERVED",
            event_id=event_id,
        )
    if not _market_symbols_payload_has_symbols(payload):
        return _verification_skipped("MARKET_REFERENCE_NO_SYMBOLS", event_id=event_id)
    return _verification_error(
        "MARKET_REFERENCE_SYMBOL_MEMBERSHIP_MISSING",
        f"market_symbol_memberships missing for event_id={event_id}",
    )


def _verify_market_index(
    connection: sqlite3.Connection,
    event_id: str,
) -> ProjectionOutboxVerificationResult:
    inline_error = _market_index_inline_error(connection, event_id)
    if inline_error is not None:
        return _verification_applied(
            "APPLIED_WITH_INLINE_ERROR",
            event_id=event_id,
            inline_projection_status="ERROR",
            inline_error=inline_error,
        )
    return _verify_event_id_table(
        connection,
        "market_index_tick_samples",
        event_id,
        success_reason="MARKET_INDEX_TICK_SAMPLE_OBSERVED",
        error_reason="MARKET_INDEX_TICK_SAMPLE_MISSING",
    )


def _verify_market_regime(
    connection: sqlite3.Connection,
    event_id: str,
) -> ProjectionOutboxVerificationResult:
    count = _count_rows(connection, "market_regime_snapshots")
    if count > 0:
        return _verification_applied(
            "MARKET_REGIME_SNAPSHOT_OBSERVED",
            event_id=event_id,
            snapshot_count=count,
        )
    return _verification_skipped(
        "MARKET_REGIME_SHADOW_VERIFY_UNSAFE",
        event_id=event_id,
        apply_projection=False,
    )


def _verify_market_scan(
    connection: sqlite3.Connection,
    event_id: str,
    payload: Mapping[str, Any],
) -> ProjectionOutboxVerificationResult:
    inline_error = _market_scan_inline_error(connection, event_id)
    if inline_error is not None:
        return _verification_applied(
            "APPLIED_WITH_INLINE_ERROR",
            event_id=event_id,
            inline_projection_status="ERROR",
            inline_error=inline_error,
        )
    request_id = str(payload.get("request_id") or "").strip()
    if request_id:
        row = connection.execute(
            """
            SELECT 1
            FROM market_scan_snapshots
            WHERE json_extract(metadata_json, '$.request_id') = ?
            LIMIT 1
            """,
            (request_id,),
        ).fetchone()
        if row is not None:
            return _verification_applied(
                "MARKET_SCAN_SNAPSHOT_OBSERVED",
                event_id=event_id,
                request_id=request_id,
            )
    return _verification_skipped(
        "MARKET_SCAN_SHADOW_VERIFY_NOT_SUPPORTED",
        event_id=event_id,
        request_id=request_id,
    )


def _verify_event_id_table(
    connection: sqlite3.Connection,
    table_name: str,
    event_id: str,
    *,
    success_reason: str,
    error_reason: str,
) -> ProjectionOutboxVerificationResult:
    if _event_id_exists(connection, table_name, event_id):
        return _verification_applied(success_reason, event_id=event_id, table=table_name)
    return _verification_error(
        error_reason,
        f"{table_name} missing for event_id={event_id}",
    )


def _event_id_exists(
    connection: sqlite3.Connection,
    table_name: str,
    event_id: str,
) -> bool:
    row = connection.execute(
        f"SELECT 1 FROM {table_name} WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    return row is not None


def _gateway_event_row(
    connection: sqlite3.Connection,
    event_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            event_id,
            event_type,
            source,
            command_id,
            idempotency_key,
            status,
            event_ts,
            payload_json
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()


def _gateway_event_from_row(row: Mapping[str, Any]) -> GatewayEvent:
    return GatewayEvent(
        event_id=str(row["event_id"]),
        event_type=str(row["event_type"]),
        source=str(row["source"]),
        command_id=row["command_id"],
        idempotency_key=row["idempotency_key"],
        ts=parse_timestamp(row["event_ts"], "event_ts"),
        payload=_json_object(str(row["payload_json"])),
    )


def _enqueue_deferred_incremental_evaluation_for_price_tick(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
) -> dict[str, Any]:
    deferred_from_gateway_path = _routing_decision_effective_skip_inline(
        connection,
        event.event_id,
    )
    try:
        result = enqueue_incremental_evaluation_for_event(
            connection,
            event,
            settings=settings,
        )
    except Exception as exc:
        return {
            "incremental_evaluation_enqueue_status": "ERROR",
            "error_message": str(exc),
            "deferred_from_gateway_path": deferred_from_gateway_path,
            "no_order_side_effects": True,
        }
    return {
        "incremental_evaluation_enqueue_status": result.status,
        "enqueued_count": result.enqueued_count,
        "candidate_ids": list(result.candidate_ids),
        "code": result.code,
        "deferred_from_gateway_path": deferred_from_gateway_path,
        "no_order_side_effects": True,
    }


def _routing_decision_effective_skip_inline(
    connection: sqlite3.Connection,
    event_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT effective_skip_inline
        FROM market_data_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_data'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return bool(row is not None and row["effective_skip_inline"])


def _classify_missing_price_tick_sample(
    connection: sqlite3.Connection,
    event_id: str,
    payload: Mapping[str, Any],
    source_event: Mapping[str, Any],
) -> ProjectionOutboxVerificationResult | None:
    real_type = _price_tick_payload_real_type(payload)
    if real_type in QUOTE_ONLY_REAL_TYPES:
        return _verification_skipped(
            "MARKET_DATA_PRICE_TICK_QUOTE_ONLY",
            event_id=event_id,
            real_type=real_type,
        )
    try:
        code = validate_stock_code(payload.get("code"))
        exchange = _price_tick_payload_exchange(payload)
    except ValueError:
        return None
    latest = connection.execute(
        """
        SELECT event_id, event_ts
        FROM market_ticks_latest
        WHERE code = ? AND exchange = ?
        """,
        (code, exchange),
    ).fetchone()
    if latest is None:
        return None
    source_event_ts = str(source_event["event_ts"] or "")
    if source_event_ts and _timestamp_is_before(source_event_ts, str(latest["event_ts"])):
        return _verification_skipped(
            "MARKET_DATA_PRICE_TICK_OLDER_THAN_LATEST",
            event_id=event_id,
            code=code,
            exchange=exchange,
            source_event_ts=source_event_ts,
            latest_event_id=latest["event_id"],
            latest_event_ts=latest["event_ts"],
        )
    return None


def _price_tick_payload_real_type(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return ""
    return str(metadata.get("real_type") or "").strip()


def _price_tick_payload_exchange(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("exchange") is not None:
        return normalize_market_data_exchange(metadata.get("exchange"))
    if payload.get("exchange") is not None:
        return normalize_market_data_exchange(payload.get("exchange"))
    return "KRX"


def _timestamp_is_before(incoming: str, current: str) -> bool:
    try:
        return parse_timestamp(incoming, "incoming_event_ts") < parse_timestamp(
            current,
            "current_event_ts",
        )
    except ValueError:
        return str(incoming) < str(current)


def _market_data_inline_error(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    return _latest_inline_error(connection, "market_projection_errors", event_id)


def _market_index_inline_error(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    return _latest_inline_error(connection, "market_index_projection_errors", event_id)


def _market_scan_inline_error(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    return _latest_inline_error(connection, "market_scan_errors", event_id)


def _latest_inline_error(
    connection: sqlite3.Connection,
    table_name: str,
    event_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE event_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


def _rows_payload_has_rows(payload: Mapping[str, Any]) -> bool:
    rows = payload.get("rows")
    return isinstance(rows, list) and bool(rows)


def _market_symbols_payload_has_symbols(payload: Mapping[str, Any]) -> bool:
    markets = payload.get("markets")
    if isinstance(markets, Mapping):
        return any(isinstance(symbols, list) and bool(symbols) for symbols in markets.values())
    if isinstance(markets, list):
        for market in markets:
            if not isinstance(market, Mapping):
                continue
            symbols = market.get("symbols")
            if isinstance(symbols, list) and bool(symbols):
                return True
    return False


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return 0 if row is None else int(row["count"])


def _json_object(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _verification_applied(reason: str, **evidence: Any) -> ProjectionOutboxVerificationResult:
    return ProjectionOutboxVerificationResult(
        status="APPLIED",
        reason=reason,
        evidence=evidence,
    )


def _verification_skipped(reason: str, **evidence: Any) -> ProjectionOutboxVerificationResult:
    return ProjectionOutboxVerificationResult(
        status="SKIPPED",
        reason=reason,
        evidence=evidence,
    )


def _verification_error(
    reason: str,
    error_message: str,
    **evidence: Any,
) -> ProjectionOutboxVerificationResult:
    normalized_message = f"{reason}: {error_message}"
    resolved_evidence = {"reason": reason, **evidence}
    return ProjectionOutboxVerificationResult(
        status="ERROR",
        reason=reason,
        evidence=resolved_evidence,
        error_message=normalized_message,
    )
