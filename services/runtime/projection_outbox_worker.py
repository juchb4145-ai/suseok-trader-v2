from __future__ import annotations

import json
import sqlite3
import time
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
    mark_projection_outbox_retryable_error,
    mark_projection_outbox_skipped,
    reset_stale_projection_outbox_processing,
)
from storage.sqlite_locking import retry_sqlite_locked

from services.config import Settings, TradingMode, TradingProfile, load_settings
from services.market_context_service import (
    get_market_context_status,
    rebuild_market_context_snapshots,
    should_rebuild_market_context_snapshots,
)
from services.market_data_service import (
    MARKET_DATA_EVENT_TYPES,
    QUOTE_ONLY_REAL_TYPES,
    normalize_market_data_exchange,
    process_gateway_event,
)
from services.market_index_service import process_market_index_event
from services.market_reference_service import (
    market_symbols_payload_has_symbols,
    process_market_symbols_event,
)
from services.market_scan_service import process_market_scan_event
from services.runtime.gateway_projection_routing import (
    record_market_data_post_apply_deferred_side_effects,
)
from services.runtime.market_data_projection_side_effects import (
    enqueue_incremental_for_candidate_quote_refresh_tr_response,
    enqueue_incremental_for_price_tick_projection,
    refresh_condition_fusion_for_condition_event_projection,
)

APPLY_MODE_SHADOW_VERIFY_ONLY = "SHADOW_VERIFY_ONLY"
APPLY_MODE_MARKET_DATA_APPLY = "MARKET_DATA_APPLY"
APPLY_MODE_MARKET_REFERENCE_APPLY = "MARKET_REFERENCE_APPLY"
APPLY_MODE_MARKET_INDEX_APPLY = "MARKET_INDEX_APPLY"
APPLY_MODE_MARKET_INDEX_REGIME_VERIFY = "MARKET_INDEX_REGIME_VERIFY"
APPLY_MODE_MARKET_REGIME_APPLY = "MARKET_REGIME_APPLY"
APPLY_MODE_MARKET_SCAN_APPLY = "MARKET_SCAN_APPLY"


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
    market_reference_apply_enabled: bool = False
    market_index_apply_enabled: bool = False
    market_regime_apply_enabled: bool = False
    market_scan_apply_enabled: bool = False
    applied_by_verify_count: int = 0
    applied_by_worker_count: int = 0
    skipped_apply_disabled_count: int = 0
    projection_apply_error_count: int = 0
    locked_retry_count: int = 0
    locked_job_count: int = 0
    lock_retry_exhausted: bool = False
    retryable: bool = False
    partial_result: bool = False
    max_wall_ms_exceeded: bool = False
    effective_limit: int | None = None
    requested_limit: int | None = None
    projection_name_filter: str | None = None
    live_safe: bool = False
    stale_processing_reset_count: int = 0
    reason_codes: tuple[str, ...] = ()
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
            "market_reference_apply_enabled": self.market_reference_apply_enabled,
            "market_index_apply_enabled": self.market_index_apply_enabled,
            "market_regime_apply_enabled": self.market_regime_apply_enabled,
            "market_scan_apply_enabled": self.market_scan_apply_enabled,
            "applied_by_verify_count": self.applied_by_verify_count,
            "applied_by_worker_count": self.applied_by_worker_count,
            "skipped_apply_disabled_count": self.skipped_apply_disabled_count,
            "projection_apply_error_count": self.projection_apply_error_count,
            "locked_retry_count": self.locked_retry_count,
            "locked_job_count": self.locked_job_count,
            "lock_retry_exhausted": self.lock_retry_exhausted,
            "retryable": self.retryable,
            "partial_result": self.partial_result,
            "max_wall_ms_exceeded": self.max_wall_ms_exceeded,
            "effective_limit": self.effective_limit,
            "requested_limit": self.requested_limit,
            "projection_name_filter": self.projection_name_filter,
            "live_safe": self.live_safe,
            "stale_processing_reset_count": self.stale_processing_reset_count,
            "reason_codes": list(self.reason_codes),
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
    live_safe: bool = False,
    projection_name: str | None = None,
) -> ProjectionOutboxBatchResult:
    resolved_settings = settings or load_settings()
    started_at = time.monotonic()
    run_id = new_message_id("projection_outbox_shadow")
    resolved_owner_id = owner_id or run_id
    normalized_projection_name = (
        None if projection_name is None else str(projection_name).strip().lower()
    )
    if normalized_projection_name == "":
        raise ValueError("projection_name must not be empty")
    apply_requested = (
        bool(resolved_settings.projection_outbox_apply_projection_enabled)
        if apply_projection is None
        else bool(apply_projection)
    )
    global_apply_enabled = bool(resolved_settings.projection_outbox_apply_projection_enabled)
    market_data_apply_enabled = bool(
        resolved_settings.projection_outbox_market_data_apply_enabled
    )
    market_reference_apply_enabled = bool(
        resolved_settings.projection_outbox_market_reference_apply_enabled
    )
    market_index_apply_enabled = bool(
        resolved_settings.projection_outbox_market_index_apply_enabled
    )
    market_regime_apply_enabled = bool(
        resolved_settings.projection_outbox_market_regime_apply_enabled
    )
    market_scan_apply_enabled = bool(
        resolved_settings.projection_outbox_market_scan_apply_enabled
    )
    apply_effective = apply_requested and global_apply_enabled and (
        market_data_apply_enabled
        or market_reference_apply_enabled
        or market_index_apply_enabled
        or market_regime_apply_enabled
        or market_scan_apply_enabled
    )
    requested_limit = limit
    bounded_limit = limit or _apply_batch_size(
        settings=resolved_settings,
        projection_name=normalized_projection_name,
        apply_effective=apply_effective,
        market_data_apply_enabled=market_data_apply_enabled,
        market_reference_apply_enabled=market_reference_apply_enabled,
        market_index_apply_enabled=market_index_apply_enabled,
        market_regime_apply_enabled=market_regime_apply_enabled,
        market_scan_apply_enabled=market_scan_apply_enabled,
    )
    if live_safe:
        bounded_limit = min(
            int(bounded_limit),
            int(resolved_settings.projection_outbox_live_run_once_batch_size),
        )
    min_age_sec = _apply_min_age_sec(
        settings=resolved_settings,
        projection_name=normalized_projection_name,
        apply_effective=apply_effective,
        market_data_apply_enabled=market_data_apply_enabled,
        market_reference_apply_enabled=market_reference_apply_enabled,
        market_index_apply_enabled=market_index_apply_enabled,
        market_regime_apply_enabled=market_regime_apply_enabled,
        market_scan_apply_enabled=market_scan_apply_enabled,
    )
    created_at = datetime_to_wire(utc_now())
    locked_retry_count = 0

    def _on_locked_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    stale_processing_reset_count = retry_sqlite_locked(
        lambda: reset_stale_projection_outbox_processing(
            connection,
            stale_sec=resolved_settings.projection_outbox_processing_ttl_sec,
        ),
        attempts=resolved_settings.operator_sqlite_lock_retry_attempts,
        base_sleep_sec=resolved_settings.operator_sqlite_lock_retry_base_sleep_sec,
        max_sleep_sec=resolved_settings.operator_sqlite_lock_retry_max_sleep_sec,
        on_retry=_on_locked_retry,
    )
    claimed_jobs = retry_sqlite_locked(
        lambda: claim_projection_outbox_jobs(
            connection,
            owner_id=resolved_owner_id,
            limit=bounded_limit,
            processing_ttl_sec=resolved_settings.projection_outbox_processing_ttl_sec,
            min_age_sec=min_age_sec,
            projection_name=normalized_projection_name,
        ),
        attempts=resolved_settings.operator_sqlite_lock_retry_attempts,
        base_sleep_sec=resolved_settings.operator_sqlite_lock_retry_base_sleep_sec,
        max_sleep_sec=resolved_settings.operator_sqlite_lock_retry_max_sleep_sec,
        on_retry=_on_locked_retry,
    )
    applied_count = 0
    skipped_count = 0
    error_count = 0
    dead_letter_count = 0
    applied_by_verify_count = 0
    applied_by_worker_count = 0
    skipped_apply_disabled_count = 0
    projection_apply_error_count = 0
    max_wall_ms_exceeded = False
    mutated_projection_names: set[str] = set()
    errors: list[dict[str, Any]] = []

    for job in claimed_jobs:
        elapsed_ms = (time.monotonic() - started_at) * 1000
        if elapsed_ms >= float(resolved_settings.projection_outbox_run_once_max_wall_ms):
            max_wall_ms_exceeded = True
            break
        if apply_requested and not apply_effective:
            verification = _verification_skipped(
                "APPLY_DISABLED_BY_SETTINGS",
                projection_name=job.get("projection_name"),
                event_id=job.get("event_id"),
                apply_projection_enabled=global_apply_enabled,
                market_data_apply_enabled=market_data_apply_enabled,
                market_reference_apply_enabled=market_reference_apply_enabled,
                market_index_apply_enabled=market_index_apply_enabled,
                market_regime_apply_enabled=market_regime_apply_enabled,
                market_scan_apply_enabled=market_scan_apply_enabled,
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
        job_apply_mode = _job_apply_mode(
            job,
            apply_effective=apply_effective,
            market_data_apply_enabled=market_data_apply_enabled,
            market_reference_apply_enabled=market_reference_apply_enabled,
            market_index_apply_enabled=market_index_apply_enabled,
            market_regime_apply_enabled=market_regime_apply_enabled,
            market_scan_apply_enabled=market_scan_apply_enabled,
        )
        evidence = {
            **dict(verification.evidence),
            "verification_reason": verification.reason,
            "worker_run_id": run_id,
            "shadow_mode": True,
            "projection_name": job.get("projection_name"),
            "event_id": job.get("event_id"),
            "event_type": job.get("event_type"),
            "apply_mode": job_apply_mode,
            "apply_projection": apply_effective,
            "apply_projection_requested": apply_requested,
            "apply_projection_enabled": global_apply_enabled,
            "market_data_apply_enabled": market_data_apply_enabled,
            "market_reference_apply_enabled": market_reference_apply_enabled,
            "market_index_apply_enabled": market_index_apply_enabled,
            "market_regime_apply_enabled": market_regime_apply_enabled,
            "market_scan_apply_enabled": market_scan_apply_enabled,
            "no_trading_side_effects": True,
            "projection_side_effects_allowed": apply_effective,
        }
        outbox_id = str(job["outbox_id"])
        if verification.status == "APPLIED":
            retry_sqlite_locked(
                lambda outbox_id=outbox_id, evidence=evidence: mark_projection_outbox_applied(
                    connection,
                    outbox_id,
                    owner_id=resolved_owner_id,
                    evidence=evidence,
                ),
                attempts=resolved_settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=resolved_settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=resolved_settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_locked_retry,
            )
            applied_count += 1
            if evidence.get("apply_result") == "APPLIED_BY_WORKER":
                applied_by_worker_count += 1
            else:
                applied_by_verify_count += 1
            mutated_projection_name = evidence.get("mutated_projection_name")
            if mutated_projection_name:
                mutated_projection_names.add(str(mutated_projection_name))
            for name in evidence.get("mutated_projection_names") or []:
                mutated_projection_names.add(str(name))
        elif verification.status == "SKIPPED":
            retry_sqlite_locked(
                lambda outbox_id=outbox_id,
                reason=verification.reason,
                evidence=evidence: mark_projection_outbox_skipped(
                    connection,
                    outbox_id,
                    owner_id=resolved_owner_id,
                    reason=reason,
                    evidence=evidence,
                ),
                attempts=resolved_settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=resolved_settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=resolved_settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_locked_retry,
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
            mark_error = (
                mark_projection_outbox_retryable_error
                if bool(evidence.get("retryable"))
                else mark_projection_outbox_error
            )
            retry_sqlite_locked(
                lambda outbox_id=outbox_id,
                message=message,
                evidence=evidence,
                mark_error=mark_error: mark_error(
                    connection,
                    outbox_id,
                    owner_id=resolved_owner_id,
                    error_message=message,
                    retry_limit=resolved_settings.projection_outbox_retry_limit,
                    evidence=evidence,
                ),
                attempts=resolved_settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=resolved_settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=resolved_settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_locked_retry,
            )
            errors.append(
                {
                    "outbox_id": outbox_id,
                    "projection_name": job.get("projection_name"),
                    "event_id": job.get("event_id"),
                    "reason": verification.reason,
                    "error_message": message,
                    "dead_letter": will_dead_letter,
                    "apply_mode": job_apply_mode,
                }
            )
            if will_dead_letter:
                dead_letter_count += 1
            else:
                error_count += 1

    status = "NOOP"
    if claimed_jobs:
        status = "COMPLETED_WITH_ERRORS" if errors else "COMPLETED"
    if max_wall_ms_exceeded and not errors:
        status = "PARTIAL_MAX_WALL_MS"
    outbox_status = get_projection_outbox_status(connection, settings=resolved_settings)
    reason_codes = []
    if locked_retry_count > 0:
        reason_codes.append("SQLITE_LOCK_RETRIED")
    if max_wall_ms_exceeded:
        reason_codes.append("PROJECTION_OUTBOX_MAX_WALL_MS_EXCEEDED")
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
        market_reference_apply_enabled=market_reference_apply_enabled,
        market_index_apply_enabled=market_index_apply_enabled,
        market_regime_apply_enabled=market_regime_apply_enabled,
        market_scan_apply_enabled=market_scan_apply_enabled,
        applied_by_verify_count=applied_by_verify_count,
        applied_by_worker_count=applied_by_worker_count,
        skipped_apply_disabled_count=skipped_apply_disabled_count,
        projection_apply_error_count=projection_apply_error_count,
        locked_retry_count=locked_retry_count,
        locked_job_count=0,
        retryable=False,
        partial_result=max_wall_ms_exceeded,
        max_wall_ms_exceeded=max_wall_ms_exceeded,
        effective_limit=int(bounded_limit),
        requested_limit=None if requested_limit is None else int(requested_limit),
        projection_name_filter=normalized_projection_name,
        live_safe=bool(live_safe),
        stale_processing_reset_count=stale_processing_reset_count,
        reason_codes=tuple(reason_codes),
        mutated_projection_names=tuple(sorted(mutated_projection_names)),
        projection_side_effects_allowed=apply_effective,
        errors=tuple(errors),
        created_at=created_at,
    )


def _apply_batch_size(
    *,
    settings: Settings,
    projection_name: str | None,
    apply_effective: bool,
    market_data_apply_enabled: bool,
    market_reference_apply_enabled: bool,
    market_index_apply_enabled: bool,
    market_regime_apply_enabled: bool,
    market_scan_apply_enabled: bool,
) -> int:
    if not apply_effective:
        return settings.projection_outbox_batch_size
    if projection_name == "market_regime" and market_regime_apply_enabled:
        return settings.projection_outbox_market_regime_apply_batch_size
    if projection_name == "market_scan" and market_scan_apply_enabled:
        return settings.projection_outbox_market_scan_apply_batch_size
    if projection_name == "market_index" and market_index_apply_enabled:
        return settings.projection_outbox_market_index_apply_batch_size
    if projection_name == "market_reference" and market_reference_apply_enabled:
        return settings.projection_outbox_market_reference_apply_batch_size
    if projection_name == "market_data" and market_data_apply_enabled:
        return settings.projection_outbox_apply_batch_size
    if market_data_apply_enabled:
        return settings.projection_outbox_apply_batch_size
    if market_reference_apply_enabled:
        return settings.projection_outbox_market_reference_apply_batch_size
    if market_index_apply_enabled:
        return settings.projection_outbox_market_index_apply_batch_size
    if market_regime_apply_enabled:
        return settings.projection_outbox_market_regime_apply_batch_size
    return settings.projection_outbox_market_scan_apply_batch_size


def _apply_min_age_sec(
    *,
    settings: Settings,
    projection_name: str | None,
    apply_effective: bool,
    market_data_apply_enabled: bool,
    market_reference_apply_enabled: bool,
    market_index_apply_enabled: bool,
    market_regime_apply_enabled: bool,
    market_scan_apply_enabled: bool,
) -> float:
    if not apply_effective:
        return settings.projection_outbox_shadow_min_age_sec
    if projection_name == "market_regime" and market_regime_apply_enabled:
        return settings.projection_outbox_market_regime_apply_min_age_sec
    if projection_name == "market_scan" and market_scan_apply_enabled:
        return settings.projection_outbox_market_scan_apply_min_age_sec
    if projection_name == "market_index" and market_index_apply_enabled:
        return settings.projection_outbox_market_index_apply_min_age_sec
    if projection_name == "market_reference" and market_reference_apply_enabled:
        return settings.projection_outbox_market_reference_apply_min_age_sec
    if projection_name == "market_data" and market_data_apply_enabled:
        return settings.projection_outbox_apply_min_age_sec
    if market_data_apply_enabled:
        return settings.projection_outbox_apply_min_age_sec
    if market_reference_apply_enabled:
        return settings.projection_outbox_market_reference_apply_min_age_sec
    if market_index_apply_enabled:
        return settings.projection_outbox_market_index_apply_min_age_sec
    if market_regime_apply_enabled:
        return settings.projection_outbox_market_regime_apply_min_age_sec
    return settings.projection_outbox_market_scan_apply_min_age_sec


def _job_apply_mode(
    job: Mapping[str, Any],
    *,
    apply_effective: bool,
    market_data_apply_enabled: bool,
    market_reference_apply_enabled: bool,
    market_index_apply_enabled: bool,
    market_regime_apply_enabled: bool,
    market_scan_apply_enabled: bool,
) -> str:
    if not apply_effective:
        return APPLY_MODE_SHADOW_VERIFY_ONLY
    projection_name = str(job.get("projection_name") or "").strip()
    if projection_name == "market_data" and market_data_apply_enabled:
        return APPLY_MODE_MARKET_DATA_APPLY
    if projection_name == "market_reference" and market_reference_apply_enabled:
        return APPLY_MODE_MARKET_REFERENCE_APPLY
    if projection_name == "market_index" and market_index_apply_enabled:
        return APPLY_MODE_MARKET_INDEX_APPLY
    if projection_name == "market_regime" and market_regime_apply_enabled:
        return APPLY_MODE_MARKET_REGIME_APPLY
    if projection_name == "market_scan" and market_scan_apply_enabled:
        return APPLY_MODE_MARKET_SCAN_APPLY
    if projection_name == "market_regime" and market_index_apply_enabled:
        return APPLY_MODE_MARKET_INDEX_REGIME_VERIFY
    return APPLY_MODE_SHADOW_VERIFY_ONLY


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
    if projection_name == "market_data" and not bool(
        settings.projection_outbox_market_data_apply_enabled
    ):
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_reference" and not bool(
        settings.projection_outbox_market_reference_apply_enabled
    ):
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_index" and not bool(
        settings.projection_outbox_market_index_apply_enabled
    ):
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_regime" and not bool(
        settings.projection_outbox_market_index_apply_enabled
        or settings.projection_outbox_market_regime_apply_enabled
    ):
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_scan" and not bool(
        settings.projection_outbox_market_scan_apply_enabled
    ):
        return _verification_skipped(
            "APPLY_NOT_ENABLED_FOR_PROJECTION",
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
            worker_run_id=worker_run_id,
        )
    if projection_name not in {
        "market_data",
        "market_reference",
        "market_index",
        "market_regime",
        "market_scan",
    }:
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
    if projection_name == "market_reference":
        if event_type != "market_symbols":
            return _verification_skipped(
                "MARKET_REFERENCE_APPLY_EVENT_TYPE_UNSUPPORTED",
                event_id=event_id,
                event_type=event_type,
            )
        return _apply_market_reference_projection(
            connection,
            job,
            source_event,
            settings=settings,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_index":
        if event_type != "market_index_tick":
            return _verification_skipped(
                "MARKET_INDEX_APPLY_EVENT_TYPE_UNSUPPORTED",
                event_id=event_id,
                event_type=event_type,
            )
        return _apply_market_index_projection(
            connection,
            job,
            source_event,
            settings=settings,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_regime":
        if event_type != "market_index_tick":
            return _verification_skipped(
                "MARKET_REGIME_APPLY_EVENT_TYPE_UNSUPPORTED",
                event_id=event_id,
                event_type=event_type,
            )
        return _apply_market_regime_continuity_verification(
            connection,
            job,
            source_event,
            settings=settings,
            worker_run_id=worker_run_id,
        )
    if projection_name == "market_scan":
        if event_type != "tr_response":
            return _verification_skipped(
                "MARKET_SCAN_APPLY_EVENT_TYPE_UNSUPPORTED",
                event_id=event_id,
                event_type=event_type,
            )
        return _apply_market_scan_projection(
            connection,
            job,
            source_event,
            settings=settings,
            worker_run_id=worker_run_id,
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
    if (
        event_type == "price_tick"
        and projection_result.status == "APPLIED"
        and projection_result.applied_count > 0
    ):
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
    elif (
        event_type == "tr_response"
        and projection_result.status == "APPLIED"
        and projection_result.applied_count > 0
    ):
        post_apply_side_effects = _enqueue_deferred_candidate_quote_refresh_for_tr_response(
            connection,
            event,
            settings=settings,
        )
        record_market_data_post_apply_deferred_side_effects(
            connection,
            event_id,
            post_apply_side_effects,
        )
    elif (
        event_type == "condition_event"
        and projection_result.status == "APPLIED"
        and projection_result.applied_count > 0
    ):
        post_apply_side_effects = _refresh_deferred_condition_fusion_for_condition_event(
            connection,
            event,
            settings=settings,
        )
        record_market_data_post_apply_deferred_side_effects(
            connection,
            event_id,
            post_apply_side_effects,
        )
    elif event_type == "tr_response":
        post_apply_side_effects = {
            "candidate_quote_refresh_enqueue_status": "SKIPPED",
            "reason": "PROJECTION_RESULT_NOT_APPLIED",
            "projection_result_status": projection_result.status,
            "projection_result_applied_count": projection_result.applied_count,
            "source": "projection_outbox_worker_tr_response",
            "deferred_from_gateway_path": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }
        record_market_data_post_apply_deferred_side_effects(
            connection,
            event_id,
            post_apply_side_effects,
        )
    elif event_type == "condition_event":
        post_apply_side_effects = {
            "condition_fusion_refresh_status": "SKIPPED",
            "reason": "PROJECTION_RESULT_NOT_APPLIED",
            "projection_result_status": projection_result.status,
            "projection_result_applied_count": projection_result.applied_count,
            "source": "projection_outbox_worker_condition_event",
            "deferred_from_gateway_path": True,
            "candidate_ingest_executed": False,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }
        record_market_data_post_apply_deferred_side_effects(
            connection,
            event_id,
            post_apply_side_effects,
        )

    append_only_cutover = _append_only_cutover_evidence(
        connection,
        event_id=event_id,
        event_type=event_type,
    )
    if event_type == "tr_response" and post_apply_side_effects:
        post_apply_side_effects.setdefault(
            "deferred_from_gateway_path",
            bool(append_only_cutover.get("gateway_inline_skipped")),
        )
        post_apply_side_effects.setdefault(
            "source",
            "projection_outbox_worker_tr_response",
        )
        post_apply_side_effects.setdefault("no_order_side_effects", True)
        post_apply_side_effects.setdefault("no_trading_side_effects", True)
        if _tr_response_side_effect_failed(post_apply_side_effects) and bool(
            settings.gateway_market_data_append_only_tr_response_fail_closed_on_side_effect_error
        ):
            return _verification_error(
                "TR_RESPONSE_DEFERRED_QUOTE_REFRESH_SIDE_EFFECT_ERROR",
                "tr_response deferred candidate quote refresh side-effect failed",
                event_id=event_id,
                event_type=event_type,
                apply_result="APPLY_ERROR",
                verification_before_apply=verification_before_payload,
                projection_result_status=projection_result_status,
                projection_result=projection_result_payload,
                post_apply_side_effects=post_apply_side_effects,
                append_only_cutover=append_only_cutover,
            )
    if event_type == "condition_event" and post_apply_side_effects:
        post_apply_side_effects.setdefault(
            "deferred_from_gateway_path",
            bool(append_only_cutover.get("gateway_inline_skipped")),
        )
        post_apply_side_effects.setdefault(
            "source",
            "projection_outbox_worker_condition_event",
        )
        post_apply_side_effects.setdefault("candidate_ingest_executed", False)
        post_apply_side_effects.setdefault("no_order_side_effects", True)
        post_apply_side_effects.setdefault("no_trading_side_effects", True)
        if _condition_event_side_effect_failed(post_apply_side_effects) and bool(
            settings.gateway_market_data_append_only_condition_event_fail_closed_on_side_effect_error
        ):
            return _verification_error(
                "CONDITION_EVENT_DEFERRED_FUSION_REFRESH_SIDE_EFFECT_ERROR",
                "condition_event deferred condition_fusion refresh failed",
                event_id=event_id,
                event_type=event_type,
                apply_result="APPLY_ERROR",
                verification_before_apply=verification_before_payload,
                projection_result_status=projection_result_status,
                projection_result=projection_result_payload,
                post_apply_side_effects=post_apply_side_effects,
                append_only_cutover=append_only_cutover,
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
        "append_only_cutover": append_only_cutover,
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


def _apply_market_scan_projection(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    source_event: Mapping[str, Any],
    *,
    settings: Settings,
    worker_run_id: str,
) -> ProjectionOutboxVerificationResult:
    event_id = str(job.get("event_id") or "")
    event_type = str(job.get("event_type") or "").lower()
    verification_before = verify_projection_outbox_job(
        connection,
        job,
        settings=settings,
    )
    verification_before_payload = verification_before.to_dict()
    if verification_before.status == "APPLIED":
        return _verification_applied(
            "MARKET_SCAN_ALREADY_APPLIED_BY_INLINE",
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLIED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
            candidate_ingest_executed=False,
            no_order_side_effects=True,
            no_trading_side_effects=True,
        )
    if verification_before.status == "ERROR":
        return _verification_error(
            verification_before.reason,
            verification_before.error_message or verification_before.reason,
            **dict(verification_before.evidence),
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )

    dependency = _market_scan_market_data_dependency(connection, event_id)
    if not dependency["ready"]:
        return _verification_error(
            "MARKET_SCAN_MARKET_DATA_DEPENDENCY_NOT_APPLIED",
            "market_data sibling projection must complete before market_scan apply",
            event_id=event_id,
            event_type=event_type,
            market_data_dependency=dependency,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
            retryable=True,
        )

    try:
        event = _gateway_event_from_row(source_event)
        projection_result = process_market_scan_event(
            connection,
            event,
            settings=settings,
            generated_by=f"projection_outbox_worker:{worker_run_id}",
        )
    except Exception as exc:
        return _verification_error(
            "MARKET_SCAN_APPLY_EXCEPTION",
            str(exc),
            event_id=event_id,
            event_type=event_type,
            market_data_dependency=dependency,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )

    projection_result_payload = projection_result.to_dict()
    if projection_result.status in {"ERROR", "PARTIAL"}:
        return _verification_error(
            "MARKET_SCAN_APPLY_FAILED",
            projection_result.error_message or projection_result.status,
            event_id=event_id,
            event_type=event_type,
            market_data_dependency=dependency,
            projection_result=projection_result_payload,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )
    if projection_result.status == "IGNORED":
        return _verification_skipped(
            "MARKET_SCAN_EVENT_NOT_RECOGNIZED",
            event_id=event_id,
            event_type=event_type,
            projection_result=projection_result_payload,
            apply_result="SKIPPED_AFTER_APPLY",
            verification_before_apply=verification_before_payload,
        )

    verification_after = verify_projection_outbox_job(
        connection,
        job,
        settings=settings,
    )
    evidence = {
        "event_id": event_id,
        "event_type": event_type,
        "verification_before_apply": verification_before_payload,
        "verification_after_apply": verification_after.to_dict(),
        "projection_result": projection_result_payload,
        "market_data_dependency": dependency,
        "candidate_ingest_executed": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    if verification_after.status == "APPLIED":
        applied_by_worker = projection_result.applied_count > 0
        return _verification_applied(
            (
                "MARKET_SCAN_APPLIED_BY_WORKER"
                if applied_by_worker
                else "MARKET_SCAN_ALREADY_APPLIED_BY_INLINE"
            ),
            **evidence,
            apply_result=("APPLIED_BY_WORKER" if applied_by_worker else "APPLIED_BY_VERIFY"),
            **(
                {"mutated_projection_name": "market_scan"}
                if applied_by_worker
                else {}
            ),
        )
    return _verification_error(
        verification_after.reason,
        verification_after.error_message or verification_after.reason,
        **evidence,
        apply_result="APPLY_ERROR",
    )


def _apply_market_reference_projection(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    source_event: Mapping[str, Any],
    *,
    settings: Settings,
    worker_run_id: str,
) -> ProjectionOutboxVerificationResult:
    del settings, worker_run_id
    event_id = str(job.get("event_id") or "")
    event_type = str(job.get("event_type") or "").lower()
    verification_before = verify_projection_outbox_job(connection, job)
    verification_before_payload = verification_before.to_dict()
    if verification_before.status == "APPLIED":
        return _verification_applied(
            "INLINE_ARTIFACT_OBSERVED",
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLIED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
            projection_result_status=None,
        )
    if verification_before.status == "SKIPPED":
        return _verification_skipped(
            verification_before.reason,
            **dict(verification_before.evidence),
            apply_result="SKIPPED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
        )

    try:
        event = _gateway_event_from_row(source_event)
        projection_result = process_market_symbols_event(connection, event)
    except Exception as exc:
        return _verification_error(
            "MARKET_REFERENCE_APPLY_EXCEPTION",
            str(exc),
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )

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
            "MARKET_REFERENCE_APPLY_FAILED",
            projection_result.error_message or projection_result.status,
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
            projection_result_status=projection_result.status,
            projection_result=projection_result_payload,
        )

    verification_after = verify_projection_outbox_job(connection, job)
    evidence = {
        "event_id": event_id,
        "event_type": event_type,
        "verification_before_apply": verification_before_payload,
        "verification_after_apply": verification_after.to_dict(),
        "projection_result_status": projection_result.status,
        "projection_result": projection_result_payload,
        "post_apply_side_effects": {},
        "candidate_ingest_executed": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    if verification_after.status == "APPLIED":
        if projection_result.applied_count > 0:
            evidence["apply_result"] = "APPLIED_BY_WORKER"
            evidence["mutated_projection_name"] = "market_reference"
        else:
            evidence["apply_result"] = "APPLIED_BY_VERIFY"
        return _verification_applied(
            "MARKET_REFERENCE_APPLIED_BY_WORKER"
            if projection_result.applied_count > 0
            else "INLINE_ARTIFACT_OBSERVED",
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


def _apply_market_regime_continuity_verification(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    source_event: Mapping[str, Any] | sqlite3.Row,
    *,
    settings: Settings,
    worker_run_id: str,
) -> ProjectionOutboxVerificationResult:
    event_id = str(job.get("event_id") or "")
    continuity: dict[str, Any] | None = None
    standalone_apply_enabled = bool(
        settings.projection_outbox_market_regime_apply_enabled
    )
    continuity_required = _market_index_regime_continuity_required(
        connection,
        event_id=event_id,
        settings=settings,
    )
    standalone_apply: dict[str, Any] | None = None
    mutated_projection_names: list[str] = []
    if continuity_required or standalone_apply_enabled:
        index_verification = _verify_market_index(connection, event_id)
        if index_verification.status != "APPLIED":
            return _verification_error(
                "MARKET_INDEX_DEPENDENCY_NOT_APPLIED",
                index_verification.error_message or index_verification.reason,
                event_id=event_id,
                index_verification=index_verification.to_dict(),
                apply_result="APPLY_ERROR",
                retryable=True,
            )
    if continuity_required:
        continuity = _ensure_market_index_regime_continuity(
            connection,
            event_id=event_id,
            settings=settings,
            worker_run_id=worker_run_id,
        )
        if continuity["status"] == "ERROR":
            return _verification_error(
                "MARKET_INDEX_REGIME_CONTINUITY_FAILED",
                str(continuity.get("error") or "regime continuity failed"),
                event_id=event_id,
                market_regime_continuity=continuity,
                apply_result="APPLY_ERROR",
                retryable=True,
            )
    elif standalone_apply_enabled:
        linked_before = _market_regime_snapshot_for_event(connection, event_id)
        regime_effective_skip = _market_regime_routing_effective_skip(
            connection,
            event_id,
        )
        superseded = _market_regime_source_event_superseded(
            connection,
            source_event,
            event_id=event_id,
        )
        if regime_effective_skip and superseded:
            return _verification_error(
                "MARKET_REGIME_EFFECTIVE_SKIP_SOURCE_SUPERSEDED",
                "effective-skip source was superseded before worker context refresh",
                event_id=event_id,
                apply_result="APPLY_ERROR",
                retryable=True,
            )
        rebuild_required = bool(
            not superseded
            and (
                regime_effective_skip
                or should_rebuild_market_context_snapshots(
                    connection,
                    settings=settings,
                )
            )
        )
        rebuild_result: dict[str, Any] | None = None
        if rebuild_required:
            try:
                rebuild_result = rebuild_market_context_snapshots(
                    connection,
                    settings=settings,
                    source_event_id=event_id,
                    source_projection="market_regime",
                    generated_by=f"projection_outbox_worker:{worker_run_id}",
                )
            except Exception as exc:
                return _verification_error(
                    "MARKET_REGIME_APPLY_EXCEPTION",
                    str(exc),
                    event_id=event_id,
                    apply_result="APPLY_ERROR",
                    retryable=True,
                )
        linked_after = _market_regime_snapshot_for_event(connection, event_id)
        context_status = get_market_context_status(connection, settings=settings)
        if not _market_context_projection_ready(context_status):
            return _verification_error(
                "MARKET_CONTEXT_PROJECTION_NOT_READY",
                "common market context structural contract is not ready",
                event_id=event_id,
                market_context_status=context_status,
                apply_result="APPLY_ERROR",
                retryable=True,
            )
        if regime_effective_skip and linked_after is None:
            return _verification_error(
                "MARKET_REGIME_EFFECTIVE_SKIP_SNAPSHOT_MISSING",
                "effective-skip worker did not create an event-linked regime snapshot",
                event_id=event_id,
                apply_result="APPLY_ERROR",
                retryable=True,
            )
        if regime_effective_skip and not _market_context_source_event_ready(
            context_status,
            event_id=event_id,
        ):
            return _verification_error(
                "MARKET_REGIME_EFFECTIVE_SKIP_CONTEXT_PAIR_MISSING",
                "effective-skip worker did not publish the event-linked context pair",
                event_id=event_id,
                market_context_status=context_status,
                apply_result="APPLY_ERROR",
                retryable=True,
            )
        if linked_before is None and linked_after is not None:
            mutated_projection_names.append("market_regime")
        if rebuild_result is not None and int(rebuild_result.get("created_count") or 0):
            mutated_projection_names.append("market_context")
        standalone_apply = {
            "status": (
                "APPLIED_BY_WORKER"
                if mutated_projection_names
                else "APPLIED_BY_VERIFY"
            ),
            "rebuild_required": rebuild_required,
            "effective_skip": regime_effective_skip,
            "source_event_superseded": superseded,
            "source_event_id": event_id,
            "linked_regime_snapshot_id": (
                None if linked_after is None else linked_after.get("snapshot_id")
            ),
            "source_watermark_hash": (
                None
                if rebuild_result is None
                else rebuild_result.get("source_watermark_hash")
            ),
            "market_context_status": context_status,
        }
    verification = _verify_market_regime(connection, event_id)
    evidence = {
        "event_id": event_id,
        "event_type": str(job.get("event_type") or "").lower(),
        "worker_run_id": worker_run_id,
        "market_index_apply_enabled": bool(
            settings.projection_outbox_market_index_apply_enabled
        ),
        "verification": verification.to_dict(),
        "market_regime_continuity": continuity,
        "market_regime_standalone_apply": standalone_apply,
        "mutated_projection_names": mutated_projection_names,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    if verification.status == "APPLIED":
        return _verification_applied(
            "MARKET_REGIME_CONTINUITY_OBSERVED",
            **evidence,
            apply_result=(
                "APPLIED_BY_WORKER"
                if mutated_projection_names
                else "APPLIED_BY_VERIFY"
            ),
        )
    if verification.status == "SKIPPED":
        return _verification_skipped(
            verification.reason,
            **dict(verification.evidence),
            **evidence,
            apply_result="SKIPPED_BY_VERIFY",
        )
    return _verification_error(
        verification.reason,
        verification.error_message or verification.reason,
        **evidence,
        apply_result="APPLY_ERROR",
    )


def _market_context_projection_ready(status: Mapping[str, Any]) -> bool:
    latest = status.get("latest")
    return bool(
        isinstance(latest, Mapping)
        and isinstance(latest.get("KOSPI"), Mapping)
        and isinstance(latest.get("KOSDAQ"), Mapping)
        and status.get("latest_watermark_coherent") is True
        and status.get("latest_regime_coherent") is True
        and int(status.get("regime_reference_missing_count") or 0) == 0
    )


def _market_context_source_event_ready(
    status: Mapping[str, Any],
    *,
    event_id: str,
) -> bool:
    latest = status.get("latest")
    if not isinstance(latest, Mapping):
        return False
    return all(
        isinstance(latest.get(market), Mapping)
        and str(latest[market].get("source_event_id") or "") == event_id
        for market in ("KOSPI", "KOSDAQ")
    )


def _market_regime_source_event_superseded(
    connection: sqlite3.Connection,
    source_event: Mapping[str, Any] | sqlite3.Row,
    *,
    event_id: str,
) -> bool:
    payload = _json_object(source_event["payload_json"])
    index_code = str(payload.get("index_code") or "").strip().upper()
    if not index_code:
        return False
    latest = connection.execute(
        "SELECT event_id FROM market_index_ticks_latest WHERE index_code = ?",
        (index_code,),
    ).fetchone()
    return bool(latest is not None and str(latest["event_id"]) != event_id)


def _apply_market_index_projection(
    connection: sqlite3.Connection,
    job: Mapping[str, Any],
    source_event: Mapping[str, Any],
    *,
    settings: Settings,
    worker_run_id: str,
) -> ProjectionOutboxVerificationResult:
    event_id = str(job.get("event_id") or "")
    event_type = str(job.get("event_type") or "").lower()
    verification_before = verify_projection_outbox_job(
        connection,
        job,
        settings=settings,
    )
    verification_before_payload = verification_before.to_dict()
    if verification_before.status == "APPLIED":
        effective_skip = _market_index_routing_effective_skip(connection, event_id)
        regime_continuity = _ensure_market_index_regime_continuity(
            connection,
            event_id=event_id,
            settings=settings,
            worker_run_id=worker_run_id,
        )
        if regime_continuity["status"] == "ERROR":
            return _verification_error(
                "MARKET_INDEX_REGIME_CONTINUITY_FAILED",
                str(regime_continuity.get("error") or "regime continuity failed"),
                event_id=event_id,
                event_type=event_type,
                apply_result="APPLY_ERROR",
                verification_before_apply=verification_before_payload,
                market_regime_continuity=regime_continuity,
                retryable=True,
            )
        mutated_names = _market_index_continuity_mutations(regime_continuity)
        return _verification_applied(
            (
                "MARKET_INDEX_WORKER_ARTIFACT_RECOVERED"
                if effective_skip
                else "MARKET_INDEX_ALREADY_APPLIED_BY_INLINE"
            ),
            event_id=event_id,
            event_type=event_type,
            apply_result=(
                "APPLIED_BY_WORKER" if effective_skip else "APPLIED_BY_VERIFY"
            ),
            verification_before_apply=verification_before_payload,
            projection_result_status=None,
            market_regime_continuity=regime_continuity,
            worker_recovery_verified_existing_artifact=effective_skip,
            market_regime_inline_path_unchanged_in_pr15=not bool(
                regime_continuity.get("required")
            ),
            mutated_projection_names=mutated_names,
            no_order_side_effects=True,
            no_trading_side_effects=True,
        )
    if verification_before.status == "SKIPPED":
        return _verification_skipped(
            verification_before.reason,
            **dict(verification_before.evidence),
            apply_result="SKIPPED_BY_VERIFY",
            verification_before_apply=verification_before_payload,
        )

    try:
        event = _gateway_event_from_row(source_event)
        projection_result = process_market_index_event(
            connection,
            event,
            settings=settings,
        )
    except Exception as exc:
        return _verification_error(
            "MARKET_INDEX_APPLY_EXCEPTION",
            str(exc),
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
        )

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
            "MARKET_INDEX_APPLY_FAILED",
            projection_result.error_message or projection_result.status,
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
            projection_result_status=projection_result.status,
            projection_result=projection_result_payload,
        )
    if projection_result.status == "IGNORED":
        return _verification_skipped(
            "MARKET_INDEX_OLDER_THAN_LATEST",
            event_id=event_id,
            event_type=event_type,
            apply_result="SKIPPED_AFTER_APPLY",
            verification_before_apply=verification_before_payload,
            projection_result_status=projection_result.status,
            projection_result=projection_result_payload,
        )

    regime_continuity = _ensure_market_index_regime_continuity(
        connection,
        event_id=event_id,
        settings=settings,
        worker_run_id=worker_run_id,
    )
    if regime_continuity["status"] == "ERROR":
        return _verification_error(
            "MARKET_INDEX_REGIME_CONTINUITY_FAILED",
            str(regime_continuity.get("error") or "regime continuity failed"),
            event_id=event_id,
            event_type=event_type,
            apply_result="APPLY_ERROR",
            verification_before_apply=verification_before_payload,
            projection_result_status=projection_result.status,
            projection_result=projection_result_payload,
            market_regime_continuity=regime_continuity,
            retryable=True,
        )

    verification_after = verify_projection_outbox_job(
        connection,
        job,
        settings=settings,
    )
    evidence = {
        "event_id": event_id,
        "event_type": event_type,
        "verification_before_apply": verification_before_payload,
        "verification_after_apply": verification_after.to_dict(),
        "projection_result_status": projection_result.status,
        "projection_result": projection_result_payload,
        "market_regime_continuity": regime_continuity,
        "market_regime_inline_path_unchanged_in_pr15": not bool(
            regime_continuity.get("required")
        ),
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    if verification_after.status == "APPLIED":
        if projection_result.applied_count > 0:
            evidence["apply_result"] = "APPLIED_BY_WORKER"
            evidence["mutated_projection_name"] = "market_index"
            evidence["mutated_projection_names"] = [
                "market_index",
                *_market_index_continuity_mutations(regime_continuity),
            ]
        else:
            evidence["apply_result"] = "APPLIED_BY_VERIFY"
        return _verification_applied(
            "MARKET_INDEX_APPLIED_BY_WORKER"
            if projection_result.applied_count > 0
            else "MARKET_INDEX_ALREADY_APPLIED_BY_INLINE",
            **evidence,
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
            "INLINE_ARTIFACT_OBSERVED",
            event_id=event_id,
            table="market_symbol_memberships",
        )
    if not market_symbols_payload_has_symbols(payload):
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
        return _verification_error(
            "MARKET_INDEX_INLINE_PROJECTION_ERROR",
            str(inline_error.get("error_message") or "market index projection failed"),
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
    if _market_index_routing_effective_skip(
        connection,
        event_id,
    ) or _market_regime_routing_effective_skip(connection, event_id):
        snapshot = _market_regime_snapshot_for_event(connection, event_id)
        if snapshot is not None:
            return _verification_applied(
                "MARKET_REGIME_EVENT_CONTINUITY_OBSERVED",
                event_id=event_id,
                snapshot_id=snapshot.get("snapshot_id"),
                regime_status=snapshot.get("regime_status"),
                quality_status=snapshot.get("quality_status"),
            )
        return _verification_error(
            "MARKET_REGIME_EVENT_CONTINUITY_MISSING",
            f"event-linked market_regime snapshot missing for event_id={event_id}",
            event_id=event_id,
        )
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
        return _verification_error(
            "MARKET_SCAN_INLINE_ERROR",
            str(inline_error.get("error_message") or "market_scan inline error"),
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
            WHERE source_event_id = ?
               OR request_id = ?
               OR json_extract(metadata_json, '$.request_id') = ?
            LIMIT 1
            """,
            (event_id, request_id, request_id),
        ).fetchone()
        if row is not None:
            return _verification_applied(
                "MARKET_SCAN_SNAPSHOT_OBSERVED",
                event_id=event_id,
                request_id=request_id,
            )
    return _verification_skipped(
        "MARKET_SCAN_ARTIFACT_NOT_YET_APPLIED",
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
    result = enqueue_incremental_for_price_tick_projection(
        connection,
        event,
        settings=settings,
        source="projection_outbox_worker_price_tick",
    )
    return {
        "incremental_evaluation_enqueue_status": result.status,
        "enqueued_count": result.enqueued_count,
        "candidate_ids": list(result.candidate_ids),
        "code": result.codes[0] if result.codes else None,
        "error_count": result.error_count,
        "source": result.source,
        "deferred_from_gateway_path": deferred_from_gateway_path,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "evidence": result.to_dict(),
    }


def _enqueue_deferred_candidate_quote_refresh_for_tr_response(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
) -> dict[str, Any]:
    result = enqueue_incremental_for_candidate_quote_refresh_tr_response(
        connection,
        event,
        settings=settings,
        source="projection_outbox_worker_tr_response",
    )
    return {
        "candidate_quote_refresh_enqueue_status": result.status,
        "candidate_quote_refresh_code_count": result.code_count,
        "candidate_quote_refresh_enqueued_count": result.enqueued_count,
        "candidate_quote_refresh_error_count": result.error_count,
        "candidate_quote_refresh_statuses": list(result.statuses),
        "candidate_quote_refresh_codes": list(result.codes),
        "candidate_quote_refresh_reason_codes": list(result.reason_codes),
        "source": result.source,
        "deferred_from_gateway_path": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "evidence": result.to_dict(),
    }


def _refresh_deferred_condition_fusion_for_condition_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
) -> dict[str, Any]:
    result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=settings,
        source="projection_outbox_worker_condition_event",
    )
    return {
        "condition_fusion_refresh_status": result.status,
        "condition_fusion_processed_event_count": result.processed_count,
        "condition_fusion_fused_code_count": result.applied_count,
        "condition_code": result.code,
        "condition_action": result.evidence.get("condition_action"),
        "condition_fusion_error_count": result.error_count,
        "condition_fusion_reason_codes": list(result.reason_codes),
        "source": result.source,
        "deferred_from_gateway_path": True,
        "candidate_ingest_executed": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "evidence": result.to_dict(),
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


def _append_only_cutover_evidence(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    event_type: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT id, effective_skip_inline, decided_at
        FROM market_data_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_data'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    gateway_inline_skipped = bool(row is not None and row["effective_skip_inline"])
    return {
        "gateway_inline_skipped": gateway_inline_skipped,
        "cutover_event_type": event_type if gateway_inline_skipped else None,
        "routing_decision_id": None if row is None else int(row["id"]),
        "routing_decision_event_id": event_id if row is not None else None,
        "routing_decision_decided_at": None if row is None else row["decided_at"],
        "event_id": event_id,
        "worker_completed_at": datetime_to_wire(utc_now()),
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def _tr_response_side_effect_failed(side_effects: Mapping[str, Any]) -> bool:
    status = str(side_effects.get("candidate_quote_refresh_enqueue_status") or "")
    return int(side_effects.get("candidate_quote_refresh_error_count") or 0) > 0 or status in {
        "ERROR",
        "COMPLETED_WITH_ERRORS",
    }


def _condition_event_side_effect_failed(side_effects: Mapping[str, Any]) -> bool:
    status = str(side_effects.get("condition_fusion_refresh_status") or "")
    return int(side_effects.get("condition_fusion_error_count") or 0) > 0 or status in {
        "ERROR",
        "COMPLETED_WITH_ERRORS",
    }


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


def _ensure_market_index_regime_continuity(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    settings: Settings,
    worker_run_id: str,
) -> dict[str, Any]:
    effective_skip = _market_index_routing_effective_skip(connection, event_id)
    cutover_armed = _market_index_cutover_armed(settings)
    if not (effective_skip or cutover_armed):
        return {
            "status": "NOT_REQUIRED",
            "required": False,
            "refreshed": False,
            "reason": "MARKET_INDEX_INLINE_PATH_RETAINED",
            "effective_skip": effective_skip,
            "cutover_armed": cutover_armed,
        }
    if not bool(settings.gateway_market_index_append_only_require_worker_regime_refresh):
        return {
            "status": "ERROR",
            "required": True,
            "refreshed": False,
            "reason": "MARKET_INDEX_REGIME_CONTINUITY_GUARD_DISABLED",
            "error": "worker regime continuity guard must remain enabled during cutover",
            "effective_skip": effective_skip,
            "cutover_armed": cutover_armed,
        }
    existing = _market_regime_snapshot_for_event(connection, event_id)
    if not settings.market_regime_enabled:
        return {
            "status": "ERROR",
            "required": True,
            "refreshed": False,
            "reason": "MARKET_REGIME_DISABLED",
            "error": "market_regime must be enabled during market_index cutover",
            "effective_skip": effective_skip,
            "cutover_armed": cutover_armed,
        }
    try:
        market_context = rebuild_market_context_snapshots(
            connection,
            settings=settings,
            source_event_id=event_id,
            source_projection="market_index",
            generated_by=f"projection_outbox_worker:{worker_run_id}",
        )
    except Exception as exc:
        return {
            "status": "ERROR",
            "required": True,
            "refreshed": False,
            "reason": "MARKET_CONTEXT_REFRESH_EXCEPTION",
            "error": str(exc),
            "effective_skip": effective_skip,
            "cutover_armed": cutover_armed,
        }
    linked = _market_regime_snapshot_for_event(connection, event_id)
    if linked is None:
        return {
            "status": "ERROR",
            "required": True,
            "refreshed": False,
            "reason": "MARKET_REGIME_EVENT_LINK_MISSING",
            "error": "market context refresh did not persist source_event_id",
            "effective_skip": effective_skip,
            "cutover_armed": cutover_armed,
        }
    regime_refreshed = existing is None
    context_created_count = int(market_context.get("created_count") or 0)
    return {
        "status": (
            "APPLIED_BY_WORKER"
            if regime_refreshed or context_created_count
            else "APPLIED_BY_VERIFY"
        ),
        "required": True,
        "refreshed": regime_refreshed,
        "market_context_refreshed": context_created_count > 0,
        "market_context_created_count": context_created_count,
        "market_context_snapshot_ids": [
            str(item.get("snapshot_id"))
            for item in market_context.get("snapshots") or []
            if isinstance(item, Mapping) and item.get("snapshot_id")
        ],
        "source_watermark_hash": market_context.get("source_watermark_hash"),
        "snapshot_id": linked.get("snapshot_id"),
        "regime_status": linked.get("regime_status"),
        "quality_status": linked.get("quality_status"),
        "source_event_id": event_id,
        "effective_skip": effective_skip,
        "cutover_armed": cutover_armed,
    }


def _market_index_continuity_mutations(continuity: Mapping[str, Any]) -> list[str]:
    mutations: list[str] = []
    if continuity.get("refreshed"):
        mutations.append("market_regime")
    if continuity.get("market_context_refreshed"):
        mutations.append("market_context")
    return mutations


def _market_index_regime_continuity_required(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    settings: Settings,
) -> bool:
    return bool(
        _market_index_routing_effective_skip(connection, event_id)
        or _market_index_cutover_armed(settings)
    )


def _market_index_cutover_armed(settings: Settings) -> bool:
    return bool(
        settings.trading_profile == TradingProfile.OBSERVE
        and settings.trading_mode == TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
        and settings.gateway_market_index_append_only_dry_run_enabled
        and settings.gateway_market_index_append_only_cutover_enabled
        and not settings.gateway_market_index_append_only_global_kill_switch
        and not settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        and settings.gateway_market_index_append_only_require_reconcile_pass
        and settings.gateway_market_index_append_only_require_data_usable
        and settings.gateway_market_index_append_only_require_parser_verified
        and settings.gateway_market_index_append_only_require_worker_regime_refresh
        and settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
        and settings.gateway_market_index_append_only_require_fresh_gateway_health
    )


def _market_index_routing_effective_skip(
    connection: sqlite3.Connection,
    event_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT effective_skip_inline
        FROM market_index_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_index'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return bool(row is not None and row["effective_skip_inline"])


def _market_regime_routing_effective_skip(
    connection: sqlite3.Connection,
    event_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT effective_skip_inline
        FROM market_regime_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_regime'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return bool(row is not None and row["effective_skip_inline"])


def _market_regime_snapshot_for_event(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT snapshot_id, regime_status, quality_status, snapshot_at, evidence_json
        FROM market_regime_snapshots
        WHERE source_event_id = ?
        ORDER BY snapshot_at DESC, created_at DESC
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


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


def _market_scan_market_data_dependency(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any]:
    artifact = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    artifact_count = int(artifact["count"] if artifact else 0)
    outbox = connection.execute(
        """
        SELECT status FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    outbox_status = None if outbox is None else str(outbox["status"]).upper()
    return {
        "ready": bool(artifact_count > 0 or outbox_status == "APPLIED"),
        "artifact_count": artifact_count,
        "outbox_status": outbox_status,
    }


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
