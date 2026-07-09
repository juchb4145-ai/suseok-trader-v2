from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now

from services.config import Settings, load_settings
from storage.gateway_command_store import canonical_json


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxBulkRetireBucket:
    projection_name: str
    event_type: str
    status: str
    reason: str
    count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection_name": self.projection_name,
            "event_type": self.event_type,
            "status": self.status,
            "reason": self.reason,
            "count": self.count,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxBulkRetireResult:
    run_id: str
    status: str
    dry_run: bool
    requested_limit: int
    scanned_count: int
    retired_count: int
    applied_count: int
    skipped_count: int
    not_eligible_count: int
    locked_retry_count: int
    by_projection_name: Mapping[str, int]
    by_event_type: Mapping[str, int]
    reason_counts: Mapping[str, int]
    pending_before: int
    pending_after: int
    pending_delta: int
    condition_event_pending_before: int
    condition_event_pending_after: int
    condition_event_pending_delta: int
    created_at: str
    buckets: Sequence[ProjectionOutboxBulkRetireBucket] = field(default_factory=tuple)
    sample_retired_jobs: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    sample_not_eligible_jobs: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    no_trading_side_effects: bool = True
    projection_side_effects_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "dry_run": self.dry_run,
            "requested_limit": self.requested_limit,
            "scanned_count": self.scanned_count,
            "retired_count": self.retired_count,
            "applied_count": self.applied_count,
            "skipped_count": self.skipped_count,
            "not_eligible_count": self.not_eligible_count,
            "locked_retry_count": self.locked_retry_count,
            "by_projection_name": dict(self.by_projection_name),
            "by_event_type": dict(self.by_event_type),
            "reason_counts": dict(self.reason_counts),
            "buckets": [bucket.to_dict() for bucket in self.buckets],
            "sample_retired_jobs": [dict(job) for job in self.sample_retired_jobs],
            "sample_not_eligible_jobs": [
                dict(job) for job in self.sample_not_eligible_jobs
            ],
            "pending_before": self.pending_before,
            "pending_after": self.pending_after,
            "pending_delta": self.pending_delta,
            "condition_event_pending_before": self.condition_event_pending_before,
            "condition_event_pending_after": self.condition_event_pending_after,
            "condition_event_pending_delta": self.condition_event_pending_delta,
            "no_trading_side_effects": self.no_trading_side_effects,
            "projection_side_effects_allowed": self.projection_side_effects_allowed,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, kw_only=True)
class BulkRetireDecision:
    outbox_id: str
    event_id: str
    projection_name: str
    event_type: str
    terminal_status: str | None
    reason: str
    inline_artifact_observed: bool = False
    source_gateway_event_status: str | None = None
    effective_skip_inline: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def eligible(self) -> bool:
        return self.terminal_status in {"APPLIED", "SKIPPED"}


def bulk_retire_projection_outbox(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 5000,
    dry_run: bool = True,
    older_than_sec: int = 60,
    include_projection_names: Iterable[str] | None = None,
    exclude_recent_condition_events: bool = True,
    live_safe: bool = True,
) -> ProjectionOutboxBulkRetireResult:
    del settings
    run_id = new_message_id("projection_outbox_bulk_retire")
    created_at = datetime_to_wire(utc_now())
    requested_limit = int(limit)
    bounded_limit = min(max(requested_limit, 1), 5000)
    if live_safe:
        bounded_limit = min(bounded_limit, 5000)
    cutoff = datetime_to_wire(utc_now() - timedelta(seconds=max(int(older_than_sec), 0)))
    include_projection_set = _normalize_projection_filter(include_projection_names)
    pending_before = _count_pending(connection)
    condition_before = _count_pending_condition_events(connection)
    rows = _candidate_rows(
        connection,
        limit=bounded_limit,
        older_than=cutoff,
        include_projection_names=include_projection_set,
    )
    decisions = [
        decide_bulk_retire_job(
            connection,
            row,
            recent_cutoff=cutoff,
            exclude_recent_condition_events=exclude_recent_condition_events,
        )
        for row in rows
    ]
    retired = [decision for decision in decisions if decision.eligible]
    not_eligible = [decision for decision in decisions if not decision.eligible]
    locked_retry_count = 0
    if retired and not dry_run:
        _apply_bulk_retire_decisions(
            connection,
            retired,
            run_id=run_id,
            retired_at=created_at,
        )
    pending_after = _count_pending(connection)
    condition_after = _count_pending_condition_events(connection)
    reason_counts = Counter(decision.reason for decision in decisions)
    by_projection_name = Counter(
        decision.projection_name for decision in retired if decision.eligible
    )
    by_event_type = Counter(decision.event_type for decision in retired if decision.eligible)
    bucket_counter: Counter[tuple[str, str, str, str]] = Counter()
    for decision in retired:
        bucket_counter[
            (
                decision.projection_name,
                decision.event_type,
                str(decision.terminal_status),
                decision.reason,
            )
        ] += 1
    status = "NOOP"
    if retired:
        status = "COMPLETED"
    if not_eligible and retired:
        status = "COMPLETED_WITH_WARNINGS"
    return ProjectionOutboxBulkRetireResult(
        run_id=run_id,
        status=status,
        dry_run=bool(dry_run),
        requested_limit=requested_limit,
        scanned_count=len(decisions),
        retired_count=len(retired),
        applied_count=sum(1 for decision in retired if decision.terminal_status == "APPLIED"),
        skipped_count=sum(1 for decision in retired if decision.terminal_status == "SKIPPED"),
        not_eligible_count=len(not_eligible),
        locked_retry_count=locked_retry_count,
        by_projection_name=dict(by_projection_name),
        by_event_type=dict(by_event_type),
        reason_counts=dict(reason_counts),
        buckets=tuple(
            ProjectionOutboxBulkRetireBucket(
                projection_name=projection_name,
                event_type=event_type,
                status=status,
                reason=reason,
                count=count,
            )
            for (projection_name, event_type, status, reason), count in sorted(
                bucket_counter.items()
            )
        ),
        sample_retired_jobs=tuple(_decision_sample(retired)),
        sample_not_eligible_jobs=tuple(_decision_sample(not_eligible)),
        pending_before=pending_before,
        pending_after=pending_after,
        pending_delta=pending_after - pending_before,
        condition_event_pending_before=condition_before,
        condition_event_pending_after=condition_after,
        condition_event_pending_delta=condition_after - condition_before,
        created_at=created_at,
    )


def decide_bulk_retire_job(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    recent_cutoff: str,
    exclude_recent_condition_events: bool,
) -> BulkRetireDecision:
    outbox_id = str(row["outbox_id"])
    event_id = str(row["event_id"])
    projection_name = str(row["projection_name"])
    event_type = str(row["event_type"])
    base = {
        "outbox_id": outbox_id,
        "event_id": event_id,
        "projection_name": projection_name,
        "event_type": event_type,
    }
    if str(row["status"]).upper() != "PENDING":
        return BulkRetireDecision(**base, terminal_status=None, reason="STATUS_NOT_PENDING")
    if int(row.get("attempts") or 0) > 0 and row.get("last_error"):
        return BulkRetireDecision(
            **base,
            terminal_status=None,
            reason="PRIOR_ERROR_ATTEMPT_NOT_ELIGIBLE",
        )
    source = _gateway_event_row(connection, event_id)
    if source is None:
        return BulkRetireDecision(
            **base,
            terminal_status=None,
            reason="SOURCE_GATEWAY_EVENT_MISSING",
        )
    source_status = str(source["status"])
    if source_status != "ACCEPTED":
        return BulkRetireDecision(
            **base,
            terminal_status=None,
            reason="SOURCE_GATEWAY_EVENT_NOT_ACCEPTED",
            source_gateway_event_status=source_status,
        )
    effective_skip = _effective_skip_inline(connection, event_id, projection_name)
    if effective_skip:
        return BulkRetireDecision(
            **base,
            terminal_status=None,
            reason="EFFECTIVE_SKIP_INLINE_EXCLUDED",
            source_gateway_event_status=source_status,
            effective_skip_inline=True,
        )
    if (
        exclude_recent_condition_events
        and event_type == "condition_event"
        and _timestamp_at_or_after(str(row["created_at"]), recent_cutoff)
        and projection_name == "condition_fusion"
    ):
        return BulkRetireDecision(
            **base,
            terminal_status=None,
            reason="RECENT_CONDITION_FUSION_PENDING_NOT_ELIGIBLE",
            source_gateway_event_status=source_status,
        )
    payload = _json_object(source["payload_json"])
    return _artifact_decision(
        connection,
        row=row,
        payload=payload,
        source_status=source_status,
        effective_skip=effective_skip,
    )


def estimate_bulk_retire_eligible_count(
    connection: sqlite3.Connection,
    *,
    older_than_sec: int = 60,
    include_projection_names: Iterable[str] | None = None,
    exclude_recent_condition_events: bool = True,
    limit: int = 20000,
) -> dict[str, int]:
    cutoff = datetime_to_wire(utc_now() - timedelta(seconds=max(int(older_than_sec), 0)))
    rows = _candidate_rows(
        connection,
        limit=max(min(int(limit), 20000), 1),
        older_than=cutoff,
        include_projection_names=_normalize_projection_filter(include_projection_names),
    )
    decisions = [
        decide_bulk_retire_job(
            connection,
            row,
            recent_cutoff=cutoff,
            exclude_recent_condition_events=exclude_recent_condition_events,
        )
        for row in rows
    ]
    eligible = [decision for decision in decisions if decision.eligible]
    return {
        "scanned_count": len(decisions),
        "bulk_retire_eligible_count": len(eligible),
        "bulk_retire_applied_eligible_count": sum(
            1 for decision in eligible if decision.terminal_status == "APPLIED"
        ),
        "bulk_retire_skipped_eligible_count": sum(
            1 for decision in eligible if decision.terminal_status == "SKIPPED"
        ),
        "not_eligible_count": len(decisions) - len(eligible),
    }


def count_blocking_pending(
    connection: sqlite3.Connection,
    *,
    older_than_sec: int = 60,
    exclude_recent_condition_events: bool = True,
    limit: int = 20000,
) -> dict[str, int]:
    del limit
    cutoff = datetime_to_wire(utc_now() - timedelta(seconds=max(int(older_than_sec), 0)))
    estimate = _fast_bulk_retire_counts(
        connection,
        older_than=cutoff,
        exclude_recent_condition_events=exclude_recent_condition_events,
    )
    total_pending = _count_pending(connection)
    effective_skip_pending = _count_effective_skip_pending(connection)
    condition_event_pending = _count_pending_condition_events(connection)
    condition_event_eligible = _count_condition_event_bulk_retire_eligible_fast(
        connection,
        recent_cutoff=cutoff,
        exclude_recent_condition_events=exclude_recent_condition_events,
    )
    condition_event_blocking = max(condition_event_pending - condition_event_eligible, 0)
    non_blocking = int(estimate["bulk_retire_eligible_count"])
    blocking = max(total_pending - non_blocking, 0)
    return {
        **estimate,
        "blocking_pending_count": blocking,
        "non_blocking_shadow_pending_count": non_blocking,
        "effective_skip_pending_count": effective_skip_pending,
        "condition_event_blocking_pending_count": condition_event_blocking,
    }


def _fast_bulk_retire_counts(
    connection: sqlite3.Connection,
    *,
    older_than: str,
    exclude_recent_condition_events: bool,
) -> dict[str, int]:
    recent_condition_fusion_clause = (
        "AND julianday(COALESCE(p.available_at, p.created_at)) < julianday(:older_than)"
        if exclude_recent_condition_events
        else ""
    )
    row = connection.execute(
        f"""
        WITH pending AS (
            SELECT
                o.outbox_id,
                o.projection_name,
                o.event_type,
                o.event_id,
                o.available_at,
                o.created_at,
                o.attempts,
                o.last_error,
                g.status AS gateway_status,
                g.payload_json
            FROM projection_outbox AS o
            JOIN gateway_events AS g
                ON g.event_id = o.event_id
            WHERE o.status = 'PENDING'
                AND julianday(COALESCE(o.available_at, o.created_at)) <= julianday(:older_than)
        ),
        decisions AS (
            SELECT
                p.*,
                CASE
                    WHEN p.gateway_status != 'ACCEPTED' THEN 0
                    WHEN COALESCE(p.attempts, 0) > 0
                        AND COALESCE(p.last_error, '') != '' THEN 0
                    WHEN EXISTS (
                        SELECT 1
                        FROM market_data_projection_routing_decisions AS d
                        WHERE d.event_id = p.event_id
                            AND d.projection_name = p.projection_name
                            AND d.effective_skip_inline = 1
                    ) THEN 0
                    WHEN p.projection_name = 'market_data'
                        AND p.event_type = 'price_tick'
                        AND (
                            EXISTS (
                                SELECT 1
                                FROM market_tick_samples AS sample
                                WHERE sample.event_id = p.event_id
                                LIMIT 1
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM market_projection_errors AS error
                                WHERE error.event_id = p.event_id
                                LIMIT 1
                            )
                        ) THEN 1
                    WHEN p.projection_name = 'market_data'
                        AND p.event_type = 'condition_event'
                        AND (
                            EXISTS (
                                SELECT 1
                                FROM market_condition_signals AS signal
                                WHERE signal.event_id = p.event_id
                                LIMIT 1
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM market_projection_errors AS error
                                WHERE error.event_id = p.event_id
                                LIMIT 1
                            )
                        ) THEN 1
                    WHEN p.projection_name = 'market_data'
                        AND p.event_type = 'tr_response'
                        AND (
                            EXISTS (
                                SELECT 1
                                FROM market_tr_snapshots AS snapshot
                                WHERE snapshot.event_id = p.event_id
                                LIMIT 1
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM market_projection_errors AS error
                                WHERE error.event_id = p.event_id
                                LIMIT 1
                            )
                            OR json_type(p.payload_json, '$.rows') IS NULL
                            OR json_type(p.payload_json, '$.rows') != 'array'
                            OR json_array_length(p.payload_json, '$.rows') = 0
                        ) THEN 1
                    WHEN p.projection_name = 'market_index'
                        AND p.event_type = 'market_index_tick'
                        AND (
                            EXISTS (
                                SELECT 1
                                FROM market_index_tick_samples AS sample
                                WHERE sample.event_id = p.event_id
                                LIMIT 1
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM market_index_projection_errors AS error
                                WHERE error.event_id = p.event_id
                                LIMIT 1
                            )
                        ) THEN 1
                    WHEN p.projection_name = 'market_reference'
                        AND p.event_type = 'market_symbols'
                        AND (
                            EXISTS (
                                SELECT 1
                                FROM market_symbol_memberships AS membership
                                WHERE membership.event_id = p.event_id
                                LIMIT 1
                            )
                            OR json_type(p.payload_json, '$.symbols') IS NULL
                            OR json_type(p.payload_json, '$.symbols') != 'array'
                            OR json_array_length(p.payload_json, '$.symbols') = 0
                        ) THEN 1
                    WHEN p.projection_name = 'market_regime'
                        AND p.event_type = 'market_index_tick'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM projection_outbox AS market_index_outbox
                            WHERE market_index_outbox.projection_name = 'market_index'
                                AND market_index_outbox.event_id = p.event_id
                                AND market_index_outbox.status IN ('ERROR', 'DEAD_LETTER')
                            LIMIT 1
                        ) THEN 1
                    WHEN p.projection_name = 'condition_fusion'
                        AND p.event_type = 'condition_event'
                        {recent_condition_fusion_clause}
                        AND EXISTS (
                            SELECT 1
                            FROM market_condition_signals AS signal
                            WHERE signal.event_id = p.event_id
                            LIMIT 1
                        ) THEN 1
                    WHEN p.projection_name = 'market_scan'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM projection_outbox AS scan_outbox
                            WHERE scan_outbox.projection_name = 'market_scan'
                                AND scan_outbox.event_id = p.event_id
                                AND scan_outbox.status IN ('ERROR', 'DEAD_LETTER')
                            LIMIT 1
                        ) THEN 1
                    ELSE 0
                END AS eligible,
                CASE
                    WHEN p.gateway_status != 'ACCEPTED' THEN 0
                    WHEN COALESCE(p.attempts, 0) > 0
                        AND COALESCE(p.last_error, '') != '' THEN 0
                    WHEN EXISTS (
                        SELECT 1
                        FROM market_data_projection_routing_decisions AS d
                        WHERE d.event_id = p.event_id
                            AND d.projection_name = p.projection_name
                            AND d.effective_skip_inline = 1
                    ) THEN 0
                    WHEN p.projection_name = 'market_data'
                        AND p.event_type = 'tr_response'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM market_tr_snapshots AS snapshot
                            WHERE snapshot.event_id = p.event_id
                            LIMIT 1
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM market_projection_errors AS error
                            WHERE error.event_id = p.event_id
                            LIMIT 1
                        )
                        AND (
                            json_type(p.payload_json, '$.rows') IS NULL
                            OR json_type(p.payload_json, '$.rows') != 'array'
                            OR json_array_length(p.payload_json, '$.rows') = 0
                        ) THEN 1
                    WHEN p.projection_name = 'market_reference'
                        AND p.event_type = 'market_symbols'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM market_symbol_memberships AS membership
                            WHERE membership.event_id = p.event_id
                            LIMIT 1
                        )
                        AND (
                            json_type(p.payload_json, '$.symbols') IS NULL
                            OR json_type(p.payload_json, '$.symbols') != 'array'
                            OR json_array_length(p.payload_json, '$.symbols') = 0
                        ) THEN 1
                    WHEN p.projection_name = 'market_regime'
                        AND p.event_type = 'market_index_tick'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM projection_outbox AS market_index_outbox
                            WHERE market_index_outbox.projection_name = 'market_index'
                                AND market_index_outbox.event_id = p.event_id
                                AND market_index_outbox.status IN ('ERROR', 'DEAD_LETTER')
                            LIMIT 1
                        ) THEN 1
                    WHEN p.projection_name = 'condition_fusion'
                        AND p.event_type = 'condition_event'
                        {recent_condition_fusion_clause}
                        AND EXISTS (
                            SELECT 1
                            FROM market_condition_signals AS signal
                            WHERE signal.event_id = p.event_id
                            LIMIT 1
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM market_condition_signals AS signal
                            JOIN candidate_condition_fusion AS fusion
                                ON fusion.code = signal.code
                            WHERE signal.event_id = p.event_id
                            LIMIT 1
                        ) THEN 1
                    WHEN p.projection_name = 'market_scan'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM projection_outbox AS scan_outbox
                            WHERE scan_outbox.projection_name = 'market_scan'
                                AND scan_outbox.event_id = p.event_id
                                AND scan_outbox.status IN ('ERROR', 'DEAD_LETTER')
                            LIMIT 1
                        ) THEN 1
                    ELSE 0
                END AS skipped_eligible
            FROM pending AS p
        )
        SELECT
            COUNT(*) AS scanned_count,
            COALESCE(SUM(eligible), 0) AS bulk_retire_eligible_count,
            COALESCE(SUM(CASE WHEN eligible = 1 AND skipped_eligible = 0 THEN 1 ELSE 0 END), 0)
                AS bulk_retire_applied_eligible_count,
            COALESCE(SUM(CASE WHEN eligible = 1 AND skipped_eligible = 1 THEN 1 ELSE 0 END), 0)
                AS bulk_retire_skipped_eligible_count
        FROM decisions
        """,
        {"older_than": older_than},
    ).fetchone()
    scanned_count = int(row["scanned_count"] or 0)
    eligible_count = int(row["bulk_retire_eligible_count"] or 0)
    return {
        "scanned_count": scanned_count,
        "bulk_retire_eligible_count": eligible_count,
        "bulk_retire_applied_eligible_count": int(
            row["bulk_retire_applied_eligible_count"] or 0
        ),
        "bulk_retire_skipped_eligible_count": int(
            row["bulk_retire_skipped_eligible_count"] or 0
        ),
        "not_eligible_count": max(scanned_count - eligible_count, 0),
    }


def _count_condition_event_bulk_retire_eligible_fast(
    connection: sqlite3.Connection,
    *,
    recent_cutoff: str,
    exclude_recent_condition_events: bool,
) -> int:
    recent_condition_fusion_clause = (
        "AND julianday(COALESCE(o.available_at, o.created_at)) < julianday(:recent_cutoff)"
        if exclude_recent_condition_events
        else ""
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM projection_outbox AS o
        JOIN gateway_events AS g
            ON g.event_id = o.event_id
        WHERE o.status = 'PENDING'
            AND o.event_type = 'condition_event'
            AND g.status = 'ACCEPTED'
            AND NOT (
                COALESCE(o.attempts, 0) > 0
                AND COALESCE(o.last_error, '') != ''
            )
            AND NOT EXISTS (
                SELECT 1
                FROM market_data_projection_routing_decisions AS d
                WHERE d.event_id = o.event_id
                    AND d.projection_name = o.projection_name
                    AND d.effective_skip_inline = 1
            )
            AND (
                (
                    o.projection_name = 'market_data'
                    AND (
                        EXISTS (
                            SELECT 1
                            FROM market_condition_signals AS signal
                            WHERE signal.event_id = o.event_id
                            LIMIT 1
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM market_projection_errors AS error
                            WHERE error.event_id = o.event_id
                            LIMIT 1
                        )
                    )
                )
                OR (
                    o.projection_name = 'condition_fusion'
                    {recent_condition_fusion_clause}
                    AND EXISTS (
                        SELECT 1
                        FROM market_condition_signals AS signal
                        WHERE signal.event_id = o.event_id
                        LIMIT 1
                    )
                )
            )
        """,
        {"recent_cutoff": recent_cutoff},
    ).fetchone()
    return int(row["count"] or 0)


def _artifact_decision(
    connection: sqlite3.Connection,
    *,
    row: Mapping[str, Any],
    payload: Mapping[str, Any],
    source_status: str,
    effective_skip: bool,
) -> BulkRetireDecision:
    outbox_id = str(row["outbox_id"])
    event_id = str(row["event_id"])
    projection_name = str(row["projection_name"])
    event_type = str(row["event_type"])
    base = {
        "outbox_id": outbox_id,
        "event_id": event_id,
        "projection_name": projection_name,
        "event_type": event_type,
        "source_gateway_event_status": source_status,
        "effective_skip_inline": effective_skip,
    }
    projection_error = _exists(
        connection,
        "market_projection_errors",
        "event_id",
        event_id,
    )
    if projection_name == "market_data" and event_type == "price_tick":
        if _exists(connection, "market_tick_samples", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_PRICE_TICK_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if projection_error:
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_PROJECTION_ERROR_OBSERVED",
            )
    if projection_name == "market_data" and event_type == "condition_event":
        if _exists(connection, "market_condition_signals", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_CONDITION_EVENT_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if projection_error:
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_PROJECTION_ERROR_OBSERVED",
            )
    if projection_name == "market_data" and event_type == "tr_response":
        if _exists(connection, "market_tr_snapshots", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_TR_RESPONSE_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if not _payload_has_rows(payload):
            return BulkRetireDecision(
                **base,
                terminal_status="SKIPPED",
                reason="INLINE_MARKET_DATA_TR_RESPONSE_ROWS_EMPTY",
            )
        if projection_error:
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_DATA_PROJECTION_ERROR_OBSERVED",
            )
    if projection_name == "market_index" and event_type == "market_index_tick":
        if _exists(connection, "market_index_tick_samples", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_INDEX_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if _exists(connection, "market_index_projection_errors", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_INDEX_PROJECTION_ERROR_OBSERVED",
            )
    if projection_name == "market_reference" and event_type == "market_symbols":
        if _exists(connection, "market_symbol_memberships", "event_id", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="INLINE_MARKET_REFERENCE_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if not _payload_symbols(payload):
            return BulkRetireDecision(
                **base,
                terminal_status="SKIPPED",
                reason="INLINE_MARKET_REFERENCE_SYMBOLS_EMPTY",
            )
    if projection_name == "market_regime" and event_type == "market_index_tick":
        if not _outbox_terminal_error_exists(connection, "market_index", event_id):
            return BulkRetireDecision(
                **base,
                terminal_status="SKIPPED",
                reason="MARKET_REGIME_SHADOW_RETIRE_UNSAFE_NOT_BLOCKING",
            )
    if projection_name == "condition_fusion" and event_type == "condition_event":
        signal = _condition_signal_row(connection, event_id)
        if signal is not None and _condition_fusion_code_exists(
            connection,
            str(signal["code"]),
        ):
            return BulkRetireDecision(
                **base,
                terminal_status="APPLIED",
                reason="CONDITION_FUSION_ARTIFACT_OBSERVED",
                inline_artifact_observed=True,
            )
        if signal is not None:
            return BulkRetireDecision(
                **base,
                terminal_status="SKIPPED",
                reason="CONDITION_FUSION_SHADOW_VERIFY_NOT_SUPPORTED_NOT_BLOCKING",
            )
    if projection_name == "market_scan" and not _market_scan_error_exists(
        connection,
        event_id,
    ):
        return BulkRetireDecision(
            **base,
            terminal_status="SKIPPED",
            reason="MARKET_SCAN_SHADOW_VERIFY_NOT_SUPPORTED_NOT_BLOCKING",
        )
    return BulkRetireDecision(
        **base,
        terminal_status=None,
        reason="NO_INLINE_ARTIFACT_OR_SKIP_SAFE_RULE",
    )


def _apply_bulk_retire_decisions(
    connection: sqlite3.Connection,
    decisions: Sequence[BulkRetireDecision],
    *,
    run_id: str,
    retired_at: str,
) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        for decision in decisions:
            metadata_json = _bulk_metadata_json(
                connection,
                decision,
                run_id=run_id,
                retired_at=retired_at,
            )
            connection.execute(
                """
                UPDATE projection_outbox
                SET
                    status = ?,
                    updated_at = ?,
                    processed_at = ?,
                    locked_by = NULL,
                    locked_at = NULL,
                    last_error = NULL,
                    metadata_json = ?
                WHERE outbox_id = ?
                    AND status = 'PENDING'
                """,
                (
                    decision.terminal_status,
                    retired_at,
                    retired_at,
                    metadata_json,
                    decision.outbox_id,
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _bulk_metadata_json(
    connection: sqlite3.Connection,
    decision: BulkRetireDecision,
    *,
    run_id: str,
    retired_at: str,
) -> str:
    row = connection.execute(
        "SELECT metadata_json FROM projection_outbox WHERE outbox_id = ?",
        (decision.outbox_id,),
    ).fetchone()
    metadata = _json_object(row["metadata_json"] if row is not None else "{}")
    metadata.update(
        {
            "bulk_retired": True,
            "bulk_retire_run_id": run_id,
            "bulk_retire_reason": decision.reason,
            "bulk_retire_mode": "SHADOW_RETIRE",
            "inline_artifact_observed": bool(decision.inline_artifact_observed),
            "source_gateway_event_status": decision.source_gateway_event_status,
            "effective_skip_inline": bool(decision.effective_skip_inline),
            "no_trading_side_effects": True,
            "projection_side_effects_allowed": False,
            "retired_at": retired_at,
        }
    )
    if decision.metadata:
        metadata["bulk_retire_evidence"] = dict(decision.metadata)
    return canonical_json(metadata)


def _candidate_rows(
    connection: sqlite3.Connection,
    *,
    limit: int,
    older_than: str,
    include_projection_names: set[str] | None,
) -> list[dict[str, Any]]:
    params: list[Any] = [older_than]
    projection_clause = ""
    if include_projection_names:
        placeholders = ",".join("?" for _ in include_projection_names)
        projection_clause = f"AND projection_name IN ({placeholders})"
        params.extend(sorted(include_projection_names))
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT *
        FROM projection_outbox
        WHERE status = 'PENDING'
            AND julianday(COALESCE(available_at, created_at)) <= julianday(?)
            {projection_clause}
        ORDER BY event_rowid ASC, created_at ASC, outbox_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _count_pending(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS count FROM projection_outbox WHERE status = 'PENDING'"
        ).fetchone()["count"]
    )


def _count_pending_condition_events(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PENDING' AND event_type = 'condition_event'
            """
        ).fetchone()["count"]
    )


def _count_effective_skip_pending(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox AS o
            JOIN market_data_projection_routing_decisions AS d
                ON d.event_id = o.event_id
            WHERE o.status = 'PENDING'
                AND d.effective_skip_inline = 1
            """
        ).fetchone()["count"]
    )


def _count_condition_event_blocking_pending(
    connection: sqlite3.Connection,
    *,
    older_than_sec: int,
    exclude_recent_condition_events: bool,
    limit: int,
) -> int:
    cutoff = datetime_to_wire(utc_now() - timedelta(seconds=max(int(older_than_sec), 0)))
    rows = connection.execute(
        """
        SELECT *
        FROM projection_outbox
        WHERE status = 'PENDING'
            AND event_type = 'condition_event'
        ORDER BY event_rowid ASC, created_at ASC, outbox_id ASC
        LIMIT ?
        """,
        (max(min(int(limit), 20000), 1),),
    ).fetchall()
    count = 0
    for row in rows:
        decision = decide_bulk_retire_job(
            connection,
            dict(row),
            recent_cutoff=cutoff,
            exclude_recent_condition_events=exclude_recent_condition_events,
        )
        if not decision.eligible:
            count += 1
    return count


def _gateway_event_row(
    connection: sqlite3.Connection,
    event_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT event_id, event_type, status, payload_json
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()


def _effective_skip_inline(
    connection: sqlite3.Connection,
    event_id: str,
    projection_name: str,
) -> bool:
    row = connection.execute(
        """
        SELECT effective_skip_inline
        FROM market_data_projection_routing_decisions
        WHERE event_id = ? AND projection_name = ?
        LIMIT 1
        """,
        (event_id, projection_name),
    ).fetchone()
    return row is not None and int(row["effective_skip_inline"] or 0) == 1


def _exists(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    value: str,
) -> bool:
    if table_name not in {
        "market_tick_samples",
        "market_condition_signals",
        "market_tr_snapshots",
        "market_projection_errors",
        "market_index_tick_samples",
        "market_index_projection_errors",
        "market_symbol_memberships",
    }:
        raise ValueError(f"unsupported table: {table_name}")
    if column_name != "event_id":
        raise ValueError(f"unsupported column: {column_name}")
    return (
        connection.execute(
            f"SELECT 1 FROM {table_name} WHERE {column_name} = ? LIMIT 1",
            (value,),
        ).fetchone()
        is not None
    )


def _condition_signal_row(
    connection: sqlite3.Connection,
    event_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT code FROM market_condition_signals WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()


def _condition_fusion_code_exists(
    connection: sqlite3.Connection,
    code: str,
) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM candidate_condition_fusion WHERE code = ? LIMIT 1",
            (code,),
        ).fetchone()
        is not None
    )


def _outbox_terminal_error_exists(
    connection: sqlite3.Connection,
    projection_name: str,
    event_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT status
        FROM projection_outbox
        WHERE projection_name = ? AND event_id = ?
        LIMIT 1
        """,
        (projection_name, event_id),
    ).fetchone()
    return row is not None and str(row["status"]) in {"ERROR", "DEAD_LETTER"}


def _market_scan_error_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM projection_outbox
        WHERE projection_name = 'market_scan'
            AND event_id = ?
            AND status IN ('ERROR', 'DEAD_LETTER')
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return row is not None


def _payload_has_rows(payload: Mapping[str, Any]) -> bool:
    rows = payload.get("rows")
    return isinstance(rows, list) and bool(rows)


def _payload_symbols(payload: Mapping[str, Any]) -> list[Any]:
    symbols = payload.get("symbols")
    return symbols if isinstance(symbols, list) else []


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str):
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _timestamp_at_or_after(value: str, cutoff: str) -> bool:
    try:
        return parse_timestamp(value, "value") >= parse_timestamp(cutoff, "cutoff")
    except ValueError:
        return False


def _normalize_projection_filter(values: Iterable[str] | None) -> set[str] | None:
    if values is None:
        return None
    normalized = {str(value).strip() for value in values if str(value).strip()}
    return normalized or None


def _decision_sample(
    decisions: Sequence[BulkRetireDecision],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    return [
        {
            "outbox_id": decision.outbox_id,
            "event_id": decision.event_id,
            "projection_name": decision.projection_name,
            "event_type": decision.event_type,
            "status": decision.terminal_status,
            "reason": decision.reason,
        }
        for decision in decisions[:limit]
    ]
