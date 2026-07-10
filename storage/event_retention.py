from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings, load_settings

from storage.projection_retention import (
    DEFAULT_PRUNABLE_EVENT_TYPES,
    build_projection_retention_rca,
    count_retention_age_eligible_events,
    count_retention_eligible_events,
    select_retention_eligible_event_ids,
)
from storage.projection_watermarks import (
    get_projection_watermark,
    get_projection_watermark_status,
)

MARKET_DATA_PROJECTION_NAME = "market_data"


class EventRetentionSafetyError(ValueError):
    def __init__(self, message: str, *, reason_codes: tuple[str, ...]) -> None:
        super().__init__(message)
        self.reason_codes = reason_codes

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "BLOCKED",
            "error_message": str(self),
            "reason_codes": list(self.reason_codes),
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }


@dataclass(frozen=True, kw_only=True)
class EventRetentionResult:
    run_id: str
    dry_run: bool
    cutoff_at: str
    retention_days: int
    candidate_event_count: int
    selected_event_count: int
    deleted_gateway_event_count: int
    deleted_raw_event_count: int
    market_data_watermark_rowid: int
    prunable_event_types: tuple[str, ...]
    age_eligible_event_count: int = 0
    projection_blocked_event_count: int = 0
    projection_watermarks: Mapping[str, Any] = field(default_factory=dict)
    blocked_reason_counts: Mapping[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "cutoff_at": self.cutoff_at,
            "retention_days": self.retention_days,
            "candidate_event_count": self.candidate_event_count,
            "selected_event_count": self.selected_event_count,
            "deleted_gateway_event_count": self.deleted_gateway_event_count,
            "deleted_raw_event_count": self.deleted_raw_event_count,
            "market_data_watermark_rowid": self.market_data_watermark_rowid,
            "prunable_event_types": list(self.prunable_event_types),
            "age_eligible_event_count": self.age_eligible_event_count,
            "projection_blocked_event_count": self.projection_blocked_event_count,
            "projection_watermarks": dict(self.projection_watermarks),
            "blocked_reason_counts": dict(self.blocked_reason_counts),
            "projection_retention_gate_pass": self.projection_blocked_event_count == 0,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }


def get_event_retention_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    retention_days: int | None = None,
    exact_counts: bool = False,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    resolved_days = _resolve_retention_days(resolved_settings, retention_days)
    cutoff_at = _cutoff_for_days(resolved_days)
    market_data_watermark = get_projection_watermark(
        connection,
        MARKET_DATA_PROJECTION_NAME,
    )
    watermark_status = get_projection_watermark_status(connection)
    age_eligible_count = count_retention_age_eligible_events(
        connection,
        cutoff_at=cutoff_at,
        event_types=DEFAULT_PRUNABLE_EVENT_TYPES,
    )
    blocked_rca = build_projection_retention_rca(
        connection,
        cutoff_at=cutoff_at,
        event_types=DEFAULT_PRUNABLE_EVENT_TYPES,
        limit=20,
        blocked_only=True,
    )
    blocked_detected = int(blocked_rca["item_count"]) > 0
    candidate_count: int | None
    blocked_count: int | None
    counts_exact = bool(exact_counts or not blocked_detected)
    if exact_counts:
        candidate_count = count_retention_eligible_events(
            connection,
            cutoff_at=cutoff_at,
            event_types=DEFAULT_PRUNABLE_EVENT_TYPES,
        )
        blocked_count = max(age_eligible_count - candidate_count, 0)
        blocked_detected = blocked_count > 0
    elif blocked_detected:
        candidate_count = None
        blocked_count = None
    else:
        candidate_count = age_eligible_count
        blocked_count = 0
    enabled = bool(resolved_settings.event_store_retention_enabled)
    reason_codes: list[str] = []
    if not enabled:
        reason_codes.append("EVENT_RETENTION_DISABLED")
    if blocked_detected:
        reason_codes.append("PROJECTION_RETENTION_GATE_BLOCKED")
    return {
        "status": "PASS" if enabled and not blocked_detected else "WARN",
        "enabled": enabled,
        "apply_ready": enabled and not blocked_detected,
        "retention_days": resolved_days,
        "cutoff_at": cutoff_at,
        "age_eligible_event_count": age_eligible_count,
        "candidate_event_count": candidate_count,
        "projection_blocked_event_count": blocked_count,
        "projection_blocked_probe_count": int(blocked_rca["item_count"]),
        "projection_blocked_detected": blocked_detected,
        "counts_exact": counts_exact,
        "projection_retention_gate_pass": not blocked_detected,
        "market_data_watermark": market_data_watermark.to_dict(),
        "projection_watermarks": watermark_status,
        "blocked_reason_counts": blocked_rca["reason_counts"],
        "blocked_samples": blocked_rca["items"],
        "prunable_event_types": list(DEFAULT_PRUNABLE_EVENT_TYPES),
        "dry_run_default": True,
        "batch_size": resolved_settings.event_store_retention_batch_size,
        "interval_sec": resolved_settings.event_store_retention_interval_sec,
        "reason_codes": reason_codes,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def prune_event_store_events(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    retention_days: int | None = None,
    dry_run: bool = True,
    limit: int | None = None,
) -> EventRetentionResult:
    resolved_settings = settings or load_settings()
    resolved_days = _resolve_retention_days(resolved_settings, retention_days)
    bounded_limit = _resolve_limit(resolved_settings, limit)
    cutoff_at = _cutoff_for_days(resolved_days)
    event_types = DEFAULT_PRUNABLE_EVENT_TYPES
    market_data_watermark = get_projection_watermark(
        connection,
        MARKET_DATA_PROJECTION_NAME,
    )
    watermark_status = get_projection_watermark_status(connection)
    watermark_payload = {
        item["projection_name"]: item
        for item in watermark_status.get("watermarks", [])
    }
    age_eligible_count = count_retention_age_eligible_events(
        connection,
        cutoff_at=cutoff_at,
        event_types=event_types,
    )
    candidate_count = count_retention_eligible_events(
        connection,
        cutoff_at=cutoff_at,
        event_types=event_types,
    )
    blocked_count = max(age_eligible_count - candidate_count, 0)
    blocked_rca = build_projection_retention_rca(
        connection,
        cutoff_at=cutoff_at,
        event_types=event_types,
        limit=100,
        blocked_only=True,
    )
    if not dry_run:
        _require_retention_apply_ready(
            settings=resolved_settings,
            projection_blocked_event_count=blocked_count,
        )
    event_ids = select_retention_eligible_event_ids(
        connection,
        cutoff_at=cutoff_at,
        event_types=event_types,
        limit=bounded_limit,
    )
    result = EventRetentionResult(
        run_id=f"event-retention-{uuid.uuid4().hex}",
        dry_run=bool(dry_run),
        cutoff_at=cutoff_at,
        retention_days=resolved_days,
        candidate_event_count=candidate_count,
        selected_event_count=len(event_ids),
        deleted_gateway_event_count=0,
        deleted_raw_event_count=0,
        market_data_watermark_rowid=market_data_watermark.last_success_event_rowid,
        prunable_event_types=event_types,
        age_eligible_event_count=age_eligible_count,
        projection_blocked_event_count=blocked_count,
        projection_watermarks=watermark_payload,
        blocked_reason_counts=blocked_rca["reason_counts"],
    )
    if dry_run:
        return result

    try:
        connection.execute("BEGIN IMMEDIATE")
        deleted_gateway_count = _delete_events_by_id(
            connection,
            "gateway_events",
            event_ids,
        )
        deleted_raw_count = _delete_events_by_id(connection, "raw_events", event_ids)
        result = replace(
            result,
            dry_run=False,
            deleted_gateway_event_count=deleted_gateway_count,
            deleted_raw_event_count=deleted_raw_count,
        )
        _record_event_retention_run(connection, result)
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def _require_retention_apply_ready(
    *,
    settings: Settings,
    projection_blocked_event_count: int,
) -> None:
    reasons: list[str] = []
    if not settings.event_store_retention_enabled:
        reasons.append("EVENT_RETENTION_DISABLED")
    if projection_blocked_event_count > 0:
        reasons.append("PROJECTION_RETENTION_GATE_BLOCKED")
    if reasons:
        raise EventRetentionSafetyError(
            "event retention apply is blocked by fail-closed safety gates",
            reason_codes=tuple(reasons),
        )


def _resolve_retention_days(settings: Settings, retention_days: int | None) -> int:
    return max(int(retention_days or settings.event_store_retention_days), 1)


def _resolve_limit(settings: Settings, limit: int | None) -> int:
    return max(int(limit or settings.event_store_retention_batch_size), 1)


def _cutoff_for_days(retention_days: int) -> str:
    return datetime_to_wire(utc_now() - timedelta(days=max(int(retention_days), 1)))


def _delete_events_by_id(
    connection: sqlite3.Connection,
    table_name: str,
    event_ids: list[str],
) -> int:
    if not event_ids:
        return 0
    placeholders = ", ".join("?" for _ in event_ids)
    cursor = connection.execute(
        f"DELETE FROM {table_name} WHERE event_id IN ({placeholders})",
        tuple(event_ids),
    )
    return int(cursor.rowcount if cursor.rowcount is not None else 0)


def _record_event_retention_run(
    connection: sqlite3.Connection,
    result: EventRetentionResult,
) -> None:
    connection.execute(
        """
        INSERT INTO event_retention_runs (
            run_id,
            cutoff_at,
            retention_days,
            dry_run,
            candidate_event_count,
            selected_event_count,
            deleted_gateway_event_count,
            deleted_raw_event_count,
            market_data_watermark_rowid,
            prunable_event_types_json,
            age_eligible_event_count,
            projection_blocked_event_count,
            projection_watermarks_json,
            blocked_reason_counts_json,
            no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.cutoff_at,
            result.retention_days,
            int(result.dry_run),
            result.candidate_event_count,
            result.selected_event_count,
            result.deleted_gateway_event_count,
            result.deleted_raw_event_count,
            result.market_data_watermark_rowid,
            json.dumps(list(result.prunable_event_types), sort_keys=True),
            result.age_eligible_event_count,
            result.projection_blocked_event_count,
            json.dumps(dict(result.projection_watermarks), sort_keys=True),
            json.dumps(dict(result.blocked_reason_counts), sort_keys=True),
            1,
        ),
    )
