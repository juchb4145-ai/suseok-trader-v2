from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.conditions import BrokerConditionEvent
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
from services.market_data_service import QUOTE_ONLY_REAL_TYPES, normalize_market_data_exchange


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
    no_trading_side_effects: bool = True
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
            "no_trading_side_effects": self.no_trading_side_effects,
            "errors": list(self.errors),
            "created_at": self.created_at,
        }


def process_projection_outbox_batch(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int | None = None,
    owner_id: str | None = None,
) -> ProjectionOutboxBatchResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("projection_outbox_shadow")
    resolved_owner_id = owner_id or run_id
    bounded_limit = limit or resolved_settings.projection_outbox_batch_size
    created_at = datetime_to_wire(utc_now())
    claimed_jobs = claim_projection_outbox_jobs(
        connection,
        owner_id=resolved_owner_id,
        limit=bounded_limit,
        processing_ttl_sec=resolved_settings.projection_outbox_processing_ttl_sec,
        min_age_sec=resolved_settings.projection_outbox_shadow_min_age_sec,
    )
    applied_count = 0
    skipped_count = 0
    error_count = 0
    dead_letter_count = 0
    errors: list[dict[str, Any]] = []

    for job in claimed_jobs:
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
            "apply_projection": False,
            "no_trading_side_effects": True,
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
        elif verification.status == "SKIPPED":
            mark_projection_outbox_skipped(
                connection,
                outbox_id,
                owner_id=resolved_owner_id,
                reason=verification.reason,
                evidence=evidence,
            )
            skipped_count += 1
        else:
            message = verification.error_message or verification.reason
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
        errors=tuple(errors),
        created_at=created_at,
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
        SELECT event_id, event_type, status, event_ts, payload_json
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()


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


def _verification_error(reason: str, error_message: str) -> ProjectionOutboxVerificationResult:
    normalized_message = f"{reason}: {error_message}"
    return ProjectionOutboxVerificationResult(
        status="ERROR",
        reason=reason,
        evidence={"reason": reason},
        error_message=normalized_message,
    )
