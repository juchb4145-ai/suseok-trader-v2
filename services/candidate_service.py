from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.fsm import determine_next_state, merge_reason_codes
from domain.candidate.models import CandidateSourceEvent
from domain.candidate.reasons import CandidateReasonCode
from domain.candidate.source import THEME_SOURCE_TYPES, CandidateEventType, CandidateSourceType
from domain.candidate.state import CandidateState
from domain.market.models import MarketDataQualityStatus
from domain.theme.state import ThemeMemberRole
from storage.gateway_command_store import canonical_json

from services.condition_fusion import (
    condition_signal_trade_date_bounds,
    is_profile_condition_metadata,
    list_condition_fusion,
    rebuild_condition_fusion,
)
from services.config import Settings, candidate_timezone, load_settings
from services.market_context_service import get_market_context_for_code
from services.market_data_service import get_latest_tick, get_market_data_readiness
from services.runtime.evaluation_run_guard import assert_runtime_execution_fence


@dataclass(frozen=True, kw_only=True)
class CandidateSourceApplyResult:
    source_event_count: int = 0
    candidate_created_count: int = 0
    candidate_updated_count: int = 0
    duplicate_source_count: int = 0
    transition_count: int = 0
    closed_count: int = 0
    error_count: int = 0


@dataclass(frozen=True, kw_only=True)
class CandidateSourceIngestResult:
    source_event_count: int = 0
    candidate_created_count: int = 0
    candidate_updated_count: int = 0
    duplicate_source_count: int = 0
    transition_count: int = 0
    error_count: int = 0


@dataclass(frozen=True, kw_only=True)
class CandidateRefreshResult:
    context_refreshed_count: int = 0
    transition_count: int = 0
    stale_count: int = 0
    closed_count: int = 0
    error_count: int = 0


@dataclass(frozen=True, kw_only=True)
class CandidateRebuildResult:
    source_event_count: int = 0
    candidate_created_count: int = 0
    candidate_updated_count: int = 0
    transition_count: int = 0
    context_refreshed_count: int = 0
    stale_count: int = 0
    closed_count: int = 0
    error_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "source_event_count": self.source_event_count,
            "candidate_created_count": self.candidate_created_count,
            "candidate_updated_count": self.candidate_updated_count,
            "transition_count": self.transition_count,
            "context_refreshed_count": self.context_refreshed_count,
            "stale_count": self.stale_count,
            "closed_count": self.closed_count,
            "error_count": self.error_count,
        }


_SOURCE_INGEST_COMMIT_CHUNK_SIZE = 10
_SOURCE_EVENT_SAVEPOINT = "candidate_source_event_apply"


def ingest_condition_sources(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
) -> CandidateSourceIngestResult:
    try:
        return _ingest_condition_sources(
            connection,
            trade_date,
            settings=settings,
        )
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise


def _ingest_condition_sources(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
) -> CandidateSourceIngestResult:
    resolved_settings = settings or load_settings()
    if not resolved_settings.candidate_fsm_enabled:
        return CandidateSourceIngestResult()

    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    rebuild_condition_fusion(connection, target_trade_date, settings=resolved_settings)
    start_at, end_at = condition_signal_trade_date_bounds(
        target_trade_date,
        resolved_settings,
    )
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_signals
        WHERE event_ts >= ?
            AND event_ts < ?
        ORDER BY event_ts ASC, id ASC
        """,
        (start_at, end_at),
    ).fetchall()
    result = _MutableIngestResult()
    processed_in_chunk = 0
    for fusion in list_condition_fusion(
        connection,
        trade_date=target_trade_date,
        settings=resolved_settings,
        limit=500,
    ):
        if not fusion.get("active_roles"):
            continue
        try:
            source_event = _fusion_row_to_source_event(fusion, target_trade_date)
            applied = _apply_candidate_source_event_atomically(
                connection,
                source_event,
                settings=resolved_settings,
            )
            result.add_apply(applied)
        except Exception as exc:
            result.error_count += 1
            _record_projection_error(
                connection,
                candidate_instance_id=None,
                source_event_id=fusion.get("latest_event_id"),
                code=fusion.get("code"),
                error_message=str(exc),
                payload=fusion,
            )
        processed_in_chunk += 1
        if processed_in_chunk >= _SOURCE_INGEST_COMMIT_CHUNK_SIZE:
            _commit_source_ingest_chunk(connection)
            processed_in_chunk = 0
    for row in rows:
        if _trade_date_for_timestamp(row["event_ts"], resolved_settings) != target_trade_date:
            continue
        if is_profile_condition_metadata(_condition_row_metadata(row)):
            continue
        try:
            source_event = _condition_row_to_source_event(row, target_trade_date, resolved_settings)
            applied = _apply_candidate_source_event_atomically(
                connection,
                source_event,
                settings=resolved_settings,
            )
            result.add_apply(applied)
        except Exception as exc:
            result.error_count += 1
            _record_projection_error(
                connection,
                candidate_instance_id=None,
                source_event_id=row["event_id"] if "event_id" in row.keys() else None,
                code=row["code"] if "code" in row.keys() else None,
                error_message=str(exc),
                payload=_row_to_dict(row),
            )
        processed_in_chunk += 1
        if processed_in_chunk >= _SOURCE_INGEST_COMMIT_CHUNK_SIZE:
            _commit_source_ingest_chunk(connection)
            processed_in_chunk = 0
    _commit_source_ingest_chunk(connection)
    return result.to_ingest_result()


def ingest_theme_sources(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
) -> CandidateSourceIngestResult:
    try:
        return _ingest_theme_sources(
            connection,
            trade_date,
            settings=settings,
        )
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise


def _ingest_theme_sources(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
) -> CandidateSourceIngestResult:
    resolved_settings = settings or load_settings()
    if not resolved_settings.candidate_fsm_enabled:
        return CandidateSourceIngestResult()

    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    rows = connection.execute(
        """
        SELECT
            l.snapshot_id,
            l.theme_id,
            l.theme_name,
            l.calculated_at,
            l.state,
            l.quality_status,
            m.code,
            m.name,
            m.member_role,
            m.readiness_status,
            m.tick_age_sec,
            m.event_ts,
            m.metadata_json
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshot_members AS m ON m.snapshot_id = l.snapshot_id
        ORDER BY l.calculated_at ASC, l.theme_name ASC, m.code ASC
        """
    ).fetchall()
    result = _MutableIngestResult()
    processed_in_chunk = 0
    for row in rows:
        if _trade_date_for_timestamp(row["calculated_at"], resolved_settings) != target_trade_date:
            continue
        if str(row["state"]).upper() not in resolved_settings.candidate_theme_source_states:
            continue
        if str(row["member_role"]).upper() not in resolved_settings.candidate_theme_member_roles:
            continue
        try:
            source_event = _theme_row_to_source_event(row, target_trade_date)
            applied = _apply_candidate_source_event_atomically(
                connection,
                source_event,
                settings=resolved_settings,
            )
            result.add_apply(applied)
        except Exception as exc:
            result.error_count += 1
            _record_projection_error(
                connection,
                candidate_instance_id=None,
                source_event_id=None,
                code=row["code"] if "code" in row.keys() else None,
                error_message=str(exc),
                payload=_row_to_dict(row),
            )
        processed_in_chunk += 1
        if processed_in_chunk >= _SOURCE_INGEST_COMMIT_CHUNK_SIZE:
            _commit_source_ingest_chunk(connection)
            processed_in_chunk = 0
    _commit_source_ingest_chunk(connection)
    return result.to_ingest_result()


def ingest_manual_watch_source(
    connection: sqlite3.Connection,
    code: str,
    name: str,
    reason: str | None = None,
    *,
    trade_date: str | None = None,
    settings: Settings | None = None,
) -> CandidateSourceApplyResult:
    resolved_settings = settings or load_settings()
    now = utc_now()
    normalized_code = validate_stock_code(code)
    source_event = CandidateSourceEvent(
        source_event_id=_source_event_id(
            "manual",
            _resolve_trade_date(trade_date, resolved_settings),
            normalized_code,
            reason or "manual",
            datetime_to_wire(now),
        ),
        trade_date=_resolve_trade_date(trade_date, resolved_settings),
        code=normalized_code,
        name=name,
        source_type=CandidateSourceType.MANUAL_WATCH,
        source_id=f"manual:{normalized_code}",
        action="ENTER",
        event_ts=now,
        observed_at=now,
        payload={"reason": reason},
        reason_codes=[CandidateReasonCode.SOURCE_DETECTED.value],
    )
    result = create_or_merge_candidate_from_source(
        connection,
        source_event,
        settings=resolved_settings,
    )
    connection.commit()
    return result


def create_or_merge_candidate_from_source(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    *,
    settings: Settings | None = None,
) -> CandidateSourceApplyResult:
    resolved_settings = settings or load_settings()
    if not resolved_settings.candidate_fsm_enabled:
        return CandidateSourceApplyResult()
    if _source_event_exists(connection, source_event.source_event_id):
        return CandidateSourceApplyResult(duplicate_source_count=1)

    if source_event.source_type is CandidateSourceType.CONDITION_EXIT:
        return _apply_exit_source_event(connection, source_event, settings=resolved_settings)

    active_candidate = _find_active_candidate(
        connection,
        trade_date=source_event.trade_date,
        code=source_event.code,
    )
    created = active_candidate is None
    transition_count = 0
    if created:
        generation = _next_generation(
            connection,
            trade_date=source_event.trade_date,
            code=source_event.code,
        )
        candidate_instance_id = _candidate_instance_id(
            source_event.trade_date,
            source_event.code,
            generation,
        )
        _insert_candidate(connection, source_event, candidate_instance_id, generation)
        transition_count += _record_transition(
            connection,
            candidate_instance_id=candidate_instance_id,
            trade_date=source_event.trade_date,
            code=source_event.code,
            from_state=None,
            to_state=CandidateState.DETECTED,
            event_type=CandidateEventType.CANDIDATE_CREATED,
            source_event_id=source_event.source_event_id,
            reason_codes=[
                CandidateReasonCode.CANDIDATE_CREATED.value,
                CandidateReasonCode.SOURCE_DETECTED.value,
            ],
            metadata={"observation_only": True},
        )
        transition_count += _apply_state_transition(
            connection,
            candidate_instance_id,
            CandidateState.HYDRATING,
            event_type=CandidateEventType.STATE_CHANGED,
            source_event_id=source_event.source_event_id,
            reason_codes=[CandidateReasonCode.SOURCE_DETECTED.value],
        )
    else:
        candidate_instance_id = active_candidate["candidate_instance_id"]

    linked_event = source_event.with_candidate(candidate_instance_id)
    _insert_source_event(connection, linked_event, active=True)
    _upsert_latest_source(connection, linked_event, active=True)
    _sync_candidate_source_counts(connection, candidate_instance_id)
    _update_candidate_from_source(connection, candidate_instance_id, linked_event)

    return CandidateSourceApplyResult(
        source_event_count=1,
        candidate_created_count=1 if created else 0,
        candidate_updated_count=0 if created else 1,
        transition_count=transition_count,
    )


def _apply_candidate_source_event_atomically(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    *,
    settings: Settings,
) -> CandidateSourceApplyResult:
    if not connection.in_transaction:
        connection.execute("BEGIN DEFERRED")
    connection.execute(f"SAVEPOINT {_SOURCE_EVENT_SAVEPOINT}")
    try:
        result = create_or_merge_candidate_from_source(
            connection,
            source_event,
            settings=settings,
        )
    except Exception:
        connection.execute(f"ROLLBACK TO SAVEPOINT {_SOURCE_EVENT_SAVEPOINT}")
        connection.execute(f"RELEASE SAVEPOINT {_SOURCE_EVENT_SAVEPOINT}")
        raise
    connection.execute(f"RELEASE SAVEPOINT {_SOURCE_EVENT_SAVEPOINT}")
    return result


def _commit_source_ingest_chunk(connection: sqlite3.Connection) -> None:
    try:
        assert_runtime_execution_fence(connection)
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    connection.commit()


def refresh_candidate_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    settings: Settings | None = None,
    freshness_reference_at: datetime | None = None,
    manage_transaction: bool = True,
) -> CandidateRefreshResult:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    row = _candidate_row(connection, normalized_id)
    if row is None:
        raise ValueError(f"candidate not found: {normalized_id}")
    if row["state"] == CandidateState.CLOSED.value:
        return CandidateRefreshResult()

    try:
        active_sources = _list_latest_source_rows(connection, normalized_id, active_only=True)
        all_sources = _list_latest_source_rows(connection, normalized_id, active_only=False)
        readiness = get_market_data_readiness(
            connection,
            row["code"],
            settings=resolved_settings,
            now=freshness_reference_at,
        )
        latest_tick = get_latest_tick(connection, row["code"])
        common_market_context = get_market_context_for_code(
            connection,
            row["code"],
            settings=resolved_settings,
        )
        market_regime = common_market_context["market_regime"]
        theme_context = _build_theme_context(connection, row, active_sources)
        source_context = _build_source_context(
            connection,
            row,
            active_sources=active_sources,
            all_sources=all_sources,
        )
        market_context = {
            "snapshot_id": common_market_context.get("snapshot_id"),
            "trade_date": common_market_context.get("trade_date"),
            "market": common_market_context.get("market"),
            "source_watermark_hash": common_market_context.get(
                "source_watermark_hash"
            ),
            "source_watermark": common_market_context.get("source_watermark", {}),
            "parser_confidence_status": common_market_context.get(
                "parser_confidence_status"
            ),
            "data_quality_status": common_market_context.get("data_quality_status"),
            "trading_data_usable": common_market_context.get("trading_data_usable"),
            "trading_eligible": common_market_context.get("trading_eligible"),
            "data_age_sec": common_market_context.get("data_age_sec"),
            "generated_by": common_market_context.get("generated_by"),
            "snapshot_at": common_market_context.get("snapshot_at"),
            "latest_tick": latest_tick,
            "readiness": readiness,
            "market_regime": market_regime,
        }
        context = _fsm_context(
            row,
            active_sources=active_sources,
            readiness=readiness,
            theme_context=theme_context,
            settings=resolved_settings,
        )
        current = _candidate_row_to_dict(row)
        decision = determine_next_state(current, context)
        reason_codes = merge_reason_codes(
            _read_json_array(row["reason_codes_json"]),
            _readiness_reason_codes(readiness),
            _theme_reason_codes(theme_context),
            context.get("reason_codes", ()),
            decision.reason_codes,
        )
        refreshed_at = datetime_to_wire(utc_now())
        _upsert_candidate_context(
            connection,
            candidate=row,
            market_context_snapshot_id=common_market_context.get("snapshot_id"),
            theme_context=theme_context,
            market_context=market_context,
            source_context=source_context,
            readiness=readiness,
            refreshed_at=refreshed_at,
        )
        _update_candidate_context_columns(
            connection,
            row,
            active_source_count=len(active_sources),
            source_count=len(all_sources),
            readiness=readiness,
            theme_context=theme_context,
            reason_codes=reason_codes,
        )
        transition_count = 0
        if decision.next_state.value != row["state"]:
            transition_count = _apply_state_transition(
                connection,
                normalized_id,
                decision.next_state,
                event_type=(
                    CandidateEventType.CANDIDATE_CLOSED
                    if decision.close_candidate
                    else CandidateEventType.CANDIDATE_STALE
                    if decision.stale_candidate
                    else CandidateEventType.STATE_CHANGED
                ),
                source_event_id=None,
                reason_codes=reason_codes,
                metadata={"context_refreshed_at": refreshed_at},
            )
        if manage_transaction:
            connection.commit()
        return CandidateRefreshResult(
            context_refreshed_count=1,
            transition_count=transition_count,
            stale_count=1 if decision.next_state is CandidateState.STALE else 0,
            closed_count=1 if decision.next_state is CandidateState.CLOSED else 0,
        )
    except Exception as exc:
        _record_projection_error(
            connection,
            candidate_instance_id=normalized_id,
            source_event_id=None,
            code=row["code"],
            error_message=str(exc),
            payload={"candidate_instance_id": normalized_id},
        )
        if manage_transaction:
            connection.commit()
        return CandidateRefreshResult(error_count=1)


def rebuild_candidates_from_observations(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
    freshness_reference_at: datetime | None = None,
) -> CandidateRebuildResult:
    resolved_settings = settings or load_settings()
    if not resolved_settings.candidate_fsm_enabled:
        return CandidateRebuildResult()
    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    before_transition_count = _count_rows(connection, "candidate_state_transitions")
    condition_result = ingest_condition_sources(
        connection,
        target_trade_date,
        settings=resolved_settings,
    )
    theme_result = ingest_theme_sources(connection, target_trade_date, settings=resolved_settings)

    refresh_total = _MutableRefreshResult()
    for candidate in _list_active_candidate_rows(connection, trade_date=target_trade_date):
        for _ in range(2):
            refresh_result = refresh_candidate_context(
                connection,
                candidate["candidate_instance_id"],
                settings=resolved_settings,
                freshness_reference_at=freshness_reference_at,
            )
            refresh_total.add(refresh_result)
            if (
                refresh_result.transition_count == 0
                or refresh_result.stale_count
                or refresh_result.closed_count
                or refresh_result.error_count
            ):
                break
    after_transition_count = _count_rows(connection, "candidate_state_transitions")
    transition_count = after_transition_count - before_transition_count
    error_count = (
        condition_result.error_count + theme_result.error_count + refresh_total.error_count
    )
    return CandidateRebuildResult(
        source_event_count=condition_result.source_event_count + theme_result.source_event_count,
        candidate_created_count=(
            condition_result.candidate_created_count + theme_result.candidate_created_count
        ),
        candidate_updated_count=(
            condition_result.candidate_updated_count + theme_result.candidate_updated_count
        ),
        transition_count=transition_count,
        context_refreshed_count=refresh_total.context_refreshed_count,
        stale_count=refresh_total.stale_count,
        closed_count=refresh_total.closed_count,
        error_count=error_count,
    )


def get_candidate(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    include_context: bool = False,
    include_sources: bool = False,
    include_transitions: bool = False,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    row = _candidate_row(connection, normalized_id)
    if row is None:
        return None
    candidate = _candidate_row_to_dict(row)
    if include_context:
        candidate["context"] = _get_candidate_context(
            connection,
            candidate["candidate_instance_id"],
        )
    if include_sources:
        candidate["sources"] = list_candidate_sources(
            connection,
            candidate["candidate_instance_id"],
        )
    if include_transitions:
        candidate["transitions"] = list_candidate_transitions(
            connection,
            candidate["candidate_instance_id"],
        )
    return candidate


def list_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    state: CandidateState | str | None = None,
    code: str | None = None,
    theme_id: str | None = None,
    active_only: bool = True,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if state is not None:
        clauses.append("state = ?")
        params.append(CandidateState(str(state)).value)
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    if theme_id is not None:
        clauses.append("theme_id = ?")
        params.append(require_non_empty_str(theme_id, "theme_id"))
    if active_only:
        clauses.append("state != ?")
        params.append(CandidateState.CLOSED.value)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM candidates
        {where_sql}
        ORDER BY last_seen_at DESC, candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_candidate_row_to_dict(row) for row in rows]


def list_candidate_sources(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    rows = connection.execute(
        """
        SELECT *
        FROM candidate_source_events
        WHERE candidate_instance_id = ?
        ORDER BY observed_at ASC, source_event_id ASC
        """,
        (normalized_id,),
    ).fetchall()
    return [_source_event_row_to_dict(row) for row in rows]


def list_candidate_transitions(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    rows = connection.execute(
        """
        SELECT *
        FROM candidate_state_transitions
        WHERE candidate_instance_id = ?
        ORDER BY transitioned_at ASC, transition_id ASC
        """,
        (normalized_id,),
    ).fetchall()
    return [_transition_row_to_dict(row) for row in rows]


def get_candidate_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = connection.execute(
        """
        SELECT state, COUNT(*) AS count
        FROM candidates
        GROUP BY state
        """
    ).fetchall()
    state_counts = {state.value: 0 for state in CandidateState}
    for row in rows:
        state_counts[row["state"]] = int(row["count"])
    return {
        "enabled": resolved_settings.candidate_fsm_enabled,
        "trade_date": _resolve_trade_date(None, resolved_settings),
        "candidate_count": _count_rows(connection, "candidates"),
        "active_candidate_count": _count_rows(
            connection,
            "candidates",
            where="state != 'CLOSED'",
        ),
        "state_counts": state_counts,
        "projection_error_count": _count_rows(connection, "candidate_projection_errors"),
        "source_stale_sec": resolved_settings.candidate_source_stale_sec,
        "episode_ttl_sec": resolved_settings.candidate_episode_ttl_sec,
        "tick_stale_sec": resolved_settings.candidate_tick_stale_sec,
        "context_require_1m_bar": resolved_settings.candidate_context_require_1m_bar,
        "context_require_vwap": resolved_settings.candidate_context_require_vwap,
    }


def list_candidate_projection_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM candidate_projection_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_projection_error_row_to_dict(row) for row in rows]


def _apply_exit_source_event(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    *,
    settings: Settings,
) -> CandidateSourceApplyResult:
    active_candidate = _find_active_candidate(
        connection,
        trade_date=source_event.trade_date,
        code=source_event.code,
    )
    candidate_instance_id = (
        active_candidate["candidate_instance_id"] if active_candidate is not None else None
    )
    linked_event = source_event.with_candidate(candidate_instance_id)
    _insert_source_event(connection, linked_event, active=False)
    updated_count = 0
    closed_count = 0
    transition_count = 0
    if candidate_instance_id is not None:
        updated_count = 1
        connection.execute(
            """
            UPDATE candidate_sources_latest
            SET active = 0,
                last_seen_at = ?,
                last_event_id = ?,
                payload_json = ?
            WHERE candidate_instance_id = ?
                AND source_type = ?
                AND source_id = ?
            """,
            (
                datetime_to_wire(source_event.observed_at),
                source_event.source_event_id,
                canonical_json(source_event.payload),
                candidate_instance_id,
                CandidateSourceType.CONDITION_ENTER.value,
                source_event.source_id,
            ),
        )
        _sync_candidate_source_counts(connection, candidate_instance_id)
        refresh_result = refresh_candidate_context(
            connection,
            candidate_instance_id,
            settings=settings,
            manage_transaction=False,
        )
        if refresh_result.error_count:
            raise RuntimeError("CONDITION_EXIT_CONTEXT_REFRESH_FAILED")
        closed_count = refresh_result.closed_count
        transition_count = refresh_result.transition_count
    return CandidateSourceApplyResult(
        source_event_count=1,
        candidate_updated_count=updated_count,
        transition_count=transition_count,
        closed_count=closed_count,
    )


def _condition_row_to_source_event(
    row: sqlite3.Row,
    trade_date: str,
    settings: Settings,
) -> CandidateSourceEvent:
    action = str(row["action"]).upper()
    normalized_code = validate_stock_code(row["code"])
    if action == settings.candidate_condition_action_enter:
        source_type = CandidateSourceType.CONDITION_ENTER
        reasons = [CandidateReasonCode.CONDITION_ENTERED.value]
    elif action == settings.candidate_condition_action_exit:
        source_type = CandidateSourceType.CONDITION_EXIT
        reasons = [
            CandidateReasonCode.CONDITION_EXITED.value,
            CandidateReasonCode.SOURCE_EXITED.value,
        ]
    else:
        raise ValueError(f"unsupported condition action for candidate source: {action}")
    payload = _row_to_dict(row)
    payload["metadata"] = json.loads(payload.pop("metadata_json"))
    return CandidateSourceEvent(
        source_event_id=_source_event_id("condition", trade_date, row["event_id"]),
        trade_date=trade_date,
        code=normalized_code,
        name=row["name"],
        source_type=source_type,
        source_id=row["condition_id"],
        action=action,
        condition_id=row["condition_id"],
        condition_name=row["condition_name"],
        event_ts=row["event_ts"],
        observed_at=row["received_at"],
        payload=payload,
        reason_codes=reasons,
    )


def _fusion_row_to_source_event(
    fusion: Mapping[str, Any],
    trade_date: str,
) -> CandidateSourceEvent:
    roles = [str(role).upper() for role in fusion.get("active_roles", ())]
    metadata = fusion.get("metadata") if isinstance(fusion.get("metadata"), Mapping) else {}
    not_buy_signal = bool(metadata.get("not_buy_signal", False))
    source_type = _fusion_source_type(roles, bool(fusion.get("risk_blocked")))
    reasons = _fusion_reason_codes(fusion)
    condition_names = [str(name) for name in fusion.get("condition_names", ())]
    condition_name = " + ".join(condition_names) or "Condition Fusion"
    source_id = f"condition_fusion:{fusion['code']}"
    latest_hit_at = fusion.get("latest_hit_at") or fusion.get("updated_at")
    return CandidateSourceEvent(
        source_event_id=_source_event_id(
            "condition_fusion",
            trade_date,
            fusion["code"],
            fusion.get("latest_event_id") or latest_hit_at,
            ",".join(roles),
        ),
        trade_date=trade_date,
        code=fusion["code"],
        name=fusion.get("name") or fusion["code"],
        source_type=source_type,
        source_id=source_id,
        action="OBSERVE",
        condition_id=source_id,
        condition_name=condition_name,
        event_ts=latest_hit_at,
        observed_at=fusion.get("updated_at") or latest_hit_at,
        payload={
            **dict(fusion),
            "sensor_evidence": True,
            "not_buy_signal": not_buy_signal,
        },
        reason_codes=reasons,
    )


def _theme_row_to_source_event(row: sqlite3.Row, trade_date: str) -> CandidateSourceEvent:
    member_role = ThemeMemberRole(str(row["member_role"]).upper())
    if member_role is ThemeMemberRole.LEADER_CANDIDATE:
        source_type = CandidateSourceType.THEME_LEADER
        role_reason = CandidateReasonCode.THEME_LEADING_MEMBER.value
    elif member_role is ThemeMemberRole.CO_LEADER_CANDIDATE:
        source_type = CandidateSourceType.THEME_CO_LEADER
        role_reason = CandidateReasonCode.THEME_CO_LEADER_MEMBER.value
    elif member_role is ThemeMemberRole.FOLLOWER_CANDIDATE:
        source_type = CandidateSourceType.THEME_FOLLOWER
        role_reason = CandidateReasonCode.THEME_FOLLOWER_MEMBER.value
    else:
        source_type = CandidateSourceType.THEME_SPREADING_MEMBER
        role_reason = CandidateReasonCode.THEME_FOLLOWER_MEMBER.value
    state_reason = (
        CandidateReasonCode.THEME_STATE_LEADING.value
        if str(row["state"]).upper() == "LEADING"
        else CandidateReasonCode.THEME_STATE_SPREADING.value
    )
    payload = _row_to_dict(row)
    payload["metadata"] = json.loads(payload.pop("metadata_json"))
    return CandidateSourceEvent(
        source_event_id=_source_event_id(
            "theme",
            trade_date,
            row["snapshot_id"],
            row["theme_id"],
            row["code"],
            row["member_role"],
        ),
        trade_date=trade_date,
        code=row["code"],
        name=row["name"],
        source_type=source_type,
        source_id=row["theme_id"],
        action="OBSERVE",
        theme_id=row["theme_id"],
        theme_name=row["theme_name"],
        event_ts=row["calculated_at"],
        observed_at=row["calculated_at"],
        payload=payload,
        reason_codes=[role_reason, state_reason],
    )


def _insert_candidate(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    candidate_instance_id: str,
    generation: int,
) -> None:
    now = datetime_to_wire(utc_now())
    detected_at = datetime_to_wire(source_event.observed_at)
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            previous_state,
            detected_at,
            last_seen_at,
            state_updated_at,
            primary_source_type,
            primary_source_id,
            theme_id,
            theme_name,
            theme_state,
            theme_role,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_instance_id,
            source_event.trade_date,
            source_event.code,
            source_event.name,
            generation,
            CandidateState.DETECTED.value,
            None,
            detected_at,
            detected_at,
            now,
            source_event.source_type.value,
            source_event.source_id,
            source_event.theme_id,
            source_event.theme_name,
            _source_theme_state(source_event),
            _source_member_role(source_event),
            _json_dumps(
                merge_reason_codes(
                    source_event.reason_codes,
                    (
                        CandidateReasonCode.CANDIDATE_CREATED.value,
                        CandidateReasonCode.SOURCE_DETECTED.value,
                    ),
                )
            ),
            canonical_json({"observation_only": True}),
        ),
    )


def _insert_source_event(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    *,
    active: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id,
            candidate_instance_id,
            trade_date,
            code,
            name,
            source_type,
            source_id,
            action,
            theme_id,
            theme_name,
            condition_id,
            condition_name,
            event_ts,
            observed_at,
            active,
            reason_codes_json,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_event.source_event_id,
            source_event.candidate_instance_id,
            source_event.trade_date,
            source_event.code,
            source_event.name,
            source_event.source_type.value,
            source_event.source_id,
            source_event.action,
            source_event.theme_id,
            source_event.theme_name,
            source_event.condition_id,
            source_event.condition_name,
            datetime_to_wire(source_event.event_ts),
            datetime_to_wire(source_event.observed_at),
            1 if active else 0,
            _json_dumps(source_event.reason_codes),
            canonical_json(source_event.payload),
        ),
    )


def _upsert_latest_source(
    connection: sqlite3.Connection,
    source_event: CandidateSourceEvent,
    *,
    active: bool,
) -> None:
    observed_at = datetime_to_wire(source_event.observed_at)
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date,
            code,
            source_type,
            source_id,
            candidate_instance_id,
            name,
            active,
            first_seen_at,
            last_seen_at,
            last_event_id,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date, code, source_type, source_id) DO UPDATE SET
            candidate_instance_id = excluded.candidate_instance_id,
            name = excluded.name,
            active = excluded.active,
            last_seen_at = excluded.last_seen_at,
            last_event_id = excluded.last_event_id,
            payload_json = excluded.payload_json
        """,
        (
            source_event.trade_date,
            source_event.code,
            source_event.source_type.value,
            source_event.source_id,
            source_event.candidate_instance_id,
            source_event.name,
            1 if active else 0,
            observed_at,
            observed_at,
            source_event.source_event_id,
            canonical_json(source_event.payload),
        ),
    )


def _sync_candidate_source_counts(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> None:
    source_count = _count_rows(
        connection,
        "candidate_source_events",
        where="candidate_instance_id = ?",
        params=(candidate_instance_id,),
    )
    active_source_count = _count_rows(
        connection,
        "candidate_sources_latest",
        where="candidate_instance_id = ? AND active = 1",
        params=(candidate_instance_id,),
    )
    connection.execute(
        """
        UPDATE candidates
        SET source_count = ?,
            active_source_count = ?
        WHERE candidate_instance_id = ?
        """,
        (source_count, active_source_count, candidate_instance_id),
    )


def _update_candidate_from_source(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    source_event: CandidateSourceEvent,
) -> None:
    connection.execute(
        """
        UPDATE candidates
        SET name = ?,
            last_seen_at = MAX(last_seen_at, ?),
            theme_id = COALESCE(?, theme_id),
            theme_name = COALESCE(?, theme_name),
            theme_state = COALESCE(?, theme_state),
            theme_role = COALESCE(?, theme_role),
            reason_codes_json = ?
        WHERE candidate_instance_id = ?
        """,
        (
            source_event.name,
            datetime_to_wire(source_event.observed_at),
            source_event.theme_id,
            source_event.theme_name,
            _source_theme_state(source_event),
            _source_member_role(source_event),
            _json_dumps(source_event.reason_codes),
            candidate_instance_id,
        ),
    )


def _update_candidate_context_columns(
    connection: sqlite3.Connection,
    candidate: sqlite3.Row,
    *,
    active_source_count: int,
    source_count: int,
    readiness: Mapping[str, Any],
    theme_context: Mapping[str, Any],
    reason_codes: list[str],
) -> None:
    connection.execute(
        """
        UPDATE candidates
        SET source_count = ?,
            active_source_count = ?,
            theme_id = COALESCE(?, theme_id),
            theme_name = COALESCE(?, theme_name),
            theme_state = COALESCE(?, theme_state),
            theme_role = COALESCE(?, theme_role),
            market_readiness_status = ?,
            tick_age_sec = ?,
            vwap_ready = ?,
            bar_1m_ready = ?,
            bar_3m_ready = ?,
            bar_5m_ready = ?,
            reason_codes_json = ?
        WHERE candidate_instance_id = ?
        """,
        (
            source_count,
            active_source_count,
            theme_context.get("theme_id"),
            theme_context.get("theme_name"),
            theme_context.get("theme_state"),
            theme_context.get("theme_role"),
            readiness.get("quality_status"),
            readiness.get("tick_age_sec"),
            1 if readiness.get("vwap_ready") else 0,
            1 if readiness.get("has_1m_bar") else 0,
            1 if readiness.get("has_3m_bar") else 0,
            1 if readiness.get("has_5m_bar") else 0,
            _json_dumps(reason_codes),
            candidate["candidate_instance_id"],
        ),
    )


def _apply_state_transition(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    next_state: CandidateState,
    *,
    event_type: CandidateEventType,
    source_event_id: str | None,
    reason_codes: list[str],
    metadata: Mapping[str, Any] | None = None,
) -> int:
    row = _candidate_row(connection, candidate_instance_id)
    if row is None:
        raise ValueError(f"candidate not found: {candidate_instance_id}")
    current_state = CandidateState(row["state"])
    if current_state is next_state:
        return 0
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        UPDATE candidates
        SET previous_state = state,
            state = ?,
            state_updated_at = ?,
            closed_at = CASE WHEN ? = 'CLOSED' THEN ? ELSE closed_at END,
            reason_codes_json = ?
        WHERE candidate_instance_id = ?
        """,
        (
            next_state.value,
            now,
            next_state.value,
            now,
            _json_dumps(reason_codes),
            candidate_instance_id,
        ),
    )
    return _record_transition(
        connection,
        candidate_instance_id=candidate_instance_id,
        trade_date=row["trade_date"],
        code=row["code"],
        from_state=current_state,
        to_state=next_state,
        event_type=event_type,
        source_event_id=source_event_id,
        reason_codes=reason_codes,
        metadata=metadata or {},
    )


def _record_transition(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str,
    trade_date: str,
    code: str,
    from_state: CandidateState | None,
    to_state: CandidateState,
    event_type: CandidateEventType,
    source_event_id: str | None,
    reason_codes: list[str],
    metadata: Mapping[str, Any],
) -> int:
    transitioned_at = datetime_to_wire(utc_now())
    transition_id = _transition_id(
        candidate_instance_id,
        from_state.value if from_state else "NONE",
        to_state.value,
        event_type.value,
        source_event_id or transitioned_at,
    )
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO candidate_state_transitions (
            transition_id,
            candidate_instance_id,
            trade_date,
            code,
            from_state,
            to_state,
            event_type,
            source_event_id,
            reason_codes_json,
            transitioned_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            transition_id,
            candidate_instance_id,
            trade_date,
            code,
            from_state.value if from_state else None,
            to_state.value,
            event_type.value,
            source_event_id,
            _json_dumps(reason_codes),
            transitioned_at,
            canonical_json(metadata),
        ),
    )
    return int(cursor.rowcount > 0)


def _upsert_candidate_context(
    connection: sqlite3.Connection,
    *,
    candidate: sqlite3.Row,
    market_context_snapshot_id: object,
    theme_context: Mapping[str, Any],
    market_context: Mapping[str, Any],
    source_context: Mapping[str, Any],
    readiness: Mapping[str, Any],
    refreshed_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_context_latest (
            candidate_instance_id,
            market_context_snapshot_id,
            trade_date,
            code,
            name,
            theme_context_json,
            market_context_json,
            source_context_json,
            readiness_json,
            refreshed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_instance_id) DO UPDATE SET
            market_context_snapshot_id = excluded.market_context_snapshot_id,
            trade_date = excluded.trade_date,
            code = excluded.code,
            name = excluded.name,
            theme_context_json = excluded.theme_context_json,
            market_context_json = excluded.market_context_json,
            source_context_json = excluded.source_context_json,
            readiness_json = excluded.readiness_json,
            refreshed_at = excluded.refreshed_at
        """,
        (
            candidate["candidate_instance_id"],
            (
                None
                if market_context_snapshot_id in (None, "")
                else str(market_context_snapshot_id)
            ),
            candidate["trade_date"],
            candidate["code"],
            candidate["name"],
            canonical_json(theme_context),
            canonical_json(market_context),
            canonical_json(source_context),
            canonical_json(readiness),
            refreshed_at,
        ),
    )


def _build_theme_context(
    connection: sqlite3.Connection,
    candidate: sqlite3.Row,
    active_sources: list[sqlite3.Row],
) -> dict[str, Any]:
    theme_source_rows = [
        row for row in active_sources if row["source_type"] in _theme_source_values()
    ]
    theme_ids = [row["source_id"] for row in theme_source_rows]
    if not theme_ids and candidate["theme_id"] is not None:
        theme_ids = [candidate["theme_id"]]
    theme_filter_sql = ""
    params: list[Any] = []
    if theme_ids:
        theme_filter_sql = f"l.theme_id IN ({','.join('?' for _ in theme_ids)}) AND"
        params.extend(theme_ids)
    rows = connection.execute(
        f"""
        SELECT
            l.snapshot_id,
            l.theme_id,
            l.theme_name,
            l.calculated_at,
            l.state AS theme_state,
            l.quality_status,
            l.leading_code,
            l.leading_name,
            l.fresh_coverage_ratio,
            l.observable_member_count,
            l.observable_fresh_member_count,
            l.observable_fresh_coverage_ratio,
            l.rising_ratio,
            m.code,
            m.name,
            m.member_role AS theme_role,
            m.readiness_status,
            m.tick_age_sec,
            m.event_ts,
            m.metadata_json
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshot_members AS m ON m.snapshot_id = l.snapshot_id
        WHERE {theme_filter_sql} m.code = ?
        ORDER BY
            CASE WHEN l.state IN ('LEADING', 'SPREADING') THEN 0 ELSE 1 END,
            CASE WHEN m.member_role IN (
                'LEADER_CANDIDATE',
                'CO_LEADER_CANDIDATE',
                'FOLLOWER_CANDIDATE'
            ) THEN 0 ELSE 1 END,
            CASE WHEN m.readiness_status = 'FRESH' THEN 0 ELSE 1 END,
            CASE WHEN l.quality_status = 'FRESH' THEN 0 ELSE 1 END,
            l.calculated_at DESC,
            l.theme_id ASC
        """,
        (*params, candidate["code"]),
    ).fetchall()
    sources = []
    for row in rows:
        item = _row_to_dict(row)
        item["metadata"] = json.loads(item.pop("metadata_json"))
        sources.append(item)
    source_contexts = _theme_source_contexts(theme_source_rows)
    legacy_primary = sources[0] if sources else {}
    source_primary = source_contexts[0] if source_contexts else {}
    reason_codes: list[str] = []
    use_source_context = bool(source_primary) and (
        not legacy_primary
        or str(legacy_primary.get("theme_state") or "").upper() == "DATA_WAIT"
        or not legacy_primary.get("theme_role")
    )
    if use_source_context:
        primary = source_primary
        reason_codes.append("RT_TLS_SOURCE_CONTEXT_FALLBACK_USED")
    else:
        primary = legacy_primary
        if source_primary and legacy_primary:
            source_state = str(source_primary.get("theme_state") or "").upper()
            legacy_state = str(legacy_primary.get("theme_state") or "").upper()
            source_role = str(source_primary.get("theme_role") or "").upper()
            legacy_role = str(legacy_primary.get("theme_role") or "").upper()
            if source_state and legacy_state and source_state != legacy_state:
                reason_codes.append("RT_TLS_LEGACY_THEME_STATE_CONFLICT")
            if source_role and legacy_role and source_role != legacy_role:
                reason_codes.append("RT_TLS_LEGACY_THEME_ROLE_CONFLICT")
    return {
        "present": bool(primary),
        "theme_id": primary.get("theme_id"),
        "theme_name": primary.get("theme_name"),
        "theme_state": primary.get("theme_state"),
        "theme_role": primary.get("theme_role"),
        "leading_code": primary.get("leading_code"),
        "leading_name": primary.get("leading_name"),
        "fresh_coverage_ratio": primary.get("fresh_coverage_ratio"),
        "observable_member_count": primary.get("observable_member_count"),
        "observable_fresh_member_count": primary.get("observable_fresh_member_count"),
        "observable_fresh_coverage_ratio": primary.get("observable_fresh_coverage_ratio"),
        "rising_ratio": primary.get("rising_ratio"),
        "sources": sources,
        "theme_leadership_source_contexts": source_contexts,
        "context_source": (
            "theme_leadership_source_context" if use_source_context else "legacy_theme_snapshot"
        ),
        "reason_codes": reason_codes,
    }


def _theme_source_contexts(rows: Sequence[sqlite3.Row]) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for row in rows:
        payload = _source_payload(row)
        theme_state = _source_theme_state(row)
        theme_role = _source_member_role(row)
        if theme_state is None and theme_role is None:
            continue
        source_detail = payload.get("source_detail")
        if not isinstance(source_detail, Mapping):
            source_detail = {}
        contexts.append(
            {
                "source": "theme_leadership_source_context",
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "last_seen_at": row["last_seen_at"],
                "theme_id": payload.get("theme_id") or row["source_id"],
                "theme_name": payload.get("theme_name"),
                "theme_state": theme_state,
                "theme_role": theme_role,
                "raw_theme_state": payload.get("theme_state"),
                "raw_stock_role": payload.get("stock_role"),
                "theme_rank": payload.get("theme_rank"),
                "priority_score": payload.get("priority_score"),
                "member_score": source_detail.get("member_score"),
                "theme_score": source_detail.get("theme_score"),
                "fresh_coverage_ratio": source_detail.get("fresh_coverage_ratio"),
                "rising_ratio": source_detail.get("rising_ratio"),
                "full_fresh_coverage_ratio": source_detail.get("full_fresh_coverage_ratio"),
                "observe_only": bool(payload.get("observe_only", True)),
                "not_order_signal": bool(payload.get("not_order_signal", True)),
                "reason_codes": merge_reason_codes(
                    [
                        *[str(reason) for reason in payload.get("reason_codes", [])],
                        "RT_TLS_SOURCE_CONTEXT_PRESENT",
                    ]
                ),
            }
        )
    contexts.sort(
        key=lambda item: (
            -float(item.get("priority_score") or 0.0),
            str(item.get("last_seen_at") or ""),
            str(item.get("source_type") or ""),
        )
    )
    return contexts


def _build_source_context(
    connection: sqlite3.Connection,
    candidate: sqlite3.Row,
    *,
    active_sources: list[sqlite3.Row],
    all_sources: list[sqlite3.Row],
) -> dict[str, Any]:
    condition_fusion = _condition_fusion_context_for_candidate(connection, candidate)
    return {
        "active_source_count": len(active_sources),
        "source_count": len(all_sources),
        "active_sources": [_latest_source_row_to_dict(row) for row in active_sources],
        "sources": [_latest_source_row_to_dict(row) for row in all_sources],
        "condition_fusion": condition_fusion,
        "condition_fusion_priority_score": condition_fusion[
            "condition_fusion_priority_score"
        ],
        "active_condition_roles": condition_fusion["active_condition_roles"],
        "condition_risk_blocked": condition_fusion["condition_risk_blocked"],
        "condition_fusion_reason_codes": condition_fusion[
            "condition_fusion_reason_codes"
        ],
        "condition_names": condition_fusion["condition_names"],
        "condition_latest_hit_at": condition_fusion["condition_latest_hit_at"],
    }


def _condition_fusion_context_for_candidate(
    connection: sqlite3.Connection,
    candidate: sqlite3.Row,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM candidate_condition_fusion
        WHERE trade_date = ? AND code = ?
        """,
        (candidate["trade_date"], candidate["code"]),
    ).fetchone()
    if row is None:
        return {
            "present": False,
            "source": "candidate_condition_fusion",
            "condition_fusion_priority_score": 0.0,
            "active_condition_roles": [],
            "condition_risk_blocked": False,
            "condition_fusion_reason_codes": [],
            "condition_names": [],
            "condition_latest_hit_at": None,
            "condition_fusion_updated_at": None,
            "condition_fusion_not_buy_signal": True,
        }
    active_roles = _read_json_array(row["active_roles_json"])
    reason_codes = _read_json_array(row["reason_codes_json"])
    condition_names = _read_json_array(row["condition_names_json"])
    metadata = _read_json_object(row["metadata_json"])
    return {
        "present": True,
        "source": "candidate_condition_fusion",
        "condition_fusion_priority_score": float(row["priority_score"] or 0.0),
        "active_condition_roles": active_roles,
        "condition_risk_blocked": bool(row["risk_blocked"]),
        "condition_fusion_reason_codes": reason_codes,
        "condition_names": condition_names,
        "condition_latest_hit_at": row["latest_hit_at"],
        "condition_fusion_updated_at": row["updated_at"],
        "condition_fusion_not_buy_signal": bool(metadata.get("not_buy_signal", False)),
    }


def _fsm_context(
    candidate: sqlite3.Row,
    *,
    active_sources: list[sqlite3.Row],
    readiness: Mapping[str, Any],
    theme_context: Mapping[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    source_types = {row["source_type"] for row in active_sources}
    theme_source = bool(source_types & _theme_source_values())
    condition_source = bool(source_types & _condition_source_values())
    fusion_reasons = _active_condition_fusion_reason_codes(active_sources)
    observation_blocked = _active_condition_fusion_blocked(active_sources)
    latest_source_seen = _latest_source_seen_at(active_sources)
    source_stale = (
        latest_source_seen is not None
        and _age_seconds(latest_source_seen) > settings.candidate_source_stale_sec
    )
    tick_age_sec = readiness.get("tick_age_sec")
    tick_stale = (
        tick_age_sec is not None and float(tick_age_sec) > settings.candidate_tick_stale_sec
    )
    ttl_expired = _age_seconds(candidate["detected_at"]) > settings.candidate_episode_ttl_sec
    reason_codes = merge_reason_codes(
        _readiness_reason_codes(readiness),
        fusion_reasons,
        [CandidateReasonCode.SOURCE_STALE.value] if source_stale else [],
    )
    return {
        "active_source_count": len(active_sources),
        "has_latest_tick": readiness.get("has_latest_tick"),
        "market_readiness_status": readiness.get("quality_status"),
        "tick_age_sec": tick_age_sec,
        "tick_stale": tick_stale,
        "source_stale": source_stale,
        "candidate_stale_requires_tick_stale": settings.candidate_stale_requires_tick_stale,
        "ttl_expired": ttl_expired,
        "vwap_ready": readiness.get("vwap_ready"),
        "bar_1m_ready": readiness.get("has_1m_bar"),
        "bar_3m_ready": readiness.get("has_3m_bar"),
        "bar_5m_ready": readiness.get("has_5m_bar"),
        "require_1m_bar": settings.candidate_context_require_1m_bar,
        "require_vwap": settings.candidate_context_require_vwap,
        "theme_source": theme_source,
        "theme_context_present": bool(theme_context.get("present")),
        "theme_state": theme_context.get("theme_state"),
        "condition_source": condition_source,
        "condition_signal_present": condition_source,
        "observation_blocked": observation_blocked,
        "reason_codes": reason_codes,
    }


def _readiness_reason_codes(readiness: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    status = str(readiness.get("quality_status") or "").upper()
    if status == MarketDataQualityStatus.FRESH.value:
        reasons.append(CandidateReasonCode.MARKET_TICK_FRESH.value)
        reasons.append(CandidateReasonCode.MARKET_READINESS_READY.value)
    if status == MarketDataQualityStatus.MISSING.value:
        reasons.append(CandidateReasonCode.MARKET_READINESS_MISSING.value)
    for reason in readiness.get("reason_codes", ()):
        value = str(reason).upper()
        if value.startswith("BAR_MISSING"):
            reasons.append(CandidateReasonCode.BAR_MISSING.value)
        elif value == "VWAP_MISSING":
            reasons.append(CandidateReasonCode.VWAP_MISSING.value)
        elif value == "TICK_STALE":
            reasons.append(CandidateReasonCode.TICK_STALE.value)
        elif value == "TICK_MISSING":
            reasons.append(CandidateReasonCode.MARKET_READINESS_MISSING.value)
    return merge_reason_codes(reasons)


def _theme_reason_codes(theme_context: Mapping[str, Any]) -> list[str]:
    if not theme_context.get("sources") and not theme_context.get(
        "theme_leadership_source_contexts"
    ):
        return []
    reasons: list[str] = []
    state = str(theme_context.get("theme_state") or "").upper()
    role = str(theme_context.get("theme_role") or "").upper()
    if state == "LEADING":
        reasons.append(CandidateReasonCode.THEME_STATE_LEADING.value)
    elif state == "SPREADING":
        reasons.append(CandidateReasonCode.THEME_STATE_SPREADING.value)
    if role == ThemeMemberRole.LEADER_CANDIDATE.value:
        reasons.append(CandidateReasonCode.THEME_LEADING_MEMBER.value)
    elif role == ThemeMemberRole.CO_LEADER_CANDIDATE.value:
        reasons.append(CandidateReasonCode.THEME_CO_LEADER_MEMBER.value)
    elif role == ThemeMemberRole.FOLLOWER_CANDIDATE.value:
        reasons.append(CandidateReasonCode.THEME_FOLLOWER_MEMBER.value)
    return reasons


def _find_active_candidate(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    code: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE trade_date = ?
            AND code = ?
            AND state != ?
        ORDER BY generation DESC
        LIMIT 1
        """,
        (trade_date, validate_stock_code(code), CandidateState.CLOSED.value),
    ).fetchone()


def _list_active_candidate_rows(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE trade_date = ?
            AND state != ?
        ORDER BY last_seen_at ASC, candidate_instance_id ASC
        """,
        (trade_date, CandidateState.CLOSED.value),
    ).fetchall()


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _list_latest_source_rows(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    active_only: bool,
) -> list[sqlite3.Row]:
    where = "WHERE candidate_instance_id = ?"
    params: list[Any] = [candidate_instance_id]
    if active_only:
        where += " AND active = 1"
    return connection.execute(
        f"""
        SELECT *
        FROM candidate_sources_latest
        {where}
        ORDER BY last_seen_at DESC, source_type ASC, source_id ASC
        """,
        tuple(params),
    ).fetchall()


def _get_candidate_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "market_context_snapshot_id": row["market_context_snapshot_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "theme_context": json.loads(row["theme_context_json"]),
        "market_context": json.loads(row["market_context_json"]),
        "source_context": json.loads(row["source_context_json"]),
        "readiness": json.loads(row["readiness_json"]),
        "refreshed_at": row["refreshed_at"],
    }


def _next_generation(connection: sqlite3.Connection, *, trade_date: str, code: str) -> int:
    row = connection.execute(
        """
        SELECT MAX(generation) AS max_generation
        FROM candidates
        WHERE trade_date = ? AND code = ?
        """,
        (trade_date, validate_stock_code(code)),
    ).fetchone()
    return int(row["max_generation"] or 0) + 1


def _candidate_instance_id(trade_date: str, code: str, generation: int) -> str:
    return f"CAND-{trade_date}-{validate_stock_code(code)}-{generation}"


def _source_event_exists(connection: sqlite3.Connection, source_event_id: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM candidate_source_events WHERE source_event_id = ?",
        (source_event_id,),
    ).fetchone()
    return row is not None


def _source_event_id(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"CSE-{digest}"


def _transition_id(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"CTR-{digest}"


def _resolve_trade_date(trade_date: str | None, settings: Settings) -> str:
    if trade_date is not None:
        return require_non_empty_str(trade_date, "trade_date")
    return (
        datetime.now(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _trade_date_for_timestamp(value: str, settings: Settings) -> str:
    return (
        parse_timestamp(value, "timestamp")
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _age_seconds(value: str) -> float:
    return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)


def _latest_source_seen_at(active_sources: list[sqlite3.Row]) -> str | None:
    if not active_sources:
        return None
    return max(str(row["last_seen_at"]) for row in active_sources)


def _theme_source_values() -> set[str]:
    return {source_type.value for source_type in THEME_SOURCE_TYPES}


def _source_theme_state(source_event: CandidateSourceEvent | sqlite3.Row) -> str | None:
    payload = (
        source_event.payload
        if isinstance(source_event, CandidateSourceEvent)
        else _source_payload(source_event)
    )
    value = _first_text(payload.get("state"), payload.get("theme_state"))
    if value is None:
        return None
    normalized = value.upper()
    if normalized in {"LEADING", "SPREADING", "FADING", "DATA_WAIT"}:
        return normalized
    return "WATCH"


def _source_member_role(source_event: CandidateSourceEvent | sqlite3.Row) -> str | None:
    payload = (
        source_event.payload
        if isinstance(source_event, CandidateSourceEvent)
        else _source_payload(source_event)
    )
    value = _first_text(payload.get("member_role"), payload.get("stock_role"))
    if value is None:
        return None
    normalized = value.upper()
    if normalized in {
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
        "LAGGARD",
        "STALE",
        "UNKNOWN",
    }:
        return normalized
    if normalized == "LEADER":
        return "LEADER_CANDIDATE"
    if normalized == "CO_LEADER":
        return "CO_LEADER_CANDIDATE"
    if normalized == "FOLLOWER":
        return "FOLLOWER_CANDIDATE"
    if normalized == "STALE":
        return "STALE"
    if normalized == "UNKNOWN":
        return "UNKNOWN"
    return "LAGGARD"


def _first_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _condition_source_values() -> set[str]:
    return {
        CandidateSourceType.CONDITION_ENTER.value,
        CandidateSourceType.CONDITION_DISCOVERY.value,
        CandidateSourceType.CONDITION_LEADER.value,
        CandidateSourceType.CONDITION_PULLBACK.value,
        CandidateSourceType.CONDITION_BREAKOUT.value,
        CandidateSourceType.CONDITION_RISK_BLOCK.value,
    }


def _fusion_source_type(
    roles: Sequence[str],
    risk_blocked: bool,
) -> CandidateSourceType:
    role_set = {str(role).upper() for role in roles}
    if risk_blocked or "RISK_BLOCK" in role_set:
        return CandidateSourceType.CONDITION_RISK_BLOCK
    if "LEADER" in role_set:
        return CandidateSourceType.CONDITION_LEADER
    if "PULLBACK" in role_set:
        return CandidateSourceType.CONDITION_PULLBACK
    if "BREAKOUT" in role_set:
        return CandidateSourceType.CONDITION_BREAKOUT
    return CandidateSourceType.CONDITION_DISCOVERY


def _fusion_reason_codes(fusion: Mapping[str, Any]) -> list[str]:
    role_reasons = {
        "DISCOVERY": CandidateReasonCode.CONDITION_DISCOVERY_OBSERVED.value,
        "LEADER": CandidateReasonCode.CONDITION_LEADER_OBSERVED.value,
        "PULLBACK": CandidateReasonCode.CONDITION_PULLBACK_OBSERVED.value,
        "BREAKOUT": CandidateReasonCode.CONDITION_BREAKOUT_OBSERVED.value,
        "RISK_BLOCK": CandidateReasonCode.CONDITION_RISK_BLOCKED.value,
    }
    metadata = fusion.get("metadata") if isinstance(fusion.get("metadata"), Mapping) else {}
    reasons = [CandidateReasonCode.CONDITION_SENSOR_EVIDENCE.value]
    if bool(metadata.get("not_buy_signal")):
        reasons.append(CandidateReasonCode.MARKET_SENSOR_NOT_BUY_SIGNAL.value)
    for role in fusion.get("active_roles", ()):
        reason = role_reasons.get(str(role).upper())
        if reason:
            reasons.append(reason)
    for reason in fusion.get("reason_codes", ()):
        text = str(reason).upper()
        if text == "DISCOVERY_OBSERVATION_ONLY":
            reasons.append(CandidateReasonCode.DISCOVERY_OBSERVATION_ONLY.value)
        elif text == "DISCOVERY_PROMOTION_PENDING":
            reasons.append(CandidateReasonCode.DISCOVERY_PROMOTION_PENDING.value)
        elif text == "RISK_BLOCKED_BY_CONDITION":
            reasons.append(CandidateReasonCode.CONDITION_RISK_BLOCKED.value)
        elif text == "CONDITION_FUSION_PRIORITY_READY":
            reasons.append(CandidateReasonCode.CONDITION_FUSION_PRIORITY_READY.value)
        else:
            reasons.append(text)
    return merge_reason_codes(reasons)


def _active_condition_fusion_reason_codes(active_sources: list[sqlite3.Row]) -> list[str]:
    reasons: list[str] = []
    for row in active_sources:
        if row["source_type"] not in _condition_source_values():
            continue
        payload = _source_payload(row)
        reasons.extend(str(reason) for reason in payload.get("reason_codes", ()))
    return merge_reason_codes(reasons)


def _active_condition_fusion_blocked(active_sources: list[sqlite3.Row]) -> bool:
    for row in active_sources:
        if row["source_type"] not in _condition_source_values():
            continue
        payload = _source_payload(row)
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
        if bool(payload.get("risk_blocked")):
            return True
        if row["source_type"] == CandidateSourceType.CONDITION_RISK_BLOCK.value:
            return True
        if (
            isinstance(metadata, Mapping)
            and metadata.get("candidate_promotion_allowed") is False
            and metadata.get("discovery_promotion_pending") is not True
        ):
            return True
    return False


def _source_payload(row: sqlite3.Row) -> dict[str, Any]:
    try:
        loaded = json.loads(row["payload_json"])
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _condition_row_metadata(row: sqlite3.Row) -> dict[str, Any]:
    try:
        loaded = json.loads(row["metadata_json"])
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _candidate_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["vwap_ready"] = bool(data["vwap_ready"])
    data["bar_1m_ready"] = bool(data["bar_1m_ready"])
    data["bar_3m_ready"] = bool(data["bar_3m_ready"])
    data["bar_5m_ready"] = bool(data["bar_5m_ready"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _source_event_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["active"] = bool(data["active"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _latest_source_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["active"] = bool(data["active"])
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _transition_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _projection_error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _read_json_array(value: str) -> list[str]:
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _read_json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _record_projection_error(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None,
    source_event_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_projection_errors (
            candidate_instance_id,
            source_event_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            candidate_instance_id,
            source_event_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            canonical_json(payload),
        ),
    )


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    where: str | None = None,
    params: tuple[Any, ...] = (),
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}",
        params,
    ).fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


@dataclass
class _MutableIngestResult:
    source_event_count: int = 0
    candidate_created_count: int = 0
    candidate_updated_count: int = 0
    duplicate_source_count: int = 0
    transition_count: int = 0
    error_count: int = 0

    def add_apply(self, result: CandidateSourceApplyResult) -> None:
        self.source_event_count += result.source_event_count
        self.candidate_created_count += result.candidate_created_count
        self.candidate_updated_count += result.candidate_updated_count
        self.duplicate_source_count += result.duplicate_source_count
        self.transition_count += result.transition_count
        self.error_count += result.error_count

    def to_ingest_result(self) -> CandidateSourceIngestResult:
        return CandidateSourceIngestResult(
            source_event_count=self.source_event_count,
            candidate_created_count=self.candidate_created_count,
            candidate_updated_count=self.candidate_updated_count,
            duplicate_source_count=self.duplicate_source_count,
            transition_count=self.transition_count,
            error_count=self.error_count,
        )


@dataclass
class _MutableRefreshResult:
    context_refreshed_count: int = 0
    transition_count: int = 0
    stale_count: int = 0
    closed_count: int = 0
    error_count: int = 0

    def add(self, result: CandidateRefreshResult) -> None:
        self.context_refreshed_count += result.context_refreshed_count
        self.transition_count += result.transition_count
        self.stale_count += result.stale_count
        self.closed_count += result.closed_count
        self.error_count += result.error_count
