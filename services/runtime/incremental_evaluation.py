from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, new_message_id, utc_now, validate_stock_code
from domain.candidate.state import CandidateState

from services.candidate_service import refresh_candidate_context
from services.config import Settings, load_settings
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.runtime.evaluation_run_guard import EVALUATION_PIPELINE_LOCK, runtime_execution_lock
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation

DIRTY_REASON_PRICE_TICK = "PRICE_TICK"


@dataclass(frozen=True, kw_only=True)
class IncrementalEvaluationEnqueueResult:
    status: str
    event_id: str
    code: str | None = None
    enqueued_count: int = 0
    candidate_ids: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "event_id": self.event_id,
            "code": self.code,
            "enqueued_count": self.enqueued_count,
            "candidate_ids": list(self.candidate_ids),
        }


@dataclass(frozen=True, kw_only=True)
class IncrementalEvaluationBatchResult:
    run_id: str
    status: str
    queued_before: int
    queued_after: int
    polled_count: int = 0
    processed_count: int = 0
    skipped_closed_count: int = 0
    strategy_observation_count: int = 0
    risk_observation_count: int = 0
    error_count: int = 0
    errors: Sequence[dict[str, Any]] = field(default_factory=tuple)
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    observe_only: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "queued_before": self.queued_before,
            "queued_after": self.queued_after,
            "polled_count": self.polled_count,
            "processed_count": self.processed_count,
            "skipped_closed_count": self.skipped_closed_count,
            "strategy_observation_count": self.strategy_observation_count,
            "risk_observation_count": self.risk_observation_count,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "created_at": self.created_at,
            "observe_only": self.observe_only,
            "not_order_intent": True,
            "no_order_side_effects": self.no_order_side_effects,
            "real_order_allowed": False,
        }


def enqueue_incremental_evaluation_for_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> IncrementalEvaluationEnqueueResult:
    resolved_settings = settings or load_settings()
    event_type = event.event_type.strip().lower()
    if not resolved_settings.incremental_evaluation_enabled:
        return IncrementalEvaluationEnqueueResult(status="DISABLED", event_id=event.event_id)
    if event_type != "price_tick":
        return IncrementalEvaluationEnqueueResult(
            status="IGNORED_EVENT_TYPE",
            event_id=event.event_id,
        )
    try:
        code = validate_stock_code(event.payload.get("code"))
    except Exception:
        return IncrementalEvaluationEnqueueResult(
            status="IGNORED_INVALID_PAYLOAD",
            event_id=event.event_id,
        )

    rows = _active_candidate_rows_for_code(connection, code)
    if not rows:
        return IncrementalEvaluationEnqueueResult(
            status="IGNORED_NO_ACTIVE_CANDIDATE",
            event_id=event.event_id,
            code=code,
        )

    now = datetime_to_wire(utc_now())
    candidate_ids = tuple(str(row["candidate_instance_id"]) for row in rows)
    for row in rows:
        connection.execute(
            """
            INSERT INTO incremental_evaluation_queue (
                candidate_instance_id,
                trade_date,
                code,
                reason,
                source_event_id,
                priority,
                enqueued_at,
                updated_at,
                attempts,
                last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
            ON CONFLICT(candidate_instance_id) DO UPDATE SET
                trade_date = excluded.trade_date,
                code = excluded.code,
                reason = excluded.reason,
                source_event_id = excluded.source_event_id,
                priority = CASE
                    WHEN excluded.priority > incremental_evaluation_queue.priority
                    THEN excluded.priority
                    ELSE incremental_evaluation_queue.priority
                END,
                updated_at = excluded.updated_at,
                last_error = NULL
            """,
            (
                row["candidate_instance_id"],
                row["trade_date"],
                row["code"],
                DIRTY_REASON_PRICE_TICK,
                event.event_id,
                100,
                now,
                now,
            ),
        )
    connection.commit()
    return IncrementalEvaluationEnqueueResult(
        status="ENQUEUED",
        event_id=event.event_id,
        code=code,
        enqueued_count=len(candidate_ids),
        candidate_ids=candidate_ids,
    )


def process_incremental_evaluation_batch(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int | None = None,
) -> IncrementalEvaluationBatchResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("incremental_eval")
    queued_before = _queue_count(connection)
    if not resolved_settings.incremental_evaluation_enabled:
        return IncrementalEvaluationBatchResult(
            run_id=run_id,
            status="DISABLED",
            queued_before=queued_before,
            queued_after=queued_before,
        )

    bounded_limit = _bounded_limit(limit or resolved_settings.incremental_evaluation_batch_size)
    with runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        details={"run_type": "incremental_evaluation", "limit": bounded_limit},
    ):
        rows = _queue_rows(
            connection,
            limit=bounded_limit,
            retry_limit=resolved_settings.incremental_evaluation_retry_limit,
        )
        if not rows:
            queued_after = _queue_count(connection)
            return IncrementalEvaluationBatchResult(
                run_id=run_id,
                status="IDLE",
                queued_before=queued_before,
                queued_after=queued_after,
            )

        processed_count = 0
        skipped_closed_count = 0
        strategy_count = 0
        risk_count = 0
        errors: list[dict[str, Any]] = []

        for row in rows:
            candidate_id = str(row["candidate_instance_id"])
            try:
                if not _is_candidate_active(connection, candidate_id):
                    _delete_queue_row(connection, candidate_id)
                    connection.commit()
                    skipped_closed_count += 1
                    continue

                refresh_result = refresh_candidate_context(
                    connection,
                    candidate_id,
                    settings=resolved_settings,
                )
                if refresh_result.error_count:
                    raise RuntimeError("candidate context refresh failed")
                if not _is_candidate_active(connection, candidate_id):
                    _delete_queue_row(connection, candidate_id)
                    connection.commit()
                    skipped_closed_count += 1
                    continue

                if resolved_settings.strategy_engine_enabled:
                    strategy_observation = evaluate_candidate_strategy(
                        connection,
                        candidate_id,
                        settings=resolved_settings,
                    )
                    save_strategy_observation(connection, strategy_observation)
                    strategy_count += 1
                if resolved_settings.risk_gate_enabled:
                    risk_observation = evaluate_risk_for_candidate(
                        connection,
                        candidate_id,
                        settings=resolved_settings,
                    )
                    save_risk_observation(connection, risk_observation)
                    risk_count += 1

                _delete_queue_row(connection, candidate_id)
                connection.commit()
                processed_count += 1
            except Exception as exc:
                connection.rollback()
                error = {
                    "candidate_instance_id": candidate_id,
                    "code": row["code"],
                    "error_message": str(exc),
                }
                errors.append(error)
                _mark_queue_error(connection, candidate_id, str(exc))
                connection.commit()

        queued_after = _queue_count(connection)
        status = "COMPLETED_WITH_ERRORS" if errors else "COMPLETED"
        return IncrementalEvaluationBatchResult(
            run_id=run_id,
            status=status,
            queued_before=queued_before,
            queued_after=queued_after,
            polled_count=len(rows),
            processed_count=processed_count,
            skipped_closed_count=skipped_closed_count,
            strategy_observation_count=strategy_count,
            risk_observation_count=risk_count,
            error_count=len(errors),
            errors=tuple(errors),
        )


def get_incremental_evaluation_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS queued_count,
            SUM(CASE WHEN attempts >= ? THEN 1 ELSE 0 END) AS retry_exhausted_count,
            MIN(enqueued_at) AS oldest_enqueued_at,
            MAX(updated_at) AS latest_updated_at,
            MAX(attempts) AS max_attempts
        FROM incremental_evaluation_queue
        """,
        (resolved_settings.incremental_evaluation_retry_limit,),
    ).fetchone()
    return {
        "enabled": resolved_settings.incremental_evaluation_enabled,
        "worker_enabled": resolved_settings.incremental_evaluation_worker_enabled,
        "worker_interval_sec": resolved_settings.incremental_evaluation_worker_interval_sec,
        "batch_size": resolved_settings.incremental_evaluation_batch_size,
        "retry_limit": resolved_settings.incremental_evaluation_retry_limit,
        "queued_count": int(row["queued_count"] or 0),
        "retry_exhausted_count": int(row["retry_exhausted_count"] or 0),
        "oldest_enqueued_at": row["oldest_enqueued_at"],
        "latest_updated_at": row["latest_updated_at"],
        "max_attempts": int(row["max_attempts"] or 0),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
    }


def _active_candidate_rows_for_code(
    connection: sqlite3.Connection,
    code: str,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT candidate_instance_id, trade_date, code
        FROM candidates
        WHERE code = ?
            AND state != ?
        ORDER BY last_seen_at DESC, candidate_instance_id ASC
        """,
        (code, CandidateState.CLOSED.value),
    ).fetchall()


def _queue_rows(
    connection: sqlite3.Connection,
    *,
    limit: int,
    retry_limit: int,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM incremental_evaluation_queue
        WHERE attempts < ?
        ORDER BY priority DESC, updated_at ASC, candidate_instance_id ASC
        LIMIT ?
        """,
        (retry_limit, limit),
    ).fetchall()


def _queue_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()
    return int(row["count"] or 0)


def _is_candidate_active(connection: sqlite3.Connection, candidate_instance_id: str) -> bool:
    row = connection.execute(
        """
        SELECT state
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()
    return row is not None and row["state"] != CandidateState.CLOSED.value


def _delete_queue_row(connection: sqlite3.Connection, candidate_instance_id: str) -> None:
    connection.execute(
        """
        DELETE FROM incremental_evaluation_queue
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    )


def _mark_queue_error(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    error_message: str,
) -> None:
    connection.execute(
        """
        UPDATE incremental_evaluation_queue
        SET attempts = attempts + 1,
            last_error = ?,
            updated_at = ?
        WHERE candidate_instance_id = ?
        """,
        (_truncate_error(error_message), datetime_to_wire(utc_now()), candidate_instance_id),
    )


def _bounded_limit(value: int) -> int:
    return max(min(int(value), 500), 1)


def _truncate_error(value: str) -> str:
    normalized = value.strip()
    return normalized[:500] if len(normalized) > 500 else normalized
