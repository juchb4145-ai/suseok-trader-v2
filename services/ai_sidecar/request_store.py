from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from domain.ai_sidecar.policy import validate_task_allowed
from domain.ai_sidecar.schemas import AISidecarInsight, insight_row_to_dict
from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import new_message_id, normalize_payload, optional_non_empty_str
from storage.gateway_command_store import canonical_json


class AIRequestStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    AI_DISABLED = "AI_DISABLED"
    API_KEY_MISSING = "API_KEY_MISSING"
    CLIENT_UNAVAILABLE = "CLIENT_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    MODEL_ERROR = "MODEL_ERROR"
    AI_OUTPUT_INVALID = "AI_OUTPUT_INVALID"
    POLICY_REJECTED = "POLICY_REJECTED"
    CONTEXT_ERROR = "CONTEXT_ERROR"


TERMINAL_STATUSES = {
    AIRequestStatus.COMPLETED.value,
    AIRequestStatus.FAILED.value,
    AIRequestStatus.AI_DISABLED.value,
    AIRequestStatus.API_KEY_MISSING.value,
    AIRequestStatus.CLIENT_UNAVAILABLE.value,
    AIRequestStatus.TIMEOUT.value,
    AIRequestStatus.MODEL_ERROR.value,
    AIRequestStatus.AI_OUTPUT_INVALID.value,
    AIRequestStatus.POLICY_REJECTED.value,
    AIRequestStatus.CONTEXT_ERROR.value,
}


def create_ai_request(
    connection: sqlite3.Connection,
    *,
    task_type: AISidecarTaskType | str,
    request_id: str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    context_id: str | None = None,
    prompt_hash: str | None = None,
    context_hash: str | None = None,
    output_schema_name: str | None = None,
    model: str | None = None,
    status: AIRequestStatus | str = AIRequestStatus.QUEUED,
    error_message: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    task = validate_task_allowed(task_type)
    request_id = request_id or new_message_id("ai_req")
    connection.execute(
        """
        INSERT INTO ai_requests (
            request_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            context_id,
            prompt_hash,
            context_hash,
            output_schema_name,
            model,
            status,
            error_message,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            request_id,
            task.value,
            trade_date,
            optional_non_empty_str(related_entity_type, "related_entity_type"),
            optional_non_empty_str(related_entity_id, "related_entity_id"),
            context_id,
            prompt_hash,
            context_hash,
            output_schema_name,
            model,
            _status_value(status),
            error_message,
            canonical_json(normalize_payload(metadata or {})),
        ),
    )
    connection.commit()
    return request_id


def update_ai_request_status(
    connection: sqlite3.Connection,
    request_id: str,
    status: AIRequestStatus | str,
    *,
    error_message: str | None = None,
    validation_error: str | None = None,
    context_id: str | None = None,
    prompt_hash: str | None = None,
    context_hash: str | None = None,
    output_schema_name: str | None = None,
    model: str | None = None,
    latency_ms: float | None = None,
    input_chars: int | None = None,
    output_chars: int | None = None,
    raw_response: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    assignments = ["status = ?"]
    params: list[Any] = [_status_value(status)]
    optional_fields = {
        "error_message": error_message,
        "validation_error": validation_error,
        "context_id": context_id,
        "prompt_hash": prompt_hash,
        "context_hash": context_hash,
        "output_schema_name": output_schema_name,
        "model": model,
        "latency_ms": latency_ms,
        "input_chars": input_chars,
        "output_chars": output_chars,
    }
    for column_name, value in optional_fields.items():
        if value is not None:
            assignments.append(f"{column_name} = ?")
            params.append(value)
    if raw_response is not None:
        assignments.append("raw_response_json = ?")
        params.append(canonical_json(normalize_payload(raw_response)))
    if metadata is not None:
        assignments.append("metadata_json = ?")
        params.append(canonical_json(normalize_payload(metadata)))
    if _status_value(status) in TERMINAL_STATUSES:
        assignments.append("completed_at = datetime('now')")
    params.append(request_id)
    connection.execute(
        f"""
        UPDATE ai_requests
        SET {", ".join(assignments)}
        WHERE request_id = ?
        """,
        tuple(params),
    )
    connection.commit()


def get_ai_request(connection: sqlite3.Connection, request_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_requests
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()
    return None if row is None else ai_request_row_to_dict(row)


def list_ai_requests(
    connection: sqlite3.Connection,
    *,
    task_type: AISidecarTaskType | str | None = None,
    status: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if task_type is not None:
        clauses.append("task_type = ?")
        params.append(validate_task_allowed(task_type).value)
    if status is not None:
        clauses.append("status = ?")
        params.append(status.strip().upper())
    if related_entity_type is not None:
        clauses.append("related_entity_type = ?")
        params.append(optional_non_empty_str(related_entity_type, "related_entity_type"))
    if related_entity_id is not None:
        clauses.append("related_entity_id = ?")
        params.append(optional_non_empty_str(related_entity_id, "related_entity_id"))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_requests
        {where_sql}
        ORDER BY created_at DESC, request_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [ai_request_row_to_dict(row) for row in rows]


def save_ai_insight(connection: sqlite3.Connection, insight: AISidecarInsight) -> str:
    data = insight.to_dict()
    connection.execute(
        """
        INSERT INTO ai_insights (
            insight_id,
            request_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            summary,
            root_cause,
            severity,
            operator_action,
            output_json,
            schema_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            insight.insight_id,
            insight.request_id,
            insight.task_type.value,
            insight.trade_date,
            insight.related_entity_type,
            insight.related_entity_id,
            insight.summary,
            insight.root_cause,
            insight.severity.value if insight.severity is not None else None,
            insight.operator_action,
            canonical_json(data["output"]),
            insight.schema_version,
        ),
    )
    connection.commit()
    return insight.insight_id


def get_ai_insight(connection: sqlite3.Connection, insight_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_insights
        WHERE insight_id = ?
        """,
        (insight_id,),
    ).fetchone()
    return None if row is None else ai_insight_row_to_dict(row, include_output=True)


def list_ai_insights(
    connection: sqlite3.Connection,
    *,
    task_type: AISidecarTaskType | str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = 100,
    include_output: bool = False,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if task_type is not None:
        clauses.append("task_type = ?")
        params.append(validate_task_allowed(task_type).value)
    if related_entity_type is not None:
        clauses.append("related_entity_type = ?")
        params.append(optional_non_empty_str(related_entity_type, "related_entity_type"))
    if related_entity_id is not None:
        clauses.append("related_entity_id = ?")
        params.append(optional_non_empty_str(related_entity_id, "related_entity_id"))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_insights
        {where_sql}
        ORDER BY created_at DESC, insight_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [ai_insight_row_to_dict(row, include_output=include_output) for row in rows]


def get_ai_request_status_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM ai_requests
        GROUP BY status
        """
    ).fetchall()
    counts = {status.value: 0 for status in AIRequestStatus}
    for row in rows:
        counts[str(row["status"])] = int(row["count"])
    return counts


def get_last_ai_request_error(connection: sqlite3.Connection) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_requests
        WHERE error_message IS NOT NULL OR validation_error IS NOT NULL
        ORDER BY COALESCE(completed_at, created_at) DESC, request_id DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else ai_request_row_to_dict(row)


def count_ai_insights(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM ai_insights").fetchone()
    return int(row["count"])


def ai_request_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "request_id": row["request_id"],
        "task_type": row["task_type"],
        "trade_date": row["trade_date"],
        "related_entity_type": row["related_entity_type"],
        "related_entity_id": row["related_entity_id"],
        "context_id": _row_get(row, "context_id"),
        "prompt_hash": row["prompt_hash"],
        "context_hash": row["context_hash"],
        "output_schema_name": _row_get(row, "output_schema_name"),
        "model": row["model"],
        "status": row["status"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
        "error_message": row["error_message"],
        "validation_error": _row_get(row, "validation_error"),
        "latency_ms": _row_get(row, "latency_ms"),
        "input_chars": _row_get(row, "input_chars"),
        "output_chars": _row_get(row, "output_chars"),
        "raw_response": _json_loads(_row_get(row, "raw_response_json")),
        "metadata": _json_loads(_row_get(row, "metadata_json")) or {},
    }


def ai_insight_row_to_dict(
    row: sqlite3.Row,
    *,
    include_output: bool = False,
) -> dict[str, Any]:
    data = insight_row_to_dict(row)
    if include_output:
        data["output"] = _json_loads(row["output_json"]) or {}
    return data


def _status_value(status: AIRequestStatus | str) -> str:
    return status.value if isinstance(status, AIRequestStatus) else str(status).strip().upper()


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _json_loads(value: object) -> Any:
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        return None
    return json.loads(value)


def _row_get(row: sqlite3.Row, column_name: str) -> Any:
    if column_name not in row.keys():
        return None
    return row[column_name]
