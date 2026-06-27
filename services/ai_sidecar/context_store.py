from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from domain.ai_sidecar.context import AISidecarContextPacket, AISidecarContextSection
from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import normalize_payload, normalize_value, optional_non_empty_str
from storage.gateway_command_store import canonical_json


def save_context_packet(connection: sqlite3.Connection, packet: AISidecarContextPacket) -> str:
    data = packet.to_dict()
    connection.execute(
        """
        INSERT OR IGNORE INTO ai_context_packets (
            context_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            context_hash,
            schema_version,
            size_chars,
            max_size_chars,
            truncated,
            redaction_applied,
            order_context_included,
            missing_sections_json,
            warnings_json,
            source_sections_json,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            packet.context_id,
            packet.task_type.value,
            packet.trade_date,
            packet.related_entity_type,
            packet.related_entity_id,
            packet.context_hash,
            packet.schema_version,
            packet.size_chars,
            packet.max_size_chars,
            1 if packet.truncated else 0,
            1 if packet.redaction_applied else 0,
            1 if packet.order_context_included else 0,
            _json_dumps(data["missing_sections"]),
            _json_dumps(data["warnings"]),
            _json_dumps(data["source_sections"]),
            canonical_json(data["payload"]),
        ),
    )
    connection.commit()
    return packet.context_id


def get_context_packet(
    connection: sqlite3.Connection,
    context_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_context_packets
        WHERE context_id = ?
        """,
        (context_id,),
    ).fetchone()
    return None if row is None else context_packet_row_to_dict(row)


def list_context_packets(
    connection: sqlite3.Connection,
    *,
    task_type: AISidecarTaskType | str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if task_type is not None:
        task_value = task_type.value if isinstance(task_type, AISidecarTaskType) else str(task_type)
        clauses.append("task_type = ?")
        params.append(task_value.strip().upper())
    if related_entity_type is not None:
        clauses.append("related_entity_type = ?")
        params.append(optional_non_empty_str(related_entity_type, "related_entity_type"))
    if related_entity_id is not None:
        clauses.append("related_entity_id = ?")
        params.append(optional_non_empty_str(related_entity_id, "related_entity_id"))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit, 500))
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_context_packets
        {where_sql}
        ORDER BY created_at DESC, context_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [context_packet_row_to_dict(row, include_payload=False) for row in rows]


def save_context_build_error(
    connection: sqlite3.Connection,
    *,
    task_type: AISidecarTaskType | str | None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    error_message: str,
    payload: Mapping[str, Any] | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO ai_context_build_errors (
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            task_type.value if isinstance(task_type, AISidecarTaskType) else task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            str(error_message),
            canonical_json(normalize_payload(payload or {})),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def list_context_build_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_context_build_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit, 500),),
    ).fetchall()
    errors = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["payload"] = json.loads(item.pop("payload_json"))
        errors.append(item)
    return errors


def context_packet_row_to_dict(
    row: sqlite3.Row,
    *,
    include_payload: bool = True,
) -> dict[str, Any]:
    data = {
        "context_id": row["context_id"],
        "task_type": row["task_type"],
        "schema_version": row["schema_version"],
        "trade_date": row["trade_date"],
        "related_entity_type": row["related_entity_type"],
        "related_entity_id": row["related_entity_id"],
        "context_hash": row["context_hash"],
        "size_chars": row["size_chars"],
        "max_size_chars": row["max_size_chars"],
        "truncated": bool(row["truncated"]),
        "redaction_applied": bool(row["redaction_applied"]),
        "order_context_included": bool(row["order_context_included"]),
        "missing_sections": json.loads(row["missing_sections_json"]),
        "warnings": json.loads(row["warnings_json"]),
        "source_sections": json.loads(row["source_sections_json"]),
        "created_at": row["created_at"],
    }
    if include_payload:
        packet_data = dict(data)
        packet_data.pop("created_at", None)
        packet_data["payload"] = json.loads(row["payload_json"])
        packet_data["generated_at"] = row["created_at"]
        return AISidecarContextPacket.from_dict(packet_data).to_dict() | {
            "created_at": row["created_at"]
        }
    return data


def packet_from_row(row: sqlite3.Row) -> AISidecarContextPacket:
    data = context_packet_row_to_dict(row, include_payload=True)
    data.pop("created_at", None)
    return AISidecarContextPacket.from_dict(data)


def sections_from_rows(rows: list[Mapping[str, Any]]) -> list[AISidecarContextSection]:
    return [AISidecarContextSection.from_dict(row) for row in rows]


def _bounded_limit(limit: int, max_limit: int) -> int:
    return min(max(int(limit), 1), max_limit)


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
