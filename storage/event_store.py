from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.orders import BrokerExecutionEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import BrokerValidationError, datetime_to_wire, utc_now

from storage.gateway_command_store import (
    canonical_json,
    hash_payload_json,
    record_command_event,
)

SUPPORTED_GATEWAY_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "heartbeat",
        "price_tick",
        "condition_event",
        "tr_response",
        "execution_event",
        "command_started",
        "command_ack",
        "command_failed",
        "gateway_error",
    }
)
_PAYLOAD_VALIDATORS = {
    "price_tick": BrokerPriceTick.from_dict,
    "condition_event": BrokerConditionEvent.from_dict,
    "tr_response": BrokerTrResponse.from_dict,
    "execution_event": BrokerExecutionEvent.from_dict,
}


@dataclass(frozen=True, kw_only=True)
class AppendGatewayEventResult:
    accepted: bool
    event_id: str
    duplicate: bool
    status: str
    payload_hash: str
    error_message: str | None = None


def append_gateway_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> AppendGatewayEventResult:
    event_type = event.event_type.strip().lower()
    payload_json = canonical_json(event.payload)
    payload_hash = hash_payload_json(payload_json)
    existing = connection.execute(
        """
        SELECT payload_hash
        FROM raw_events
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()

    if existing is not None:
        if existing["payload_hash"] == payload_hash:
            connection.execute(
                """
                UPDATE raw_events
                SET duplicate_count = duplicate_count + 1
                WHERE event_id = ?
                """,
                (event.event_id,),
            )
            connection.commit()
            gateway_row = connection.execute(
                "SELECT status, error_message FROM gateway_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            status = gateway_row["status"] if gateway_row is not None else "ACCEPTED"
            error_message = gateway_row["error_message"] if gateway_row is not None else None
            return AppendGatewayEventResult(
                accepted=True,
                event_id=event.event_id,
                duplicate=True,
                status=status,
                payload_hash=payload_hash,
                error_message=error_message,
            )
        return AppendGatewayEventResult(
            accepted=False,
            event_id=event.event_id,
            duplicate=False,
            status="CONFLICT",
            payload_hash=payload_hash,
            error_message="event_id already exists with a different payload_hash",
        )

    status, error_message = _classify_event(event_type, event.payload)
    event_ts = datetime_to_wire(event.ts)
    received_at = datetime_to_wire(utc_now())

    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO raw_events (
                event_id,
                event_type,
                source,
                command_id,
                idempotency_key,
                event_ts,
                received_at,
                payload_json,
                payload_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event_type,
                event.source,
                event.command_id,
                event.idempotency_key,
                event_ts,
                received_at,
                payload_json,
                payload_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO gateway_events (
                event_id,
                event_type,
                source,
                command_id,
                idempotency_key,
                event_ts,
                received_at,
                payload_json,
                status,
                error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event_type,
                event.source,
                event.command_id,
                event.idempotency_key,
                event_ts,
                received_at,
                payload_json,
                status,
                error_message,
            ),
        )
        _upsert_gateway_status(connection, "last_event_received_at", received_at)
        if event_type == "heartbeat":
            _upsert_gateway_status(connection, "last_heartbeat_at", event_ts)
        if event_type in {"command_started", "command_ack", "command_failed"} and event.command_id:
            record_command_event(
                connection,
                command_id=event.command_id,
                event_type=event_type,
                payload=dict(event.payload),
                commit=False,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return AppendGatewayEventResult(
        accepted=status != "REJECTED",
        event_id=event.event_id,
        duplicate=False,
        status=status,
        payload_hash=payload_hash,
        error_message=error_message,
    )


def list_recent_gateway_events(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT
            event_id,
            event_type,
            source,
            command_id,
            idempotency_key,
            event_ts,
            received_at,
            payload_json,
            status,
            error_message
        FROM gateway_events
        ORDER BY received_at DESC, event_id DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_gateway_event_row_to_dict(row) for row in rows]


def get_gateway_status_values(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute("SELECT key, value FROM gateway_status").fetchall()
    return {row["key"]: row["value"] for row in rows}


def count_recent_gateway_events(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM gateway_events").fetchone()
    return int(row["count"])


def _classify_event(event_type: str, payload: dict[str, Any]) -> tuple[str, str | None]:
    if event_type not in SUPPORTED_GATEWAY_EVENT_TYPES:
        return "UNKNOWN_EVENT_TYPE", f"Unknown gateway event_type: {event_type}"

    validator = _PAYLOAD_VALIDATORS.get(event_type)
    if validator is None:
        return "ACCEPTED", None

    try:
        validator(payload)
    except (BrokerValidationError, ValueError) as exc:
        return "REJECTED", str(exc)
    return "ACCEPTED", None


def _upsert_gateway_status(
    connection: sqlite3.Connection,
    key: str,
    value: str,
) -> None:
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, value, datetime_to_wire(utc_now())),
    )


def _gateway_event_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data: dict[str, Any] = {
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "source": row["source"],
        "event_ts": row["event_ts"],
        "received_at": row["received_at"],
        "payload": json.loads(row["payload_json"]),
        "status": row["status"],
    }
    if row["command_id"] is not None:
        data["command_id"] = row["command_id"]
    if row["idempotency_key"] is not None:
        data["idempotency_key"] = row["idempotency_key"]
    if row["error_message"] is not None:
        data["error_message"] = row["error_message"]
    return data
