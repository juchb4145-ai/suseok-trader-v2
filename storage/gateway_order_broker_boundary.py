from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now

ORDER_COMMAND_TYPES: frozenset[str] = frozenset({"send_order", "cancel_order"})


class OrderBrokerBoundaryState(StrEnum):
    CLAIMED = "CLAIMED"
    GATEWAY_STARTED = "GATEWAY_STARTED"
    PRE_ACK_RECORDED = "PRE_ACK_RECORDED"
    BROKER_ACCEPTED = "BROKER_ACCEPTED"
    CHEJAN_CONFIRMED = "CHEJAN_CONFIRMED"
    UNCONFIRMED = "UNCONFIRMED"


_STATE_RANK = {
    OrderBrokerBoundaryState.CLAIMED.value: 10,
    OrderBrokerBoundaryState.GATEWAY_STARTED.value: 20,
    OrderBrokerBoundaryState.PRE_ACK_RECORDED.value: 30,
    OrderBrokerBoundaryState.UNCONFIRMED.value: 35,
    OrderBrokerBoundaryState.BROKER_ACCEPTED.value: 40,
    OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value: 50,
}
_DURABLE_PRE_ACK_STATES = {
    OrderBrokerBoundaryState.PRE_ACK_RECORDED.value,
    OrderBrokerBoundaryState.BROKER_ACCEPTED.value,
    OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value,
}
_MIGRATION_SAVEPOINT = "gateway_order_broker_boundary_migration"


def ensure_gateway_order_broker_boundary_schema(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(f"SAVEPOINT {_MIGRATION_SAVEPOINT}")
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS gateway_order_broker_boundaries (
                command_id TEXT PRIMARY KEY,
                idempotency_key TEXT,
                command_type TEXT NOT NULL,
                source TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                account_id TEXT,
                code TEXT,
                side TEXT,
                broker_order_no TEXT,
                broker_result_code TEXT,
                broker_message TEXT,
                claimed_at TEXT,
                gateway_started_at TEXT,
                pre_ack_recorded_at TEXT,
                broker_accepted_at TEXT,
                chejan_confirmed_at TEXT,
                unconfirmed_at TEXT,
                last_event_id TEXT,
                pre_ack_payload_json TEXT NOT NULL DEFAULT '{}',
                latest_payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                live_sim_only INTEGER NOT NULL DEFAULT 1,
                live_real_allowed INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_gateway_order_boundary_idempotency
            ON gateway_order_broker_boundaries (idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gateway_order_boundary_state_updated
            ON gateway_order_broker_boundaries (state, updated_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gateway_order_boundary_broker_order_no
            ON gateway_order_broker_boundaries (broker_order_no)
            WHERE broker_order_no IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gateway_events_command_event
            ON gateway_events (command_id, event_type, received_at)
            WHERE command_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gateway_command_events_command_event
            ON gateway_command_events (command_id, event_type, created_at)
            """
        )
        _backfill_order_broker_boundaries(connection)
    except Exception:
        connection.execute(f"ROLLBACK TO {_MIGRATION_SAVEPOINT}")
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")
        raise
    else:
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")


def is_order_command_type(command_type: object) -> bool:
    return str(command_type or "").strip().lower() in ORDER_COMMAND_TYPES


def record_order_command_claim(
    connection: sqlite3.Connection,
    command: Mapping[str, Any] | sqlite3.Row,
    *,
    claimed_at: str,
) -> str | None:
    if not is_order_command_type(_value(command, "command_type")):
        return None
    return _upsert_boundary(
        connection,
        command=command,
        proposed_state=OrderBrokerBoundaryState.CLAIMED.value,
        payload=_payload_from_command(command),
        event_id=None,
        occurred_at=claimed_at,
    )


def record_order_broker_event(
    connection: sqlite3.Connection,
    command: Mapping[str, Any] | sqlite3.Row,
    *,
    event_type: str,
    payload: Mapping[str, Any],
    event_id: str | None = None,
    occurred_at: str | None = None,
) -> str | None:
    if not is_order_command_type(_value(command, "command_type")):
        return None
    proposed_state = _state_for_event(event_type, payload)
    if proposed_state is None:
        return _update_latest_boundary_payload(
            connection,
            command=command,
            payload=payload,
            event_id=event_id,
            occurred_at=occurred_at or datetime_to_wire(utc_now()),
        )
    return _upsert_boundary(
        connection,
        command=command,
        proposed_state=proposed_state,
        payload=payload,
        event_id=event_id,
        occurred_at=occurred_at or datetime_to_wire(utc_now()),
        pre_ack_observed=str(event_type).strip().lower() == "order_pre_ack",
    )


def mark_order_commands_unconfirmed(
    connection: sqlite3.Connection,
    command_ids: Sequence[str],
    *,
    occurred_at: str,
    reason: str,
) -> None:
    for command_id in dict.fromkeys(str(item) for item in command_ids if str(item)):
        command = connection.execute(
            "SELECT * FROM gateway_commands WHERE command_id = ?",
            (command_id,),
        ).fetchone()
        if command is None or not is_order_command_type(command["command_type"]):
            continue
        _upsert_boundary(
            connection,
            command=command,
            proposed_state=OrderBrokerBoundaryState.UNCONFIRMED.value,
            payload={"reason": reason},
            event_id=None,
            occurred_at=occurred_at,
        )


def get_order_broker_boundary(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM gateway_order_broker_boundaries
        WHERE command_id = ?
        """,
        (str(command_id),),
    ).fetchone()
    return None if row is None else _boundary_row_to_dict(row)


def list_order_broker_boundaries(
    connection: sqlite3.Connection,
    *,
    state: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if state is not None:
        clauses.append("state = ?")
        params.append(str(state).strip().upper())
    params.append(min(max(int(limit), 1), 500))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_order_broker_boundaries
        {where_sql}
        ORDER BY updated_at DESC, command_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_boundary_row_to_dict(row) for row in rows]


def get_order_broker_boundary_status(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    table_exists = _table_exists(connection, "gateway_order_broker_boundaries")
    required_indexes = {
        "uq_gateway_order_boundary_idempotency",
        "idx_gateway_order_boundary_state_updated",
        "idx_gateway_events_command_event",
        "idx_gateway_command_events_command_event",
    }
    existing_indexes = _index_names(connection)
    if not table_exists:
        return {
            "status": "FAIL",
            "reason_codes": ["ORDER_BROKER_BOUNDARY_TABLE_MISSING"],
            "warning_codes": [],
            "table_exists": False,
            "required_indexes_present": False,
            "state_counts": {},
            "read_only": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }

    state_rows = connection.execute(
        """
        SELECT state, COUNT(*) AS count
        FROM gateway_order_broker_boundaries
        GROUP BY state
        """
    ).fetchall()
    state_counts = {state.value: 0 for state in OrderBrokerBoundaryState}
    for row in state_rows:
        state_counts[str(row["state"])] = int(row["count"] or 0)

    counts = connection.execute(
        """
        WITH expected AS (
            SELECT command_id
            FROM gateway_commands
            WHERE lower(command_type) IN ('send_order', 'cancel_order')
              AND (
                  dispatched_at IS NOT NULL
                  OR status IN (
                      'CLAIMED', 'GATEWAY_STARTED', 'PRE_ACK_RECORDED',
                      'BROKER_ACCEPTED', 'CHEJAN_CONFIRMED', 'UNCONFIRMED',
                      'DISPATCHED', 'ACKED'
                  )
              )
        )
        SELECT
            (SELECT COUNT(*) FROM gateway_commands
             WHERE lower(command_type) IN ('send_order', 'cancel_order'))
                AS order_command_count,
            (SELECT COUNT(*) FROM expected) AS expected_boundary_count,
            (SELECT COUNT(*) FROM gateway_order_broker_boundaries)
                AS boundary_count,
            (SELECT COUNT(*) FROM expected e
             LEFT JOIN gateway_order_broker_boundaries b
               ON b.command_id = e.command_id
             WHERE b.command_id IS NULL) AS missing_boundary_count,
            (SELECT COUNT(*) FROM gateway_order_broker_boundaries
             WHERE pre_ack_recorded_at IS NOT NULL)
                AS durable_pre_ack_count,
            (SELECT COUNT(*) FROM gateway_order_broker_boundaries
             WHERE state IN ('PRE_ACK_RECORDED', 'BROKER_ACCEPTED', 'CHEJAN_CONFIRMED')
               AND pre_ack_recorded_at IS NULL)
                AS durable_pre_ack_gap_count,
            (SELECT COUNT(*) FROM (
                SELECT idempotency_key
                FROM gateway_order_broker_boundaries
                WHERE idempotency_key IS NOT NULL
                GROUP BY idempotency_key
                HAVING COUNT(*) > 1
            )) AS duplicate_idempotency_count,
            (SELECT COUNT(*)
             FROM gateway_order_broker_boundaries b
             JOIN gateway_commands c ON c.command_id = b.command_id
             WHERE c.status IN (
                 'CLAIMED', 'GATEWAY_STARTED', 'PRE_ACK_RECORDED',
                 'BROKER_ACCEPTED', 'CHEJAN_CONFIRMED', 'UNCONFIRMED'
             )
               AND c.status <> b.state) AS command_state_mismatch_count
        """
    ).fetchone()
    reason_codes: list[str] = []
    warning_codes: list[str] = []
    required_indexes_present = required_indexes.issubset(existing_indexes)
    if not required_indexes_present:
        reason_codes.append("ORDER_BROKER_BOUNDARY_INDEX_MISSING")
    for field, reason in (
        ("missing_boundary_count", "ORDER_BROKER_BOUNDARY_ROW_MISSING"),
        ("durable_pre_ack_gap_count", "DURABLE_PRE_ACK_GAP"),
        ("duplicate_idempotency_count", "ORDER_BOUNDARY_IDEMPOTENCY_DUPLICATE"),
        ("command_state_mismatch_count", "ORDER_COMMAND_BOUNDARY_STATE_MISMATCH"),
    ):
        if int(counts[field] or 0) > 0:
            reason_codes.append(reason)
    unconfirmed_count = int(
        state_counts.get(OrderBrokerBoundaryState.UNCONFIRMED.value, 0)
    )
    if unconfirmed_count > 0:
        warning_codes.append("UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE")

    status = "FAIL" if reason_codes else "WARN" if warning_codes else "PASS"
    return {
        "status": status,
        "reason_codes": reason_codes,
        "warning_codes": warning_codes,
        "table_exists": True,
        "required_indexes_present": required_indexes_present,
        "order_command_count": int(counts["order_command_count"] or 0),
        "expected_boundary_count": int(counts["expected_boundary_count"] or 0),
        "boundary_count": int(counts["boundary_count"] or 0),
        "missing_boundary_count": int(counts["missing_boundary_count"] or 0),
        "durable_pre_ack_count": int(counts["durable_pre_ack_count"] or 0),
        "durable_pre_ack_gap_count": int(counts["durable_pre_ack_gap_count"] or 0),
        "duplicate_idempotency_count": int(
            counts["duplicate_idempotency_count"] or 0
        ),
        "command_state_mismatch_count": int(
            counts["command_state_mismatch_count"] or 0
        ),
        "unconfirmed_count": unconfirmed_count,
        "state_counts": state_counts,
        "block_new_order_routing": bool(reason_codes or unconfirmed_count),
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def _backfill_order_broker_boundaries(connection: sqlite3.Connection) -> None:
    commands = connection.execute(
        """
        SELECT *
        FROM gateway_commands
        WHERE lower(command_type) IN ('send_order', 'cancel_order')
          AND (
              dispatched_at IS NOT NULL
              OR status IN (
                  'CLAIMED', 'GATEWAY_STARTED', 'PRE_ACK_RECORDED',
                  'BROKER_ACCEPTED', 'CHEJAN_CONFIRMED', 'UNCONFIRMED',
                  'DISPATCHED', 'ACKED'
              )
          )
        ORDER BY created_at, command_id
        """
    ).fetchall()
    if not commands:
        return
    command_ids = {str(row["command_id"]) for row in commands}
    events_by_command: dict[str, list[dict[str, Any]]] = defaultdict(list)
    event_rows = connection.execute(
        """
        SELECT *
        FROM (
            SELECT
                e.command_id AS command_id,
                e.event_type AS event_type,
                e.payload_json AS payload_json,
                e.event_id AS event_id,
                e.received_at AS occurred_at
            FROM gateway_events e
            JOIN gateway_commands c ON c.command_id = e.command_id
            WHERE lower(c.command_type) IN ('send_order', 'cancel_order')
            UNION ALL
            SELECT
                e.command_id AS command_id,
                e.event_type AS event_type,
                e.payload_json AS payload_json,
                'command-event:' || e.id AS event_id,
                e.created_at AS occurred_at
            FROM gateway_command_events e
            JOIN gateway_commands c ON c.command_id = e.command_id
            WHERE lower(c.command_type) IN ('send_order', 'cancel_order')
        )
        ORDER BY command_id, occurred_at, event_id
        """
    ).fetchall()
    for event in event_rows:
        command_id = str(event["command_id"])
        if command_id not in command_ids:
            continue
        events_by_command[command_id].append(
            {
                "event_type": event["event_type"],
                "payload": _json_object(event["payload_json"]),
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
        )

    for command in commands:
        command_id = str(command["command_id"])
        claimed_at = str(command["dispatched_at"] or command["created_at"])
        record_order_command_claim(connection, command, claimed_at=claimed_at)
        initial_state = _state_from_legacy_command_status(str(command["status"]))
        if initial_state is not None:
            _upsert_boundary(
                connection,
                command=command,
                proposed_state=initial_state,
                payload=_payload_from_command(command),
                event_id=None,
                occurred_at=claimed_at,
            )
        for event in events_by_command.get(command_id, []):
            record_order_broker_event(
                connection,
                command,
                event_type=str(event["event_type"]),
                payload=event["payload"],
                event_id=str(event["event_id"]),
                occurred_at=str(event["occurred_at"]),
            )
        boundary = get_order_broker_boundary(connection, command_id)
        if boundary is None:
            continue
        current_status = str(command["status"]).upper()
        if current_status in {
            "DISPATCHED",
            "ACKED",
            "CLAIMED",
            "GATEWAY_STARTED",
            "PRE_ACK_RECORDED",
            "BROKER_ACCEPTED",
            "CHEJAN_CONFIRMED",
            "UNCONFIRMED",
        }:
            connection.execute(
                "UPDATE gateway_commands SET status = ? WHERE command_id = ?",
                (boundary["state"], command_id),
            )


def _upsert_boundary(
    connection: sqlite3.Connection,
    *,
    command: Mapping[str, Any] | sqlite3.Row,
    proposed_state: str,
    payload: Mapping[str, Any],
    event_id: str | None,
    occurred_at: str,
    pre_ack_observed: bool = False,
) -> str:
    command_id = str(_value(command, "command_id") or "")
    existing = connection.execute(
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (command_id,),
    ).fetchone()
    current_state = None if existing is None else str(existing["state"])
    effective_state = _resolve_state(current_state, proposed_state)
    command_payload = _payload_from_command(command)
    details = _details(payload)
    account_id = _first_text(
        payload.get("account_id"),
        payload.get("account"),
        command_payload.get("account_id"),
        command_payload.get("account"),
    )
    code = _first_text(payload.get("code"), command_payload.get("code"))
    side = _first_text(payload.get("side"), command_payload.get("side"))
    broker_order_no = _first_text(
        details.get("broker_order_no"),
        details.get("broker_order_id"),
        payload.get("broker_order_no"),
        payload.get("broker_order_id"),
    )
    broker_result_code = _first_text(
        details.get("broker_result_code"),
        payload.get("broker_result_code"),
    )
    broker_message = _first_text(
        details.get("broker_message"),
        payload.get("broker_message"),
        payload.get("message"),
        payload.get("error_message"),
    )
    pre_ack_json = _canonical_json(payload) if pre_ack_observed else None
    timestamps = _state_timestamps(
        proposed_state,
        occurred_at,
        pre_ack_observed=pre_ack_observed,
    )
    created_at = (
        occurred_at
        if existing is None
        else str(existing["created_at"] or occurred_at)
    )
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundaries (
            command_id,
            idempotency_key,
            command_type,
            source,
            state,
            attempts,
            account_id,
            code,
            side,
            broker_order_no,
            broker_result_code,
            broker_message,
            claimed_at,
            gateway_started_at,
            pre_ack_recorded_at,
            broker_accepted_at,
            chejan_confirmed_at,
            unconfirmed_at,
            last_event_id,
            pre_ack_payload_json,
            latest_payload_json,
            created_at,
            updated_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        ON CONFLICT(command_id) DO UPDATE SET
            idempotency_key = COALESCE(
                gateway_order_broker_boundaries.idempotency_key,
                excluded.idempotency_key
            ),
            state = excluded.state,
            attempts = MAX(gateway_order_broker_boundaries.attempts, excluded.attempts),
            account_id = COALESCE(gateway_order_broker_boundaries.account_id, excluded.account_id),
            code = COALESCE(gateway_order_broker_boundaries.code, excluded.code),
            side = COALESCE(gateway_order_broker_boundaries.side, excluded.side),
            broker_order_no = COALESCE(
                excluded.broker_order_no,
                gateway_order_broker_boundaries.broker_order_no
            ),
            broker_result_code = COALESCE(
                excluded.broker_result_code,
                gateway_order_broker_boundaries.broker_result_code
            ),
            broker_message = COALESCE(
                excluded.broker_message,
                gateway_order_broker_boundaries.broker_message
            ),
            claimed_at = COALESCE(gateway_order_broker_boundaries.claimed_at, excluded.claimed_at),
            gateway_started_at = COALESCE(
                gateway_order_broker_boundaries.gateway_started_at,
                excluded.gateway_started_at
            ),
            pre_ack_recorded_at = COALESCE(
                gateway_order_broker_boundaries.pre_ack_recorded_at,
                excluded.pre_ack_recorded_at
            ),
            broker_accepted_at = COALESCE(
                gateway_order_broker_boundaries.broker_accepted_at,
                excluded.broker_accepted_at
            ),
            chejan_confirmed_at = COALESCE(
                gateway_order_broker_boundaries.chejan_confirmed_at,
                excluded.chejan_confirmed_at
            ),
            unconfirmed_at = COALESCE(
                gateway_order_broker_boundaries.unconfirmed_at,
                excluded.unconfirmed_at
            ),
            last_event_id = COALESCE(
                excluded.last_event_id,
                gateway_order_broker_boundaries.last_event_id
            ),
            pre_ack_payload_json = CASE
                WHEN excluded.pre_ack_payload_json <> '{}'
                THEN excluded.pre_ack_payload_json
                ELSE gateway_order_broker_boundaries.pre_ack_payload_json
            END,
            latest_payload_json = excluded.latest_payload_json,
            updated_at = excluded.updated_at
        """,
        (
            command_id,
            _optional_text(_value(command, "idempotency_key")),
            str(_value(command, "command_type") or "").strip().lower(),
            str(_value(command, "source") or "unknown"),
            effective_state,
            int(_value(command, "attempts") or 0),
            account_id,
            code,
            side,
            broker_order_no,
            broker_result_code,
            broker_message,
            timestamps["claimed_at"],
            timestamps["gateway_started_at"],
            timestamps["pre_ack_recorded_at"],
            timestamps["broker_accepted_at"],
            timestamps["chejan_confirmed_at"],
            timestamps["unconfirmed_at"],
            event_id,
            pre_ack_json or "{}",
            _canonical_json(payload),
            created_at,
            occurred_at,
        ),
    )
    return effective_state


def _update_latest_boundary_payload(
    connection: sqlite3.Connection,
    *,
    command: Mapping[str, Any] | sqlite3.Row,
    payload: Mapping[str, Any],
    event_id: str | None,
    occurred_at: str,
) -> str | None:
    command_id = str(_value(command, "command_id") or "")
    row = connection.execute(
        "SELECT state FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (command_id,),
    ).fetchone()
    if row is None:
        return None
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET latest_payload_json = ?,
            last_event_id = COALESCE(?, last_event_id),
            updated_at = ?
        WHERE command_id = ?
        """,
        (_canonical_json(payload), event_id, occurred_at, command_id),
    )
    return str(row["state"])


def _state_for_event(event_type: str, payload: Mapping[str, Any]) -> str | None:
    normalized = str(event_type).strip().lower()
    if normalized == "command_started":
        return OrderBrokerBoundaryState.GATEWAY_STARTED.value
    if normalized == "order_pre_ack":
        payload_status = str(payload.get("status") or "").upper()
        if "BROKER_ACCEPTED" in payload_status:
            return OrderBrokerBoundaryState.BROKER_ACCEPTED.value
        return OrderBrokerBoundaryState.PRE_ACK_RECORDED.value
    if normalized == "command_ack":
        return OrderBrokerBoundaryState.BROKER_ACCEPTED.value
    if normalized == "order_broker_unconfirmed":
        return OrderBrokerBoundaryState.UNCONFIRMED.value
    if normalized in {"execution_event", "kiwoom_order_chejan"}:
        return OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value
    return None


def _state_from_legacy_command_status(status: str) -> str | None:
    normalized = str(status).strip().upper()
    if normalized == "DISPATCHED":
        return OrderBrokerBoundaryState.CLAIMED.value
    if normalized == "ACKED":
        return OrderBrokerBoundaryState.BROKER_ACCEPTED.value
    if normalized in _STATE_RANK:
        return normalized
    return None


def _resolve_state(current_state: str | None, proposed_state: str) -> str:
    proposed = str(proposed_state).upper()
    if current_state is None:
        return proposed
    current = str(current_state).upper()
    if current == OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value:
        return current
    if proposed == OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value:
        return proposed
    if proposed == OrderBrokerBoundaryState.BROKER_ACCEPTED.value:
        return proposed
    if current == OrderBrokerBoundaryState.UNCONFIRMED.value:
        return current
    return proposed if _STATE_RANK.get(proposed, 0) >= _STATE_RANK.get(current, 0) else current


def _state_timestamps(
    state: str,
    occurred_at: str,
    *,
    pre_ack_observed: bool,
) -> dict[str, str | None]:
    rank = _STATE_RANK.get(state, 0)
    return {
        "claimed_at": occurred_at if rank >= 10 else None,
        "gateway_started_at": occurred_at if rank >= 20 and state != "UNCONFIRMED" else None,
        "pre_ack_recorded_at": occurred_at if pre_ack_observed else None,
        "broker_accepted_at": occurred_at if rank >= 40 else None,
        "chejan_confirmed_at": (
            occurred_at
            if state == OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value
            else None
        ),
        "unconfirmed_at": (
            occurred_at
            if state == OrderBrokerBoundaryState.UNCONFIRMED.value
            else None
        ),
    }


def _payload_from_command(command: Mapping[str, Any] | sqlite3.Row) -> dict[str, Any]:
    return _json_object(_value(command, "payload_json"))


def _boundary_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["pre_ack_payload"] = _json_object(item.pop("pre_ack_payload_json"))
    item["latest_payload"] = _json_object(item.pop("latest_payload_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["durable_pre_ack_recorded"] = bool(item.get("pre_ack_recorded_at"))
    item["broker_boundary_reached"] = item["state"] in _DURABLE_PRE_ACK_STATES
    return item


def _details(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("details")
    return value if isinstance(value, Mapping) else {}


def _value(row: Mapping[str, Any] | sqlite3.Row, key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return None


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    try:
        loaded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _first_text(*values: object) -> str | None:
    for value in values:
        normalized = _optional_text(value)
        if normalized is not None:
            return normalized
    return None


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _index_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index'"
    ).fetchall()
    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows}
