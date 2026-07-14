from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.utils import datetime_to_wire, normalize_payload, parse_timestamp, utc_now

from storage.gateway_order_broker_boundary import (
    get_order_broker_boundary_status,
    is_order_command_type,
    mark_order_commands_unconfirmed,
    record_order_broker_event,
    record_order_command_claim,
)


class GatewayCommandStatus(StrEnum):
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    CLAIMED = "CLAIMED"
    GATEWAY_STARTED = "GATEWAY_STARTED"
    PRE_ACK_RECORDED = "PRE_ACK_RECORDED"
    BROKER_ACCEPTED = "BROKER_ACCEPTED"
    CHEJAN_CONFIRMED = "CHEJAN_CONFIRMED"
    UNCONFIRMED = "UNCONFIRMED"
    ACKED = "ACKED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


ALLOWED_COMMAND_TYPES: frozenset[str] = frozenset(
    {
        "heartbeat_request",
        "request_tr",
        "register_realtime",
        "remove_realtime",
        "load_conditions",
        "send_condition",
        "stop_condition",
    }
)
FORBIDDEN_ORDER_COMMAND_TYPES: frozenset[str] = frozenset(
    {
        "send_order",
        "submit_order",
        "cancel_order",
        "modify_order",
        "enqueue_order",
        "order_intent",
        "gateway_order",
        "live_order",
    }
)
DEFAULT_DISPATCH_TIMEOUT_SEC = 120


@dataclass(frozen=True, kw_only=True)
class EnqueueCommandResult:
    accepted: bool
    command_id: str
    status: GatewayCommandStatus
    payload_hash: str | None = None
    duplicate: bool = False
    error_message: str | None = None


def enqueue_command(
    connection: sqlite3.Connection,
    command: GatewayCommand,
    *,
    expires_at: datetime | str | None = None,
) -> EnqueueCommandResult:
    command_type = _normalize_command_type(command.command_type)
    payload_json = canonical_json(command.payload)
    payload_hash = hash_payload_json(payload_json)

    safety_error = validate_command_type_allowed(command_type, command=command)
    if safety_error is not None:
        return EnqueueCommandResult(
            accepted=False,
            command_id=command.command_id,
            status=GatewayCommandStatus.REJECTED,
            payload_hash=payload_hash,
            error_message=safety_error,
        )

    existing_command = connection.execute(
        "SELECT command_id, status FROM gateway_commands WHERE command_id = ?",
        (command.command_id,),
    ).fetchone()
    if existing_command is not None:
        return EnqueueCommandResult(
            accepted=False,
            command_id=command.command_id,
            status=GatewayCommandStatus(existing_command["status"]),
            payload_hash=payload_hash,
            duplicate=True,
            error_message="command_id already exists",
        )

    now = datetime_to_wire(utc_now())
    expires_at_wire = _optional_timestamp(expires_at)
    if command.idempotency_key is not None:
        dedupe_error = _find_active_dedupe_error(connection, command.idempotency_key)
        if dedupe_error is not None:
            return EnqueueCommandResult(
                accepted=False,
                command_id=command.command_id,
                status=GatewayCommandStatus.REJECTED,
                payload_hash=payload_hash,
                duplicate=True,
                error_message=dedupe_error,
            )

    try:
        connection.execute(
            """
            INSERT INTO gateway_commands (
                command_id,
                command_type,
                source,
                status,
                idempotency_key,
                payload_json,
                payload_hash,
                created_at,
                available_at,
                expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                command.command_id,
                command_type,
                command.source,
                GatewayCommandStatus.QUEUED.value,
                command.idempotency_key,
                payload_json,
                payload_hash,
                datetime_to_wire(command.ts),
                None,
                expires_at_wire,
            ),
        )
        if command.idempotency_key is not None:
            connection.execute(
                """
                INSERT INTO gateway_command_dedupe_keys (
                    idempotency_key,
                    command_id,
                    command_type,
                    created_at,
                    retained_until
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    command.idempotency_key,
                    command.command_id,
                    command_type,
                    now,
                    expires_at_wire,
                ),
            )
        connection.commit()
    except sqlite3.IntegrityError as exc:
        connection.rollback()
        return EnqueueCommandResult(
            accepted=False,
            command_id=command.command_id,
            status=GatewayCommandStatus.REJECTED,
            payload_hash=payload_hash,
            duplicate=True,
            error_message=str(exc),
        )

    return EnqueueCommandResult(
        accepted=True,
        command_id=command.command_id,
        status=GatewayCommandStatus.QUEUED,
        payload_hash=payload_hash,
    )


def poll_commands(
    connection: sqlite3.Connection,
    *,
    limit: int = 20,
    wait_sec: float = 0,
) -> list[GatewayCommand]:
    bounded_limit = min(max(int(limit), 1), 100)
    bounded_wait_sec = min(max(float(wait_sec), 0), 5)
    deadline = time.monotonic() + bounded_wait_sec

    while True:
        try:
            expire_stale_gateway_commands(connection)
            commands = _dispatch_ready_commands(connection, bounded_limit)
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if not _is_database_locked_error(exc):
                raise
            if time.monotonic() >= deadline:
                return []
            time.sleep(min(0.1, max(deadline - time.monotonic(), 0)))
            continue
        if commands or time.monotonic() >= deadline:
            return commands
        time.sleep(min(0.1, max(deadline - time.monotonic(), 0)))


def record_command_event(
    connection: sqlite3.Connection,
    *,
    command_id: str,
    event_type: str,
    payload: dict[str, Any],
    event_id: str | None = None,
    occurred_at: str | None = None,
    commit: bool = True,
) -> GatewayCommandStatus | None:
    payload_json = canonical_json(payload)
    command = connection.execute(
        "SELECT * FROM gateway_commands WHERE command_id = ?",
        (command_id,),
    ).fetchone()
    order_command = bool(
        command is not None and is_order_command_type(command["command_type"])
    )
    next_status = _status_for_command_event(
        event_type,
        order_command=order_command,
    )
    boundary_state = None
    if order_command and command is not None:
        boundary_state = record_order_broker_event(
            connection,
            command,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            occurred_at=occurred_at,
        )
        if next_status in {
            GatewayCommandStatus.GATEWAY_STARTED,
            GatewayCommandStatus.PRE_ACK_RECORDED,
            GatewayCommandStatus.BROKER_ACCEPTED,
            GatewayCommandStatus.CHEJAN_CONFIRMED,
        } and boundary_state is not None:
            next_status = GatewayCommandStatus(boundary_state)
        elif next_status is not None:
            next_status = _effective_order_command_status(
                current_status=str(command["status"]),
                proposed_status=next_status,
            )
    last_error = (
        _extract_error_message(payload) if next_status is GatewayCommandStatus.FAILED else None
    )
    now = occurred_at or datetime_to_wire(utc_now())

    connection.execute(
        """
        INSERT INTO gateway_command_events (
            command_id,
            event_type,
            status,
            payload_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (command_id, event_type, next_status.value if next_status else None, payload_json, now),
    )

    if event_type.strip().lower() == "rate_limited":
        current_status = str(command["status"]) if command is not None else ""
        if order_command and current_status in {
            GatewayCommandStatus.PRE_ACK_RECORDED.value,
            GatewayCommandStatus.BROKER_ACCEPTED.value,
            GatewayCommandStatus.CHEJAN_CONFIRMED.value,
            GatewayCommandStatus.UNCONFIRMED.value,
        }:
            if commit:
                connection.commit()
            return GatewayCommandStatus(current_status)
        wait_time_sec = _extract_wait_time_sec(payload)
        available_at = datetime_to_wire(utc_now() + timedelta(seconds=wait_time_sec))
        connection.execute(
            """
            UPDATE gateway_commands
            SET
                status = ?,
                available_at = ?,
                completed_at = NULL,
                last_error = NULL
            WHERE command_id = ?
            """,
            (GatewayCommandStatus.QUEUED.value, available_at, command_id),
        )
        if commit:
            connection.commit()
        return GatewayCommandStatus.QUEUED

    if next_status is not None:
        completed_at = now if next_status in _COMPLETED_STATUSES else None
        connection.execute(
            """
            UPDATE gateway_commands
            SET
                status = ?,
                completed_at = COALESCE(?, completed_at),
                last_error = COALESCE(?, last_error)
            WHERE command_id = ?
            """,
            (next_status.value, completed_at, last_error, command_id),
        )

    if commit:
        connection.commit()
    return next_status


def get_command_status_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM gateway_commands
        GROUP BY status
        """
    ).fetchall()
    counts = {status.value: 0 for status in GatewayCommandStatus}
    for row in rows:
        counts[row["status"]] = row["count"]
    return counts


def get_command_type_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT LOWER(command_type) AS command_type, COUNT(*) AS count
        FROM gateway_commands
        GROUP BY LOWER(command_type)
        """
    ).fetchall()
    return {str(row["command_type"]): int(row["count"]) for row in rows}


def expire_queued_commands(connection: sqlite3.Connection) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        UPDATE gateway_commands
        SET status = ?, completed_at = ?
        WHERE status = ?
            AND expires_at IS NOT NULL
            AND expires_at <= ?
        """,
        (
            GatewayCommandStatus.EXPIRED.value,
            now,
            GatewayCommandStatus.QUEUED.value,
            now,
        ),
    )
    connection.commit()


def expire_stale_gateway_commands(
    connection: sqlite3.Connection,
    *,
    dispatched_timeout_sec: int = DEFAULT_DISPATCH_TIMEOUT_SEC,
) -> dict[str, int]:
    now_dt = utc_now()
    now = datetime_to_wire(now_dt)
    dispatched_cutoff = datetime_to_wire(
        now_dt - timedelta(seconds=max(int(dispatched_timeout_sec), 1))
    )
    candidate_counts = connection.execute(
        """
        SELECT
            SUM(
                CASE
                    WHEN status = ?
                        AND expires_at IS NOT NULL
                        AND expires_at <= ?
                    THEN 1 ELSE 0
                END
            ) AS expired_queued_count,
            SUM(
                CASE
                    WHEN status = ?
                        AND command_type NOT IN ('send_order', 'cancel_order')
                        AND dispatched_at IS NOT NULL
                        AND dispatched_at <= ?
                    THEN 1 ELSE 0
                END
            ) AS timed_out_dispatched_count,
            SUM(
                CASE
                    WHEN status IN (?, ?, ?, ?)
                        AND command_type IN ('send_order', 'cancel_order')
                        AND dispatched_at IS NOT NULL
                        AND dispatched_at <= ?
                    THEN 1 ELSE 0
                END
            ) AS unconfirmed_order_count
        FROM gateway_commands
        """,
        (
            GatewayCommandStatus.QUEUED.value,
            now,
            GatewayCommandStatus.DISPATCHED.value,
            dispatched_cutoff,
            GatewayCommandStatus.DISPATCHED.value,
            GatewayCommandStatus.CLAIMED.value,
            GatewayCommandStatus.GATEWAY_STARTED.value,
            GatewayCommandStatus.PRE_ACK_RECORDED.value,
            dispatched_cutoff,
        ),
    ).fetchone()
    stale_counts = {
        "expired_queued_count": int(candidate_counts["expired_queued_count"] or 0),
        "timed_out_dispatched_count": int(
            candidate_counts["timed_out_dispatched_count"] or 0
        ),
        "unconfirmed_order_count": int(candidate_counts["unconfirmed_order_count"] or 0),
    }
    if not any(stale_counts.values()):
        return stale_counts

    queued_cursor = connection.execute(
        """
        UPDATE gateway_commands
        SET status = ?, completed_at = ?
        WHERE status = ?
            AND expires_at IS NOT NULL
            AND expires_at <= ?
        """,
        (
            GatewayCommandStatus.EXPIRED.value,
            now,
            GatewayCommandStatus.QUEUED.value,
            now,
        ),
    )
    dispatched_cursor = connection.execute(
        """
        UPDATE gateway_commands
        SET status = ?,
            completed_at = ?,
            last_error = COALESCE(
                last_error,
                'Gateway command dispatch timed out before ack/failure event.'
            )
        WHERE status = ?
            AND command_type NOT IN ('send_order', 'cancel_order')
            AND dispatched_at IS NOT NULL
            AND dispatched_at <= ?
        """,
        (
            GatewayCommandStatus.FAILED.value,
            now,
            GatewayCommandStatus.DISPATCHED.value,
            dispatched_cutoff,
        ),
    )
    stale_order_rows = connection.execute(
        """
        SELECT command_id
        FROM gateway_commands
        WHERE status IN (?, ?, ?, ?)
            AND command_type IN ('send_order', 'cancel_order')
            AND dispatched_at IS NOT NULL
            AND dispatched_at <= ?
        """,
        (
            GatewayCommandStatus.DISPATCHED.value,
            GatewayCommandStatus.CLAIMED.value,
            GatewayCommandStatus.GATEWAY_STARTED.value,
            GatewayCommandStatus.PRE_ACK_RECORDED.value,
            dispatched_cutoff,
        ),
    ).fetchall()
    order_cursor = connection.execute(
        """
        UPDATE gateway_commands
        SET status = ?,
            completed_at = NULL,
            last_error = COALESCE(
                last_error,
                'Gateway order dispatch timed out; reconciliation required.'
            )
        WHERE status IN (?, ?, ?, ?)
            AND command_type IN ('send_order', 'cancel_order')
            AND dispatched_at IS NOT NULL
            AND dispatched_at <= ?
        """,
        (
            GatewayCommandStatus.UNCONFIRMED.value,
            GatewayCommandStatus.DISPATCHED.value,
            GatewayCommandStatus.CLAIMED.value,
            GatewayCommandStatus.GATEWAY_STARTED.value,
            GatewayCommandStatus.PRE_ACK_RECORDED.value,
            dispatched_cutoff,
        ),
    )
    mark_order_commands_unconfirmed(
        connection,
        [str(row["command_id"]) for row in stale_order_rows],
        occurred_at=now,
        reason="Gateway order command timed out before durable broker confirmation.",
    )
    _upsert_command_queue_health(connection, healthy=True)
    connection.commit()
    return {
        "expired_queued_count": max(int(queued_cursor.rowcount or 0), 0),
        "timed_out_dispatched_count": max(int(dispatched_cursor.rowcount or 0), 0),
        "unconfirmed_order_count": max(int(order_cursor.rowcount or 0), 0),
    }


def validate_command_type_allowed(
    command_type: str,
    *,
    command: GatewayCommand | None = None,
) -> str | None:
    normalized = _normalize_command_type(command_type)
    if normalized == "send_order":
        return _validate_live_sim_send_order_allowed(command)
    if normalized == "cancel_order":
        return _validate_live_sim_cancel_order_allowed(command)
    if normalized == "modify_order":
        return "modify_order is disabled for LIVE_SIM"
    if normalized in FORBIDDEN_ORDER_COMMAND_TYPES or "order" in normalized:
        return f"Order command_type is disabled for PR 2B: {command_type}"
    if normalized not in ALLOWED_COMMAND_TYPES:
        return f"Unsupported gateway command_type for PR 2B: {command_type}"
    return None


def _validate_live_sim_send_order_allowed(command: GatewayCommand | None) -> str | None:
    if command is None:
        return "send_order requires LIVE_SIM command envelope validation"
    if command.source.strip().lower() != "live_sim":
        return "send_order disabled except live_sim source"
    if not command.idempotency_key:
        return "send_order LIVE_SIM requires idempotency_key"

    payload = command.payload
    metadata = _mapping_value(payload, "metadata")
    if str(payload.get("mode", payload.get("live_mode", ""))).upper() != "LIVE_SIM":
        return "send_order payload mode must be LIVE_SIM"
    if str(payload.get("live_mode", payload.get("mode", ""))).upper() != "LIVE_SIM":
        return "send_order payload live_mode must be LIVE_SIM"
    if payload.get("idempotency_key") != command.idempotency_key:
        return "send_order payload idempotency_key must match command idempotency_key"
    if str(metadata.get("source", "live_sim")).lower() != "live_sim":
        return "send_order metadata source must be live_sim"
    if metadata.get("live_sim_only") is not True:
        return "send_order metadata.live_sim_only must be true"
    if metadata.get("live_real_allowed") is not False:
        return "send_order metadata.live_real_allowed must be false"
    if not metadata.get("live_sim_intent_id") and not payload.get("live_sim_intent_id"):
        return "send_order requires live_sim_intent_id"
    if metadata.get("idempotency_key") != command.idempotency_key:
        return "send_order metadata idempotency_key must match command idempotency_key"
    if not _is_simulation_like(payload.get("account_mode")):
        return "send_order account_mode must be simulation-like"
    if not _is_simulation_like(payload.get("broker_env")):
        return "send_order broker_env must be simulation-like"
    if not _is_simulation_like(payload.get("server_mode")):
        return "send_order server_mode must be simulation-like"
    side = str(payload.get("side", "")).upper()
    if side == "BUY":
        return None
    if side == "SELL":
        close_only = payload.get("close_only") is True or metadata.get("close_only") is True
        if not close_only:
            return "send_order LIVE_SIM SELL requires close_only=true"
        if payload.get("live_real_allowed") is not False:
            return "send_order SELL requires live_real_allowed=false"
        if payload.get("live_sim_only") is not True:
            return "send_order SELL requires live_sim_only=true"
        if str(payload.get("broker_order_path", "")).upper() != "LIVE_SIM_ONLY":
            return "send_order SELL requires broker_order_path=LIVE_SIM_ONLY"
        if not metadata.get("position_id"):
            return "send_order SELL close-only requires metadata.position_id"
        if not metadata.get("exit_intent_id") and not payload.get("exit_intent_id"):
            return "send_order SELL close-only requires exit_intent_id"
        if str(payload.get("order_type", "")).upper() == "MARKET":
            return "send_order SELL market order disabled by default"
        if str(payload.get("allow_short", "false")).lower() in {"1", "true", "yes", "y"}:
            return "send_order SELL allow_short must be false"
        return None
    return "send_order LIVE_SIM allows BUY or close-only SELL only"


def _validate_live_sim_cancel_order_allowed(command: GatewayCommand | None) -> str | None:
    if command is None:
        return "cancel_order requires LIVE_SIM command envelope validation"
    if command.source.strip().lower() != "live_sim":
        return "cancel_order disabled except live_sim source"
    if not command.idempotency_key:
        return "cancel_order LIVE_SIM requires idempotency_key"

    payload = command.payload
    metadata = _mapping_value(payload, "metadata")
    if str(payload.get("mode", payload.get("live_mode", ""))).upper() != "LIVE_SIM":
        return "cancel_order payload mode must be LIVE_SIM"
    if str(payload.get("live_mode", payload.get("mode", ""))).upper() != "LIVE_SIM":
        return "cancel_order payload live_mode must be LIVE_SIM"
    if payload.get("idempotency_key") != command.idempotency_key:
        return "cancel_order payload idempotency_key must match command idempotency_key"
    if metadata.get("idempotency_key") != command.idempotency_key:
        return "cancel_order metadata idempotency_key must match command idempotency_key"
    if payload.get("live_sim_only") is not True or metadata.get("live_sim_only") is not True:
        return "cancel_order requires live_sim_only=true"
    if payload.get("live_real_allowed") is not False:
        return "cancel_order requires live_real_allowed=false"
    if metadata.get("live_real_allowed") is not False:
        return "cancel_order metadata.live_real_allowed must be false"
    if str(payload.get("broker_order_path", "")).upper() != "LIVE_SIM_ONLY":
        return "cancel_order requires broker_order_path=LIVE_SIM_ONLY"
    if not _is_simulation_like(payload.get("account_mode")):
        return "cancel_order account_mode must be simulation-like"
    if not _is_simulation_like(payload.get("broker_env")):
        return "cancel_order broker_env must be simulation-like"
    if not _is_simulation_like(payload.get("server_mode")):
        return "cancel_order server_mode must be simulation-like"
    if str(payload.get("side", "")).upper() not in {"BUY_CANCEL", "CANCEL_BUY"}:
        return "cancel_order requires side=BUY_CANCEL"
    if not payload.get("original_order_no"):
        return "cancel_order requires original_order_no"
    if not metadata.get("cancel_intent_id"):
        return "cancel_order requires metadata.cancel_intent_id"
    if not metadata.get("original_live_sim_order_id"):
        return "cancel_order requires metadata.original_live_sim_order_id"
    return None


def canonical_json(payload: object) -> str:
    return json.dumps(
        normalize_payload(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def hash_payload_json(payload_json: str) -> str:
    return sha256(payload_json.encode("utf-8")).hexdigest()


def _dispatch_ready_commands(
    connection: sqlite3.Connection,
    limit: int,
) -> list[GatewayCommand]:
    now = datetime_to_wire(utc_now())
    has_expired_queued = _has_expired_queued_command(connection, now)
    has_ready_queued = _has_ready_queued_command(
        connection,
        now,
        order_routing_blocked=False,
    )
    if not has_expired_queued and not has_ready_queued:
        return []

    precheck_order_routing_blocked = _order_broker_boundary_blocks_routing(
        get_order_broker_boundary_status(connection)
    )
    if precheck_order_routing_blocked:
        has_ready_queued = _has_ready_queued_command(
            connection,
            now,
            order_routing_blocked=True,
        )
    if not has_expired_queued and not has_ready_queued:
        return []

    try:
        connection.execute("BEGIN IMMEDIATE")
        order_routing_blocked = _order_broker_boundary_blocks_routing(
            get_order_broker_boundary_status(connection)
        )
        now = datetime_to_wire(utc_now())
        connection.execute(
            """
            UPDATE gateway_commands
            SET status = ?, completed_at = ?
            WHERE status = ?
                AND expires_at IS NOT NULL
                AND expires_at <= ?
            """,
            (
                GatewayCommandStatus.EXPIRED.value,
                now,
                GatewayCommandStatus.QUEUED.value,
                now,
            ),
        )
        rows = connection.execute(
            """
            SELECT
                command_id,
                command_type,
                source,
                idempotency_key,
                payload_json,
                created_at,
                attempts,
                expires_at
            FROM gateway_commands
            WHERE status = ?
                AND (available_at IS NULL OR available_at <= ?)
                AND (expires_at IS NULL OR expires_at > ?)
                AND (
                    ? = 0
                    OR lower(command_type) <> 'send_order'
                )
            ORDER BY
                CASE
                    WHEN command_type IN ('send_order', 'cancel_order') THEN 0
                    WHEN command_type IN ('register_realtime', 'request_tr') THEN 2
                    ELSE 1
                END ASC,
                created_at ASC,
                command_id ASC
            LIMIT ?
            """,
            (
                GatewayCommandStatus.QUEUED.value,
                now,
                now,
                int(order_routing_blocked),
                limit,
            ),
        ).fetchall()
        for row in rows:
            next_status = (
                GatewayCommandStatus.CLAIMED
                if is_order_command_type(row["command_type"])
                else GatewayCommandStatus.DISPATCHED
            )
            connection.execute(
                """
                UPDATE gateway_commands
                SET status = ?,
                    dispatched_at = ?,
                    attempts = attempts + 1
                WHERE command_id = ? AND status = ?
                """,
                (
                    next_status.value,
                    now,
                    row["command_id"],
                    GatewayCommandStatus.QUEUED.value,
                ),
            )
            if next_status is GatewayCommandStatus.CLAIMED:
                command_data = dict(row)
                command_data["attempts"] = int(row["attempts"] or 0) + 1
                record_order_command_claim(
                    connection,
                    command_data,
                    claimed_at=now,
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return [_row_to_gateway_command(row) for row in rows]


def _order_broker_boundary_blocks_routing(status: Mapping[str, Any]) -> bool:
    effective = status.get("effective_block_new_order_routing")
    if isinstance(effective, bool):
        return effective
    return status.get("block_new_order_routing") is True


def _has_expired_queued_command(connection: sqlite3.Connection, now: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM gateway_commands
        WHERE status = ?
            AND expires_at IS NOT NULL
            AND expires_at <= ?
        LIMIT 1
        """,
        (GatewayCommandStatus.QUEUED.value, now),
    ).fetchone()
    return row is not None


def _has_ready_queued_command(
    connection: sqlite3.Connection,
    now: str,
    *,
    order_routing_blocked: bool,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM gateway_commands
        WHERE status = ?
            AND (available_at IS NULL OR available_at <= ?)
            AND (expires_at IS NULL OR expires_at > ?)
            AND (
                ? = 0
                OR lower(command_type) <> 'send_order'
            )
        LIMIT 1
        """,
        (
            GatewayCommandStatus.QUEUED.value,
            now,
            now,
            int(order_routing_blocked),
        ),
    ).fetchone()
    return row is not None


def _row_to_gateway_command(row: sqlite3.Row) -> GatewayCommand:
    payload = json.loads(row["payload_json"])
    if row["expires_at"] is not None and "_gateway_command_expires_at" not in payload:
        payload["_gateway_command_expires_at"] = row["expires_at"]
    return GatewayCommand(
        command_id=row["command_id"],
        command_type=row["command_type"],
        source=row["source"],
        ts=parse_timestamp(row["created_at"], "created_at"),
        payload=payload,
        idempotency_key=row["idempotency_key"],
    )


def _find_active_dedupe_error(connection: sqlite3.Connection, idempotency_key: str) -> str | None:
    row = connection.execute(
        """
        SELECT command_id, retained_until
        FROM gateway_command_dedupe_keys
        WHERE idempotency_key = ?
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        return None

    retained_until = row["retained_until"]
    if retained_until is None or parse_timestamp(retained_until, "retained_until") > utc_now():
        return f"idempotency_key already retained for command_id={row['command_id']}"

    connection.execute(
        "DELETE FROM gateway_command_dedupe_keys WHERE idempotency_key = ?",
        (idempotency_key,),
    )
    return None


def _upsert_command_queue_health(connection: sqlite3.Connection, *, healthy: bool) -> None:
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES ('command_queue_healthy', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        ("true" if healthy else "false", datetime_to_wire(utc_now())),
    )


def _status_for_command_event(
    event_type: str,
    *,
    order_command: bool = False,
) -> GatewayCommandStatus | None:
    normalized = event_type.strip().lower()
    if normalized == "command_started":
        return (
            GatewayCommandStatus.GATEWAY_STARTED
            if order_command
            else GatewayCommandStatus.DISPATCHED
        )
    if normalized == "order_pre_ack" and order_command:
        return GatewayCommandStatus.PRE_ACK_RECORDED
    if normalized == "command_ack":
        return (
            GatewayCommandStatus.BROKER_ACCEPTED
            if order_command
            else GatewayCommandStatus.ACKED
        )
    if normalized == "execution_event":
        return (
            GatewayCommandStatus.CHEJAN_CONFIRMED
            if order_command
            else GatewayCommandStatus.ACKED
        )
    if normalized == "kiwoom_order_chejan" and order_command:
        return GatewayCommandStatus.CHEJAN_CONFIRMED
    if normalized == "order_broker_unconfirmed" and order_command:
        return GatewayCommandStatus.UNCONFIRMED
    if normalized == "command_failed":
        return GatewayCommandStatus.FAILED
    return None


def _effective_order_command_status(
    *,
    current_status: str,
    proposed_status: GatewayCommandStatus,
) -> GatewayCommandStatus:
    current = str(current_status).upper()
    if current == GatewayCommandStatus.CHEJAN_CONFIRMED.value:
        return GatewayCommandStatus.CHEJAN_CONFIRMED
    if proposed_status is GatewayCommandStatus.CHEJAN_CONFIRMED:
        return proposed_status
    if proposed_status is GatewayCommandStatus.BROKER_ACCEPTED:
        return proposed_status
    if current == GatewayCommandStatus.BROKER_ACCEPTED.value:
        return GatewayCommandStatus.BROKER_ACCEPTED
    if proposed_status in {
        GatewayCommandStatus.FAILED,
        GatewayCommandStatus.REJECTED,
        GatewayCommandStatus.EXPIRED,
        GatewayCommandStatus.CANCELLED,
    }:
        return proposed_status
    if current in {
        GatewayCommandStatus.FAILED.value,
        GatewayCommandStatus.REJECTED.value,
        GatewayCommandStatus.EXPIRED.value,
        GatewayCommandStatus.CANCELLED.value,
    }:
        return GatewayCommandStatus(current)
    if current == GatewayCommandStatus.UNCONFIRMED.value and proposed_status in {
        GatewayCommandStatus.CLAIMED,
        GatewayCommandStatus.GATEWAY_STARTED,
        GatewayCommandStatus.PRE_ACK_RECORDED,
    }:
        return GatewayCommandStatus.UNCONFIRMED
    rank = {
        GatewayCommandStatus.CLAIMED: 10,
        GatewayCommandStatus.GATEWAY_STARTED: 20,
        GatewayCommandStatus.PRE_ACK_RECORDED: 30,
        GatewayCommandStatus.UNCONFIRMED: 35,
    }
    try:
        current_enum = GatewayCommandStatus(current)
    except ValueError:
        return proposed_status
    return (
        proposed_status
        if rank.get(proposed_status, 0) >= rank.get(current_enum, 0)
        else current_enum
    )


def _extract_error_message(payload: dict[str, Any]) -> str | None:
    for key in ("error_message", "message", "error"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_wait_time_sec(payload: dict[str, Any]) -> float:
    for key in ("next_available_in_sec", "wait_time_sec", "retry_after_sec"):
        value = payload.get(key)
        try:
            wait_time = float(value)
        except (TypeError, ValueError):
            continue
        return min(max(wait_time, 0.001), 300.0)
    return 1.0


def _optional_timestamp(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return datetime_to_wire(parse_timestamp(value, "timestamp"))


def _normalize_command_type(command_type: str) -> str:
    return command_type.strip().lower()


def _is_database_locked_error(exc: sqlite3.OperationalError) -> bool:
    return "database is locked" in str(exc).lower()


def _mapping_value(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _is_simulation_like(value: object) -> bool:
    return str(value or "").strip().upper() in {
        "SIMULATION",
        "MOCK",
        "PAPER",
        "MOCK_TRADING",
        "LIVE_SIM",
    }


_COMPLETED_STATUSES = {
    GatewayCommandStatus.ACKED,
    GatewayCommandStatus.BROKER_ACCEPTED,
    GatewayCommandStatus.CHEJAN_CONFIRMED,
    GatewayCommandStatus.REJECTED,
    GatewayCommandStatus.FAILED,
    GatewayCommandStatus.EXPIRED,
    GatewayCommandStatus.CANCELLED,
}
