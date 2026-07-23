from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    market_today,
    new_message_id,
    parse_timestamp,
    utc_now,
)

ORDER_COMMAND_TYPES: frozenset[str] = frozenset({"send_order", "cancel_order"})
RESOLUTION_TABLE = "gateway_order_broker_boundary_resolutions"
RESOLVED_BROKER_NOT_REACHED = "RESOLVED_BROKER_NOT_REACHED"
BROKER_NOT_REACHED = "BROKER_NOT_REACHED"
RESOLUTION_ACTION_RESOLVE = "RESOLVE_BROKER_NOT_REACHED"
RESOLUTION_ACTION_REVOKE = "REVOKE"
FENCE_EVENT_TABLE = "gateway_order_broker_boundary_fence_events"
FENCE_ACTION_RELEASE = "RELEASE"
FENCE_ACTION_REINSTATE = "REINSTATE"
FENCE_APPROVAL_CONTRACT = "gateway-order-boundary-fence-approval.v2"
FENCE_EXPECTED_APP_NAME = "suseok-trader-v2"
FENCE_EXPECTED_SCHEMA_VERSION = 63
FENCE_RELEASE_REASON_CODE = "APPROVED_MAINTENANCE_FENCE_RELEASE"
FENCE_REINSTATE_REASON_CODE = "OPERATOR_REINSTATED_MAINTENANCE_FENCE"
_RESOLUTION_MIGRATION_SAVEPOINT = "gateway_order_boundary_resolution_migration"
_RESOLUTION_UPDATE_TRIGGER = "trg_gateway_order_boundary_resolutions_no_update"
_RESOLUTION_DELETE_TRIGGER = "trg_gateway_order_boundary_resolutions_no_delete"
_RESOLUTION_CREATED_INDEX = "idx_gateway_order_boundary_resolutions_created"
_RESOLUTION_REQUEST_INDEX = "uq_gateway_order_boundary_resolutions_request_id"
_RESOLUTION_COMMAND_SEQUENCE_INDEX = "uq_gateway_order_boundary_resolutions_command_sequence"
_FENCE_EVENT_MIGRATION_SAVEPOINT = "gateway_order_boundary_fence_event_migration"
_FENCE_EVENT_UPDATE_TRIGGER = "trg_gateway_order_boundary_fence_events_no_update"
_FENCE_EVENT_DELETE_TRIGGER = "trg_gateway_order_boundary_fence_events_no_delete"
_FENCE_EVENT_CREATED_INDEX = "idx_gateway_order_boundary_fence_events_created"
_FENCE_EVENT_REQUEST_INDEX = "uq_gateway_order_boundary_fence_events_request_id"
_FENCE_EVENT_COMMAND_SEQUENCE_INDEX = "uq_gateway_order_boundary_fence_events_command_sequence"
_FENCE_DATABASE_IDENTITY_CONTRACT = "sqlite-database-instance.v1"
_FENCE_ORDER_COMMAND_TYPES = frozenset({"send_order", "cancel_order", "modify_order"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TRADE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,127}$")
_LONG_DIGIT_RUN_RE = re.compile(r"[0-9]{8,}")
_SEPARATED_DIGIT_RUN_RE = re.compile(r"(?:[0-9][_.:@-]*){8,}")
_RESOLUTION_GATEWAY_QUIESCENCE_SEC = 120.0
_RESOLUTION_MIN_SOURCE_SCHEMA_VERSION = 60
_RESOLUTION_REQUIRED_COLUMNS = frozenset(
    {
        "resolution_id",
        "request_id",
        "request_hash",
        "command_id",
        "sequence_no",
        "action",
        "resolution_type",
        "supersedes_resolution_id",
        "reason_code",
        "evidence_type",
        "evidence_ref",
        "evidence_sha256",
        "operator_id",
        "source_boundary_fingerprint",
        "source_boundary_updated_at",
        "boundary_snapshot_json",
        "created_at",
        "live_sim_only",
        "live_real_allowed",
        "routing_fence_active",
    }
)
_FENCE_EVENT_REQUIRED_COLUMNS = (
    "fence_event_id",
    "request_id",
    "request_hash",
    "command_id",
    "command_alias",
    "sequence_no",
    "action",
    "supersedes_fence_event_id",
    "resolution_id",
    "resolution_request_hash",
    "source_boundary_fingerprint",
    "approval_id",
    "approval_trade_date",
    "approval_sha256",
    "evidence_sha256",
    "database_identity_sha256",
    "expected_app_name",
    "expected_schema_version",
    "expected_gateway_command_total_count",
    "expected_order_command_count",
    "expected_gateway_command_state_fingerprint",
    "reason_code",
    "operator_id",
    "created_at",
    "live_sim_only",
    "live_real_allowed",
)
_FENCE_EVENT_REQUIRED_COLUMN_CONTRACTS = (
    ("fence_event_id", "TEXT", False, None, 1, 0),
    ("request_id", "TEXT", True, None, 0, 0),
    ("request_hash", "TEXT", True, None, 0, 0),
    ("command_id", "TEXT", True, None, 0, 0),
    ("command_alias", "TEXT", True, None, 0, 0),
    ("sequence_no", "INTEGER", True, None, 0, 0),
    ("action", "TEXT", True, None, 0, 0),
    ("supersedes_fence_event_id", "TEXT", False, None, 0, 0),
    ("resolution_id", "TEXT", True, None, 0, 0),
    ("resolution_request_hash", "TEXT", True, None, 0, 0),
    ("source_boundary_fingerprint", "TEXT", True, None, 0, 0),
    ("approval_id", "TEXT", True, None, 0, 0),
    ("approval_trade_date", "TEXT", True, None, 0, 0),
    ("approval_sha256", "TEXT", True, None, 0, 0),
    ("evidence_sha256", "TEXT", True, None, 0, 0),
    ("database_identity_sha256", "TEXT", True, None, 0, 0),
    ("expected_app_name", "TEXT", True, None, 0, 0),
    ("expected_schema_version", "INTEGER", True, None, 0, 0),
    ("expected_gateway_command_total_count", "INTEGER", True, None, 0, 0),
    ("expected_order_command_count", "INTEGER", True, None, 0, 0),
    (
        "expected_gateway_command_state_fingerprint",
        "TEXT",
        True,
        None,
        0,
        0,
    ),
    ("reason_code", "TEXT", True, None, 0, 0),
    ("operator_id", "TEXT", True, None, 0, 0),
    ("created_at", "TEXT", True, None, 0, 0),
    ("live_sim_only", "INTEGER", True, "1", 0, 0),
    ("live_real_allowed", "INTEGER", True, "0", 0, 0),
)
_RESOLUTION_SOURCE_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "app_metadata": frozenset({"key", "value"}),
    "gateway_commands": frozenset(
        {
            "command_id",
            "command_type",
            "status",
            "attempts",
            "dispatched_at",
            "completed_at",
            "expires_at",
        }
    ),
    "gateway_order_broker_boundaries": frozenset(
        {
            "command_id",
            "command_type",
            "source",
            "state",
            "idempotency_key",
            "attempts",
            "code",
            "side",
            "claimed_at",
            "gateway_started_at",
            "pre_ack_recorded_at",
            "broker_accepted_at",
            "chejan_confirmed_at",
            "broker_order_no",
            "broker_result_code",
            "broker_message",
            "unconfirmed_at",
            "created_at",
            "updated_at",
            "live_sim_only",
            "live_real_allowed",
        }
    ),
    "gateway_events": frozenset({"command_id", "event_type"}),
    "gateway_command_events": frozenset({"command_id", "event_type"}),
    "live_sim_orders": frozenset(
        {
            "live_sim_order_id",
            "gateway_command_id",
            "broker_order_no",
            "broker_result_code",
            "broker_acked_at",
            "filled_quantity",
            "status",
            "live_sim_only",
            "live_real_allowed",
        }
    ),
    "live_sim_intents": frozenset(
        {
            "live_sim_intent_id",
            "gateway_command_id",
            "broker_order_sent",
            "live_sim_only",
            "live_real_allowed",
        }
    ),
    "live_sim_executions": frozenset(
        {"live_sim_execution_id", "live_sim_order_id", "live_sim_intent_id"}
    ),
    "runtime_execution_locks": frozenset(),
    "gateway_status": frozenset({"key", "value"}),
}
_BROKER_REACH_EVENT_TYPES: dict[str, str] = {
    "command_started": "GATEWAY_STARTED_EVENT",
    "order_pre_ack": "PRE_ACK_EVENT",
    "command_ack": "COMMAND_ACK_EVENT",
    "order_broker_unconfirmed": "BROKER_CALL_OUTCOME_UNCONFIRMED",
    "execution_event": "EXECUTION_EVENT",
    "kiwoom_order_chejan": "CHEJAN_EVENT",
    "kiwoom_balance_chejan": "BALANCE_CHEJAN_EVENT",
    "kiwoom_special_chejan": "SPECIAL_CHEJAN_EVENT",
    "order_rejected": "ORDER_REJECTED_EVENT",
    "cancel_ack": "CANCEL_ACK_EVENT",
    "cancel_rejected": "CANCEL_REJECTED_EVENT",
}


class OrderBrokerBoundaryState(StrEnum):
    CLAIMED = "CLAIMED"
    GATEWAY_STARTED = "GATEWAY_STARTED"
    PRE_ACK_RECORDED = "PRE_ACK_RECORDED"
    BROKER_ACCEPTED = "BROKER_ACCEPTED"
    CHEJAN_CONFIRMED = "CHEJAN_CONFIRMED"
    UNCONFIRMED = "UNCONFIRMED"


class OrderBrokerBoundaryResolutionError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code)
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "ORDER_BROKER_BOUNDARY_RESOLUTION_REJECTED",
            "code": self.code,
            "message": str(self),
            "details": self.details,
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "raw_boundary_changed": False,
            "live_real_allowed": False,
        }


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
        _ensure_order_broker_boundary_resolution_schema(connection)
        _backfill_order_broker_boundaries(connection)
    except Exception:
        connection.execute(f"ROLLBACK TO {_MIGRATION_SAVEPOINT}")
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")
        raise
    else:
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")


def _ensure_order_broker_boundary_resolution_schema(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(f"SAVEPOINT {_RESOLUTION_MIGRATION_SAVEPOINT}")
    try:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RESOLUTION_TABLE} (
                resolution_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                request_hash TEXT NOT NULL CHECK (length(request_hash) = 64),
                command_id TEXT NOT NULL,
                sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
                action TEXT NOT NULL CHECK (
                    action IN ('RESOLVE_BROKER_NOT_REACHED', 'REVOKE')
                ),
                resolution_type TEXT NOT NULL
                    CHECK (resolution_type = 'BROKER_NOT_REACHED'),
                supersedes_resolution_id TEXT,
                reason_code TEXT NOT NULL,
                evidence_type TEXT NOT NULL,
                evidence_ref TEXT NOT NULL,
                evidence_sha256 TEXT NOT NULL
                    CHECK (length(evidence_sha256) = 64),
                operator_id TEXT NOT NULL,
                source_boundary_fingerprint TEXT NOT NULL
                    CHECK (length(source_boundary_fingerprint) = 64),
                source_boundary_updated_at TEXT NOT NULL,
                boundary_snapshot_json TEXT NOT NULL DEFAULT '{{}}',
                created_at TEXT NOT NULL,
                live_sim_only INTEGER NOT NULL DEFAULT 1
                    CHECK (live_sim_only = 1),
                live_real_allowed INTEGER NOT NULL DEFAULT 0
                    CHECK (live_real_allowed = 0),
                routing_fence_active INTEGER NOT NULL DEFAULT 1
                    CHECK (routing_fence_active = 1),
                FOREIGN KEY (command_id)
                    REFERENCES gateway_order_broker_boundaries (command_id),
                FOREIGN KEY (supersedes_resolution_id)
                    REFERENCES gateway_order_broker_boundary_resolutions (
                        resolution_id
                    )
            )
            """
        )
        existing_columns = {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in connection.execute(f"PRAGMA table_info({RESOLUTION_TABLE})").fetchall()
        }
        missing_columns = _RESOLUTION_REQUIRED_COLUMNS - existing_columns
        if missing_columns:
            raise RuntimeError(
                "gateway order-boundary resolution schema is incomplete: "
                + ",".join(sorted(missing_columns))
            )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_RESOLUTION_CREATED_INDEX}
            ON {RESOLUTION_TABLE} (created_at DESC, resolution_id DESC)
            """
        )
        connection.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_RESOLUTION_REQUEST_INDEX}
            ON {RESOLUTION_TABLE} (request_id)
            """
        )
        connection.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_RESOLUTION_COMMAND_SEQUENCE_INDEX}
            ON {RESOLUTION_TABLE} (command_id, sequence_no)
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {_RESOLUTION_UPDATE_TRIGGER}
            BEFORE UPDATE ON {RESOLUTION_TABLE}
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'gateway order-boundary resolutions are append-only'
                );
            END
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {_RESOLUTION_DELETE_TRIGGER}
            BEFORE DELETE ON {RESOLUTION_TABLE}
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'gateway order-boundary resolutions are append-only'
                );
            END
            """
        )
        schema_status = _resolution_schema_status(connection)
        if not schema_status["ready"]:
            raise RuntimeError("gateway order-boundary resolution schema contract is invalid")
    except Exception:
        connection.execute(f"ROLLBACK TO {_RESOLUTION_MIGRATION_SAVEPOINT}")
        connection.execute(f"RELEASE {_RESOLUTION_MIGRATION_SAVEPOINT}")
        raise
    else:
        connection.execute(f"RELEASE {_RESOLUTION_MIGRATION_SAVEPOINT}")


def _ensure_order_broker_boundary_fence_event_schema(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(f"SAVEPOINT {_FENCE_EVENT_MIGRATION_SAVEPOINT}")
    try:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FENCE_EVENT_TABLE} (
                fence_event_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                request_hash TEXT NOT NULL CHECK (length(request_hash) = 64),
                command_id TEXT NOT NULL,
                command_alias TEXT NOT NULL,
                sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
                action TEXT NOT NULL CHECK (action IN ('RELEASE', 'REINSTATE')),
                supersedes_fence_event_id TEXT,
                resolution_id TEXT NOT NULL,
                resolution_request_hash TEXT NOT NULL
                    CHECK (length(resolution_request_hash) = 64),
                source_boundary_fingerprint TEXT NOT NULL
                    CHECK (length(source_boundary_fingerprint) = 64),
                approval_id TEXT NOT NULL,
                approval_trade_date TEXT NOT NULL
                    CHECK (length(approval_trade_date) = 10),
                approval_sha256 TEXT NOT NULL
                    CHECK (length(approval_sha256) = 64),
                evidence_sha256 TEXT NOT NULL
                    CHECK (length(evidence_sha256) = 64),
                database_identity_sha256 TEXT NOT NULL
                    CHECK (length(database_identity_sha256) = 64),
                expected_app_name TEXT NOT NULL
                    CHECK (expected_app_name = 'suseok-trader-v2'),
                expected_schema_version INTEGER NOT NULL
                    CHECK (expected_schema_version = 63),
                expected_gateway_command_total_count INTEGER NOT NULL
                    CHECK (expected_gateway_command_total_count >= 0),
                expected_order_command_count INTEGER NOT NULL
                    CHECK (
                        expected_order_command_count >= 0
                        AND expected_order_command_count
                            <= expected_gateway_command_total_count
                    ),
                expected_gateway_command_state_fingerprint TEXT NOT NULL
                    CHECK (
                        length(expected_gateway_command_state_fingerprint) = 64
                    ),
                reason_code TEXT NOT NULL,
                operator_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                live_sim_only INTEGER NOT NULL DEFAULT 1
                    CHECK (live_sim_only = 1),
                live_real_allowed INTEGER NOT NULL DEFAULT 0
                    CHECK (live_real_allowed = 0),
                FOREIGN KEY (command_id)
                    REFERENCES gateway_order_broker_boundaries (command_id),
                FOREIGN KEY (resolution_id)
                    REFERENCES gateway_order_broker_boundary_resolutions (
                        resolution_id
                    ),
                FOREIGN KEY (supersedes_fence_event_id)
                    REFERENCES gateway_order_broker_boundary_fence_events (
                        fence_event_id
                    )
            )
            """
        )
        existing_column_rows = connection.execute(
            f"PRAGMA table_xinfo({FENCE_EVENT_TABLE})"
        ).fetchall()
        existing_columns = tuple(
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in existing_column_rows
        )
        existing_hidden = tuple(
            int(row["hidden"] if isinstance(row, sqlite3.Row) else row[6])
            for row in existing_column_rows
        )
        if (
            existing_columns != _FENCE_EVENT_REQUIRED_COLUMNS
            or any(existing_hidden)
        ):
            raise RuntimeError("gateway order-boundary fence-event schema columns are not exact")
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_FENCE_EVENT_CREATED_INDEX}
            ON {FENCE_EVENT_TABLE} (created_at DESC, fence_event_id DESC)
            """
        )
        connection.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_FENCE_EVENT_REQUEST_INDEX}
            ON {FENCE_EVENT_TABLE} (request_id)
            """
        )
        connection.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_FENCE_EVENT_COMMAND_SEQUENCE_INDEX}
            ON {FENCE_EVENT_TABLE} (command_id, sequence_no)
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {_FENCE_EVENT_UPDATE_TRIGGER}
            BEFORE UPDATE ON {FENCE_EVENT_TABLE}
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'gateway order-boundary fence events are append-only'
                );
            END
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {_FENCE_EVENT_DELETE_TRIGGER}
            BEFORE DELETE ON {FENCE_EVENT_TABLE}
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'gateway order-boundary fence events are append-only'
                );
            END
            """
        )
        if not _fence_event_schema_status(connection)["ready"]:
            raise RuntimeError("gateway order-boundary fence-event schema contract is invalid")
    except Exception:
        connection.execute(f"ROLLBACK TO {_FENCE_EVENT_MIGRATION_SAVEPOINT}")
        connection.execute(f"RELEASE {_FENCE_EVENT_MIGRATION_SAVEPOINT}")
        raise
    else:
        connection.execute(f"RELEASE {_FENCE_EVENT_MIGRATION_SAVEPOINT}")


def ensure_gateway_order_broker_boundary_fence_event_schema(
    connection: sqlite3.Connection,
) -> None:
    """Idempotently install the append-only maintenance-fence event ledger."""
    _ensure_order_broker_boundary_fence_event_schema(connection)


def get_order_broker_boundary_fence_approval_binding(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Return the exact database and command state used by a fence approval."""
    metadata_rows = connection.execute(
        """
        SELECT key, value
        FROM app_metadata
        WHERE key IN ('app_name', 'schema_version')
        ORDER BY key
        """
    ).fetchall()
    metadata: dict[str, list[str]] = defaultdict(list)
    for row in metadata_rows:
        key = str(row["key"] if isinstance(row, sqlite3.Row) else row[0])
        value = str(row["value"] if isinstance(row, sqlite3.Row) else row[1])
        metadata[key].append(value)
    app_names = metadata.get("app_name", [])
    schema_versions = metadata.get("schema_version", [])
    app_name = app_names[0] if len(app_names) == 1 else None
    schema_version: int | None = None
    if len(schema_versions) == 1:
        try:
            schema_version = int(schema_versions[0])
        except ValueError:
            schema_version = None
    command_snapshot = _gateway_command_approval_snapshot(connection)
    return {
        "database_identity_sha256": _database_identity_sha256(connection),
        "app_name": app_name,
        "schema_version": schema_version,
        "gateway_commands": command_snapshot,
    }


def build_order_broker_boundary_fence_approval(
    *,
    action: str,
    approval_id: str,
    request_id: str,
    operator_id: str,
    reason_code: str,
    approval_trade_date: str,
    command_alias: str,
    command_id: str,
    expected_previous_fence_event_id: str | None,
    expected_resolution_id: str,
    expected_resolution_request_hash: str,
    expected_source_boundary_fingerprint: str,
    evidence_sha256: str,
    database_identity_sha256: str,
    expected_app_name: str,
    expected_schema_version: int,
    expected_gateway_command_total_count: int,
    expected_order_command_count: int,
    expected_gateway_command_state_fingerprint: str,
) -> dict[str, Any]:
    """Build the sole canonical approval contract accepted by storage apply."""
    normalized_action = str(action).strip().upper()
    if normalized_action not in {
        FENCE_ACTION_RELEASE,
        FENCE_ACTION_REINSTATE,
    }:
        raise OrderBrokerBoundaryResolutionError("INVALID_ACTION", "unsupported fence-event action")
    normalized_reason = _require_safe_identifier("reason_code", reason_code)
    expected_reason = (
        FENCE_RELEASE_REASON_CODE
        if normalized_action == FENCE_ACTION_RELEASE
        else FENCE_REINSTATE_REASON_CODE
    )
    if normalized_reason != expected_reason:
        raise OrderBrokerBoundaryResolutionError(
            "INVALID_REASON_CODE", "reason_code does not match fence action"
        )
    normalized_trade_date = str(approval_trade_date).strip()
    if not _TRADE_DATE_RE.fullmatch(normalized_trade_date):
        raise OrderBrokerBoundaryResolutionError(
            "INVALID_APPROVAL_TRADE_DATE",
            "approval_trade_date must use YYYY-MM-DD",
        )
    normalized_previous = (
        None
        if expected_previous_fence_event_id is None
        else _require_safe_identifier(
            "expected_previous_fence_event_id",
            expected_previous_fence_event_id,
        )
    )
    normalized = {
        "approval_id": _require_safe_identifier("approval_id", approval_id),
        "request_id": _require_safe_identifier("request_id", request_id),
        "operator_id": _require_safe_identifier("operator_id", operator_id),
        "command_alias": _require_safe_identifier("command_alias", command_alias),
        "command_id": str(command_id).strip(),
        "resolution_id": _require_safe_identifier("expected_resolution_id", expected_resolution_id),
        "resolution_request_hash": _require_sha256(
            "expected_resolution_request_hash",
            expected_resolution_request_hash,
        ),
        "source_boundary_fingerprint": _require_sha256(
            "expected_source_boundary_fingerprint",
            expected_source_boundary_fingerprint,
        ),
        "evidence_sha256": _require_sha256("evidence_sha256", evidence_sha256),
        "database_identity_sha256": _require_sha256(
            "database_identity_sha256", database_identity_sha256
        ),
        "gateway_command_state_fingerprint": _require_sha256(
            "expected_gateway_command_state_fingerprint",
            expected_gateway_command_state_fingerprint,
        ),
    }
    if not normalized["command_id"]:
        raise OrderBrokerBoundaryResolutionError("INVALID_COMMAND_ID", "command_id is required")
    for field in (
        "approval_id",
        "request_id",
        "operator_id",
        "command_alias",
        "reason_code",
    ):
        value = normalized_reason if field == "reason_code" else normalized[field]
        if _contains_account_like_digit_sequence(str(value)):
            raise OrderBrokerBoundaryResolutionError(
                f"INVALID_{field.upper()}",
                f"{field} must not contain an account-like digit sequence",
            )
    if str(expected_app_name) != FENCE_EXPECTED_APP_NAME:
        raise OrderBrokerBoundaryResolutionError(
            "APP_METADATA_MISMATCH",
            "fence approval requires the exact application identity",
        )
    try:
        schema_version = int(expected_schema_version)
        total_count = int(expected_gateway_command_total_count)
        order_count = int(expected_order_command_count)
    except (TypeError, ValueError) as exc:
        raise OrderBrokerBoundaryResolutionError(
            "APPROVAL_BINDING_INVALID", "approval binding counts are invalid"
        ) from exc
    if schema_version != FENCE_EXPECTED_SCHEMA_VERSION:
        raise OrderBrokerBoundaryResolutionError(
            "SCHEMA_VERSION_MISMATCH",
            "fence approval requires the exact schema version",
        )
    if total_count < 0 or order_count < 0 or order_count > total_count:
        raise OrderBrokerBoundaryResolutionError(
            "GATEWAY_COMMAND_SNAPSHOT_INVALID",
            "gateway command approval counts are invalid",
        )
    target = {
        "resolution_id": normalized["resolution_id"],
        "resolution_request_hash": normalized["resolution_request_hash"],
        "source_boundary_fingerprint": normalized["source_boundary_fingerprint"],
    }
    if normalized_action == FENCE_ACTION_RELEASE:
        target["expected_previous_fence_event_id"] = normalized_previous
    else:
        if normalized_previous is None:
            raise OrderBrokerBoundaryResolutionError(
                "INVALID_EXPECTED_RELEASE_EVENT_ID",
                "reinstate requires the approved release event",
            )
        target["expected_release_event_id"] = normalized_previous
    payload = {
        "contract": FENCE_APPROVAL_CONTRACT,
        "action": normalized_action,
        "approval_id": normalized["approval_id"],
        "request_id": normalized["request_id"],
        "operator_id": normalized["operator_id"],
        "reason_code": normalized_reason,
        "trade_date": normalized_trade_date,
        "command_alias": normalized["command_alias"],
        "command_id": normalized["command_id"],
        "target": target,
        "evidence_sha256": normalized["evidence_sha256"],
        "database_identity_sha256": normalized["database_identity_sha256"],
        "expected_app_name": FENCE_EXPECTED_APP_NAME,
        "expected_schema_version": FENCE_EXPECTED_SCHEMA_VERSION,
        "gateway_commands": {
            "total_count": total_count,
            "order_count": order_count,
            "state_fingerprint": normalized["gateway_command_state_fingerprint"],
        },
        "one_shot": True,
        "append_only": True,
        "raw_boundary_changed": False,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_only": True,
        "live_real_allowed": False,
    }
    canonical_json = _canonical_json(payload)
    return {
        "payload": payload,
        "canonical_json": canonical_json,
        "sha256": hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
    }


def _gateway_command_approval_snapshot(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    cursor = connection.execute("SELECT * FROM gateway_commands ORDER BY command_id")
    column_names = tuple(str(item[0]) for item in (cursor.description or ()))
    rows = cursor.fetchall()
    state_payload = [
        {
            column: (row[column] if isinstance(row, sqlite3.Row) else row[index])
            for index, column in enumerate(column_names)
        }
        for row in rows
    ]
    order_count = sum(
        1
        for row in state_payload
        if str(row.get("command_type") or "").lower() in _FENCE_ORDER_COMMAND_TYPES
    )
    return {
        "total_count": len(state_payload),
        "order_count": order_count,
        "state_fingerprint": hashlib.sha256(
            _canonical_json({"rows": state_payload}).encode("utf-8")
        ).hexdigest(),
    }


def _database_identity_sha256(connection: sqlite3.Connection) -> str:
    rows = connection.execute("PRAGMA database_list").fetchall()
    main_rows = [
        row
        for row in rows
        if str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) == "main"
    ]
    if len(main_rows) != 1:
        raise OrderBrokerBoundaryResolutionError(
            "DATABASE_IDENTITY_UNAVAILABLE",
            "main database identity is not uniquely available",
        )
    row = main_rows[0]
    raw_path = str(row["file"] if isinstance(row, sqlite3.Row) else row[2])
    if raw_path:
        resolved = Path(raw_path).resolve(strict=True)
        stat_result = os.stat(resolved)
        payload = {
            "contract": _FENCE_DATABASE_IDENTITY_CONTRACT,
            "kind": "file",
            "normalized_path_sha256": hashlib.sha256(
                os.path.normcase(str(resolved)).encode("utf-8")
            ).hexdigest(),
            "device": int(stat_result.st_dev),
            "inode": int(stat_result.st_ino),
        }
    else:
        payload = {
            "contract": _FENCE_DATABASE_IDENTITY_CONTRACT,
            "kind": "memory",
            "connection_identity": f"{id(connection):x}",
        }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _fence_event_insert_authorizer(
    action_code: int,
    parameter_one: str | None,
    _parameter_two: str | None,
    database_name: str | None,
    _trigger_name: str | None,
) -> int:
    if action_code == sqlite3.SQLITE_INSERT:
        if parameter_one == FENCE_EVENT_TABLE and database_name == "main":
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY
    if action_code in {sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


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


def get_effective_order_broker_boundary(
    connection: sqlite3.Connection,
    command_id: str,
    *,
    public: bool = True,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM gateway_order_broker_boundaries
        WHERE command_id = ?
        """,
        (str(command_id),),
    ).fetchone()
    if row is None:
        return None
    return _effective_boundary_from_row(connection, row, public=public)


def list_order_broker_boundaries(
    connection: sqlite3.Connection,
    *,
    state: str | None = None,
    effective_state: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    normalized_effective = None if effective_state is None else str(effective_state).strip().upper()
    valid_effective_states = {
        *(state_item.value for state_item in OrderBrokerBoundaryState),
        RESOLVED_BROKER_NOT_REACHED,
    }
    resolution_table_exists = _table_exists(connection, RESOLUTION_TABLE)
    if normalized_effective is not None and normalized_effective not in valid_effective_states:
        return []
    if state is not None:
        clauses.append("state = ?")
        params.append(str(state).strip().upper())
    unbounded_effective_candidates = normalized_effective in {
        OrderBrokerBoundaryState.UNCONFIRMED.value,
        RESOLVED_BROKER_NOT_REACHED,
    }
    if normalized_effective == RESOLVED_BROKER_NOT_REACHED:
        if not resolution_table_exists:
            return []
        clauses.append("state = ?")
        params.append(OrderBrokerBoundaryState.UNCONFIRMED.value)
        clauses.append(
            f"EXISTS (SELECT 1 FROM {RESOLUTION_TABLE} r "
            "WHERE r.command_id = gateway_order_broker_boundaries.command_id)"
        )
    elif normalized_effective is not None:
        clauses.append("state = ?")
        params.append(normalized_effective)
    normalized_limit = min(max(int(limit), 1), 500)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_sql = "" if unbounded_effective_candidates else "LIMIT ?"
    if limit_sql:
        params.append(normalized_limit)
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_order_broker_boundaries
        {where_sql}
        ORDER BY updated_at DESC, command_id DESC
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()
    resolution_counts: dict[str, int] = {}
    if rows and resolution_table_exists:
        count_rows = connection.execute(
            f"""
            SELECT command_id, COUNT(*) AS count
            FROM {RESOLUTION_TABLE}
            GROUP BY command_id
            """,
        ).fetchall()
        resolution_counts = {str(row["command_id"]): int(row["count"] or 0) for row in count_rows}
    items: list[dict[str, Any]] = []
    for row in rows:
        if resolution_counts.get(str(row["command_id"]), 0):
            items.append(_effective_boundary_from_row(connection, row, public=True))
        else:
            item = _public_boundary_row(row)
            item.update(
                {
                    "raw_state": str(row["state"]),
                    "effective_state": str(row["state"]),
                    "resolution_status": ("NONE" if resolution_table_exists else "LEDGER_INVALID"),
                    "resolution_effective": False,
                    "late_broker_evidence": False,
                    "resolution_event_count": 0,
                    "resolution": None,
                    "resolution_chain_valid": resolution_table_exists,
                    "maintenance_fence_active": True,
                    "maintenance_fence_released": False,
                    "fence_release_status": "NO_ACTIVE_RESOLUTION",
                    "fence_event_count": 0,
                    "fence_event": None,
                    "fence_chain_valid": _fence_event_schema_status(connection)["ready"],
                }
            )
            items.append(item)
    if normalized_effective is None:
        return items
    return [item for item in items if item["effective_state"] == normalized_effective][
        :normalized_limit
    ]


def preview_order_broker_boundary_resolution(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM gateway_order_broker_boundaries
        WHERE command_id = ?
        """,
        (str(command_id),),
    ).fetchone()
    if row is None:
        raise OrderBrokerBoundaryResolutionError(
            "BOUNDARY_NOT_FOUND",
            "order broker-boundary row was not found",
        )
    source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
    projection = _resolution_projection(connection, row)
    resolution_schema = _resolution_schema_status(connection)
    raw = _public_boundary_row(row)
    reason_codes: list[str] = []
    if not resolution_schema["ready"]:
        reason_codes.append("RESOLUTION_SCHEMA_INVALID")
    reason_codes.extend(source_schema_reason_codes)
    if not projection["chain_valid"]:
        reason_codes.append("RESOLUTION_LEDGER_INVALID")
    if str(row["state"]) != OrderBrokerBoundaryState.UNCONFIRMED.value:
        reason_codes.append("RAW_BOUNDARY_NOT_UNCONFIRMED")
    if not bool(row["live_sim_only"]) or bool(row["live_real_allowed"]):
        reason_codes.append("BOUNDARY_NOT_LIVE_SIM_ONLY")
    reason_codes.extend(projection["broker_reach_reason_codes"])
    active_resolution = projection["active_resolution"]
    if active_resolution is not None:
        reason_codes.append("ACTIVE_RESOLUTION_EXISTS")
    reason_codes = list(dict.fromkeys(reason_codes))
    return {
        **raw,
        "raw_state": str(row["state"]),
        "effective_state": projection["effective_state"],
        "source_boundary_fingerprint": projection["fingerprint"],
        "source_boundary_updated_at": str(row["updated_at"]),
        "eligible": not reason_codes,
        "resolution_allowed": not reason_codes,
        "revocation_allowed": bool(
            resolution_schema["ready"]
            and not source_schema_reason_codes
            and projection["chain_valid"]
            and active_resolution is not None
        ),
        "reason_codes": reason_codes,
        "broker_reach_reason_codes": projection["broker_reach_reason_codes"],
        "broker_reach_evidence_count": projection["broker_reach_evidence_count"],
        "resolution_event_count": len(projection["resolution_rows"]),
        "resolution": _public_resolution_row(active_resolution),
        "chain_valid": projection["chain_valid"],
        "resolution_schema_ready": resolution_schema["ready"],
        "source_schema_ready": not source_schema_reason_codes,
        "read_only": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "raw_boundary_changed": False,
        "resolution_live_sim_only": True,
        "resolution_live_real_allowed": False,
    }


def record_order_broker_boundary_resolution(
    connection: sqlite3.Connection,
    *,
    command_id: str,
    request_id: str,
    expected_fingerprint: str,
    reason_code: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    operator_id: str,
) -> dict[str, Any]:
    return _append_order_broker_boundary_resolution(
        connection,
        action=RESOLUTION_ACTION_RESOLVE,
        command_id=command_id,
        request_id=request_id,
        expected_fingerprint=expected_fingerprint,
        reason_code=reason_code,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        operator_id=operator_id,
        supersedes_resolution_id=None,
    )


def revoke_order_broker_boundary_resolution(
    connection: sqlite3.Connection,
    *,
    command_id: str,
    request_id: str,
    expected_fingerprint: str,
    supersedes_resolution_id: str,
    reason_code: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    operator_id: str,
) -> dict[str, Any]:
    return _append_order_broker_boundary_resolution(
        connection,
        action=RESOLUTION_ACTION_REVOKE,
        command_id=command_id,
        request_id=request_id,
        expected_fingerprint=expected_fingerprint,
        reason_code=reason_code,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        operator_id=operator_id,
        supersedes_resolution_id=supersedes_resolution_id,
    )


def preview_order_broker_boundary_fence_release(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM gateway_order_broker_boundaries
        WHERE command_id = ?
        """,
        (str(command_id),),
    ).fetchone()
    if row is None:
        raise OrderBrokerBoundaryResolutionError(
            "BOUNDARY_NOT_FOUND",
            "order broker-boundary row was not found",
        )
    resolution_schema = _resolution_schema_status(connection)
    fence_schema = _fence_event_schema_status(connection)
    source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
    resolution_projection = _resolution_projection(connection, row)
    fence_projection = _fence_event_projection(
        connection,
        row,
        resolution_projection=resolution_projection,
    )
    active_resolution = resolution_projection["active_resolution"]
    reason_codes: list[str] = []
    if not resolution_schema["ready"]:
        reason_codes.append("RESOLUTION_SCHEMA_INVALID")
    if not fence_schema["ready"]:
        reason_codes.append("FENCE_EVENT_SCHEMA_INVALID")
    reason_codes.extend(source_schema_reason_codes)
    if not resolution_projection["chain_valid"]:
        reason_codes.append("RESOLUTION_LEDGER_INVALID")
    if not fence_projection["chain_valid"]:
        reason_codes.append("FENCE_EVENT_LEDGER_INVALID")
    if active_resolution is None:
        reason_codes.append("ACTIVE_RESOLUTION_MISSING")
    if resolution_projection["resolution_status"] != "EFFECTIVE":
        reason_codes.append("ACTIVE_RESOLUTION_NOT_EFFECTIVE")
    reason_codes.extend(resolution_projection["broker_reach_reason_codes"])
    if fence_projection["active_release"] is not None:
        reason_codes.append("ACTIVE_FENCE_RELEASE_EXISTS")
    if not reason_codes:
        reason_codes.extend(
            _resolution_quiescence_reason_codes(
                connection,
                command_id=str(row["command_id"]),
            )
        )
    reason_codes = list(dict.fromkeys(reason_codes))
    latest_event = fence_projection["latest_event"]
    return {
        "command_id": str(row["command_id"]),
        "eligible": not reason_codes,
        "release_allowed": not reason_codes,
        "reason_codes": reason_codes,
        "resolution_id": (
            None if active_resolution is None else str(active_resolution["resolution_id"])
        ),
        "resolution_request_hash": (
            None if active_resolution is None else str(active_resolution["request_hash"])
        ),
        "source_boundary_fingerprint": resolution_projection["fingerprint"],
        "expected_previous_fence_event_id": (
            None if latest_event is None else str(latest_event["fence_event_id"])
        ),
        "resolution_status": resolution_projection["resolution_status"],
        "broker_reach_reason_codes": resolution_projection["broker_reach_reason_codes"],
        "broker_reach_evidence_count": resolution_projection["broker_reach_evidence_count"],
        "fence_event_count": len(fence_projection["event_rows"]),
        "fence_chain_valid": fence_projection["chain_valid"],
        "maintenance_fence_released": fence_projection["released"],
        "maintenance_fence_active": not fence_projection["released"],
        "fence_release_status": fence_projection["status"],
        "fence_release_reason_codes": fence_projection["reason_codes"],
        "read_only": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "raw_boundary_changed": False,
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def preview_order_broker_boundary_fence_reinstate(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (str(command_id),),
    ).fetchone()
    if row is None:
        raise OrderBrokerBoundaryResolutionError(
            "BOUNDARY_NOT_FOUND",
            "order broker-boundary row was not found",
        )
    resolution_projection = _resolution_projection(connection, row)
    fence_projection = _fence_event_projection(
        connection,
        row,
        resolution_projection=resolution_projection,
    )
    active_release = fence_projection["active_release"]
    reason_codes: list[str] = []
    if not _fence_event_schema_status(connection)["ready"]:
        reason_codes.append("FENCE_EVENT_SCHEMA_INVALID")
    if not fence_projection["chain_valid"]:
        reason_codes.append("FENCE_EVENT_LEDGER_INVALID")
    if active_release is None:
        reason_codes.append("ACTIVE_FENCE_RELEASE_MISSING")
    reason_codes = list(dict.fromkeys(reason_codes))
    return {
        "command_id": str(row["command_id"]),
        "eligible": not reason_codes,
        "reinstate_allowed": not reason_codes,
        "reason_codes": reason_codes,
        "expected_release_event_id": (
            None if active_release is None else str(active_release["fence_event_id"])
        ),
        "resolution_id": (None if active_release is None else str(active_release["resolution_id"])),
        "resolution_request_hash": (
            None if active_release is None else str(active_release["resolution_request_hash"])
        ),
        "source_boundary_fingerprint": (
            None if active_release is None else str(active_release["source_boundary_fingerprint"])
        ),
        "maintenance_fence_released": fence_projection["released"],
        "maintenance_fence_active": not fence_projection["released"],
        "fence_release_status": fence_projection["status"],
        "read_only": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "raw_boundary_changed": False,
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def release_order_broker_boundary_maintenance_fence(
    connection: sqlite3.Connection,
    *,
    command_id: str,
    request_id: str,
    expected_previous_fence_event_id: str | None,
    expected_resolution_id: str,
    expected_resolution_request_hash: str,
    expected_source_boundary_fingerprint: str,
    approval_id: str,
    command_alias: str,
    approval_trade_date: str,
    approval_sha256: str,
    evidence_sha256: str,
    database_identity_sha256: str,
    expected_app_name: str,
    expected_schema_version: int,
    expected_gateway_command_total_count: int,
    expected_order_command_count: int,
    expected_gateway_command_state_fingerprint: str,
    reason_code: str,
    operator_id: str,
) -> dict[str, Any]:
    return _append_order_broker_boundary_fence_event(
        connection,
        action=FENCE_ACTION_RELEASE,
        command_id=command_id,
        request_id=request_id,
        expected_previous_fence_event_id=expected_previous_fence_event_id,
        expected_resolution_id=expected_resolution_id,
        expected_resolution_request_hash=expected_resolution_request_hash,
        expected_source_boundary_fingerprint=(expected_source_boundary_fingerprint),
        approval_id=approval_id,
        command_alias=command_alias,
        approval_trade_date=approval_trade_date,
        approval_sha256=approval_sha256,
        evidence_sha256=evidence_sha256,
        database_identity_sha256=database_identity_sha256,
        expected_app_name=expected_app_name,
        expected_schema_version=expected_schema_version,
        expected_gateway_command_total_count=(expected_gateway_command_total_count),
        expected_order_command_count=expected_order_command_count,
        expected_gateway_command_state_fingerprint=(expected_gateway_command_state_fingerprint),
        reason_code=reason_code,
        operator_id=operator_id,
    )


def reinstate_order_broker_boundary_maintenance_fence(
    connection: sqlite3.Connection,
    *,
    command_id: str,
    request_id: str,
    expected_release_event_id: str,
    expected_resolution_id: str,
    expected_resolution_request_hash: str,
    expected_source_boundary_fingerprint: str,
    approval_id: str,
    command_alias: str,
    approval_trade_date: str,
    approval_sha256: str,
    evidence_sha256: str,
    database_identity_sha256: str,
    expected_app_name: str,
    expected_schema_version: int,
    expected_gateway_command_total_count: int,
    expected_order_command_count: int,
    expected_gateway_command_state_fingerprint: str,
    reason_code: str,
    operator_id: str,
) -> dict[str, Any]:
    return _append_order_broker_boundary_fence_event(
        connection,
        action=FENCE_ACTION_REINSTATE,
        command_id=command_id,
        request_id=request_id,
        expected_previous_fence_event_id=expected_release_event_id,
        expected_resolution_id=expected_resolution_id,
        expected_resolution_request_hash=expected_resolution_request_hash,
        expected_source_boundary_fingerprint=(expected_source_boundary_fingerprint),
        approval_id=approval_id,
        command_alias=command_alias,
        approval_trade_date=approval_trade_date,
        approval_sha256=approval_sha256,
        evidence_sha256=evidence_sha256,
        database_identity_sha256=database_identity_sha256,
        expected_app_name=expected_app_name,
        expected_schema_version=expected_schema_version,
        expected_gateway_command_total_count=(expected_gateway_command_total_count),
        expected_order_command_count=expected_order_command_count,
        expected_gateway_command_state_fingerprint=(expected_gateway_command_state_fingerprint),
        reason_code=reason_code,
        operator_id=operator_id,
    )


def _append_order_broker_boundary_fence_event(
    connection: sqlite3.Connection,
    *,
    action: str,
    command_id: str,
    request_id: str,
    expected_previous_fence_event_id: str | None,
    expected_resolution_id: str,
    expected_resolution_request_hash: str,
    expected_source_boundary_fingerprint: str,
    approval_id: str,
    command_alias: str,
    approval_trade_date: str,
    approval_sha256: str,
    evidence_sha256: str,
    database_identity_sha256: str,
    expected_app_name: str,
    expected_schema_version: int,
    expected_gateway_command_total_count: int,
    expected_order_command_count: int,
    expected_gateway_command_state_fingerprint: str,
    reason_code: str,
    operator_id: str,
) -> dict[str, Any]:
    normalized = {
        "action": str(action),
        "command_id": str(command_id).strip(),
        "request_id": _require_safe_identifier("request_id", request_id),
        "expected_previous_fence_event_id": (
            None
            if expected_previous_fence_event_id is None
            else _require_safe_identifier(
                "expected_previous_fence_event_id",
                expected_previous_fence_event_id,
            )
        ),
        "expected_resolution_id": _require_safe_identifier(
            "expected_resolution_id", expected_resolution_id
        ),
        "expected_resolution_request_hash": _require_sha256(
            "expected_resolution_request_hash",
            expected_resolution_request_hash,
        ),
        "expected_source_boundary_fingerprint": _require_sha256(
            "expected_source_boundary_fingerprint",
            expected_source_boundary_fingerprint,
        ),
        "approval_id": _require_safe_identifier("approval_id", approval_id),
        "command_alias": _require_safe_identifier("command_alias", command_alias),
        "approval_trade_date": str(approval_trade_date).strip(),
        "approval_sha256": _require_sha256("approval_sha256", approval_sha256),
        "evidence_sha256": _require_sha256("evidence_sha256", evidence_sha256),
        "database_identity_sha256": _require_sha256(
            "database_identity_sha256", database_identity_sha256
        ),
        "expected_app_name": str(expected_app_name),
        "expected_schema_version": int(expected_schema_version),
        "expected_gateway_command_total_count": int(expected_gateway_command_total_count),
        "expected_order_command_count": int(expected_order_command_count),
        "expected_gateway_command_state_fingerprint": _require_sha256(
            "expected_gateway_command_state_fingerprint",
            expected_gateway_command_state_fingerprint,
        ),
        "reason_code": _require_safe_identifier("reason_code", reason_code),
        "operator_id": _require_safe_identifier("operator_id", operator_id),
    }
    if not normalized["command_id"]:
        raise OrderBrokerBoundaryResolutionError("INVALID_COMMAND_ID", "command_id is required")
    for field in (
        "request_id",
        "approval_id",
        "command_alias",
        "reason_code",
        "operator_id",
    ):
        if _contains_account_like_digit_sequence(str(normalized[field])):
            raise OrderBrokerBoundaryResolutionError(
                f"INVALID_{field.upper()}",
                f"{field} must not contain an account-like digit sequence",
            )
    approval = build_order_broker_boundary_fence_approval(
        action=action,
        approval_id=normalized["approval_id"],
        request_id=normalized["request_id"],
        operator_id=normalized["operator_id"],
        reason_code=normalized["reason_code"],
        approval_trade_date=normalized["approval_trade_date"],
        command_alias=normalized["command_alias"],
        command_id=normalized["command_id"],
        expected_previous_fence_event_id=normalized["expected_previous_fence_event_id"],
        expected_resolution_id=normalized["expected_resolution_id"],
        expected_resolution_request_hash=normalized["expected_resolution_request_hash"],
        expected_source_boundary_fingerprint=normalized["expected_source_boundary_fingerprint"],
        evidence_sha256=normalized["evidence_sha256"],
        database_identity_sha256=normalized["database_identity_sha256"],
        expected_app_name=normalized["expected_app_name"],
        expected_schema_version=normalized["expected_schema_version"],
        expected_gateway_command_total_count=normalized["expected_gateway_command_total_count"],
        expected_order_command_count=normalized["expected_order_command_count"],
        expected_gateway_command_state_fingerprint=normalized[
            "expected_gateway_command_state_fingerprint"
        ],
    )
    if str(approval["sha256"]) != normalized["approval_sha256"]:
        raise OrderBrokerBoundaryResolutionError(
            "APPROVAL_SHA256_MISMATCH",
            "approval_sha256 does not match the canonical storage contract",
        )
    request_hash = hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()
    if connection.in_transaction:
        raise OrderBrokerBoundaryResolutionError(
            "ACTIVE_TRANSACTION",
            "fence event requires a connection without an active transaction",
        )

    transaction_started = False
    try:
        connection.execute("BEGIN IMMEDIATE")
        transaction_started = True
        fence_schema = _fence_event_schema_status(connection)
        if not fence_schema["ready"]:
            raise OrderBrokerBoundaryResolutionError(
                "FENCE_EVENT_SCHEMA_INVALID",
                "fence-event schema is not append-only safe",
                details=fence_schema,
            )
        resolution_schema = _resolution_schema_status(connection)
        if not resolution_schema["ready"]:
            raise OrderBrokerBoundaryResolutionError(
                "RESOLUTION_SCHEMA_INVALID",
                "resolution schema is not append-only safe",
                details=resolution_schema,
            )
        existing = connection.execute(
            f"SELECT * FROM {FENCE_EVENT_TABLE} WHERE request_id = ?",
            (normalized["request_id"],),
        ).fetchone()
        if existing is not None:
            if str(existing["request_hash"]) != request_hash:
                raise OrderBrokerBoundaryResolutionError(
                    "REQUEST_CONFLICT",
                    "request_id was already used for a different request",
                )
            boundary = connection.execute(
                "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
                (normalized["command_id"],),
            ).fetchone()
            projection = None if boundary is None else _fence_event_projection(connection, boundary)
            connection.commit()
            transaction_started = False
            public_existing = _public_fence_event_row(existing)
            assert public_existing is not None
            replay_is_latest = bool(
                projection is not None
                and projection["latest_event"] is not None
                and str(projection["latest_event"]["fence_event_id"])
                == str(existing["fence_event_id"])
            )
            replay_is_effective = bool(
                replay_is_latest
                and (
                    (
                        action == FENCE_ACTION_RELEASE
                        and projection is not None
                        and projection["active_release"] is not None
                        and projection["released"]
                    )
                    or (
                        action == FENCE_ACTION_REINSTATE
                        and projection is not None
                        and projection["active_release"] is None
                        and projection["chain_valid"]
                        and not projection["released"]
                    )
                )
            )
            return {
                **public_existing,
                "idempotent_replay": True,
                "idempotent_replay_effective": replay_is_effective,
                "maintenance_fence_released": bool(
                    projection is not None and projection["released"]
                ),
                "maintenance_fence_active": not bool(
                    projection is not None and projection["released"]
                ),
                "no_order_commands_created": True,
                "no_broker_calls": True,
                "raw_boundary_changed": False,
                "routing_safety_state_changed": False,
                "live_sim_only": True,
                "live_real_allowed": False,
            }

        current_binding = get_order_broker_boundary_fence_approval_binding(connection)
        binding_reason_codes: list[str] = []
        if current_binding["database_identity_sha256"] != normalized["database_identity_sha256"]:
            binding_reason_codes.append("DATABASE_IDENTITY_MISMATCH")
        if current_binding["app_name"] != normalized["expected_app_name"]:
            binding_reason_codes.append("APP_NAME_MISMATCH")
        if current_binding["schema_version"] != normalized["expected_schema_version"]:
            binding_reason_codes.append("SCHEMA_VERSION_MISMATCH")
        current_commands = current_binding["gateway_commands"]
        if (
            int(current_commands["total_count"])
            != normalized["expected_gateway_command_total_count"]
        ):
            binding_reason_codes.append("GATEWAY_COMMAND_TOTAL_COUNT_MISMATCH")
        if int(current_commands["order_count"]) != normalized["expected_order_command_count"]:
            binding_reason_codes.append("ORDER_COMMAND_COUNT_MISMATCH")
        if (
            str(current_commands["state_fingerprint"])
            != normalized["expected_gateway_command_state_fingerprint"]
        ):
            binding_reason_codes.append("GATEWAY_COMMAND_STATE_FINGERPRINT_MISMATCH")
        if normalized["approval_trade_date"] != market_today():
            binding_reason_codes.append("APPROVAL_TRADE_DATE_EXPIRED")
        if binding_reason_codes:
            raise OrderBrokerBoundaryResolutionError(
                "APPROVAL_BINDING_STALE",
                "approved database state changed before fence apply",
                details={"reason_codes": binding_reason_codes},
            )
        if action == FENCE_ACTION_RELEASE:
            source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
            if source_schema_reason_codes:
                raise OrderBrokerBoundaryResolutionError(
                    "RESOLUTION_SOURCE_SCHEMA_INVALID",
                    "release evidence or quiescence schema is incomplete",
                    details={"reason_codes": source_schema_reason_codes},
                )

        row = connection.execute(
            "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
            (normalized["command_id"],),
        ).fetchone()
        if row is None:
            raise OrderBrokerBoundaryResolutionError(
                "BOUNDARY_NOT_FOUND",
                "order broker-boundary row was not found",
            )
        resolution_projection = _resolution_projection(connection, row)
        fence_projection = _fence_event_projection(
            connection,
            row,
            resolution_projection=resolution_projection,
        )
        if not fence_projection["chain_valid"]:
            raise OrderBrokerBoundaryResolutionError(
                "FENCE_EVENT_LEDGER_INVALID",
                "fence-event ledger is not a valid append-only chain",
            )
        latest_event = fence_projection["latest_event"]
        current_previous_id = None if latest_event is None else str(latest_event["fence_event_id"])
        if normalized["expected_previous_fence_event_id"] != current_previous_id:
            raise OrderBrokerBoundaryResolutionError(
                "STALE_FENCE_EVENT_CAS",
                "fence-event chain changed after preview",
                details={"current_previous_fence_event_id": current_previous_id},
            )

        if action == FENCE_ACTION_RELEASE:
            if not resolution_projection["chain_valid"]:
                raise OrderBrokerBoundaryResolutionError(
                    "RESOLUTION_LEDGER_INVALID",
                    "resolution ledger is not a valid append-only chain",
                )
            active_resolution = resolution_projection["active_resolution"]
            rejection_codes: list[str] = []
            if active_resolution is None:
                rejection_codes.append("ACTIVE_RESOLUTION_MISSING")
            if resolution_projection["resolution_status"] != "EFFECTIVE":
                rejection_codes.append("ACTIVE_RESOLUTION_NOT_EFFECTIVE")
            rejection_codes.extend(resolution_projection["broker_reach_reason_codes"])
            if fence_projection["active_release"] is not None:
                rejection_codes.append("ACTIVE_FENCE_RELEASE_EXISTS")
            if active_resolution is not None:
                if str(active_resolution["resolution_id"]) != normalized["expected_resolution_id"]:
                    rejection_codes.append("RESOLUTION_ID_MISMATCH")
                if (
                    str(active_resolution["request_hash"])
                    != normalized["expected_resolution_request_hash"]
                ):
                    rejection_codes.append("RESOLUTION_REQUEST_HASH_MISMATCH")
                if (
                    str(active_resolution["source_boundary_fingerprint"])
                    != normalized["expected_source_boundary_fingerprint"]
                ):
                    rejection_codes.append("RESOLUTION_FINGERPRINT_MISMATCH")
            if (
                resolution_projection["fingerprint"]
                != normalized["expected_source_boundary_fingerprint"]
            ):
                rejection_codes.append("SOURCE_BOUNDARY_FINGERPRINT_MISMATCH")
            rejection_codes.extend(
                _resolution_quiescence_reason_codes(
                    connection,
                    command_id=normalized["command_id"],
                )
            )
            if rejection_codes:
                raise OrderBrokerBoundaryResolutionError(
                    "FENCE_RELEASE_NOT_PROVABLE",
                    "maintenance-fence release was rejected fail-closed",
                    details={"reason_codes": list(dict.fromkeys(rejection_codes))},
                )
        elif action == FENCE_ACTION_REINSTATE:
            active_release = fence_projection["active_release"]
            if active_release is None:
                raise OrderBrokerBoundaryResolutionError(
                    "ACTIVE_FENCE_RELEASE_MISSING",
                    "there is no active fence release to reinstate",
                )
            binding_mismatches = []
            for row_field, expected_field, reason_code_value in (
                (
                    "resolution_id",
                    "expected_resolution_id",
                    "RESOLUTION_ID_MISMATCH",
                ),
                (
                    "resolution_request_hash",
                    "expected_resolution_request_hash",
                    "RESOLUTION_REQUEST_HASH_MISMATCH",
                ),
                (
                    "source_boundary_fingerprint",
                    "expected_source_boundary_fingerprint",
                    "SOURCE_BOUNDARY_FINGERPRINT_MISMATCH",
                ),
            ):
                if str(active_release[row_field]) != normalized[expected_field]:
                    binding_mismatches.append(reason_code_value)
            if binding_mismatches:
                raise OrderBrokerBoundaryResolutionError(
                    "FENCE_REINSTATE_BINDING_MISMATCH",
                    "reinstate must bind the active fence release",
                    details={"reason_codes": binding_mismatches},
                )
        else:
            raise OrderBrokerBoundaryResolutionError(
                "INVALID_ACTION", "unsupported fence-event action"
            )

        before_status = get_order_broker_boundary_status(connection)
        fence_event_id = new_message_id("boundary_fence_event")
        created_at = datetime_to_wire(utc_now())
        sequence_no = len(fence_projection["event_rows"]) + 1
        connection.set_authorizer(_fence_event_insert_authorizer)
        try:
            connection.execute(
                f"""
                INSERT INTO {FENCE_EVENT_TABLE} (
                    fence_event_id, request_id, request_hash, command_id,
                    command_alias, sequence_no, action,
                    supersedes_fence_event_id, resolution_id,
                    resolution_request_hash, source_boundary_fingerprint,
                    approval_id, approval_trade_date, approval_sha256,
                    evidence_sha256, database_identity_sha256,
                    expected_app_name, expected_schema_version,
                    expected_gateway_command_total_count,
                    expected_order_command_count,
                    expected_gateway_command_state_fingerprint,
                    reason_code, operator_id, created_at,
                    live_sim_only, live_real_allowed
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, 1, 0
                )
                """,
                (
                    fence_event_id,
                    normalized["request_id"],
                    request_hash,
                    normalized["command_id"],
                    normalized["command_alias"],
                    sequence_no,
                    action,
                    normalized["expected_previous_fence_event_id"],
                    normalized["expected_resolution_id"],
                    normalized["expected_resolution_request_hash"],
                    normalized["expected_source_boundary_fingerprint"],
                    normalized["approval_id"],
                    normalized["approval_trade_date"],
                    normalized["approval_sha256"],
                    normalized["evidence_sha256"],
                    normalized["database_identity_sha256"],
                    normalized["expected_app_name"],
                    normalized["expected_schema_version"],
                    normalized["expected_gateway_command_total_count"],
                    normalized["expected_order_command_count"],
                    normalized["expected_gateway_command_state_fingerprint"],
                    normalized["reason_code"],
                    normalized["operator_id"],
                    created_at,
                ),
            )
        finally:
            connection.set_authorizer(None)
        inserted = connection.execute(
            f"SELECT * FROM {FENCE_EVENT_TABLE} WHERE fence_event_id = ?",
            (fence_event_id,),
        ).fetchone()
        after_status = get_order_broker_boundary_status(connection)
        after_projection = _fence_event_projection(connection, row)
        assert inserted is not None
        public_inserted = _public_fence_event_row(inserted)
        assert public_inserted is not None
        response = {
            **public_inserted,
            "idempotent_replay": False,
            "idempotent_replay_effective": False,
            "maintenance_fence_released": after_projection["released"],
            "maintenance_fence_active": not after_projection["released"],
            "fence_release_status": after_projection["status"],
            "effective_block_new_order_routing": after_status["effective_block_new_order_routing"],
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "raw_boundary_changed": False,
            "routing_safety_state_changed": bool(
                before_status["effective_block_new_order_routing"]
                != after_status["effective_block_new_order_routing"]
            ),
            "live_sim_only": True,
            "live_real_allowed": False,
        }
        connection.commit()
        transaction_started = False
        return response
    except OrderBrokerBoundaryResolutionError:
        if transaction_started:
            connection.rollback()
        raise
    except sqlite3.DatabaseError as exc:
        if transaction_started:
            connection.rollback()
        if "locked" in str(exc).lower():
            raise OrderBrokerBoundaryResolutionError(
                "LOCKED_RETRYABLE",
                "database is locked; retry the same request_id",
            ) from exc
        raise OrderBrokerBoundaryResolutionError(
            "DATABASE_OPERATION_FAILED",
            "database operation failed without applying the fence event",
        ) from exc
    except Exception:
        if transaction_started:
            connection.rollback()
        raise


def _append_order_broker_boundary_resolution(
    connection: sqlite3.Connection,
    *,
    action: str,
    command_id: str,
    request_id: str,
    expected_fingerprint: str,
    reason_code: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    operator_id: str,
    supersedes_resolution_id: str | None,
) -> dict[str, Any]:
    normalized = {
        "action": str(action),
        "command_id": str(command_id).strip(),
        "request_id": _require_safe_identifier("request_id", request_id),
        "expected_fingerprint": _require_sha256("expected_fingerprint", expected_fingerprint),
        "reason_code": _require_safe_identifier("reason_code", reason_code),
        "evidence_type": _require_safe_identifier("evidence_type", evidence_type),
        "evidence_ref": _require_safe_identifier("evidence_ref", evidence_ref),
        "evidence_sha256": _require_sha256("evidence_sha256", evidence_sha256),
        "operator_id": _require_safe_identifier("operator_id", operator_id),
        "supersedes_resolution_id": (
            None
            if supersedes_resolution_id is None
            else _require_safe_identifier("supersedes_resolution_id", supersedes_resolution_id)
        ),
    }
    for field in (
        "request_id",
        "reason_code",
        "evidence_type",
        "evidence_ref",
        "operator_id",
    ):
        if _contains_account_like_digit_sequence(str(normalized[field])):
            raise OrderBrokerBoundaryResolutionError(
                f"INVALID_{field.upper()}",
                f"{field} must not contain an account-like digit sequence",
            )
    if not normalized["command_id"]:
        raise OrderBrokerBoundaryResolutionError("INVALID_COMMAND_ID", "command_id is required")
    request_hash = hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()
    if connection.in_transaction:
        raise OrderBrokerBoundaryResolutionError(
            "ACTIVE_TRANSACTION",
            "resolution requires a connection without an active transaction",
        )

    transaction_started = False
    try:
        connection.execute("BEGIN IMMEDIATE")
        transaction_started = True
        resolution_schema = _resolution_schema_status(connection)
        if not resolution_schema["ready"]:
            raise OrderBrokerBoundaryResolutionError(
                "RESOLUTION_SCHEMA_INVALID",
                "resolution schema is not append-only safe",
                details=resolution_schema,
            )
        source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
        if source_schema_reason_codes:
            raise OrderBrokerBoundaryResolutionError(
                "RESOLUTION_SOURCE_SCHEMA_INVALID",
                "resolution evidence or quiescence schema is incomplete",
                details={"reason_codes": source_schema_reason_codes},
            )
        existing = connection.execute(
            f"SELECT * FROM {RESOLUTION_TABLE} WHERE request_id = ?",
            (normalized["request_id"],),
        ).fetchone()
        if existing is not None:
            if str(existing["request_hash"]) != request_hash:
                raise OrderBrokerBoundaryResolutionError(
                    "REQUEST_CONFLICT",
                    "request_id was already used for a different request",
                )
            effective = get_effective_order_broker_boundary(connection, normalized["command_id"])
            latest = connection.execute(
                f"""
                SELECT resolution_id, action
                FROM {RESOLUTION_TABLE}
                WHERE command_id = ?
                ORDER BY sequence_no DESC, created_at DESC, resolution_id DESC
                LIMIT 1
                """,
                (normalized["command_id"],),
            ).fetchone()
            replay_is_current = bool(
                effective is not None
                and latest is not None
                and str(latest["resolution_id"]) == str(existing["resolution_id"])
                and (
                    (
                        action == RESOLUTION_ACTION_RESOLVE
                        and effective["resolution_status"] == "EFFECTIVE"
                    )
                    or (
                        action == RESOLUTION_ACTION_REVOKE
                        and effective["resolution_status"] == "REVOKED"
                    )
                )
            )
            connection.commit()
            transaction_started = False
            public_existing = _public_resolution_row(existing)
            assert public_existing is not None
            return {
                **public_existing,
                "idempotent_replay": True,
                "idempotent_replay_effective": replay_is_current,
                "effective_state": (None if effective is None else effective["effective_state"]),
                "resolution_status": (
                    "MISSING_BOUNDARY" if effective is None else effective["resolution_status"]
                ),
                "no_order_commands_created": True,
                "no_broker_calls": True,
                "raw_boundary_changed": False,
                "routing_safety_state_changed": False,
                "live_sim_only": True,
                "live_real_allowed": False,
            }

        row = connection.execute(
            """
            SELECT *
            FROM gateway_order_broker_boundaries
            WHERE command_id = ?
            """,
            (normalized["command_id"],),
        ).fetchone()
        if row is None:
            raise OrderBrokerBoundaryResolutionError(
                "BOUNDARY_NOT_FOUND",
                "order broker-boundary row was not found",
            )
        projection = _resolution_projection(connection, row)
        if not projection["chain_valid"]:
            raise OrderBrokerBoundaryResolutionError(
                "RESOLUTION_LEDGER_INVALID",
                "resolution ledger is not a valid append-only chain",
            )
        if projection["fingerprint"] != normalized["expected_fingerprint"]:
            raise OrderBrokerBoundaryResolutionError(
                "STALE_BOUNDARY_FINGERPRINT",
                "order broker-boundary safety evidence changed",
                details={"current_fingerprint": projection["fingerprint"]},
            )

        active_resolution = projection["active_resolution"]
        if action == RESOLUTION_ACTION_RESOLVE:
            if active_resolution is not None:
                raise OrderBrokerBoundaryResolutionError(
                    "ACTIVE_RESOLUTION_EXISTS",
                    "an active resolution already exists",
                )
            quiescence_reasons = _resolution_quiescence_reason_codes(
                connection,
                command_id=normalized["command_id"],
            )
            if quiescence_reasons:
                raise OrderBrokerBoundaryResolutionError(
                    "RUNTIME_NOT_QUIESCENT",
                    "resolution requires a quiescent Core/Gateway database",
                    details={"reason_codes": quiescence_reasons},
                )
            rejection_codes: list[str] = []
            if str(row["state"]) != OrderBrokerBoundaryState.UNCONFIRMED.value:
                rejection_codes.append("RAW_BOUNDARY_NOT_UNCONFIRMED")
            if not bool(row["live_sim_only"]) or bool(row["live_real_allowed"]):
                rejection_codes.append("BOUNDARY_NOT_LIVE_SIM_ONLY")
            rejection_codes.extend(projection["broker_reach_reason_codes"])
            if rejection_codes:
                raise OrderBrokerBoundaryResolutionError(
                    "BROKER_NOT_REACHED_NOT_PROVABLE",
                    "broker-not-reached resolution was rejected",
                    details={"reason_codes": list(dict.fromkeys(rejection_codes))},
                )
        elif action == RESOLUTION_ACTION_REVOKE:
            if active_resolution is None:
                raise OrderBrokerBoundaryResolutionError(
                    "NO_ACTIVE_RESOLUTION",
                    "there is no active resolution to revoke",
                )
            if normalized["supersedes_resolution_id"] != str(active_resolution["resolution_id"]):
                raise OrderBrokerBoundaryResolutionError(
                    "SUPERSEDES_RESOLUTION_MISMATCH",
                    "revoke must supersede the active resolution",
                )
        else:
            raise OrderBrokerBoundaryResolutionError(
                "INVALID_ACTION", "unsupported resolution action"
            )

        before_status = get_order_broker_boundary_status(connection)
        resolution_id = new_message_id("boundary_resolution")
        created_at = datetime_to_wire(utc_now())
        sequence_no = len(projection["resolution_rows"]) + 1
        connection.execute(
            f"""
            INSERT INTO {RESOLUTION_TABLE} (
                resolution_id,
                request_id,
                request_hash,
                command_id,
                sequence_no,
                action,
                resolution_type,
                supersedes_resolution_id,
                reason_code,
                evidence_type,
                evidence_ref,
                evidence_sha256,
                operator_id,
                source_boundary_fingerprint,
                source_boundary_updated_at,
                boundary_snapshot_json,
                created_at,
                live_sim_only,
                live_real_allowed,
                routing_fence_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 1)
            """,
            (
                resolution_id,
                normalized["request_id"],
                request_hash,
                normalized["command_id"],
                sequence_no,
                action,
                BROKER_NOT_REACHED,
                normalized["supersedes_resolution_id"],
                normalized["reason_code"],
                normalized["evidence_type"],
                normalized["evidence_ref"],
                normalized["evidence_sha256"],
                normalized["operator_id"],
                projection["fingerprint"],
                str(row["updated_at"]),
                _canonical_json(projection["snapshot"]),
                created_at,
            ),
        )
        inserted = connection.execute(
            f"SELECT * FROM {RESOLUTION_TABLE} WHERE resolution_id = ?",
            (resolution_id,),
        ).fetchone()
        after_status = get_order_broker_boundary_status(connection)
        effective = get_effective_order_broker_boundary(connection, normalized["command_id"])
        assert inserted is not None
        assert effective is not None
        public_inserted = _public_resolution_row(inserted)
        assert public_inserted is not None
        response = {
            **public_inserted,
            "idempotent_replay": False,
            "idempotent_replay_effective": False,
            "effective_state": effective["effective_state"],
            "resolution_status": effective["resolution_status"],
            "effective_block_new_order_routing": after_status["effective_block_new_order_routing"],
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "raw_boundary_changed": False,
            "routing_safety_state_changed": bool(
                before_status["effective_block_new_order_routing"]
                != after_status["effective_block_new_order_routing"]
            ),
            "live_sim_only": True,
            "live_real_allowed": False,
        }
        connection.commit()
        transaction_started = False
        return response
    except OrderBrokerBoundaryResolutionError:
        if transaction_started:
            connection.rollback()
        raise
    except sqlite3.OperationalError as exc:
        if transaction_started:
            connection.rollback()
        if "locked" in str(exc).lower():
            raise OrderBrokerBoundaryResolutionError(
                "LOCKED_RETRYABLE",
                "database is locked; retry the same request_id",
            ) from exc
        raise OrderBrokerBoundaryResolutionError(
            "DATABASE_OPERATION_FAILED",
            "database operation failed without applying the resolution",
        ) from exc
    except Exception:
        if transaction_started:
            connection.rollback()
        raise


def get_order_broker_boundary_status(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    table_exists = _table_exists(connection, "gateway_order_broker_boundaries")
    raw_required_indexes = {
        "uq_gateway_order_boundary_idempotency",
        "idx_gateway_order_boundary_state_updated",
        "idx_gateway_events_command_event",
        "idx_gateway_command_events_command_event",
    }
    existing_indexes = _index_names(connection)
    if not table_exists:
        return {
            "status": "FAIL",
            "raw_status": "FAIL",
            "effective_status": "FAIL",
            "fast_0_status": "BLOCKED",
            "reason_codes": ["ORDER_BROKER_BOUNDARY_TABLE_MISSING"],
            "warning_codes": [],
            "table_exists": False,
            "required_indexes_present": False,
            "state_counts": {},
            "raw_state_counts": {},
            "effective_state_counts": {},
            "unconfirmed_count": 0,
            "raw_unconfirmed_count": 0,
            "effective_unconfirmed_count": 0,
            "effective_resolution_count": 0,
            "invalidated_resolution_count": 0,
            "resolution_maintenance_fence_active_count": 0,
            "resolution_maintenance_fence_active": False,
            "resolution_maintenance_fence_released_count": 0,
            "resolution_event_count": 0,
            "resolution_schema_ready": False,
            "resolution_source_schema_ready": False,
            "resolution_source_schema_reason_codes": ["RESOLUTION_SOURCE_SCHEMA_UNAVAILABLE"],
            "fence_event_count": 0,
            "fence_event_schema_ready": False,
            "fence_event_table_exists": False,
            "fence_event_required_indexes_present": False,
            "fence_event_append_only_triggers_present": False,
            "active_fence_release_count": 0,
            "invalid_fence_event_chain_count": 0,
            "invalidated_fence_release_count": 0,
            "block_new_order_routing": True,
            "raw_block_new_order_routing": True,
            "qualification_block_new_order_routing": True,
            "effective_block_new_order_routing": True,
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
            (SELECT COUNT(*) FROM gateway_commands
             WHERE lower(command_type) IN (
                 'send_order', 'cancel_order', 'modify_order'
             )
               AND upper(status) IN (
                   'QUEUED', 'CLAIMED', 'DISPATCHED', 'GATEWAY_STARTED',
                   'PRE_ACK_RECORDED'
               )) AS active_order_command_count,
            (SELECT COUNT(*) FROM gateway_commands
             WHERE lower(command_type) IN (
                 'send_order', 'cancel_order', 'modify_order'
             )
               AND upper(status) NOT IN (
                   'QUEUED', 'DISPATCHED', 'CLAIMED', 'GATEWAY_STARTED',
                   'PRE_ACK_RECORDED', 'BROKER_ACCEPTED',
                   'CHEJAN_CONFIRMED', 'UNCONFIRMED', 'ACKED', 'REJECTED',
                   'FAILED', 'EXPIRED', 'CANCELLED'
               )) AS unknown_command_status_count,
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
            (SELECT COUNT(*)
             FROM gateway_order_broker_boundaries b
             LEFT JOIN gateway_commands c ON c.command_id = b.command_id
             WHERE b.pre_ack_recorded_at IS NULL
               AND (
                   b.state IN (
                       'PRE_ACK_RECORDED', 'BROKER_ACCEPTED',
                       'CHEJAN_CONFIRMED'
                   )
                   OR upper(c.status) IN (
                       'PRE_ACK_RECORDED', 'BROKER_ACCEPTED',
                       'CHEJAN_CONFIRMED', 'ACKED'
                   )
               ))
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
             WHERE CASE upper(c.status)
                       WHEN 'DISPATCHED' THEN 'CLAIMED'
                       WHEN 'ACKED' THEN 'BROKER_ACCEPTED'
                       WHEN 'CLAIMED' THEN 'CLAIMED'
                       WHEN 'GATEWAY_STARTED' THEN 'GATEWAY_STARTED'
                       WHEN 'PRE_ACK_RECORDED' THEN 'PRE_ACK_RECORDED'
                       WHEN 'BROKER_ACCEPTED' THEN 'BROKER_ACCEPTED'
                       WHEN 'CHEJAN_CONFIRMED' THEN 'CHEJAN_CONFIRMED'
                       WHEN 'UNCONFIRMED' THEN 'UNCONFIRMED'
                       ELSE NULL
                   END IS NOT NULL
               AND CASE upper(c.status)
                       WHEN 'DISPATCHED' THEN 'CLAIMED'
                       WHEN 'ACKED' THEN 'BROKER_ACCEPTED'
                       ELSE upper(c.status)
                   END <> upper(b.state))
                AS command_state_mismatch_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries
              WHERE state NOT IN (
                  'CLAIMED', 'GATEWAY_STARTED', 'PRE_ACK_RECORDED',
                  'BROKER_ACCEPTED', 'CHEJAN_CONFIRMED', 'UNCONFIRMED'
              )) AS unknown_state_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries b
              LEFT JOIN gateway_commands c ON c.command_id = b.command_id
              WHERE c.command_id IS NULL) AS orphan_boundary_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries b
              LEFT JOIN expected e ON e.command_id = b.command_id
              WHERE e.command_id IS NULL) AS unexpected_boundary_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries b
              JOIN gateway_commands c ON c.command_id = b.command_id
              WHERE lower(c.command_type) NOT IN ('send_order', 'cancel_order'))
                AS linked_command_type_invalid_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries b
              JOIN gateway_commands c ON c.command_id = b.command_id
              WHERE lower(b.command_type) <> lower(c.command_type))
                AS linked_command_type_mismatch_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries
              WHERE lower(command_type) NOT IN ('send_order', 'cancel_order'))
                AS invalid_command_type_count
            ,(SELECT COUNT(*)
              FROM gateway_order_broker_boundaries
              WHERE live_sim_only <> 1 OR live_real_allowed <> 0)
                AS invalid_scope_count
        """
    ).fetchone()
    reason_codes: list[str] = []
    warning_codes: list[str] = []
    resolution_schema = _resolution_schema_status(connection)
    fence_event_schema = _fence_event_schema_status(connection)
    resolution_source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
    required_indexes_present = bool(
        raw_required_indexes.issubset(existing_indexes)
        and resolution_schema["required_indexes_present"]
        and fence_event_schema["required_indexes_present"]
    )
    if not required_indexes_present:
        reason_codes.append("ORDER_BROKER_BOUNDARY_INDEX_MISSING")
    if not resolution_schema["ready"]:
        reason_codes.append("ORDER_BROKER_BOUNDARY_RESOLUTION_SCHEMA_INVALID")
    if not fence_event_schema["ready"]:
        reason_codes.append("ORDER_BROKER_BOUNDARY_FENCE_EVENT_SCHEMA_INVALID")
    if resolution_source_schema_reason_codes:
        reason_codes.append("ORDER_BROKER_BOUNDARY_RESOLUTION_SOURCE_SCHEMA_INVALID")
        reason_codes.extend(resolution_source_schema_reason_codes)
    for field, reason in (
        ("missing_boundary_count", "ORDER_BROKER_BOUNDARY_ROW_MISSING"),
        ("durable_pre_ack_gap_count", "DURABLE_PRE_ACK_GAP"),
        ("duplicate_idempotency_count", "ORDER_BOUNDARY_IDEMPOTENCY_DUPLICATE"),
        ("command_state_mismatch_count", "ORDER_COMMAND_BOUNDARY_STATE_MISMATCH"),
        ("unknown_state_count", "ORDER_BROKER_BOUNDARY_STATE_INVALID"),
        ("orphan_boundary_count", "ORDER_BROKER_BOUNDARY_ORPHAN"),
        ("unexpected_boundary_count", "ORDER_BROKER_BOUNDARY_UNEXPECTED"),
        (
            "linked_command_type_invalid_count",
            "ORDER_BOUNDARY_LINKED_COMMAND_TYPE_INVALID",
        ),
        (
            "linked_command_type_mismatch_count",
            "ORDER_BOUNDARY_COMMAND_TYPE_MISMATCH",
        ),
        (
            "unknown_command_status_count",
            "ORDER_BOUNDARY_COMMAND_STATUS_INVALID",
        ),
        ("invalid_command_type_count", "ORDER_BOUNDARY_COMMAND_TYPE_INVALID"),
        ("invalid_scope_count", "ORDER_BOUNDARY_SCOPE_INVALID"),
    ):
        if int(counts[field] or 0) > 0:
            reason_codes.append(reason)
    unconfirmed_count = int(state_counts.get(OrderBrokerBoundaryState.UNCONFIRMED.value, 0))
    if unconfirmed_count > 0:
        warning_codes.append("UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE")

    effective_state_counts = dict(state_counts)
    effective_state_counts[RESOLVED_BROKER_NOT_REACHED] = 0
    effective_resolution_count = 0
    invalidated_resolution_count = 0
    maintenance_fence_active_count = 0
    maintenance_fence_released_count = 0
    invalid_resolution_chain_count = 0
    resolution_event_count = 0
    fence_event_count = 0
    active_fence_release_count = 0
    invalid_fence_event_chain_count = 0
    invalidated_fence_release_count = 0
    if resolution_schema["table_exists"]:
        resolution_event_count = int(
            connection.execute(f"SELECT COUNT(*) AS count FROM {RESOLUTION_TABLE}").fetchone()[
                "count"
            ]
            or 0
        )
    if fence_event_schema["table_exists"]:
        fence_event_count = int(
            connection.execute(f"SELECT COUNT(*) AS count FROM {FENCE_EVENT_TABLE}").fetchone()[
                "count"
            ]
            or 0
        )
    if resolution_schema["ready"] and not resolution_source_schema_reason_codes:
        resolution_rows = connection.execute(
            f"""
            SELECT *
            FROM {RESOLUTION_TABLE}
            ORDER BY command_id, sequence_no, created_at, resolution_id
            """
        ).fetchall()
        rows_by_command: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for resolution_row in resolution_rows:
            rows_by_command[str(resolution_row["command_id"])].append(resolution_row)
        boundary_rows = connection.execute(
            f"""
            SELECT b.*
            FROM gateway_order_broker_boundaries b
            WHERE EXISTS (
                SELECT 1 FROM {RESOLUTION_TABLE} r
                WHERE r.command_id = b.command_id
            )
            """
        ).fetchall()
        for boundary_row in boundary_rows:
            command_resolutions = rows_by_command.get(str(boundary_row["command_id"]), [])
            chain_valid, active_resolution = _validate_resolution_chain(command_resolutions)
            state = str(boundary_row["state"])
            if command_resolutions and not chain_valid:
                invalid_resolution_chain_count += 1
            elif active_resolution is not None:
                projection = _resolution_projection(connection, boundary_row)
                state = str(projection["effective_state"])
            resolution_projection = _resolution_projection(connection, boundary_row)
            fence_projection = _fence_event_projection(
                connection,
                boundary_row,
                resolution_projection=resolution_projection,
            )
            if fence_projection["event_rows"] and not fence_projection["chain_valid"]:
                invalid_fence_event_chain_count += 1
            if fence_projection["active_release"] is not None:
                active_fence_release_count += 1
                if fence_projection["released"]:
                    maintenance_fence_released_count += 1
                else:
                    invalidated_fence_release_count += 1
            if state == RESOLVED_BROKER_NOT_REACHED:
                raw_state = str(boundary_row["state"])
                effective_state_counts[raw_state] = max(
                    effective_state_counts.get(raw_state, 0) - 1,
                    0,
                )
                effective_state_counts[state] = effective_state_counts.get(state, 0) + 1
                effective_resolution_count += 1
            elif active_resolution is not None:
                invalidated_resolution_count += 1
            if (
                active_resolution is not None
                and bool(active_resolution["routing_fence_active"])
                and not fence_projection["released"]
            ):
                maintenance_fence_active_count += 1
            elif (
                active_resolution is None
                and fence_projection["active_release"] is not None
                and not fence_projection["released"]
            ):
                maintenance_fence_active_count += 1
        orphan_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {RESOLUTION_TABLE} r
                LEFT JOIN gateway_order_broker_boundaries b
                  ON b.command_id = r.command_id
                WHERE b.command_id IS NULL
                """
            ).fetchone()["count"]
            or 0
        )
        if orphan_count:
            reason_codes.append("ORDER_BOUNDARY_RESOLUTION_ORPHAN")
        fence_orphan_count = (
            int(
                connection.execute(
                    f"""
                SELECT COUNT(*) AS count
                FROM {FENCE_EVENT_TABLE} f
                LEFT JOIN gateway_order_broker_boundaries b
                  ON b.command_id = f.command_id
                LEFT JOIN {RESOLUTION_TABLE} r
                  ON r.resolution_id = f.resolution_id
                WHERE b.command_id IS NULL OR r.resolution_id IS NULL
                """
                ).fetchone()["count"]
                or 0
            )
            if fence_event_schema["table_exists"]
            else 0
        )
        if fence_orphan_count:
            reason_codes.append("ORDER_BOUNDARY_FENCE_EVENT_ORPHAN")
    else:
        effective_state_counts = dict(state_counts)
        effective_state_counts[RESOLVED_BROKER_NOT_REACHED] = 0

    if invalid_resolution_chain_count:
        reason_codes.append("ORDER_BOUNDARY_RESOLUTION_CHAIN_INVALID")
    if invalid_fence_event_chain_count:
        reason_codes.append("ORDER_BOUNDARY_FENCE_EVENT_CHAIN_INVALID")
    reason_codes = list(dict.fromkeys(reason_codes))
    effective_unconfirmed_count = int(
        effective_state_counts.get(OrderBrokerBoundaryState.UNCONFIRMED.value, 0)
    )
    if effective_unconfirmed_count > 0:
        warning_codes.append("EFFECTIVE_ORDER_BOUNDARY_REQUIRES_RESOLUTION")
    if invalidated_resolution_count > 0:
        warning_codes.append("ORDER_BOUNDARY_RESOLUTION_INVALIDATED")
    if invalidated_fence_release_count > 0:
        warning_codes.append("ORDER_BOUNDARY_FENCE_RELEASE_INVALIDATED")
    active_order_command_count = int(counts["active_order_command_count"] or 0)
    if active_order_command_count > 0:
        warning_codes.append("ACTIVE_ORDER_COMMANDS_PRESENT")
    warning_codes = list(dict.fromkeys(warning_codes))

    raw_status = "FAIL" if reason_codes else "WARN" if unconfirmed_count else "PASS"
    effective_status = (
        "FAIL"
        if reason_codes
        else "WARN"
        if (
            effective_unconfirmed_count
            or invalidated_resolution_count
            or active_order_command_count
        )
        else "PASS"
    )
    raw_block = bool(reason_codes or unconfirmed_count)
    qualification_block = bool(
        reason_codes
        or effective_unconfirmed_count
        or invalidated_resolution_count
        or active_order_command_count
    )
    maintenance_fence_active = maintenance_fence_active_count > 0
    effective_block = bool(
        reason_codes
        or effective_unconfirmed_count
        or invalidated_resolution_count
        or maintenance_fence_active
    )
    return {
        "status": raw_status,
        "raw_status": raw_status,
        "effective_status": effective_status,
        "fast_0_status": "BLOCKED" if qualification_block else "CLEAR",
        "reason_codes": reason_codes,
        "warning_codes": warning_codes,
        "table_exists": True,
        "required_indexes_present": required_indexes_present,
        "resolution_schema_ready": resolution_schema["ready"],
        "resolution_source_schema_ready": not (resolution_source_schema_reason_codes),
        "resolution_source_schema_reason_codes": (resolution_source_schema_reason_codes),
        "resolution_table_exists": resolution_schema["table_exists"],
        "resolution_required_indexes_present": resolution_schema["required_indexes_present"],
        "resolution_append_only_triggers_present": resolution_schema[
            "append_only_triggers_present"
        ],
        "order_command_count": int(counts["order_command_count"] or 0),
        "active_order_command_count": active_order_command_count,
        "unknown_command_status_count": int(counts["unknown_command_status_count"] or 0),
        "expected_boundary_count": int(counts["expected_boundary_count"] or 0),
        "boundary_count": int(counts["boundary_count"] or 0),
        "missing_boundary_count": int(counts["missing_boundary_count"] or 0),
        "durable_pre_ack_count": int(counts["durable_pre_ack_count"] or 0),
        "durable_pre_ack_gap_count": int(counts["durable_pre_ack_gap_count"] or 0),
        "duplicate_idempotency_count": int(counts["duplicate_idempotency_count"] or 0),
        "command_state_mismatch_count": int(counts["command_state_mismatch_count"] or 0),
        "unknown_state_count": int(counts["unknown_state_count"] or 0),
        "orphan_boundary_count": int(counts["orphan_boundary_count"] or 0),
        "unexpected_boundary_count": int(counts["unexpected_boundary_count"] or 0),
        "linked_command_type_invalid_count": int(counts["linked_command_type_invalid_count"] or 0),
        "linked_command_type_mismatch_count": int(
            counts["linked_command_type_mismatch_count"] or 0
        ),
        "invalid_command_type_count": int(counts["invalid_command_type_count"] or 0),
        "invalid_scope_count": int(counts["invalid_scope_count"] or 0),
        "unconfirmed_count": unconfirmed_count,
        "raw_unconfirmed_count": unconfirmed_count,
        "effective_unconfirmed_count": effective_unconfirmed_count,
        "state_counts": state_counts,
        "raw_state_counts": dict(state_counts),
        "effective_state_counts": effective_state_counts,
        "effective_resolution_count": effective_resolution_count,
        "invalidated_resolution_count": invalidated_resolution_count,
        "resolution_maintenance_fence_active_count": (maintenance_fence_active_count),
        "resolution_maintenance_fence_active": maintenance_fence_active,
        "resolution_maintenance_fence_released_count": (maintenance_fence_released_count),
        "invalid_resolution_chain_count": invalid_resolution_chain_count,
        "resolution_event_count": resolution_event_count,
        "fence_event_count": fence_event_count,
        "fence_event_schema_ready": fence_event_schema["ready"],
        "fence_event_table_exists": fence_event_schema["table_exists"],
        "fence_event_required_indexes_present": fence_event_schema["required_indexes_present"],
        "fence_event_append_only_triggers_present": fence_event_schema[
            "append_only_triggers_present"
        ],
        "active_fence_release_count": active_fence_release_count,
        "invalid_fence_event_chain_count": invalid_fence_event_chain_count,
        "invalidated_fence_release_count": invalidated_fence_release_count,
        "block_new_order_routing": raw_block,
        "raw_block_new_order_routing": raw_block,
        "qualification_block_new_order_routing": qualification_block,
        "effective_block_new_order_routing": effective_block,
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
    created_at = occurred_at if existing is None else str(existing["created_at"] or occurred_at)
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
            occurred_at if state == OrderBrokerBoundaryState.CHEJAN_CONFIRMED.value else None
        ),
        "unconfirmed_at": (
            occurred_at if state == OrderBrokerBoundaryState.UNCONFIRMED.value else None
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


def _public_boundary_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "command_id": str(row["command_id"]),
        "command_type": str(row["command_type"]),
        "source": str(row["source"]),
        "state": str(row["state"]),
        "attempts": int(row["attempts"] or 0),
        "code": _optional_text(row["code"]),
        "side": _optional_text(row["side"]),
        "claimed_at": _optional_text(row["claimed_at"]),
        "gateway_started_at": _optional_text(row["gateway_started_at"]),
        "pre_ack_recorded_at": _optional_text(row["pre_ack_recorded_at"]),
        "broker_accepted_at": _optional_text(row["broker_accepted_at"]),
        "chejan_confirmed_at": _optional_text(row["chejan_confirmed_at"]),
        "unconfirmed_at": _optional_text(row["unconfirmed_at"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
        "broker_order_no_present": bool(_optional_text(row["broker_order_no"])),
        "broker_result_code_present": bool(_optional_text(row["broker_result_code"])),
        "broker_message_present": bool(_optional_text(row["broker_message"])),
        "durable_pre_ack_recorded": bool(row["pre_ack_recorded_at"]),
        "broker_boundary_reached": str(row["state"]) in _DURABLE_PRE_ACK_STATES,
        "live_sim_only": bool(row["live_sim_only"]),
        "live_real_allowed": bool(row["live_real_allowed"]),
    }


def _effective_boundary_from_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    public: bool,
) -> dict[str, Any]:
    projection = _resolution_projection(connection, row)
    fence_projection = _fence_event_projection(
        connection,
        row,
        resolution_projection=projection,
    )
    item = _public_boundary_row(row) if public else _boundary_row_to_dict(row)
    item.update(
        {
            "raw_state": str(row["state"]),
            "effective_state": projection["effective_state"],
            "resolution_status": projection["resolution_status"],
            "resolution_effective": (projection["resolution_status"] == "EFFECTIVE"),
            "late_broker_evidence": bool(
                projection["active_resolution"] is not None
                and projection["broker_reach_reason_codes"]
            ),
            "resolution_event_count": len(projection["resolution_rows"]),
            "resolution": _public_resolution_row(projection["active_resolution"]),
            "resolution_chain_valid": projection["chain_valid"],
            "broker_reach_evidence_count": projection["broker_reach_evidence_count"],
            "broker_reach_reason_codes": projection["broker_reach_reason_codes"],
            "maintenance_fence_active": not fence_projection["released"],
            "maintenance_fence_released": fence_projection["released"],
            "fence_release_status": fence_projection["status"],
            "fence_release_reason_codes": fence_projection["reason_codes"],
            "fence_event_count": len(fence_projection["event_rows"]),
            "fence_event": _public_fence_event_row(fence_projection["active_release"]),
            "fence_chain_valid": fence_projection["chain_valid"],
        }
    )
    return item


def _resolution_projection(
    connection: sqlite3.Connection,
    boundary_row: sqlite3.Row,
) -> dict[str, Any]:
    command_id = str(boundary_row["command_id"])
    snapshot, reason_codes, evidence_count = _collect_resolution_snapshot(connection, boundary_row)
    fingerprint = hashlib.sha256(_canonical_json(snapshot).encode("utf-8")).hexdigest()
    table_exists = _table_exists(connection, RESOLUTION_TABLE)
    resolution_rows = (
        connection.execute(
            f"""
            SELECT *
            FROM {RESOLUTION_TABLE}
            WHERE command_id = ?
            ORDER BY sequence_no, created_at, resolution_id
            """,
            (command_id,),
        ).fetchall()
        if table_exists
        else []
    )
    chain_valid, active_resolution = _validate_resolution_chain(resolution_rows)
    if not table_exists:
        chain_valid = False
    raw_state = str(boundary_row["state"])
    effective_state = raw_state
    resolution_status = "NONE"
    if not chain_valid:
        resolution_status = "LEDGER_INVALID"
    elif active_resolution is not None:
        if raw_state != OrderBrokerBoundaryState.UNCONFIRMED.value:
            resolution_status = "OVERRIDDEN_BY_RAW_STATE"
        elif reason_codes:
            resolution_status = "OVERRIDDEN_BY_BROKER_EVIDENCE"
        elif str(active_resolution["source_boundary_fingerprint"]) != fingerprint:
            resolution_status = "OVERRIDDEN_BY_FINGERPRINT_CHANGE"
        else:
            effective_state = RESOLVED_BROKER_NOT_REACHED
            resolution_status = "EFFECTIVE"
    elif resolution_rows:
        resolution_status = "REVOKED"
    return {
        "snapshot": snapshot,
        "fingerprint": fingerprint,
        "broker_reach_reason_codes": reason_codes,
        "broker_reach_evidence_count": evidence_count,
        "resolution_rows": resolution_rows,
        "chain_valid": chain_valid,
        "active_resolution": active_resolution,
        "effective_state": effective_state,
        "resolution_status": resolution_status,
    }


def _fence_event_projection(
    connection: sqlite3.Connection,
    boundary_row: sqlite3.Row,
    *,
    resolution_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    command_id = str(boundary_row["command_id"])
    table_exists = _table_exists(connection, FENCE_EVENT_TABLE)
    event_rows = (
        connection.execute(
            f"""
            SELECT *
            FROM {FENCE_EVENT_TABLE}
            WHERE command_id = ?
            ORDER BY sequence_no, created_at, fence_event_id
            """,
            (command_id,),
        ).fetchall()
        if table_exists
        else []
    )
    chain_valid, active_release = _validate_fence_event_chain(event_rows)
    if not table_exists:
        chain_valid = False
    latest_event = event_rows[-1] if event_rows else None
    projection = (
        _resolution_projection(connection, boundary_row)
        if resolution_projection is None
        else resolution_projection
    )
    reason_codes: list[str] = []
    if not _fence_event_schema_status(connection)["ready"]:
        reason_codes.append("FENCE_EVENT_SCHEMA_INVALID")
    if not chain_valid:
        reason_codes.append("FENCE_EVENT_LEDGER_INVALID")
    if active_release is None:
        status = "REINSTATED" if event_rows else "NOT_RELEASED"
    else:
        if str(active_release["approval_trade_date"]) != market_today():
            reason_codes.append("FENCE_RELEASE_TRADE_DATE_EXPIRED")
        try:
            current_binding = get_order_broker_boundary_fence_approval_binding(connection)
        except (OSError, sqlite3.Error, OrderBrokerBoundaryResolutionError):
            reason_codes.append("DATABASE_IDENTITY_UNAVAILABLE")
        else:
            if str(active_release["database_identity_sha256"]) != str(
                current_binding["database_identity_sha256"]
            ):
                reason_codes.append("DATABASE_IDENTITY_MISMATCH")
            if str(active_release["expected_app_name"]) != str(current_binding["app_name"]):
                reason_codes.append("APP_NAME_MISMATCH")
            if int(active_release["expected_schema_version"]) != int(
                current_binding["schema_version"] or -1
            ):
                reason_codes.append("SCHEMA_VERSION_MISMATCH")
        active_resolution = projection["active_resolution"]
        if not projection["chain_valid"]:
            reason_codes.append("RESOLUTION_LEDGER_INVALID")
        if projection["resolution_status"] != "EFFECTIVE":
            reason_codes.append("ACTIVE_RESOLUTION_NOT_EFFECTIVE")
        reason_codes.extend(projection["broker_reach_reason_codes"])
        if active_resolution is None:
            reason_codes.append("ACTIVE_RESOLUTION_MISSING")
        else:
            if str(active_release["resolution_id"]) != str(active_resolution["resolution_id"]):
                reason_codes.append("RESOLUTION_ID_MISMATCH")
            if str(active_release["resolution_request_hash"]) != str(
                active_resolution["request_hash"]
            ):
                reason_codes.append("RESOLUTION_REQUEST_HASH_MISMATCH")
            if str(active_release["source_boundary_fingerprint"]) != str(
                active_resolution["source_boundary_fingerprint"]
            ):
                reason_codes.append("RESOLUTION_FINGERPRINT_MISMATCH")
        if str(active_release["source_boundary_fingerprint"]) != str(projection["fingerprint"]):
            reason_codes.append("SOURCE_BOUNDARY_FINGERPRINT_MISMATCH")
        status = "RELEASED" if not reason_codes else "RELEASE_INVALIDATED"
    reason_codes = list(dict.fromkeys(reason_codes))
    released = bool(active_release is not None and not reason_codes)
    if active_release is not None and not released:
        status = "RELEASE_INVALIDATED"
    return {
        "event_rows": event_rows,
        "chain_valid": chain_valid,
        "latest_event": latest_event,
        "active_release": active_release,
        "released": released,
        "status": status,
        "reason_codes": reason_codes,
    }


def _collect_resolution_snapshot(
    connection: sqlite3.Connection,
    boundary_row: sqlite3.Row,
) -> tuple[dict[str, Any], list[str], int]:
    command_id = str(boundary_row["command_id"])
    source_schema_reason_codes = _resolution_source_schema_reason_codes(connection)
    command = (
        connection.execute(
            "SELECT * FROM gateway_commands WHERE command_id = ?",
            (command_id,),
        ).fetchone()
        if _table_has_columns(
            connection,
            "gateway_commands",
            _RESOLUTION_SOURCE_TABLE_COLUMNS["gateway_commands"],
        )
        else None
    )
    reason_codes: list[str] = list(source_schema_reason_codes)
    evidence_count = 0

    boundary_markers = {
        "gateway_started_at": "BOUNDARY_GATEWAY_STARTED",
        "pre_ack_recorded_at": "BOUNDARY_PRE_ACK_RECORDED",
        "broker_accepted_at": "BOUNDARY_BROKER_ACCEPTED",
        "chejan_confirmed_at": "BOUNDARY_CHEJAN_CONFIRMED",
        "broker_order_no": "BOUNDARY_BROKER_ORDER_PRESENT",
        "broker_result_code": "BOUNDARY_BROKER_RESULT_PRESENT",
        "broker_message": "BOUNDARY_BROKER_MESSAGE_PRESENT",
    }
    boundary_evidence: dict[str, bool] = {}
    for field, reason_code in boundary_markers.items():
        present = bool(_optional_text(boundary_row[field]))
        boundary_evidence[f"{field}_present"] = present
        if present:
            reason_codes.append(reason_code)
            evidence_count += 1

    event_types = tuple(sorted(_BROKER_REACH_EVENT_TYPES))
    placeholders = ",".join("?" for _ in event_types)
    event_counts: dict[str, dict[str, int]] = {}
    for table_name in ("gateway_events", "gateway_command_events"):
        rows = (
            connection.execute(
                f"""
                SELECT lower(event_type) AS event_type, COUNT(*) AS count
                FROM {table_name}
                WHERE command_id = ?
                  AND lower(event_type) IN ({placeholders})
                GROUP BY lower(event_type)
                ORDER BY lower(event_type)
                """,
                (command_id, *event_types),
            ).fetchall()
            if _table_has_columns(
                connection,
                table_name,
                _RESOLUTION_SOURCE_TABLE_COLUMNS[table_name],
            )
            else []
        )
        counts = {str(row["event_type"]): int(row["count"] or 0) for row in rows}
        event_counts[table_name] = counts
        for event_type, count in counts.items():
            if count:
                reason_codes.append(_BROKER_REACH_EVENT_TYPES[event_type])
                evidence_count += count

    order_summary = {
        "count": 0,
        "broker_order_no_count": 0,
        "broker_result_count": 0,
        "broker_acked_count": 0,
        "filled_count": 0,
        "broker_terminal_status_count": 0,
        "unsafe_scope_count": 0,
    }
    if _table_has_columns(
        connection,
        "live_sim_orders",
        _RESOLUTION_SOURCE_TABLE_COLUMNS["live_sim_orders"],
    ):
        order_row = connection.execute(
            """
            SELECT
                COUNT(*) AS count,
                SUM(CASE WHEN broker_order_no IS NOT NULL
                          AND trim(broker_order_no) <> '' THEN 1 ELSE 0 END)
                    AS broker_order_no_count,
                SUM(CASE WHEN broker_result_code IS NOT NULL
                          AND trim(broker_result_code) <> '' THEN 1 ELSE 0 END)
                    AS broker_result_count,
                SUM(CASE WHEN broker_acked_at IS NOT NULL THEN 1 ELSE 0 END)
                    AS broker_acked_count,
                SUM(CASE WHEN filled_quantity > 0 THEN 1 ELSE 0 END)
                    AS filled_count,
                SUM(CASE WHEN upper(status) IN (
                        'BROKER_ACKED', 'PARTIALLY_FILLED', 'FILLED',
                        'BROKER_REJECTED', 'CANCEL_ACKED', 'CANCEL_REJECTED',
                        'EXIT_FILLED'
                    ) THEN 1 ELSE 0 END) AS broker_terminal_status_count,
                SUM(CASE WHEN live_sim_only <> 1 OR live_real_allowed <> 0
                         THEN 1 ELSE 0 END) AS unsafe_scope_count
            FROM live_sim_orders
            WHERE gateway_command_id = ?
            """,
            (command_id,),
        ).fetchone()
        assert order_row is not None
        order_summary = {key: int(order_row[key] or 0) for key in order_summary}
    for key, reason_code in (
        ("broker_order_no_count", "LIVE_SIM_ORDER_BROKER_ORDER_PRESENT"),
        ("broker_result_count", "LIVE_SIM_ORDER_BROKER_RESULT_PRESENT"),
        ("broker_acked_count", "LIVE_SIM_ORDER_BROKER_ACK_PRESENT"),
        ("filled_count", "LIVE_SIM_ORDER_FILL_PRESENT"),
        (
            "broker_terminal_status_count",
            "LIVE_SIM_ORDER_BROKER_STATUS_PRESENT",
        ),
        ("unsafe_scope_count", "LINKED_ORDER_NOT_LIVE_SIM_ONLY"),
    ):
        count = order_summary[key]
        if count:
            reason_codes.append(reason_code)
            evidence_count += count

    intent_summary = {
        "count": 0,
        "broker_order_sent_count": 0,
        "unsafe_scope_count": 0,
    }
    if _table_has_columns(
        connection,
        "live_sim_intents",
        _RESOLUTION_SOURCE_TABLE_COLUMNS["live_sim_intents"],
    ):
        intent_row = connection.execute(
            """
            SELECT
                COUNT(*) AS count,
                SUM(CASE WHEN broker_order_sent <> 0 THEN 1 ELSE 0 END)
                    AS broker_order_sent_count,
                SUM(CASE WHEN live_sim_only <> 1 OR live_real_allowed <> 0
                         THEN 1 ELSE 0 END) AS unsafe_scope_count
            FROM live_sim_intents
            WHERE gateway_command_id = ?
            """,
            (command_id,),
        ).fetchone()
        assert intent_row is not None
        intent_summary = {key: int(intent_row[key] or 0) for key in intent_summary}
    if intent_summary["broker_order_sent_count"]:
        reason_codes.append("LIVE_SIM_INTENT_BROKER_ORDER_SENT")
        evidence_count += intent_summary["broker_order_sent_count"]
    if intent_summary["unsafe_scope_count"]:
        reason_codes.append("LINKED_INTENT_NOT_LIVE_SIM_ONLY")
        evidence_count += intent_summary["unsafe_scope_count"]

    execution_count = 0
    if all(
        _table_has_columns(
            connection,
            table_name,
            _RESOLUTION_SOURCE_TABLE_COLUMNS[table_name],
        )
        for table_name in (
            "live_sim_executions",
            "live_sim_orders",
            "live_sim_intents",
        )
    ):
        execution_count = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT e.live_sim_execution_id) AS count
                FROM live_sim_executions e
                LEFT JOIN live_sim_orders o
                  ON o.live_sim_order_id = e.live_sim_order_id
                LEFT JOIN live_sim_intents i
                  ON i.live_sim_intent_id = e.live_sim_intent_id
                WHERE o.gateway_command_id = ? OR i.gateway_command_id = ?
                """,
                (command_id, command_id),
            ).fetchone()["count"]
            or 0
        )
    if execution_count:
        reason_codes.append("LIVE_SIM_EXECUTION_PRESENT")
        evidence_count += execution_count

    command_status = None if command is None else str(command["status"]).upper()
    if command is None:
        reason_codes.append("GATEWAY_COMMAND_MISSING")
        evidence_count += 1
    elif command_status in {
        "GATEWAY_STARTED",
        "PRE_ACK_RECORDED",
        "BROKER_ACCEPTED",
        "CHEJAN_CONFIRMED",
        "ACKED",
    }:
        reason_codes.append("GATEWAY_COMMAND_BROKER_STAGE_STATUS")
        evidence_count += 1

    snapshot = {
        "contract": "gateway-order-boundary-resolution-fingerprint.v1",
        "source_schema_reason_codes": source_schema_reason_codes,
        "command_id": command_id,
        "command": {
            "present": command is not None,
            "command_type": (None if command is None else str(command["command_type"]).lower()),
            "status": command_status,
            "attempts": 0 if command is None else int(command["attempts"] or 0),
            "dispatched_at": (
                None if command is None else _optional_text(command["dispatched_at"])
            ),
            "completed_at": (None if command is None else _optional_text(command["completed_at"])),
            "expires_at": (None if command is None else _optional_text(command["expires_at"])),
        },
        "boundary": {
            "state": str(boundary_row["state"]),
            "attempts": int(boundary_row["attempts"] or 0),
            "live_sim_only": bool(boundary_row["live_sim_only"]),
            "live_real_allowed": bool(boundary_row["live_real_allowed"]),
            **boundary_evidence,
        },
        "broker_reach_event_counts": event_counts,
        "linked_live_sim_orders": order_summary,
        "linked_live_sim_intents": intent_summary,
        "linked_live_sim_execution_count": execution_count,
    }
    return snapshot, list(dict.fromkeys(reason_codes)), evidence_count


def _resolution_quiescence_reason_codes(
    connection: sqlite3.Connection,
    *,
    command_id: str,
) -> list[str]:
    reason_codes: list[str] = _resolution_source_schema_reason_codes(connection)
    if not _table_has_columns(
        connection,
        "gateway_commands",
        _RESOLUTION_SOURCE_TABLE_COLUMNS["gateway_commands"],
    ):
        return list(dict.fromkeys(reason_codes))
    target_command = connection.execute(
        """
        SELECT command_type, status
        FROM gateway_commands
        WHERE command_id = ?
        """,
        (command_id,),
    ).fetchone()
    if target_command is None:
        reason_codes.append("TARGET_GATEWAY_COMMAND_MISSING")
    else:
        if str(target_command["command_type"]).lower() not in ORDER_COMMAND_TYPES:
            reason_codes.append("TARGET_COMMAND_TYPE_INVALID")
        if str(target_command["status"]).upper() not in {
            "UNCONFIRMED",
            "FAILED",
            "EXPIRED",
            "REJECTED",
        }:
            reason_codes.append("TARGET_COMMAND_STATUS_RUNNABLE_OR_UNSAFE")
    active_order_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM gateway_commands
            WHERE command_id <> ?
              AND lower(command_type) IN (
                  'send_order', 'cancel_order', 'modify_order'
              )
              AND upper(status) IN (
                  'QUEUED', 'CLAIMED', 'DISPATCHED', 'GATEWAY_STARTED',
                  'PRE_ACK_RECORDED'
              )
            """,
            (command_id,),
        ).fetchone()["count"]
        or 0
    )
    if active_order_count:
        reason_codes.append("ACTIVE_ORDER_COMMANDS_PRESENT")
    modify_order_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM gateway_commands
            WHERE lower(command_type) = 'modify_order'
            """
        ).fetchone()["count"]
        or 0
    )
    if modify_order_count:
        reason_codes.append("MODIFY_ORDER_COMMAND_PRESENT")
    if _table_has_columns(
        connection,
        "runtime_execution_locks",
        _RESOLUTION_SOURCE_TABLE_COLUMNS["runtime_execution_locks"],
    ):
        lock_count = int(
            connection.execute("SELECT COUNT(*) AS count FROM runtime_execution_locks").fetchone()[
                "count"
            ]
            or 0
        )
        if lock_count:
            reason_codes.append("RUNTIME_EXECUTION_LOCK_PRESENT")
    else:
        reason_codes.append("RUNTIME_EXECUTION_LOCK_TABLE_MISSING")
    if _table_has_columns(
        connection,
        "gateway_status",
        _RESOLUTION_SOURCE_TABLE_COLUMNS["gateway_status"],
    ):
        now = utc_now()
        activity_rows = connection.execute(
            """
            SELECT key, value
            FROM gateway_status
            WHERE key IN ('last_heartbeat_at', 'last_event_received_at')
              AND value IS NOT NULL
              AND trim(value) <> ''
            """
        ).fetchall()
        for row in activity_rows:
            try:
                observed_at = parse_timestamp(
                    str(row["value"]),
                    str(row["key"]),
                )
            except (TypeError, ValueError):
                reason_codes.append("GATEWAY_ACTIVITY_TIMESTAMP_INVALID")
                continue
            age_sec = (now - observed_at).total_seconds()
            if age_sec < 0:
                reason_codes.append("GATEWAY_ACTIVITY_TIMESTAMP_IN_FUTURE")
            elif age_sec <= _RESOLUTION_GATEWAY_QUIESCENCE_SEC:
                reason_codes.append("RECENT_GATEWAY_ACTIVITY_PRESENT")
    else:
        reason_codes.append("GATEWAY_STATUS_TABLE_MISSING")
    return list(dict.fromkeys(reason_codes))


def _validate_resolution_chain(
    rows: Sequence[sqlite3.Row],
) -> tuple[bool, sqlite3.Row | None]:
    active: sqlite3.Row | None = None
    expected_sequence = 1
    for row in rows:
        if not _resolution_row_contract_valid(row):
            return False, active
        if int(row["sequence_no"]) != expected_sequence:
            return False, active
        action = str(row["action"])
        supersedes = _optional_text(row["supersedes_resolution_id"])
        if action == RESOLUTION_ACTION_RESOLVE:
            if active is not None or supersedes is not None:
                return False, active
            active = row
        elif action == RESOLUTION_ACTION_REVOKE:
            if active is None or supersedes != str(active["resolution_id"]):
                return False, active
            active = None
        else:
            return False, active
        expected_sequence += 1
    return True, active


def _validate_fence_event_chain(
    rows: Sequence[sqlite3.Row],
) -> tuple[bool, sqlite3.Row | None]:
    previous: sqlite3.Row | None = None
    active_release: sqlite3.Row | None = None
    expected_sequence = 1
    for row in rows:
        if not _fence_event_row_contract_valid(row):
            return False, active_release
        if int(row["sequence_no"]) != expected_sequence:
            return False, active_release
        action = str(row["action"])
        supersedes = _optional_text(row["supersedes_fence_event_id"])
        expected_supersedes = None if previous is None else str(previous["fence_event_id"])
        if supersedes != expected_supersedes:
            return False, active_release
        if action == FENCE_ACTION_RELEASE:
            if active_release is not None:
                return False, active_release
            if previous is not None and str(previous["action"]) != FENCE_ACTION_REINSTATE:
                return False, active_release
            active_release = row
        elif action == FENCE_ACTION_REINSTATE:
            if active_release is None or previous is None:
                return False, active_release
            if str(previous["fence_event_id"]) != str(active_release["fence_event_id"]):
                return False, active_release
            for field in (
                "resolution_id",
                "resolution_request_hash",
                "source_boundary_fingerprint",
            ):
                if str(row[field]) != str(active_release[field]):
                    return False, active_release
            active_release = None
        else:
            return False, active_release
        previous = row
        expected_sequence += 1
    return True, active_release


def _fence_event_row_contract_valid(row: sqlite3.Row) -> bool:
    if str(row["action"]) not in {
        FENCE_ACTION_RELEASE,
        FENCE_ACTION_REINSTATE,
    }:
        return False
    for field in (
        "request_hash",
        "resolution_request_hash",
        "source_boundary_fingerprint",
        "approval_sha256",
        "evidence_sha256",
        "database_identity_sha256",
        "expected_gateway_command_state_fingerprint",
    ):
        if not _SHA256_RE.fullmatch(str(row[field])):
            return False
    for field in (
        "fence_event_id",
        "request_id",
        "resolution_id",
        "approval_id",
        "command_alias",
        "reason_code",
        "operator_id",
    ):
        if not _SAFE_IDENTIFIER_RE.fullmatch(str(row[field])):
            return False
    for field in (
        "request_id",
        "approval_id",
        "command_alias",
        "reason_code",
        "operator_id",
    ):
        if _contains_account_like_digit_sequence(str(row[field])):
            return False
    if not _TRADE_DATE_RE.fullmatch(str(row["approval_trade_date"])):
        return False
    if str(row["expected_app_name"]) != FENCE_EXPECTED_APP_NAME:
        return False
    if int(row["expected_schema_version"]) != FENCE_EXPECTED_SCHEMA_VERSION:
        return False
    total_count = int(row["expected_gateway_command_total_count"])
    order_count = int(row["expected_order_command_count"])
    if total_count < 0 or order_count < 0 or order_count > total_count:
        return False
    supersedes = _optional_text(row["supersedes_fence_event_id"])
    if supersedes is not None and not _SAFE_IDENTIFIER_RE.fullmatch(supersedes):
        return False
    if not bool(row["live_sim_only"]) or bool(row["live_real_allowed"]):
        return False
    normalized_request = {
        "action": str(row["action"]),
        "command_id": str(row["command_id"]),
        "request_id": str(row["request_id"]),
        "expected_previous_fence_event_id": supersedes,
        "expected_resolution_id": str(row["resolution_id"]),
        "expected_resolution_request_hash": str(row["resolution_request_hash"]),
        "expected_source_boundary_fingerprint": str(row["source_boundary_fingerprint"]),
        "approval_id": str(row["approval_id"]),
        "command_alias": str(row["command_alias"]),
        "approval_trade_date": str(row["approval_trade_date"]),
        "approval_sha256": str(row["approval_sha256"]),
        "evidence_sha256": str(row["evidence_sha256"]),
        "database_identity_sha256": str(row["database_identity_sha256"]),
        "expected_app_name": str(row["expected_app_name"]),
        "expected_schema_version": int(row["expected_schema_version"]),
        "expected_gateway_command_total_count": total_count,
        "expected_order_command_count": order_count,
        "expected_gateway_command_state_fingerprint": str(
            row["expected_gateway_command_state_fingerprint"]
        ),
        "reason_code": str(row["reason_code"]),
        "operator_id": str(row["operator_id"]),
    }
    expected_request_hash = hashlib.sha256(
        _canonical_json(normalized_request).encode("utf-8")
    ).hexdigest()
    if str(row["request_hash"]) != expected_request_hash:
        return False
    try:
        approval = build_order_broker_boundary_fence_approval(
            action=str(row["action"]),
            approval_id=str(row["approval_id"]),
            request_id=str(row["request_id"]),
            operator_id=str(row["operator_id"]),
            reason_code=str(row["reason_code"]),
            approval_trade_date=str(row["approval_trade_date"]),
            command_alias=str(row["command_alias"]),
            command_id=str(row["command_id"]),
            expected_previous_fence_event_id=supersedes,
            expected_resolution_id=str(row["resolution_id"]),
            expected_resolution_request_hash=str(row["resolution_request_hash"]),
            expected_source_boundary_fingerprint=str(row["source_boundary_fingerprint"]),
            evidence_sha256=str(row["evidence_sha256"]),
            database_identity_sha256=str(row["database_identity_sha256"]),
            expected_app_name=str(row["expected_app_name"]),
            expected_schema_version=int(row["expected_schema_version"]),
            expected_gateway_command_total_count=total_count,
            expected_order_command_count=order_count,
            expected_gateway_command_state_fingerprint=str(
                row["expected_gateway_command_state_fingerprint"]
            ),
        )
    except (OrderBrokerBoundaryResolutionError, TypeError, ValueError):
        return False
    return str(row["approval_sha256"]) == str(approval["sha256"])


def _resolution_row_contract_valid(row: sqlite3.Row) -> bool:
    action = str(row["action"])
    source_fingerprint = str(row["source_boundary_fingerprint"])
    evidence_sha256 = str(row["evidence_sha256"])
    request_hash = str(row["request_hash"])
    if action not in {RESOLUTION_ACTION_RESOLVE, RESOLUTION_ACTION_REVOKE}:
        return False
    if str(row["resolution_type"]) != BROKER_NOT_REACHED:
        return False
    if not all(
        _SHA256_RE.fullmatch(value) for value in (source_fingerprint, evidence_sha256, request_hash)
    ):
        return False
    for field in (
        "resolution_id",
        "request_id",
        "reason_code",
        "evidence_type",
        "evidence_ref",
        "operator_id",
    ):
        if not _SAFE_IDENTIFIER_RE.fullmatch(str(row[field])):
            return False
    for field in (
        "request_id",
        "reason_code",
        "evidence_type",
        "evidence_ref",
        "operator_id",
    ):
        if _contains_account_like_digit_sequence(str(row[field])):
            return False
    supersedes = _optional_text(row["supersedes_resolution_id"])
    if supersedes is not None and not _SAFE_IDENTIFIER_RE.fullmatch(supersedes):
        return False
    if (
        not bool(row["live_sim_only"])
        or bool(row["live_real_allowed"])
        or not bool(row["routing_fence_active"])
    ):
        return False
    try:
        snapshot = json.loads(str(row["boundary_snapshot_json"]))
    except (TypeError, ValueError):
        return False
    if not isinstance(snapshot, dict):
        return False
    if hashlib.sha256(_canonical_json(snapshot).encode("utf-8")).hexdigest() != source_fingerprint:
        return False
    normalized_request = {
        "action": action,
        "command_id": str(row["command_id"]),
        "request_id": str(row["request_id"]),
        "expected_fingerprint": source_fingerprint,
        "reason_code": str(row["reason_code"]),
        "evidence_type": str(row["evidence_type"]),
        "evidence_ref": str(row["evidence_ref"]),
        "evidence_sha256": evidence_sha256,
        "operator_id": str(row["operator_id"]),
        "supersedes_resolution_id": supersedes,
    }
    expected_request_hash = hashlib.sha256(
        _canonical_json(normalized_request).encode("utf-8")
    ).hexdigest()
    return request_hash == expected_request_hash


def _public_resolution_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "resolution_id": str(row["resolution_id"]),
        "command_id": str(row["command_id"]),
        "sequence_no": int(row["sequence_no"]),
        "action": str(row["action"]),
        "resolution_type": str(row["resolution_type"]),
        "supersedes_resolution_id": _optional_text(row["supersedes_resolution_id"]),
        "reason_code": str(row["reason_code"]),
        "evidence_type": str(row["evidence_type"]),
        "evidence_sha256_verified": bool(_SHA256_RE.fullmatch(str(row["evidence_sha256"]))),
        "created_at": str(row["created_at"]),
        "live_sim_only": bool(row["live_sim_only"]),
        "live_real_allowed": bool(row["live_real_allowed"]),
        "routing_fence_active": bool(row["routing_fence_active"]),
    }


def _public_fence_event_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "fence_event_id": str(row["fence_event_id"]),
        "command_id": str(row["command_id"]),
        "command_alias": str(row["command_alias"]),
        "sequence_no": int(row["sequence_no"]),
        "action": str(row["action"]),
        "supersedes_fence_event_id": _optional_text(row["supersedes_fence_event_id"]),
        "resolution_id": str(row["resolution_id"]),
        "resolution_request_hash": str(row["resolution_request_hash"]),
        "source_boundary_fingerprint": str(row["source_boundary_fingerprint"]),
        "approval_id": str(row["approval_id"]),
        "approval_trade_date": str(row["approval_trade_date"]),
        "approval_sha256": str(row["approval_sha256"]),
        "evidence_sha256": str(row["evidence_sha256"]),
        "database_identity_sha256": str(row["database_identity_sha256"]),
        "expected_app_name": str(row["expected_app_name"]),
        "expected_schema_version": int(row["expected_schema_version"]),
        "expected_gateway_command_total_count": int(row["expected_gateway_command_total_count"]),
        "expected_order_command_count": int(row["expected_order_command_count"]),
        "expected_gateway_command_state_fingerprint": str(
            row["expected_gateway_command_state_fingerprint"]
        ),
        "reason_code": str(row["reason_code"]),
        "created_at": str(row["created_at"]),
        "live_sim_only": bool(row["live_sim_only"]),
        "live_real_allowed": bool(row["live_real_allowed"]),
    }


def _require_safe_identifier(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_IDENTIFIER_RE.fullmatch(normalized):
        raise OrderBrokerBoundaryResolutionError(
            f"INVALID_{name.upper()}",
            f"{name} must be an opaque identifier without path separators",
        )
    return normalized


def _contains_account_like_digit_sequence(value: str) -> bool:
    return bool(_LONG_DIGIT_RUN_RE.search(value) or _SEPARATED_DIGIT_RUN_RE.search(value))


def _require_sha256(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not _SHA256_RE.fullmatch(normalized):
        raise OrderBrokerBoundaryResolutionError(
            f"INVALID_{name.upper()}",
            f"{name} must be a lowercase SHA-256 digest",
        )
    return normalized


def _resolution_schema_status(connection: sqlite3.Connection) -> dict[str, bool]:
    table_exists = _table_exists(connection, RESOLUTION_TABLE)
    if not table_exists:
        return {
            "table_exists": False,
            "required_columns_present": False,
            "required_indexes_present": False,
            "append_only_triggers_present": False,
            "ready": False,
        }
    columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in connection.execute(f"PRAGMA table_info({RESOLUTION_TABLE})").fetchall()
    }
    required_columns_present = _RESOLUTION_REQUIRED_COLUMNS.issubset(columns)
    required_indexes_present = all(
        _resolution_index_contract_valid(
            connection,
            name=name,
            columns=columns,
            unique=unique,
            descending=descending,
        )
        for name, columns, unique, descending in (
            (
                _RESOLUTION_CREATED_INDEX,
                ("created_at", "resolution_id"),
                False,
                (True, True),
            ),
            (
                _RESOLUTION_REQUEST_INDEX,
                ("request_id",),
                True,
                (False,),
            ),
            (
                _RESOLUTION_COMMAND_SEQUENCE_INDEX,
                ("command_id", "sequence_no"),
                True,
                (False, False),
            ),
        )
    )
    trigger_rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
    ).fetchall()
    trigger_sql = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): str(
            (row["sql"] if isinstance(row, sqlite3.Row) else row[1]) or ""
        )
        for row in trigger_rows
    }
    append_only_triggers_present = all(
        _resolution_trigger_contract_valid(
            trigger_sql.get(name),
            name=name,
            operation=operation,
        )
        for name, operation in (
            (_RESOLUTION_UPDATE_TRIGGER, "UPDATE"),
            (_RESOLUTION_DELETE_TRIGGER, "DELETE"),
        )
    )
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (RESOLUTION_TABLE,),
    ).fetchone()
    table_sql = "" if table_row is None else str(table_row["sql"] or "").upper()
    constraints_present = all(
        token in table_sql
        for token in (
            "CHECK (SEQUENCE_NO > 0)",
            "RESOLVE_BROKER_NOT_REACHED",
            "BROKER_NOT_REACHED",
            "CHECK (LIVE_SIM_ONLY = 1)",
            "CHECK (LIVE_REAL_ALLOWED = 0)",
            "CHECK (ROUTING_FENCE_ACTIVE = 1)",
        )
    )
    return {
        "table_exists": True,
        "required_columns_present": required_columns_present,
        "required_indexes_present": required_indexes_present,
        "append_only_triggers_present": append_only_triggers_present,
        "constraints_present": constraints_present,
        "ready": bool(
            required_columns_present
            and required_indexes_present
            and append_only_triggers_present
            and constraints_present
        ),
    }


def _fence_event_schema_status(connection: sqlite3.Connection) -> dict[str, bool]:
    table_exists = _table_exists(connection, FENCE_EVENT_TABLE)
    if not table_exists:
        return {
            "table_exists": False,
            "required_columns_present": False,
            "required_indexes_present": False,
            "append_only_triggers_present": False,
            "foreign_keys_present": False,
            "constraints_present": False,
            "ready": False,
        }
    column_rows = connection.execute(f"PRAGMA table_xinfo({FENCE_EVENT_TABLE})").fetchall()
    columns = tuple(
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) for row in column_rows
    )
    column_contracts = tuple(
        (
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1]),
            str(row["type"] if isinstance(row, sqlite3.Row) else row[2]).upper(),
            bool(row["notnull"] if isinstance(row, sqlite3.Row) else row[3]),
            (
                None
                if (row["dflt_value"] if isinstance(row, sqlite3.Row) else row[4]) is None
                else str(row["dflt_value"] if isinstance(row, sqlite3.Row) else row[4])
            ),
            int(row["pk"] if isinstance(row, sqlite3.Row) else row[5]),
            int(row["hidden"] if isinstance(row, sqlite3.Row) else row[6]),
        )
        for row in column_rows
    )
    required_columns_present = bool(
        columns == _FENCE_EVENT_REQUIRED_COLUMNS
        and column_contracts == _FENCE_EVENT_REQUIRED_COLUMN_CONTRACTS
    )
    required_index_names = {
        _FENCE_EVENT_CREATED_INDEX,
        _FENCE_EVENT_REQUEST_INDEX,
        _FENCE_EVENT_COMMAND_SEQUENCE_INDEX,
    }
    index_rows = connection.execute(f"PRAGMA index_list({FENCE_EVENT_TABLE})").fetchall()
    custom_index_names = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in index_rows
        if str(row["origin"] if isinstance(row, sqlite3.Row) else row[3]) == "c"
    }
    automatic_index_contracts: set[tuple[str, bool, bool, tuple[str, ...], tuple[bool, ...]]] = (
        set()
    )
    for index_row in index_rows:
        origin = str(
            index_row["origin"] if isinstance(index_row, sqlite3.Row) else index_row[3]
        ).lower()
        if origin == "c":
            continue
        index_name = str(index_row["name"] if isinstance(index_row, sqlite3.Row) else index_row[1])
        key_rows = [
            row
            for row in connection.execute(f"PRAGMA index_xinfo({index_name})").fetchall()
            if int(row["key"] if isinstance(row, sqlite3.Row) else row[5]) == 1
            and int(row["cid"] if isinstance(row, sqlite3.Row) else row[1]) >= 0
        ]
        key_rows.sort(key=lambda row: int(row["seqno"] if isinstance(row, sqlite3.Row) else row[0]))
        automatic_index_contracts.add(
            (
                origin,
                bool(index_row["unique"] if isinstance(index_row, sqlite3.Row) else index_row[2]),
                bool(index_row["partial"] if isinstance(index_row, sqlite3.Row) else index_row[4]),
                tuple(
                    str(row["name"] if isinstance(row, sqlite3.Row) else row[2]) for row in key_rows
                ),
                tuple(
                    bool(row["desc"] if isinstance(row, sqlite3.Row) else row[3])
                    for row in key_rows
                ),
            )
        )
    automatic_indexes_exact = automatic_index_contracts == {
        ("pk", True, False, ("fence_event_id",), (False,)),
        ("u", True, False, ("request_id",), (False,)),
    }
    required_indexes_present = (
        custom_index_names == required_index_names
        and automatic_indexes_exact
        and all(
            _fence_event_index_contract_valid(
                connection,
                name=name,
                columns=index_columns,
                unique=unique,
                descending=descending,
            )
            for name, index_columns, unique, descending in (
                (
                    _FENCE_EVENT_CREATED_INDEX,
                    ("created_at", "fence_event_id"),
                    False,
                    (True, True),
                ),
                (
                    _FENCE_EVENT_REQUEST_INDEX,
                    ("request_id",),
                    True,
                    (False,),
                ),
                (
                    _FENCE_EVENT_COMMAND_SEQUENCE_INDEX,
                    ("command_id", "sequence_no"),
                    True,
                    (False, False),
                ),
            )
        )
    )
    trigger_rows = connection.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'trigger' AND tbl_name = ?
        """,
        (FENCE_EVENT_TABLE,),
    ).fetchall()
    trigger_sql = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): str(
            (row["sql"] if isinstance(row, sqlite3.Row) else row[1]) or ""
        )
        for row in trigger_rows
    }
    required_trigger_names = {
        _FENCE_EVENT_UPDATE_TRIGGER,
        _FENCE_EVENT_DELETE_TRIGGER,
    }
    append_only_triggers_present = set(trigger_sql) == required_trigger_names and all(
        _fence_event_trigger_contract_valid(
            trigger_sql.get(name),
            name=name,
            operation=operation,
        )
        for name, operation in (
            (_FENCE_EVENT_UPDATE_TRIGGER, "UPDATE"),
            (_FENCE_EVENT_DELETE_TRIGGER, "DELETE"),
        )
    )
    foreign_key_rows = connection.execute(
        f"PRAGMA foreign_key_list({FENCE_EVENT_TABLE})"
    ).fetchall()
    foreign_keys = {
        (
            str(row["table"] if isinstance(row, sqlite3.Row) else row[2]),
            str(row["from"] if isinstance(row, sqlite3.Row) else row[3]),
            str(row["to"] if isinstance(row, sqlite3.Row) else row[4]),
            str(row["on_update"] if isinstance(row, sqlite3.Row) else row[5]),
            str(row["on_delete"] if isinstance(row, sqlite3.Row) else row[6]),
            str(row["match"] if isinstance(row, sqlite3.Row) else row[7]),
        )
        for row in foreign_key_rows
    }
    expected_foreign_keys = {
        (
            "gateway_order_broker_boundaries",
            "command_id",
            "command_id",
            "NO ACTION",
            "NO ACTION",
            "NONE",
        ),
        (
            "gateway_order_broker_boundary_resolutions",
            "resolution_id",
            "resolution_id",
            "NO ACTION",
            "NO ACTION",
            "NONE",
        ),
        (
            FENCE_EVENT_TABLE,
            "supersedes_fence_event_id",
            "fence_event_id",
            "NO ACTION",
            "NO ACTION",
            "NONE",
        ),
    }
    foreign_keys_present = foreign_keys == expected_foreign_keys
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (FENCE_EVENT_TABLE,),
    ).fetchone()
    table_sql = (
        ""
        if table_row is None
        else str(
            (table_row["sql"] if isinstance(table_row, sqlite3.Row) else table_row[0]) or ""
        ).upper()
    )
    compact_table_sql = re.sub(r"\s+", "", table_sql)
    constraints_present = all(
        re.sub(r"\s+", "", token) in compact_table_sql
        for token in (
            "CHECK (SEQUENCE_NO > 0)",
            "CHECK (ACTION IN ('RELEASE', 'REINSTATE'))",
            "CHECK (LENGTH(REQUEST_HASH) = 64)",
            "CHECK (LENGTH(RESOLUTION_REQUEST_HASH) = 64)",
            "CHECK (LENGTH(SOURCE_BOUNDARY_FINGERPRINT) = 64)",
            "CHECK (LENGTH(APPROVAL_SHA256) = 64)",
            "CHECK (LENGTH(EVIDENCE_SHA256) = 64)",
            "CHECK (LENGTH(DATABASE_IDENTITY_SHA256) = 64)",
            "CHECK (LENGTH(EXPECTED_GATEWAY_COMMAND_STATE_FINGERPRINT) = 64)",
            "CHECK (LENGTH(APPROVAL_TRADE_DATE) = 10)",
            "CHECK (LIVE_SIM_ONLY = 1)",
            "CHECK (LIVE_REAL_ALLOWED = 0)",
            "CHECK (EXPECTED_APP_NAME = 'SUSEOK-TRADER-V2')",
            "CHECK (EXPECTED_SCHEMA_VERSION = 63)",
            "CHECK (EXPECTED_GATEWAY_COMMAND_TOTAL_COUNT >= 0)",
            (
                "CHECK (EXPECTED_ORDER_COMMAND_COUNT >= 0 "
                "AND EXPECTED_ORDER_COMMAND_COUNT "
                "<= EXPECTED_GATEWAY_COMMAND_TOTAL_COUNT)"
            ),
            "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARIES",
            "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARY_RESOLUTIONS",
            "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENTS",
        )
    )
    return {
        "table_exists": True,
        "required_columns_present": required_columns_present,
        "required_indexes_present": required_indexes_present,
        "append_only_triggers_present": append_only_triggers_present,
        "foreign_keys_present": foreign_keys_present,
        "constraints_present": constraints_present,
        "ready": bool(
            required_columns_present
            and required_indexes_present
            and append_only_triggers_present
            and foreign_keys_present
            and constraints_present
        ),
    }


def _fence_event_index_contract_valid(
    connection: sqlite3.Connection,
    *,
    name: str,
    columns: tuple[str, ...],
    unique: bool,
    descending: tuple[bool, ...],
) -> bool:
    index_rows = connection.execute(f"PRAGMA index_list({FENCE_EVENT_TABLE})").fetchall()
    index_row = next(
        (
            row
            for row in index_rows
            if str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) == name
        ),
        None,
    )
    if index_row is None:
        return False
    is_unique = bool(index_row["unique"] if isinstance(index_row, sqlite3.Row) else index_row[2])
    is_partial = bool(index_row["partial"] if isinstance(index_row, sqlite3.Row) else index_row[4])
    if is_unique is not unique or is_partial:
        return False
    xinfo_rows = connection.execute(f"PRAGMA index_xinfo({name})").fetchall()
    key_rows = sorted(
        (
            row
            for row in xinfo_rows
            if int(row["key"] if isinstance(row, sqlite3.Row) else row[5]) == 1
            and int(row["cid"] if isinstance(row, sqlite3.Row) else row[1]) >= 0
        ),
        key=lambda row: int(row["seqno"] if isinstance(row, sqlite3.Row) else row[0]),
    )
    actual_columns = tuple(
        str(row["name"] if isinstance(row, sqlite3.Row) else row[2]) for row in key_rows
    )
    actual_descending = tuple(
        bool(row["desc"] if isinstance(row, sqlite3.Row) else row[3]) for row in key_rows
    )
    return actual_columns == columns and actual_descending == descending


def _fence_event_trigger_contract_valid(
    sql: str | None,
    *,
    name: str,
    operation: str,
) -> bool:
    if not sql:
        return False
    compact = re.sub(r"\s+", "", sql.upper()).replace("IFNOTEXISTS", "").rstrip(";")
    expected = (
        f"CREATETRIGGER{name.upper()}BEFORE{operation}ON"
        f"{FENCE_EVENT_TABLE.upper()}BEGINSELECTRAISE(ABORT,"
        "'GATEWAYORDER-BOUNDARYFENCEEVENTSAREAPPEND-ONLY');END"
    )
    return compact == expected


def _resolution_index_contract_valid(
    connection: sqlite3.Connection,
    *,
    name: str,
    columns: tuple[str, ...],
    unique: bool,
    descending: tuple[bool, ...],
) -> bool:
    index_rows = connection.execute(f"PRAGMA index_list({RESOLUTION_TABLE})").fetchall()
    index_row = next(
        (
            row
            for row in index_rows
            if str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) == name
        ),
        None,
    )
    if index_row is None:
        return False
    is_unique = bool(index_row["unique"] if isinstance(index_row, sqlite3.Row) else index_row[2])
    is_partial = bool(index_row["partial"] if isinstance(index_row, sqlite3.Row) else index_row[4])
    if is_unique is not unique or is_partial:
        return False
    xinfo_rows = connection.execute(f"PRAGMA index_xinfo({name})").fetchall()
    key_rows = sorted(
        (
            row
            for row in xinfo_rows
            if int(row["key"] if isinstance(row, sqlite3.Row) else row[5]) == 1
            and int(row["cid"] if isinstance(row, sqlite3.Row) else row[1]) >= 0
        ),
        key=lambda row: int(row["seqno"] if isinstance(row, sqlite3.Row) else row[0]),
    )
    actual_columns = tuple(
        str(row["name"] if isinstance(row, sqlite3.Row) else row[2]) for row in key_rows
    )
    actual_descending = tuple(
        bool(row["desc"] if isinstance(row, sqlite3.Row) else row[3]) for row in key_rows
    )
    return actual_columns == columns and actual_descending == descending


def _resolution_trigger_contract_valid(
    sql: str | None,
    *,
    name: str,
    operation: str,
) -> bool:
    if not sql:
        return False
    compact = re.sub(r"\s+", "", sql.upper()).replace("IFNOTEXISTS", "").rstrip(";")
    expected = (
        f"CREATETRIGGER{name.upper()}BEFORE{operation}ON"
        f"{RESOLUTION_TABLE.upper()}BEGINSELECTRAISE(ABORT,"
        "'GATEWAYORDER-BOUNDARYRESOLUTIONSAREAPPEND-ONLY');END"
    )
    return compact == expected


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


def _table_has_columns(
    connection: sqlite3.Connection,
    table_name: str,
    required_columns: frozenset[str],
) -> bool:
    if not _table_exists(connection, table_name):
        return False
    columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    }
    return required_columns.issubset(columns)


def _resolution_source_schema_reason_codes(
    connection: sqlite3.Connection,
) -> list[str]:
    reason_codes: list[str] = []
    for table_name, required_columns in _RESOLUTION_SOURCE_TABLE_COLUMNS.items():
        if not _table_has_columns(connection, table_name, required_columns):
            reason_codes.append(f"RESOLUTION_SOURCE_SCHEMA_INVALID_{table_name.upper()}")
    if "RESOLUTION_SOURCE_SCHEMA_INVALID_APP_METADATA" not in reason_codes:
        schema_row = connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()
        try:
            schema_version = int(schema_row["value"] if schema_row else "")
        except (TypeError, ValueError):
            schema_version = -1
        if schema_version < _RESOLUTION_MIN_SOURCE_SCHEMA_VERSION:
            reason_codes.append("RESOLUTION_SOURCE_SCHEMA_VERSION_UNSUPPORTED")
    return reason_codes


def _index_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows}
