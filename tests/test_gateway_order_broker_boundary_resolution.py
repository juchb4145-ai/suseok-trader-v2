from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from storage.gateway_order_broker_boundary import (
    OrderBrokerBoundaryResolutionError,
    get_effective_order_broker_boundary,
    get_order_broker_boundary_status,
    list_order_broker_boundaries,
    preview_order_broker_boundary_resolution,
    record_order_broker_boundary_resolution,
    revoke_order_broker_boundary_resolution,
)
from storage.sqlite import SCHEMA_VERSION, initialize_database

COMMAND_ID = "cmd-fast0r1-unconfirmed"
RAW_ACCOUNT_SENTINEL = "9988776655"
RAW_IDEMPOTENCY_SENTINEL = f"live-sim:{RAW_ACCOUNT_SENTINEL}:secret-idempotency"
RAW_PAYLOAD_SECRET_SENTINEL = "must-never-leave-storage-row"
NOW = "2026-07-14T03:00:00.000000Z"
LATER = "2026-07-14T03:01:00.000000Z"
RESOLUTION_TYPE = "BROKER_NOT_REACHED"
RESOLVED_EFFECTIVE_STATE = "RESOLVED_BROKER_NOT_REACHED"
REASON_CODE = "OPERATOR_CONFIRMED_BROKER_NOT_REACHED"
EVIDENCE_TYPE = "SIMULATION_HTS_ORDER_HISTORY_EXPORT"
EVIDENCE_REF = "fast0r1-evidence-001"
EVIDENCE_SHA256 = hashlib.sha256(b"sanitized-fast0r1-evidence").hexdigest()
OPERATOR_ID = "operator.fast0r1"


@pytest.fixture
def boundary_connection(tmp_path):
    connection = initialize_database(tmp_path / "broker-boundary-resolution.sqlite3")
    _insert_unconfirmed_boundary(connection)
    try:
        yield connection
    finally:
        connection.close()


def test_resolution_preserves_raw_rows_and_clears_only_effective_gate(
    boundary_connection,
) -> None:
    connection = boundary_connection
    raw_boundary_before = _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (COMMAND_ID,),
    )
    raw_command_before = _row(
        connection,
        "SELECT * FROM gateway_commands WHERE command_id = ?",
        (COMMAND_ID,),
    )
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    assert preview["eligible"] is True
    assert preview["reason_codes"] == []
    assert _is_sha256(preview["source_boundary_fingerprint"])

    result = _record_resolution(connection, preview=preview)

    raw_boundary_after = _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (COMMAND_ID,),
    )
    raw_command_after = _row(
        connection,
        "SELECT * FROM gateway_commands WHERE command_id = ?",
        (COMMAND_ID,),
    )
    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)

    assert result["idempotent_replay"] is False
    assert result["resolution_id"]
    assert raw_boundary_after == raw_boundary_before
    assert raw_command_after == raw_command_before
    assert effective["raw_state"] == "UNCONFIRMED"
    assert effective["effective_state"] == RESOLVED_EFFECTIVE_STATE
    assert effective["resolution"]["resolution_type"] == RESOLUTION_TYPE
    assert status["state_counts"]["UNCONFIRMED"] == 1
    assert status["unconfirmed_count"] == 1
    assert status["block_new_order_routing"] is True
    assert status["effective_state_counts"][RESOLVED_EFFECTIVE_STATE] == 1
    assert status["effective_unconfirmed_count"] == 0
    assert status["effective_resolution_count"] == 1
    assert status["invalidated_resolution_count"] == 0
    assert status["qualification_block_new_order_routing"] is False
    assert status["effective_block_new_order_routing"] is True
    assert status["resolution_maintenance_fence_active"] is True
    assert status["fast_0_status"] == "CLEAR"
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1


def test_resolution_rejects_stale_cross_table_fingerprint(boundary_connection) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    connection.execute(
        "UPDATE gateway_commands SET attempts = attempts + 1 WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.commit()

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    _assert_error_mentions(error.value, "STALE", "FINGERPRINT")
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0


def test_resolution_rejects_invalid_evidence_sha256(boundary_connection) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    raw_before = _raw_boundary(connection)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(
            connection,
            preview=preview,
            evidence_sha256="not-a-sha256",
        )

    _assert_error_mentions(error.value, "EVIDENCE", "SHA256")
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0
    assert _raw_boundary(connection) == raw_before


@pytest.mark.parametrize(
    "event_type",
    (
        "command_started",
        "order_pre_ack",
        "command_ack",
        "order_broker_unconfirmed",
        "execution_event",
        "kiwoom_order_chejan",
        "kiwoom_balance_chejan",
        "kiwoom_special_chejan",
        "order_rejected",
        "cancel_ack",
        "cancel_rejected",
    ),
)
def test_resolution_rejects_command_linked_broker_reach_evidence(
    boundary_connection,
    event_type: str,
) -> None:
    connection = boundary_connection
    raw_before = _raw_boundary(connection)
    _insert_gateway_event(connection, event_type=event_type)
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    assert preview["eligible"] is False
    assert preview["reason_codes"]
    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    _assert_error_mentions(error.value, "BROKER", "EVIDENCE")
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0
    assert _raw_boundary(connection) == raw_before


@pytest.mark.parametrize(
    "marker",
    ("boundary_broker_order_no", "live_order_broker_order_no", "live_execution"),
)
def test_resolution_rejects_broker_order_and_execution_markers(
    boundary_connection,
    marker: str,
) -> None:
    connection = boundary_connection
    if marker == "boundary_broker_order_no":
        connection.execute(
            """
            UPDATE gateway_order_broker_boundaries
            SET broker_order_no = 'SIM-BROKER-001'
            WHERE command_id = ?
            """,
            (COMMAND_ID,),
        )
    else:
        _insert_live_sim_order(
            connection,
            broker_order_no=(
                "SIM-BROKER-002" if marker == "live_order_broker_order_no" else None
            ),
        )
        if marker == "live_execution":
            _insert_live_sim_execution(connection)
    connection.commit()
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    assert preview["eligible"] is False
    with pytest.raises(OrderBrokerBoundaryResolutionError):
        _record_resolution(connection, preview=preview)
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0


@pytest.mark.parametrize(
    "event_type",
    ("order_pre_ack", "command_ack", "kiwoom_order_chejan", "execution_event"),
)
def test_late_broker_evidence_invalidates_resolution_and_reblocks(
    boundary_connection,
    event_type: str,
) -> None:
    connection = boundary_connection
    raw_before = _raw_boundary(connection)
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    _record_resolution(connection, preview=preview)
    cleared = get_order_broker_boundary_status(connection)

    assert cleared["qualification_block_new_order_routing"] is False
    assert cleared["effective_block_new_order_routing"] is True
    _insert_gateway_event(
        connection,
        event_type=event_type,
        event_id=f"evt-late-{event_type.replace('_', '-')}",
    )

    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)

    assert _raw_boundary(connection) == raw_before
    assert effective["raw_state"] == "UNCONFIRMED"
    assert effective["effective_state"] != RESOLVED_EFFECTIVE_STATE
    assert effective["resolution_effective"] is False
    assert effective["late_broker_evidence"] is True
    assert status["effective_block_new_order_routing"] is True
    assert status["invalidated_resolution_count"] == 1
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1


def test_late_ack_raw_transition_keeps_invalidated_resolution_blocked(
    boundary_connection,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    _record_resolution(connection, preview=preview)
    connection.execute(
        "UPDATE gateway_commands SET status = 'ACKED' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET state = 'BROKER_ACCEPTED',
            pre_ack_recorded_at = ?,
            broker_accepted_at = ?,
            updated_at = ?
        WHERE command_id = ?
        """,
        (LATER, LATER, LATER, COMMAND_ID),
    )
    connection.commit()

    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)

    assert effective["raw_state"] == "BROKER_ACCEPTED"
    assert effective["effective_state"] == "BROKER_ACCEPTED"
    assert effective["resolution_status"] == "OVERRIDDEN_BY_RAW_STATE"
    assert status["invalidated_resolution_count"] == 1
    assert status["qualification_block_new_order_routing"] is True
    assert status["effective_block_new_order_routing"] is True
    assert status["fast_0_status"] == "BLOCKED"


def test_unrelated_late_event_does_not_invalidate_resolution(
    boundary_connection,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    _record_resolution(connection, preview=preview)
    _insert_gateway_event(
        connection,
        event_type="gateway_log",
        event_id="evt-late-unrelated-log",
    )

    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)

    assert effective["effective_state"] == RESOLVED_EFFECTIVE_STATE
    assert effective["resolution_effective"] is True
    assert effective["late_broker_evidence"] is False
    assert status["qualification_block_new_order_routing"] is False
    assert status["effective_block_new_order_routing"] is True


def test_resolution_request_is_idempotent_and_conflicting_reuse_is_rejected(
    boundary_connection,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    first = _record_resolution(connection, preview=preview, request_id="request-idempotent")
    replay = _record_resolution(connection, preview=preview, request_id="request-idempotent")

    assert first["resolution_id"] == replay["resolution_id"]
    assert first["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert replay["idempotent_replay_effective"] is True
    assert replay["resolution_status"] == "EFFECTIVE"
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1

    with pytest.raises(OrderBrokerBoundaryResolutionError) as reused_request:
        _record_resolution(
            connection,
            preview=preview,
            request_id="request-idempotent",
            evidence_sha256=hashlib.sha256(b"different-evidence").hexdigest(),
        )
    _assert_error_mentions(reused_request.value, "REQUEST", "CONFLICT")

    with pytest.raises(OrderBrokerBoundaryResolutionError) as duplicate_resolution:
        _record_resolution(
            connection,
            preview=preview,
            request_id="request-second-active-resolution",
        )
    _assert_error_mentions(duplicate_resolution.value, "ACTIVE", "RESOLUTION")
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1


def test_idempotent_replay_reports_when_original_resolution_is_no_longer_effective(
    boundary_connection,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    _record_resolution(
        connection,
        preview=preview,
        request_id="request-replay-invalidated",
    )
    _insert_gateway_event(
        connection,
        event_type="command_ack",
        event_id="evt-replay-invalidated",
    )

    replay = _record_resolution(
        connection,
        preview=preview,
        request_id="request-replay-invalidated",
    )

    assert replay["idempotent_replay"] is True
    assert replay["idempotent_replay_effective"] is False
    assert replay["resolution_status"] == "OVERRIDDEN_BY_BROKER_EVIDENCE"
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("request_id", "request.account1234-5678"),
        ("evidence_ref", "artifact.account123-45-6789"),
        ("operator_id", "operator.account12-3456-78"),
    ),
)
def test_resolution_rejects_account_like_digits_in_ledger_labels(
    boundary_connection,
    field: str,
    value: str,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    kwargs = {field: value}

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview, **kwargs)

    _assert_error_mentions(error.value, "INVALID", field.upper())
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0


def test_resolution_preview_and_apply_reject_invalid_append_only_schema(
    boundary_connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        "DROP TRIGGER trg_gateway_order_boundary_resolutions_no_update"
    )
    connection.commit()
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    assert preview["eligible"] is False
    assert preview["resolution_schema_ready"] is False
    assert "RESOLUTION_SCHEMA_INVALID" in preview["reason_codes"]
    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    _assert_error_mentions(error.value, "SCHEMA", "INVALID")
    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 0


def test_resolution_rejects_incomplete_evidence_or_quiescence_schema(
    boundary_connection,
) -> None:
    connection = boundary_connection
    connection.execute("DROP TABLE runtime_execution_locks")
    connection.commit()
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    assert preview["eligible"] is False
    assert preview["source_schema_ready"] is False
    assert any(
        "RUNTIME_EXECUTION_LOCKS" in code for code in preview["reason_codes"]
    )
    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    _assert_error_mentions(error.value, "SOURCE", "SCHEMA")
    status = get_order_broker_boundary_status(connection)
    assert status["fast_0_status"] == "BLOCKED"
    assert status["qualification_block_new_order_routing"] is True
    assert status["resolution_source_schema_ready"] is False


def test_resolution_rejects_recent_gateway_activity(boundary_connection) -> None:
    connection = boundary_connection
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES ('last_heartbeat_at', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (
            datetime_to_wire(utc_now()),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    assert error.value.code == "RUNTIME_NOT_QUIESCENT"
    assert "RECENT_GATEWAY_ACTIVITY_PRESENT" in error.value.details[
        "reason_codes"
    ]


def test_resolution_rejects_runnable_target_command(boundary_connection) -> None:
    connection = boundary_connection
    connection.execute(
        "UPDATE gateway_commands SET status = 'QUEUED' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.commit()
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _record_resolution(connection, preview=preview)

    assert error.value.code == "RUNTIME_NOT_QUIESCENT"
    assert "TARGET_COMMAND_STATUS_RUNNABLE_OR_UNSAFE" in error.value.details[
        "reason_codes"
    ]


def test_revoke_is_append_only_and_restores_effective_block(boundary_connection) -> None:
    connection = boundary_connection
    raw_before = _raw_boundary(connection)
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    resolved = _record_resolution(connection, preview=preview)
    revoke_preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)

    revoked = revoke_order_broker_boundary_resolution(
        connection,
        command_id=COMMAND_ID,
        request_id="request-revoke-001",
        expected_fingerprint=revoke_preview["source_boundary_fingerprint"],
        supersedes_resolution_id=resolved["resolution_id"],
        reason_code="OPERATOR_REVOKED_BROKER_NOT_REACHED",
        evidence_type="OPERATOR_CORRECTION",
        evidence_ref="fast0r1-revoke-001",
        evidence_sha256=hashlib.sha256(b"sanitized-revoke-evidence").hexdigest(),
        operator_id=OPERATOR_ID,
    )

    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)
    ledger = connection.execute(
        """
        SELECT sequence_no, action, supersedes_resolution_id
        FROM gateway_order_broker_boundary_resolutions
        WHERE command_id = ?
        ORDER BY sequence_no
        """,
        (COMMAND_ID,),
    ).fetchall()

    assert revoked["idempotent_replay"] is False
    assert _raw_boundary(connection) == raw_before
    assert [(row["sequence_no"], row["action"]) for row in ledger] == [
        (1, "RESOLVE_BROKER_NOT_REACHED"),
        (2, "REVOKE"),
    ]
    assert ledger[1]["supersedes_resolution_id"] == resolved["resolution_id"]
    assert effective["raw_state"] == "UNCONFIRMED"
    assert effective["effective_state"] == "UNCONFIRMED"
    assert effective["resolution_effective"] is False
    assert status["effective_unconfirmed_count"] == 1
    assert status["effective_resolution_count"] == 0
    assert status["effective_block_new_order_routing"] is True


def test_resolution_ledger_rejects_direct_update_and_delete(boundary_connection) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    result = _record_resolution(connection, preview=preview)

    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(
            """
            UPDATE gateway_order_broker_boundary_resolutions
            SET reason_code = 'MUTATED'
            WHERE resolution_id = ?
            """,
            (result["resolution_id"],),
        )
    connection.rollback()

    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(
            "DELETE FROM gateway_order_broker_boundary_resolutions WHERE resolution_id = ?",
            (result["resolution_id"],),
        )
    connection.rollback()

    assert _count(connection, "gateway_order_broker_boundary_resolutions") == 1


def test_invalid_direct_ledger_insert_fails_closed(boundary_connection) -> None:
    connection = boundary_connection
    empty_snapshot = "{}"
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundary_resolutions (
            resolution_id, request_id, request_hash, command_id, sequence_no,
            action, resolution_type, supersedes_resolution_id, reason_code,
            evidence_type, evidence_ref, evidence_sha256, operator_id,
            source_boundary_fingerprint, source_boundary_updated_at,
            boundary_snapshot_json, created_at, live_sim_only,
            live_real_allowed
        )
        VALUES (
            'boundary_resolution_invalid', 'request-invalid-direct', ?, ?, 1,
            'RESOLVE_BROKER_NOT_REACHED', 'BROKER_NOT_REACHED', NULL,
            'OPERATOR_CONFIRMED_BROKER_NOT_REACHED',
            'SIMULATION_HTS_ORDER_HISTORY_EXPORT', 'invalid-direct-ledger', ?,
            'operator.fast0r1', ?, ?, ?, ?, 1, 0
        )
        """,
        (
            "0" * 64,
            COMMAND_ID,
            EVIDENCE_SHA256,
            hashlib.sha256(empty_snapshot.encode("utf-8")).hexdigest(),
            NOW,
            empty_snapshot,
            NOW,
        ),
    )
    connection.commit()

    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    status = get_order_broker_boundary_status(connection)

    assert effective["effective_state"] == "UNCONFIRMED"
    assert effective["resolution_chain_valid"] is False
    assert status["status"] == "FAIL"
    assert status["effective_block_new_order_routing"] is True
    assert status["invalid_resolution_chain_count"] == 1


def test_unknown_raw_boundary_state_fails_closed(boundary_connection) -> None:
    connection = boundary_connection
    connection.execute(
        "UPDATE gateway_commands SET status = 'FAILED' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET state = 'UNKNOWN'
        WHERE command_id = ?
        """,
        (COMMAND_ID,),
    )
    connection.commit()

    status = get_order_broker_boundary_status(connection)

    assert status["status"] == "FAIL"
    assert status["unknown_state_count"] == 1
    assert status["qualification_block_new_order_routing"] is True
    assert status["effective_block_new_order_routing"] is True
    assert "ORDER_BROKER_BOUNDARY_STATE_INVALID" in status["reason_codes"]


def test_status_rejects_boundary_linked_to_non_order_command(
    boundary_connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        "UPDATE gateway_commands SET command_type = 'request_tr' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.commit()

    status = get_order_broker_boundary_status(connection)

    assert status["status"] == "FAIL"
    assert status["unexpected_boundary_count"] == 1
    assert status["linked_command_type_invalid_count"] == 1
    assert status["qualification_block_new_order_routing"] is True


def test_status_rejects_boundary_and_command_type_mismatch(
    boundary_connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET command_type = 'cancel_order'
        WHERE command_id = ?
        """,
        (COMMAND_ID,),
    )
    connection.commit()

    status = get_order_broker_boundary_status(connection)

    assert status["status"] == "FAIL"
    assert status["linked_command_type_mismatch_count"] == 1
    assert "ORDER_BOUNDARY_COMMAND_TYPE_MISMATCH" in status["reason_codes"]


def test_status_maps_acked_command_to_broker_accepted_and_requires_pre_ack(
    boundary_connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        "UPDATE gateway_commands SET status = 'ACKED' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET state = 'CLAIMED', pre_ack_recorded_at = NULL
        WHERE command_id = ?
        """,
        (COMMAND_ID,),
    )
    connection.commit()

    status = get_order_broker_boundary_status(connection)

    assert status["status"] == "FAIL"
    assert status["command_state_mismatch_count"] == 1
    assert status["durable_pre_ack_gap_count"] == 1


def test_public_views_and_ledger_snapshot_do_not_copy_sensitive_raw_values(
    boundary_connection,
) -> None:
    connection = boundary_connection
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    result = _record_resolution(connection, preview=preview)
    effective = get_effective_order_broker_boundary(connection, COMMAND_ID)
    listed = list_order_broker_boundaries(connection, limit=10)
    ledger = _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundary_resolutions WHERE command_id = ?",
        (COMMAND_ID,),
    )

    public_serialized = _json_text([preview, result, effective, listed])
    ledger_serialized = _json_text(ledger)
    for secret in (
        RAW_ACCOUNT_SENTINEL,
        RAW_IDEMPOTENCY_SENTINEL,
        RAW_PAYLOAD_SECRET_SENTINEL,
    ):
        assert secret not in public_serialized
        assert secret not in ledger_serialized

    snapshot = json.loads(str(ledger["boundary_snapshot_json"]))
    assert "account_id" not in snapshot
    assert "idempotency_key" not in snapshot
    assert "payload_json" not in snapshot
    assert "pre_ack_payload_json" not in snapshot
    assert "latest_payload_json" not in snapshot


def test_schema_reinitialize_is_idempotent_and_does_not_backfill_resolution(
    tmp_path,
) -> None:
    db_path = tmp_path / "schema-reinitialize.sqlite3"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    raw_before = _raw_boundary(connection)
    connection.close()

    first = initialize_database(db_path)
    first_raw = _raw_boundary(first)
    first_ledger_count = _count(first, "gateway_order_broker_boundary_resolutions")
    first_triggers = _resolution_trigger_names(first)
    first.close()

    second = initialize_database(db_path)
    second_raw = _raw_boundary(second)
    second_ledger_count = _count(second, "gateway_order_broker_boundary_resolutions")
    second_triggers = _resolution_trigger_names(second)
    schema_version = second.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()["value"]
    second.close()

    assert SCHEMA_VERSION >= 60
    assert schema_version == str(SCHEMA_VERSION)
    assert first_raw == raw_before
    assert second_raw == raw_before
    assert first_ledger_count == 0
    assert second_ledger_count == 0
    assert first_triggers == second_triggers == {
        "trg_gateway_order_boundary_resolutions_no_delete",
        "trg_gateway_order_boundary_resolutions_no_update",
    }


def _record_resolution(
    connection: sqlite3.Connection,
    *,
    preview: Mapping[str, Any],
    request_id: str = "request-resolve-001",
    evidence_sha256: str = EVIDENCE_SHA256,
    evidence_ref: str = EVIDENCE_REF,
    operator_id: str = OPERATOR_ID,
) -> dict[str, Any]:
    return record_order_broker_boundary_resolution(
        connection,
        command_id=COMMAND_ID,
        request_id=request_id,
        expected_fingerprint=str(preview["source_boundary_fingerprint"]),
        reason_code=REASON_CODE,
        evidence_type=EVIDENCE_TYPE,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        operator_id=operator_id,
    )


def _insert_unconfirmed_boundary(connection: sqlite3.Connection) -> None:
    payload_json = json.dumps(
        {
            "account_id": RAW_ACCOUNT_SENTINEL,
            "code": "005930",
            "idempotency_key": RAW_IDEMPOTENCY_SENTINEL,
            "metadata": {
                "debug_secret": RAW_PAYLOAD_SECRET_SENTINEL,
                "live_real_allowed": False,
                "live_sim_only": True,
            },
            "mode": "LIVE_SIM",
            "side": "BUY",
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id, command_type, source, status, idempotency_key,
            payload_json, payload_hash, created_at, dispatched_at, expires_at,
            attempts, last_error
        )
        VALUES (?, 'send_order', 'live_sim', 'UNCONFIRMED', ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            COMMAND_ID,
            RAW_IDEMPOTENCY_SENTINEL,
            payload_json,
            payload_hash,
            NOW,
            NOW,
            NOW,
            "Gateway order dispatch timed out; reconciliation required.",
        ),
    )
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundaries (
            command_id, idempotency_key, command_type, source, state, attempts,
            account_id, code, side, broker_order_no, broker_result_code,
            broker_message, claimed_at, gateway_started_at, pre_ack_recorded_at,
            broker_accepted_at, chejan_confirmed_at, unconfirmed_at,
            last_event_id, pre_ack_payload_json, latest_payload_json,
            created_at, updated_at, live_sim_only, live_real_allowed
        )
        VALUES (
            ?, ?, 'send_order', 'live_sim', 'UNCONFIRMED', 1,
            ?, '005930', 'BUY', NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL, ?,
            NULL, '{}', ?, ?, ?, 1, 0
        )
        """,
        (
            COMMAND_ID,
            RAW_IDEMPOTENCY_SENTINEL,
            RAW_ACCOUNT_SENTINEL,
            NOW,
            NOW,
            payload_json,
            NOW,
            NOW,
        ),
    )
    connection.commit()


def _insert_gateway_event(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    event_id: str | None = None,
) -> None:
    resolved_event_id = event_id or f"evt-{event_type.replace('_', '-')}"
    payload_json = json.dumps(
        {"broker_order_no": "SIM-LATE-001"} if "chejan" in event_type else {},
        separators=(",", ":"),
        sort_keys=True,
    )
    connection.execute(
        """
        INSERT INTO gateway_events (
            event_id, event_type, source, command_id, idempotency_key,
            event_ts, received_at, payload_json, status, error_message
        )
        VALUES (?, ?, 'test-gateway', ?, NULL, ?, ?, ?, 'ACCEPTED', NULL)
        """,
        (resolved_event_id, event_type, COMMAND_ID, LATER, LATER, payload_json),
    )
    connection.commit()


def _insert_live_sim_order(
    connection: sqlite3.Connection,
    *,
    broker_order_no: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, gateway_command_id,
            trade_date, account_id, code, name, side, order_type, quantity,
            limit_price, notional, status, broker_order_no, filled_quantity,
            remaining_quantity, idempotency_key, created_at, command_queued_at
        )
        VALUES (
            'order-fast0r1', 'intent-fast0r1', ?, '2026-07-14', ?, '005930',
            'TEST', 'BUY', 'LIMIT', 1, 70000, 70000, 'COMMAND_QUEUED', ?, 0, 1,
            'order-idem-fast0r1', ?, ?
        )
        """,
        (COMMAND_ID, RAW_ACCOUNT_SENTINEL, broker_order_no, NOW, NOW),
    )


def _insert_live_sim_execution(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id, broker_execution_id, execution_key,
            live_sim_order_id, live_sim_intent_id, broker_order_no, account_id,
            code, side, quantity, price, notional, executed_at, raw_event_json,
            live_sim_only
        )
        VALUES (
            'execution-fast0r1', 'broker-execution-fast0r1',
            'execution-key-fast0r1', 'order-fast0r1', 'intent-fast0r1',
            'SIM-BROKER-EXEC-001', ?, '005930', 'BUY', 1, 70000, 70000, ?, '{}', 1
        )
        """,
        (RAW_ACCOUNT_SENTINEL, LATER),
    )


def _raw_boundary(connection: sqlite3.Connection) -> dict[str, Any]:
    return _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (COMMAND_ID,),
    )


def _row(
    connection: sqlite3.Connection,
    query: str,
    params: tuple[object, ...],
) -> dict[str, Any]:
    row = connection.execute(query, params).fetchone()
    assert row is not None
    return {key: row[key] for key in row.keys()}


def _count(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(
        f'SELECT COUNT(*) AS count FROM "{table_name}"'
    ).fetchone()
    return int(row["count"])


def _resolution_trigger_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger'
              AND tbl_name = 'gateway_order_broker_boundary_resolutions'
            """
        ).fetchall()
    }


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _assert_error_mentions(
    error: OrderBrokerBoundaryResolutionError,
    *expected_fragments: str,
) -> None:
    text = f"{error.code} {error}".upper()
    assert any(fragment.upper() in text for fragment in expected_fragments)


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
