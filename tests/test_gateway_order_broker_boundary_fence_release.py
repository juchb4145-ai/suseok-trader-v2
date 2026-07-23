from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest
import storage.gateway_order_broker_boundary as boundary_storage
from storage.gateway_order_broker_boundary import (
    FENCE_EVENT_TABLE,
    FENCE_EXPECTED_APP_NAME,
    FENCE_EXPECTED_SCHEMA_VERSION,
    OrderBrokerBoundaryResolutionError,
    build_order_broker_boundary_fence_approval,
    ensure_gateway_order_broker_boundary_fence_event_schema,
    get_order_broker_boundary_fence_approval_binding,
    get_order_broker_boundary_status,
    preview_order_broker_boundary_fence_reinstate,
    preview_order_broker_boundary_fence_release,
    preview_order_broker_boundary_resolution,
    record_order_broker_boundary_resolution,
    reinstate_order_broker_boundary_maintenance_fence,
    release_order_broker_boundary_maintenance_fence,
)
from storage.sqlite import initialize_database
from tests.test_gateway_order_broker_boundary_resolution import (
    COMMAND_ID,
    EVIDENCE_REF,
    EVIDENCE_SHA256,
    EVIDENCE_TYPE,
    OPERATOR_ID,
    REASON_CODE,
    _insert_gateway_event,
    _insert_unconfirmed_boundary,
)

ARBITRARY_APPROVAL_SHA256 = hashlib.sha256(b"public-fence-release-approval").hexdigest()
FENCE_EVIDENCE_SHA256 = hashlib.sha256(b"sanitized-fence-release-evidence").hexdigest()


@pytest.fixture
def boundary_connection(tmp_path, monkeypatch):
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    connection = initialize_database(tmp_path / "broker-fence-release.sqlite3")
    ensure_gateway_order_broker_boundary_fence_event_schema(connection)
    _insert_unconfirmed_boundary(connection)
    try:
        yield connection
    finally:
        connection.close()


def test_release_is_append_only_and_clears_only_maintenance_fence(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    raw_boundary_before = _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
        (COMMAND_ID,),
    )
    resolution_before = _row(
        connection,
        "SELECT * FROM gateway_order_broker_boundary_resolutions WHERE command_id = ?",
        (COMMAND_ID,),
    )

    assert preview["eligible"] is True
    released = _release(connection, preview=preview)
    status = get_order_broker_boundary_status(connection)

    assert released["action"] == "RELEASE"
    assert released["maintenance_fence_released"] is True
    assert released["maintenance_fence_active"] is False
    assert len(released["approval_sha256"]) == 64
    assert released["evidence_sha256"] == FENCE_EVIDENCE_SHA256
    assert status["resolution_maintenance_fence_active"] is False
    assert status["resolution_maintenance_fence_active_count"] == 0
    assert status["resolution_maintenance_fence_released_count"] == 1
    assert status["active_fence_release_count"] == 1
    assert status["effective_block_new_order_routing"] is False
    assert _count(connection, FENCE_EVENT_TABLE) == 1
    assert (
        _row(
            connection,
            "SELECT * FROM gateway_order_broker_boundaries WHERE command_id = ?",
            (COMMAND_ID,),
        )
        == raw_boundary_before
    )
    assert (
        _row(
            connection,
            "SELECT * FROM gateway_order_broker_boundary_resolutions WHERE command_id = ?",
            (COMMAND_ID,),
        )
        == resolution_before
    )


def test_release_request_id_is_idempotent_and_conflict_safe(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    first = _release(connection, preview=preview)
    replay = _release(connection, preview=preview)

    assert replay["fence_event_id"] == first["fence_event_id"]
    assert replay["idempotent_replay"] is True
    assert replay["idempotent_replay_effective"] is True
    assert _count(connection, FENCE_EVENT_TABLE) == 1

    with pytest.raises(OrderBrokerBoundaryResolutionError) as error:
        _release(
            connection,
            preview=preview,
            evidence_sha256=hashlib.sha256(b"different-evidence").hexdigest(),
        )
    assert error.value.code == "REQUEST_CONFLICT"
    assert _count(connection, FENCE_EVENT_TABLE) == 1


def test_release_rejects_stale_resolution_binding_and_non_lowercase_sha(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as stale:
        _release(
            connection,
            preview=preview,
            resolution_request_hash="0" * 64,
        )
    assert stale.value.code == "FENCE_RELEASE_NOT_PROVABLE"

    with pytest.raises(OrderBrokerBoundaryResolutionError) as invalid_sha:
        _release(
            connection,
            preview=preview,
            approval_sha256=ARBITRARY_APPROVAL_SHA256.upper(),
        )
    assert invalid_sha.value.code == "INVALID_APPROVAL_SHA256"
    assert _count(connection, FENCE_EVENT_TABLE) == 0


def test_direct_storage_api_rejects_arbitrary_lowercase_approval_sha(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as rejected:
        _release(
            connection,
            preview=preview,
            approval_sha256=ARBITRARY_APPROVAL_SHA256,
        )

    assert rejected.value.code == "APPROVAL_SHA256_MISMATCH"
    assert _count(connection, FENCE_EVENT_TABLE) == 0


def test_reinstate_appends_and_restores_fence(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    release_preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    _release(connection, preview=release_preview)
    reinstate_preview = preview_order_broker_boundary_fence_reinstate(connection, COMMAND_ID)

    reinstated = _reinstate(connection, preview=reinstate_preview)
    replay = _reinstate(connection, preview=reinstate_preview)
    status = get_order_broker_boundary_status(connection)

    assert reinstated["action"] == "REINSTATE"
    assert reinstated["maintenance_fence_released"] is False
    assert reinstated["maintenance_fence_active"] is True
    assert replay["fence_event_id"] == reinstated["fence_event_id"]
    assert replay["idempotent_replay"] is True
    assert replay["idempotent_replay_effective"] is True
    assert status["resolution_maintenance_fence_active"] is True
    assert status["effective_block_new_order_routing"] is True
    assert status["active_fence_release_count"] == 0
    assert _count(connection, FENCE_EVENT_TABLE) == 2


def test_late_broker_evidence_invalidates_release_fail_closed(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    _release(connection, preview=preview)

    _insert_gateway_event(connection, event_type="command_ack")
    status = get_order_broker_boundary_status(connection)
    public_preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)

    assert status["resolution_maintenance_fence_active"] is True
    assert status["effective_block_new_order_routing"] is True
    assert status["invalidated_fence_release_count"] == 1
    assert "ORDER_BOUNDARY_FENCE_RELEASE_INVALIDATED" in status["warning_codes"]
    assert public_preview["maintenance_fence_released"] is False
    assert public_preview["fence_release_status"] == "RELEASE_INVALIDATED"
    assert public_preview["broker_reach_reason_codes"]


def test_release_expires_on_next_market_trade_date_fail_closed(
    boundary_connection: sqlite3.Connection,
    monkeypatch,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    _release(connection, preview=preview)

    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-24")
    replay = _release(connection, preview=preview)
    status = get_order_broker_boundary_status(connection)
    next_day = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)

    assert status["resolution_maintenance_fence_active"] is True
    assert status["effective_block_new_order_routing"] is True
    assert replay["idempotent_replay"] is True
    assert replay["idempotent_replay_effective"] is False
    assert _count(connection, FENCE_EVENT_TABLE) == 1
    assert next_day["maintenance_fence_released"] is False
    assert next_day["fence_release_status"] == "RELEASE_INVALIDATED"
    assert "FENCE_RELEASE_TRADE_DATE_EXPIRED" in next_day["fence_release_reason_codes"]


def test_release_projection_rejects_copied_database_identity(
    tmp_path,
    monkeypatch,
) -> None:
    import shutil

    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    source_path = tmp_path / "source.sqlite3"
    clone_path = tmp_path / "clone.sqlite3"
    connection = initialize_database(source_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    _release(connection, preview=preview)
    connection.close()
    shutil.copy2(source_path, clone_path)

    clone = sqlite3.connect(clone_path)
    clone.row_factory = sqlite3.Row
    clone.execute("PRAGMA foreign_keys=ON")
    status = get_order_broker_boundary_status(clone)
    clone.close()

    assert status["resolution_maintenance_fence_active"] is True
    assert status["effective_block_new_order_routing"] is True
    assert status["invalidated_fence_release_count"] == 1


def test_fence_event_update_delete_and_invalid_chain_are_fail_closed(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    released = _release(connection, preview=preview)

    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            f"UPDATE {FENCE_EVENT_TABLE} SET reason_code = 'changed' WHERE fence_event_id = ?",
            (released["fence_event_id"],),
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            f"DELETE FROM {FENCE_EVENT_TABLE} WHERE fence_event_id = ?",
            (released["fence_event_id"],),
        )
    connection.rollback()

    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(
        f"""
        INSERT INTO {FENCE_EVENT_TABLE} (
            fence_event_id, request_id, request_hash, command_id, command_alias,
            sequence_no, action, supersedes_fence_event_id, resolution_id,
            resolution_request_hash, source_boundary_fingerprint,
            approval_id, approval_trade_date, approval_sha256, evidence_sha256,
            database_identity_sha256, expected_app_name,
            expected_schema_version, expected_gateway_command_total_count,
            expected_order_command_count,
            expected_gateway_command_state_fingerprint,
            reason_code, operator_id,
            created_at, live_sim_only, live_real_allowed
        )
        SELECT
            'boundary_fence_invalid', 'request-fence-invalid', ?, command_id,
            command_alias, 3, 'REINSTATE', fence_event_id, resolution_id,
            resolution_request_hash, source_boundary_fingerprint,
            approval_id, approval_trade_date, approval_sha256, evidence_sha256,
            database_identity_sha256, expected_app_name,
            expected_schema_version, expected_gateway_command_total_count,
            expected_order_command_count,
            expected_gateway_command_state_fingerprint,
            reason_code, operator_id,
            created_at, 1, 0
        FROM gateway_order_broker_boundary_fence_events
        WHERE fence_event_id = ?
        """,
        ("0" * 64, released["fence_event_id"]),
    )
    connection.commit()

    status = get_order_broker_boundary_status(connection)
    assert status["invalid_fence_event_chain_count"] == 1
    assert "ORDER_BOUNDARY_FENCE_EVENT_CHAIN_INVALID" in status["reason_codes"]
    assert status["resolution_maintenance_fence_active"] is True
    assert status["effective_block_new_order_routing"] is True


def test_fence_schema_rejects_extra_trigger_and_missing_foreign_key(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        f"""
        CREATE TRIGGER unexpected_fence_trigger
        AFTER INSERT ON {FENCE_EVENT_TABLE}
        BEGIN
            SELECT 1;
        END
        """
    )
    extra_trigger_status = boundary_storage._fence_event_schema_status(connection)
    assert extra_trigger_status["append_only_triggers_present"] is False
    assert extra_trigger_status["ready"] is False

    isolated = sqlite3.connect(":memory:")
    isolated.row_factory = sqlite3.Row
    isolated.execute(f"CREATE TABLE {FENCE_EVENT_TABLE} (fence_event_id TEXT PRIMARY KEY)")
    missing_fk_status = boundary_storage._fence_event_schema_status(isolated)
    isolated.close()
    assert missing_fk_status["foreign_keys_present"] is False
    assert missing_fk_status["ready"] is False


def test_fence_schema_rejects_hidden_generated_extra_column(
    boundary_connection: sqlite3.Connection,
) -> None:
    connection = boundary_connection
    connection.execute(
        f"""
        ALTER TABLE {FENCE_EVENT_TABLE}
        ADD COLUMN unexpected_generated TEXT
        GENERATED ALWAYS AS (command_alias) VIRTUAL
        """
    )

    generated_column = next(
        row
        for row in connection.execute(f"PRAGMA table_xinfo({FENCE_EVENT_TABLE})")
        if str(row["name"]) == "unexpected_generated"
    )
    status = boundary_storage._fence_event_schema_status(connection)

    assert int(generated_column["hidden"]) == 2
    assert status["required_columns_present"] is False
    assert status["ready"] is False


def test_insert_authorizer_denies_trigger_write_even_if_schema_gate_is_bypassed(
    boundary_connection: sqlite3.Connection,
    monkeypatch,
) -> None:
    connection = boundary_connection
    _resolve(connection)
    preview = preview_order_broker_boundary_fence_release(connection, COMMAND_ID)
    before_status = connection.execute(
        "SELECT status FROM gateway_commands WHERE command_id = ?",
        (COMMAND_ID,),
    ).fetchone()[0]
    connection.execute(
        f"""
        CREATE TRIGGER malicious_fence_side_effect
        AFTER INSERT ON {FENCE_EVENT_TABLE}
        BEGIN
            UPDATE gateway_commands
            SET status = 'MALICIOUS'
            WHERE command_id = NEW.command_id;
        END
        """
    )
    connection.commit()
    monkeypatch.setattr(
        boundary_storage,
        "_fence_event_schema_status",
        lambda _connection: {
            "table_exists": True,
            "required_columns_present": True,
            "required_indexes_present": True,
            "append_only_triggers_present": True,
            "foreign_keys_present": True,
            "constraints_present": True,
            "ready": True,
        },
    )

    with pytest.raises(OrderBrokerBoundaryResolutionError) as denied:
        _release(connection, preview=preview)

    after_status = connection.execute(
        "SELECT status FROM gateway_commands WHERE command_id = ?",
        (COMMAND_ID,),
    ).fetchone()[0]
    assert denied.value.code == "DATABASE_OPERATION_FAILED"
    assert after_status == before_status
    assert _count(connection, FENCE_EVENT_TABLE) == 0


def _resolve(connection: sqlite3.Connection) -> dict[str, Any]:
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    return record_order_broker_boundary_resolution(
        connection,
        command_id=COMMAND_ID,
        request_id="request-resolution-fence",
        expected_fingerprint=str(preview["source_boundary_fingerprint"]),
        reason_code=REASON_CODE,
        evidence_type=EVIDENCE_TYPE,
        evidence_ref=EVIDENCE_REF,
        evidence_sha256=EVIDENCE_SHA256,
        operator_id=OPERATOR_ID,
    )


def _release(
    connection: sqlite3.Connection,
    *,
    preview: Mapping[str, Any],
    request_id: str = "request-fence-release",
    resolution_request_hash: str | None = None,
    approval_sha256: str | None = None,
    evidence_sha256: str = FENCE_EVIDENCE_SHA256,
) -> dict[str, Any]:
    binding = get_order_broker_boundary_fence_approval_binding(connection)
    commands = binding["gateway_commands"]
    effective_resolution_request_hash = resolution_request_hash or str(
        preview["resolution_request_hash"]
    )
    approval = build_order_broker_boundary_fence_approval(
        action="RELEASE",
        approval_id="approval-fence-release",
        request_id=request_id,
        operator_id="operator.bootstrap",
        reason_code="APPROVED_MAINTENANCE_FENCE_RELEASE",
        approval_trade_date="2026-07-23",
        command_alias="U01",
        command_id=COMMAND_ID,
        expected_previous_fence_event_id=preview["expected_previous_fence_event_id"],
        expected_resolution_id=str(preview["resolution_id"]),
        expected_resolution_request_hash=effective_resolution_request_hash,
        expected_source_boundary_fingerprint=str(preview["source_boundary_fingerprint"]),
        evidence_sha256=evidence_sha256,
        database_identity_sha256=str(binding["database_identity_sha256"]),
        expected_app_name=FENCE_EXPECTED_APP_NAME,
        expected_schema_version=FENCE_EXPECTED_SCHEMA_VERSION,
        expected_gateway_command_total_count=int(commands["total_count"]),
        expected_order_command_count=int(commands["order_count"]),
        expected_gateway_command_state_fingerprint=str(commands["state_fingerprint"]),
    )
    return release_order_broker_boundary_maintenance_fence(
        connection,
        command_id=COMMAND_ID,
        request_id=request_id,
        expected_previous_fence_event_id=preview["expected_previous_fence_event_id"],
        expected_resolution_id=str(preview["resolution_id"]),
        expected_resolution_request_hash=(effective_resolution_request_hash),
        expected_source_boundary_fingerprint=str(preview["source_boundary_fingerprint"]),
        approval_id="approval-fence-release",
        command_alias="U01",
        approval_trade_date="2026-07-23",
        approval_sha256=(str(approval["sha256"]) if approval_sha256 is None else approval_sha256),
        evidence_sha256=evidence_sha256,
        database_identity_sha256=str(binding["database_identity_sha256"]),
        expected_app_name=FENCE_EXPECTED_APP_NAME,
        expected_schema_version=FENCE_EXPECTED_SCHEMA_VERSION,
        expected_gateway_command_total_count=int(commands["total_count"]),
        expected_order_command_count=int(commands["order_count"]),
        expected_gateway_command_state_fingerprint=str(commands["state_fingerprint"]),
        reason_code="APPROVED_MAINTENANCE_FENCE_RELEASE",
        operator_id="operator.bootstrap",
    )


def _reinstate(
    connection: sqlite3.Connection,
    *,
    preview: Mapping[str, Any],
) -> dict[str, Any]:
    binding = get_order_broker_boundary_fence_approval_binding(connection)
    commands = binding["gateway_commands"]
    evidence_sha256 = hashlib.sha256(b"reinstate-evidence").hexdigest()
    approval = build_order_broker_boundary_fence_approval(
        action="REINSTATE",
        approval_id="approval-fence-reinstate",
        request_id="request-fence-reinstate",
        operator_id="operator.bootstrap",
        reason_code="OPERATOR_REINSTATED_MAINTENANCE_FENCE",
        approval_trade_date="2026-07-23",
        command_alias="U01",
        command_id=COMMAND_ID,
        expected_previous_fence_event_id=str(preview["expected_release_event_id"]),
        expected_resolution_id=str(preview["resolution_id"]),
        expected_resolution_request_hash=str(preview["resolution_request_hash"]),
        expected_source_boundary_fingerprint=str(preview["source_boundary_fingerprint"]),
        evidence_sha256=evidence_sha256,
        database_identity_sha256=str(binding["database_identity_sha256"]),
        expected_app_name=FENCE_EXPECTED_APP_NAME,
        expected_schema_version=FENCE_EXPECTED_SCHEMA_VERSION,
        expected_gateway_command_total_count=int(commands["total_count"]),
        expected_order_command_count=int(commands["order_count"]),
        expected_gateway_command_state_fingerprint=str(commands["state_fingerprint"]),
    )
    return reinstate_order_broker_boundary_maintenance_fence(
        connection,
        command_id=COMMAND_ID,
        request_id="request-fence-reinstate",
        expected_release_event_id=str(preview["expected_release_event_id"]),
        expected_resolution_id=str(preview["resolution_id"]),
        expected_resolution_request_hash=str(preview["resolution_request_hash"]),
        expected_source_boundary_fingerprint=str(preview["source_boundary_fingerprint"]),
        approval_id="approval-fence-reinstate",
        command_alias="U01",
        approval_trade_date="2026-07-23",
        approval_sha256=str(approval["sha256"]),
        evidence_sha256=evidence_sha256,
        database_identity_sha256=str(binding["database_identity_sha256"]),
        expected_app_name=FENCE_EXPECTED_APP_NAME,
        expected_schema_version=FENCE_EXPECTED_SCHEMA_VERSION,
        expected_gateway_command_total_count=int(commands["total_count"]),
        expected_order_command_count=int(commands["order_count"]),
        expected_gateway_command_state_fingerprint=str(commands["state_fingerprint"]),
        reason_code="OPERATOR_REINSTATED_MAINTENANCE_FENCE",
        operator_id="operator.bootstrap",
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
    row = connection.execute(f'SELECT COUNT(*) AS count FROM "{table_name}"').fetchone()
    return int(row["count"])
