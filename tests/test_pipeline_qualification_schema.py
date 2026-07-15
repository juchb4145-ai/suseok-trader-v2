from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest
from storage.sqlite import (
    SCHEMA_VERSION,
    initialize_database,
    initialize_database_for_offline_migration,
    migrate_schema_61_to_62,
)

TABLE = "pipeline_coherency_dispositions"
ALLOWED_ACTIONS = (
    "DISPOSE_EXPIRED_PLAN_READY",
    "DISPOSE_ORPHAN_PIPELINE_OBSERVATION",
    "DISPOSE_STALE_OTHER_DATE",
    "REVOKE",
)
DEFAULTED_COLUMNS = {
    "evidence_json",
    "safety_snapshot_json",
    "observe_only",
    "live_sim_allowed",
    "live_real_allowed",
    "order_commands_allowed",
    "not_order_intent",
    "no_order_side_effects",
    "auto_run_evaluation",
}


def test_schema_61_to_62_migration_is_additive_and_initializer_is_noop(tmp_path) -> None:
    db_path = tmp_path / "schema-61-to-62.sqlite3"
    source = initialize_database(db_path)
    source.execute(f"DROP TABLE {TABLE}")
    source.execute(
        """
        CREATE TABLE schema61_migration_sentinel (
            sentinel_id TEXT PRIMARY KEY,
            payload_text TEXT NOT NULL,
            payload_blob BLOB NOT NULL,
            payload_integer INTEGER NOT NULL
        )
        """
    )
    source.execute(
        """
        INSERT INTO schema61_migration_sentinel (
            sentinel_id, payload_text, payload_blob, payload_integer
        ) VALUES (?, ?, ?, ?)
        """,
        ("sentinel-61", "preserve-me", b"\x00schema-61\xff", 62_061),
    )
    source.execute("UPDATE app_metadata SET value = '61' WHERE key = 'schema_version'")
    source.commit()
    tables_before = _user_tables(source)
    sentinel_before = _sentinel_fingerprint(source)
    source.close()
    source_sha256 = hashlib.sha256(db_path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="exact preflight-qualified"):
        initialize_database(db_path)
    with pytest.raises(RuntimeError, match="generic offline migration is blocked"):
        initialize_database_for_offline_migration(db_path)
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == source_sha256
    assert all(
        not (db_path.parent / f"{db_path.name}{suffix}").exists()
        for suffix in ("-wal", "-shm", "-journal")
    )

    guarded = sqlite3.connect(db_path)
    assert _schema_version(guarded) == "61"
    assert TABLE not in _user_tables(guarded)
    assert _sentinel_fingerprint(guarded) == sentinel_before
    guarded.close()

    first = sqlite3.connect(db_path)
    first.execute("BEGIN EXCLUSIVE")
    migrate_schema_61_to_62(first)
    first.commit()
    assert SCHEMA_VERSION == 62
    assert _schema_version(first) == "62"
    assert _user_tables(first) - tables_before == {TABLE}
    assert _row_count(first) == 0
    assert _sentinel_fingerprint(first) == sentinel_before
    objects_after_first = _table_objects(first)
    first.close()

    second = initialize_database(db_path)
    assert _schema_version(second) == "62"
    assert _user_tables(second) - tables_before == {TABLE}
    assert _row_count(second) == 0
    assert _sentinel_fingerprint(second) == sentinel_before
    assert _table_objects(second) == objects_after_first
    second.close()


@pytest.mark.parametrize("schema_version", ("60", "63"))
def test_initialize_database_rejects_existing_non_target_schema_without_writes(
    tmp_path,
    schema_version: str,
) -> None:
    db_path = tmp_path / f"schema-{schema_version}-startup-guard.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        "UPDATE app_metadata SET value = ? WHERE key = 'schema_version'",
        (schema_version,),
    )
    connection.commit()
    connection.close()
    before_sha256 = hashlib.sha256(db_path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="existing database schema mismatch"):
        initialize_database(db_path)

    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before_sha256
    assert all(
        not (db_path.parent / f"{db_path.name}{suffix}").exists()
        for suffix in ("-wal", "-shm", "-journal")
    )
    check = sqlite3.connect(
        f"file:{db_path.resolve().as_posix()}?mode=ro&immutable=1",
        uri=True,
    )
    try:
        assert _schema_version(check) == schema_version
    finally:
        check.close()


@pytest.mark.parametrize("app_name", (None, "different-application"))
def test_initialize_database_rejects_invalid_existing_app_identity_without_writes(
    tmp_path,
    app_name: str | None,
) -> None:
    db_path = tmp_path / "invalid-app-identity.sqlite3"
    connection = initialize_database(db_path)
    if app_name is None:
        connection.execute("DELETE FROM app_metadata WHERE key = 'app_name'")
    else:
        connection.execute(
            "UPDATE app_metadata SET value = ? WHERE key = 'app_name'",
            (app_name,),
        )
    connection.commit()
    connection.close()
    before_sha256 = hashlib.sha256(db_path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="existing database schema mismatch"):
        initialize_database(db_path)

    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before_sha256
    assert all(
        not (db_path.parent / f"{db_path.name}{suffix}").exists()
        for suffix in ("-wal", "-shm", "-journal")
    )


def test_initialize_database_rejects_missing_schema_metadata_without_writes(
    tmp_path,
) -> None:
    db_path = tmp_path / "missing-schema-metadata.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("DROP TABLE app_metadata")
    connection.commit()
    connection.close()
    before_sha256 = hashlib.sha256(db_path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="no schema metadata"):
        initialize_database(db_path)

    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before_sha256
    assert all(
        not (db_path.parent / f"{db_path.name}{suffix}").exists()
        for suffix in ("-wal", "-shm", "-journal")
    )


def test_pipeline_disposition_schema_defaults_indexes_and_append_only_guards(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "pipeline-dispositions.sqlite3")
    connection.execute("PRAGMA foreign_keys=ON")

    columns = connection.execute(f"PRAGMA table_info({TABLE})").fetchall()
    assert [str(row["name"]) for row in columns] == [
        "disposition_id",
        "request_id",
        "request_hash",
        "candidate_instance_id",
        "subject_key",
        "trade_date",
        "order_plan_id",
        "sequence_no",
        "action",
        "supersedes_disposition_id",
        "reason_code",
        "operator_id",
        "expected_pipeline_fingerprint",
        "expected_subject_version",
        "expected_source_fingerprint",
        "expected_candidate_fingerprint",
        "expected_downstream_fingerprint",
        "expected_boundary_fingerprint",
        "evidence_type",
        "evidence_ref",
        "evidence_sha256",
        "evidence_json",
        "safety_snapshot_json",
        "created_at",
        "observe_only",
        "live_sim_allowed",
        "live_real_allowed",
        "order_commands_allowed",
        "not_order_intent",
        "no_order_side_effects",
        "auto_run_evaluation",
    ]
    defaults = {str(row["name"]): row["dflt_value"] for row in columns}
    expected_defaults = {
        "evidence_json": "'{}'",
        "safety_snapshot_json": "'{}'",
        "observe_only": "1",
        "live_sim_allowed": "0",
        "live_real_allowed": "0",
        "order_commands_allowed": "0",
        "not_order_intent": "1",
        "no_order_side_effects": "1",
        "auto_run_evaluation": "0",
    }
    assert {key: defaults[key] for key in expected_defaults} == expected_defaults

    assert _index_key_columns(connection, "idx_pipeline_coherency_disposition_effective") == [
        ("subject_key", False),
        ("sequence_no", True),
    ]
    assert _index_key_columns(connection, "idx_pipeline_coherency_disposition_action_created") == [
        ("action", False),
        ("created_at", True),
    ]
    trigger_names = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?",
            (TABLE,),
        )
    }
    assert trigger_names == {
        "trg_pipeline_coherency_dispositions_no_update",
        "trg_pipeline_coherency_dispositions_no_delete",
    }

    _insert_disposition(connection, "one", omit_defaults=True)
    stored = connection.execute(
        f"""
        SELECT evidence_json, safety_snapshot_json, observe_only,
               live_sim_allowed, live_real_allowed, order_commands_allowed,
               not_order_intent, no_order_side_effects, auto_run_evaluation
        FROM {TABLE}
        WHERE disposition_id = 'disposition-one'
        """
    ).fetchone()
    assert tuple(stored) == ("{}", "{}", 1, 0, 0, 0, 1, 1, 0)

    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        _insert_disposition(
            connection,
            "missing-parent",
            supersedes_disposition_id="does-not-exist",
        )
    _insert_disposition(
        connection,
        "revoke",
        candidate_instance_id="candidate-one",
        subject_key="subject-one",
        sequence_no=2,
        action="REVOKE",
        supersedes_disposition_id="disposition-one",
    )

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        _insert_disposition(connection, "duplicate-request", request_id="request-one")
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        _insert_disposition(
            connection,
            "duplicate-sequence",
            subject_key="subject-one",
            sequence_no=2,
        )

    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(
            f"UPDATE {TABLE} SET reason_code = 'MUTATED' WHERE disposition_id = 'disposition-one'"
        )
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute(f"DELETE FROM {TABLE} WHERE disposition_id = 'disposition-one'")
    assert _row_count(connection) == 2
    connection.close()


@pytest.mark.parametrize("action", ALLOWED_ACTIONS)
def test_pipeline_disposition_accepts_only_declared_actions(tmp_path, action: str) -> None:
    connection = initialize_database(tmp_path / f"action-{action}.sqlite3")
    _insert_disposition(connection, action.lower(), action=action)
    assert _row_count(connection) == 1
    connection.close()


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    (
        ("request_hash", "a" * 63),
        ("request_hash", "A" * 64),
        ("expected_pipeline_fingerprint", "g" * 64),
        ("expected_subject_version", "0" * 63 + "x"),
        ("expected_source_fingerprint", "z" * 64),
        ("expected_candidate_fingerprint", "5" * 63),
        ("expected_downstream_fingerprint", "F" * 64),
        ("expected_boundary_fingerprint", b"6" * 64),
        ("evidence_sha256", b"a" * 64),
    ),
)
def test_pipeline_disposition_rejects_non_sha256_values(
    tmp_path,
    column: str,
    invalid_value: object,
) -> None:
    connection = initialize_database(tmp_path / f"invalid-{column}.sqlite3")
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        _insert_disposition(connection, column, **{column: invalid_value})
    assert _row_count(connection) == 0
    connection.close()


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    (
        ("sequence_no", 0),
        ("sequence_no", 1.5),
        ("action", "DISPOSE_WITHOUT_EVIDENCE"),
        ("observe_only", 0),
        ("live_sim_allowed", 1),
        ("live_real_allowed", 1),
        ("order_commands_allowed", 1),
        ("not_order_intent", 0),
        ("no_order_side_effects", 0),
        ("auto_run_evaluation", 1),
    ),
)
def test_pipeline_disposition_rejects_unsafe_values(
    tmp_path,
    column: str,
    invalid_value: object,
) -> None:
    connection = initialize_database(tmp_path / f"unsafe-{column}.sqlite3")
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        _insert_disposition(connection, column, **{column: invalid_value})
    assert _row_count(connection) == 0
    connection.close()


def _insert_disposition(
    connection: sqlite3.Connection,
    tag: str,
    *,
    omit_defaults: bool = False,
    **overrides: object,
) -> None:
    values: dict[str, object] = {
        "disposition_id": f"disposition-{tag}",
        "request_id": f"request-{tag}",
        "request_hash": "1" * 64,
        "candidate_instance_id": f"candidate-{tag}",
        "subject_key": f"subject-{tag}",
        "trade_date": "2026-07-15",
        "order_plan_id": None,
        "sequence_no": 1,
        "action": "DISPOSE_EXPIRED_PLAN_READY",
        "supersedes_disposition_id": None,
        "reason_code": "FAST_0R3_QUALIFICATION",
        "operator_id": "operator-test",
        "expected_pipeline_fingerprint": "2" * 64,
        "expected_subject_version": "3" * 64,
        "expected_source_fingerprint": "5" * 64,
        "expected_candidate_fingerprint": "6" * 64,
        "expected_downstream_fingerprint": "7" * 64,
        "expected_boundary_fingerprint": "8" * 64,
        "evidence_type": "STRICT_READ_ONLY_SNAPSHOT",
        "evidence_ref": f"evidence/{tag}.json",
        "evidence_sha256": "4" * 64,
        "evidence_json": "{}",
        "safety_snapshot_json": "{}",
        "created_at": "2026-07-15T10:00:00Z",
        "observe_only": 1,
        "live_sim_allowed": 0,
        "live_real_allowed": 0,
        "order_commands_allowed": 0,
        "not_order_intent": 1,
        "no_order_side_effects": 1,
        "auto_run_evaluation": 0,
    }
    values.update(overrides)
    if omit_defaults:
        for column in DEFAULTED_COLUMNS:
            values.pop(column)
    columns = tuple(values)
    connection.execute(
        f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        tuple(values[column] for column in columns),
    )


def _schema_version(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()
    assert row is not None
    return str(row[0])


def _row_count(connection: sqlite3.Connection) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0])


def _user_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _table_objects(connection: sqlite3.Connection) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (str(row[0]), str(row[1]), str(row[2] or ""))
        for row in connection.execute(
            "SELECT type, name, sql FROM sqlite_master WHERE tbl_name = ? ORDER BY type, name",
            (TABLE,),
        )
    )


def _index_key_columns(
    connection: sqlite3.Connection,
    index_name: str,
) -> list[tuple[str, bool]]:
    return [
        (str(row["name"]), bool(row["desc"]))
        for row in connection.execute(f"PRAGMA index_xinfo({index_name})")
        if int(row["key"] or 0) == 1
    ]


def _sentinel_fingerprint(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        """
        SELECT sentinel_id, payload_text, payload_blob, payload_integer
        FROM schema61_migration_sentinel
        ORDER BY sentinel_id
        """
    ).fetchone()
    assert row is not None
    payload = {
        "sentinel_id": str(row[0]),
        "payload_text": str(row[1]),
        "payload_blob_hex": bytes(row[2]).hex(),
        "payload_integer": int(row[3]),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
