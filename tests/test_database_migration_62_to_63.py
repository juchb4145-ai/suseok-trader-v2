from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from storage.sqlite import (
    APP_NAME,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS,
    SCHEMA_VERSION,
    initialize_database,
    initialize_database_for_offline_migration,
    migrate_schema_62_to_63,
)
from tools import ops_database_migration_62_to_63_apply as apply_tool
from tools import ops_database_migration_62_to_63_preflight as preflight_tool


@pytest.mark.parametrize(
    ("module_name", "required_flag"),
    (
        ("tools.ops_database_migration_62_to_63_preflight", "--clone-db"),
        (
            "tools.ops_database_migration_62_to_63_apply",
            "--preflight-raw-sha256",
        ),
    ),
)
def test_schema_62_to_63_tools_are_executable_modules(
    module_name: str,
    required_flag: str,
) -> None:
    result = subprocess.run(
        [sys.executable, "-m", module_name, "--help"],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
    assert "--source-db" in result.stdout
    assert required_flag in result.stdout


def test_schema_62_startup_fails_closed_and_exact_migration_is_additive(
    tmp_path: Path,
) -> None:
    database = tmp_path / "schema-62.sqlite3"
    _make_schema_62(database)
    source_sha256 = _sha256(database)
    source_state = _order_state(database)

    with pytest.raises(RuntimeError, match="exact preflight-qualified 62 -> 63"):
        initialize_database(database)
    with pytest.raises(RuntimeError, match="generic offline migration is blocked"):
        initialize_database_for_offline_migration(database)
    assert _sha256(database) == source_sha256
    assert _sidecars(database) == []

    connection = sqlite3.connect(database)
    with pytest.raises(RuntimeError, match="requires an active transaction"):
        migrate_schema_62_to_63(connection)
    connection.execute("BEGIN EXCLUSIVE")
    migrate_schema_62_to_63(connection)
    connection.commit()

    assert SCHEMA_VERSION == 63
    assert _schema_version(connection) == "63"
    assert _app_name(connection) == APP_NAME
    assert _migration_objects(connection) == {
        GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE: "table",
        **{name: "index" for name in GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES},
        **{name: "trigger" for name in GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS},
    }
    assert (
        connection.execute(
            f"SELECT COUNT(*) FROM {GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}"
        ).fetchone()[0]
        == 0
    )
    connection.close()
    assert _order_state(database) == source_state

    initialized = initialize_database(database)
    assert _schema_version(initialized) == "63"
    initialized.close()


def test_exact_schema_62_to_63_migration_enforces_source_app_main_and_target_cas(
    tmp_path: Path,
) -> None:
    wrong_source = tmp_path / "wrong-source.sqlite3"
    _make_schema_62(wrong_source)
    connection = sqlite3.connect(wrong_source)
    connection.execute("UPDATE app_metadata SET value = '61' WHERE key = 'schema_version'")
    connection.commit()
    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(RuntimeError, match="source CAS mismatch"):
        migrate_schema_62_to_63(connection)
    connection.rollback()
    connection.close()

    wrong_app = tmp_path / "wrong-app.sqlite3"
    _make_schema_62(wrong_app)
    connection = sqlite3.connect(wrong_app)
    connection.execute("UPDATE app_metadata SET value = 'different-app' WHERE key = 'app_name'")
    connection.commit()
    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(RuntimeError, match="application identity mismatch"):
        migrate_schema_62_to_63(connection)
    connection.rollback()
    connection.close()

    attached = tmp_path / "attached.sqlite3"
    _make_schema_62(attached)
    connection = sqlite3.connect(attached)
    connection.execute("ATTACH DATABASE ':memory:' AS auxiliary")
    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(RuntimeError, match="only the main database"):
        migrate_schema_62_to_63(connection)
    connection.rollback()
    connection.close()

    preexisting = tmp_path / "preexisting.sqlite3"
    _make_schema_62(preexisting)
    connection = sqlite3.connect(preexisting)
    connection.execute(f"CREATE TABLE {GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE} (id TEXT)")
    connection.commit()
    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(RuntimeError, match="target objects must be absent"):
        migrate_schema_62_to_63(connection)
    connection.rollback()
    assert _schema_version(connection) == "62"
    connection.close()


def test_clone_preflight_is_read_only_exact_and_preserves_order_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    clone = tmp_path / "clone.sqlite3"
    _make_schema_62(source)
    source_sha256 = _sha256(source)
    source_state = _order_state(source)

    report = preflight_tool.run_preflight(
        source_db=source,
        clone_db=clone,
        out_dir=tmp_path / "preflight-reports",
    )

    assert report["verdict"]["status"] == "PASS"
    assert report["required_source_schema"] == "62"
    assert report["target_schema_version"] == "63"
    assert report["clone"]["migration_method"] == preflight_tool.EXACT_MIGRATION_METHOD
    assert report["verdict"]["source_files_unchanged"] is True
    assert report["verdict"]["source_original_write_detected"] is False
    assert report["verdict"]["existing_table_content_preserved"] is True
    assert report["verdict"]["order_state_preserved"] is True
    assert report["verdict"]["target_ledger_empty"] is True
    assert report["verdict"]["quick_checks_ok"] is True
    assert report["verdict"]["initializer_noop"] is True
    assert _sha256(source) == source_sha256
    assert _schema_version_path(source) == "62"
    assert _order_state(source) == source_state
    assert _sidecars(source) == []
    assert _schema_version_path(clone) == "63"

    raw = json.loads(Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8"))
    assert preflight_tool.evaluate_report(raw)["status"] == "PASS"


def test_clone_preflight_rejects_non_62_or_preexisting_target_without_source_write(
    tmp_path: Path,
) -> None:
    target_source = tmp_path / "target-source.sqlite3"
    initialize_database(target_source).close()
    target_sha256 = _sha256(target_source)
    with pytest.raises(RuntimeError, match="source CAS mismatch"):
        preflight_tool.run_preflight(
            source_db=target_source,
            clone_db=tmp_path / "target-clone.sqlite3",
            out_dir=tmp_path / "target-reports",
        )
    assert _sha256(target_source) == target_sha256
    assert _sidecars(target_source) == []

    object_source = tmp_path / "object-source.sqlite3"
    _make_schema_62(object_source)
    connection = sqlite3.connect(object_source)
    connection.execute(f"CREATE TABLE {GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE} (id TEXT)")
    connection.commit()
    connection.close()
    object_sha256 = _sha256(object_source)
    with pytest.raises(RuntimeError, match="target objects must be absent"):
        preflight_tool.run_preflight(
            source_db=object_source,
            clone_db=tmp_path / "object-clone.sqlite3",
            out_dir=tmp_path / "object-reports",
        )
    assert _sha256(object_source) == object_sha256
    assert _sidecars(object_source) == []


def test_target_contract_rejects_extra_fence_insert_trigger(
    tmp_path: Path,
) -> None:
    database = tmp_path / "extra-trigger.sqlite3"
    _make_schema_62(database)
    connection = sqlite3.connect(database)
    connection.execute("BEGIN EXCLUSIVE")
    migrate_schema_62_to_63(connection)
    connection.commit()
    connection.execute(
        """
        CREATE TRIGGER unexpected_fence_insert_side_effect
        AFTER INSERT ON gateway_order_broker_boundary_fence_events
        BEGIN
            UPDATE gateway_commands SET last_error = 'unexpected';
        END
        """
    )
    connection.commit()

    contract = preflight_tool._fence_event_contract(connection)

    assert contract["ready"] is False
    assert contract["trigger_set_exact"] is False
    connection.close()


def test_target_contract_rejects_conditional_append_only_trigger(
    tmp_path: Path,
) -> None:
    database = tmp_path / "conditional-trigger.sqlite3"
    initialize_database(database).close()
    trigger_name = "trg_gateway_order_boundary_fence_events_no_update"

    connection = sqlite3.connect(database)
    connection.execute(f"DROP TRIGGER {trigger_name}")
    connection.execute(
        f"""
        CREATE TRIGGER {trigger_name}
        BEFORE UPDATE ON gateway_order_broker_boundary_fence_events
        WHEN OLD.action = 'RELEASE'
        BEGIN
            SELECT RAISE(
                ABORT,
                'gateway order-boundary fence events are append-only'
            );
        END
        """
    )
    connection.commit()

    contract = preflight_tool._fence_event_contract(connection)

    assert contract["ready"] is False
    assert contract["trigger_set_exact"] is True
    assert contract["triggers"][trigger_name] is False
    connection.close()


def test_target_contract_rejects_relaxed_column_and_count_constraints(
    tmp_path: Path,
) -> None:
    database = tmp_path / "relaxed-contract.sqlite3"
    initialize_database(database).close()

    connection = sqlite3.connect(database)
    table_sql = str(
        connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,),
        ).fetchone()[0]
    )
    relaxed_sql = table_sql.replace(
        "command_alias TEXT NOT NULL",
        "command_alias TEXT",
    ).replace(
        "AND expected_order_command_count\n"
        "                            <= expected_gateway_command_total_count",
        "",
    )
    assert relaxed_sql != table_sql
    connection.execute("PRAGMA writable_schema=ON")
    connection.execute(
        "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = ?",
        (relaxed_sql, GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE),
    )
    connection.commit()
    connection.close()

    connection = sqlite3.connect(database)
    contract = preflight_tool._fence_event_contract(connection)

    assert contract["ready"] is False
    assert contract["columns_exact"] is True
    assert contract["column_contracts_exact"] is False
    assert contract["table_constraints_valid"] is False
    connection.close()


def test_target_contract_rejects_hidden_generated_extra_column(
    tmp_path: Path,
) -> None:
    database = tmp_path / "generated-extra-column.sqlite3"
    initialize_database(database).close()

    connection = sqlite3.connect(database)
    connection.execute(
        f"""
        ALTER TABLE {GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}
        ADD COLUMN unexpected_generated TEXT
        GENERATED ALWAYS AS (command_alias) VIRTUAL
        """
    )
    connection.commit()

    assert "unexpected_generated" not in {
        str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE})"
        )
    }
    generated_column = next(
        row
        for row in connection.execute(
            f"PRAGMA table_xinfo({GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE})"
        )
        if str(row[1]) == "unexpected_generated"
    )
    contract = preflight_tool._fence_event_contract(connection)

    assert int(generated_column[6]) == 2
    assert contract["ready"] is False
    assert contract["columns_exact"] is False
    assert contract["column_contracts_exact"] is False
    connection.close()


def test_exact_apply_binds_pass_preflight_and_preserves_backup_and_order_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "apply-source.sqlite3"
    clone = tmp_path / "apply-clone.sqlite3"
    backup = tmp_path / "apply-backup.sqlite3"
    _make_schema_62(source)
    source_sha256 = _sha256(source)
    source_state = _order_state(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    report = apply_tool.run_apply(
        source_db=source,
        preflight_raw=Path(preflight["report_paths"]["raw_json"]),
        expected_preflight_raw_sha256=_sha256(Path(preflight["report_paths"]["raw_json"])),
        backup_db=backup,
        acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "apply-reports",
    )

    assert report["status"] == "PASS"
    assert report["committed"] is True
    assert report["migration"]["source_schema"] == "62"
    assert report["migration"]["target_schema"] == "63"
    assert report["migration"]["authorizer_denial_count"] == 0
    assert report["safety"]["existing_table_content_preserved"] is True
    assert report["safety"]["order_state_preserved"] is True
    assert report["safety"]["target_ledger_empty"] is True
    assert report["source"]["post_commit_quick_check"] == ["ok"]
    assert report["source"]["exclusive_post_commit_quick_check"] == ["ok"]
    assert _schema_version_path(source) == "63"
    assert _schema_version_path(backup) == "62"
    assert _sha256(backup) == source_sha256
    assert _order_state(source) == source_state
    assert _order_state(backup) == source_state
    assert _sidecars(source) == []
    assert _sidecars(backup) == []


def test_apply_rejects_wrong_ack_and_source_fingerprint_drift_before_backup(
    tmp_path: Path,
) -> None:
    source = tmp_path / "guard-source.sqlite3"
    clone = tmp_path / "guard-clone.sqlite3"
    backup = tmp_path / "guard-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    preflight_raw = Path(preflight["report_paths"]["raw_json"])

    with pytest.raises(apply_tool.MigrationApplyError, match="EXACT_ACKNOWLEDGEMENT_REQUIRED"):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge="wrong",
            out_dir=tmp_path / "wrong-ack-reports",
        )
    assert not backup.exists()
    assert _schema_version_path(source) == "62"

    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="PREFLIGHT_RAW_SHA256_MISMATCH",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256="0" * 64,
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "wrong-preflight-sha-reports",
        )
    assert not backup.exists()
    assert _schema_version_path(source) == "62"

    connection = sqlite3.connect(source)
    connection.execute(
        "UPDATE gateway_commands SET status = 'CHANGED' WHERE command_id = 'command-one'"
    )
    connection.commit()
    connection.close()
    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="SOURCE_FINGERPRINT_CHANGED_SINCE_PREFLIGHT",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "drift-reports",
        )
    assert not backup.exists()
    assert _schema_version_path(source) == "62"


def test_apply_authorizer_rejects_unrelated_write_and_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "authorizer-source.sqlite3"
    clone = tmp_path / "authorizer-clone.sqlite3"
    backup = tmp_path / "authorizer-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    def unrelated_write(connection: sqlite3.Connection) -> None:
        connection.execute(
            "UPDATE gateway_commands SET status = 'UNAUTHORIZED' WHERE command_id = 'command-one'"
        )

    monkeypatch.setattr(apply_tool, "migrate_schema_62_to_63", unrelated_write)
    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="MIGRATION_AUTHORIZER_OR_SQL_FAILURE",
    ) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            expected_preflight_raw_sha256=_sha256(Path(preflight["report_paths"]["raw_json"])),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "authorizer-reports",
        )

    assert captured.value.committed is False
    assert _schema_version_path(source) == "62"
    assert _order_state(source)["command_status"] == "PENDING"
    assert backup.exists()


@pytest.mark.parametrize(
    ("failure_point", "expected_code"),
    (
        ("second_target_snapshot", "INJECTED_POSTCOMMIT_TARGET_SNAPSHOT_FAILURE"),
        ("postcommit_sidecar", "INJECTED_POSTCOMMIT_SIDECAR_FAILURE"),
        ("postcommit_leases", "INJECTED_POSTCOMMIT_LEASE_FAILURE"),
    ),
)
def test_postcommit_contract_error_is_reported_as_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    expected_code: str,
) -> None:
    source = tmp_path / f"{failure_point}-source.sqlite3"
    clone = tmp_path / f"{failure_point}-clone.sqlite3"
    backup = tmp_path / f"{failure_point}-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    if failure_point == "second_target_snapshot":
        original_target_snapshot = apply_tool._assert_target_snapshot
        target_snapshot_calls = 0

        def fail_second_target_snapshot(*args, **kwargs) -> None:
            nonlocal target_snapshot_calls
            target_snapshot_calls += 1
            if target_snapshot_calls == 2:
                raise apply_tool.MigrationApplyError(expected_code)
            original_target_snapshot(*args, **kwargs)

        monkeypatch.setattr(
            apply_tool,
            "_assert_target_snapshot",
            fail_second_target_snapshot,
        )
    elif failure_point == "postcommit_sidecar":
        original_no_sidecars = apply_tool.historical_apply._assert_no_sidecars

        def fail_postcommit_sidecar(path: Path, *, code: str) -> None:
            if code == "SOURCE_SIDECAR_LEFT_AFTER_COMMIT":
                raise apply_tool.MigrationApplyError(expected_code)
            original_no_sidecars(path, code=code)

        monkeypatch.setattr(
            apply_tool.historical_apply,
            "_assert_no_sidecars",
            fail_postcommit_sidecar,
        )
    else:
        original_zero_leases = apply_tool.historical_apply._assert_zero_leases
        zero_lease_calls = 0

        def fail_postcommit_leases(lease_counts) -> None:
            nonlocal zero_lease_calls
            zero_lease_calls += 1
            if zero_lease_calls == 3:
                raise apply_tool.MigrationApplyError(expected_code)
            original_zero_leases(lease_counts)

        monkeypatch.setattr(
            apply_tool.historical_apply,
            "_assert_zero_leases",
            fail_postcommit_leases,
        )

    preflight_raw = Path(preflight["report_paths"]["raw_json"])
    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / f"{failure_point}-reports",
        )

    assert captured.value.code == expected_code
    assert captured.value.committed is True
    assert _schema_version_path(source) == "63"
    assert _schema_version_path(backup) == "62"


def test_postcommit_backup_stat_failure_is_reported_as_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "backup-stat-source.sqlite3"
    clone = tmp_path / "backup-stat-clone.sqlite3"
    backup = tmp_path / "backup-stat-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    original_exclusive_snapshot = apply_tool._exclusive_postcommit_snapshot
    original_stat = Path.stat
    postcommit_snapshot_complete = False
    late_backup_stat_calls = 0
    resolved_backup = backup.resolve()

    def mark_postcommit_snapshot_complete(path: Path):
        nonlocal postcommit_snapshot_complete
        result = original_exclusive_snapshot(path)
        postcommit_snapshot_complete = True
        return result

    def fail_evidence_backup_stat(self: Path, *args, **kwargs):
        nonlocal late_backup_stat_calls
        if postcommit_snapshot_complete and self == resolved_backup:
            late_backup_stat_calls += 1
            if late_backup_stat_calls == 2:
                raise OSError("simulated backup stat evidence failure")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(
        apply_tool,
        "_exclusive_postcommit_snapshot",
        mark_postcommit_snapshot_complete,
    )
    monkeypatch.setattr(Path, "stat", fail_evidence_backup_stat)
    preflight_raw = Path(preflight["report_paths"]["raw_json"])

    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "backup-stat-reports",
        )

    assert captured.value.code == "EVIDENCE_BUILD_FAILED"
    assert captured.value.committed is True
    assert _schema_version_path(source) == "63"
    assert _schema_version_path(backup) == "62"


def test_postcommit_report_build_failure_is_reported_as_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "report-build-source.sqlite3"
    clone = tmp_path / "report-build-clone.sqlite3"
    backup = tmp_path / "report-build-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    def fail_report_build() -> str:
        raise OSError("simulated report build failure")

    monkeypatch.setattr(apply_tool, "_now", fail_report_build)
    preflight_raw = Path(preflight["report_paths"]["raw_json"])

    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "report-build-reports",
        )

    assert captured.value.code == "EVIDENCE_BUILD_FAILED"
    assert captured.value.committed is True
    assert _schema_version_path(source) == "63"
    assert _schema_version_path(backup) == "62"


def test_postcommit_report_write_failure_is_reported_as_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "report-write-source.sqlite3"
    clone = tmp_path / "report-write-clone.sqlite3"
    backup = tmp_path / "report-write-backup.sqlite3"
    _make_schema_62(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    def fail_report_write(*_args, **_kwargs):
        raise OSError("simulated report write failure")

    monkeypatch.setattr(apply_tool, "_write_report", fail_report_write)
    preflight_raw = Path(preflight["report_paths"]["raw_json"])

    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=preflight_raw,
            expected_preflight_raw_sha256=_sha256(preflight_raw),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "report-write-reports",
        )

    assert captured.value.code == "EVIDENCE_WRITE_FAILED"
    assert captured.value.committed is True
    assert _schema_version_path(source) == "63"
    assert _schema_version_path(backup) == "62"


def _make_schema_62(path: Path) -> None:
    connection = initialize_database(path)
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id, command_type, source, status,
            payload_json, payload_hash, created_at
        ) VALUES ('command-one', 'PLACE_ORDER', 'test', 'PENDING', '{}', ?, ?)
        """,
        ("1" * 64, "2026-07-23T00:00:00Z"),
    )
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundaries (
            command_id, idempotency_key, command_type, source, state,
            attempts, account_id, code, side, created_at, updated_at
        ) VALUES (
            'command-one', 'idem-one', 'PLACE_ORDER', 'test', 'PRE_ACK',
            1, 'masked', '005930', 'BUY', ?, ?
        )
        """,
        ("2026-07-23T00:00:00Z", "2026-07-23T00:00:01Z"),
    )
    connection.execute(f"DROP TABLE {GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}")
    connection.execute(
        "UPDATE app_metadata SET value = '62', updated_at = '2026-07-23T00:00:02Z' "
        "WHERE key = 'schema_version' AND value = '63'"
    )
    connection.commit()
    connection.close()
    assert _sidecars(path) == []


def _preflight(*, source: Path, clone: Path, tmp_path: Path) -> dict[str, object]:
    return preflight_tool.run_preflight(
        source_db=source,
        clone_db=clone,
        out_dir=tmp_path / f"preflight-{clone.stem}",
    )


def _migration_objects(connection: sqlite3.Connection) -> dict[str, str]:
    names = {
        GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,
        *GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES,
        *GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS,
    }
    rows = connection.execute(
        "SELECT name, type FROM sqlite_master WHERE name IN (" + ",".join("?" for _ in names) + ")",
        tuple(sorted(names)),
    )
    return {str(row[0]): str(row[1]) for row in rows}


def _order_state(path: Path) -> dict[str, object]:
    connection = sqlite3.connect(path)
    try:
        command = connection.execute(
            "SELECT status, payload_hash FROM gateway_commands WHERE command_id = 'command-one'"
        ).fetchone()
        boundary = connection.execute(
            "SELECT state, attempts, broker_order_no, updated_at "
            "FROM gateway_order_broker_boundaries WHERE command_id = 'command-one'"
        ).fetchone()
        assert command is not None
        assert boundary is not None
        return {
            "command_status": str(command[0]),
            "command_payload_hash": str(command[1]),
            "boundary": tuple(boundary),
        }
    finally:
        connection.close()


def _schema_version(connection: sqlite3.Connection) -> str:
    return str(
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
    )


def _schema_version_path(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        return _schema_version(connection)
    finally:
        connection.close()


def _app_name(connection: sqlite3.Connection) -> str:
    return str(
        connection.execute("SELECT value FROM app_metadata WHERE key = 'app_name'").fetchone()[0]
    )


def _sidecars(path: Path) -> list[Path]:
    return [
        Path(f"{path}{suffix}")
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{path}{suffix}").exists()
    ]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
