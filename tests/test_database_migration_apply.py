from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest
from storage.sqlite import (
    initialize_database,
    migrate_schema_61_to_62,
)
from tools import ops_database_migration_apply as apply_tool
from tools.ops_database_migration_preflight import run_preflight


def test_exact_public_migration_requires_transaction_and_schema_61(tmp_path) -> None:
    db_path = tmp_path / "exact-public.sqlite3"
    _make_schema_61(db_path)
    connection = sqlite3.connect(db_path)

    with pytest.raises(RuntimeError, match="active transaction"):
        migrate_schema_61_to_62(connection)

    connection.execute("BEGIN EXCLUSIVE")
    migrate_schema_61_to_62(connection)
    assert (
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        == "62"
    )
    assert (
        connection.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0]
        == 0
    )
    connection.rollback()

    assert (
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        == "61"
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'table' AND name = 'pipeline_coherency_dispositions'"
        ).fetchone()[0]
        == 0
    )
    connection.close()


def test_exact_public_migration_rejects_preexisting_target_object(tmp_path) -> None:
    db_path = tmp_path / "preexisting-target.sqlite3"
    _make_schema_61(db_path)
    connection = sqlite3.connect(db_path)
    connection.execute("BEGIN EXCLUSIVE")
    connection.execute("CREATE TABLE pipeline_coherency_dispositions (unexpected TEXT)")

    with pytest.raises(RuntimeError, match="must be absent"):
        migrate_schema_61_to_62(connection)

    connection.rollback()
    assert (
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        == "61"
    )
    connection.close()


def test_exact_public_migration_is_intentionally_not_rerunnable(tmp_path) -> None:
    db_path = tmp_path / "one-shot-exact.sqlite3"
    _make_schema_61(db_path)
    connection = sqlite3.connect(db_path)
    connection.execute("BEGIN EXCLUSIVE")
    migrate_schema_61_to_62(connection)
    connection.commit()

    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(RuntimeError, match="source CAS mismatch"):
        migrate_schema_61_to_62(connection)
    connection.rollback()

    assert (
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        == "62"
    )
    assert (
        connection.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0]
        == 0
    )
    connection.close()


def test_apply_uses_pass_preflight_byte_backup_and_preserves_existing_data(
    tmp_path,
) -> None:
    source = tmp_path / "source.sqlite3"
    clone = tmp_path / "preflight-clone.sqlite3"
    backup = tmp_path / "backup" / "source-schema61.sqlite3"
    _make_schema_61(source)
    source_sha256 = _sha256(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    report = apply_tool.run_apply(
        source_db=source,
        preflight_raw=Path(preflight["report_paths"]["raw_json"]),
        backup_db=backup,
        acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "apply-reports",
    )

    assert report["status"] == "PASS"
    assert report["committed"] is True
    assert report["source"]["schema_after"] == "62"
    assert report["backup"]["schema_version"] == "61"
    assert report["backup"]["sha256"] == source_sha256 == _sha256(backup)
    assert report["safety"]["target_ledger_empty"] is True
    assert all(report["safety"]["target_contract_probes"].values())
    assert report["source"]["exclusive_post_commit_quick_check"] == ["ok"]
    assert report["safety"]["exclusive_post_commit_runtime_lease_count"] == 0
    assert report["safety"]["exact_precommit_postcommit_snapshot_equal"] is True
    assert report["safety"]["postcommit_readonly_exclusive_snapshot_equal"] is True
    assert report["safety"]["exclusive_postcommit_final_fingerprint_equal"] is True
    assert (
        report["source"]["exclusive_post_commit_main"]["sha256"]
        == report["source"]["post_main"]["sha256"]
    )
    assert len(report["source"]["exclusive_post_commit_main"]["sha256"]) == 64
    persisted = json.loads(Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8"))
    assert persisted["source"]["exclusive_post_commit_quick_check"] == ["ok"]
    assert persisted["safety"]["exclusive_post_commit_runtime_lease_count"] == 0
    assert persisted["safety"]["exact_precommit_postcommit_snapshot_equal"] is True
    assert persisted["safety"]["postcommit_readonly_exclusive_snapshot_equal"] is True
    assert persisted["safety"]["exclusive_postcommit_final_fingerprint_equal"] is True
    assert (
        persisted["source"]["exclusive_post_commit_main"]["sha256"]
        == persisted["source"]["post_main"]["sha256"]
    )
    assert len(persisted["source"]["exclusive_post_commit_main"]["sha256"]) == 64
    assert all(not Path(f"{source}{suffix}").exists() for suffix in apply_tool.SIDECAR_SUFFIXES)

    migrated = sqlite3.connect(source)
    assert (
        migrated.execute("SELECT value FROM app_metadata WHERE key = 'schema_version'").fetchone()[
            0
        ]
        == "62"
    )
    assert (
        migrated.execute(
            "SELECT payload FROM schema61_migration_sentinel WHERE sentinel_id = 'one'"
        ).fetchone()[0]
        == "preserve-me"
    )
    assert (
        migrated.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
    )
    migrated.close()


def test_apply_postcommit_detects_schema_version_record_metadata_write(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "metadata-race-source.sqlite3"
    clone = tmp_path / "metadata-race-clone.sqlite3"
    backup = tmp_path / "metadata-race-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    original = apply_tool._exclusive_postcommit_snapshot

    def mutate_schema_version_record(path: Path):
        writer = sqlite3.connect(path)
        writer.execute(
            "UPDATE app_metadata SET updated_at = ? WHERE key = 'schema_version'",
            ("2099-12-31T23:59:59Z",),
        )
        writer.commit()
        writer.close()
        return original(path)

    monkeypatch.setattr(
        apply_tool,
        "_exclusive_postcommit_snapshot",
        mutate_schema_version_record,
    )
    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "metadata-race-apply-reports",
        )

    assert captured.value.code == "EXCLUSIVE_POST_COMMIT_SNAPSHOT_MISMATCH"
    assert captured.value.committed is True


def test_apply_rejects_wrong_ack_without_backup_or_source_write(tmp_path) -> None:
    source = tmp_path / "wrong-ack-source.sqlite3"
    clone = tmp_path / "wrong-ack-clone.sqlite3"
    backup = tmp_path / "wrong-ack-backup.sqlite3"
    _make_schema_61(source)
    source_sha256 = _sha256(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="EXACT_ACKNOWLEDGEMENT_REQUIRED",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge="yes",
            out_dir=tmp_path / "apply-reports",
        )

    assert not backup.exists()
    assert _sha256(source) == source_sha256
    assert _schema_version(source) == "61"


def test_apply_rejects_active_runtime_lease_before_backup(tmp_path) -> None:
    source = tmp_path / "active-lease-source.sqlite3"
    clone = tmp_path / "active-lease-clone.sqlite3"
    backup = tmp_path / "active-lease-backup.sqlite3"
    _make_schema_61(source)
    connection = sqlite3.connect(source)
    connection.execute(
        """
        INSERT INTO runtime_execution_locks (
            lock_name, owner_id, acquired_at, expires_at,
            process_id, thread_id, heartbeat_at, fencing_token, detail_json
        ) VALUES (
            'apply-test', 'another-writer', '2026-01-01T00:00:00Z',
            '2099-01-01T00:00:00Z', 999, 1,
            '2026-01-01T00:00:00Z', 1, '{}'
        )
        """
    )
    connection.commit()
    connection.close()
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="ACTIVE_WRITER_OR_LEASE_PRESENT",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    assert not backup.exists()
    assert _schema_version(source) == "61"


def test_authorizer_rejects_unrelated_write_and_rolls_back(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "authorizer-source.sqlite3"
    clone = tmp_path / "authorizer-clone.sqlite3"
    backup = tmp_path / "authorizer-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)

    def unsafe_migration(connection: sqlite3.Connection) -> None:
        connection.execute("UPDATE schema61_migration_sentinel SET payload = 'mutated'")

    monkeypatch.setattr(apply_tool, "migrate_schema_61_to_62", unsafe_migration)
    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="MIGRATION_AUTHORIZER_OR_SQL_FAILURE",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    connection = sqlite3.connect(source)
    assert (
        connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        == "61"
    )
    assert (
        connection.execute(
            "SELECT payload FROM schema61_migration_sentinel WHERE sentinel_id = 'one'"
        ).fetchone()[0]
        == "preserve-me"
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'table' AND name = 'pipeline_coherency_dispositions'"
        ).fetchone()[0]
        == 0
    )
    connection.close()
    assert backup.exists()


def test_apply_rejects_source_fingerprint_change_after_preflight(tmp_path) -> None:
    source = tmp_path / "changed-source.sqlite3"
    clone = tmp_path / "changed-clone.sqlite3"
    backup = tmp_path / "changed-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    connection = sqlite3.connect(source)
    connection.execute("UPDATE schema61_migration_sentinel SET payload = 'changed-after-preflight'")
    connection.commit()
    connection.close()

    with pytest.raises(
        apply_tool.MigrationApplyError,
        match="SOURCE_FINGERPRINT_CHANGED_SINCE_PREFLIGHT",
    ):
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    assert not backup.exists()
    assert _schema_version(source) == "61"


def test_postcommit_failure_is_reported_as_committed(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "postcommit-source.sqlite3"
    clone = tmp_path / "postcommit-clone.sqlite3"
    backup = tmp_path / "postcommit-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    original = apply_tool._strict_read_only_snapshot
    call_count = 0

    def fail_only_postcommit(path: Path):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise OSError("simulated post-commit verification failure")
        return original(path)

    monkeypatch.setattr(
        apply_tool,
        "_strict_read_only_snapshot",
        fail_only_postcommit,
    )
    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    assert captured.value.code == "UNEXPECTED_POSTCOMMIT_FAILURE"
    assert captured.value.committed is True
    assert _schema_version(source) == "62"
    assert backup.exists()


@pytest.mark.parametrize(
    ("failure_mode", "expected_code", "expected_committed", "expected_schema"),
    (
        ("commit", "UNEXPECTED_PRECOMMIT_FAILURE", True, "62"),
        ("close", "CONNECTION_CLOSE_FAILED", True, "62"),
        ("commit_before", "UNEXPECTED_PRECOMMIT_FAILURE", False, "61"),
    ),
)
def test_commit_or_close_exception_reconciles_durable_schema_as_committed(
    tmp_path,
    monkeypatch,
    failure_mode: str,
    expected_code: str,
    expected_committed: bool,
    expected_schema: str,
) -> None:
    source = tmp_path / f"ambiguous-{failure_mode}-source.sqlite3"
    clone = tmp_path / f"ambiguous-{failure_mode}-clone.sqlite3"
    backup = tmp_path / f"ambiguous-{failure_mode}-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    original_open = apply_tool._open_read_write
    open_count = 0

    def injected_open(path: Path):
        nonlocal open_count
        open_count += 1
        connection = original_open(path)
        if open_count == 1:
            return _DurableThenRaiseConnection(connection, failure_mode=failure_mode)
        return connection

    monkeypatch.setattr(apply_tool, "_open_read_write", injected_open)
    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    assert captured.value.code == expected_code
    assert captured.value.committed is expected_committed
    assert _schema_version(source) == expected_schema
    assert backup.exists()


def test_final_fingerprint_detects_writer_after_locked_postcheck(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "postcheck-race-source.sqlite3"
    clone = tmp_path / "postcheck-race-clone.sqlite3"
    backup = tmp_path / "postcheck-race-backup.sqlite3"
    _make_schema_61(source)
    preflight = _preflight(source=source, clone=clone, tmp_path=tmp_path)
    original = apply_tool._exclusive_postcommit_snapshot

    def mutate_after_locked_snapshot(path: Path):
        result = original(path)
        writer = sqlite3.connect(path)
        writer.execute("UPDATE schema61_migration_sentinel SET payload = 'external-writer-race'")
        writer.commit()
        writer.close()
        return result

    monkeypatch.setattr(
        apply_tool,
        "_exclusive_postcommit_snapshot",
        mutate_after_locked_snapshot,
    )
    with pytest.raises(apply_tool.MigrationApplyError) as captured:
        apply_tool.run_apply(
            source_db=source,
            preflight_raw=Path(preflight["report_paths"]["raw_json"]),
            backup_db=backup,
            acknowledge=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=tmp_path / "apply-reports",
        )

    assert captured.value.code == "SOURCE_FINGERPRINT_CHANGED_AFTER_FINAL_SNAPSHOT"
    assert captured.value.committed is True
    assert _schema_version(source) == "62"


def test_cli_prints_committed_state_before_best_effort_evidence_retry(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    def fail_apply(**_kwargs):
        raise apply_tool.MigrationApplyError(
            "EVIDENCE_WRITE_FAILED",
            committed=True,
        )

    def fail_evidence(*_args, **_kwargs):
        raise OSError("simulated full evidence volume")

    monkeypatch.setattr(apply_tool, "run_apply", fail_apply)
    monkeypatch.setattr(apply_tool, "_write_report", fail_evidence)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ops_database_migration_apply",
            "--source-db",
            str(tmp_path / "source.sqlite3"),
            "--preflight-raw",
            str(tmp_path / "preflight.json"),
            "--backup-db",
            str(tmp_path / "backup.sqlite3"),
            "--acknowledge",
            apply_tool.APPLY_ACKNOWLEDGEMENT,
            "--out-dir",
            str(tmp_path / "unwritable-reports"),
        ],
    )

    assert apply_tool.main() == 2
    lines = capsys.readouterr().out.splitlines()
    assert "committed=true" in lines[0]
    assert "commit_state=APPLIED_OR_UNKNOWN_FAIL_CLOSED" in lines[0]
    assert "evidence=pending" in lines[0]
    assert "failure_evidence=UNAVAILABLE" in lines[1]


class _DurableThenRaiseConnection:
    def __init__(self, connection: sqlite3.Connection, *, failure_mode: str) -> None:
        self._connection = connection
        self._failure_mode = failure_mode

    def __getattr__(self, name: str):
        return getattr(self._connection, name)

    def commit(self) -> None:
        if self._failure_mode == "commit_before":
            raise sqlite3.OperationalError("simulated exception before commit")
        self._connection.commit()
        if self._failure_mode == "commit":
            raise sqlite3.OperationalError("simulated exception after durable commit")

    def close(self) -> None:
        self._connection.close()
        if self._failure_mode == "close":
            raise sqlite3.OperationalError("simulated exception after durable close")


def _make_schema_61(path: Path) -> None:
    connection = initialize_database(path)
    connection.execute("DROP TABLE pipeline_coherency_dispositions")
    connection.execute(
        "DROP TABLE gateway_order_broker_boundary_fence_events"
    )
    connection.execute(
        """
        CREATE TABLE schema61_migration_sentinel (
            sentinel_id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            payload_blob BLOB NOT NULL
        )
        """
    )
    connection.execute(
        "INSERT INTO schema61_migration_sentinel VALUES ('one', 'preserve-me', ?)",
        (b"\x00schema61\xff",),
    )
    connection.execute("UPDATE app_metadata SET value = '61' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()


def _preflight(*, source: Path, clone: Path, tmp_path: Path) -> dict[str, object]:
    return run_preflight(
        source_db=source,
        clone_db=clone,
        required_source_schema="61",
        run_quick_check=True,
        out_dir=tmp_path / f"preflight-reports-{clone.stem}",
    )


def _schema_version(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        return str(
            connection.execute(
                "SELECT value FROM app_metadata WHERE key = 'schema_version'"
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
