from __future__ import annotations

import copy
import sqlite3

import pytest
from storage.sqlite import initialize_database
from tools.ops_database_migration_preflight import (
    REQUIRED_TARGET_INDEX_CONTRACTS,
    REQUIRED_TARGET_TABLES,
    REQUIRED_TARGET_TRIGGER_CONTRACTS,
    evaluate_report,
    run_preflight,
)


def test_database_migration_preflight_clones_and_preserves_outbox(tmp_path) -> None:
    source = tmp_path / "source.sqlite3"
    clone = tmp_path / "clone.sqlite3"
    connection = initialize_database(source)
    for index, status in enumerate(("PENDING", "APPLIED", "SKIPPED"), start=1):
        connection.execute(
            """
            INSERT INTO projection_outbox (
                outbox_id, projection_name, event_id, event_type, status
            ) VALUES (?, 'market_data', ?, 'price_tick', ?)
            """,
            (f"outbox-{index}", f"event-{index}", status),
        )
    for table in REQUIRED_TARGET_TABLES:
        connection.execute(f"DROP TABLE IF EXISTS {table}")
    connection.execute("UPDATE app_metadata SET value = '52' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    report = run_preflight(
        source_db=source,
        clone_db=clone,
        required_source_schema="52",
        run_quick_check=True,
        out_dir=tmp_path / "reports",
    )

    assert report["verdict"]["status"] == "PASS"
    assert report["verdict"]["source_data_files_unchanged"] is True
    assert report["verdict"]["outbox_preserved"] is True
    assert report["verdict"]["required_columns_present"] is True
    assert report["verdict"]["required_indexes_present"] is True
    assert report["verdict"]["required_append_only_triggers_present"] is True
    assert report["verdict"]["backup_table_content_preserved"] is True
    assert report["verdict"]["migration_table_content_preserved"] is True
    assert report["verdict"]["target_behavior_contract_valid"] is True
    assert report["verdict"]["target_resolution_ledger_empty"] is True
    assert report["verdict"]["target_dead_letter_disposition_ledger_empty"] is True
    assert report["verdict"]["sqlite_sequence_preserved"] is True
    assert report["verdict"]["clone_disk_space_sufficient"] is True
    assert report["source"]["quick_check"] == ["ok"]
    assert set(report["source"]["files_before"]) == {"main", "wal", "shm"}
    assert report["clone"]["after_migration"]["schema_version"] == "61"
    assert report["clone"]["quick_check"] == ["ok"]
    assert report["clone"]["after_migration"]["projection_outbox"] == {
        "APPLIED": 1,
        "PENDING": 1,
        "SKIPPED": 1,
    }

    migrated = sqlite3.connect(clone)
    tables = {
        row[0] for row in migrated.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    migrated.close()
    assert set(REQUIRED_TARGET_TABLES) <= tables

    skipped = copy.deepcopy(report)
    skipped["clone"]["quick_check"] = ["SKIPPED"]
    skipped_verdict = evaluate_report(skipped)
    assert skipped_verdict["status"] == "FAIL"
    assert "CLONE_QUICK_CHECK_SKIPPED" in skipped_verdict["failures"]

    changed = copy.deepcopy(report)
    changed["clone"]["after_migration"]["table_content"]["gateway_commands"]["sha256"] = "0" * 64
    changed_verdict = evaluate_report(changed)
    assert changed_verdict["status"] == "FAIL"
    assert "MIGRATION_TABLE_CONTENT_MISMATCH" in changed_verdict["failures"]

    sequence_changed = copy.deepcopy(report)
    sequence_changed["clone"]["after_migration"]["sqlite_sequence"] = {"projection_outbox": 0}
    sequence_verdict = evaluate_report(sequence_changed)
    assert sequence_verdict["status"] == "FAIL"
    assert "MIGRATION_SQLITE_SEQUENCE_MISMATCH" in sequence_verdict["failures"]

    newer_source = copy.deepcopy(report)
    newer_source["required_source_schema"] = "62"
    newer_source["source"]["snapshot"]["schema_version"] = "62"
    newer_source["clone"]["before_migration"]["schema_version"] = "62"
    newer_verdict = evaluate_report(newer_source)
    assert newer_verdict["status"] == "FAIL"
    assert "SOURCE_SCHEMA_NEWER_THAN_TARGET" in newer_verdict["failures"]

    pretarget_ledger = copy.deepcopy(report)
    pretarget_ledger["source"]["snapshot"]["table_content"][
        "gateway_order_broker_boundary_resolutions"
    ] = {"row_count": 1, "sha256": "1" * 64}
    pretarget_ledger["clone"]["before_migration"]["table_content"][
        "gateway_order_broker_boundary_resolutions"
    ] = {"row_count": 1, "sha256": "1" * 64}
    pretarget_ledger["clone"]["after_migration"]["table_content"][
        "gateway_order_broker_boundary_resolutions"
    ] = {"row_count": 1, "sha256": "1" * 64}
    pretarget_verdict = evaluate_report(pretarget_ledger)
    assert pretarget_verdict["status"] == "FAIL"
    assert "SOURCE_PRETARGET_RESOLUTION_TABLE_PRESENT" in pretarget_verdict["failures"]

    pretarget_disposition_ledger = copy.deepcopy(report)
    for snapshot in (
        pretarget_disposition_ledger["source"]["snapshot"],
        pretarget_disposition_ledger["clone"]["before_migration"],
        pretarget_disposition_ledger["clone"]["after_migration"],
    ):
        snapshot["table_content"]["incremental_evaluation_dead_letter_dispositions"] = {
            "row_count": 1,
            "sha256": "2" * 64,
        }
    pretarget_disposition_verdict = evaluate_report(pretarget_disposition_ledger)
    assert pretarget_disposition_verdict["status"] == "FAIL"
    assert (
        "SOURCE_PRETARGET_DEAD_LETTER_DISPOSITION_TABLE_PRESENT"
        in pretarget_disposition_verdict["failures"]
    )


def test_database_migration_preflight_requires_resolution_schema_contract(
    tmp_path,
) -> None:
    source = tmp_path / "source-schema-59.sqlite3"
    clone = tmp_path / "clone-schema-61.sqlite3"
    connection = initialize_database(source)
    connection.execute("DROP TABLE gateway_order_broker_boundary_resolutions")
    connection.execute("DROP TABLE incremental_evaluation_dead_letter_dispositions")
    connection.execute("UPDATE app_metadata SET value = '59' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    report = run_preflight(
        source_db=source,
        clone_db=clone,
        required_source_schema="59",
        run_quick_check=True,
        out_dir=tmp_path / "reports-schema-61",
    )

    after = report["clone"]["after_migration"]
    assert report["verdict"]["status"] == "PASS"
    assert after["schema_version"] == "61"
    assert after["required_tables"]["gateway_order_broker_boundary_resolutions"] is True
    assert all(after["required_indexes"][name]["valid"] for name in REQUIRED_TARGET_INDEX_CONTRACTS)
    assert all(
        after["required_triggers"][name]["valid"] for name in REQUIRED_TARGET_TRIGGER_CONTRACTS
    )

    invalid = copy.deepcopy(report)
    invalid["clone"]["after_migration"]["required_indexes"][
        "uq_gateway_order_boundary_resolutions_command_sequence"
    ]["valid"] = False
    invalid_verdict = evaluate_report(invalid)
    assert invalid_verdict["status"] == "FAIL"
    assert "TARGET_INDEX_CONTRACT_INVALID" in invalid_verdict["failures"]


def test_database_migration_preflight_migrates_schema_60_to_61_append_only(
    tmp_path,
) -> None:
    source = tmp_path / "source-schema-60.sqlite3"
    clone = tmp_path / "clone-schema-61.sqlite3"
    connection = initialize_database(source)
    connection.execute("DROP TABLE incremental_evaluation_dead_letter_dispositions")
    connection.execute("DROP TRIGGER IF EXISTS trg_incremental_evaluation_dead_letters_no_update")
    connection.execute("DROP TRIGGER IF EXISTS trg_incremental_evaluation_dead_letters_no_delete")
    connection.execute(
        "DROP INDEX IF EXISTS idx_incremental_evaluation_dead_letter_candidate_time"
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX uq_incremental_evaluation_dead_letter_active
        ON incremental_evaluation_dead_letters (candidate_instance_id)
        WHERE status = 'DEAD_LETTER'
        """
    )
    connection.execute(
        """
        INSERT INTO incremental_evaluation_dead_letters (
            dead_letter_id, candidate_instance_id, trade_date, code, reason,
            source_event_id, priority, original_enqueued_at,
            last_queue_updated_at, attempts, last_error, status,
            dead_lettered_at
        ) VALUES (
            'legacy-dead-letter', 'legacy-candidate', '2026-07-03', '005930',
            'PRICE_TICK', 'legacy-event', 0, '2026-07-03T00:00:00Z',
            '2026-07-03T00:00:00Z', 3, 'LEGACY_RETRY_EXHAUSTED',
            'DEAD_LETTER', '2026-07-13T00:00:00Z'
        )
        """
    )
    connection.execute("UPDATE app_metadata SET value = '60' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    report = run_preflight(
        source_db=source,
        clone_db=clone,
        required_source_schema="60",
        run_quick_check=True,
        out_dir=tmp_path / "reports-schema-61",
    )

    assert report["verdict"]["status"] == "PASS"
    assert report["verdict"]["migration_table_content_preserved"] is True
    assert report["verdict"]["target_dead_letter_disposition_ledger_empty"] is True
    migrated = sqlite3.connect(clone)
    try:
        raw_row = migrated.execute(
            "SELECT status, last_error FROM incremental_evaluation_dead_letters "
            "WHERE dead_letter_id = 'legacy-dead-letter'"
        ).fetchone()
        disposition_count = migrated.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()[0]
        legacy_unique_index = migrated.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'index' "
            "AND name = 'uq_incremental_evaluation_dead_letter_active'"
        ).fetchone()[0]
    finally:
        migrated.close()
    assert raw_row == ("DEAD_LETTER", "LEGACY_RETRY_EXHAUSTED")
    assert disposition_count == 0
    assert legacy_unique_index == 0


def test_database_migration_preflight_refuses_existing_clone(tmp_path) -> None:
    source = tmp_path / "source.sqlite3"
    clone = tmp_path / "clone.sqlite3"
    initialize_database(source).close()
    clone.touch()

    with pytest.raises(FileExistsError):
        run_preflight(
            source_db=source,
            clone_db=clone,
            required_source_schema=None,
            run_quick_check=False,
            out_dir=tmp_path / "reports",
        )


def test_database_migration_preflight_refuses_source_sidecars(tmp_path) -> None:
    source = tmp_path / "source-with-sidecar.sqlite3"
    clone = tmp_path / "clone.sqlite3"
    initialize_database(source).close()
    sidecar = tmp_path / "source-with-sidecar.sqlite3-wal"
    sidecar.touch()

    with pytest.raises(RuntimeError, match="quiescent"):
        run_preflight(
            source_db=source,
            clone_db=clone,
            required_source_schema="61",
            run_quick_check=True,
            out_dir=tmp_path / "reports-sidecar",
        )
