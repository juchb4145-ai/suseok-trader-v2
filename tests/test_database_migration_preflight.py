from __future__ import annotations

import sqlite3

import pytest
from storage.sqlite import initialize_database
from tools.ops_database_migration_preflight import REQUIRED_TARGET_TABLES, run_preflight


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
    assert set(report["source"]["files_before"]) == {"main", "wal", "shm"}
    assert report["clone"]["after_migration"]["schema_version"] == "59"
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
