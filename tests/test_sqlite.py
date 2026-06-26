from storage.sqlite import APP_NAME, SCHEMA_VERSION, initialize_database


def test_sqlite_initialization_creates_app_metadata_and_pragmas(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    metadata_rows = connection.execute("SELECT key, value FROM app_metadata").fetchall()
    metadata = {row["key"]: row["value"] for row in metadata_rows}
    journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
    synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
    connection.close()

    assert db_path.exists()
    assert metadata["app_name"] == APP_NAME
    assert metadata["schema_version"] == str(SCHEMA_VERSION)
    assert journal_mode == "wal"
    assert busy_timeout == 5000
    assert synchronous == 1
