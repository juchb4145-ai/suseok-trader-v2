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


def test_sqlite_initialization_creates_ai_sidecar_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'ai_requests',
                'ai_insights',
                'ai_prompt_templates',
                'ai_evaluation_cases',
                'ai_context_packets',
                'ai_context_build_errors'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "ai_requests",
        "ai_insights",
        "ai_prompt_templates",
        "ai_evaluation_cases",
        "ai_context_packets",
        "ai_context_build_errors",
    }


def test_sqlite_initialization_adds_ai_request_execution_columns(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(ai_requests)").fetchall()
    }
    connection.close()

    assert {
        "context_id",
        "output_schema_name",
        "validation_error",
        "latency_ms",
        "input_chars",
        "output_chars",
        "raw_response_json",
        "metadata_json",
    }.issubset(columns)


def test_sqlite_initialization_creates_gateway_transport_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'raw_events',
                'gateway_events',
                'gateway_commands',
                'gateway_command_events',
                'gateway_command_dedupe_keys',
                'gateway_status'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "raw_events",
        "gateway_events",
        "gateway_commands",
        "gateway_command_events",
        "gateway_command_dedupe_keys",
        "gateway_status",
    }


def test_sqlite_initialization_creates_market_data_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'market_ticks_latest',
                'market_tick_samples',
                'market_minute_bars',
                'market_condition_signals',
                'market_condition_latest',
                'market_tr_snapshots',
                'market_projection_errors'
            )
        """
    ).fetchall()
    existing_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'raw_events',
                'gateway_events',
                'gateway_commands',
                'ai_requests',
                'ai_insights'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "market_ticks_latest",
        "market_tick_samples",
        "market_minute_bars",
        "market_condition_signals",
        "market_condition_latest",
        "market_tr_snapshots",
        "market_projection_errors",
    }
    assert {row["name"] for row in existing_rows} == {
        "raw_events",
        "gateway_events",
        "gateway_commands",
        "ai_requests",
        "ai_insights",
    }


def test_sqlite_initialization_creates_theme_projection_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'themes',
                'theme_members',
                'theme_snapshots',
                'theme_snapshot_members',
                'theme_latest_snapshots',
                'theme_import_batches',
                'theme_projection_errors'
            )
        """
    ).fetchall()
    existing_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'market_ticks_latest',
                'market_minute_bars',
                'gateway_events',
                'ai_requests'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "themes",
        "theme_members",
        "theme_snapshots",
        "theme_snapshot_members",
        "theme_latest_snapshots",
        "theme_import_batches",
        "theme_projection_errors",
    }
    assert {row["name"] for row in existing_rows} == {
        "market_ticks_latest",
        "market_minute_bars",
        "gateway_events",
        "ai_requests",
    }


def test_sqlite_initialization_creates_candidate_projection_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'candidates',
                'candidate_source_events',
                'candidate_sources_latest',
                'candidate_state_transitions',
                'candidate_context_latest',
                'candidate_projection_errors'
            )
        """
    ).fetchall()
    existing_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'theme_latest_snapshots',
                'market_ticks_latest',
                'gateway_events',
                'ai_requests'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "candidates",
        "candidate_source_events",
        "candidate_sources_latest",
        "candidate_state_transitions",
        "candidate_context_latest",
        "candidate_projection_errors",
    }
    assert {row["name"] for row in existing_rows} == {
        "theme_latest_snapshots",
        "market_ticks_latest",
        "gateway_events",
        "ai_requests",
    }


def test_sqlite_initialization_creates_strategy_projection_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'strategy_observations',
                'strategy_observations_latest',
                'strategy_setup_observations',
                'strategy_evaluation_runs',
                'strategy_evaluation_errors'
            )
        """
    ).fetchall()
    existing_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'candidates',
                'candidate_context_latest',
                'theme_latest_snapshots',
                'market_ticks_latest',
                'gateway_events',
                'ai_requests'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "strategy_observations",
        "strategy_observations_latest",
        "strategy_setup_observations",
        "strategy_evaluation_runs",
        "strategy_evaluation_errors",
    }
    assert {row["name"] for row in existing_rows} == {
        "candidates",
        "candidate_context_latest",
        "theme_latest_snapshots",
        "market_ticks_latest",
        "gateway_events",
        "ai_requests",
    }


def test_sqlite_initialization_creates_risk_projection_tables(tmp_path) -> None:
    db_path = tmp_path / "app.sqlite3"
    connection = initialize_database(db_path)

    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'risk_observations',
                'risk_observations_latest',
                'risk_check_observations',
                'risk_evaluation_runs',
                'risk_evaluation_errors'
            )
        """
    ).fetchall()
    existing_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'strategy_observations',
                'strategy_observations_latest',
                'candidates',
                'market_ticks_latest',
                'theme_latest_snapshots'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "risk_observations",
        "risk_observations_latest",
        "risk_check_observations",
        "risk_evaluation_runs",
        "risk_evaluation_errors",
    }
    assert {row["name"] for row in existing_rows} == {
        "strategy_observations",
        "strategy_observations_latest",
        "candidates",
        "market_ticks_latest",
        "theme_latest_snapshots",
    }
