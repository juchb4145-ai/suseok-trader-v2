from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 4
APP_NAME = "suseok-trader-v2"


def open_connection(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    _configure_connection(connection)
    return connection


def initialize_database(db_path: str | Path) -> sqlite3.Connection:
    connection = open_connection(db_path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    _create_ai_sidecar_tables(connection)
    _create_gateway_transport_tables(connection)
    _create_market_data_tables(connection)
    _create_theme_projection_tables(connection)
    _upsert_metadata(connection, "app_name", APP_NAME)
    _upsert_metadata(connection, "schema_version", str(SCHEMA_VERSION))
    connection.commit()
    return connection


def _configure_connection(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA synchronous=NORMAL")


def _upsert_metadata(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        """
        INSERT INTO app_metadata (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = datetime('now')
        """,
        (key, value),
    )


def _create_ai_sidecar_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_requests (
            request_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            prompt_hash TEXT,
            context_hash TEXT,
            model TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            completed_at TEXT,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_insights (
            insight_id TEXT PRIMARY KEY,
            request_id TEXT,
            task_type TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            summary TEXT NOT NULL,
            root_cause TEXT,
            severity TEXT,
            operator_action TEXT,
            output_json TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_prompt_templates (
            template_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            version TEXT NOT NULL,
            system_prompt TEXT NOT NULL,
            user_template TEXT NOT NULL,
            output_schema_json TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_evaluation_cases (
            case_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            input_json TEXT NOT NULL,
            expected_properties_json TEXT NOT NULL,
            grade_result_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def _create_gateway_transport_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            command_id TEXT,
            idempotency_key TEXT,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL DEFAULT (datetime('now')),
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            duplicate_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            command_id TEXT,
            idempotency_key TEXT,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL DEFAULT (datetime('now')),
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACCEPTED',
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_commands (
            command_id TEXT PRIMARY KEY,
            command_type TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            idempotency_key TEXT,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            available_at TEXT,
            dispatched_at TEXT,
            completed_at TEXT,
            expires_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_command_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_command_dedupe_keys (
            idempotency_key TEXT PRIMARY KEY,
            command_id TEXT NOT NULL,
            command_type TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            retained_until TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_status (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gateway_commands_poll
        ON gateway_commands (status, available_at, expires_at, created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gateway_events_received_at
        ON gateway_events (received_at)
        """
    )


def _create_market_data_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_ticks_latest (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price INTEGER NOT NULL,
            change_rate REAL NOT NULL,
            cumulative_volume INTEGER NOT NULL,
            cumulative_trade_value REAL NOT NULL,
            execution_strength REAL NOT NULL,
            best_bid INTEGER NOT NULL,
            best_ask INTEGER NOT NULL,
            spread_ticks INTEGER NOT NULL,
            day_high INTEGER NOT NULL,
            day_low INTEGER NOT NULL,
            trade_time TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source TEXT NOT NULL,
            event_id TEXT NOT NULL,
            quality_status TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_tick_samples (
            event_id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            price INTEGER NOT NULL,
            cumulative_volume INTEGER NOT NULL,
            cumulative_trade_value REAL NOT NULL,
            volume_delta INTEGER NOT NULL,
            trade_value_delta REAL NOT NULL,
            execution_strength REAL NOT NULL,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_minute_bars (
            code TEXT NOT NULL,
            interval_sec INTEGER NOT NULL,
            bucket_start TEXT NOT NULL,
            open INTEGER NOT NULL,
            high INTEGER NOT NULL,
            low INTEGER NOT NULL,
            close INTEGER NOT NULL,
            volume_delta INTEGER NOT NULL,
            trade_value_delta REAL NOT NULL,
            tick_count INTEGER NOT NULL,
            vwap REAL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (code, interval_sec, bucket_start)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_condition_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            condition_id TEXT NOT NULL,
            condition_name TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            action TEXT NOT NULL,
            price INTEGER,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_condition_latest (
            condition_id TEXT NOT NULL,
            code TEXT NOT NULL,
            condition_name TEXT NOT NULL,
            name TEXT NOT NULL,
            action TEXT NOT NULL,
            price INTEGER,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source TEXT NOT NULL,
            event_id TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            PRIMARY KEY (condition_id, code)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_tr_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            request_id TEXT NOT NULL,
            tr_code TEXT NOT NULL,
            request_name TEXT NOT NULL,
            code TEXT,
            row_json TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            received_at TEXT NOT NULL,
            source TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_projection_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            event_type TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_tick_samples_code_event_ts
        ON market_tick_samples (code, event_ts)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_minute_bars_code_interval_bucket
        ON market_minute_bars (code, interval_sec, bucket_start)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_condition_signals_code_event_ts
        ON market_condition_signals (code, event_ts)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_tr_snapshots_code_event_ts
        ON market_tr_snapshots (code, event_ts)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_projection_errors_created_at
        ON market_projection_errors (created_at)
        """
    )


def _create_theme_projection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS themes (
            theme_id TEXT PRIMARY KEY,
            theme_name TEXT NOT NULL UNIQUE,
            source_type TEXT NOT NULL,
            source_name TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_members (
            theme_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_name TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            weight REAL NOT NULL DEFAULT 1.0,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (theme_id, code)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            theme_id TEXT NOT NULL,
            theme_name TEXT NOT NULL,
            calculated_at TEXT NOT NULL,
            member_count INTEGER NOT NULL,
            active_member_count INTEGER NOT NULL,
            observed_member_count INTEGER NOT NULL,
            fresh_member_count INTEGER NOT NULL,
            fresh_coverage_ratio REAL NOT NULL,
            rising_member_count INTEGER NOT NULL,
            rising_ratio REAL NOT NULL,
            avg_change_rate REAL NOT NULL,
            max_change_rate REAL NOT NULL,
            total_trade_value REAL NOT NULL,
            trade_value_delta_1m REAL NOT NULL,
            trade_value_delta_3m REAL NOT NULL,
            trade_value_delta_5m REAL NOT NULL,
            leading_code TEXT,
            leading_name TEXT,
            co_leader_codes_json TEXT NOT NULL,
            follower_codes_json TEXT NOT NULL,
            state TEXT NOT NULL,
            quality_status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_snapshot_members (
            snapshot_id TEXT NOT NULL,
            theme_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            price INTEGER,
            change_rate REAL,
            cumulative_trade_value REAL,
            volume_delta_1m INTEGER NOT NULL DEFAULT 0,
            trade_value_delta_1m REAL NOT NULL DEFAULT 0,
            trade_value_delta_3m REAL NOT NULL DEFAULT 0,
            trade_value_delta_5m REAL NOT NULL DEFAULT 0,
            execution_strength REAL,
            vwap REAL,
            above_vwap INTEGER NOT NULL DEFAULT 0,
            readiness_status TEXT NOT NULL,
            member_role TEXT NOT NULL,
            tick_age_sec REAL,
            event_ts TEXT,
            calculated_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (snapshot_id, code)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_latest_snapshots (
            theme_id TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            theme_name TEXT NOT NULL,
            calculated_at TEXT NOT NULL,
            state TEXT NOT NULL,
            quality_status TEXT NOT NULL,
            leading_code TEXT,
            leading_name TEXT,
            fresh_coverage_ratio REAL NOT NULL,
            rising_ratio REAL NOT NULL,
            total_trade_value REAL NOT NULL,
            trade_value_delta_1m REAL NOT NULL,
            trade_value_delta_3m REAL NOT NULL,
            trade_value_delta_5m REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_import_batches (
            batch_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_name TEXT,
            imported_at TEXT NOT NULL DEFAULT (datetime('now')),
            theme_count INTEGER NOT NULL,
            member_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            payload_hash TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_projection_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_members_code
        ON theme_members (code)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_members_theme_active
        ON theme_members (theme_id, active)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_snapshots_theme_calculated
        ON theme_snapshots (theme_id, calculated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_snapshot_members_code
        ON theme_snapshot_members (code)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_latest_snapshots_state
        ON theme_latest_snapshots (state)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_projection_errors_created_at
        ON theme_projection_errors (created_at)
        """
    )
