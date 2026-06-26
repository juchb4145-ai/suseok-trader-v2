from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1
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
