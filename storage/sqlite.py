from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 16
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
    _create_ai_rca_tables(connection)
    _create_ai_codex_prompt_tables(connection)
    _create_ai_live_sim_review_tables(connection)
    _create_gateway_transport_tables(connection)
    _create_market_data_tables(connection)
    _create_theme_projection_tables(connection)
    _create_candidate_projection_tables(connection)
    _create_strategy_projection_tables(connection)
    _create_risk_projection_tables(connection)
    _create_entry_timing_tables(connection)
    _create_dry_run_oms_tables(connection)
    _create_dry_run_exit_tables(connection)
    _create_live_sim_tables(connection)
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
            context_id TEXT,
            prompt_hash TEXT,
            context_hash TEXT,
            output_schema_name TEXT,
            model TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            completed_at TEXT,
            error_message TEXT,
            validation_error TEXT,
            latency_ms REAL,
            input_chars INTEGER,
            output_chars INTEGER,
            raw_response_json TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}'
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
    _ensure_columns(
        connection,
        "ai_requests",
        {
            "context_id": "TEXT",
            "output_schema_name": "TEXT",
            "validation_error": "TEXT",
            "latency_ms": "REAL",
            "input_chars": "INTEGER",
            "output_chars": "INTEGER",
            "raw_response_json": "TEXT",
            "metadata_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_requests_task_status_created
        ON ai_requests (task_type, status, created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_requests_related_entity
        ON ai_requests (related_entity_type, related_entity_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_insights_task_created
        ON ai_insights (task_type, created_at)
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
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_context_packets (
            context_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            context_hash TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            size_chars INTEGER NOT NULL,
            max_size_chars INTEGER NOT NULL,
            truncated INTEGER NOT NULL DEFAULT 0,
            redaction_applied INTEGER NOT NULL DEFAULT 0,
            order_context_included INTEGER NOT NULL DEFAULT 0,
            missing_sections_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            source_sections_json TEXT NOT NULL DEFAULT '[]',
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_context_build_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_context_packets_task_created
        ON ai_context_packets (task_type, created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_context_packets_related_entity
        ON ai_context_packets (related_entity_type, related_entity_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_context_packets_hash
        ON ai_context_packets (context_hash)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_context_build_errors_created_at
        ON ai_context_build_errors (created_at)
        """
    )


def _create_ai_rca_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_rca_reports (
            report_id TEXT PRIMARY KEY,
            report_type TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            root_cause_category TEXT NOT NULL,
            root_cause TEXT NOT NULL,
            context_id TEXT,
            ai_request_id TEXT,
            ai_insight_id TEXT,
            suggested_checks_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            observe_only INTEGER NOT NULL DEFAULT 1,
            no_trading_side_effects INTEGER NOT NULL DEFAULT 1,
            generated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_rca_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT NOT NULL,
            section_name TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            summary TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            source_refs_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_rca_report_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT NOT NULL,
            link_type TEXT NOT NULL,
            related_entity_type TEXT NOT NULL,
            related_entity_id TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_rca_report_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_reports_type_generated
        ON ai_rca_reports (report_type, generated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_reports_trade_date
        ON ai_rca_reports (trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_reports_related_entity
        ON ai_rca_reports (related_entity_type, related_entity_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_sections_report_id
        ON ai_rca_sections (report_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_report_links_report_id
        ON ai_rca_report_links (report_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_rca_report_errors_created_at
        ON ai_rca_report_errors (created_at)
        """
    )


def _create_ai_codex_prompt_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_codex_prompt_drafts (
            draft_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            source_type TEXT NOT NULL,
            target_area TEXT NOT NULL,
            status TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            rca_report_id TEXT,
            context_id TEXT,
            ai_request_id TEXT,
            ai_insight_id TEXT,
            summary TEXT NOT NULL,
            prompt_text TEXT NOT NULL,
            safety_notes_json TEXT NOT NULL DEFAULT '[]',
            acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
            forbidden_scope_json TEXT NOT NULL DEFAULT '[]',
            test_plan_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            generated_by TEXT NOT NULL,
            run_ai INTEGER NOT NULL DEFAULT 0,
            observe_only INTEGER NOT NULL DEFAULT 1,
            human_review_required INTEGER NOT NULL DEFAULT 1,
            auto_apply_allowed INTEGER NOT NULL DEFAULT 0,
            github_write_allowed INTEGER NOT NULL DEFAULT 0,
            codex_execution_allowed INTEGER NOT NULL DEFAULT 0,
            no_trading_side_effects INTEGER NOT NULL DEFAULT 1,
            generated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_codex_prompt_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id TEXT NOT NULL,
            section_name TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            source_refs_json TEXT NOT NULL DEFAULT '[]',
            required INTEGER NOT NULL DEFAULT 1,
            order_index INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_codex_prompt_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id TEXT NOT NULL,
            link_type TEXT NOT NULL,
            related_entity_type TEXT NOT NULL,
            related_entity_id TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_codex_prompt_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT,
            target_area TEXT,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_drafts_source_generated
        ON ai_codex_prompt_drafts (source_type, generated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_drafts_target_generated
        ON ai_codex_prompt_drafts (target_area, generated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_drafts_related_entity
        ON ai_codex_prompt_drafts (related_entity_type, related_entity_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_drafts_rca_report
        ON ai_codex_prompt_drafts (rca_report_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_sections_draft
        ON ai_codex_prompt_sections (draft_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_links_draft
        ON ai_codex_prompt_links (draft_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_codex_prompt_errors_created_at
        ON ai_codex_prompt_errors (created_at)
        """
    )


def _create_ai_live_sim_review_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_live_sim_review_reports (
            review_id TEXT PRIMARY KEY,
            report_type TEXT NOT NULL,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            live_sim_intent_id TEXT,
            live_sim_order_id TEXT,
            live_sim_execution_id TEXT,
            reconcile_id TEXT,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            root_cause_category TEXT NOT NULL,
            root_cause TEXT NOT NULL,
            ai_request_id TEXT,
            ai_insight_id TEXT,
            context_id TEXT,
            suggested_checks_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            observe_only INTEGER NOT NULL DEFAULT 1,
            review_only INTEGER NOT NULL DEFAULT 1,
            no_trading_side_effects INTEGER NOT NULL DEFAULT 1,
            live_real_allowed INTEGER NOT NULL DEFAULT 0,
            order_action_allowed INTEGER NOT NULL DEFAULT 0,
            gateway_command_allowed INTEGER NOT NULL DEFAULT 0,
            generated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_live_sim_review_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            review_id TEXT NOT NULL,
            section_name TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            summary TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            source_refs_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_live_sim_review_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            review_id TEXT NOT NULL,
            link_type TEXT NOT NULL,
            related_entity_type TEXT NOT NULL,
            related_entity_id TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_live_sim_review_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT,
            trade_date TEXT,
            related_entity_type TEXT,
            related_entity_id TEXT,
            live_sim_intent_id TEXT,
            live_sim_order_id TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_reports_type_generated
        ON ai_live_sim_review_reports (report_type, generated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_reports_trade_date
        ON ai_live_sim_review_reports (trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_reports_order
        ON ai_live_sim_review_reports (live_sim_order_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_reports_related_entity
        ON ai_live_sim_review_reports (related_entity_type, related_entity_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_sections_review
        ON ai_live_sim_review_sections (review_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_links_review
        ON ai_live_sim_review_links (review_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ai_live_sim_review_errors_created
        ON ai_live_sim_review_errors (created_at)
        """
    )


def _ensure_columns(
    connection: sqlite3.Connection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, definition in columns.items():
        if column_name not in existing:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


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
        CREATE TABLE IF NOT EXISTS theme_import_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT,
            source_type TEXT NOT NULL,
            source_name TEXT,
            stage TEXT NOT NULL,
            theme_id TEXT,
            theme_name TEXT,
            code TEXT,
            source_url TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
        CREATE INDEX IF NOT EXISTS idx_theme_import_errors_created_at
        ON theme_import_errors (created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_import_errors_batch
        ON theme_import_errors (batch_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_theme_projection_errors_created_at
        ON theme_projection_errors (created_at)
        """
    )


def _create_candidate_projection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_instance_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            generation INTEGER NOT NULL,
            state TEXT NOT NULL,
            previous_state TEXT,
            detected_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            state_updated_at TEXT NOT NULL,
            closed_at TEXT,
            primary_source_type TEXT NOT NULL,
            primary_source_id TEXT NOT NULL,
            source_count INTEGER NOT NULL DEFAULT 0,
            active_source_count INTEGER NOT NULL DEFAULT 0,
            theme_id TEXT,
            theme_name TEXT,
            theme_state TEXT,
            theme_role TEXT,
            market_readiness_status TEXT,
            tick_age_sec REAL,
            vwap_ready INTEGER NOT NULL DEFAULT 0,
            bar_1m_ready INTEGER NOT NULL DEFAULT 0,
            bar_3m_ready INTEGER NOT NULL DEFAULT 0,
            bar_5m_ready INTEGER NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_source_events (
            source_event_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            action TEXT NOT NULL,
            theme_id TEXT,
            theme_name TEXT,
            condition_id TEXT,
            condition_name TEXT,
            event_ts TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_sources_latest (
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_event_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (trade_date, code, source_type, source_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_state_transitions (
            transition_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT NOT NULL,
            event_type TEXT NOT NULL,
            source_event_id TEXT,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            transitioned_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_context_latest (
            candidate_instance_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            theme_context_json TEXT NOT NULL DEFAULT '{}',
            market_context_json TEXT NOT NULL DEFAULT '{}',
            source_context_json TEXT NOT NULL DEFAULT '{}',
            readiness_json TEXT NOT NULL DEFAULT '{}',
            refreshed_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_projection_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_instance_id TEXT,
            source_event_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidates_trade_date_state
        ON candidates (trade_date, state)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidates_code_trade_date
        ON candidates (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidates_theme_trade_date
        ON candidates (theme_id, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_source_events_code_trade_date_observed
        ON candidate_source_events (code, trade_date, observed_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_sources_latest_trade_code_active
        ON candidate_sources_latest (trade_date, code, active)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_transitions_candidate_time
        ON candidate_state_transitions (candidate_instance_id, transitioned_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_projection_errors_created_at
        ON candidate_projection_errors (created_at)
        """
    )


def _create_strategy_projection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_observations (
            strategy_observation_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            overall_status TEXT NOT NULL,
            primary_setup_type TEXT,
            primary_setup_status TEXT,
            score REAL NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            config_version TEXT NOT NULL,
            observe_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_observations_latest (
            candidate_instance_id TEXT PRIMARY KEY,
            strategy_observation_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            overall_status TEXT NOT NULL,
            primary_setup_type TEXT,
            primary_setup_status TEXT,
            score REAL NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            config_version TEXT NOT NULL,
            observe_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_setup_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_observation_id TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            setup_type TEXT NOT NULL,
            status TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            evaluated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_evaluation_runs (
            run_id TEXT PRIMARY KEY,
            trade_date TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            candidate_count INTEGER NOT NULL DEFAULT 0,
            evaluated_count INTEGER NOT NULL DEFAULT 0,
            data_wait_count INTEGER NOT NULL DEFAULT 0,
            matched_observation_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            config_version TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_evaluation_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            candidate_instance_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_observations_trade_evaluated
        ON strategy_observations (trade_date, evaluated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_observations_code_trade
        ON strategy_observations (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_observations_status_trade
        ON strategy_observations (overall_status, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_setup_candidate_type
        ON strategy_setup_observations (candidate_instance_id, setup_type)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_setup_type_status
        ON strategy_setup_observations (setup_type, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_runs_started_at
        ON strategy_evaluation_runs (started_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_errors_created_at
        ON strategy_evaluation_errors (created_at)
        """
    )


def _create_risk_projection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_observations (
            risk_observation_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            strategy_observation_id TEXT,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            overall_status TEXT NOT NULL,
            max_severity TEXT NOT NULL,
            blocked_count INTEGER NOT NULL DEFAULT 0,
            caution_count INTEGER NOT NULL DEFAULT 0,
            pass_count INTEGER NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            config_version TEXT NOT NULL,
            observe_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_observations_latest (
            candidate_instance_id TEXT PRIMARY KEY,
            risk_observation_id TEXT NOT NULL,
            strategy_observation_id TEXT,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            overall_status TEXT NOT NULL,
            max_severity TEXT NOT NULL,
            blocked_count INTEGER NOT NULL DEFAULT 0,
            caution_count INTEGER NOT NULL DEFAULT 0,
            pass_count INTEGER NOT NULL DEFAULT 0,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            config_version TEXT NOT NULL,
            observe_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_check_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            risk_observation_id TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            category TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            message TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            evaluated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_evaluation_runs (
            run_id TEXT PRIMARY KEY,
            trade_date TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            strategy_observation_count INTEGER NOT NULL DEFAULT 0,
            evaluated_count INTEGER NOT NULL DEFAULT 0,
            observe_pass_count INTEGER NOT NULL DEFAULT 0,
            caution_count INTEGER NOT NULL DEFAULT 0,
            block_count INTEGER NOT NULL DEFAULT 0,
            data_wait_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            config_version TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_evaluation_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            candidate_instance_id TEXT,
            strategy_observation_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_observations_trade_evaluated
        ON risk_observations (trade_date, evaluated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_observations_code_trade
        ON risk_observations (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_observations_status_trade
        ON risk_observations (overall_status, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_checks_candidate_category
        ON risk_check_observations (candidate_instance_id, category)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_checks_category_status
        ON risk_check_observations (category, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_runs_started_at
        ON risk_evaluation_runs (started_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_errors_created_at
        ON risk_evaluation_errors (created_at)
        """
    )


def _create_entry_timing_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS entry_timing_evaluations (
            entry_timing_evaluation_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            setup_type TEXT NOT NULL,
            entry_timing_state TEXT NOT NULL,
            price_location_state TEXT NOT NULL,
            status TEXT NOT NULL,
            order_plan_id TEXT,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            observe_only INTEGER NOT NULL DEFAULT 1,
            not_order_intent INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS order_plan_drafts (
            order_plan_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'BUY',
            status TEXT NOT NULL,
            setup_type TEXT NOT NULL,
            entry_timing_state TEXT NOT NULL,
            price_location_state TEXT NOT NULL,
            theme_id TEXT,
            theme_name TEXT,
            theme_state TEXT,
            theme_rank INTEGER,
            stock_role TEXT,
            priority_score REAL,
            current_price REAL NOT NULL,
            limit_price REAL NOT NULL,
            limit_price_source TEXT NOT NULL,
            limit_price_offset_ticks INTEGER NOT NULL DEFAULT 0,
            suggested_quantity INTEGER NOT NULL DEFAULT 0,
            suggested_notional REAL NOT NULL DEFAULT 0,
            max_notional REAL NOT NULL DEFAULT 0,
            risk_budget_source TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            observe_only INTEGER NOT NULL DEFAULT 1,
            not_order_intent INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS order_plan_drafts_latest (
            idempotency_key TEXT PRIMARY KEY,
            order_plan_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            candidate_instance_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'BUY',
            status TEXT NOT NULL,
            setup_type TEXT NOT NULL,
            entry_timing_state TEXT NOT NULL,
            price_location_state TEXT NOT NULL,
            theme_id TEXT,
            theme_name TEXT,
            theme_state TEXT,
            theme_rank INTEGER,
            stock_role TEXT,
            priority_score REAL,
            current_price REAL NOT NULL,
            limit_price REAL NOT NULL,
            limit_price_source TEXT NOT NULL,
            limit_price_offset_ticks INTEGER NOT NULL DEFAULT 0,
            suggested_quantity INTEGER NOT NULL DEFAULT 0,
            suggested_notional REAL NOT NULL DEFAULT 0,
            max_notional REAL NOT NULL DEFAULT 0,
            risk_budget_source TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            observe_only INTEGER NOT NULL DEFAULT 1,
            not_order_intent INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS entry_timing_evaluation_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_instance_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entry_timing_evaluations_trade_status
        ON entry_timing_evaluations (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entry_timing_evaluations_candidate_time
        ON entry_timing_evaluations (candidate_instance_id, evaluated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_plan_drafts_trade_status
        ON order_plan_drafts (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_plan_drafts_code_trade
        ON order_plan_drafts (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_plan_drafts_latest_trade_status
        ON order_plan_drafts_latest (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entry_timing_errors_created_at
        ON entry_timing_evaluation_errors (created_at)
        """
    )


def _create_dry_run_oms_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_intents (
            dry_run_intent_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            strategy_observation_id TEXT NOT NULL,
            risk_observation_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            intended_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            source TEXT NOT NULL,
            observe_only INTEGER NOT NULL DEFAULT 1,
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            live_order_allowed INTEGER NOT NULL DEFAULT 0,
            gateway_command_allowed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_orders (
            dry_run_order_id TEXT PRIMARY KEY,
            dry_run_intent_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            requested_price REAL NOT NULL,
            simulated_fill_price REAL,
            filled_quantity INTEGER NOT NULL DEFAULT 0,
            remaining_quantity INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            simulated_submitted_at TEXT,
            simulated_filled_at TEXT,
            expires_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_executions (
            dry_run_execution_id TEXT PRIMARY KEY,
            dry_run_order_id TEXT NOT NULL,
            dry_run_intent_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            notional REAL NOT NULL,
            commission REAL NOT NULL DEFAULT 0,
            tax REAL NOT NULL DEFAULT 0,
            executed_at TEXT NOT NULL,
            execution_type TEXT NOT NULL DEFAULT 'SIMULATED',
            dry_run_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_positions (
            dry_run_position_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            avg_price REAL NOT NULL,
            invested_notional REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0,
            unrealized_pnl REAL NOT NULL DEFAULT 0,
            last_price REAL,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            dry_run_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_ledger (
            ledger_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            event_type TEXT NOT NULL,
            related_entity_type TEXT NOT NULL,
            related_entity_id TEXT NOT NULL,
            code TEXT,
            amount REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_eligibility_checks (
            check_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            strategy_observation_id TEXT,
            risk_observation_id TEXT,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            eligible INTEGER NOT NULL,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            safety_gate_json TEXT NOT NULL DEFAULT '{}',
            computed_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_intent_rejections (
            rejection_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT,
            strategy_observation_id TEXT,
            risk_observation_id TEXT,
            trade_date TEXT,
            code TEXT,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_runs (
            run_id TEXT PRIMARY KEY,
            trade_date TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            evaluated_count INTEGER NOT NULL DEFAULT 0,
            eligible_count INTEGER NOT NULL DEFAULT 0,
            intent_count INTEGER NOT NULL DEFAULT 0,
            order_count INTEGER NOT NULL DEFAULT 0,
            execution_count INTEGER NOT NULL DEFAULT 0,
            rejection_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            candidate_instance_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_intents_trade_status
        ON dry_run_intents (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_intents_code_trade
        ON dry_run_intents (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_orders_trade_status
        ON dry_run_orders (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_orders_code_trade
        ON dry_run_orders (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_executions_code_trade
        ON dry_run_executions (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_positions_code_trade_status
        ON dry_run_positions (code, trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_ledger_trade_event
        ON dry_run_ledger (trade_date, event_type)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_eligibility_candidate_computed
        ON dry_run_eligibility_checks (candidate_instance_id, computed_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_errors_created_at
        ON dry_run_errors (created_at)
        """
    )


def _create_dry_run_exit_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_evaluations (
            exit_evaluation_id TEXT PRIMARY KEY,
            dry_run_position_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            status TEXT NOT NULL,
            primary_signal_type TEXT,
            signal_count INTEGER NOT NULL DEFAULT 0,
            caution_count INTEGER NOT NULL DEFAULT 0,
            hold_count INTEGER NOT NULL DEFAULT 0,
            last_price REAL,
            avg_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            unrealized_pnl REAL NOT NULL DEFAULT 0,
            unrealized_pnl_pct REAL NOT NULL DEFAULT 0,
            high_watermark_price REAL,
            drawdown_from_high_pct REAL,
            hold_sec REAL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            config_version TEXT NOT NULL,
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            broker_order_allowed INTEGER NOT NULL DEFAULT 0,
            gateway_command_allowed INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_signals (
            exit_signal_id TEXT PRIMARY KEY,
            exit_evaluation_id TEXT NOT NULL,
            dry_run_position_id TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            trigger_price REAL,
            current_price REAL,
            threshold_value REAL,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            observed_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_intents (
            dry_run_exit_intent_id TEXT PRIMARY KEY,
            exit_evaluation_id TEXT NOT NULL,
            dry_run_position_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'SELL',
            quantity INTEGER NOT NULL,
            intended_price REAL NOT NULL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT,
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            close_only INTEGER NOT NULL DEFAULT 1,
            live_order_allowed INTEGER NOT NULL DEFAULT 0,
            gateway_command_allowed INTEGER NOT NULL DEFAULT 0,
            broker_order_sent INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_orders (
            dry_run_exit_order_id TEXT PRIMARY KEY,
            dry_run_exit_intent_id TEXT NOT NULL,
            dry_run_position_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'SELL',
            quantity INTEGER NOT NULL,
            requested_price REAL NOT NULL,
            simulated_fill_price REAL,
            filled_quantity INTEGER NOT NULL DEFAULT 0,
            remaining_quantity INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            close_only INTEGER NOT NULL DEFAULT 1,
            broker_order_sent INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            simulated_submitted_at TEXT,
            simulated_filled_at TEXT,
            expires_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_executions (
            dry_run_exit_execution_id TEXT PRIMARY KEY,
            dry_run_exit_order_id TEXT NOT NULL,
            dry_run_exit_intent_id TEXT NOT NULL,
            dry_run_position_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'SELL',
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            notional REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0,
            commission REAL NOT NULL DEFAULT 0,
            tax REAL NOT NULL DEFAULT 0,
            executed_at TEXT NOT NULL,
            execution_type TEXT NOT NULL DEFAULT 'SIMULATED_EXIT',
            dry_run_only INTEGER NOT NULL DEFAULT 1,
            broker_order_sent INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_runs (
            run_id TEXT PRIMARY KEY,
            trade_date TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            evaluated_position_count INTEGER NOT NULL DEFAULT 0,
            exit_signal_count INTEGER NOT NULL DEFAULT 0,
            exit_intent_count INTEGER NOT NULL DEFAULT 0,
            exit_order_count INTEGER NOT NULL DEFAULT 0,
            exit_execution_count INTEGER NOT NULL DEFAULT 0,
            rejection_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_exit_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            dry_run_position_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dry_run_position_metrics (
            dry_run_position_id TEXT PRIMARY KEY,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            high_watermark_price REAL,
            low_watermark_price REAL,
            max_unrealized_pnl REAL NOT NULL DEFAULT 0,
            min_unrealized_pnl REAL NOT NULL DEFAULT 0,
            last_evaluated_at TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_evaluations_trade_status
        ON dry_run_exit_evaluations (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_evaluations_code_time
        ON dry_run_exit_evaluations (code, evaluated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_signals_position_type
        ON dry_run_exit_signals (dry_run_position_id, signal_type)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_intents_trade_status
        ON dry_run_exit_intents (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_orders_trade_status
        ON dry_run_exit_orders (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_executions_code_trade
        ON dry_run_exit_executions (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_runs_started_at
        ON dry_run_exit_runs (started_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dry_run_exit_errors_created_at
        ON dry_run_exit_errors (created_at)
        """
    )


def _create_live_sim_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_intents (
            live_sim_intent_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            strategy_observation_id TEXT NOT NULL,
            risk_observation_id TEXT NOT NULL,
            dry_run_intent_id TEXT,
            dry_run_order_id TEXT,
            trade_date TEXT NOT NULL,
            account_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            limit_price REAL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            idempotency_key TEXT NOT NULL UNIQUE,
            gateway_command_id TEXT,
            live_sim_only INTEGER NOT NULL DEFAULT 1,
            live_real_allowed INTEGER NOT NULL DEFAULT 0,
            broker_order_sent INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_orders (
            live_sim_order_id TEXT PRIMARY KEY,
            live_sim_intent_id TEXT NOT NULL,
            gateway_command_id TEXT,
            trade_date TEXT NOT NULL,
            account_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            limit_price REAL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
            broker_order_no TEXT,
            broker_result_code TEXT,
            broker_message TEXT,
            filled_quantity INTEGER NOT NULL DEFAULT 0,
            remaining_quantity INTEGER NOT NULL DEFAULT 0,
            avg_fill_price REAL,
            idempotency_key TEXT NOT NULL UNIQUE,
            live_sim_only INTEGER NOT NULL DEFAULT 1,
            live_real_allowed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            command_queued_at TEXT,
            command_dispatched_at TEXT,
            broker_acked_at TEXT,
            last_event_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_executions (
            live_sim_execution_id TEXT PRIMARY KEY,
            live_sim_order_id TEXT,
            live_sim_intent_id TEXT,
            broker_order_no TEXT,
            account_id TEXT NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            notional REAL NOT NULL,
            executed_at TEXT NOT NULL,
            raw_event_json TEXT NOT NULL DEFAULT '{}',
            live_sim_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_rejections (
            rejection_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT,
            strategy_observation_id TEXT,
            risk_observation_id TEXT,
            trade_date TEXT,
            account_id TEXT,
            code TEXT,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_runs (
            run_id TEXT PRIMARY KEY,
            trade_date TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            evaluated_count INTEGER NOT NULL DEFAULT 0,
            eligible_count INTEGER NOT NULL DEFAULT 0,
            intent_count INTEGER NOT NULL DEFAULT 0,
            command_count INTEGER NOT NULL DEFAULT 0,
            rejection_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_reconcile_snapshots (
            reconcile_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            trade_date TEXT,
            code TEXT,
            broker_open_order_count INTEGER NOT NULL DEFAULT 0,
            broker_position_count INTEGER NOT NULL DEFAULT 0,
            local_open_order_count INTEGER NOT NULL DEFAULT 0,
            local_position_count INTEGER NOT NULL DEFAULT 0,
            mismatch_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            snapshot_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            live_sim_only INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS live_sim_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            live_sim_intent_id TEXT,
            live_sim_order_id TEXT,
            code TEXT,
            error_message TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_intents_trade_status
        ON live_sim_intents (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_intents_code_trade
        ON live_sim_intents (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_orders_trade_status
        ON live_sim_orders (trade_date, status)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_orders_code_trade
        ON live_sim_orders (code, trade_date)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_orders_gateway_command
        ON live_sim_orders (gateway_command_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_orders_broker_order_no
        ON live_sim_orders (broker_order_no)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_executions_code_executed
        ON live_sim_executions (code, executed_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_reconcile_created
        ON live_sim_reconcile_snapshots (created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_live_sim_errors_created
        ON live_sim_errors (created_at)
        """
    )
