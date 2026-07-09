from __future__ import annotations

import json
from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from services.config import Settings, clear_settings_cache
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database, open_connection
from tests.test_market_data_condition_event_side_effects import _profile_condition_event


def test_condition_event_cutover_default_keeps_gateway_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "condition-cutover-default.sqlite3"
    _configure_env(monkeypatch, tmp_path, db_path)
    event = _profile_condition_event("evt_condition_default").to_dict()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=event,
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_condition_default")
        signal_count = _table_event_count(
            connection,
            "market_condition_signals",
            "evt_condition_default",
        )
        fusion_count = _count_rows(connection, "candidate_condition_fusion")
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data"] == "APPLIED"
    assert statuses["condition_fusion"] == "APPLIED"
    assert statuses["market_data_effective_skip_inline"] == "FALSE"
    assert decision["effective_skip_inline"] == 0
    assert signal_count == 1
    assert fusion_count == 1


def test_condition_event_cutover_skips_gateway_inline_when_all_guards_pass(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "condition-cutover-skip.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    connection = initialize_database(db_path)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()
    event = _profile_condition_event("evt_condition_cutover_skip").to_dict()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=event,
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_condition_cutover_skip")
        signal_count = _table_event_count(
            connection,
            "market_condition_signals",
            "evt_condition_cutover_skip",
        )
        evidence = json.loads(decision["evidence_json"])
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data"] == "SKIPPED_INLINE_APPEND_ONLY_CONDITION_EVENT"
    assert statuses["market_data_effective_skip_inline"] == "TRUE"
    assert statuses["condition_fusion"] == (
        "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_CONDITION_EVENT"
    )
    assert decision["effective_skip_inline"] == 1
    assert decision["condition_event_skip_budget_limit"] == 10
    assert decision["condition_event_worker_side_effect_ready"] == 1
    assert decision["condition_event_backlog_ready"] == 1
    assert decision["condition_event_code"] == "005930"
    assert decision["condition_event_action"] == "ENTER"
    assert "CONDITION_EVENT_EFFECTIVE_SKIP_ALLOWED" in (
        decision["blocked_reason_codes_json"]
    )
    assert evidence["candidate_ingest_executed"] is False
    assert signal_count == 0


def test_condition_event_cutover_budget_exhaustion_falls_back_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "condition-cutover-budget.sqlite3"
    _configure_cutover_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE="1",
    )
    connection = initialize_database(db_path)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()

    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/gateway/events",
                json=_profile_condition_event("evt_condition_budget_1").to_dict(),
                headers={"X-Local-Token": "test-token"},
            )
            second = client.post(
                "/api/gateway/events",
                json=_profile_condition_event("evt_condition_budget_2").to_dict(),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        second_decision = _routing_decision(connection, "evt_condition_budget_2")
        second_signal_count = _table_event_count(
            connection,
            "market_condition_signals",
            "evt_condition_budget_2",
        )
    finally:
        connection.close()

    assert first.json()["projection_statuses"]["market_data"] == (
        "SKIPPED_INLINE_APPEND_ONLY_CONDITION_EVENT"
    )
    assert second.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert second_decision["effective_skip_inline"] == 0
    assert "CONDITION_EVENT_SKIP_BUDGET_EXHAUSTED" in (
        second_decision["blocked_reason_codes_json"]
    )
    assert second_signal_count == 1


def _configure_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("CONDITION_FUSION_SWEEP_ENABLED", "false")
    values = dict(overrides)
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()


def _configure_cutover_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    values = {
        "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE": "10",
        "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED": "true",
        "PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED": "true",
        "PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC": "0",
    }
    values.update(overrides)
    _configure_env(monkeypatch, tmp_path, db_path, **values)


def _cutover_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": True,
        "gateway_market_data_append_only_condition_event_cutover_enabled": True,
        "gateway_market_data_append_only_condition_event_max_skip_per_minute": 10,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
        "projection_outbox_apply_min_age_sec": 0,
    }
    values.update(overrides)
    return Settings(**values)


def _insert_reconcile_run(connection, *, status: str, append_only_ready: bool) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_reconcile_runs (
            run_id,
            status,
            checked_event_count,
            checked_price_tick_count,
            checked_condition_event_count,
            checked_tr_response_count,
            outbox_job_count,
            outbox_pending_count,
            outbox_processing_count,
            outbox_applied_count,
            outbox_skipped_count,
            outbox_error_count,
            outbox_dead_letter_count,
            missing_projection_count,
            inline_projection_error_count,
            outbox_error_issue_count,
            duplicate_or_conflict_count,
            synthetic_child_event_issue_count,
            watermark_risk_count,
            append_only_ready,
            reason_codes_json,
            summary_json,
            created_at
        )
        VALUES (?, ?, 1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, '[]', ?, ?)
        """,
        (
            f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
            status,
            int(append_only_ready),
            canonical_json({"status": status, "append_only_ready": append_only_ready}),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()


def _routing_decision(connection, event_id: str):
    row = connection.execute(
        """
        SELECT *
        FROM market_data_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()
    assert row is not None
    return row


def _table_event_count(connection, table_name: str, event_id: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name} WHERE event_id = ?",
            (event_id,),
        ).fetchone()["count"]
    )


def _count_rows(connection, table_name: str) -> int:
    return int(
        connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()[
            "count"
        ]
    )
