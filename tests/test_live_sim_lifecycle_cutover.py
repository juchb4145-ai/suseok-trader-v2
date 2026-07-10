from __future__ import annotations

import time
from dataclasses import replace

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from services.config import Settings
from services.runtime.gateway_live_sim_lifecycle_routing import (
    build_live_sim_lifecycle_cutover_status,
    get_latest_live_sim_lifecycle_routing_status,
    route_live_sim_lifecycle_gateway_event,
)
from services.runtime.live_sim_lifecycle_consumer import (
    enqueue_live_sim_lifecycle_event,
    process_live_sim_lifecycle_batch,
)
from storage.event_store import append_gateway_event
from storage.sqlite import SCHEMA_VERSION, initialize_database


def _event(event_id: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="command_started",
        source="lifecycle-cutover-test",
        payload={"status": "observed"},
    )


def _settings(**overrides) -> Settings:
    values = {
        "live_sim_lifecycle_consumer_enabled": True,
        "live_sim_lifecycle_worker_enabled": True,
        "live_sim_lifecycle_retry_delay_sec": 0.0,
        "live_sim_lifecycle_cutover_enabled": True,
        "live_sim_lifecycle_global_kill_switch": False,
    }
    values.update(overrides)
    return replace(Settings(), **values)


def _record_worker_heartbeat(connection, settings: Settings) -> None:
    result = process_live_sim_lifecycle_batch(connection, settings=settings, limit=1)
    assert result.status == "IDLE"


def test_lifecycle_cutover_tables_migrate_and_rerun(tmp_path) -> None:
    db_path = tmp_path / "lifecycle-cutover-migration.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("UPDATE app_metadata SET value = '56' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    rerun = initialize_database(db_path)
    try:
        schema_version = migrated.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()["value"]
        tables = {
            row["name"]
            for row in migrated.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        indexes = {
            row["name"]
            for row in rerun.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    finally:
        migrated.close()
        rerun.close()

    assert schema_version == str(SCHEMA_VERSION) == "58"
    assert "live_sim_lifecycle_consumer_runs" in tables
    assert "live_sim_lifecycle_routing_decisions" in tables
    assert "idx_live_sim_lifecycle_routing_effective" in indexes


def test_default_kill_switch_keeps_inline_compatibility(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-default-inline.sqlite3")
    event = _event("evt-lifecycle-default-inline")
    append_gateway_event(connection, event)

    result = route_live_sim_lifecycle_gateway_event(
        connection,
        event,
        settings=Settings(),
    )
    inbox = connection.execute(
        "SELECT status, consumer_source FROM live_sim_lifecycle_inbox"
    ).fetchone()
    connection.close()

    assert result["status"] == "APPLIED"
    assert result["routing"]["effective_defer_inline"] is False
    assert result["routing"]["inline_fallback"] is True
    assert "LIFECYCLE_GLOBAL_KILL_SWITCH" in result["routing"]["reason_codes"]
    assert inbox["status"] == "APPLIED"
    assert inbox["consumer_source"] == "gateway_inline_compatibility"


def test_dry_run_records_would_defer_but_keeps_inline(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-dry-run.sqlite3")
    settings = _settings(
        live_sim_lifecycle_cutover_enabled=False,
        live_sim_lifecycle_cutover_dry_run_enabled=True,
        live_sim_lifecycle_global_kill_switch=True,
    )
    _record_worker_heartbeat(connection, settings)
    event = _event("evt-lifecycle-dry-run")
    append_gateway_event(connection, event)

    result = route_live_sim_lifecycle_gateway_event(
        connection,
        event,
        settings=settings,
    )
    connection.close()

    assert result["status"] == "APPLIED"
    assert result["routing"]["would_defer_inline"] is True
    assert result["routing"]["effective_defer_inline"] is False
    assert result["routing"]["worker_healthy"] is True


def test_healthy_worker_cutover_defers_then_applies_once(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-effective.sqlite3")
    settings = _settings()
    _record_worker_heartbeat(connection, settings)
    event = _event("evt-lifecycle-effective")
    append_gateway_event(connection, event)

    routed = route_live_sim_lifecycle_gateway_event(
        connection,
        event,
        settings=settings,
    )
    before = connection.execute(
        "SELECT status FROM live_sim_lifecycle_inbox WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    worker = process_live_sim_lifecycle_batch(connection, settings=settings, limit=1)
    after = connection.execute(
        "SELECT status, consumer_source FROM live_sim_lifecycle_inbox WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    routing = get_latest_live_sim_lifecycle_routing_status(
        connection,
        settings=settings,
    )
    cutover = build_live_sim_lifecycle_cutover_status(
        connection,
        settings=settings,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert routed["status"] == "DEFERRED_TO_DURABLE_WORKER"
    assert routed["routing"]["effective_defer_inline"] is True
    assert before["status"] == "PENDING"
    assert worker.status == "COMPLETED"
    assert worker.applied_count == 1
    assert after["status"] == "APPLIED"
    assert after["consumer_source"] == "durable_worker"
    assert routing["effective_defer_inline"] is True
    assert cutover["status"] == "PASS"
    assert cutover["effective_defer_count"] == 1
    assert command_count == 0


def test_missing_worker_health_falls_back_inline(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-worker-unhealthy.sqlite3")
    settings = _settings()
    event = _event("evt-lifecycle-worker-unhealthy")
    append_gateway_event(connection, event)

    result = route_live_sim_lifecycle_gateway_event(
        connection,
        event,
        settings=settings,
    )
    connection.close()

    assert result["status"] == "APPLIED"
    assert result["routing"]["effective_defer_inline"] is False
    assert result["routing"]["inline_fallback"] is True
    assert "LIFECYCLE_WORKER_UNHEALTHY" in result["routing"]["reason_codes"]


def test_prior_dead_letter_blocks_newer_inline_application(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-ordered-block.sqlite3")
    settings = _settings()
    first = _event("evt-lifecycle-ordered-block-1")
    second = _event("evt-lifecycle-ordered-block-2")
    append_gateway_event(connection, first)
    enqueue_live_sim_lifecycle_event(connection, first)
    connection.execute(
        """
        UPDATE live_sim_lifecycle_inbox
        SET status = 'DEAD_LETTER', attempts = 3, last_error = 'forced prior failure'
        WHERE event_id = ?
        """,
        (first.event_id,),
    )
    connection.commit()
    append_gateway_event(connection, second)

    result = route_live_sim_lifecycle_gateway_event(
        connection,
        second,
        settings=settings,
    )
    second_row = connection.execute(
        "SELECT status, consumer_source FROM live_sim_lifecycle_inbox WHERE event_id = ?",
        (second.event_id,),
    ).fetchone()
    connection.close()

    assert result["status"] == "BLOCKED_ORDERED_BACKLOG"
    assert result["routing"]["ordered_backlog_blocked"] is True
    assert result["routing"]["inline_fallback"] is False
    assert "LIFECYCLE_OUT_OF_ORDER_INLINE_BLOCKED" in result["routing"]["reason_codes"]
    assert second_row["status"] == "PENDING"
    assert second_row["consumer_source"] is None


def test_core_background_worker_establishes_health_for_api_cutover(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "lifecycle-cutover-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_LIFECYCLE_WORKER_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_LIFECYCLE_WORKER_INTERVAL_SEC", "5")
    monkeypatch.setenv("LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_LIFECYCLE_GLOBAL_KILL_SWITCH", "false")
    event = _event("evt-lifecycle-cutover-api")

    with TestClient(app) as client:
        status_payload = {}
        for _ in range(50):
            status_payload = client.get(
                "/api/operator/live-sim/lifecycle-consumer/status"
            ).json()
            if status_payload.get("worker_health", {}).get("healthy"):
                break
            time.sleep(0.02)
        routed = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        worker = client.post(
            "/api/operator/live-sim/lifecycle-consumer/run-once?limit=1",
            headers={"X-Local-Token": "test-token"},
        )
        final_status = client.get(
            "/api/operator/live-sim/lifecycle-consumer/status"
        )
        routing_status = client.get(
            "/api/operator/live-sim/lifecycle-consumer/routing/status"
        )
        routing_rows = client.get(
            "/api/operator/live-sim/lifecycle-consumer/routing?limit=10"
        )
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "live_sim_lifecycle_consumer"
        )

    assert status_payload["worker_health"]["healthy"] is True
    assert routed.status_code == 200
    assert routed.json()["live_sim_lifecycle"]["status"] == (
        "DEFERRED_TO_DURABLE_WORKER"
    )
    assert routed.json()["live_sim_lifecycle"]["routing"][
        "effective_defer_inline"
    ] is True
    assert worker.json()["applied_count"] == 1
    assert final_status.json()["applied_count"] == 1
    assert final_status.json()["dead_letter_count"] == 0
    assert routing_status.json()["effective_defer_inline"] is True
    assert routing_rows.json()["count"] == 1
    assert routing_rows.json()["read_only"] is True
    assert dashboard.json()["live_sim_lifecycle_consumer"][
        "effective_defer_count"
    ] == 1
