from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_retention import (
    EventRetentionSafetyError,
    get_event_retention_status,
    prune_event_store_events,
)
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.projection_retention import build_projection_retention_rca
from storage.projection_watermarks import (
    backfill_projection_event_results_from_outbox,
    get_projection_event_result,
    get_projection_watermark,
    get_projection_watermark_status,
)
from storage.sqlite import SCHEMA_VERSION, initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_market_data_success_and_error_watermarks_are_independent(tmp_path) -> None:
    connection = initialize_database(tmp_path / "watermarks.sqlite3")
    success = _price_tick("evt-watermark-success", volume=1000)
    invalid = _price_tick("evt-watermark-error", volume=1010)
    invalid_payload = dict(invalid.payload)
    invalid_payload["metadata"] = {"reason_codes": ["PRICE_MISSING"]}
    invalid = GatewayEvent(
        event_id=invalid.event_id,
        event_type=invalid.event_type,
        source=invalid.source,
        ts=invalid.ts,
        payload=invalid_payload,
    )
    append_gateway_event(connection, success)
    success_rowid = _event_rowid(connection, success.event_id)
    success_result = process_gateway_event(connection, success, settings=Settings())
    append_gateway_event(connection, invalid)
    error_rowid = _event_rowid(connection, invalid.event_id)
    invalid_result = process_gateway_event(connection, invalid, settings=Settings())

    watermark = get_projection_watermark(connection, "market_data")
    success_state = get_projection_event_result(
        connection,
        projection_name="market_data",
        event_id=success.event_id,
    )
    error_state = get_projection_event_result(
        connection,
        projection_name="market_data",
        event_id=invalid.event_id,
    )
    status = get_projection_watermark_status(connection)
    connection.close()

    assert success_result.status == "APPLIED"
    assert invalid_result.status == "IGNORED"
    assert watermark.last_event_rowid == success_rowid
    assert watermark.last_success_event_rowid == success_rowid
    assert watermark.last_success_event_id == success.event_id
    assert watermark.last_error_event_rowid == error_rowid
    assert watermark.last_error_event_id == invalid.event_id
    assert success_state is not None and success_state.status == "SUCCESS"
    assert error_state is not None and error_state.status == "ERROR"
    assert status["status"] == "WARN"
    assert status["unresolved_error_count"] == 1


def test_outbox_terminal_state_updates_projection_result_in_same_commit(tmp_path) -> None:
    connection = initialize_database(tmp_path / "outbox-watermark.sqlite3")
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _price_tick("evt-outbox-watermark", volume=1000)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    before = get_projection_event_result(
        connection,
        projection_name="market_data",
        event_id=event.event_id,
    )
    batch = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=False,
        projection_name="market_data",
    )
    after = get_projection_event_result(
        connection,
        projection_name="market_data",
        event_id=event.event_id,
    )
    outbox = connection.execute(
        "SELECT status FROM projection_outbox WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert before is not None and before.outcome == "APPLIED"
    assert batch.applied_count == 1
    assert outbox["status"] == "APPLIED"
    assert after is not None
    assert after.status == "SUCCESS"
    assert after.outcome == "OUTBOX_APPLIED"
    assert after.attempt_count == 2


def test_projection_retention_rca_blocks_missing_result_and_backfills_applied(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "retention-rca.sqlite3")
    event = _price_tick("evt-retention-rca", volume=1000)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _age_events(connection, event.event_id)
    settings = Settings(event_store_retention_enabled=True)
    retention = get_event_retention_status(
        connection,
        settings=settings,
        exact_counts=True,
    )
    rca = build_projection_retention_rca(
        connection,
        cutoff_at=retention["cutoff_at"],
        event_id=event.event_id,
    )

    assert retention["projection_blocked_event_count"] == 1
    assert rca["items"][0]["retention_eligible"] is False
    assert "PROJECTION_OUTBOX_NOT_APPLIED" in rca["items"][0]["reason_codes"]
    assert "PROJECTION_RESULT_MISSING" in rca["items"][0]["reason_codes"]
    with pytest.raises(EventRetentionSafetyError) as exc_info:
        prune_event_store_events(connection, settings=settings, dry_run=False)
    assert "PROJECTION_RETENTION_GATE_BLOCKED" in exc_info.value.reason_codes

    connection.execute(
        "UPDATE projection_outbox SET status = 'APPLIED' WHERE event_id = ?",
        (event.event_id,),
    )
    connection.commit()
    dry_backfill = backfill_projection_event_results_from_outbox(
        connection,
        dry_run=True,
    )
    applied_backfill = backfill_projection_event_results_from_outbox(
        connection,
        dry_run=False,
        apply_enabled=True,
    )
    ready = get_event_retention_status(
        connection,
        settings=settings,
        exact_counts=True,
    )
    view = connection.execute(
        """
        SELECT projection_retention_ready
        FROM projection_retention_event_rca
        WHERE event_id = ? AND projection_name = 'market_data'
        """,
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert dry_backfill["candidate_count"] == 1
    assert dry_backfill["applied_count"] == 0
    assert applied_backfill["applied_count"] == 1
    assert ready["projection_blocked_event_count"] == 0
    assert ready["candidate_event_count"] == 1
    assert view["projection_retention_ready"] == 1


def test_schema_48_migrates_legacy_watermark_and_retention_tables(tmp_path) -> None:
    db_path = tmp_path / "legacy-schema-47.sqlite3"
    legacy = sqlite3.connect(db_path)
    legacy.execute(
        """
        CREATE TABLE app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    legacy.execute(
        "INSERT INTO app_metadata (key, value) VALUES ('schema_version', '47')"
    )
    legacy.execute(
        """
        CREATE TABLE projection_watermarks (
            projection_name TEXT PRIMARY KEY,
            last_event_rowid INTEGER NOT NULL DEFAULT 0,
            last_event_id TEXT,
            last_event_received_at TEXT,
            last_processed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    legacy.execute(
        """
        CREATE TABLE event_retention_runs (
            run_id TEXT PRIMARY KEY,
            cutoff_at TEXT NOT NULL,
            retention_days INTEGER NOT NULL,
            dry_run INTEGER NOT NULL DEFAULT 0,
            candidate_event_count INTEGER NOT NULL,
            selected_event_count INTEGER NOT NULL,
            deleted_gateway_event_count INTEGER NOT NULL,
            deleted_raw_event_count INTEGER NOT NULL,
            market_data_watermark_rowid INTEGER NOT NULL,
            prunable_event_types_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    legacy.commit()
    legacy.close()

    connection = initialize_database(db_path)
    watermark_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(projection_watermarks)")
    }
    retention_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(event_retention_runs)")
    }
    metadata = connection.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()
    connection.close()
    initialize_database(db_path).close()

    assert metadata["value"] == str(SCHEMA_VERSION) == "55"
    assert {"last_success_event_rowid", "last_error_event_rowid"}.issubset(
        watermark_columns
    )
    assert {
        "projection_blocked_event_count",
        "projection_watermarks_json",
    }.issubset(retention_columns)


def test_projection_retention_operator_and_dashboard_are_fail_closed(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-retention.sqlite3"
    initialize_database(db_path).close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        watermark = client.get("/api/operator/projection-watermarks/status")
        results = client.get("/api/operator/projection-watermarks/results")
        rca = client.get("/api/operator/projection-retention/rca")
        unauthorized_backfill = client.post(
            "/api/operator/projection-watermarks/backfill"
        )
        backfill = client.post(
            "/api/operator/projection-watermarks/backfill?dry_run=true",
            headers={"X-Local-Token": "secret-token"},
        )
        blocked_backfill = client.post(
            "/api/operator/projection-watermarks/backfill?dry_run=false",
            headers={"X-Local-Token": "secret-token"},
        )
        blocked_prune = client.post(
            "/api/operator/event-retention/prune?dry_run=false",
            headers={"X-Local-Token": "secret-token"},
        )
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "projection_watermarks,projection_retention,errors"
        )

    assert watermark.status_code == 200
    assert watermark.json()["read_only"] is True
    assert results.status_code == 200
    assert results.json()["items"] == []
    assert rca.status_code == 200
    assert rca.json()["read_only"] is True
    assert unauthorized_backfill.status_code == 401
    assert backfill.status_code == 200
    assert backfill.json()["dry_run"] is True
    assert blocked_backfill.status_code == 409
    assert "PROJECTION_EVENT_RESULT_BACKFILL_DISABLED" in (
        blocked_backfill.json()["detail"]["reason_codes"]
    )
    assert blocked_prune.status_code == 409
    assert "EVENT_RETENTION_DISABLED" in blocked_prune.json()["detail"]["reason_codes"]
    assert dashboard.status_code == 200
    assert dashboard.json()["projection_watermarks"]["status"] == "PASS"
    assert dashboard.json()["projection_retention"]["apply_ready"] is False
    assert "projection_retention_rca" in dashboard.json()["errors"]


def _price_tick(event_id: str, *, volume: int) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="Samsung Electronics",
        price=70_000,
        change_rate=0.1,
        volume=volume,
        trade_value=70_000 * volume,
        execution_strength=101.5,
        best_bid=69_900,
        best_ask=70_000,
        spread_ticks=1,
        day_high=71_000,
        day_low=69_000,
        trade_time=TS,
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        ts=TS,
        payload=tick.to_dict(),
    )


def _event_rowid(connection, event_id: str) -> int:
    row = connection.execute(
        "SELECT rowid FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return int(row["rowid"])


def _age_events(connection, *event_ids: str) -> None:
    old_received_at = datetime_to_wire(utc_now() - timedelta(days=40))
    placeholders = ", ".join("?" for _ in event_ids)
    for table_name in ("gateway_events", "raw_events"):
        connection.execute(
            f"UPDATE {table_name} SET received_at = ? WHERE event_id IN ({placeholders})",
            (old_received_at, *event_ids),
        )
    connection.commit()
