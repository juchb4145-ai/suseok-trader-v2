from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from fastapi.testclient import TestClient
from services.config import Settings
from services.market_data_service import process_gateway_event
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 7, 9, 9, 1, 2, tzinfo=UTC)


def test_projection_outbox_drain_once_reduces_pending(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "drain-once.sqlite3"
    connection = initialize_database(db_path)
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _price_tick_event("evt_drain_once")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    connection.close()
    _set_env(monkeypatch, db_path)

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/projection-outbox/drain-once?"
            "limit=10&apply_projection=true&live_safe=true&max_batches=1",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "COMPLETED"
    assert payload["pending_before"] == 1
    assert payload["pending_after"] == 0
    assert payload["pending_delta"] == -1
    assert payload["claimed_count"] == 1
    assert payload["applied_count"] == 0
    assert payload["skipped_count"] == 1
    assert payload["no_trading_side_effects"] is True
    assert payload["projection_side_effects_allowed"] is False


def test_projection_outbox_drain_once_locked_retryable_is_not_500(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "drain-once-locked.sqlite3"
    initialize_database(db_path).close()
    _set_env(monkeypatch, db_path)

    def raise_locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("api.routes.operator.process_projection_outbox_batch", raise_locked)

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/projection-outbox/drain-once?"
            "limit=10&apply_projection=true&live_safe=true&max_batches=1",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "LOCKED_RETRYABLE"
    assert payload["retryable"] is True
    assert "SQLITE_DATABASE_LOCKED" in payload["reason_codes"]
    assert payload["no_trading_side_effects"] is True


def test_projection_outbox_bulk_retire_api_dry_run_and_apply(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bulk-retire-api.sqlite3"
    connection = initialize_database(db_path)
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _price_tick_event("evt_bulk_retire_api")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    connection.close()
    _set_env(monkeypatch, db_path)

    with TestClient(app) as client:
        dry_response = client.post(
            "/api/operator/projection-outbox/bulk-retire?"
            "limit=10&dry_run=true&older_than_sec=0&live_safe=true",
            headers={"X-Local-Token": "secret-token"},
        )
        apply_response = client.post(
            "/api/operator/projection-outbox/bulk-retire?"
            "limit=10&dry_run=false&older_than_sec=0&live_safe=true",
            headers={"X-Local-Token": "secret-token"},
        )

    assert dry_response.status_code == 200
    dry_payload = dry_response.json()
    assert dry_payload["dry_run"] is True
    assert dry_payload["retired_count"] == 1
    assert dry_payload["pending_before"] == 1
    assert dry_payload["pending_after"] == 1
    assert dry_payload["projection_side_effects_allowed"] is False

    assert apply_response.status_code == 200
    apply_payload = apply_response.json()
    assert apply_payload["dry_run"] is False
    assert apply_payload["retired_count"] == 1
    assert apply_payload["applied_count"] == 1
    assert apply_payload["pending_before"] == 1
    assert apply_payload["pending_after"] == 0
    assert apply_payload["no_trading_side_effects"] is True
    assert apply_payload["projection_side_effects_allowed"] is False
    assert _outbox_status(db_path, "market_data:evt_bulk_retire_api") == "APPLIED"


def test_projection_outbox_bulk_retire_locked_retryable_is_not_500(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bulk-retire-locked.sqlite3"
    initialize_database(db_path).close()
    _set_env(monkeypatch, db_path)

    def raise_locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("api.routes.operator.bulk_retire_projection_outbox", raise_locked)

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/projection-outbox/bulk-retire?"
            "limit=10&dry_run=true&older_than_sec=0&live_safe=true",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "LOCKED_RETRYABLE"
    assert payload["retryable"] is True
    assert "SQLITE_DATABASE_LOCKED" in payload["reason_codes"]
    assert payload["no_trading_side_effects"] is True


def _set_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_PROFILE", "OBSERVE")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "false")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "false")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "false")
    monkeypatch.setenv("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC", "0")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC", "0")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC", "0")


def _outbox_status(db_path, outbox_id: str) -> str:
    connection = initialize_database(db_path)
    try:
        row = connection.execute(
            "SELECT status FROM projection_outbox WHERE outbox_id = ?",
            (outbox_id,),
        ).fetchone()
        assert row is not None
        return str(row["status"])
    finally:
        connection.close()


def _price_tick_event(event_id: str) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=70_000,
        change_rate=0.1,
        volume=1_000,
        trade_value=70_000_000,
        execution_strength=101.5,
        best_bid=69_900,
        best_ask=70_000,
        spread_ticks=1,
        day_high=70_500,
        day_low=69_500,
        trade_time=TS,
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=TS,
    )
