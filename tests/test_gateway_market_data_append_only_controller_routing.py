from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_price_tick_event
from services.config import Settings, clear_settings_cache
from services.runtime.gateway_projection_routing import (
    decide_market_data_projection_routing,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection


def test_controller_mode_off_blocks_effective_skip_after_event_guard_passes(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "controller-routing-off.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event, outbox_status = _append_price_event(connection, "evt_controller_mode_off")

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=_controller_settings(),
        outbox_status=outbox_status,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "EFFECTIVE_SKIP_ALLOWED_PRICE_TICK" in decision.blocked_reason_codes
    assert "MARKET_DATA_APPEND_ONLY_MODE_OFF" in decision.blocked_reason_codes
    assert "MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH" in (
        decision.blocked_reason_codes
    )
    assert "MARKET_DATA_APPEND_ONLY_CONTROLLER_NOT_READY" in (
        decision.blocked_reason_codes
    )


def test_controller_global_kill_switch_overrides_allowed_mode(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-routing-kill.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event, outbox_status = _append_price_event(connection, "evt_controller_kill")

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=_controller_settings(
            gateway_market_data_append_only_operating_mode="PRICE_TICK_ONLY",
            gateway_market_data_append_only_global_kill_switch=True,
            gateway_market_data_append_only_global_max_skip_per_minute=10,
        ),
        outbox_status=outbox_status,
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH" in (
        decision.blocked_reason_codes
    )


def test_controller_global_budget_exhaustion_blocks_second_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-routing-budget.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    settings = _controller_settings(
        gateway_market_data_append_only_operating_mode="PRICE_TICK_ONLY",
        gateway_market_data_append_only_global_kill_switch=False,
        gateway_market_data_append_only_global_max_skip_per_minute=1,
    )
    first_event, first_outbox_status = _append_price_event(
        connection,
        "evt_controller_budget_1",
    )
    second_event, second_outbox_status = _append_price_event(
        connection,
        "evt_controller_budget_2",
    )

    first = decide_market_data_projection_routing(
        connection,
        first_event,
        settings=settings,
        outbox_status=first_outbox_status,
    )
    second = decide_market_data_projection_routing(
        connection,
        second_event,
        settings=settings,
        outbox_status=second_outbox_status,
    )
    connection.close()

    assert first.effective_skip_inline is True
    assert second.effective_skip_inline is False
    assert "MARKET_DATA_APPEND_ONLY_GLOBAL_BUDGET_EXHAUSTED" in (
        second.blocked_reason_codes
    )


def test_controller_auto_rollback_records_event_and_blocks_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-routing-rollback.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    _insert_outbox_error(connection)
    event, outbox_status = _append_price_event(connection, "evt_controller_rollback")

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=_controller_settings(
            gateway_market_data_append_only_operating_mode="PRICE_TICK_ONLY",
            gateway_market_data_append_only_global_kill_switch=False,
            gateway_market_data_append_only_global_max_skip_per_minute=10,
        ),
        outbox_status=outbox_status,
    )
    rollback_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_data_append_only_auto_rollback_events"
    ).fetchone()["count"]
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_REQUIRED" in (
        decision.blocked_reason_codes
    )
    assert rollback_count == 1


def test_operator_controller_status_and_snapshot_api_are_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "controller-api.sqlite3"
    initialize_database(db_path).close()
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    clear_settings_cache()

    try:
        with TestClient(app) as client:
            status_response = client.get(
                "/api/operator/market-data-append-only/controller/status"
            )
            snapshot_response = client.post(
                "/api/operator/market-data-append-only/controller/snapshot",
                headers={"X-Local-Token": "test-token"},
            )
            events_response = client.get(
                "/api/operator/market-data-append-only/controller/rollback-events"
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        snapshot_count = connection.execute(
            "SELECT COUNT(*) AS count FROM market_data_append_only_controller_snapshots"
        ).fetchone()["count"]
    finally:
        connection.close()

    status_payload = status_response.json()
    snapshot_payload = snapshot_response.json()
    assert status_response.status_code == 200
    assert status_payload["read_only"] is True
    assert status_payload["no_trading_side_effects"] is True
    assert "price_tick_gate" in status_payload
    assert "tr_response_gate" in status_payload
    assert "condition_event_gate" in status_payload
    assert snapshot_response.status_code == 200
    assert snapshot_payload["config_changed"] is False
    assert snapshot_payload["snapshot_id"]
    assert snapshot_count == 1
    assert events_response.json()["read_only"] is True
    assert events_response.json()["no_trading_side_effects"] is True


def _controller_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": True,
        "gateway_market_data_append_only_price_tick_cutover_enabled": True,
        "gateway_market_data_append_only_price_tick_max_skip_per_minute": 10,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _append_price_event(
    connection: sqlite3.Connection,
    event_id: str,
) -> tuple[GatewayEvent, str]:
    payload = make_price_tick_event(source="test-gateway").to_dict()
    payload["event_id"] = event_id
    event = GatewayEvent.from_dict(payload)
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    return event, outbox.status


def _insert_reconcile_run(
    connection: sqlite3.Connection,
    *,
    status: str,
    append_only_ready: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_reconcile_runs (
            run_id, status, checked_event_count, checked_price_tick_count,
            checked_condition_event_count, checked_tr_response_count,
            outbox_job_count, outbox_pending_count, outbox_processing_count,
            outbox_applied_count, outbox_skipped_count, outbox_error_count,
            outbox_dead_letter_count, missing_projection_count,
            inline_projection_error_count, outbox_error_issue_count,
            duplicate_or_conflict_count, synthetic_child_event_issue_count,
            watermark_risk_count, append_only_ready, reason_codes_json,
            summary_json, created_at
        )
        VALUES (?, ?, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, '[]', ?, ?)
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


def _insert_outbox_error(connection: sqlite3.Connection) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id, projection_name, event_id, event_type, source, status,
            priority, attempts, available_at, created_at, updated_at, metadata_json
        )
        VALUES (
            'market_data:evt_prior_error', 'market_data', 'evt_prior_error',
            'price_tick', 'test', 'ERROR', 0, 0, ?, ?, ?, '{}'
        )
        """,
        (now, now, now),
    )
    connection.commit()
