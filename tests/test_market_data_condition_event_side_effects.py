from __future__ import annotations

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event
from services.condition_fusion import list_condition_fusion
from services.config import Settings, clear_settings_cache
from services.market_data_service import process_gateway_event
from services.runtime.market_data_projection_side_effects import (
    refresh_condition_fusion_for_condition_event_projection,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database, open_connection


def test_gateway_inline_condition_event_refresh_behavior_is_unchanged(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "condition-inline-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("CONDITION_FUSION_SWEEP_ENABLED", "false")
    clear_settings_cache()
    event = _profile_condition_event("evt_condition_inline_api").to_dict()

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
        signal_count = _count_rows(connection, "market_condition_signals")
        fusion_rows = list_condition_fusion(connection, settings=Settings())
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data"] == "APPLIED"
    assert statuses["condition_fusion"] == "APPLIED"
    assert signal_count == 1
    assert fusion_rows[0]["code"] == "005930"
    assert fusion_rows[0]["latest_event_id"] == "evt_condition_inline_api"


def test_condition_event_side_effect_service_refreshes_condition_fusion(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-side-effect.sqlite3")
    settings = Settings()
    event = _profile_condition_event("evt_condition_side_effect")
    append_gateway_event(connection, event)
    projection = process_gateway_event(connection, event, settings=settings)

    result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=settings,
        source="projection_outbox_worker_condition_event",
    )
    fusion = list_condition_fusion(connection, settings=settings)
    connection.close()

    assert projection.status == "APPLIED"
    assert result.status == "APPLIED"
    assert result.side_effect_type == "condition_fusion_refresh"
    assert result.code == "005930"
    assert result.processed_count == 1
    assert result.applied_count == 1
    assert result.error_count == 0
    assert result.evidence["parent_event_id"] == event.event_id
    assert result.evidence["parent_command_id"] == event.command_id
    assert result.evidence["candidate_ingest_executed"] is False
    assert result.evidence["no_order_side_effects"] is True
    assert result.evidence["no_trading_side_effects"] is True
    assert fusion[0]["latest_event_id"] == event.event_id


def test_condition_event_side_effect_service_skips_when_fusion_disabled(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-side-effect-disabled.sqlite3")
    settings = Settings(condition_fusion_event_incremental_enabled=False)
    event = _profile_condition_event("evt_condition_side_effect_disabled")
    append_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=settings,
        source="projection_outbox_worker_condition_event",
    )
    fusion = list_condition_fusion(connection, settings=Settings())
    connection.close()

    assert result.status == "SKIPPED"
    assert result.skipped_count == 1
    assert result.error_count == 0
    assert "CONDITION_FUSION_INCREMENTAL_DISABLED" in result.reason_codes
    assert fusion == []


def test_condition_event_side_effect_service_reports_invalid_payload(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-side-effect-invalid.sqlite3")
    event = GatewayEvent(
        event_id="evt_condition_invalid_payload",
        event_type="condition_event",
        source="test-gateway",
        payload={"condition_id": "cond-invalid"},
    )

    result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=Settings(),
        source="projection_outbox_worker_condition_event",
    )
    connection.close()

    assert result.status == "ERROR"
    assert result.error_count == 1
    assert "CONDITION_EVENT_PAYLOAD_INVALID" in result.reason_codes
    assert result.evidence["parent_event_id"] == event.event_id
    assert result.evidence["candidate_ingest_executed"] is False


def _profile_condition_event(event_id: str) -> GatewayEvent:
    event = make_condition_event(
        source="test-gateway",
        condition_id="cond-leader",
        condition_name="Leader",
        code="005930",
        name="삼성전자",
        action="ENTER",
        metadata={
            "sensor_evidence": True,
            "condition_profile_id": "profile-leader",
            "condition_role": "LEADER",
            "condition_profile": {
                "profile_id": "profile-leader",
                "condition_name": "Leader",
                "condition_index": 500,
                "role": "LEADER",
                "priority": 500,
                "ttl_sec": 999_999_999,
                "enabled": True,
                "price_subscribe_policy": "immediate",
            },
            "condition_admission": {
                "subscribed": True,
                "reason_codes": ["TEST"],
            },
        },
    )
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
        command_id="cmd-condition",
        idempotency_key=f"idempotency:{event_id}",
    )


def _count_rows(connection, table_name: str) -> int:
    return int(
        connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()[
            "count"
        ]
    )
