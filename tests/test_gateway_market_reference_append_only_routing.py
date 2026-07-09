from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from services.config import Settings, clear_settings_cache
from services.market_reference_service import process_market_symbols_event
from services.runtime.gateway_market_reference_routing import (
    MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON,
    decide_market_reference_append_only_routing,
)
from services.runtime.market_reference_projection_reconcile import (
    run_market_reference_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_reference_routing_dry_run_would_skip_but_effective_false(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-reference-routing.sqlite3")
    event = _event("evt_ref_route")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")
    settings = _settings(gateway_market_reference_append_only_dry_run_enabled=True)
    reconcile = run_market_reference_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert reconcile.status == "PASS"
    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON in decision.blocked_reason_codes


def test_market_reference_cutover_flag_still_never_effective_skips(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-routing-cutover.sqlite3")
    event = _event("evt_ref_route_cutover")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")
    settings = _settings(
        gateway_market_reference_append_only_dry_run_enabled=True,
        gateway_market_reference_append_only_cutover_enabled=True,
    )
    run_market_reference_projection_reconcile(connection, settings=settings, persist=True)

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON in decision.blocked_reason_codes


def test_gateway_market_symbols_inline_projection_remains_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-reference-gateway.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT", "1")
    clear_settings_cache()
    body = _event("evt_ref_gateway").to_dict()

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=body,
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    membership_count = _count_rows(connection, "market_symbol_memberships")
    routing_count = _count_rows(connection, "market_reference_projection_routing_decisions")
    connection.close()
    clear_settings_cache()

    assert response.status_code == 200
    payload = response.json()
    assert payload["projection_statuses"]["market_reference"] == "APPLIED"
    assert payload["projection_statuses"]["market_reference_effective_skip_inline"] == (
        "FALSE"
    )
    assert payload["market_reference_append_only_routing"]["effective_skip_inline"] is False
    assert membership_count == 1
    assert routing_count == 1


def _settings(**overrides) -> Settings:
    values = {
        "gateway_market_reference_append_only_min_membership_count": 1,
        "gateway_market_reference_append_only_reconcile_max_age_sec": 1800,
    }
    values.update(overrides)
    return Settings(**values)


def _event(event_id: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {"KOSPI": [{"code": "005930", "name": "삼성전자"}]}},
        ts=TS,
    )


def _mark_outbox(connection, event_id: str, status: str) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?, processed_at = datetime('now'), updated_at = datetime('now')
        WHERE projection_name = 'market_reference' AND event_id = ?
        """,
        (status, event_id),
    )
    connection.commit()


def _count_rows(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
