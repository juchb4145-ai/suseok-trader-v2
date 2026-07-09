from __future__ import annotations

from datetime import UTC, datetime

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.market_reference_service import process_market_symbols_event
from services.runtime.market_reference_projection_reconcile import (
    get_latest_market_reference_projection_reconcile,
    run_market_reference_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_reference_reconcile_passes_with_memberships_and_outbox(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-reconcile-pass.sqlite3")
    event = _dict_event("evt_ref_reconcile_pass", "005930")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")

    result = run_market_reference_projection_reconcile(
        connection,
        settings=_settings(),
        persist=True,
    )
    latest = get_latest_market_reference_projection_reconcile(connection)
    connection.close()

    assert result.status == "PASS"
    assert result.append_only_ready is True
    assert result.checked_event_count == 1
    assert result.checked_symbol_count == 1
    assert result.stored_membership_count == 1
    assert result.missing_membership_count == 0
    assert result.outbox_applied_count == 1
    assert result.payload_shape_counts["dict"] == 1
    assert latest["latest_run"]["status"] == "PASS"


def test_market_reference_reconcile_fails_missing_membership(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-reconcile-fail.sqlite3")
    event = _dict_event("evt_ref_reconcile_missing", "005930")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")

    result = run_market_reference_projection_reconcile(
        connection,
        settings=_settings(),
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.append_only_ready is False
    assert result.missing_membership_count == 1
    assert "MARKET_REFERENCE_MEMBERSHIP_MISSING" in result.reason_codes


def test_market_reference_reconcile_warns_for_empty_symbols_not_fail(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-reconcile-empty.sqlite3")
    event = GatewayEvent(
        event_id="evt_ref_reconcile_empty",
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {}},
        ts=TS,
    )
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_outbox(connection, event.event_id, "SKIPPED")

    result = run_market_reference_projection_reconcile(
        connection,
        settings=_settings(gateway_market_reference_append_only_min_membership_count=0),
        persist=False,
    )
    connection.close()

    assert result.status == "WARN"
    assert result.missing_membership_count == 0
    assert "MARKET_REFERENCE_EMPTY_SYMBOLS" in result.reason_codes


def test_market_reference_reconcile_accepts_list_payload_shape(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-reconcile-list.sqlite3")
    event = GatewayEvent(
        event_id="evt_ref_reconcile_list",
        event_type="market_symbols",
        source="test-gateway",
        payload={
            "markets": [
                {
                    "market": "KOSDAQ",
                    "symbols": [{"code": "035420", "name": "NAVER"}],
                }
            ]
        },
        ts=TS,
    )
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")

    result = run_market_reference_projection_reconcile(
        connection,
        settings=_settings(),
        persist=False,
    )
    connection.close()

    assert result.status == "PASS"
    assert result.payload_shape_counts["list"] == 1
    assert result.stored_membership_count == 1


def _settings(**overrides) -> Settings:
    values = {"gateway_market_reference_append_only_min_membership_count": 1}
    values.update(overrides)
    return Settings(**values)


def _dict_event(event_id: str, code: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {"KOSPI": [{"code": code, "name": "삼성전자"}]}},
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
