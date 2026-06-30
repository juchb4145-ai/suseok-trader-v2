from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.market_index_service import process_market_index_event
from services.market_reference_service import process_market_symbols_event
from services.market_regime_service import get_market_regime_for_code
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database
from tests.test_market_index_service import index_tick_event

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_regime_uses_membership_for_primary_and_secondary_indexes(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_regime.sqlite3")
    settings = Settings(market_index_stale_sec=999_999_999)
    _append_market_symbols(connection)
    _append_index(
        connection,
        index_tick_event("evt_kosdaq_1", index_code="KOSDAQ", price=1000.0),
        settings,
    )
    _append_index(
        connection,
        index_tick_event(
            "evt_kosdaq_2",
            index_code="KOSDAQ",
            price=994.0,
            change_rate=-0.6,
            change_value=-6.0,
            ts=TS + timedelta(minutes=4),
        ),
        settings,
    )
    _append_index(
        connection,
        index_tick_event(
            "evt_kospi_1",
            index_code="KOSPI",
            price=2800.0,
            ts=TS + timedelta(minutes=4),
        ),
        settings,
    )

    kosdaq_regime = get_market_regime_for_code(connection, "035420", settings=settings)
    kospi_regime = get_market_regime_for_code(connection, "005930", settings=settings)
    connection.close()

    assert kosdaq_regime["primary_index_code"] == "KOSDAQ"
    assert kosdaq_regime["secondary_index_code"] == "KOSPI"
    assert kosdaq_regime["regime_status"] == "RISK_OFF"
    assert kosdaq_regime["quality_status"] == "FRESH"
    assert "PRIMARY_INDEX_RISK_OFF" in kosdaq_regime["reason_codes"]
    assert kospi_regime["primary_index_code"] == "KOSPI"
    assert kospi_regime["secondary_index_code"] == "KOSDAQ"


def test_market_regime_unknown_membership_and_stale_indexes_are_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_regime_data_wait.sqlite3")
    stale_settings = Settings(market_index_stale_sec=1)
    _append_market_symbols(connection)
    _append_index(
        connection,
        index_tick_event("evt_old_kospi", index_code="KOSPI", price=2800.0, ts=TS),
        stale_settings,
    )

    unknown = get_market_regime_for_code(connection, "000660", settings=stale_settings)
    stale = get_market_regime_for_code(connection, "005930", settings=stale_settings)
    connection.close()

    assert unknown["primary_index_code"] == "UNKNOWN"
    assert unknown["regime_status"] == "DATA_WAIT"
    assert unknown["quality_status"] == "DEGRADED"
    assert "MARKET_MEMBERSHIP_UNKNOWN" in unknown["reason_codes"]
    assert stale["regime_status"] == "DATA_WAIT"
    assert "MARKET_INDEX_STALE" in stale["reason_codes"]


def _append_market_symbols(connection) -> None:
    event = GatewayEvent(
        event_id="evt_symbols",
        event_type="market_symbols",
        source="test-gateway",
        ts=TS,
        payload={
            "KOSPI": [{"code": "005930", "name": "삼성전자"}],
            "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
        },
    )
    append_gateway_event(connection, event)
    result = process_market_symbols_event(connection, event)
    assert result.status == "APPLIED"


def _append_index(connection, event: GatewayEvent, settings: Settings) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_market_index_event(connection, event, settings=settings)
    assert result.status == "APPLIED"
