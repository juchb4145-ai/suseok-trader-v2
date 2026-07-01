from __future__ import annotations

from datetime import UTC, datetime

from domain.broker.events import GatewayEvent
from services.market_reference_service import (
    get_market_for_code,
    list_market_symbol_memberships,
    process_market_symbols_event,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_symbols_projection_stores_membership_and_ignores_duplicate(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_reference.sqlite3")
    event = GatewayEvent(
        event_id="evt_market_symbols",
        event_type="market_symbols",
        source="test-gateway",
        ts=TS,
        payload={
            "markets": {
                "KOSPI": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
                "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
            }
        },
    )
    append_result = append_gateway_event(connection, event)

    result = process_market_symbols_event(connection, event)
    duplicate = process_market_symbols_event(connection, event)
    samsung = get_market_for_code(connection, "A005930")
    navers = list_market_symbol_memberships(connection, market="KOSDAQ")
    rows = connection.execute("SELECT * FROM market_symbol_memberships").fetchall()
    connection.close()

    assert append_result.status == "ACCEPTED"
    assert result.status == "APPLIED"
    assert result.applied_count == 3
    assert duplicate.status == "DUPLICATE"
    assert len(rows) == 3
    assert samsung is not None
    assert samsung["market"] == "KOSPI"
    assert navers[0]["code"] == "035420"
    assert navers[0]["market"] == "KOSDAQ"


def test_market_symbols_projection_accepts_gateway_market_list_payload(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_reference_gateway.sqlite3")
    event = GatewayEvent(
        event_id="evt_market_symbols_gateway_shape",
        event_type="market_symbols",
        source="kiwoom_gateway",
        ts=TS,
        payload={
            "source": "kiwoom_code_list",
            "markets": [
                {
                    "market_code": "0",
                    "market": "KOSPI",
                    "symbols": ["005930", "000660", "005930"],
                },
                {
                    "market_code": "10",
                    "market": "KOSDAQ",
                    "symbols": ["035420"],
                },
            ],
        },
    )
    append_gateway_event(connection, event)

    result = process_market_symbols_event(connection, event)
    samsung = get_market_for_code(connection, "005930")
    kosdaq = list_market_symbol_memberships(connection, market="KOSDAQ")
    rows = connection.execute("SELECT * FROM market_symbol_memberships").fetchall()
    connection.close()

    assert result.status == "APPLIED"
    assert result.applied_count == 3
    assert len(rows) == 3
    assert samsung is not None
    assert samsung["market"] == "KOSPI"
    assert kosdaq[0]["code"] == "035420"
