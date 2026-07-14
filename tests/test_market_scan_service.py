from __future__ import annotations

import sqlite3

import pytest
from domain.broker.events import GatewayEvent
from gateway.command_handlers import GatewayCommandHandler
from gateway.event_factory import make_tr_response_event
from services import market_scan_service
from services.config import Settings
from services.market_scan_service import (
    get_latest_market_scan,
    list_market_scan_errors,
    process_market_scan_event,
    run_market_scan_once,
)
from storage.gateway_command_store import poll_commands
from storage.sqlite import initialize_database


def test_market_scan_queues_request_tr_and_projects_mock_gateway_response(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan.sqlite3")
    settings = Settings(market_scan_enabled=True)

    result = run_market_scan_once(connection, settings=settings, queue_commands=True)
    commands = poll_commands(connection, limit=10)
    events = GatewayCommandHandler().handle(commands[0])
    tr_response = next(event for event in events if event.event_type == "tr_response")
    projection = process_market_scan_event(connection, tr_response, settings=settings)

    command_rows = connection.execute(
        "SELECT command_type FROM gateway_commands ORDER BY created_at"
    ).fetchall()
    order_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
        """
    ).fetchone()["count"]
    latest = get_latest_market_scan(connection, "005930")
    command_payloads = [
        command.payload
        for command in commands
        if command.command_type == "request_tr"
    ]
    connection.close()

    assert result.status == "QUEUED"
    assert result.command_count == 4
    assert len(commands) == 4
    assert {row["command_type"] for row in command_rows} == {"request_tr"}
    assert all(payload["fields"] for payload in command_payloads)
    assert {"종목코드", "종목명", "현재가"}.issubset(set(command_payloads[0]["fields"]))
    by_tr_code = {str(payload["tr_code"]): payload for payload in command_payloads}
    assert by_tr_code["OPT10032"]["row_mode"] == "multi"
    assert by_tr_code["OPT10032"]["output_record_name"] == "거래대금상위"
    assert by_tr_code["OPT10032"]["params"] == {
        "시장구분": "101",
        "관리종목포함": "0",
        "거래소구분": "1",
    }
    assert by_tr_code["OPT10027"]["output_record_name"] == "전일대비등락률상위"
    assert by_tr_code["OPT10027"]["params"] == {
        "시장구분": "101",
        "정렬구분": "1",
        "거래량조건": "0",
        "종목조건": "1",
        "신용조건": "0",
        "상하한포함": "0",
        "가격조건": "0",
        "거래대금조건": "0",
        "거래소구분": "1",
    }
    assert order_count == 0
    assert projection.status == "APPLIED"
    assert latest is not None
    assert latest["metadata"]["parser_status"] == "PILOT_UNVERIFIED"


def test_market_scan_parses_korean_rows_and_latest_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-parse.sqlite3")
    settings = Settings(market_scan_enabled=True)
    event = make_tr_response_event(
        request_id="market_scan:TRADE_VALUE:KOSPI:run1",
        tr_code="OPT10032",
        request_name="market_scan_trade_value_kospi",
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "순위": "1",
                "현재가": "+70000",
                "등락률": "+2.5%",
                "거래대금": "1,200,000,000",
                "거래량": "100000",
            }
        ],
        source="mock_gateway",
    )

    result = process_market_scan_event(connection, event, settings=settings)
    latest = get_latest_market_scan(connection, "005930")
    connection.close()

    assert result.status == "APPLIED"
    assert latest is not None
    assert latest["scan_type"] == "TRADE_VALUE"
    assert latest["market"] == "KOSPI"
    assert latest["rank"] == 1
    assert latest["price"] == 70000
    assert latest["change_rate"] == 2.5
    assert latest["trade_value"] == 1_200_000_000


def test_market_scan_reraises_sqlite_lock_without_recording_data_error(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-scan-locked.sqlite3")
    settings = Settings(market_scan_enabled=True)
    event = make_tr_response_event(
        request_id="market_scan:TRADE_VALUE:KOSPI:locked",
        tr_code="OPT10032",
        request_name="market_scan_trade_value_kospi",
        rows=[
            {
                "종목코드": "005930",
                "종목명": "삼성전자",
                "순위": "1",
                "현재가": "+70000",
                "등락률": "+1.25",
                "거래대금": "1000000000",
                "거래량": "100000",
            }
        ],
        source="test-gateway",
    )

    def raise_locked(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(market_scan_service, "_insert_scan_snapshot", raise_locked)

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        process_market_scan_event(connection, event, settings=settings)

    assert list_market_scan_errors(connection) == []
    assert connection.execute(
        "SELECT COUNT(*) FROM market_scan_snapshots"
    ).fetchone()[0] == 0
    connection.close()


def test_market_scan_accepts_alphanumeric_krx_short_codes(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-alnum.sqlite3")
    settings = Settings(market_scan_enabled=True)
    event = make_tr_response_event(
        request_id="market_scan:CHANGE_RATE:KOSPI:run1",
        tr_code="OPT10027",
        request_name="market_scan_change_rate_kospi",
        rows=[
            {
                "종목코드": "0197X0",
                "종목명": "SOL SK하이닉스선물단일종목인버스2X",
                "현재가": "+10545",
                "등락률": "+34.67",
                "현재거래량": "228934477",
            },
            {
                "종목코드": "A00279K",
                "종목명": "아모레퍼시픽홀딩스3우C",
                "현재가": "+20100",
                "등락률": "+3.08",
                "현재거래량": "30787",
            },
        ],
        source="mock_gateway",
    )

    result = process_market_scan_event(connection, event, settings=settings)
    latest_etf = get_latest_market_scan(connection, "0197X0")
    latest_preferred = get_latest_market_scan(connection, "A00279K")
    errors = list_market_scan_errors(connection)
    connection.close()

    assert result.status == "APPLIED"
    assert result.applied_count == 2
    assert result.error_count == 0
    assert errors == []
    assert latest_etf is not None
    assert latest_etf["code"] == "0197X0"
    assert latest_etf["change_rate"] == 34.67
    assert latest_preferred is not None
    assert latest_preferred["code"] == "00279K"


def test_market_scan_records_row_parse_errors_with_reason_code(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-error.sqlite3")
    settings = Settings(market_scan_enabled=True)
    event = GatewayEvent(
        event_type="tr_response",
        source="mock_gateway",
        payload={
            "request_id": "market_scan:CHANGE_RATE:KOSDAQ:run1",
            "tr_code": "OPT10027",
            "request_name": "market_scan_change_rate_kosdaq",
            "success": True,
            "rows": [{"종목코드": "BAD", "종목명": "bad"}],
        },
    )

    result = process_market_scan_event(connection, event, settings=settings)
    errors = list_market_scan_errors(connection)
    latest_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_scan_latest"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "ERROR"
    assert result.error_count == 1
    assert latest_count == 0
    assert errors[0]["reason_code"] == "MARKET_SCAN_CODE_PARSE_FAILED"
