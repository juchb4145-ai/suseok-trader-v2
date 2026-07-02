from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.commands import GatewayCommand
from fastapi.testclient import TestClient
from gateway.event_factory import make_tr_response_event
from storage.gateway_command_store import GatewayCommandStatus, enqueue_command
from storage.sqlite import open_connection

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC).isoformat()


def heartbeat_event(event_id: str = "evt_api_heartbeat") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "heartbeat",
        "source": "test-gateway",
        "ts": TS,
        "payload": {"status": "ok"},
    }


def test_gateway_event_api_accepts_duplicates_and_lists_recent(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        first = client.post(
            "/api/gateway/events",
            json=heartbeat_event(),
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events",
            json=heartbeat_event(),
            headers={"X-Local-Token": "test-token"},
        )
        recent = client.get("/api/gateway/events/recent")
        gateway_status = client.get("/api/gateway/status")

    assert first.status_code == 200
    assert first.json()["accepted"] is True
    assert first.json()["duplicate"] is False
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert recent.status_code == 200
    assert recent.json()["events"][0]["event_id"] == "evt_api_heartbeat"
    assert gateway_status.status_code == 200
    assert gateway_status.json()["last_heartbeat_at"] is not None
    assert gateway_status.json()["recent_event_count"] == 1
    assert gateway_status.json()["order_commands_allowed"] is False


def test_gateway_commands_api_dispatches_queued_commands(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))

    with TestClient(app) as client:
        connection = open_connection(db_path)
        try:
            enqueue_command(
                connection,
                GatewayCommand(
                    command_id="cmd_api_poll",
                    command_type="request_tr",
                    source="core",
                    payload={"tr_code": "OPT10001", "params": {"code": "005930"}},
                ),
            )
        finally:
            connection.close()

        response = client.get(
            "/api/gateway/commands",
            headers={"X-Local-Token": "test-token"},
        )
        status_response = client.get("/api/gateway/commands/status")

    assert response.status_code == 200
    assert response.json()["commands"][0]["command_id"] == "cmd_api_poll"
    assert response.json()["commands"][0]["command_type"] == "request_tr"
    assert status_response.status_code == 200
    assert status_response.json()["counts"][GatewayCommandStatus.DISPATCHED.value] == 1


def test_gateway_event_api_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        missing = client.post("/api/gateway/events", json=heartbeat_event("evt_missing_token"))
        wrong = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_wrong_token"),
            headers={"X-Local-Token": "wrong"},
        )
        accepted = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_with_token"),
            headers={"X-Core-Token": "secret-token"},
        )
        read_only_status = client.get("/api/gateway/status")

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert read_only_status.status_code == 200
    assert read_only_status.json()["token_required"] is True


def test_gateway_status_exposes_market_index_adapter_separate_from_projection_errors(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api_index_status.sqlite3"))

    heartbeat = heartbeat_event("evt_index_adapter_heartbeat")
    heartbeat["payload"] = {
        "status": "ok",
        "market_index_enabled": True,
        "market_index_registered_codes": ["KOSPI", "KOSDAQ"],
        "market_index_callback_count": 1,
        "parsed_market_index_tick_count": 0,
        "market_index_parse_error_count": 1,
        "latest_market_index_tick_at": "",
        "latest_market_index_parse_error": {
            "reason": "INDEX_PARSE_ERROR",
            "index_code": "KOSPI",
        },
        "market_index_adapter_health": "PARSE_ERROR",
    }
    invalid_index_event = {
        "event_id": "evt_invalid_index_projection",
        "event_type": "market_index_tick",
        "source": "test-gateway",
        "ts": TS,
        "payload": {"index_code": "KOSPI", "index_name": "KOSPI"},
    }

    with TestClient(app) as client:
        heartbeat_response = client.post(
            "/api/gateway/events",
            json=heartbeat,
            headers={"X-Local-Token": "test-token"},
        )
        projection_response = client.post(
            "/api/gateway/events",
            json=invalid_index_event,
            headers={"X-Local-Token": "test-token"},
        )
        gateway_status = client.get("/api/gateway/status")
        market_index_status = client.get("/api/market-indexes/status")

    assert heartbeat_response.status_code == 200
    assert projection_response.status_code == 200
    assert projection_response.json()["projection_statuses"]["market_index"] == "ERROR"
    assert gateway_status.json()["market_index_parse_error_count"] == 1
    assert gateway_status.json()["latest_market_index_parse_error"]["reason"] == (
        "INDEX_PARSE_ERROR"
    )
    assert market_index_status.json()["projection_error_count"] == 1


def test_gateway_event_api_projects_market_scan_tr_response(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api_market_scan.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_SCAN_ENABLED", "true")

    event = make_tr_response_event(
        request_id="market_scan:TRADE_VALUE:KOSPI:run-api",
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
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        latest = connection.execute(
            """
            SELECT code, scan_type, market, trade_value, metadata_json
            FROM market_scan_latest
            WHERE code = '005930'
            """
        ).fetchone()
        order_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM gateway_commands
            WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
            """
        ).fetchone()["count"]
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["market_scan"] == "APPLIED"
    assert latest is not None
    assert latest["scan_type"] == "TRADE_VALUE"
    assert latest["market"] == "KOSPI"
    assert latest["trade_value"] == 1_200_000_000
    assert '"parser_status":"PILOT_UNVERIFIED"' in latest["metadata_json"]
    assert order_count == 0
