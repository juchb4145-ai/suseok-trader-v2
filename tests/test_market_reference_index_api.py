from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from tests.test_market_index_service import index_tick_event


def test_market_reference_api_reads_memberships_after_gateway_post(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "reference_api.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json={
                "event_id": "evt_api_market_symbols",
                "event_type": "market_symbols",
                "source": "test-gateway",
                "payload": {
                    "KOSPI": [{"code": "005930", "name": "삼성전자"}],
                    "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
                },
            },
        )
        duplicate = client.post(
            "/api/gateway/events",
            json={
                "event_id": "evt_api_market_symbols",
                "event_type": "market_symbols",
                "source": "test-gateway",
                "payload": {
                    "KOSPI": [{"code": "005930", "name": "삼성전자"}],
                    "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
                },
            },
        )
        symbols = client.get("/api/market-reference/symbols")
        samsung = client.get("/api/market-reference/symbols/A005930")

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["market_reference"] == "APPLIED"
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert "projection_statuses" not in duplicate.json()
    assert symbols.status_code == 200
    assert len(symbols.json()["symbols"]) == 2
    assert samsung.status_code == 200
    assert samsung.json()["symbol"]["market"] == "KOSPI"


def test_market_index_api_reads_projection_after_gateway_post(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "index_api.sqlite3"))
    monkeypatch.setenv("MARKET_INDEX_STALE_SEC", "999999999")

    event = index_tick_event("evt_api_kospi_index", index_code="KOSPI", price=2800.0)
    with TestClient(app) as client:
        response = client.post("/api/gateway/events", json=event.to_dict())
        status = client.get("/api/market-indexes/status")
        latest = client.get("/api/market-indexes/latest")
        tick = client.get("/api/market-indexes/KOSPI")
        bars = client.get("/api/market-indexes/KOSPI/bars?interval_sec=60")
        regime = client.get("/api/market-regime/latest")

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["market_index"] == "APPLIED"
    assert "market_regime" in response.json()["projection_statuses"]
    assert status.status_code == 200
    assert status.json()["latest_tick_count"] == 1
    assert latest.status_code == 200
    assert latest.json()["ticks"][0]["index_code"] == "KOSPI"
    assert tick.status_code == 200
    assert tick.json()["tick"]["price"] == 2800.0
    assert tick.json()["readiness"]["quality_status"] == "FRESH"
    assert bars.status_code == 200
    assert bars.json()["bars"][0]["tick_count"] == 1
    assert regime.status_code == 200
    assert regime.json()["latest"]["primary_index_code"] == "KOSPI"


def test_gateway_index_event_throttles_recent_market_regime_rebuild(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "index_api_throttle.sqlite3"))
    monkeypatch.setenv("MARKET_INDEX_STALE_SEC", "999999999")

    first = index_tick_event("evt_api_kospi_index_first", index_code="KOSPI", price=2800.0)
    second = index_tick_event("evt_api_kospi_index_second", index_code="KOSPI", price=2801.0)

    with TestClient(app) as client:
        first_response = client.post("/api/gateway/events", json=first.to_dict())
        second_response = client.post("/api/gateway/events", json=second.to_dict())

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert first_response.json()["projection_statuses"]["market_regime"] != "SKIPPED_RECENT"
    assert second_response.json()["projection_statuses"]["market_index"] == "APPLIED"
    assert second_response.json()["projection_statuses"]["market_regime"] == "SKIPPED_RECENT"
