from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import (
    make_condition_event,
    make_price_tick_event,
    make_tr_response_event,
)


def test_market_data_api_reads_projection_after_gateway_posts(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        tick_event = make_price_tick_event(price=70000, volume=1000)
        condition_event = make_condition_event(action="ENTER", price=70000)
        tr_event = make_tr_response_event(
            request_id="tr_api",
            tr_code="OPT10001",
            request_name="stock_basic",
            rows=[{"code": "A005930", "name": "삼성전자", "price": 70000}],
        )

        tick_response = client.post("/api/gateway/events", json=tick_event.to_dict())
        duplicate_response = client.post("/api/gateway/events", json=tick_event.to_dict())
        condition_response = client.post("/api/gateway/events", json=condition_event.to_dict())
        tr_response = client.post("/api/gateway/events", json=tr_event.to_dict())

        status = client.get("/api/market-data/status")
        latest = client.get("/api/market-data/ticks/latest")
        tick = client.get("/api/market-data/ticks/A005930")
        bars = client.get("/api/market-data/bars/005930?interval_sec=60")
        readiness = client.get("/api/market-data/readiness/005930")
        conditions = client.get("/api/market-data/conditions/recent")
        tr_snapshots = client.get("/api/market-data/tr-snapshots/recent")
        errors = client.get("/api/market-data/projection-errors")

    assert tick_response.status_code == 200
    assert tick_response.json()["projection_status"] == "APPLIED"
    assert duplicate_response.status_code == 200
    assert duplicate_response.json()["duplicate"] is True
    assert "projection_status" not in duplicate_response.json()
    assert condition_response.json()["projection_status"] == "APPLIED"
    assert tr_response.json()["projection_status"] == "APPLIED"

    assert status.status_code == 200
    assert status.json()["latest_tick_count"] == 1
    assert status.json()["sample_count"] == 1
    assert status.json()["bar_count"] == 3
    assert status.json()["condition_signal_count"] == 1
    assert status.json()["tr_snapshot_count"] == 1
    assert status.json()["projection_error_count"] == 0
    assert status.json()["bar_intervals_sec"] == [60, 180, 300]

    assert latest.status_code == 200
    assert latest.json()["ticks"][0]["code"] == "005930"
    assert tick.status_code == 200
    assert tick.json()["tick"]["price"] == 70000
    assert bars.status_code == 200
    assert bars.json()["bars"][0]["tick_count"] == 1
    assert readiness.status_code == 200
    assert readiness.json()["has_latest_tick"] is True
    assert readiness.json()["vwap_ready"] is True
    assert conditions.status_code == 200
    assert conditions.json()["signals"][0]["action"] == "ENTER"
    assert tr_snapshots.status_code == 200
    assert tr_snapshots.json()["snapshots"][0]["code"] == "005930"
    assert errors.status_code == 200
    assert errors.json()["errors"] == []


def test_market_data_tick_endpoint_404s_for_missing_code(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/api/market-data/ticks/005930")

    assert response.status_code == 404


def test_invalid_price_tick_rejected_without_projection(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json={
                "event_id": "evt_invalid_price_tick",
                "event_type": "price_tick",
                "source": "test-gateway",
                "payload": {"code": "005930"},
            },
        )
        status = client.get("/api/market-data/status")

    assert response.status_code == 422
    assert status.json()["latest_tick_count"] == 0
    assert status.json()["sample_count"] == 0
