from __future__ import annotations

from datetime import datetime, timedelta, timezone

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from fastapi.testclient import TestClient
from gateway.event_factory import (
    make_condition_event,
    make_price_tick_event,
    make_tr_response_event,
)


def test_market_data_api_reads_projection_after_gateway_posts(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        tick_event = make_price_tick_event(price=70000, volume=1000)
        condition_event = make_condition_event(action="ENTER", price=70000)
        tr_event = make_tr_response_event(
            request_id="tr_api",
            tr_code="OPT10001",
            request_name="stock_basic",
            rows=[{"code": "A005930", "name": "삼성전자", "price": 70000}],
        )

        tick_response = client.post(
            "/api/gateway/events",
            json=tick_event.to_dict(),
            headers=headers,
        )
        duplicate_response = client.post(
            "/api/gateway/events",
            json=tick_event.to_dict(),
            headers=headers,
        )
        condition_response = client.post(
            "/api/gateway/events",
            json=condition_event.to_dict(),
            headers=headers,
        )
        tr_response = client.post(
            "/api/gateway/events",
            json=tr_event.to_dict(),
            headers=headers,
        )

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


def test_market_data_api_exchange_query_param_filters_ticks_and_bars(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        krx_event = make_price_tick_event(price=70000, volume=1000)
        nxt_event = make_price_tick_event(price=70200, volume=500)
        nxt_payload = dict(nxt_event.payload)
        nxt_payload["metadata"] = {"exchange": "NXT"}
        nxt_event = type(nxt_event)(
            event_id=nxt_event.event_id,
            event_type=nxt_event.event_type,
            source=nxt_event.source,
            payload=nxt_payload,
            ts=nxt_event.ts,
        )

        client.post("/api/gateway/events", json=krx_event.to_dict(), headers=headers)
        client.post("/api/gateway/events", json=nxt_event.to_dict(), headers=headers)

        latest_default = client.get("/api/market-data/ticks/005930")
        latest_nxt = client.get("/api/market-data/ticks/005930?exchange=NXT")
        latest_all = client.get("/api/market-data/ticks/005930?exchange=ALL")
        bars_nxt = client.get("/api/market-data/bars/005930?exchange=NXT&interval_sec=60")
        readiness_all = client.get("/api/market-data/readiness/005930?exchange=ALL")

    assert latest_default.status_code == 200
    assert latest_default.json()["tick"]["exchange"] == "KRX"
    assert latest_default.json()["tick"]["price"] == 70000
    assert latest_nxt.status_code == 200
    assert latest_nxt.json()["tick"]["exchange"] == "NXT"
    assert latest_nxt.json()["tick"]["price"] == 70200
    assert latest_all.status_code == 200
    assert {tick["exchange"] for tick in latest_all.json()["ticks"]} == {"KRX", "NXT"}
    assert bars_nxt.status_code == 200
    assert bars_nxt.json()["exchange"] == "NXT"
    assert bars_nxt.json()["bars"][0]["exchange"] == "NXT"
    assert readiness_all.status_code == 200
    assert {
        item["exchange"] for item in readiness_all.json()["readiness"]
    } == {"KRX", "NXT"}


def test_market_data_api_returns_premarket_snapshots(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))
    monkeypatch.setenv("MARKET_DATA_PREMARKET_SNAPSHOT_ENABLED", "true")
    kst = timezone(timedelta(hours=9))
    previous_close_time = datetime(2026, 6, 25, 15, 19, tzinfo=kst)
    premarket_time = datetime(2026, 6, 26, 8, 10, tzinfo=kst)

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        client.post(
            "/api/gateway/events",
            json=_price_tick_event(
                "evt_api_prev_close",
                price=10_000,
                volume=100,
                trade_value=1_000_000,
                ts=previous_close_time,
                trade_time=previous_close_time,
            ).to_dict(),
            headers=headers,
        )
        client.post(
            "/api/gateway/events",
            json=_price_tick_event(
                "evt_api_premarket",
                price=10_500,
                volume=10,
                trade_value=105_000,
                ts=premarket_time,
                trade_time=premarket_time,
                exchange="NXT",
            ).to_dict(),
            headers=headers,
        )
        response = client.get("/api/market-data/premarket/2026-06-26")

    assert response.status_code == 200
    assert response.json()["trade_date"] == "2026-06-26"
    assert len(response.json()["snapshots"]) == 1
    assert response.json()["snapshots"][0]["premarket_gap_pct"] == 5.0


def test_invalid_price_tick_rejected_without_projection(tmp_path, monkeypatch) -> None:
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
            headers={"X-Local-Token": "test-token"},
        )
        status = client.get("/api/market-data/status")

    assert response.status_code == 422
    assert status.json()["latest_tick_count"] == 0
    assert status.json()["sample_count"] == 0


def _price_tick_event(
    event_id: str,
    *,
    price: int,
    volume: int,
    trade_value: int,
    ts: datetime,
    trade_time: datetime,
    exchange: str | None = None,
) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=price,
        change_rate=0.0,
        volume=volume,
        trade_value=trade_value,
        execution_strength=100.0,
        best_bid=max(price - 100, 1),
        best_ask=price,
        spread_ticks=1,
        day_high=price,
        day_low=price,
        trade_time=trade_time,
        ts=ts,
    )
    payload = tick.to_dict()
    if exchange is not None:
        payload["metadata"] = {"exchange": exchange}
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=payload,
        ts=ts,
    )
