from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from services.config import Settings
from services.market_index_service import (
    get_latest_market_index_tick,
    get_market_index_readiness,
    list_market_index_bars,
    process_market_index_event,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def index_tick_event(
    event_id: str,
    *,
    index_code: str = "KOSPI",
    index_name: str = "KOSPI",
    price: float = 2800.0,
    change_rate: float = 0.1,
    change_value: float = 2.8,
    ts: datetime = TS,
) -> GatewayEvent:
    tick = BrokerMarketIndexTick(
        index_code=index_code,
        index_name=index_name,
        price=price,
        change_rate=change_rate,
        change_value=change_value,
        trade_time=ts,
        ts=ts,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
    )


def append_and_project(connection, event: GatewayEvent, settings: Settings):
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    assert append_result.duplicate is False
    return process_market_index_event(connection, event, settings=settings)


def test_market_index_tick_projection_updates_latest_samples_and_bars(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_index.sqlite3")
    settings = Settings(market_index_stale_sec=999_999_999)
    first = index_tick_event("evt_kospi_1", price=2800.0, ts=TS)
    second = index_tick_event(
        "evt_kospi_2",
        price=2805.0,
        change_rate=0.25,
        change_value=7.0,
        ts=TS + timedelta(seconds=5),
    )

    append_and_project(connection, first, settings)
    result = append_and_project(connection, second, settings)

    latest = get_latest_market_index_tick(connection, "KOSPI")
    samples = connection.execute(
        "SELECT event_id, price FROM market_index_tick_samples ORDER BY event_ts"
    ).fetchall()
    bars_60 = list_market_index_bars(connection, "KOSPI", interval_sec=60)
    bars_180 = list_market_index_bars(connection, "KOSPI", interval_sec=180)
    bars_300 = list_market_index_bars(connection, "KOSPI", interval_sec=300)
    readiness = get_market_index_readiness(connection, "KOSPI", settings=settings)
    stock_tick_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_ticks_latest"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "APPLIED"
    assert latest is not None
    assert latest["price"] == 2805.0
    assert [row["event_id"] for row in samples] == ["evt_kospi_1", "evt_kospi_2"]
    assert bars_60[0]["open"] == 2800.0
    assert bars_60[0]["high"] == 2805.0
    assert bars_60[0]["close"] == 2805.0
    assert bars_60[0]["tick_count"] == 2
    assert len(bars_180) == 1
    assert len(bars_300) == 1
    assert readiness["quality_status"] == "FRESH"
    assert stock_tick_count == 0


def test_older_market_index_tick_is_sampled_without_rewinding_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_index.sqlite3")
    settings = Settings(market_index_stale_sec=999_999_999)
    newer = index_tick_event(
        "evt_kospi_newer",
        index_code="KOSPI",
        price=2805.0,
        ts=TS + timedelta(seconds=10),
    )
    older = index_tick_event(
        "evt_kospi_older",
        index_code="KOSPI",
        price=2795.0,
        ts=TS + timedelta(seconds=5),
    )

    append_and_project(connection, newer, settings)
    append_gateway_event(connection, older)
    result = process_market_index_event(connection, older, settings=settings)

    latest = get_latest_market_index_tick(connection, "KOSPI")
    samples = connection.execute(
        "SELECT event_id FROM market_index_tick_samples ORDER BY event_ts"
    ).fetchall()
    bars_60 = list_market_index_bars(connection, "KOSPI", interval_sec=60)
    connection.close()

    assert result.status == "APPLIED"
    assert result.applied_count == 1
    assert latest is not None
    assert latest["event_id"] == "evt_kospi_newer"
    assert latest["price"] == 2805.0
    assert [row["event_id"] for row in samples] == [
        "evt_kospi_older",
        "evt_kospi_newer",
    ]
    assert bars_60[0]["close"] == 2805.0
    assert bars_60[0]["tick_count"] == 1


def test_market_index_readiness_reports_stale_without_hard_failure(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market_index_stale.sqlite3")
    stale_settings = Settings(market_index_stale_sec=1)
    event = index_tick_event("evt_stale_kosdaq", index_code="KOSDAQ", price=900.0, ts=TS)
    append_and_project(connection, event, stale_settings)

    readiness = get_market_index_readiness(connection, "KOSDAQ", settings=stale_settings)
    connection.close()

    assert readiness["quality_status"] in {"STALE", "DEGRADED"}
    assert any(reason.startswith("INDEX_TICK_") for reason in readiness["reason_codes"])


def test_market_index_records_unverified_and_implausible_guard(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-guard.sqlite3")
    settings = Settings(market_index_stale_sec=999_999_999)
    unverified = index_tick_event(
        "evt_kospi_unverified",
        index_code="KOSPI",
        price=2800.0,
        ts=TS,
    )
    unverified_payload = dict(unverified.payload)
    unverified_payload["metadata"] = {"parser_status": "PILOT_UNVERIFIED"}
    unverified = GatewayEvent(
        event_id=unverified.event_id,
        event_type=unverified.event_type,
        source=unverified.source,
        payload=unverified_payload,
        ts=unverified.ts,
    )
    implausible = index_tick_event(
        "evt_kospi_implausible",
        index_code="KOSPI",
        price=800.0,
        ts=TS + timedelta(seconds=5),
    )

    append_and_project(connection, unverified, settings)
    result = append_and_project(connection, implausible, settings)
    latest = get_latest_market_index_tick(connection, "KOSPI")
    error = connection.execute(
        "SELECT reason_code FROM market_index_projection_errors ORDER BY id DESC LIMIT 1"
    ).fetchone()
    connection.close()

    assert latest is not None
    assert latest["parser_status"] == "PILOT_UNVERIFIED"
    assert latest["unverified"] is True
    assert result.status == "ERROR"
    assert error["reason_code"] == "INDEX_IMPLAUSIBLE"


def test_market_index_accepts_high_kospi_level_after_2026_rally(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-high-kospi.sqlite3")
    settings = Settings(market_index_stale_sec=999_999_999)
    event = index_tick_event(
        "evt_kospi_high_2026",
        index_code="KOSPI",
        price=7700.0,
        ts=TS,
    )

    result = append_and_project(connection, event, settings)
    latest = get_latest_market_index_tick(connection, "KOSPI")
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_index_projection_errors"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "APPLIED"
    assert latest is not None
    assert latest["price"] == 7700.0
    assert error_count == 0
