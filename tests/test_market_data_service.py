from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import utc_now
from services.config import Settings
from services.market_data_service import (
    get_latest_tick,
    get_market_data_readiness,
    list_bars,
    process_gateway_event,
    rebuild_market_data_projection,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def price_tick_event(
    event_id: str,
    *,
    price: int = 70000,
    volume: int = 1000,
    trade_value: int | None = None,
    ts: datetime = TS,
    trade_time: datetime = TS,
) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=price,
        change_rate=0.1,
        volume=volume,
        trade_value=trade_value if trade_value is not None else price * volume,
        execution_strength=101.5,
        best_bid=max(price - 100, 1),
        best_ask=price,
        spread_ticks=1,
        day_high=price + 1000,
        day_low=max(price - 1000, 1),
        trade_time=trade_time,
        ts=ts,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
    )


def append_and_project(connection, event: GatewayEvent, settings: Settings | None = None):
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    assert append_result.duplicate is False
    return process_gateway_event(connection, event, settings=settings or Settings())


def test_price_tick_projection_updates_latest_samples_bars_and_deltas(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings()
    first = price_tick_event("evt_tick_1", price=70000, volume=1000, trade_value=70_000_000)
    second = price_tick_event(
        "evt_tick_2",
        price=70100,
        volume=1010,
        trade_value=70_701_000,
        ts=TS + timedelta(seconds=5),
        trade_time=TS + timedelta(seconds=5),
    )

    append_and_project(connection, first, settings)
    append_and_project(connection, second, settings)

    latest = get_latest_tick(connection, "A005930")
    samples = connection.execute(
        """
        SELECT event_id, volume_delta, trade_value_delta
        FROM market_tick_samples
        ORDER BY event_ts
        """
    ).fetchall()
    bars_60 = list_bars(connection, "005930", interval_sec=60)
    bars_180 = list_bars(connection, "005930", interval_sec=180)
    bars_300 = list_bars(connection, "005930", interval_sec=300)
    connection.close()

    assert latest is not None
    assert latest["price"] == 70100
    assert latest["quality_status"] == "FRESH"
    assert samples[0]["volume_delta"] == 1000
    assert samples[1]["volume_delta"] == 10
    assert samples[1]["trade_value_delta"] == 701_000
    assert len(bars_60) == 1
    assert bars_60[0]["open"] == 70000
    assert bars_60[0]["high"] == 70100
    assert bars_60[0]["low"] == 70000
    assert bars_60[0]["close"] == 70100
    assert bars_60[0]["tick_count"] == 2
    assert bars_60[0]["vwap"] is not None
    assert len(bars_180) == 1
    assert len(bars_300) == 1


def test_price_tick_duplicate_and_negative_delta_are_not_double_counted(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    first = price_tick_event("evt_tick_1", volume=2000, trade_value=140_000_000)
    reset_like = price_tick_event(
        "evt_tick_2",
        price=70100,
        volume=1000,
        trade_value=70_100_000,
        ts=TS + timedelta(seconds=5),
        trade_time=TS + timedelta(seconds=5),
    )

    append_and_project(connection, first)
    duplicate = process_gateway_event(connection, first, settings=Settings())
    append_and_project(connection, reset_like)

    sample_rows = connection.execute(
        """
        SELECT event_id, volume_delta, trade_value_delta
        FROM market_tick_samples
        ORDER BY event_ts
        """
    ).fetchall()
    bar = list_bars(connection, "005930", interval_sec=60)[0]
    connection.close()

    assert duplicate.status == "DUPLICATE"
    assert len(sample_rows) == 2
    assert sample_rows[1]["volume_delta"] == 0
    assert sample_rows[1]["trade_value_delta"] == 0
    assert bar["tick_count"] == 2


def test_price_missing_tick_is_ignored_without_polluting_latest_or_bars(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    event = price_tick_event("evt_quote_only", price=1, volume=0, trade_value=0)
    payload = dict(event.payload)
    payload["metadata"] = {
        "real_type": "주식우선호가",
        "reason_codes": ["PRICE_MISSING", "TRADE_VALUE_MISSING"],
    }
    event = GatewayEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        source=event.source,
        payload=payload,
        ts=event.ts,
    )

    append_result = append_gateway_event(connection, event)
    result = process_gateway_event(connection, event, settings=Settings())

    sample_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tick_samples"
    ).fetchone()["count"]
    bar_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_minute_bars"
    ).fetchone()["count"]
    error = connection.execute("SELECT * FROM market_projection_errors").fetchone()
    latest = get_latest_tick(connection, "005930")
    connection.close()

    assert append_result.status == "ACCEPTED"
    assert result.status == "IGNORED"
    assert result.ignored_count == 1
    assert sample_count == 0
    assert bar_count == 0
    assert latest is None
    assert error is None


def test_condition_event_projection_preserves_actions_and_metadata(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    enter = GatewayEvent(
        event_id="evt_condition_enter",
        event_type="condition_event",
        source="test-gateway",
        ts=TS,
        payload=BrokerConditionEvent(
            condition_id="cond1",
            condition_name="Breakout",
            code="005930",
            name="삼성전자",
            action="ENTER",
            price=70000,
            metadata={"rank": 1},
            ts=TS,
        ).to_dict(),
    )
    exit_event = GatewayEvent(
        event_id="evt_condition_exit",
        event_type="condition_event",
        source="test-gateway",
        ts=TS + timedelta(seconds=30),
        payload=BrokerConditionEvent(
            condition_id="cond1",
            condition_name="Breakout",
            code="005930",
            name="삼성전자",
            action="EXIT",
            price=69900,
            metadata={"rank": 2},
            ts=TS + timedelta(seconds=30),
        ).to_dict(),
    )

    append_and_project(connection, enter)
    append_and_project(connection, exit_event)

    signal_rows = connection.execute(
        "SELECT action, metadata_json FROM market_condition_signals ORDER BY event_ts"
    ).fetchall()
    latest = connection.execute(
        "SELECT action, event_id, metadata_json FROM market_condition_latest"
    ).fetchone()
    connection.close()

    assert [row["action"] for row in signal_rows] == ["ENTER", "EXIT"]
    assert json.loads(signal_rows[0]["metadata_json"]) == {"rank": 1}
    assert latest["action"] == "EXIT"
    assert latest["event_id"] == "evt_condition_exit"
    assert json.loads(latest["metadata_json"]) == {"rank": 2}


def test_tr_response_projection_stores_rows_without_touching_latest_tick(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    response = BrokerTrResponse(
        request_id="tr1",
        tr_code="OPT10001",
        request_name="stock_basic",
        success=True,
        rows=[
            {"code": "A005930", "name": "삼성전자"},
            {"stock_code": "000660", "name": "SK하이닉스"},
            {"종목코드": "035420", "name": "NAVER"},
        ],
        ts=TS,
    )
    event = GatewayEvent(
        event_id="evt_tr_1",
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=TS,
    )

    append_and_project(connection, event)

    rows = connection.execute(
        "SELECT code, row_json FROM market_tr_snapshots ORDER BY id"
    ).fetchall()
    latest = get_latest_tick(connection, "005930")
    connection.close()

    assert [row["code"] for row in rows] == ["005930", "000660", "035420"]
    assert json.loads(rows[0]["row_json"])["code"] == "A005930"
    assert latest is None


def test_readiness_reports_missing_fresh_stale_and_bar_gaps(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings(market_data_bar_intervals_sec=(60, 180, 300, 600))
    missing = get_market_data_readiness(connection, "005930", settings=settings)

    now = utc_now()
    fresh_event = price_tick_event("evt_fresh", ts=now, trade_time=now)
    append_and_project(connection, fresh_event, settings=Settings())
    fresh = get_market_data_readiness(connection, "005930", settings=settings)

    stale_connection = initialize_database(tmp_path / "stale.sqlite3")
    stale_settings = Settings(market_data_tick_stale_sec=10, market_data_degraded_tick_stale_sec=30)
    stale_time = utc_now() - timedelta(seconds=20)
    stale_event = price_tick_event("evt_stale", ts=stale_time, trade_time=stale_time)
    append_and_project(stale_connection, stale_event, settings=stale_settings)
    stale = get_market_data_readiness(stale_connection, "005930", settings=stale_settings)
    connection.close()
    stale_connection.close()

    assert missing["quality_status"] == "MISSING"
    assert "TICK_MISSING" in missing["reason_codes"]
    assert fresh["quality_status"] == "FRESH"
    assert fresh["has_1m_bar"] is True
    assert fresh["vwap_ready"] is True
    assert "BAR_MISSING" in fresh["reason_codes"]
    assert "BAR_MISSING_600" in fresh["reason_codes"]
    assert stale["quality_status"] == "STALE"
    assert "TICK_STALE" in stale["reason_codes"]


def test_projection_errors_are_recorded_without_raising(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    invalid_event = GatewayEvent(
        event_id="evt_invalid_tick",
        event_type="price_tick",
        source="test-gateway",
        payload={"code": "005930"},
        ts=TS,
    )
    append_result = append_gateway_event(connection, invalid_event)

    result = process_gateway_event(connection, invalid_event, settings=Settings())

    errors = connection.execute("SELECT * FROM market_projection_errors").fetchall()
    latest = get_latest_tick(connection, "005930")
    connection.close()

    assert append_result.status == "REJECTED"
    assert result.status == "ERROR"
    assert result.error_count == 1
    assert len(errors) == 1
    assert latest is None


def test_rebuild_replays_accepted_gateway_events_with_clear_guard(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    tick = price_tick_event("evt_rebuild_tick", ts=TS, trade_time=TS)
    condition = GatewayEvent(
        event_id="evt_rebuild_condition",
        event_type="condition_event",
        source="test-gateway",
        ts=TS + timedelta(seconds=1),
        payload=BrokerConditionEvent(
            condition_id="cond1",
            condition_name="Breakout",
            code="005930",
            name="삼성전자",
            action="ENTER",
            price=70000,
            metadata={"mock": True},
            ts=TS,
        ).to_dict(),
    )
    append_gateway_event(connection, tick)
    append_gateway_event(connection, condition)

    try:
        rebuild_market_data_projection(connection, clear_projection=True)
    except ValueError as exc:
        assert "require_clear=True" in str(exc)
    else:
        raise AssertionError("expected clear guard to reject unsafe rebuild")

    result = rebuild_market_data_projection(
        connection,
        clear_projection=True,
        require_clear=True,
        settings=Settings(),
    )

    latest = get_latest_tick(connection, "005930")
    signal_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_condition_signals"
    ).fetchone()["count"]
    connection.close()

    assert result.processed_count == 2
    assert result.applied_count == 6
    assert result.error_count == 0
    assert latest is not None
    assert signal_count == 1
