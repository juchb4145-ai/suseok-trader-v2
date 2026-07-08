from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import utc_now
from services.config import Settings
from services.market_data_service import (
    get_latest_tick,
    get_market_data_projection_watermark,
    get_market_data_readiness,
    get_market_data_status,
    list_bars,
    list_cross_exchange_observations,
    list_latest_ticks,
    list_latest_ticks_for_code,
    list_premarket_snapshots,
    market_session_for_tick,
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
    exchange: str | None = None,
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


def test_price_tick_projection_splits_same_code_by_exchange(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings()
    kst = timezone(timedelta(hours=9))
    regular_time = datetime(2026, 6, 26, 9, 1, tzinfo=kst)
    krx = price_tick_event(
        "evt_tick_krx",
        price=70000,
        volume=1000,
        trade_value=70_000_000,
        ts=regular_time,
        trade_time=regular_time,
    )
    nxt = price_tick_event(
        "evt_tick_nxt",
        price=70200,
        volume=500,
        trade_value=35_100_000,
        ts=regular_time + timedelta(seconds=5),
        trade_time=regular_time + timedelta(seconds=5),
        exchange="NXT",
    )

    append_and_project(connection, krx, settings)
    append_and_project(connection, nxt, settings)

    krx_latest = get_latest_tick(connection, "005930")
    nxt_latest = get_latest_tick(connection, "005930", exchange="NXT")
    all_latest = list_latest_ticks_for_code(connection, "005930", exchange="ALL")
    latest_list_all = list_latest_ticks(connection, exchange="ALL")
    samples = connection.execute(
        """
        SELECT event_id, exchange, volume_delta, trade_value_delta
        FROM market_tick_samples
        ORDER BY event_ts
        """
    ).fetchall()
    krx_bars = list_bars(connection, "005930", interval_sec=60)
    nxt_bars = list_bars(connection, "005930", exchange="NXT", interval_sec=60)
    all_bars = list_bars(connection, "005930", exchange="ALL", interval_sec=60)
    connection.close()

    assert krx_latest is not None
    assert krx_latest["exchange"] == "KRX"
    assert krx_latest["session"] == "REGULAR"
    assert krx_latest["price"] == 70000
    assert nxt_latest is not None
    assert nxt_latest["exchange"] == "NXT"
    assert nxt_latest["price"] == 70200
    assert {tick["exchange"] for tick in all_latest} == {"KRX", "NXT"}
    assert {tick["exchange"] for tick in latest_list_all} == {"KRX", "NXT"}
    assert [(row["exchange"], row["volume_delta"]) for row in samples] == [
        ("KRX", 1000),
        ("NXT", 500),
    ]
    assert samples[1]["trade_value_delta"] == 35_100_000
    assert len(krx_bars) == 1
    assert len(nxt_bars) == 1
    assert len(all_bars) == 2
    assert krx_bars[0]["close"] == 70000
    assert nxt_bars[0]["close"] == 70200


def test_cross_exchange_observation_calculates_divergence_and_volume_share(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings(market_data_bar_intervals_sec=(60,))
    kst = timezone(timedelta(hours=9))
    regular_time = datetime(2026, 6, 26, 9, 1, tzinfo=kst)

    append_and_project(
        connection,
        price_tick_event(
            "evt_cross_krx",
            price=10_000,
            volume=100,
            trade_value=1_000_000,
            ts=regular_time,
            trade_time=regular_time,
        ),
        settings,
    )
    append_and_project(
        connection,
        price_tick_event(
            "evt_cross_nxt",
            price=10_100,
            volume=40,
            trade_value=404_000,
            ts=regular_time + timedelta(seconds=5),
            trade_time=regular_time + timedelta(seconds=5),
            exchange="NXT",
        ),
        settings,
    )

    observations = list_cross_exchange_observations(connection, "005930")
    connection.close()

    assert len(observations) == 1
    observation = observations[0]
    assert observation["krx_last_price"] == 10_000
    assert observation["nxt_last_price"] == 10_100
    assert observation["divergence_bp"] == 100.0
    assert observation["krx_volume"] == 100
    assert observation["nxt_volume"] == 40
    assert round(observation["krx_volume_share"], 4) == round(100 / 140, 4)
    assert round(observation["nxt_volume_share"], 4) == round(40 / 140, 4)
    assert observation["krx_tick_count"] == 1
    assert observation["nxt_tick_count"] == 1
    assert observation["total_tick_count"] == 2
    assert observation["metadata"]["both_markets_present"] is True


def test_cross_exchange_observation_records_null_divergence_for_single_market(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings(market_data_bar_intervals_sec=(60,))
    kst = timezone(timedelta(hours=9))
    regular_time = datetime(2026, 6, 26, 9, 1, tzinfo=kst)

    append_and_project(
        connection,
        price_tick_event(
            "evt_cross_krx_only",
            price=10_000,
            volume=100,
            trade_value=1_000_000,
            ts=regular_time,
            trade_time=regular_time,
        ),
        settings,
    )

    observations = list_cross_exchange_observations(connection, "005930")
    connection.close()

    assert len(observations) == 1
    observation = observations[0]
    assert observation["krx_last_price"] == 10_000
    assert observation["nxt_last_price"] is None
    assert observation["divergence_bp"] is None
    assert observation["krx_tick_count"] == 1
    assert observation["nxt_tick_count"] == 0
    assert observation["metadata"]["both_markets_present"] is False


def test_nxt_session_boundary_prevents_bar_merge(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    kst = timezone(timedelta(hours=9))
    premarket_time = datetime(2026, 6, 26, 8, 49, tzinfo=kst)
    off_hours_time = datetime(2026, 6, 26, 8, 50, tzinfo=kst)
    premarket = price_tick_event(
        "evt_nxt_premarket",
        price=10000,
        volume=100,
        trade_value=1_000_000,
        ts=premarket_time,
        trade_time=premarket_time,
        exchange="NXT",
    )
    off_hours = price_tick_event(
        "evt_nxt_off_hours",
        price=10010,
        volume=110,
        trade_value=1_101_100,
        ts=off_hours_time,
        trade_time=off_hours_time,
        exchange="NXT",
    )

    append_and_project(
        connection,
        premarket,
        Settings(market_data_bar_intervals_sec=(180,)),
    )
    append_and_project(
        connection,
        off_hours,
        Settings(market_data_bar_intervals_sec=(180,)),
    )

    bars = list_bars(connection, "005930", exchange="NXT", interval_sec=180)
    latest = get_latest_tick(connection, "005930", exchange="NXT")
    connection.close()

    assert market_session_for_tick(premarket_time, "NXT") == "PREMARKET_NXT"
    assert market_session_for_tick(off_hours_time, "NXT") == "OFF_HOURS"
    assert len(bars) == 2
    assert {bar["session"] for bar in bars} == {"PREMARKET_NXT", "OFF_HOURS"}
    assert all(bar["tick_count"] == 1 for bar in bars)
    assert latest is not None
    assert latest["session"] == "OFF_HOURS"


def test_nxt_premarket_snapshot_uses_previous_krx_close_and_session_boundary(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    settings = Settings(market_data_premarket_snapshot_enabled=True)
    kst = timezone(timedelta(hours=9))
    previous_close_time = datetime(2026, 6, 25, 15, 19, tzinfo=kst)
    first_time = datetime(2026, 6, 26, 8, 0, tzinfo=kst)
    last_time = datetime(2026, 6, 26, 8, 49, tzinfo=kst)
    boundary_time = datetime(2026, 6, 26, 8, 50, tzinfo=kst)

    append_and_project(
        connection,
        price_tick_event(
            "evt_krx_previous_close",
            price=10_000,
            volume=100,
            trade_value=1_000_000,
            ts=previous_close_time,
            trade_time=previous_close_time,
        ),
        settings,
    )
    append_and_project(
        connection,
        price_tick_event(
            "evt_nxt_premarket_first",
            price=10_500,
            volume=10,
            trade_value=105_000,
            ts=first_time,
            trade_time=first_time,
            exchange="NXT",
        ),
        settings,
    )
    append_and_project(
        connection,
        price_tick_event(
            "evt_nxt_premarket_last",
            price=10_600,
            volume=30,
            trade_value=318_000,
            ts=last_time,
            trade_time=last_time,
            exchange="NXT",
        ),
        settings,
    )
    append_and_project(
        connection,
        price_tick_event(
            "evt_nxt_boundary_excluded",
            price=10_700,
            volume=40,
            trade_value=428_000,
            ts=boundary_time,
            trade_time=boundary_time,
            exchange="NXT",
        ),
        settings,
    )

    snapshots = list_premarket_snapshots(connection, "2026-06-26")
    connection.close()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot["trade_date"] == "2026-06-26"
    assert snapshot["code"] == "005930"
    assert snapshot["first_price"] == 10_500
    assert snapshot["last_price"] == 10_600
    assert snapshot["prev_krx_close"] == 10_000
    assert snapshot["premarket_gap_pct"] == 6.0
    assert snapshot["volume"] == 30
    assert snapshot["tick_count"] == 2
    assert snapshot["last_event_id"] == "evt_nxt_premarket_last"
    assert snapshot["metadata"]["premarket_observation_is_not_buy_signal"] is True


def test_premarket_snapshot_is_opt_in_and_gap_skips_without_previous_close(tmp_path) -> None:
    kst = timezone(timedelta(hours=9))
    premarket_time = datetime(2026, 6, 26, 8, 10, tzinfo=kst)
    disabled_connection = initialize_database(tmp_path / "disabled.sqlite3")
    append_and_project(
        disabled_connection,
        price_tick_event(
            "evt_nxt_disabled",
            price=10_500,
            volume=10,
            trade_value=105_000,
            ts=premarket_time,
            trade_time=premarket_time,
            exchange="NXT",
        ),
        Settings(),
    )
    disabled = list_premarket_snapshots(disabled_connection, "2026-06-26")
    disabled_connection.close()

    enabled_connection = initialize_database(tmp_path / "enabled.sqlite3")
    append_and_project(
        enabled_connection,
        price_tick_event(
            "evt_nxt_no_prev_close",
            price=10_500,
            volume=10,
            trade_value=105_000,
            ts=premarket_time,
            trade_time=premarket_time,
            exchange="NXT",
        ),
        Settings(market_data_premarket_snapshot_enabled=True),
    )
    enabled = list_premarket_snapshots(enabled_connection, "2026-06-26")
    enabled_connection.close()

    assert disabled == []
    assert len(enabled) == 1
    assert enabled[0]["prev_krx_close"] is None
    assert enabled[0]["premarket_gap_pct"] is None


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


def test_older_price_tick_appends_sample_without_rewinding_latest_or_bars(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    newer = price_tick_event(
        "evt_tick_newer",
        price=70100,
        volume=1010,
        trade_value=70_801_000,
        ts=TS + timedelta(seconds=10),
        trade_time=TS + timedelta(seconds=10),
    )
    older = price_tick_event(
        "evt_tick_older",
        price=69900,
        volume=1000,
        trade_value=69_900_000,
        ts=TS + timedelta(seconds=5),
        trade_time=TS + timedelta(seconds=5),
    )

    append_and_project(connection, newer)
    append_gateway_event(connection, older)
    result = process_gateway_event(connection, older, settings=Settings())

    latest = get_latest_tick(connection, "005930")
    sample_rows = connection.execute(
        """
        SELECT event_id, volume_delta, trade_value_delta
        FROM market_tick_samples
        ORDER BY event_ts
        """
    ).fetchall()
    bar = list_bars(connection, "005930", interval_sec=60)[0]
    projection_error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_projection_errors"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "APPLIED"
    assert latest is not None
    assert latest["event_id"] == "evt_tick_newer"
    assert latest["price"] == 70100
    assert [row["event_id"] for row in sample_rows] == [
        "evt_tick_older",
        "evt_tick_newer",
    ]
    assert sample_rows[0]["volume_delta"] == 0
    assert sample_rows[0]["trade_value_delta"] == 0
    assert bar["close"] == 70100
    assert bar["tick_count"] == 1
    assert projection_error_count == 0


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


def test_candidate_quote_refresh_tr_response_updates_latest_tick(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate-quote-refresh.sqlite3")
    response = BrokerTrResponse(
        request_id="candidate_quote_refresh:2026-06-26:005930:1",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70500",
                "등락율": "+1.25",
                "거래량": "1,234",
                "거래대금": "87,000,000",
                "고가": "+71000",
                "저가": "-69000",
            }
        ],
        ts=TS,
    )
    event = GatewayEvent(
        event_id="evt_candidate_quote_refresh",
        event_type="tr_response",
        source="test-gateway",
        command_id="cmd_candidate_quote_refresh",
        payload=response.to_dict(),
        ts=TS,
    )

    result = append_and_project(connection, event)

    latest = get_latest_tick(connection, "005930")
    snapshot_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots"
    ).fetchone()["count"]
    sample_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tick_samples"
    ).fetchone()["count"]
    sample = connection.execute(
        """
        SELECT event_id, metadata_json
        FROM market_tick_samples
        WHERE code = ?
        """,
        ("005930",),
    ).fetchone()
    projection_error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_projection_errors"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "APPLIED"
    assert latest is not None
    assert latest["price"] == 70_500
    assert latest["source"] == "test-gateway"
    assert snapshot_count == 1
    assert sample_count == 1
    assert sample is not None
    assert sample["event_id"] == "evt_candidate_quote_refresh:synthetic_price_tick:0:005930:KRX"
    metadata = json.loads(sample["metadata_json"])
    assert metadata["parent_event_id"] == event.event_id
    assert metadata["parent_command_id"] == event.command_id
    assert metadata["parent_tr_code"] == response.tr_code
    assert metadata["parent_request_name"] == response.request_name
    assert metadata["synthetic_event"] is True
    assert metadata["row_index"] == 0
    assert projection_error_count == 0


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


def test_projection_watermark_advances_on_live_ingest(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    tick = price_tick_event("evt_watermark_live", ts=TS, trade_time=TS)

    result = append_and_project(connection, tick, Settings())
    row = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (tick.event_id,),
    ).fetchone()
    watermark = get_market_data_projection_watermark(connection)
    status = get_market_data_status(connection, settings=Settings())
    connection.close()

    assert result.status == "APPLIED"
    assert watermark.last_event_rowid == row["event_rowid"]
    assert watermark.last_event_id == tick.event_id
    assert status["projection_watermark"]["last_event_id"] == tick.event_id


def test_incremental_rebuild_replays_after_projection_watermark_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market.sqlite3")
    first = price_tick_event(
        "evt_watermark_first",
        price=70000,
        volume=1000,
        trade_value=70_000_000,
        ts=TS,
        trade_time=TS,
    )
    second = price_tick_event(
        "evt_watermark_second",
        price=70100,
        volume=1010,
        trade_value=70_701_000,
        ts=TS + timedelta(seconds=5),
        trade_time=TS + timedelta(seconds=5),
    )
    append_and_project(connection, first, Settings())
    append_gateway_event(connection, second)
    first_row = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (first.event_id,),
    ).fetchone()
    second_row = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (second.event_id,),
    ).fetchone()

    result = rebuild_market_data_projection(
        connection,
        incremental=True,
        settings=Settings(),
    )
    repeat = rebuild_market_data_projection(
        connection,
        incremental=True,
        settings=Settings(),
    )
    samples = connection.execute(
        """
        SELECT event_id
        FROM market_tick_samples
        ORDER BY event_ts
        """
    ).fetchall()
    watermark = get_market_data_projection_watermark(connection)
    connection.close()

    assert result.mode == "incremental"
    assert result.from_event_rowid == first_row["event_rowid"]
    assert result.last_event_rowid == second_row["event_rowid"]
    assert result.processed_count == 1
    assert result.applied_count == 4
    assert repeat.processed_count == 0
    assert [row["event_id"] for row in samples] == [
        first.event_id,
        second.event_id,
    ]
    assert watermark.last_event_id == second.event_id
