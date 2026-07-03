from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from services.theme_service import import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database
from tools.replay_observe_pipeline import replay_observe_pipeline

KST = timezone(timedelta(hours=9), name="Asia/Seoul")
TRADE_DATE = "2026-06-27"


def test_replay_observe_pipeline_writes_summary_without_forbidden_table_changes(
    tmp_path,
    monkeypatch,
) -> None:
    operational_db_path = tmp_path / "operational.sqlite3"
    replay_db_path = tmp_path / "replay.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(operational_db_path))
    monkeypatch.setenv("THEME_MIN_OBSERVABLE_MEMBERS", "2")
    initialize_database(operational_db_path).close()
    connection = initialize_database(replay_db_path)
    try:
        import_theme_memberships(connection, _theme_payload())
        for event in _replay_events():
            result = append_gateway_event(connection, event)
            assert result.status == "ACCEPTED"
    finally:
        connection.close()

    result = replay_observe_pipeline(
        trade_date=TRADE_DATE,
        db_path=replay_db_path,
        speed=0,
        report_root=tmp_path / "reports",
    )
    markdown = result.report_path.read_text(encoding="utf-8")

    assert result.processed_event_count == len(_replay_events())
    assert result.pipeline_run_count == len(_replay_events())
    assert result.matched_observation_count >= 1
    assert result.matched_by_setup
    assert result.virtual_entry_count >= 1
    assert result.no_forbidden_writes is True
    assert all(delta == 0 for delta in result.forbidden_table_delta.values())
    assert result.report_path == tmp_path / "reports" / TRADE_DATE / "summary.md"
    assert "MATCHED_OBSERVATION" in markdown
    assert "OBSERVE_PASS Conversion" in markdown
    assert "Virtual Entry Return Distribution" in markdown
    assert "Forbidden Table Delta" in markdown


def test_replay_rejects_configured_operational_db_path(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "operational.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    initialize_database(db_path).close()

    try:
        replay_observe_pipeline(trade_date=TRADE_DATE, db_path=db_path, speed=0)
    except ValueError as exc:
        assert "operational DB" in str(exc)
    else:
        raise AssertionError("expected operational DB path guard to reject the path")


def _replay_events() -> list[GatewayEvent]:
    base = datetime(2026, 6, 27, 9, 0, tzinfo=KST)
    return [
        _price_tick(
            "evt_tick_005930_1",
            base,
            code="005930",
            name="삼성전자",
            price=97_000,
            volume=7_000,
            trade_value=679_000_000,
            change_rate=2.0,
        ),
        _price_tick(
            "evt_tick_000660_1",
            base + timedelta(seconds=1),
            code="000660",
            name="SK하이닉스",
            price=120_000,
            volume=3_000,
            trade_value=360_000_000,
            change_rate=1.0,
            day_high=122_000,
            day_low=118_000,
        ),
        _condition_event(base + timedelta(seconds=2)),
        _price_tick(
            "evt_tick_005930_fill",
            base + timedelta(seconds=3),
            code="005930",
            name="삼성전자",
            price=96_000,
            volume=7_500,
            trade_value=720_000_000,
            change_rate=1.8,
        ),
        _price_tick(
            "evt_tick_005930_5m",
            base + timedelta(minutes=5, seconds=3),
            code="005930",
            name="삼성전자",
            price=97_000,
            volume=8_000,
            trade_value=776_000_000,
            change_rate=2.2,
        ),
        _price_tick(
            "evt_tick_005930_15m",
            base + timedelta(minutes=15, seconds=3),
            code="005930",
            name="삼성전자",
            price=98_000,
            volume=9_000,
            trade_value=882_000_000,
            change_rate=2.5,
        ),
        _price_tick(
            "evt_tick_005930_30m",
            base + timedelta(minutes=30, seconds=3),
            code="005930",
            name="삼성전자",
            price=99_000,
            volume=10_000,
            trade_value=990_000_000,
            change_rate=2.8,
        ),
    ]


def _price_tick(
    event_id: str,
    ts: datetime,
    *,
    code: str,
    name: str,
    price: int,
    volume: int,
    trade_value: int,
    change_rate: float,
    day_high: int = 100_000,
    day_low: int = 94_000,
) -> GatewayEvent:
    tick = BrokerPriceTick(
        code=code,
        name=name,
        price=price,
        change_rate=change_rate,
        volume=volume,
        trade_value=trade_value,
        execution_strength=125.0,
        best_bid=price - 100,
        best_ask=price,
        spread_ticks=1,
        day_high=day_high,
        day_low=day_low,
        trade_time=ts,
        ts=ts,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts.astimezone(UTC),
    )


def _condition_event(ts: datetime) -> GatewayEvent:
    condition = BrokerConditionEvent(
        condition_id="leader_pullback",
        condition_name="LeaderPullback",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=97_000,
        metadata={
            "sensor_evidence": True,
            "condition_profile_id": "profile-leader-pullback",
            "condition_role": "LEADER",
            "condition_profile": {
                "profile_id": "profile-leader-pullback",
                "condition_name": "LeaderPullback",
                "role": "LEADER",
                "priority": 100,
                "ttl_sec": 999_999_999,
                "enabled": True,
                "price_subscribe_policy": "immediate",
            },
            "condition_admission": {
                "subscribed": True,
                "reason_codes": ["TEST_REPLAY"],
            },
        },
        ts=ts,
    )
    return GatewayEvent(
        event_id="evt_condition_005930",
        event_type="condition_event",
        source="test-gateway",
        payload=condition.to_dict(),
        ts=ts.astimezone(UTC),
    )


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "replay_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }
