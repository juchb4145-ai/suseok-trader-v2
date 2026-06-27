from __future__ import annotations

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import utc_now
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.theme_service import (
    calculate_all_theme_snapshots,
    calculate_theme_snapshot,
    import_theme_memberships,
    list_theme_members,
    list_theme_snapshot_members,
    list_themes,
    list_themes_for_code,
    upsert_theme,
    upsert_theme_member,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_theme_membership_import_upsert_lists_and_replace_scope(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme.sqlite3")
    payload = _theme_payload(
        [
            {"code": "005930", "name": "삼성전자"},
            {"code": "000660", "name": "SK하이닉스"},
        ]
    )

    first = import_theme_memberships(connection, payload)
    duplicate = import_theme_memberships(connection, payload)
    append_payload = _theme_payload(
        [
            {"code": "005930", "name": "삼성전자"},
        ]
    )
    import_theme_memberships(connection, append_payload, replace=False)
    members_after_append = list_theme_members(connection, "semiconductor")
    import_theme_memberships(connection, append_payload, replace=True)
    members_after_replace = list_theme_members(connection, "semiconductor")
    themes = list_themes(connection)
    themes_for_code = list_themes_for_code(connection, "A005930")
    batch_count = connection.execute(
        "SELECT COUNT(*) AS count FROM theme_import_batches WHERE status = 'SUCCESS'"
    ).fetchone()["count"]
    connection.close()

    assert first.theme_count == 1
    assert first.member_count == 2
    assert duplicate.status == "SUCCESS"
    assert len(themes) == 1
    assert themes[0]["source_type"] == "MOCK"
    assert len(themes_for_code) == 1
    assert len(members_after_append) == 2
    assert sum(1 for member in members_after_append if member["active"]) == 2
    assert len(members_after_replace) == 2
    assert sum(1 for member in members_after_replace if member["active"]) == 1
    assert batch_count == 4


def test_theme_direct_upserts_validate_codes_and_active_lists(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme.sqlite3")
    upsert_theme(
        connection,
        theme_id="manual_theme",
        theme_name="수동테스트",
        source_type="MANUAL",
        source_name="unit",
    )
    upsert_theme_member(
        connection,
        theme_id="manual_theme",
        theme_name="수동테스트",
        code="A005930",
        name="삼성전자",
        source_type="MANUAL",
        source_name="unit",
    )
    connection.commit()

    try:
        upsert_theme_member(
            connection,
            theme_id="manual_theme",
            theme_name="수동테스트",
            code="BAD",
            name="bad",
            source_type="MANUAL",
        )
    except ValueError as exc:
        assert "6-digit domestic stock code" in str(exc)
    else:
        raise AssertionError("expected invalid code to fail")

    members = list_theme_members(connection, "manual_theme")
    connection.close()

    assert members[0]["code"] == "005930"
    assert members[0]["active"] is True


def test_theme_snapshot_calculates_leader_roles_state_and_persistence(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme.sqlite3")
    settings = _fresh_settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            [
                {"code": "005930", "name": "삼성전자"},
                {"code": "000660", "name": "SK하이닉스"},
                {"code": "035420", "name": "NAVER"},
            ]
        ),
    )
    _append_and_project(
        connection,
        _price_tick_event(
            "evt_tick_005930",
            code="005930",
            name="삼성전자",
            price=70000,
            change_rate=1.5,
            volume=1000,
            trade_value=70_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        _price_tick_event(
            "evt_tick_000660",
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=0.9,
            volume=750,
            trade_value=90_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        _price_tick_event(
            "evt_tick_035420",
            code="035420",
            name="NAVER",
            price=220000,
            change_rate=0.2,
            volume=45,
            trade_value=10_000_000,
        ),
        settings,
    )
    _append_and_project(connection, _condition_event("evt_condition_005930"), settings)

    snapshot = calculate_theme_snapshot(connection, "semiconductor", settings=settings)
    latest_row = connection.execute("SELECT * FROM theme_latest_snapshots").fetchone()
    member_rows = list_theme_snapshot_members(connection, snapshot.snapshot_id)
    connection.close()

    assert snapshot.observed_member_count == 3
    assert snapshot.fresh_member_count == 3
    assert snapshot.rising_ratio == 1.0
    assert snapshot.total_trade_value == 170_000_000
    assert snapshot.trade_value_delta_1m == 170_000_000
    assert snapshot.trade_value_delta_3m == 170_000_000
    assert snapshot.trade_value_delta_5m == 170_000_000
    assert snapshot.leading_code == "005930"
    assert snapshot.co_leader_codes == ["000660"]
    assert snapshot.follower_codes == ["035420"]
    assert snapshot.state == "LEADING"
    assert snapshot.quality_status == "FRESH"
    assert latest_row["snapshot_id"] == snapshot.snapshot_id
    assert len(member_rows) == 3
    assert member_rows[0]["member_role"] == "LEADER_CANDIDATE"
    assert member_rows[0]["above_vwap"] is True
    assert member_rows[0]["readiness_status"] == "FRESH"
    assert member_rows[0]["metadata"]["condition_latest"][0]["action"] == "ENTER"


def test_theme_snapshot_state_rules_cover_wait_watch_spreading_and_low_coverage(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "theme.sqlite3")
    settings = _fresh_settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            [
                {"code": "005930", "name": "삼성전자"},
                {"code": "000660", "name": "SK하이닉스"},
                {"code": "035420", "name": "NAVER"},
            ]
        ),
    )
    no_observed = calculate_theme_snapshot(
        connection,
        "semiconductor",
        calculated_at="2026-06-26T00:00:00Z",
        settings=settings,
    )

    _append_and_project(
        connection,
        _price_tick_event(
            "evt_watch_005930",
            code="005930",
            name="삼성전자",
            price=70000,
            change_rate=-0.2,
            volume=100,
            trade_value=7_000_000,
        ),
        settings,
    )
    low_coverage = calculate_theme_snapshot(
        connection,
        "semiconductor",
        calculated_at="2026-06-26T00:01:00Z",
        settings=Settings(
            market_data_tick_stale_sec=999_999_999,
            market_data_degraded_tick_stale_sec=999_999_999,
            theme_min_fresh_coverage_ratio=0.8,
        ),
    )
    watch = calculate_theme_snapshot(
        connection,
        "semiconductor",
        calculated_at="2026-06-26T00:02:00Z",
        settings=settings,
    )
    _append_and_project(
        connection,
        _price_tick_event(
            "evt_spreading_000660",
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=0.3,
            volume=100,
            trade_value=12_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        _price_tick_event(
            "evt_spreading_035420",
            code="035420",
            name="NAVER",
            price=220000,
            change_rate=0.4,
            volume=100,
            trade_value=22_000_000,
        ),
        settings,
    )
    spreading = calculate_theme_snapshot(
        connection,
        "semiconductor",
        calculated_at="2026-06-26T00:03:00Z",
        settings=Settings(
            market_data_tick_stale_sec=999_999_999,
            market_data_degraded_tick_stale_sec=999_999_999,
            theme_leading_rising_ratio=0.9,
            theme_spreading_rising_ratio=0.35,
        ),
    )
    result = calculate_all_theme_snapshots(
        connection,
        calculated_at="2026-06-26T00:04:00Z",
        settings=settings,
    )
    connection.close()

    assert no_observed.state == "DATA_WAIT"
    assert no_observed.quality_status == "DATA_WAIT"
    assert "NO_OBSERVED_MEMBERS" in no_observed.reason_codes
    assert low_coverage.state == "DATA_WAIT"
    assert low_coverage.quality_status == "PARTIAL"
    assert "LOW_FRESH_COVERAGE" in low_coverage.reason_codes
    assert watch.state == "WATCH"
    assert spreading.state == "SPREADING"
    assert result.processed_theme_count == 1
    assert result.snapshot_count == 1


def _theme_payload(members: list[dict[str, str]]) -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "unit_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": members,
            }
        ],
    }


def _fresh_settings() -> Settings:
    return Settings(
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
    )


def _price_tick_event(
    event_id: str,
    *,
    code: str,
    name: str,
    price: int,
    change_rate: float,
    volume: int,
    trade_value: int,
) -> GatewayEvent:
    now = utc_now()
    tick = BrokerPriceTick(
        code=code,
        name=name,
        price=price,
        change_rate=change_rate,
        volume=volume,
        trade_value=trade_value,
        execution_strength=100.0,
        best_bid=max(price - 100, 1),
        best_ask=price,
        spread_ticks=1,
        day_high=price + 1000,
        day_low=max(price - 1000, 1),
        trade_time=now,
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=now,
    )


def _condition_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    condition = BrokerConditionEvent(
        condition_id="cond1",
        condition_name="Breakout",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=70000,
        metadata={"rank": 1},
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="condition_event",
        source="test-gateway",
        payload=condition.to_dict(),
        ts=now,
    )


def _append_and_project(
    connection,
    event: GatewayEvent,
    settings: Settings,
) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"


def test_invalid_theme_import_records_error_batch(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme.sqlite3")
    payload = _theme_payload([{"code": "bad", "name": "bad"}])

    try:
        import_theme_memberships(connection, payload)
    except ValueError:
        pass
    else:
        raise AssertionError("expected invalid import to fail")

    row = connection.execute(
        "SELECT status, error_message FROM theme_import_batches ORDER BY imported_at DESC LIMIT 1"
    ).fetchone()
    connection.close()

    assert row["status"] == "ERROR"
    assert "6-digit domestic stock code" in row["error_message"]
