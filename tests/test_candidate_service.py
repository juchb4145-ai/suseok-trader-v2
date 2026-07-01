from __future__ import annotations

from datetime import timedelta

from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import utc_now
from domain.candidate.state import CandidateState
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.candidate_service import (
    get_candidate,
    ingest_condition_sources,
    ingest_theme_sources,
    list_candidates,
    refresh_candidate_context,
)
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_condition_source_creates_candidate_and_exit_closes_episode(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)

    result = ingest_condition_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date, active_only=False)
    candidate = candidates[0]
    sources = connection.execute("SELECT * FROM candidate_sources_latest").fetchall()

    assert result.source_event_count == 1
    assert result.candidate_created_count == 1
    assert candidate["candidate_instance_id"] == f"CAND-{trade_date}-005930-1"
    assert candidate["state"] == CandidateState.HYDRATING.value
    assert candidate["primary_source_type"] == "CONDITION_ENTER"
    assert "CONDITION_ENTERED" in candidate["reason_codes"]
    assert len(sources) == 1
    assert sources[0]["active"] == 1

    duplicate = ingest_condition_sources(connection, trade_date, settings=settings)
    _append_and_project(connection, make_condition_event(action="EXIT"), settings)
    exit_result = ingest_condition_sources(connection, trade_date, settings=settings)
    closed = get_candidate(
        connection,
        candidate["candidate_instance_id"],
        include_sources=True,
        include_transitions=True,
    )
    connection.close()

    assert duplicate.duplicate_source_count == 1
    assert exit_result.source_event_count == 1
    assert closed is not None
    assert closed["state"] == CandidateState.CLOSED.value
    assert closed["active_source_count"] == 0
    assert any(source["action"] == "EXIT" for source in closed["sources"])
    assert closed["transitions"][-1]["to_state"] == CandidateState.CLOSED.value


def test_closed_candidate_re_detection_increments_generation(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_generation.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    _append_and_project(connection, make_condition_event(action="EXIT"), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)

    _append_and_project(
        connection,
        make_condition_event(action="ENTER", metadata={"second": True}),
        settings,
    )
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date, active_only=False)
    active_candidates = list_candidates(connection, trade_date=trade_date, active_only=True)
    connection.close()

    assert [candidate["generation"] for candidate in candidates] == [2, 1]
    assert len(active_candidates) == 1
    assert active_candidates[0]["candidate_instance_id"] == f"CAND-{trade_date}-005930-2"


def test_theme_sources_create_leader_co_leader_and_follower_candidates(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_theme.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
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
        make_price_tick_event(
            code="005930",
            name="삼성전자",
            change_rate=1.5,
            trade_value=100_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=1.1,
            volume=900,
            trade_value=108_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="035420",
            name="NAVER",
            price=220000,
            change_rate=0.2,
            volume=100,
            trade_value=22_000_000,
        ),
        settings,
    )
    snapshot = calculate_theme_snapshot(connection, "semiconductor", settings=settings)

    result = ingest_theme_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date, active_only=True)
    source_types = {
        row["source_type"]
        for row in connection.execute("SELECT source_type FROM candidate_sources_latest")
    }
    connection.close()

    assert snapshot.state.value == "LEADING"
    assert result.candidate_created_count == 3
    assert len(candidates) == 3
    assert source_types == {"THEME_LEADER", "THEME_CO_LEADER", "THEME_FOLLOWER"}
    assert all(candidate["theme_id"] == "semiconductor" for candidate in candidates)


def test_theme_sources_skip_non_source_theme_states(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_theme_watch.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    import_theme_memberships(
        connection,
        _theme_payload(
            [
                {"code": "005930", "name": "삼성전자"},
                {"code": "000660", "name": "SK하이닉스"},
            ]
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="005930", name="삼성전자", change_rate=-0.5),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="000660", name="SK하이닉스", change_rate=-0.2),
        settings,
    )
    snapshot = calculate_theme_snapshot(connection, "semiconductor", settings=settings)

    result = ingest_theme_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date, active_only=False)
    connection.close()

    assert snapshot.state.value == "WATCH"
    assert result.source_event_count == 0
    assert candidates == []


def test_candidate_context_refresh_transitions_through_data_wait_watching_and_ready(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "candidate_context.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]

    missing = refresh_candidate_context(connection, candidate_id, settings=settings)
    data_wait = get_candidate(connection, candidate_id, include_context=True)
    _append_and_project(connection, make_price_tick_event(), settings)
    watching = refresh_candidate_context(connection, candidate_id, settings=settings)
    watching_candidate = get_candidate(connection, candidate_id, include_context=True)
    ready = refresh_candidate_context(connection, candidate_id, settings=settings)
    ready_candidate = get_candidate(
        connection,
        candidate_id,
        include_context=True,
        include_transitions=True,
    )
    repeated = refresh_candidate_context(connection, candidate_id, settings=settings)
    connection.close()

    assert missing.transition_count == 1
    assert data_wait is not None
    assert data_wait["state"] == CandidateState.DATA_WAIT.value
    assert "MARKET_READINESS_MISSING" in data_wait["reason_codes"]
    assert watching.transition_count == 1
    assert watching_candidate is not None
    assert watching_candidate["state"] == CandidateState.WATCHING.value
    assert ready.transition_count == 1
    assert ready_candidate is not None
    assert ready_candidate["state"] == CandidateState.CONTEXT_READY.value
    assert ready_candidate["context"]["readiness"]["has_1m_bar"] is True
    assert ready_candidate["context"]["readiness"]["vwap_ready"] is True
    assert ready_candidate["context"]["market_context"]["market_regime"]["regime_status"] == (
        "DATA_WAIT"
    )
    assert ready_candidate["context"]["market_context"]["market_regime"]["primary_index_code"] == (
        "UNKNOWN"
    )
    assert repeated.transition_count == 0
    assert [row["to_state"] for row in ready_candidate["transitions"]] == [
        CandidateState.DETECTED.value,
        CandidateState.HYDRATING.value,
        CandidateState.DATA_WAIT.value,
        CandidateState.WATCHING.value,
        CandidateState.CONTEXT_READY.value,
    ]


def test_condition_candidate_context_uses_latest_theme_snapshot_fallback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_condition_theme_fallback.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
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
        make_price_tick_event(
            code="005930",
            name="삼성전자",
            change_rate=1.5,
            trade_value=100_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=1.1,
            volume=900,
            trade_value=108_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="035420",
            name="NAVER",
            price=220000,
            change_rate=0.2,
            volume=100,
            trade_value=22_000_000,
        ),
        settings,
    )
    calculate_theme_snapshot(connection, "semiconductor", settings=settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]

    refresh_candidate_context(connection, candidate_id, settings=settings)
    refreshed = get_candidate(connection, candidate_id, include_context=True)
    connection.close()

    assert refreshed is not None
    assert refreshed["theme_id"] == "semiconductor"
    assert refreshed["theme_name"] == "반도체"
    assert refreshed["theme_state"] == "LEADING"
    assert refreshed["theme_role"] == "LEADER_CANDIDATE"
    assert refreshed["context"]["theme_context"]["present"] is True
    assert refreshed["context"]["theme_context"]["theme_id"] == "semiconductor"


def test_candidate_refresh_marks_stale_tick_without_order_like_side_effects(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_stale.sqlite3")
    settings = _candidate_settings(candidate_tick_stale_sec=1)
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    old_tick = _old_price_tick_event()
    _append_and_project(connection, old_tick, settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]

    refresh = refresh_candidate_context(connection, candidate_id, settings=settings)
    candidate = get_candidate(connection, candidate_id)
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert refresh.stale_count == 1
    assert candidate is not None
    assert candidate["state"] == CandidateState.STALE.value
    assert "TICK_STALE" in candidate["reason_codes"]
    assert command_count == 0


def _candidate_settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _trade_date(settings: Settings) -> str:
    return (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _theme_payload(members: list[dict[str, str]]) -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "candidate_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": members,
            }
        ],
    }


def _old_price_tick_event() -> GatewayEvent:
    old_ts = utc_now() - timedelta(seconds=60)
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=70000,
        change_rate=0.1,
        volume=1000,
        trade_value=70_000_000,
        execution_strength=100.0,
        best_bid=69900,
        best_ask=70000,
        spread_ticks=1,
        day_high=70500,
        day_low=69500,
        trade_time=old_ts,
        ts=old_ts,
    )
    return GatewayEvent(
        event_id="evt_old_tick",
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=old_ts,
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
