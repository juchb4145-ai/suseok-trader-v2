from __future__ import annotations

from datetime import timedelta

import pytest
import services.candidate_service as candidate_service
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.candidate_service import (
    CandidateSourceApplyResult,
    get_candidate,
    ingest_condition_sources,
    ingest_theme_sources,
    list_candidates,
    rebuild_candidates_from_observations,
    refresh_candidate_context,
)
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database, open_connection


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


def test_condition_exit_context_failure_rolls_back_whole_source_event(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "candidate-exit-rollback.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    _append_and_project(connection, make_condition_event(action="EXIT"), settings)

    monkeypatch.setattr(
        candidate_service,
        "_upsert_candidate_context",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("candidate context write failed")
        ),
    )
    result = ingest_condition_sources(connection, trade_date, settings=settings)
    candidate = list_candidates(
        connection,
        trade_date=trade_date,
        active_only=False,
    )[0]
    exit_event_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_source_events
        WHERE action = 'EXIT'
        """
    ).fetchone()["count"]
    latest_active = connection.execute(
        "SELECT active FROM candidate_sources_latest"
    ).fetchone()["active"]
    projection_errors = connection.execute(
        """
        SELECT error_message
        FROM candidate_projection_errors
        ORDER BY id
        """
    ).fetchall()
    connection.close()

    assert result.source_event_count == 0
    assert result.error_count == 1
    assert candidate["state"] == CandidateState.HYDRATING.value
    assert exit_event_count == 0
    assert latest_active == 1
    assert [row["error_message"] for row in projection_errors] == [
        "CONDITION_EXIT_CONTEXT_REFRESH_FAILED"
    ]


def test_condition_ingest_uses_bounded_queries_for_both_fusion_and_sources(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate-condition-date-bound.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    statements: list[str] = []
    connection.set_trace_callback(statements.append)

    result = ingest_condition_sources(connection, trade_date, settings=settings)
    connection.set_trace_callback(None)
    connection.close()

    bounded_selects = [
        statement.upper()
        for statement in statements
        if "FROM MARKET_CONDITION_SIGNALS" in statement.upper()
        and "SELECT *" in statement.upper()
        and "ORDER BY EVENT_TS ASC" in statement.upper()
    ]
    assert result.source_event_count == 1
    assert len(bounded_selects) == 2
    assert all("EVENT_TS >=" in sql and "EVENT_TS <" in sql for sql in bounded_selects)


def test_condition_ingest_releases_writer_lock_at_small_chunk_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "candidate-condition-chunk.sqlite3"
    connection = initialize_database(db_path)
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    for index in range(11):
        _append_and_project(
            connection,
            make_condition_event(
                condition_id=f"condition-{index}",
                metadata={"chunk_index": index},
            ),
            settings,
        )

    call_count = 0
    competing_writer_acquired = False

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count, competing_writer_acquired
        call_count += 1
        if call_count == 11:
            competing = open_connection(db_path)
            competing.execute("PRAGMA busy_timeout=0")
            try:
                competing.execute("BEGIN IMMEDIATE")
                competing_writer_acquired = True
                competing.rollback()
            finally:
                competing.close()
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, ?, NULL, 'chunk-probe', '{}')
            """,
            (source_event.source_event_id,),
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        candidate_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )

    result = ingest_condition_sources(connection, trade_date, settings=settings)
    connection.close()

    assert call_count == 11
    assert result.source_event_count == 11
    assert competing_writer_acquired is True


def test_condition_ingest_fence_loss_rolls_back_current_chunk(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "candidate-condition-fence.sqlite3")
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    for index in range(11):
        _append_and_project(
            connection,
            make_condition_event(
                condition_id=f"fence-condition-{index}",
                metadata={"fence_index": index},
            ),
            settings,
        )
    call_count = 0

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count
        call_count += 1
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, ?, NULL, 'candidate-fence-chunk', '{}')
            """,
            (source_event.source_event_id,),
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        candidate_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )
    monkeypatch.setattr(
        candidate_service,
        "assert_runtime_execution_fence",
        lambda connection: (_ for _ in ()).throw(
            RuntimeError("EVALUATION_RUN_FENCE_LOST")
        ),
    )

    with pytest.raises(RuntimeError, match="EVALUATION_RUN_FENCE_LOST"):
        ingest_condition_sources(connection, trade_date, settings=settings)
    durable_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_projection_errors
        WHERE error_message = 'candidate-fence-chunk'
        """
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    assert call_count == 10
    assert durable_count == 0
    assert in_transaction is False


def test_condition_ingest_rolls_back_partial_source_event_and_leaked_transaction(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "candidate-condition-event-rollback.sqlite3"
    connection = initialize_database(db_path)
    settings = _candidate_settings()
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)

    def partially_write_then_fail(actual_connection, source_event, *, settings):
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, ?, NULL, 'partial-source-write', '{}')
            """,
            (source_event.source_event_id,),
        )
        raise RuntimeError("source apply failed")

    monkeypatch.setattr(
        candidate_service,
        "create_or_merge_candidate_from_source",
        partially_write_then_fail,
    )
    result = ingest_condition_sources(connection, trade_date, settings=settings)
    partial_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_projection_errors
        WHERE error_message = 'partial-source-write'
        """
    ).fetchone()["count"]

    def fusion_write_then_fail(actual_connection, *args, **kwargs):
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, NULL, NULL, 'leaked-fusion-write', '{}')
            """
        )
        raise RuntimeError("fusion failed")

    monkeypatch.setattr(
        candidate_service,
        "rebuild_condition_fusion",
        fusion_write_then_fail,
    )
    with pytest.raises(RuntimeError, match="fusion failed"):
        ingest_condition_sources(connection, trade_date, settings=settings)
    leaked_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_projection_errors
        WHERE error_message = 'leaked-fusion-write'
        """
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    competing = open_connection(db_path)
    try:
        competing.execute("BEGIN IMMEDIATE")
        competing.rollback()
    finally:
        competing.close()

    assert result.error_count == 1
    assert partial_count == 0
    assert leaked_count == 0
    assert in_transaction is False


def test_theme_ingest_releases_writer_lock_at_small_chunk_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "candidate-theme-chunk.sqlite3"
    connection = initialize_database(db_path)
    settings = _candidate_settings(
        candidate_theme_source_states=("DATA_WAIT",),
        candidate_theme_member_roles=("UNKNOWN",),
    )
    trade_date = _trade_date(settings)
    import_theme_memberships(
        connection,
        _theme_payload(
            [
                {"code": f"{100000 + index:06d}", "name": f"member-{index}"}
                for index in range(11)
            ]
        ),
    )
    calculate_theme_snapshot(connection, "semiconductor", settings=settings)
    call_count = 0
    competing_writer_acquired = False

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count, competing_writer_acquired
        call_count += 1
        if call_count == 11:
            competing = open_connection(db_path)
            competing.execute("PRAGMA busy_timeout=0")
            try:
                competing.execute("BEGIN IMMEDIATE")
                competing_writer_acquired = True
                competing.rollback()
            finally:
                competing.close()
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, ?, NULL, 'theme-chunk-probe', '{}')
            """,
            (source_event.source_event_id,),
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        candidate_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )

    result = ingest_theme_sources(connection, trade_date, settings=settings)
    connection.close()

    assert call_count == 11
    assert result.source_event_count == 11
    assert competing_writer_acquired is True


def test_theme_ingest_fence_loss_rolls_back_current_chunk(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "candidate-theme-fence.sqlite3")
    settings = _candidate_settings(
        candidate_theme_source_states=("DATA_WAIT",),
        candidate_theme_member_roles=("UNKNOWN",),
    )
    trade_date = _trade_date(settings)
    import_theme_memberships(
        connection,
        _theme_payload(
            [
                {"code": f"{100000 + index:06d}", "name": f"member-{index}"}
                for index in range(11)
            ]
        ),
    )
    calculate_theme_snapshot(connection, "semiconductor", settings=settings)
    call_count = 0

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count
        call_count += 1
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, ?, NULL, 'candidate-theme-fence-chunk', '{}')
            """,
            (source_event.source_event_id,),
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        candidate_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )
    monkeypatch.setattr(
        candidate_service,
        "assert_runtime_execution_fence",
        lambda connection: (_ for _ in ()).throw(
            RuntimeError("EVALUATION_RUN_FENCE_LOST")
        ),
    )

    with pytest.raises(RuntimeError, match="EVALUATION_RUN_FENCE_LOST"):
        ingest_theme_sources(connection, trade_date, settings=settings)
    durable_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_projection_errors
        WHERE error_message = 'candidate-theme-fence-chunk'
        """
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    assert call_count == 10
    assert durable_count == 0
    assert in_transaction is False


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
    settings = _candidate_settings(theme_min_observable_members=2)
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


def test_candidate_rebuild_uses_cycle_freshness_reference(tmp_path, monkeypatch) -> None:
    connection = initialize_database(tmp_path / "candidate_cycle_freshness.sqlite3")
    settings = _candidate_settings(
        market_data_tick_stale_sec=10,
        market_data_degraded_tick_stale_sec=30,
        candidate_tick_stale_sec=90,
    )
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    _append_and_project(connection, make_price_tick_event(), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    freshness_reference_at = utc_now()
    monkeypatch.setattr(
        "domain.market.quality.utc_now",
        lambda: freshness_reference_at + timedelta(seconds=20),
    )

    result = rebuild_candidates_from_observations(
        connection,
        trade_date,
        settings=settings,
        freshness_reference_at=freshness_reference_at,
    )
    candidate = list_candidates(connection, trade_date=trade_date)[0]
    connection.close()

    assert result.context_refreshed_count == 2
    assert candidate["state"] == CandidateState.CONTEXT_READY.value
    assert candidate["market_readiness_status"] == "FRESH"
    assert float(candidate["tick_age_sec"]) < settings.market_data_tick_stale_sec


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


def test_candidate_tick_between_30_and_90_seconds_is_not_stale(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_60s_tick.sqlite3")
    settings = _candidate_settings(candidate_tick_stale_sec=90)
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    _append_and_project(connection, _old_price_tick_event(), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]

    refresh = refresh_candidate_context(connection, candidate_id, settings=settings)
    candidate = get_candidate(connection, candidate_id)
    connection.close()

    assert refresh.stale_count == 0
    assert candidate is not None
    assert candidate["state"] != CandidateState.STALE.value
    assert "TICK_STALE" not in candidate["reason_codes"]


def test_source_stale_without_tick_stale_does_not_mark_candidate_stale(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_source_stale_info.sqlite3")
    settings = _candidate_settings(
        candidate_source_stale_sec=1,
        candidate_tick_stale_sec=90,
    )
    trade_date = _trade_date(settings)
    _append_and_project(connection, make_condition_event(action="ENTER"), settings)
    _append_and_project(connection, _old_price_tick_event(), settings)
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]
    old_seen_at = datetime_to_wire(utc_now() - timedelta(seconds=10))
    connection.execute(
        "UPDATE candidate_sources_latest SET last_seen_at = ? WHERE candidate_instance_id = ?",
        (old_seen_at, candidate_id),
    )
    connection.commit()

    refresh = refresh_candidate_context(connection, candidate_id, settings=settings)
    candidate = get_candidate(connection, candidate_id)
    connection.close()

    assert refresh.stale_count == 0
    assert candidate is not None
    assert candidate["state"] != CandidateState.STALE.value
    assert "SOURCE_STALE" in candidate["reason_codes"]
    assert "TICK_STALE" not in candidate["reason_codes"]


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
