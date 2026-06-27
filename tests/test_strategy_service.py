from __future__ import annotations

import json

from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from domain.strategy.status import StrategyObservationStatus
from services.config import Settings
from services.strategy_engine import (
    evaluate_candidate_strategy,
    evaluate_candidates,
    get_latest_strategy_observation,
    load_strategy_candidate_context,
    save_strategy_observation,
)
from storage.sqlite import initialize_database


def test_load_strategy_candidate_context_reads_candidate_market_and_theme(tmp_path) -> None:
    connection = initialize_database(tmp_path / "strategy_context.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)

    context = load_strategy_candidate_context(connection, candidate_id, settings)
    connection.close()

    assert context.candidate_instance_id == candidate_id
    assert context.candidate_state == CandidateState.CONTEXT_READY.value
    assert context.theme_state == "LEADING"
    assert context.theme_role == "LEADER_CANDIDATE"
    assert context.price == 97_000
    assert context.day_high == 100_000
    assert context.vwap == 96_500
    assert context.bar_1m_ready is True
    assert context.raw_context["context_hash"]


def test_evaluate_candidate_strategy_saves_latest_and_setups(tmp_path) -> None:
    connection = initialize_database(tmp_path / "strategy_persistence.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)

    observation = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, observation)
    latest = get_latest_strategy_observation(connection, candidate_id, include_setups=True)
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert observation.overall_status is StrategyObservationStatus.MATCHED_OBSERVATION
    assert observation.observe_only is True
    assert latest is not None
    assert latest["strategy_observation_id"] == observation.strategy_observation_id
    assert latest["observe_only"] is True
    assert latest["config_version"] == settings.strategy_config_version
    assert latest["setup_observations"]
    assert any(
        setup["status"] == StrategyObservationStatus.MATCHED_OBSERVATION.value
        for setup in latest["setup_observations"]
    )
    assert command_count == 0


def test_candidate_data_wait_and_stale_context_statuses(tmp_path) -> None:
    connection = initialize_database(tmp_path / "strategy_states.sqlite3")
    settings = _settings(strategy_engine_stale_tick_sec=5)
    data_wait_id = _insert_strategy_fixture(
        connection,
        candidate_id="CAND-2026-06-27-005930-2",
        state=CandidateState.DATA_WAIT.value,
    )
    stale_id = _insert_strategy_fixture(
        connection,
        candidate_id="CAND-2026-06-27-000660-1",
        code="000660",
        name="SK하이닉스",
        state=CandidateState.STALE.value,
    )

    data_wait = evaluate_candidate_strategy(connection, data_wait_id, settings=settings)
    stale = evaluate_candidate_strategy(connection, stale_id, settings=settings)
    connection.close()

    assert data_wait.overall_status is StrategyObservationStatus.DATA_WAIT
    assert stale.overall_status is StrategyObservationStatus.STALE_CONTEXT


def test_batch_evaluation_records_run_counts_and_candidate_errors(tmp_path) -> None:
    connection = initialize_database(tmp_path / "strategy_batch.sqlite3")
    settings = _settings()
    _insert_strategy_fixture(connection, candidate_id="CAND-2026-06-27-005930-1")
    _insert_strategy_fixture(
        connection,
        candidate_id="CAND-2026-06-27-000660-1",
        code="000660",
        name="SK하이닉스",
        reason_codes_json="not-json",
    )

    result = evaluate_candidates(connection, trade_date="2026-06-27", settings=settings)
    latest_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_observations_latest"
    ).fetchone()["count"]
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_evaluation_errors"
    ).fetchone()["count"]
    run = connection.execute("SELECT * FROM strategy_evaluation_runs").fetchone()
    connection.close()

    assert result.candidate_count == 2
    assert result.evaluated_count == 1
    assert result.error_count == 1
    assert result.status == "COMPLETED_WITH_ERRORS"
    assert latest_count == 1
    assert error_count == 1
    assert run["candidate_count"] == 2
    assert run["matched_observation_count"] == 1


def _settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _insert_strategy_fixture(
    connection,
    *,
    candidate_id: str = "CAND-2026-06-27-005930-1",
    trade_date: str = "2026-06-27",
    code: str = "005930",
    name: str = "삼성전자",
    state: str = CandidateState.CONTEXT_READY.value,
    reason_codes_json: str | None = None,
) -> str:
    now = datetime_to_wire(utc_now())
    theme_id = f"theme-{code}"
    snapshot_id = f"snapshot-{code}"
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            previous_state,
            detected_at,
            last_seen_at,
            state_updated_at,
            closed_at,
            primary_source_type,
            primary_source_id,
            source_count,
            active_source_count,
            theme_id,
            theme_name,
            theme_state,
            theme_role,
            market_readiness_status,
            tick_age_sec,
            vwap_ready,
            bar_1m_ready,
            bar_3m_ready,
            bar_5m_ready,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, ?, NULL, ?, ?, 1, 1, ?, ?, ?, ?, ?,
            1.0, 1, 1, 1, 1, ?, '{}')
        """,
        (
            candidate_id,
            trade_date,
            code,
            name,
            state,
            now,
            now,
            now,
            "THEME_LEADER",
            theme_id,
            theme_id,
            "반도체",
            "LEADING",
            "LEADER_CANDIDATE",
            "FRESH",
            reason_codes_json if reason_codes_json is not None else "[]",
        ),
    )
    connection.execute(
        """
        INSERT INTO candidate_context_latest (
            candidate_instance_id,
            trade_date,
            code,
            name,
            theme_context_json,
            market_context_json,
            source_context_json,
            readiness_json,
            refreshed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            trade_date,
            code,
            name,
            json.dumps(
                {
                    "present": True,
                    "theme_id": theme_id,
                    "theme_name": "반도체",
                    "theme_state": "LEADING",
                    "theme_role": "LEADER_CANDIDATE",
                },
                ensure_ascii=False,
            ),
            json.dumps({"latest_tick": {"price": 97_000}}, ensure_ascii=False),
            json.dumps({"source_count": 1, "active_source_count": 1}),
            json.dumps(
                {
                    "quality_status": "FRESH",
                    "has_latest_tick": True,
                    "tick_age_sec": 1.0,
                    "has_1m_bar": True,
                    "has_3m_bar": True,
                    "has_5m_bar": True,
                    "vwap_ready": True,
                    "reason_codes": [],
                }
            ),
            now,
        ),
    )
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
            name,
            price,
            change_rate,
            cumulative_volume,
            cumulative_trade_value,
            execution_strength,
            best_bid,
            best_ask,
            spread_ticks,
            day_high,
            day_low,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            updated_at
        )
        VALUES (?, ?, 97000, 2.0, 1000, 97000000, 120.0, 96900, 97000, 1,
            100000, 94000, ?, ?, ?, 'test', ?, 'FRESH', ?)
        """,
        (code, name, now, now, now, f"evt-{code}", now),
    )
    for interval_sec, trade_value_delta in ((60, 10_000_000), (180, 20_000_000), (300, 30_000_000)):
        connection.execute(
            """
            INSERT INTO market_minute_bars (
                code,
                interval_sec,
                bucket_start,
                open,
                high,
                low,
                close,
                volume_delta,
                trade_value_delta,
                tick_count,
                vwap,
                updated_at
            )
            VALUES (?, ?, ?, 96000, 100000, 94000, 97000, 1000, ?, 3, 96500, ?)
            """,
            (code, interval_sec, f"{now}-{interval_sec}", trade_value_delta, now),
        )
    connection.execute(
        """
        INSERT INTO theme_latest_snapshots (
            theme_id,
            snapshot_id,
            theme_name,
            calculated_at,
            state,
            quality_status,
            leading_code,
            leading_name,
            fresh_coverage_ratio,
            rising_ratio,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m
        )
        VALUES (?, ?, '반도체', ?, 'LEADING', 'FRESH', ?, ?, 1.0, 0.6,
            97000000, 10000000, 20000000, 30000000)
        """,
        (theme_id, snapshot_id, now, code, name),
    )
    connection.execute(
        """
        INSERT INTO theme_snapshot_members (
            snapshot_id,
            theme_id,
            code,
            name,
            price,
            change_rate,
            cumulative_trade_value,
            volume_delta_1m,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m,
            execution_strength,
            vwap,
            above_vwap,
            readiness_status,
            member_role,
            tick_age_sec,
            event_ts,
            calculated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 97000, 2.0, 97000000, 1000, 10000000, 20000000,
            30000000, 120.0, 96500, 1, 'FRESH', 'LEADER_CANDIDATE', 1.0, ?, ?, '{}')
        """,
        (snapshot_id, theme_id, code, name, now, now),
    )
    connection.commit()
    return candidate_id
