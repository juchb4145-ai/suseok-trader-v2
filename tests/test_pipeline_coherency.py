from __future__ import annotations

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from fastapi.testclient import TestClient
from services.entry_timing.service import evaluate_entry_timing
from services.live_sim.order_plan_eligibility import (
    evaluate_live_sim_order_plan_eligibility,
)
from services.pipeline_coherency import build_pipeline_coherency_status
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import SCHEMA_VERSION, initialize_database
from tests.test_entry_timing import _raise_fixture_turnover, _settings
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
)
from tests.test_strategy_service import _insert_strategy_fixture

LINEAGE_COLUMNS = {
    "source_run_id",
    "source_watermark",
    "source_watermark_hash",
    "source_event_id",
    "source_observed_at",
    "data_age_sec",
    "generated_by",
}


def test_schema_59_pipeline_lineage_migration_is_additive_and_reentrant(tmp_path) -> None:
    db_path = tmp_path / "pipeline-lineage-migration.sqlite3"
    connection = initialize_database(db_path)
    for index_name in (
        "idx_strategy_latest_source_watermark",
        "idx_risk_latest_source_watermark",
        "idx_entry_timing_candidate_lineage",
        "idx_order_plan_latest_candidate_lineage",
    ):
        connection.execute(f"DROP INDEX {index_name}")
    table_columns = {
        "strategy_observations": LINEAGE_COLUMNS,
        "strategy_observations_latest": LINEAGE_COLUMNS,
        "risk_observations": LINEAGE_COLUMNS,
        "risk_observations_latest": LINEAGE_COLUMNS,
        "entry_timing_evaluations": LINEAGE_COLUMNS
        | {"strategy_observation_id", "risk_observation_id"},
        "order_plan_drafts": LINEAGE_COLUMNS
        | {
            "entry_timing_evaluation_id",
            "strategy_observation_id",
            "risk_observation_id",
        },
        "order_plan_drafts_latest": LINEAGE_COLUMNS
        | {
            "entry_timing_evaluation_id",
            "strategy_observation_id",
            "risk_observation_id",
        },
    }
    for table_name, columns in table_columns.items():
        for column_name in columns:
            connection.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
    connection.execute("UPDATE app_metadata SET value = '58' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    rerun = initialize_database(db_path)
    try:
        schema_version = migrated.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()["value"]
        migrated_columns = {
            table_name: {
                row["name"]
                for row in migrated.execute(f"PRAGMA table_info({table_name})")
            }
            for table_name in table_columns
        }
        indexes = {
            row["name"]
            for table_name in table_columns
            for row in rerun.execute(f"PRAGMA index_list({table_name})")
        }
    finally:
        migrated.close()
        rerun.close()

    assert schema_version == str(SCHEMA_VERSION) == "60"
    for table_name, columns in table_columns.items():
        assert columns <= migrated_columns[table_name]
    assert {
        "idx_strategy_latest_source_watermark",
        "idx_risk_latest_source_watermark",
        "idx_entry_timing_candidate_lineage",
        "idx_order_plan_latest_candidate_lineage",
    } <= indexes


def test_candidate_to_order_plan_persists_one_coherent_source_run(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-coherent.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)

    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(
        connection,
        strategy,
        source_run_id="pipeline-run-coherent",
    )
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(
        connection,
        risk,
        source_run_id="pipeline-run-coherent",
    )
    result = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
        source_run_id="pipeline-run-coherent",
    )
    status = build_pipeline_coherency_status(
        connection,
        max_age_sec=settings.entry_timing_stale_max_seconds,
    )
    rows = {
        "strategy": connection.execute(
            "SELECT * FROM strategy_observations_latest"
        ).fetchone(),
        "risk": connection.execute("SELECT * FROM risk_observations_latest").fetchone(),
        "entry": connection.execute(
            "SELECT * FROM entry_timing_evaluations"
        ).fetchone(),
        "plan": connection.execute("SELECT * FROM order_plan_drafts_latest").fetchone(),
    }
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert result.plan_ready_count == 1
    assert status["status"] == "PASS"
    assert status["coherent_count"] == 1
    assert {row["source_run_id"] for row in rows.values()} == {
        "pipeline-run-coherent"
    }
    assert len({row["source_watermark_hash"] for row in rows.values()}) == 1
    assert rows["risk"]["strategy_observation_id"] == strategy.strategy_observation_id
    assert rows["entry"]["risk_observation_id"] == risk.risk_observation_id
    assert (
        rows["plan"]["entry_timing_evaluation_id"]
        == rows["entry"]["entry_timing_evaluation_id"]
    )
    assert command_count == 0


def test_watermark_mismatch_blocks_plan_ready_and_dashboard_reports_fail(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-mismatch.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy, source_run_id="pipeline-run-a")
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk, source_run_id="pipeline-run-a")
    connection.execute(
        """
        UPDATE risk_observations_latest
        SET source_watermark_hash = 'mismatched-watermark'
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    connection.commit()

    result = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
        source_run_id="pipeline-run-a",
    )
    status = build_pipeline_coherency_status(
        connection,
        max_age_sec=settings.entry_timing_stale_max_seconds,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert result.plan_ready_count == 0
    assert result.data_wait_count == 1
    assert result.evaluations[0].status.value == "DATA_WAIT"
    assert "STRATEGY_RISK_WATERMARK_MISMATCH" in result.evaluations[0].reason_codes
    assert "PIPELINE_COHERENCY_GUARD_BLOCKED" in result.evaluations[0].reason_codes
    assert status["status"] == "FAIL"
    assert status["mismatch_count"] == 1
    assert command_count == 0


def test_pipeline_status_detects_risk_only_orphan_candidate(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-risk-orphan.sqlite3")
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id,
            risk_observation_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (
            'candidate-risk-orphan',
            'risk-orphan',
            'strategy-missing',
            '2026-07-10',
            '005930',
            '삼성전자',
            ?,
            'OBSERVE_PASS',
            'INFO',
            0,
            0,
            1,
            '[]',
            'test',
            1
        )
        """,
        (now,),
    )
    connection.commit()

    status = build_pipeline_coherency_status(connection)
    connection.close()

    assert status["status"] == "FAIL"
    assert status["candidate_count"] == 1
    assert status["items"][0]["candidate_instance_id"] == "candidate-risk-orphan"
    assert "STRATEGY_OBSERVATION_MISSING" in status["items"][0]["reason_codes"]


def test_new_tick_after_risk_blocks_entry_timing_source_mix(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-newer-tick.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy, source_run_id="pipeline-run-tick")
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk, source_run_id="pipeline-run-tick")
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET event_id = 'evt-newer-than-risk', event_ts = ?
        WHERE code = '005930' AND exchange = 'KRX'
        """,
        (datetime_to_wire(utc_now()),),
    )
    connection.commit()

    result = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
        source_run_id="pipeline-run-tick",
    )
    status = build_pipeline_coherency_status(
        connection,
        max_age_sec=settings.entry_timing_stale_max_seconds,
    )
    connection.close()

    assert result.plan_ready_count == 0
    assert result.data_wait_count == 1
    assert "CURRENT_SOURCE_WATERMARK_MISMATCH" in result.evaluations[0].reason_codes
    assert status["status"] == "FAIL"
    assert "CURRENT_SOURCE_WATERMARK_MISMATCH" in status["reason_codes"]


def test_pipeline_coherency_operator_and_fast_dashboard_are_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "pipeline-api.sqlite3"
    connection = initialize_database(db_path)
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))

    with TestClient(app) as client:
        operator = client.get(
            "/api/operator/pipeline-coherency/status?trade_date=2026-07-10&limit=5"
        )
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "pipeline_coherency,pipeline_summary"
        )

    assert operator.status_code == 200
    assert operator.json()["status"] == "WARN"
    assert operator.json()["trade_date"] == "2026-07-10"
    assert operator.json()["read_only"] is True
    assert operator.json()["no_order_side_effects"] is True
    assert dashboard.status_code == 200
    assert dashboard.json()["pipeline_coherency"]["status"] == "WARN"
    assert dashboard.json()["pipeline_summary"]["coherency"]["status"] == "WARN"


def test_live_sim_order_plan_revalidates_persisted_lineage(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "live-sim-lineage.sqlite3"
    )
    connection.execute(
        """
        UPDATE order_plan_drafts
        SET source_watermark = '{"tampered":true}'
        WHERE order_plan_id = ?
        """,
        (order_plan_id,),
    )
    connection.execute(
        """
        UPDATE order_plan_drafts_latest
        SET source_watermark = '{"tampered":true}'
        WHERE order_plan_id = ?
        """,
        (order_plan_id,),
    )
    connection.commit()

    eligibility = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    coherency = build_pipeline_coherency_status(
        connection,
        max_age_sec=999_999_999,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert eligibility.eligible is False
    assert LiveSimReasonCode.ORDER_PLAN_LINEAGE_INVALID.value in eligibility.reason_codes
    assert eligibility.evidence_json["pipeline_lineage_guard"]["status"] == "FAIL"
    assert "ORDER_PLAN_WATERMARK_HASH_INVALID" in eligibility.evidence_json[
        "pipeline_lineage_guard"
    ]["reason_codes"]
    assert coherency["status"] == "FAIL"
    assert command_count == 0
