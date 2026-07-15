from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime import evaluation_run_guard
from services.runtime import targeted_pipeline_rebuild as targeted
from services.runtime.targeted_pipeline_rebuild import (
    TargetedPipelineRebuildError,
    run_targeted_pipeline_rebuild,
)
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture

TRADE_DATE = "2026-07-15"
STOCK_CODES = ("005930", "000660", "035420", "051910", "068270", "012330")


def test_targeted_pipeline_rebuild_runs_exact_max_five_without_order_artifacts(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(
        tmp_path,
        count=5,
    )

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    rows = {
        "strategy": connection.execute(
            "SELECT * FROM strategy_observations ORDER BY candidate_instance_id"
        ).fetchall(),
        "risk": connection.execute(
            "SELECT * FROM risk_observations ORDER BY candidate_instance_id"
        ).fetchall(),
        "entry": connection.execute(
            "SELECT * FROM entry_timing_evaluations ORDER BY candidate_instance_id"
        ).fetchall(),
    }
    plan_count = _count(connection, "order_plan_drafts")
    command_count = _count(connection, "gateway_commands")
    lock_count = _count(connection, "runtime_execution_locks")
    connection.close()

    assert result["status"] == "COMPLETED"
    assert result["candidate_count"] == 5
    assert result["single_evaluation_pipeline_lease"] is True
    assert result["single_source_run"] is True
    assert result["order_artifacts_unchanged"] is True
    assert result["command_type_counts_unchanged"] is True
    assert len(result["runtime_safety"]["trading_env_file_sha256"]) == 64
    assert len(result["runtime_safety"]["settings_sha256"]) == 64
    assert all(len(item["source_watermark_hash"]) == 64 for item in result["items"])
    assert all(item["order_plan_id"] is None for item in result["items"])
    assert len(rows["strategy"]) == len(rows["risk"]) == len(rows["entry"]) == 5
    assert {row["source_run_id"] for row in rows["strategy"]} == {
        result["source_run_id"]
    }
    assert {row["source_run_id"] for row in rows["risk"]} == {
        result["source_run_id"]
    }
    assert {row["source_run_id"] for row in rows["entry"]} == {
        result["source_run_id"]
    }
    assert all(row["order_plan_id"] is None for row in rows["entry"])
    assert all(row["not_order_intent"] == 1 for row in rows["entry"])
    assert plan_count == command_count == lock_count == 0


def test_targeted_pipeline_rebuild_rejects_batch_six_before_environment_check() -> None:
    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            None,  # type: ignore[arg-type]
            [f"candidate-{index}" for index in range(6)],
            trade_date=TRADE_DATE,
            environ={},
        )

    assert exc_info.value.reason_codes == ("BATCH_SIZE_EXCEEDED",)


def test_targeted_pipeline_rebuild_rejects_not_order_intent_false_before_run() -> None:
    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            None,  # type: ignore[arg-type]
            ["candidate-one"],
            trade_date=TRADE_DATE,
            environ={},
            not_order_intent=False,
        )

    assert exc_info.value.reason_codes == ("NOT_ORDER_INTENT_REQUIRED",)


def test_targeted_pipeline_rebuild_rejects_preexisting_transaction(tmp_path) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute("BEGIN")

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert exc_info.value.reason_codes == ("PREEXISTING_TRANSACTION_FORBIDDEN",)
    connection.rollback()
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_rejects_unsafe_audited_environment(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    env_path = Path(environ["TRADING_ENV_FILE"])
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=true",
        ),
        encoding="utf-8",
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )
    connection.close()

    assert "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE" in (
        exc_info.value.reason_codes
    )


@pytest.mark.parametrize(
    ("setting_name", "safe_value", "unsafe_value", "expected_reason"),
    [
        (
            "INCREMENTAL_EVALUATION_WORKER_ENABLED",
            "false",
            "true",
            "INCREMENTAL_EVALUATION_WORKER_NOT_FALSE",
        ),
        (
            "ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS",
            "false",
            "true",
            "ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS_NOT_FALSE",
        ),
        (
            "STRATEGY_ENGINE_ENABLED",
            "true",
            "false",
            "AUDITED_STRATEGY_ENGINE_DISABLED",
        ),
        (
            "STRATEGY_ENGINE_OBSERVE_ONLY",
            "true",
            "false",
            "AUDITED_STRATEGY_ENGINE_NOT_OBSERVE_ONLY",
        ),
        (
            "RISK_GATE_ENABLED",
            "true",
            "false",
            "AUDITED_RISK_GATE_DISABLED",
        ),
        (
            "RISK_GATE_OBSERVE_ONLY",
            "true",
            "false",
            "AUDITED_RISK_GATE_NOT_OBSERVE_ONLY",
        ),
        (
            "ENTRY_TIMING_ENABLED",
            "true",
            "false",
            "AUDITED_ENTRY_TIMING_DISABLED",
        ),
        (
            "REALTIME_SUBSCRIPTION_QUEUE_COMMANDS",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:REALTIME_SUBSCRIPTION_QUEUE_COMMANDS",
        ),
        (
            "DRY_RUN_SIMULATED_FILL_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:DRY_RUN_SIMULATED_FILL_ENABLED",
        ),
        (
            "DRY_RUN_EXIT_SIMULATED_FILL_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:DRY_RUN_EXIT_SIMULATED_FILL_ENABLED",
        ),
        (
            "LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED",
        ),
        (
            "LIVE_SIM_LIFECYCLE_WORKER_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:LIVE_SIM_LIFECYCLE_WORKER_ENABLED",
        ),
        (
            "LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED",
        ),
        (
            "PROJECTION_OUTBOX_WORKER_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:PROJECTION_OUTBOX_WORKER_ENABLED",
        ),
        (
            "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED",
            "false",
            "true",
            "COMMAND_PRODUCER_ENV_NOT_FALSE:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED",
        ),
    ],
)
def test_targeted_pipeline_rebuild_rejects_each_unsafe_runtime_flag(
    tmp_path,
    setting_name,
    safe_value,
    unsafe_value,
    expected_reason,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    env_path = Path(environ["TRADING_ENV_FILE"])
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            f"{setting_name}={safe_value}",
            f"{setting_name}={unsafe_value}",
        ),
        encoding="utf-8",
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert expected_reason in exc_info.value.reason_codes
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_rejects_missing_explicit_engine_flag(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    env_path = Path(environ["TRADING_ENV_FILE"])
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            "ENTRY_TIMING_ENABLED=true\n",
            "",
        ),
        encoding="utf-8",
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "ENV_SETTING_NOT_EXPLICIT:ENTRY_TIMING_ENABLED" in (
        exc_info.value.reason_codes
    )
    connection.close()


def test_targeted_pipeline_rebuild_rejects_runtime_settings_not_in_audited_env(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    mismatched = replace(settings, entry_timing_min_turnover_krw=2_000)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=mismatched,
            environ=environ,
        )

    assert "RUNTIME_SETTINGS_DO_NOT_MATCH_AUDITED_ENV" in (
        exc_info.value.reason_codes
    )
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_rejects_zero_eligible_without_lease_write(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute(
        "UPDATE candidates SET state = 'CLOSED' WHERE candidate_instance_id = ?",
        (candidate_ids[0],),
    )
    connection.commit()

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "NO_ELIGIBLE_CANDIDATES" in exc_info.value.reason_codes
    assert _count(connection, "runtime_execution_locks") == 0
    assert _count(connection, "runtime_execution_lock_fences") == 0
    assert _count(connection, "strategy_observations") == 0
    connection.close()


@pytest.mark.parametrize("state", ["DETECTED", "HYDRATING", "DATA_WAIT", "COOLDOWN"])
def test_targeted_pipeline_rebuild_rejects_non_evaluation_active_states(
    tmp_path,
    state,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute(
        "UPDATE candidates SET state = ? WHERE candidate_instance_id = ?",
        (state, candidate_ids[0]),
    )
    connection.commit()

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "NO_ELIGIBLE_CANDIDATES" in exc_info.value.reason_codes
    assert "CANDIDATE_NOT_STRATEGY_ELIGIBLE" in exc_info.value.details["blocked"][
        candidate_ids[0]
    ]
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_rejects_active_source_count_mismatch(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute(
        "UPDATE candidates SET active_source_count = 2 WHERE candidate_instance_id = ?",
        (candidate_ids[0],),
    )
    connection.commit()

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "CANDIDATE_ACTIVE_SOURCE_COUNT_MISMATCH" in exc_info.value.details[
        "blocked"
    ][candidate_ids[0]]
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_detects_active_source_inventory_cas_drift(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_classify = targeted._classify_candidates
    call_count = 0

    def classify_with_drift(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            connection.execute(
                """
                UPDATE candidate_sources_latest
                SET payload_json = '{"drift":true}'
                WHERE candidate_instance_id = ?
                """,
                (candidate_ids[0],),
            )
        return real_classify(*args, **kwargs)

    monkeypatch.setattr(targeted, "_classify_candidates", classify_with_drift)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "CURRENT_SOURCE_CHANGED_DURING_REBUILD" in exc_info.value.reason_codes
    payload = connection.execute(
        """
        SELECT payload_json
        FROM candidate_sources_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_ids[0],),
    ).fetchone()[0]
    assert payload == "{}"
    assert _count(connection, "strategy_observations") == 0
    connection.close()


def test_targeted_pipeline_rebuild_fence_loss_rolls_back_partial_pipeline(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_assert = targeted.assert_runtime_execution_fence
    call_count = 0

    def lose_fence(connection, *, lease=None, renew=True):
        nonlocal call_count
        call_count += 1
        if call_count == 4:
            raise RuntimeError("synthetic fence loss")
        return real_assert(connection, lease=lease, renew=renew)

    monkeypatch.setattr(targeted, "assert_runtime_execution_fence", lose_fence)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "EVALUATION_RUN_FENCE_LOST" in exc_info.value.reason_codes
    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "risk_observations") == 0
    assert _count(connection, "entry_timing_evaluations") == 0
    assert _count(connection, "order_plan_drafts") == 0
    assert _count(connection, "gateway_commands") == 0
    assert _count(connection, "runtime_execution_locks") == 0
    connection.close()


def test_targeted_pipeline_rebuild_middle_failure_rolls_back_all_stages(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    def fail_risk(*args, **kwargs):
        raise RuntimeError("synthetic risk failure")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        fail_risk,
    )

    with pytest.raises(RuntimeError, match="synthetic risk failure"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "strategy_observations_latest") == 0
    assert _count(connection, "risk_observations") == 0
    assert _count(connection, "entry_timing_evaluations") == 0
    assert _count(connection, "order_plan_drafts") == 0
    assert _count(connection, "gateway_commands") == 0
    connection.close()


def test_targeted_pipeline_rebuild_authorizer_denies_command_write(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    def write_command(connection, *args, **kwargs):
        connection.execute(
            """
            INSERT INTO gateway_commands (
                command_id,
                command_type,
                source,
                status,
                payload_json,
                payload_hash
            )
            VALUES ('forbidden-command', 'send_order', 'test', 'PENDING', '{}', 'hash')
            """
        )
        raise AssertionError("authorizer did not reject gateway command write")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        write_command,
    )

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "gateway_commands") == 0
    connection.close()


@pytest.mark.parametrize(
    "forbidden_sql",
    [
        "SAVEPOINT forbidden_stage",
        "PRAGMA user_version = 61",
        "CREATE TABLE forbidden_stage_table (id INTEGER)",
    ],
)
def test_targeted_pipeline_rebuild_authorizer_denies_transactional_escape_sql(
    tmp_path,
    monkeypatch,
    forbidden_sql,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    def execute_forbidden_sql(connection, *args, **kwargs):
        connection.execute(forbidden_sql)
        raise AssertionError("strict authorizer did not reject forbidden SQL")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        execute_forbidden_sql,
    )

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert _count(connection, "strategy_observations") == 0
    assert connection.execute("PRAGMA user_version").fetchone()[0] != 61
    assert connection.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'forbidden_stage_table'"
    ).fetchone() is None
    connection.close()


def test_targeted_pipeline_rebuild_internal_commit_cannot_bypass_outer_rollback(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_save = targeted.save_strategy_observation

    def save_then_commit(connection, observation, **kwargs):
        real_save(connection, observation, **kwargs)
        connection.commit()

    monkeypatch.setattr(targeted, "save_strategy_observation", save_then_commit)

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "strategy_setup_observations") == 0
    assert _count(connection, "strategy_observations_latest") == 0
    assert _count(connection, "runtime_execution_locks") == 0
    connection.close()


def test_targeted_pipeline_rebuild_authorizer_denies_attach_and_non_main_dml(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    auxiliary_path = tmp_path / "auxiliary.sqlite3"

    def attach_database(connection, *args, **kwargs):
        connection.execute("ATTACH DATABASE ? AS auxiliary", (str(auxiliary_path),))
        raise AssertionError("strict authorizer did not reject ATTACH")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        attach_database,
    )
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )
    assert _count(connection, "strategy_observations") == 0

    connection.execute("ATTACH DATABASE ? AS auxiliary", (str(auxiliary_path),))
    connection.execute(
        "CREATE TABLE auxiliary.strategy_observations (value TEXT NOT NULL)"
    )
    connection.commit()

    def write_auxiliary(connection, *args, **kwargs):
        connection.execute(
            "INSERT INTO auxiliary.strategy_observations (value) VALUES ('forbidden')"
        )
        raise AssertionError("strict authorizer allowed non-main DML")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        write_auxiliary,
    )
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )
    assert connection.execute(
        "SELECT COUNT(*) FROM auxiliary.strategy_observations"
    ).fetchone()[0] == 0

    def detach_auxiliary(connection, *args, **kwargs):
        connection.execute("DETACH DATABASE auxiliary")
        raise AssertionError("strict authorizer did not reject DETACH")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        detach_auxiliary,
    )
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )
    connection.close()


def test_targeted_pipeline_rebuild_rejects_closed_at_even_if_state_is_ready(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute(
        "UPDATE candidates SET closed_at = ? WHERE candidate_instance_id = ?",
        (datetime_to_wire(utc_now()), candidate_ids[0]),
    )
    connection.commit()

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    blocked = exc_info.value.details["blocked"][candidate_ids[0]]
    assert "CANDIDATE_CLOSED_AT_PRESENT" in blocked
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


@pytest.mark.parametrize("column", ["trade_date", "code", "source_id"])
def test_targeted_pipeline_rebuild_rejects_source_identity_drift(
    tmp_path,
    column,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    replacement = {
        "trade_date": "2026-07-14",
        "code": "999999",
        "source_id": "wrong-source",
    }[column]
    connection.execute(
        f"UPDATE candidate_sources_latest SET {column} = ? "
        "WHERE candidate_instance_id = ?",
        (replacement, candidate_ids[0]),
    )
    connection.commit()

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "ACTIVE_CANDIDATE_SOURCE_IDENTITY_MISMATCH" in exc_info.value.details[
        "blocked"
    ][candidate_ids[0]]
    assert _count(connection, "runtime_execution_lock_fences") == 0
    connection.close()


def test_targeted_pipeline_rebuild_checks_freshness_of_every_active_source(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    candidate_id = candidate_ids[0]
    candidate = connection.execute(
        "SELECT code, name FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    stale_at = datetime_to_wire(utc_now() - timedelta(seconds=120))
    event_id = "evt-secondary-stale"
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active, payload_json
        ) VALUES (?, ?, ?, ?, ?, 'CONDITION', 'secondary', 'UPSERT', ?, ?, 1, '{}')
        """,
        (
            event_id,
            candidate_id,
            TRADE_DATE,
            candidate["code"],
            candidate["name"],
            stale_at,
            stale_at,
        ),
    )
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date, code, source_type, source_id, candidate_instance_id,
            name, active, first_seen_at, last_seen_at, last_event_id, payload_json
        ) VALUES (?, ?, 'CONDITION', 'secondary', ?, ?, 1, ?, ?, ?, '{}')
        """,
        (
            TRADE_DATE,
            candidate["code"],
            candidate_id,
            candidate["name"],
            stale_at,
            stale_at,
            event_id,
        ),
    )
    connection.execute(
        "UPDATE candidates SET source_count = 2, active_source_count = 2 "
        "WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    connection.commit()
    strict_settings = replace(settings, candidate_source_stale_sec=60)
    env_path = Path(environ["TRADING_ENV_FILE"])
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            "CANDIDATE_SOURCE_STALE_SEC=999999999",
            "CANDIDATE_SOURCE_STALE_SEC=60",
        ),
        encoding="utf-8",
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=strict_settings,
            environ=environ,
        )

    assert "ACTIVE_CANDIDATE_SOURCE_STALE" in exc_info.value.details["blocked"][
        candidate_id
    ]
    connection.close()


def test_targeted_pipeline_rebuild_detects_non_target_stage_mutation(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    connection.execute(
        """
        INSERT INTO entry_timing_evaluation_errors (
            candidate_instance_id, code, error_message, payload_json
        ) VALUES ('other-candidate', '999999', 'original', '{}')
        """
    )
    connection.commit()
    real_evaluate = targeted.evaluate_risk_for_strategy_observation

    def mutate_non_target(connection, *args, **kwargs):
        connection.execute(
            "UPDATE entry_timing_evaluation_errors SET error_message = 'tampered' "
            "WHERE candidate_instance_id = 'other-candidate'"
        )
        return real_evaluate(connection, *args, **kwargs)

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        mutate_non_target,
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "NON_TARGET_PIPELINE_ROW_CHANGED" in exc_info.value.reason_codes
    assert connection.execute(
        "SELECT error_message FROM entry_timing_evaluation_errors "
        "WHERE candidate_instance_id = 'other-candidate'"
    ).fetchone()[0] == "original"
    assert _count(connection, "strategy_observations") == 0
    connection.close()


def test_targeted_pipeline_rebuild_detects_target_row_after_capture_tamper(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_capture = targeted._capture_persisted_row_fingerprints

    def capture_then_tamper(connection, **kwargs):
        fingerprints = real_capture(connection, **kwargs)
        connection.execute(
            "UPDATE strategy_observations SET name = 'tampered' "
            "WHERE strategy_observation_id = ?",
            (kwargs["strategy_observation_id"],),
        )
        return fingerprints

    monkeypatch.setattr(
        targeted,
        "_capture_persisted_row_fingerprints",
        capture_then_tamper,
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "TARGET_PIPELINE_CANONICAL_ROW_CHANGED" in exc_info.value.reason_codes
    assert _count(connection, "strategy_observations") == 0
    connection.close()


def test_targeted_pipeline_rebuild_returns_committed_cleanup_failed_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    def fail_release(*args, **kwargs):
        raise RuntimeError("synthetic lock cleanup failure")

    monkeypatch.setattr(evaluation_run_guard, "_release_lock", fail_release)

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    assert result["status"] == "COMMITTED_CLEANUP_FAILED"
    assert result["data_committed"] is True
    assert result["lock_cleanup"]["reason_code"] == "COMMITTED_CLEANUP_FAILED"
    assert result["lock_cleanup"]["operator_action_required"] is True
    assert _count(connection, "strategy_observations") == 1
    assert _count(connection, "risk_observations") == 1
    assert _count(connection, "entry_timing_evaluations") == 1
    assert _count(connection, "runtime_execution_locks") == 1
    connection.close()


def test_targeted_pipeline_rebuild_reconciles_durable_commit_then_signal_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_connect = targeted.sqlite3.connect
    reconciliation_connect_calls: list[tuple[object, dict[str, object]]] = []

    def track_reconciliation_connection(*args, **kwargs):
        reconciliation_connect_calls.append((args[0], dict(kwargs)))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(targeted.sqlite3, "connect", track_reconciliation_connection)

    @contextmanager
    def commit_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.commit()
        raise sqlite3.OperationalError("synthetic post-durable commit signal failure")

    monkeypatch.setattr(targeted, "immediate_transaction", commit_then_raise)

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    assert result["status"] == "COMMITTED_TRANSACTION_SIGNAL_FAILED"
    assert result["data_committed"] is True
    assert result["operator_action_required"] is True
    assert result["transaction_outcome"]["outcome"] == "COMMITTED_RECONCILED"
    checks = result["transaction_outcome"]["checks"]
    assert checks["canonical_target_rows"] is True
    assert checks["pipeline_stage_delta"] is True
    assert checks["source_watermark"] is True
    assert checks["order_artifacts"] is True
    assert checks["fence_current"] is True
    assert checks["audited_env_unchanged"] is True
    assert checks["independent_read_only_connection"] is True
    assert len(reconciliation_connect_calls) == 1
    assert "mode=ro" in str(reconciliation_connect_calls[0][0])
    assert reconciliation_connect_calls[0][1]["uri"] is True
    assert result["lock_cleanup"]["status"] == "COMPLETED"
    assert _count(connection, "strategy_observations") == 1
    assert _count(connection, "risk_observations") == 1
    assert _count(connection, "entry_timing_evaluations") == 1
    assert _count(connection, "order_plan_drafts") == 0
    assert _count(connection, "gateway_commands") == 0
    connection.close()


def test_targeted_pipeline_rebuild_marks_tampered_durable_commit_outcome_unknown(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    @contextmanager
    def tamper_commit_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.execute(
            "UPDATE strategy_observations SET name = 'tampered-after-stage'"
        )
        connection.commit()
        raise sqlite3.OperationalError("synthetic ambiguous commit signal failure")

    monkeypatch.setattr(
        targeted,
        "immediate_transaction",
        tamper_commit_then_raise,
    )

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    assert result["status"] == "OUTCOME_UNKNOWN"
    assert result["data_committed"] is None
    assert result["operator_action_required"] is True
    assert result["transaction_outcome"]["outcome"] == "OUTCOME_UNKNOWN"
    assert result["transaction_outcome"]["checks"]["canonical_target_rows"] is False
    assert result["no_order_plans_created"] is True
    assert result["no_order_commands_created"] is True
    assert _count(connection, "order_plan_drafts") == 0
    assert _count(connection, "gateway_commands") == 0
    connection.close()


def test_targeted_pipeline_rebuild_requires_independent_durable_visibility(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    @contextmanager
    def commit_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.commit()
        raise sqlite3.OperationalError("synthetic durable commit signal failure")

    def fail_independent_connection(*_args, **_kwargs):
        raise sqlite3.OperationalError("synthetic independent read-only open failure")

    monkeypatch.setattr(targeted, "immediate_transaction", commit_then_raise)
    monkeypatch.setattr(targeted.sqlite3, "connect", fail_independent_connection)

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    assert result["status"] == "OUTCOME_UNKNOWN"
    assert result["data_committed"] is None
    checks = result["transaction_outcome"]["checks"]
    assert checks["independent_read_only_connection"] is False
    assert (
        result["transaction_outcome"]["check_error_types"][
            "independent_read_only_connection"
        ]
        == "OperationalError"
    )
    assert _count(connection, "strategy_observations") == 1
    assert _count(connection, "order_plan_drafts") == 0
    assert _count(connection, "gateway_commands") == 0
    connection.close()


def test_targeted_pipeline_rebuild_confirms_rollback_before_reraising_commit_error(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)

    @contextmanager
    def rollback_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.rollback()
        raise sqlite3.OperationalError("synthetic confirmed rollback")

    monkeypatch.setattr(targeted, "immediate_transaction", rollback_then_raise)

    with pytest.raises(sqlite3.OperationalError, match="confirmed rollback"):
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "risk_observations") == 0
    assert _count(connection, "entry_timing_evaluations") == 0
    assert _count(connection, "runtime_execution_locks") == 0
    connection.close()


def test_targeted_pipeline_rebuild_preserves_unknown_when_lock_exit_also_fails(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    real_runtime_lock = targeted.runtime_execution_lock

    @contextmanager
    def commit_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.commit()
        raise sqlite3.OperationalError("synthetic ambiguous commit signal")

    class ExitFailingLockContext:
        def __init__(self, inner) -> None:
            self.inner = inner

        def __enter__(self):
            return self.inner.__enter__()

        def __exit__(self, *args):
            self.inner.__exit__(*args)
            raise RuntimeError("synthetic lock context exit failure")

    def exit_failing_runtime_lock(*args, **kwargs):
        return ExitFailingLockContext(real_runtime_lock(*args, **kwargs))

    def force_open_unknown(*_args, **_kwargs):
        return targeted._TransactionReconciliation(
            outcome="OUTCOME_UNKNOWN",
            evidence={
                "connection_in_transaction_after_reconciliation": True,
                "checks": {},
                "check_error_types": {},
                "operator_action_required": True,
            },
        )

    monkeypatch.setattr(targeted, "immediate_transaction", commit_then_raise)
    monkeypatch.setattr(
        targeted,
        "runtime_execution_lock",
        exit_failing_runtime_lock,
    )
    monkeypatch.setattr(
        targeted,
        "_reconcile_transaction_finalization",
        force_open_unknown,
    )

    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )

    assert result["status"] == "OUTCOME_UNKNOWN"
    assert result["data_committed"] is None
    assert result["lock_cleanup"]["status"] == "FAILED"
    assert result["lock_cleanup"]["error_type"] == "RuntimeError"
    assert "LOCK_CLEANUP_FAILED_WITH_OUTCOME_UNKNOWN" in result["failure_codes"]
    assert result["operator_action_required"] is True
    connection.close()


def test_targeted_pipeline_rebuild_rejects_env_hardlink_and_redacts_path(
    tmp_path,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    env_path = Path(environ["TRADING_ENV_FILE"])
    hardlink_path = tmp_path / "fast0r3-hardlink.env"
    os.link(env_path, hardlink_path)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )
    assert "TRADING_ENV_FILE_HARDLINK_FORBIDDEN" in exc_info.value.reason_codes

    hardlink_path.unlink()
    result = run_targeted_pipeline_rebuild(
        connection,
        candidate_ids,
        trade_date=TRADE_DATE,
        settings=settings,
        environ=environ,
    )
    assert str(env_path) not in str(result)
    assert result["runtime_safety"]["trading_env_file_path_redacted"] is True
    connection.close()


def test_targeted_pipeline_rebuild_detects_env_toctou_before_commit(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    env_path = Path(environ["TRADING_ENV_FILE"])
    real_run_candidate = targeted._run_candidate_pipeline

    def run_then_change_env(*args, **kwargs):
        item = real_run_candidate(*args, **kwargs)
        env_path.write_text(
            env_path.read_text(encoding="utf-8") + "\n# changed during run\n",
            encoding="utf-8",
        )
        return item

    monkeypatch.setattr(targeted, "_run_candidate_pipeline", run_then_change_env)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "AUDITED_ENV_CHANGED_DURING_REBUILD" in exc_info.value.reason_codes
    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "runtime_execution_locks") == 0
    connection.close()


def test_targeted_pipeline_rebuild_sql_progress_deadline_rolls_back(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    monkeypatch.setattr(targeted, "TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC", 0.1)

    def run_long_sql(connection, *args, **kwargs):
        connection.execute(
            """
            WITH RECURSIVE counter(value) AS (
                VALUES(1)
                UNION ALL
                SELECT value + 1 FROM counter WHERE value < 100000000
            )
            SELECT SUM(value) FROM counter
            """
        ).fetchone()
        raise AssertionError("deadline progress handler did not interrupt SQL")

    monkeypatch.setattr(
        targeted,
        "evaluate_risk_for_strategy_observation",
        run_long_sql,
    )

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "TARGETED_REBUILD_WALL_TIME_EXCEEDED" in exc_info.value.reason_codes
    assert _count(connection, "strategy_observations") == 0
    assert connection.execute("SELECT 1").fetchone()[0] == 1
    connection.close()


def test_targeted_pipeline_rebuild_hard_wall_limit_rolls_back(
    tmp_path,
    monkeypatch,
) -> None:
    connection, settings, environ, candidate_ids = _prepared_runner(tmp_path)
    monkeypatch.setattr(targeted, "TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC", -1.0)

    with pytest.raises(TargetedPipelineRebuildError) as exc_info:
        run_targeted_pipeline_rebuild(
            connection,
            candidate_ids,
            trade_date=TRADE_DATE,
            settings=settings,
            environ=environ,
        )

    assert "TARGETED_REBUILD_WALL_TIME_EXCEEDED" in exc_info.value.reason_codes
    assert _count(connection, "strategy_observations") == 0
    assert _count(connection, "runtime_execution_locks") == 0
    connection.close()


def _prepared_runner(tmp_path, *, count: int = 1):
    db_path = (tmp_path / "targeted-pipeline.sqlite3").resolve()
    connection = initialize_database(db_path)
    candidate_ids: list[str] = []
    for index, code in enumerate(STOCK_CODES[:count], start=1):
        candidate_id = f"CAND-{TRADE_DATE}-{code}-{index}"
        _insert_strategy_fixture(
            connection,
            candidate_id=candidate_id,
            trade_date=TRADE_DATE,
            code=code,
            name=f"종목-{index}",
        )
        _insert_active_source(
            connection,
            candidate_id=candidate_id,
            code=code,
            name=f"종목-{index}",
        )
        candidate_ids.append(candidate_id)
    connection.commit()

    settings = Settings(
        trading_db_path=db_path,
        incremental_evaluation_worker_enabled=False,
        live_sim_operating_cycle_enabled=False,
        live_sim_lifecycle_inline_fallback_enabled=False,
        entry_timing_write_order_plan_drafts=False,
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
        candidate_source_stale_sec=999_999_999,
        candidate_tick_stale_sec=999_999_999,
        candidate_episode_ttl_sec=999_999_999,
        strategy_engine_stale_tick_sec=999_999_999,
        risk_gate_stale_tick_sec=999_999_999,
        risk_gate_strategy_stale_sec=999_999_999,
        entry_timing_stale_max_seconds=999_999_999,
        entry_timing_min_turnover_krw=1_000,
        entry_timing_min_execution_strength=1,
    )
    env_path = tmp_path / "fast0r3-safe.env"
    env_path.write_text(
        "\n".join(
            [
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                f"TRADING_DB_PATH={db_path}",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS=false",
                "STRATEGY_ENGINE_ENABLED=true",
                "STRATEGY_ENGINE_OBSERVE_ONLY=true",
                "RISK_GATE_ENABLED=true",
                "RISK_GATE_OBSERVE_ONLY=true",
                "ENTRY_TIMING_ENABLED=true",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *[
                    f"{field_name.upper()}=false"
                    for field_name in targeted._COMMAND_PRODUCER_SETTINGS
                ],
                "MARKET_DATA_TICK_STALE_SEC=999999999",
                "MARKET_DATA_DEGRADED_TICK_STALE_SEC=999999999",
                "CANDIDATE_SOURCE_STALE_SEC=999999999",
                "CANDIDATE_TICK_STALE_SEC=999999999",
                "CANDIDATE_EPISODE_TTL_SEC=999999999",
                "STRATEGY_ENGINE_STALE_TICK_SEC=999999999",
                "RISK_GATE_STALE_TICK_SEC=999999999",
                "RISK_GATE_STRATEGY_STALE_SEC=999999999",
                "ENTRY_TIMING_STALE_MAX_SECONDS=999999999",
                "ENTRY_TIMING_MIN_TURNOVER_KRW=1000",
                "ENTRY_TIMING_MIN_EXECUTION_STRENGTH=1",
            ]
        ),
        encoding="utf-8",
    )
    return connection, settings, {"TRADING_ENV_FILE": str(env_path)}, candidate_ids


def _insert_active_source(
    connection,
    *,
    candidate_id: str,
    code: str,
    name: str,
) -> None:
    now = datetime_to_wire(utc_now())
    event_id = f"evt-{code}"
    source_id = f"theme-{code}"
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id,
            candidate_instance_id,
            trade_date,
            code,
            name,
            source_type,
            source_id,
            action,
            event_ts,
            observed_at,
            active,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, 'THEME_LEADER', ?, 'UPSERT', ?, ?, 1, '{}')
        """,
        (event_id, candidate_id, TRADE_DATE, code, name, source_id, now, now),
    )
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date,
            code,
            source_type,
            source_id,
            candidate_instance_id,
            name,
            active,
            first_seen_at,
            last_seen_at,
            last_event_id,
            payload_json
        )
        VALUES (?, ?, 'THEME_LEADER', ?, ?, ?, 1, ?, ?, ?, '{}')
        """,
        (
            TRADE_DATE,
            code,
            source_id,
            candidate_id,
            name,
            now,
            now,
            event_id,
        ),
    )


def _count(connection, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
