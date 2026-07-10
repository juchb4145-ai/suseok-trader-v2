from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.utils import datetime_to_wire
from fastapi.testclient import TestClient
from services.config import Settings, TradingMode, TradingProfile
from services.market_index_service import process_market_index_event
from services.market_regime_service import rebuild_market_regime_snapshot
from services.runtime import projection_outbox_worker
from services.runtime.gateway_market_index_routing import (
    decide_market_index_append_only_routing,
    get_latest_market_index_append_only_routing_status,
)
from services.runtime.market_index_projection_reconcile import (
    run_market_index_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 7, 10, 4, 20, tzinfo=UTC)


def test_market_index_limited_cutover_applies_index_and_regime_by_worker(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-cutover.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    event = _event("evt_index_cutover", "KOSPI", 2805.0, TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    sample_before = _event_count(connection, "market_index_tick_samples", event.event_id)
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )
    sample_after = _event_count(connection, "market_index_tick_samples", event.event_id)
    index_job = _outbox(connection, "market_index", event.event_id)
    regime_job = _outbox(connection, "market_regime", event.event_id)
    regime_snapshot = connection.execute(
        """
        SELECT snapshot_id, evidence_json
        FROM market_regime_snapshots
        WHERE source_event_id = ?
        LIMIT 1
        """,
        (event.event_id,),
    ).fetchone()
    status = get_latest_market_index_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is True
    assert decision.skip_budget_limit == 1
    assert decision.skip_budget_used == 1
    assert sample_before == 0
    assert sample_after == 1
    assert worker.applied_by_worker_count == 1
    assert set(worker.mutated_projection_names) == {
        "market_context",
        "market_index",
        "market_regime",
    }
    assert index_job["status"] == "APPLIED"
    assert regime_job["status"] == "APPLIED"
    assert regime_snapshot is not None
    evidence = json.loads(regime_snapshot["evidence_json"])
    assert evidence["source_event_id"] == event.event_id
    assert evidence["source_projection"] == "market_index"
    assert status["status"] == "PASS"
    assert status["rollback_required"] is False
    assert status["effective_skip_health"]["regime_snapshot_missing_count"] == 0


def test_market_index_worker_can_finish_before_routing_without_losing_continuity(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-index-worker-race.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    event = _event("evt_index_worker_race", "KOSPI", 2805.5, TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )
    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    linked_regime_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count FROM market_regime_snapshots
            WHERE source_event_id = ?
            """,
            (event.event_id,),
        ).fetchone()["count"]
    )
    connection.close()

    assert worker.applied_count == 2
    assert linked_regime_count == 1
    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is True
    assert decision.outbox_status == "APPLIED"
    assert decision.evidence["preapplied_event_continuity"]["ready"] is True


def test_market_index_preapplied_without_regime_continuity_falls_back_inline(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-index-preapplied-gap.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    event = _event("evt_index_preapplied_gap", "KOSPI", 2805.5, TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    worker = process_projection_outbox_batch(
        connection,
        settings=_cutover_settings(
            gateway_market_index_append_only_cutover_enabled=False
        ),
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert worker.applied_by_worker_count == 1
    assert decision.outbox_status == "APPLIED"
    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert "MARKET_INDEX_PREAPPLIED_CONTINUITY_NOT_READY" in (
        decision.blocked_reason_codes
    )


def test_market_index_budget_one_forces_second_event_inline_fallback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-budget.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    first = _event("evt_index_budget_first", "KOSPI", 2805.0, TS + timedelta(seconds=10))
    append_gateway_event(connection, first)
    enqueue_projection_jobs_for_gateway_event(connection, first)
    first_decision = decide_market_index_append_only_routing(
        connection,
        first,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )

    second = _event(
        "evt_index_budget_second",
        "KOSDAQ",
        905.0,
        TS + timedelta(seconds=20),
    )
    append_gateway_event(connection, second)
    enqueue_projection_jobs_for_gateway_event(connection, second)
    second_decision = decide_market_index_append_only_routing(
        connection,
        second,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert first_decision.effective_skip_inline is True
    assert second_decision.would_skip_inline is True
    assert second_decision.effective_skip_inline is False
    assert second_decision.skip_budget_used == 1
    assert "MARKET_INDEX_SKIP_BUDGET_EXHAUSTED" in second_decision.blocked_reason_codes


def test_market_index_pending_effective_skip_triggers_inline_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-pending-rollback.sqlite3")
    settings = _cutover_settings(gateway_market_index_append_only_max_skip_per_minute=2)
    _seed_ready(connection, settings)
    first = _event("evt_index_pending_first", "KOSPI", 2805.0, TS + timedelta(seconds=10))
    append_gateway_event(connection, first)
    enqueue_projection_jobs_for_gateway_event(connection, first)
    first_decision = decide_market_index_append_only_routing(
        connection,
        first,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    second = _event(
        "evt_index_pending_second",
        "KOSDAQ",
        905.0,
        TS + timedelta(seconds=20),
    )
    append_gateway_event(connection, second)
    enqueue_projection_jobs_for_gateway_event(connection, second)
    second_decision = decide_market_index_append_only_routing(
        connection,
        second,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert first_decision.effective_skip_inline is True
    assert second_decision.effective_skip_inline is False
    assert second_decision.rollback_required is True
    assert "MARKET_INDEX_EFFECTIVE_SKIP_PENDING_WORKER" in (
        second_decision.rollback_reason_codes
    )
    assert "MARKET_INDEX_INLINE_ROLLBACK_REQUIRED" in (
        second_decision.blocked_reason_codes
    )


def test_market_index_regime_outbox_error_triggers_inline_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-regime-rollback.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    event = _event("evt_index_regime_rollback", "KOSPI", 2805.0, TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'ERROR', last_error = 'forced regime error'
        WHERE projection_name = 'market_regime' AND event_id = ?
        """,
        (event.event_id,),
    )
    connection.commit()

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert decision.rollback_required is True
    assert decision.regime_continuity_ready is False
    assert "MARKET_REGIME_OUTBOX_ERROR" in decision.rollback_reason_codes


def test_market_index_regime_refresh_failure_is_fail_closed(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-index-regime-failure.sqlite3")
    settings = _cutover_settings(projection_outbox_retry_limit=2)
    worker_settings = _cutover_settings(
        projection_outbox_retry_limit=2,
        gateway_market_index_append_only_fail_closed_on_regime_refresh_error=False,
    )
    _seed_ready(connection, settings)
    event = _event("evt_index_regime_failure", "KOSPI", 2805.0, TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    def fail_regime(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("forced regime refresh failure")

    original_rebuild = projection_outbox_worker.rebuild_market_context_snapshots
    monkeypatch.setattr(
        projection_outbox_worker,
        "rebuild_market_context_snapshots",
        fail_regime,
    )
    worker = process_projection_outbox_batch(
        connection,
        settings=worker_settings,
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    index_job_after_failure = dict(_outbox(connection, "market_index", event.event_id))
    linked_regime_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count FROM market_regime_snapshots
            WHERE source_event_id = ?
            """,
            (event.event_id,),
        ).fetchone()["count"]
    )
    monkeypatch.setattr(
        projection_outbox_worker,
        "rebuild_market_context_snapshots",
        original_rebuild,
    )
    recovery = process_projection_outbox_batch(
        connection,
        settings=worker_settings,
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    regime_verification = process_projection_outbox_batch(
        connection,
        settings=worker_settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    recovered_index_job = dict(_outbox(connection, "market_index", event.event_id))
    recovered_regime_job = dict(_outbox(connection, "market_regime", event.event_id))
    recovered_metadata = json.loads(recovered_index_job["metadata_json"])
    recovered_status = get_latest_market_index_append_only_routing_status(
        connection,
        settings=settings,
    )
    linked_regime_after_recovery = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count FROM market_regime_snapshots
            WHERE source_event_id = ?
            """,
            (event.event_id,),
        ).fetchone()["count"]
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert worker.error_count == 1
    assert worker.projection_apply_error_count == 1
    assert index_job_after_failure["status"] == "PENDING"
    assert index_job_after_failure["attempts"] == 1
    assert linked_regime_count == 0
    assert recovery.applied_by_worker_count == 1
    assert regime_verification.applied_count == 1
    assert recovered_index_job["status"] == "APPLIED"
    assert recovered_regime_job["status"] == "APPLIED"
    assert recovered_metadata["last_worker_evidence"]["apply_result"] == (
        "APPLIED_BY_WORKER"
    )
    assert recovered_metadata["last_worker_evidence"][
        "worker_recovery_verified_existing_artifact"
    ] is True
    assert linked_regime_after_recovery == 1
    assert recovered_status["rollback_required"] is False


def test_market_index_regime_retry_exhaustion_reaches_dead_letter(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-index-regime-dead-letter.sqlite3")
    settings = _cutover_settings(projection_outbox_retry_limit=2)
    _seed_ready(connection, settings)
    event = _event("evt_index_regime_dead_letter", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    def fail_regime(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("forced persistent regime refresh failure")

    monkeypatch.setattr(
        projection_outbox_worker,
        "rebuild_market_context_snapshots",
        fail_regime,
    )
    first = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )
    first_job = dict(_outbox(connection, "market_index", event.event_id))
    first_regime_job = dict(_outbox(connection, "market_regime", event.event_id))
    second = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )
    second_job = dict(_outbox(connection, "market_index", event.event_id))
    second_regime_job = dict(_outbox(connection, "market_regime", event.event_id))
    connection.close()

    assert decision.effective_skip_inline is True
    assert first.error_count == 2
    assert first_job["status"] == "PENDING"
    assert first_regime_job["status"] == "PENDING"
    assert second.dead_letter_count == 2
    assert second_job["status"] == "DEAD_LETTER"
    assert second_regime_job["status"] == "DEAD_LETTER"
    assert second_job["attempts"] == 2
    assert second_regime_job["attempts"] == 2


def test_market_index_kill_switch_blocks_effective_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-kill-switch.sqlite3")
    settings = _cutover_settings(
        gateway_market_index_append_only_global_kill_switch=True
    )
    _seed_ready(connection, settings)
    event = _event("evt_index_kill_switch", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "MARKET_INDEX_GLOBAL_KILL_SWITCH" in decision.blocked_reason_codes


@pytest.mark.parametrize(
    "unsafe_overrides",
    [
        {"trading_profile": TradingProfile.LIVE_SIM_PILOT},
        {"trading_mode": TradingMode.LIVE_SIM},
        {"trading_allow_live_sim": True},
        {"trading_allow_live_real": True},
    ],
)
def test_market_index_cutover_requires_observe_safe_core(
    tmp_path,
    unsafe_overrides: dict[str, object],
) -> None:
    connection = initialize_database(tmp_path / "market-index-observe-gate.sqlite3")
    _seed_ready(connection, _cutover_settings())
    event = _event("evt_index_observe_gate", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    settings = _cutover_settings(**unsafe_overrides)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.observe_safe is False
    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert decision.controller_status == "FAIL"
    assert "MARKET_INDEX_CORE_NOT_OBSERVE_SAFE" in decision.blocked_reason_codes


@pytest.mark.parametrize(
    ("case_name", "reason_code"),
    [
        ("stale", "MARKET_INDEX_EVENT_STALE"),
        ("future", "MARKET_INDEX_EVENT_FUTURE_SKEW"),
        ("weekend", "MARKET_INDEX_EVENT_NON_TRADING_DAY"),
        ("off_hours", "MARKET_INDEX_EVENT_OUTSIDE_KRX_SESSION"),
    ],
)
def test_market_index_cutover_requires_current_krx_session_event(
    tmp_path,
    case_name: str,
    reason_code: str,
) -> None:
    connection = initialize_database(tmp_path / f"market-index-time-{case_name}.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    event_settings = settings
    envelope_ts: datetime | None = None
    if case_name == "stale":
        event_ts = datetime.now(UTC) - timedelta(seconds=60)
        event_settings = _cutover_settings(
            gateway_market_index_append_only_max_event_age_sec=1
        )
    elif case_name == "future":
        event_ts = datetime.now(UTC) + timedelta(seconds=60)
        event_settings = _cutover_settings(
            gateway_market_index_append_only_max_future_skew_sec=1
        )
    elif case_name == "weekend":
        event_ts = datetime(2026, 7, 11, 4, 0, tzinfo=UTC)
    else:
        event_ts = datetime(2026, 7, 9, 16, 0, tzinfo=UTC)
        envelope_ts = TS
    event = _event(
        f"evt_index_time_{case_name}",
        "KOSPI",
        2805.0,
        event_ts,
        envelope_ts=envelope_ts,
    )
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=event_settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert reason_code in decision.blocked_reason_codes


@pytest.mark.parametrize(
    ("case_name", "health_reason"),
    [
        ("missing", "MARKET_INDEX_GATEWAY_HEARTBEAT_MISSING"),
        ("stale", "MARKET_INDEX_GATEWAY_HEARTBEAT_STALE"),
        ("future", "MARKET_INDEX_GATEWAY_HEARTBEAT_FUTURE_SKEW"),
        ("parse_error", "MARKET_INDEX_GATEWAY_ADAPTER_NOT_CALLBACK_ACTIVE"),
        ("realtime_disabled", "MARKET_INDEX_GATEWAY_REALTIME_DISABLED"),
    ],
)
def test_market_index_cutover_requires_fresh_gateway_adapter_health(
    tmp_path,
    case_name: str,
    health_reason: str,
) -> None:
    connection = initialize_database(tmp_path / f"market-index-health-{case_name}.sqlite3")
    settings = _cutover_settings()
    _seed_ready(connection, settings)
    if case_name == "missing":
        connection.execute(
            "DELETE FROM gateway_status WHERE key = 'last_heartbeat_at'"
        )
    elif case_name == "stale":
        stale_at = datetime_to_wire(datetime.now(UTC) - timedelta(seconds=60))
        connection.execute(
            "UPDATE gateway_status SET value = ? WHERE key = 'last_heartbeat_at'",
            (stale_at,),
        )
        settings = _cutover_settings(
            gateway_market_index_append_only_gateway_health_max_age_sec=1
        )
    elif case_name == "future":
        future_at = datetime_to_wire(datetime.now(UTC) + timedelta(seconds=60))
        connection.execute(
            "UPDATE gateway_status SET value = ? WHERE key = 'last_heartbeat_at'",
            (future_at,),
        )
        settings = _cutover_settings(
            gateway_market_index_append_only_max_future_skew_sec=1
        )
    elif case_name == "parse_error":
        connection.execute(
            """
            UPDATE gateway_status SET value = 'PARSE_ERROR'
            WHERE key = 'market_index_adapter_health'
            """
        )
    else:
        connection.execute(
            """
            UPDATE gateway_status SET value = 'false'
            WHERE key = 'market_index_realtime_enabled'
            """
        )
    connection.commit()
    event = _event(f"evt_index_health_{case_name}", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.gateway_health_ready is False
    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert "MARKET_INDEX_GATEWAY_HEALTH_NOT_READY" in decision.blocked_reason_codes
    assert health_reason in decision.evidence["gateway_health"]["reason_codes"]


def test_market_index_gateway_health_guard_cannot_be_disabled(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-health-guard.sqlite3")
    _seed_ready(connection, _cutover_settings())
    event = _event("evt_index_health_guard", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    settings = _cutover_settings(
        gateway_market_index_append_only_require_fresh_gateway_health=False
    )

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.gateway_health_required is False
    assert decision.would_skip_inline is False
    assert "MARKET_INDEX_GATEWAY_HEALTH_GUARD_DISABLED" in (
        decision.blocked_reason_codes
    )


@pytest.mark.parametrize(
    ("setting_name", "reason_code"),
    [
        (
            "gateway_market_index_append_only_require_reconcile_pass",
            "MARKET_INDEX_RECONCILE_GUARD_DISABLED",
        ),
        (
            "gateway_market_index_append_only_require_data_usable",
            "MARKET_INDEX_DATA_USABILITY_GUARD_DISABLED",
        ),
        (
            "gateway_market_index_append_only_require_parser_verified",
            "MARKET_INDEX_PARSER_VERIFICATION_GUARD_DISABLED",
        ),
        (
            "gateway_market_index_append_only_fail_closed_on_regime_refresh_error",
            "MARKET_INDEX_REGIME_REFRESH_FAIL_CLOSED_DISABLED",
        ),
    ],
)
def test_market_index_required_guard_cannot_be_disabled_during_cutover(
    tmp_path,
    setting_name: str,
    reason_code: str,
) -> None:
    connection = initialize_database(tmp_path / f"market-index-guard-{setting_name}.sqlite3")
    _seed_ready(connection, _cutover_settings())
    event = _event(f"evt_index_guard_{setting_name}", "KOSPI", 2805.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    settings = _cutover_settings(**{setting_name: False})

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert decision.controller_status == "FAIL"
    assert reason_code in decision.blocked_reason_codes


def test_market_index_gateway_api_limited_skip_closes_worker_regime(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-index-cutover-api.sqlite3"
    settings = _cutover_settings()
    connection = initialize_database(db_path)
    _seed_ready(connection, settings)
    before_commands = _count(connection, "gateway_commands")
    connection.close()
    safe_env = tmp_path / "missing-safe.env"
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH", "false")
    monkeypatch.setenv(
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15",
        "false",
    )
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE", "1")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC", "999999999")
    monkeypatch.setenv(
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC",
        "999999999",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC",
        "999999999",
    )
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC", "0")
    event = _event(
        "evt_index_cutover_api",
        "KOSPI",
        2806.0,
        TS + timedelta(seconds=30),
    )

    with TestClient(app) as client:
        posted = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        worker = client.post(
            "/api/operator/projection-outbox/run-once"
            "?limit=2&apply_projection=true&live_safe=true",
            headers={"X-Local-Token": "test-token"},
        )
        routing = client.get(
            "/api/operator/market-index-append-only-routing/status"
        )

    connection = open_connection(db_path)
    index_job = _outbox(connection, "market_index", event.event_id)
    regime_job = _outbox(connection, "market_regime", event.event_id)
    linked_regime = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count FROM market_regime_snapshots
            WHERE source_event_id = ?
            """,
            (event.event_id,),
        ).fetchone()["count"]
    )
    after_commands = _count(connection, "gateway_commands")
    connection.close()

    assert posted.status_code == 200
    posted_payload = posted.json()
    assert posted_payload["projection_statuses"]["market_index"] == (
        "SKIPPED_INLINE_APPEND_ONLY_MARKET_INDEX"
    )
    assert posted_payload["market_index_append_only_routing"][
        "effective_skip_inline"
    ] is True
    assert worker.status_code == 200
    assert set(worker.json()["mutated_projection_names"]) == {
        "market_context",
        "market_index",
        "market_regime",
    }
    assert index_job["status"] == "APPLIED"
    assert regime_job["status"] == "APPLIED"
    assert linked_regime == 1
    assert routing.json()["rollback_required"] is False
    assert after_commands == before_commands


def _seed_ready(connection, settings: Settings) -> None:
    for index, code in enumerate(("KOSPI", "KOSDAQ")):
        event = _event(
            f"evt_index_cutover_seed_{code.lower()}",
            code,
            2800.0 if code == "KOSPI" else 900.0,
            TS + timedelta(seconds=index),
        )
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
        assert process_market_index_event(connection, event, settings=settings).status == "APPLIED"
        process_projection_outbox_batch(
            connection,
            settings=Settings(projection_outbox_shadow_min_age_sec=0),
            limit=1,
            projection_name="market_index",
        )
    _seed_gateway_health(connection, TS + timedelta(seconds=2))
    rebuild_market_regime_snapshot(connection, settings=settings)
    process_projection_outbox_batch(
        connection,
        settings=Settings(projection_outbox_shadow_min_age_sec=0),
        limit=2,
        projection_name="market_regime",
    )
    reconcile = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=True,
    )
    assert reconcile.status == "PASS"
    assert reconcile.append_only_ready is True


def _cutover_settings(**overrides) -> Settings:
    values = {
        "gateway_market_index_append_only_dry_run_enabled": True,
        "gateway_market_index_append_only_cutover_enabled": True,
        "gateway_market_index_append_only_global_kill_switch": False,
        "gateway_market_index_append_only_effective_skip_disabled_in_pr15": False,
        "gateway_market_index_append_only_max_skip_per_minute": 1,
        "gateway_market_index_append_only_max_pending_within_sla": 1,
        "gateway_market_index_append_only_require_reconcile_pass": True,
        "gateway_market_index_append_only_require_data_usable": True,
        "gateway_market_index_append_only_require_parser_verified": True,
        "gateway_market_index_append_only_require_worker_regime_refresh": True,
        "gateway_market_index_append_only_fail_closed_on_regime_refresh_error": True,
        "gateway_market_index_append_only_max_event_age_sec": 999_999_999,
        "gateway_market_index_append_only_max_future_skew_sec": 999_999_999,
        "gateway_market_index_append_only_gateway_health_max_age_sec": 999_999_999,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": False,
        "projection_outbox_market_reference_apply_enabled": False,
        "projection_outbox_market_index_apply_enabled": True,
        "projection_outbox_market_index_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
        "market_index_stale_sec": 999_999_999,
        "market_regime_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _seed_gateway_health(connection, ts: datetime) -> None:
    append_gateway_event(
        connection,
        GatewayEvent(
            event_id=f"evt_gateway_health_{int(ts.timestamp())}",
            event_type="heartbeat",
            source="test-gateway",
            ts=ts,
            payload={
                "market_index_realtime_enabled": True,
                "market_index_adapter_health": "CALLBACK_ACTIVE",
                "parsed_market_index_tick_count": 2,
                "latest_market_index_tick_at": datetime_to_wire(ts),
            },
        ),
    )


def _event(
    event_id: str,
    index_code: str,
    price: float,
    ts: datetime = TS,
    *,
    envelope_ts: datetime | None = None,
) -> GatewayEvent:
    tick = BrokerMarketIndexTick(
        index_code=index_code,
        index_name=index_code,
        price=price,
        change_rate=0.2,
        change_value=5.0,
        trade_time=ts,
        ts=ts,
        metadata={
            "parser_status": "VERIFIED",
            "source": "KIWOOM_REALTIME_MARKET_INDEX",
        },
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=envelope_ts or ts,
    )


def _outbox(connection, projection_name: str, event_id: str):
    return connection.execute(
        """
        SELECT * FROM projection_outbox
        WHERE projection_name = ? AND event_id = ?
        """,
        (projection_name, event_id),
    ).fetchone()


def _event_count(connection, table_name: str, event_id: str) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return int(row["count"])


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
