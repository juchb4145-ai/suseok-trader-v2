from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from services.config import Settings, clear_settings_cache
from services.market_reference_service import process_market_symbols_event
from services.runtime.gateway_market_reference_routing import (
    MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON,
    decide_market_reference_append_only_routing,
    get_latest_market_reference_append_only_routing_status,
)
from services.runtime.market_reference_projection_reconcile import (
    run_market_reference_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_reference_dry_run_keeps_inline_fallback_without_cutover(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-reference-routing.sqlite3")
    settings = _settings(gateway_market_reference_append_only_dry_run_enabled=True)
    _seed_ready_reference(connection, settings=settings)
    event = _append_pending_event(connection, "evt_ref_dry_run", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_CUTOVER_DISABLED" in decision.blocked_reason_codes


def test_market_reference_global_kill_switch_forces_inline_fallback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-kill.sqlite3")
    settings = _active_settings(
        gateway_market_reference_append_only_global_kill_switch=True
    )
    _seed_ready_reference(connection, settings=settings)
    event = _append_pending_event(connection, "evt_ref_kill", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_GLOBAL_KILL_SWITCH" in decision.blocked_reason_codes


def test_market_reference_legacy_guard_remains_emergency_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-legacy-guard.sqlite3")
    settings = _active_settings(
        gateway_market_reference_append_only_effective_skip_disabled_in_pr13=True
    )
    _seed_ready_reference(connection, settings=settings)
    event = _append_pending_event(connection, "evt_ref_legacy_guard", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON in (
        decision.blocked_reason_codes
    )


def test_market_reference_worker_apply_is_required_for_effective_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-gate.sqlite3")
    settings = _active_settings(
        projection_outbox_apply_projection_enabled=False,
        projection_outbox_market_reference_apply_enabled=False,
    )
    _seed_ready_reference(connection, settings=settings)
    event = _append_pending_event(connection, "evt_ref_worker_gate", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_WORKER_APPLY_NOT_ENABLED" in (
        decision.blocked_reason_codes
    )


def test_market_reference_stale_reconcile_forces_inline_fallback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-stale.sqlite3")
    settings = _active_settings()
    _seed_ready_reference(connection, settings=settings)
    connection.execute(
        "UPDATE market_reference_projection_reconcile_runs SET created_at = ?",
        ("2000-01-01T00:00:00Z",),
    )
    connection.commit()
    event = _append_pending_event(connection, "evt_ref_stale", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_RECONCILE_STALE" in decision.blocked_reason_codes


def test_market_reference_membership_threshold_forces_inline_fallback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-membership.sqlite3")
    seed_settings = _active_settings()
    _seed_ready_reference(connection, settings=seed_settings)
    settings = _active_settings(
        gateway_market_reference_append_only_min_membership_count=2
    )
    event = _append_pending_event(connection, "evt_ref_membership", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_MEMBERSHIP_COUNT_BELOW_MIN" in (
        decision.blocked_reason_codes
    )


def test_market_reference_limited_cutover_applies_one_event_and_exhausts_budget(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-reference-limited.sqlite3")
    settings = _active_settings()
    _seed_ready_reference(connection, settings=settings)
    first = _append_pending_event(connection, "evt_ref_limited_1", "000660")

    first_decision = decide_market_reference_append_only_routing(
        connection,
        first,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    assert first_decision.effective_skip_inline is True
    assert first_decision.skip_budget_used == 1

    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        live_safe=True,
        projection_name="market_reference",
    )
    reconcile = run_market_reference_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )
    status = get_latest_market_reference_append_only_routing_status(
        connection,
        settings=settings,
    )
    first_membership_count = _count_event_memberships(connection, first.event_id)
    second = _append_pending_event(connection, "evt_ref_limited_2", "000660")
    second_decision = decide_market_reference_append_only_routing(
        connection,
        second,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    process_market_symbols_event(connection, second)
    status_after_inline_supersede = (
        get_latest_market_reference_append_only_routing_status(
            connection,
            settings=settings,
        )
    )
    connection.close()

    assert worker.applied_by_worker_count == 1
    assert reconcile.status == "PASS"
    assert first_membership_count == 1
    assert status["status"] == "PASS"
    assert status["rollback_required"] is False
    assert status["skip_budget_used_current_minute"] == 1
    assert second_decision.effective_skip_inline is False
    assert "MARKET_REFERENCE_SKIP_BUDGET_EXHAUSTED" in (
        second_decision.blocked_reason_codes
    )
    assert status_after_inline_supersede["status"] == "PASS"
    assert status_after_inline_supersede["effective_skip_health"][
        "artifact_missing_count"
    ] == 0


def test_market_reference_outbox_error_triggers_inline_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-rollback.sqlite3")
    settings = _active_settings()
    _seed_ready_reference(connection, settings=settings)
    failed = _append_pending_event(connection, "evt_ref_failed", "051910")
    _mark_outbox(connection, failed.event_id, "ERROR")
    event = _append_pending_event(connection, "evt_ref_rollback", "000660")

    decision = decide_market_reference_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert decision.rollback_required is True
    assert "MARKET_REFERENCE_OUTBOX_ERROR" in decision.rollback_reason_codes
    assert "MARKET_REFERENCE_INLINE_ROLLBACK_REQUIRED" in (
        decision.blocked_reason_codes
    )


def test_gateway_market_symbols_limited_cutover_then_worker_apply(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-reference-gateway.sqlite3"
    settings = _active_settings()
    connection = initialize_database(db_path)
    _seed_ready_reference(connection, settings=settings)
    connection.close()
    _configure_active_env(monkeypatch, tmp_path, db_path)
    body = _event("evt_ref_gateway", "000660").to_dict()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=body,
                headers={"X-Local-Token": "test-token"},
            )
            worker = client.post(
                (
                    "/api/operator/projection-outbox/run-once"
                    "?projection_name=market_reference&limit=1"
                    "&apply_projection=true&live_safe=true"
                ),
                headers={"X-Local-Token": "test-token"},
            )
            reconcile = client.post(
                (
                    "/api/operator/market-reference-projection-reconcile/run-once"
                    "?limit=100&persist=true&live_safe=true"
                ),
                headers={"X-Local-Token": "test-token"},
            )
            status = client.get(
                "/api/operator/market-reference-append-only-routing/status"
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    membership_count = _count_event_memberships(connection, "evt_ref_gateway")
    connection.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["projection_statuses"]["market_reference"] == (
        "SKIPPED_INLINE_APPEND_ONLY_MARKET_REFERENCE"
    )
    assert payload["projection_statuses"][
        "market_reference_effective_skip_inline"
    ] == "TRUE"
    assert payload["market_reference_append_only_routing"][
        "effective_skip_inline"
    ] is True
    assert worker.status_code == 200
    assert worker.json()["applied_by_worker_count"] == 1
    assert reconcile.status_code == 200
    assert reconcile.json()["status"] == "PASS"
    assert status.json()["status"] == "PASS"
    assert status.json()["outbox"]["error_count"] == 0
    assert status.json()["outbox"]["dead_letter_count"] == 0
    assert membership_count == 1


def test_gateway_market_symbols_routing_error_falls_back_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-reference-routing-error.sqlite3"
    _configure_active_env(monkeypatch, tmp_path, db_path)

    def fail_routing(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("forced routing failure")

    monkeypatch.setattr(
        "api.routes.gateway.decide_market_reference_append_only_routing",
        fail_routing,
    )
    body = _event("evt_ref_inline_fallback", "005930").to_dict()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=body,
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    membership_count = _count_event_memberships(
        connection,
        "evt_ref_inline_fallback",
    )
    connection.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["projection_statuses"]["market_reference"] == "APPLIED"
    assert payload["projection_statuses"][
        "market_reference_effective_skip_inline"
    ] == "FALSE"
    assert membership_count == 1


def _settings(**overrides) -> Settings:
    values = {
        "gateway_market_reference_append_only_min_membership_count": 1,
        "gateway_market_reference_append_only_reconcile_max_age_sec": 300,
        "projection_outbox_shadow_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_market_reference_apply_min_age_sec": 0,
        "projection_outbox_market_data_apply_enabled": False,
    }
    values.update(overrides)
    return Settings(**values)


def _active_settings(**overrides) -> Settings:
    values = {
        "gateway_market_reference_append_only_dry_run_enabled": True,
        "gateway_market_reference_append_only_cutover_enabled": True,
        "gateway_market_reference_append_only_global_kill_switch": False,
        "gateway_market_reference_append_only_max_skip_per_minute": 1,
        "gateway_market_reference_append_only_max_pending_within_sla": 1,
        "gateway_market_reference_append_only_effective_skip_disabled_in_pr13": False,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_reference_apply_enabled": True,
    }
    values.update(overrides)
    return _settings(**values)


def _seed_ready_reference(connection, *, settings: Settings) -> None:
    seed = _event("evt_ref_seed", "005930")
    append_gateway_event(connection, seed)
    enqueue_projection_jobs_for_gateway_event(connection, seed)
    process_market_symbols_event(connection, seed)
    _mark_outbox(connection, seed.event_id, "APPLIED")
    result = run_market_reference_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )
    assert result.status == "PASS"


def _append_pending_event(connection, event_id: str, code: str) -> GatewayEvent:
    event = _event(event_id, code)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    return event


def _event(event_id: str, code: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {"KOSPI": [{"code": code, "name": code}]}},
        ts=TS,
    )


def _mark_outbox(connection, event_id: str, status: str) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?, processed_at = datetime('now'), updated_at = datetime('now')
        WHERE projection_name = 'market_reference' AND event_id = ?
        """,
        (status, event_id),
    )
    connection.commit()


def _count_event_memberships(connection, event_id: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_symbol_memberships WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return int(row["count"])


def _configure_active_env(monkeypatch, tmp_path, db_path) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED", "true")
    monkeypatch.setenv(
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH",
        "false",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
        "1",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
        "1",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT",
        "1",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13",
        "false",
    )
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC", "0")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED", "false")
    monkeypatch.setenv("PROJECTION_OUTBOX_WORKER_ENABLED", "false")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    clear_settings_cache()
