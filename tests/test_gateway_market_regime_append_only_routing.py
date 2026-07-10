from __future__ import annotations

import json

import api.routes.gateway as gateway_routes
from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_market_index_tick_event
from services.config import TradingMode, clear_settings_cache
from services.runtime.gateway_market_regime_routing import (
    MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_REASON,
    decide_market_regime_append_only_routing,
    get_latest_market_regime_append_only_routing_status,
    list_market_regime_append_only_routing_decisions,
)
from services.runtime.market_regime_projection_reconcile import (
    run_market_regime_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.sqlite import initialize_database, open_connection
from tests.support_market_regime_projection import (
    market_regime_settings,
    seed_index_event,
    seed_ready_context,
)


def test_market_regime_routing_dry_run_would_skip_but_keeps_inline(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-routing.sqlite3")
    settings = market_regime_settings(
        gateway_market_regime_append_only_dry_run_enabled=True,
    )
    event = _prepare_current_event(connection, settings=settings, prefix="dry_run")

    first = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=settings,
    )
    status = get_latest_market_regime_append_only_routing_status(
        connection,
        settings=settings,
    )
    decisions = list_market_regime_append_only_routing_decisions(connection)
    connection.close()

    assert first.would_skip_inline is True
    assert first.effective_skip_inline is False
    assert "DRY_RUN_WOULD_SKIP_INLINE" in first.blocked_reason_codes
    assert "MARKET_REGIME_CUTOVER_DISABLED" in first.blocked_reason_codes
    assert len(decisions) == 1
    assert status["status"] == "WARN"
    assert status["would_skip_inline_count"] == 1
    assert status["effective_skip_inline_count"] == 0


def test_market_regime_limited_cutover_is_budgeted_idempotent_and_worker_closed(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-regime-cutover.sqlite3")
    settings = _cutover_settings()
    first_event = _prepare_current_event(
        connection,
        settings=settings,
        prefix="cutover",
    )

    first = decide_market_regime_append_only_routing(
        connection,
        first_event,
        settings=settings,
    )
    repeated = decide_market_regime_append_only_routing(
        connection,
        first_event,
        settings=settings,
    )
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    reconcile = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=20,
        persist=True,
    )
    second_event = seed_index_event(
        connection,
        "KOSDAQ",
        "evt_regime_cutover_second",
        settings=settings,
    )
    _seed_ready_index_routing(connection, second_event.event_id)
    second = decide_market_regime_append_only_routing(
        connection,
        second_event,
        settings=settings,
    )
    status = get_latest_market_regime_append_only_routing_status(
        connection,
        settings=settings,
    )
    budget = connection.execute(
        "SELECT used_count FROM market_regime_append_only_budget_state"
    ).fetchone()
    context_count = connection.execute(
        """
        SELECT COUNT(DISTINCT market) AS count
        FROM market_context_snapshots
        WHERE source_event_id = ?
        """,
        (first_event.event_id,),
    ).fetchone()["count"]
    connection.close()

    assert first.would_skip_inline is True
    assert first.effective_skip_inline is True
    assert first.controller_status == "PASS"
    assert repeated.effective_skip_inline is True
    assert repeated.skip_budget_used == 1
    assert worker.applied_by_worker_count == 1
    assert set(worker.mutated_projection_names) == {"market_context", "market_regime"}
    assert reconcile.status == "PASS"
    assert context_count == 2
    assert second.would_skip_inline is True
    assert second.effective_skip_inline is False
    assert "MARKET_REGIME_SKIP_BUDGET_EXHAUSTED" in second.blocked_reason_codes
    assert budget["used_count"] == 1
    assert status["controller_status"] == "PASS"
    assert status["effective_skip_inline_count"] == 1
    assert status["effective_skip_health"]["pending_worker_count"] == 0


def test_market_regime_cutover_rolls_back_while_prior_skip_is_pending(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-rollback.sqlite3")
    settings = _cutover_settings()
    first_event = _prepare_current_event(
        connection,
        settings=settings,
        prefix="rollback",
    )
    first = decide_market_regime_append_only_routing(
        connection,
        first_event,
        settings=settings,
    )
    second_event = seed_index_event(
        connection,
        "KOSDAQ",
        "evt_regime_rollback_second",
        settings=settings,
    )
    _seed_ready_index_routing(connection, second_event.event_id)

    second = decide_market_regime_append_only_routing(
        connection,
        second_event,
        settings=settings,
    )
    connection.close()

    assert first.effective_skip_inline is True
    assert second.effective_skip_inline is False
    assert second.rollback_required is True
    assert "MARKET_REGIME_INLINE_ROLLBACK_REQUIRED" in second.blocked_reason_codes
    assert "MARKET_REGIME_EFFECTIVE_SKIP_PENDING_WORKER" in (second.blocked_reason_codes)


def test_market_regime_cutover_legacy_guard_blocks_effective_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-legacy-guard.sqlite3")
    settings = _cutover_settings(
        gateway_market_regime_append_only_effective_skip_disabled_in_pr18=True
    )
    event = _prepare_current_event(connection, settings=settings, prefix="legacy")

    decision = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_REASON in (decision.blocked_reason_codes)


def test_market_regime_routing_is_fail_closed_when_disabled_or_unsafe(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-routing-closed.sqlite3")
    ready_settings = market_regime_settings()
    event = _prepare_current_event(
        connection,
        settings=ready_settings,
        prefix="closed",
    )

    disabled = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=ready_settings,
    )
    unsafe = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=market_regime_settings(
            trading_mode=TradingMode.LIVE_SIM,
            gateway_market_regime_append_only_dry_run_enabled=True,
        ),
    )
    connection.close()

    assert disabled.would_skip_inline is False
    assert "DRY_RUN_DISABLED" in disabled.blocked_reason_codes
    assert unsafe.would_skip_inline is False
    assert "MARKET_REGIME_CORE_NOT_OBSERVE_SAFE" in unsafe.blocked_reason_codes
    assert disabled.effective_skip_inline is False
    assert unsafe.effective_skip_inline is False


def test_gateway_market_regime_effective_skip_defers_context_to_worker(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-regime-gateway-cutover.sqlite3"
    connection = initialize_database(db_path)
    settings = _cutover_settings()
    seed_ready_context(connection, settings=settings, prefix="evt_gateway_cutover")
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'APPLIED', processed_at = updated_at
        WHERE projection_name = 'market_regime'
        """
    )
    connection.commit()
    reconcile = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=20,
        persist=True,
    )
    assert reconcile.status == "PASS"
    event = make_market_index_tick_event(
        source="test-gateway",
        index_code="KOSPI",
        price=2802.0,
        metadata={"parser_status": "VERIFIED", "projection_source": "REALTIME"},
    )
    _seed_ready_index_routing(connection, event.event_id)
    before_context_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_context_snapshots"
    ).fetchone()["count"]
    connection.close()

    env = {
        "TRADING_DB_PATH": str(db_path),
        "TRADING_PROFILE": "OBSERVE",
        "TRADING_MODE": "OBSERVE",
        "TRADING_ALLOW_LIVE_SIM": "false",
        "TRADING_ALLOW_LIVE_REAL": "false",
        "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED": "true",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED": "true",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC": "0",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED": "true",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "1",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18": ("false"),
        "MARKET_INDEX_STALE_SEC": "3600",
        "MARKET_CONTEXT_SNAPSHOT_STALE_SEC": "3600",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()
    monkeypatch.setattr(
        gateway_routes,
        "_decide_market_index_append_only_routing",
        lambda *args, **kwargs: None,
    )

    with TestClient(app) as client:
        posted = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        worker = client.post(
            "/api/operator/projection-outbox/run-once?"
            "projection_name=market_regime&limit=1&"
            "apply_projection=true&live_safe=true",
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    after_context_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_context_snapshots"
    ).fetchone()["count"]
    latest_sources = {
        row["source_event_id"]
        for row in connection.execute(
            """
            SELECT snapshot.source_event_id
            FROM market_context_latest AS latest
            JOIN market_context_snapshots AS snapshot
              ON snapshot.snapshot_id = latest.snapshot_id
            """
        ).fetchall()
    }
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert posted.status_code == 200
    payload = posted.json()
    assert payload["market_regime_append_only_routing"]["effective_skip_inline"] is True
    assert payload["projection_statuses"]["market_regime"] == (
        "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_MARKET_REGIME"
    )
    assert payload["projection_statuses"]["market_context"] == (
        "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_MARKET_REGIME"
    )
    assert worker.status_code == 200
    assert worker.json()["applied_by_worker_count"] == 1
    assert after_context_count == before_context_count + 2
    assert latest_sources == {event.event_id}
    assert command_count == 0


def _prepare_current_event(connection, *, settings, prefix: str):
    seed_ready_context(connection, settings=settings, prefix=f"evt_regime_{prefix}")
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'APPLIED', processed_at = updated_at
        WHERE projection_name = 'market_regime'
        """
    )
    connection.commit()
    reconcile = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=20,
        persist=True,
    )
    assert reconcile.status == "PASS"
    current = seed_index_event(
        connection,
        "KOSPI",
        f"evt_regime_{prefix}_current",
        settings=settings,
    )
    _seed_ready_index_routing(connection, current.event_id)
    return current


def _seed_ready_index_routing(connection, event_id: str) -> None:
    evidence = {
        "observe_safe": True,
        "event_age_sec": 0.1,
        "event_future_skew_sec": 0.0,
        "event_market_session": "REGULAR",
        "event_market_weekday": True,
        "max_event_age_sec": 30,
        "max_future_skew_sec": 5,
        "gateway_health_required": True,
        "gateway_health": {"ready": True},
    }
    connection.execute(
        """
        INSERT INTO market_index_projection_routing_decisions (
            event_id, event_type, parser_status, parser_verified,
            data_source, data_usable, evidence_json, decided_at
        ) VALUES (?, 'market_index_tick', 'VERIFIED', 1, 'REALTIME', 1, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            parser_status = excluded.parser_status,
            parser_verified = excluded.parser_verified,
            data_source = excluded.data_source,
            data_usable = excluded.data_usable,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            event_id,
            json.dumps(evidence, sort_keys=True),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()


def _cutover_settings(**overrides):
    values = {
        "gateway_market_regime_append_only_dry_run_enabled": True,
        "gateway_market_regime_append_only_cutover_enabled": True,
        "gateway_market_regime_append_only_global_kill_switch": False,
        "gateway_market_regime_append_only_effective_skip_disabled_in_pr18": False,
        "gateway_market_regime_append_only_max_skip_per_minute": 1,
    }
    values.update(overrides)
    return market_regime_settings(**values)
