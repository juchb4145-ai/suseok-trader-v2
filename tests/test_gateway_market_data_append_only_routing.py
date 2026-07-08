from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import (
    make_condition_event,
    make_heartbeat_event,
    make_price_tick_event,
    make_tr_response_event,
)
from services.config import Settings, clear_settings_cache
from services.dashboard_service import build_dashboard_snapshot
from services.market_data_service import process_gateway_event
from services.runtime.gateway_projection_routing import (
    decide_market_data_projection_routing,
    get_latest_market_data_append_only_routing_status,
)
from services.runtime.market_data_projection_reconcile import (
    run_market_data_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection


def test_gateway_market_data_append_only_routing_dry_run_disabled_keeps_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-disabled.sqlite3"
    _configure_env(monkeypatch, tmp_path, db_path)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_disabled"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_disabled")
        sample_count = _sample_count(connection, "evt_routing_disabled")
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data_append_only_dry_run"] == "DISABLED"
    assert statuses["market_data_effective_skip_inline"] == "FALSE"
    assert statuses["market_data"] == "APPLIED"
    assert decision["would_skip_inline"] == 0
    assert decision["effective_skip_inline"] == 0
    assert "DRY_RUN_DISABLED" in decision["blocked_reason_codes_json"]
    assert sample_count == 1


def test_gateway_market_data_append_only_routing_would_skip_when_reconcile_ready(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-ready.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
    )
    _seed_reconcile_pass(db_path)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_ready"),
                headers={"X-Local-Token": "test-token"},
            )
            status_response = client.get(
                "/api/operator/market-data-append-only-routing/status"
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_ready")
        sample_count = _sample_count(connection, "evt_routing_ready")
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data_append_only_dry_run"] == "WOULD_SKIP_INLINE"
    assert statuses["market_data_effective_skip_inline"] == "FALSE"
    assert statuses["market_data"] == "APPLIED"
    assert decision["would_skip_inline"] == 1
    assert decision["effective_skip_inline"] == 0
    assert "DRY_RUN_WOULD_SKIP_INLINE" in decision["blocked_reason_codes_json"]
    assert sample_count == 1
    assert status_response.json()["would_skip_inline_count"] == 1
    assert status_response.json()["effective_skip_inline_count"] == 0
    assert status_response.json()["read_only"] is True
    assert status_response.json()["no_trading_side_effects"] is True


def test_gateway_market_data_append_only_routing_blocks_when_reconcile_missing(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-reconcile-missing.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
    )

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_missing_reconcile"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_missing_reconcile")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data_append_only_dry_run"] == (
        "BLOCKED"
    )
    assert decision["would_skip_inline"] == 0
    assert decision["effective_skip_inline"] == 0
    assert "MARKET_DATA_RECONCILE_MISSING" in decision["blocked_reason_codes_json"]


def test_gateway_market_data_append_only_routing_blocks_when_reconcile_fails(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-reconcile-fail.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
    )
    _insert_reconcile_run(db_path, status="FAIL", append_only_ready=False)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_fail_reconcile"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_fail_reconcile")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data_append_only_dry_run"] == (
        "BLOCKED"
    )
    assert decision["would_skip_inline"] == 0
    assert "MARKET_DATA_RECONCILE_NOT_PASS" in decision["blocked_reason_codes_json"]


def test_gateway_market_data_append_only_routing_blocks_when_reconcile_stale(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-reconcile-stale.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
        GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC="1",
    )
    _insert_reconcile_run(
        db_path,
        status="PASS",
        append_only_ready=True,
        created_at=datetime_to_wire(utc_now() - timedelta(seconds=30)),
    )

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_stale_reconcile"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_stale_reconcile")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data_append_only_dry_run"] == (
        "BLOCKED"
    )
    assert decision["would_skip_inline"] == 0
    assert "MARKET_DATA_RECONCILE_STALE" in decision["blocked_reason_codes_json"]


def test_gateway_market_data_append_only_cutover_flag_still_cannot_skip_in_pr6(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-cutover-disabled.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
        GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED="true",
    )
    _seed_reconcile_pass(db_path)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_routing_cutover_pr6"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_routing_cutover_pr6")
        sample_count = _sample_count(connection, "evt_routing_cutover_pr6")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data_append_only_dry_run"] == (
        "WOULD_SKIP_INLINE"
    )
    assert response.json()["projection_statuses"]["market_data_effective_skip_inline"] == (
        "FALSE"
    )
    assert decision["would_skip_inline"] == 1
    assert decision["effective_skip_inline"] == 0
    assert "EFFECTIVE_SKIP_DISABLED_IN_PR6" in decision["blocked_reason_codes_json"]
    assert sample_count == 1


def test_gateway_market_data_append_only_routing_handles_condition_and_tr_response(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "routing-condition-tr.sqlite3"
    _configure_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true",
    )
    _seed_reconcile_pass(db_path)
    condition = make_condition_event(
        condition_id="cond-routing",
        metadata={"test": "routing"},
    ).to_dict()
    condition["event_id"] = "evt_routing_condition"
    tr_response = make_tr_response_event(
        request_id="candidate_quote_refresh:routing",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70000",
                "등락율": "+0.10",
                "거래량": "1000",
                "거래대금": "70000000",
                "고가": "+70100",
                "저가": "+69900",
            }
        ],
    ).to_dict()
    tr_response["event_id"] = "evt_routing_tr"

    try:
        with TestClient(app) as client:
            condition_response = client.post(
                "/api/gateway/events",
                json=condition,
                headers={"X-Local-Token": "test-token"},
            )
            tr_response_response = client.post(
                "/api/gateway/events",
                json=tr_response,
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        condition_decision = _routing_decision(connection, "evt_routing_condition")
        tr_decision = _routing_decision(connection, "evt_routing_tr")
        condition_signal_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM market_condition_signals
            WHERE event_id = 'evt_routing_condition'
            """
        ).fetchone()["count"]
        tr_snapshot_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM market_tr_snapshots
            WHERE event_id = 'evt_routing_tr'
            """
        ).fetchone()["count"]
    finally:
        connection.close()

    assert condition_response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert tr_response_response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert condition_decision["would_skip_inline"] == 1
    assert tr_decision["would_skip_inline"] == 1
    assert condition_decision["effective_skip_inline"] == 0
    assert tr_decision["effective_skip_inline"] == 0
    assert condition_signal_count == 1
    assert tr_snapshot_count == 1


def test_market_data_append_only_routing_non_market_event_never_skips(tmp_path) -> None:
    connection = initialize_database(tmp_path / "routing-non-market.sqlite3")
    event = make_heartbeat_event().to_dict()
    event["event_id"] = "evt_routing_heartbeat"
    gateway_event = GatewayEvent.from_dict(event)
    append_gateway_event(connection, gateway_event)

    decision = decide_market_data_projection_routing(
        connection,
        gateway_event,
        settings=Settings(gateway_market_data_append_only_dry_run_enabled=True),
    )
    connection.close()

    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert "NOT_MARKET_DATA_EVENT" in decision.blocked_reason_codes


def test_market_data_append_only_routing_dashboard_snapshot_includes_status(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "routing-dashboard.sqlite3")
    settings = Settings(gateway_market_data_append_only_dry_run_enabled=True)
    _seed_reconcile_pass_on_connection(connection, settings=settings)
    event = make_price_tick_event().to_dict()
    event["event_id"] = "evt_routing_dashboard"
    gateway_event = GatewayEvent.from_dict(event)
    append_gateway_event(connection, gateway_event)
    enqueue_projection_jobs_for_gateway_event(connection, gateway_event)
    decide_market_data_projection_routing(
        connection,
        gateway_event,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    snapshot = build_dashboard_snapshot(connection, settings)
    connection.close()

    summary = snapshot["pipeline_summary"]["market_data_append_only_routing"]
    assert status["would_skip_inline_count"] == 1
    assert summary["would_skip_inline_count"] == 1
    assert summary["effective_skip_inline_count"] == 0
    assert (
        "condition_event/tr_response inline projection remains enabled"
        in summary["warnings"]
    )
    assert snapshot["market_data_append_only_routing"]["read_only"] is True


def _configure_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("CONDITION_FUSION_SWEEP_ENABLED", "false")
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()


def _price_tick_payload(event_id: str) -> dict[str, object]:
    payload = make_price_tick_event(source="test-gateway").to_dict()
    payload["event_id"] = event_id
    return payload


def _seed_reconcile_pass(db_path) -> None:
    connection = initialize_database(db_path)
    try:
        _seed_reconcile_pass_on_connection(connection, settings=Settings())
    finally:
        connection.close()


def _seed_reconcile_pass_on_connection(connection, *, settings: Settings) -> None:
    event = make_price_tick_event(source="test-gateway").to_dict()
    event["event_id"] = f"evt_seed_reconcile_{datetime.now(UTC).timestamp()}"
    gateway_event = GatewayEvent.from_dict(event)
    append_gateway_event(connection, gateway_event)
    enqueue_projection_jobs_for_gateway_event(connection, gateway_event)
    process_gateway_event(connection, gateway_event, settings=settings)
    _mark_market_data_outbox(connection, gateway_event.event_id, "APPLIED")
    run_market_data_projection_reconcile(connection, settings=settings, limit=10)


def _insert_reconcile_run(
    db_path,
    *,
    status: str,
    append_only_ready: bool,
    created_at: str | None = None,
) -> None:
    connection = initialize_database(db_path)
    try:
        connection.execute(
            """
            INSERT INTO market_data_projection_reconcile_runs (
                run_id,
                status,
                checked_event_count,
                checked_price_tick_count,
                checked_condition_event_count,
                checked_tr_response_count,
                outbox_job_count,
                outbox_pending_count,
                outbox_processing_count,
                outbox_applied_count,
                outbox_skipped_count,
                outbox_error_count,
                outbox_dead_letter_count,
                missing_projection_count,
                inline_projection_error_count,
                outbox_error_issue_count,
                duplicate_or_conflict_count,
                synthetic_child_event_issue_count,
                watermark_risk_count,
                append_only_ready,
                reason_codes_json,
                created_at
            )
            VALUES (?, ?, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?)
            """,
            (
                f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
                status,
                int(append_only_ready),
                "[]",
                created_at or datetime_to_wire(utc_now()),
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _mark_market_data_outbox(connection, event_id: str, status: str) -> None:
    now = datetime_to_wire(utc_now())
    metadata = {
        "last_worker_evidence": {
            "verification_reason": "TEST_MARKET_DATA_APPEND_ONLY_ROUTING",
            "apply_mode": "SHADOW_VERIFY_ONLY",
        }
    }
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?,
            updated_at = ?,
            processed_at = ?,
            metadata_json = ?,
            last_error = NULL
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (status, now, now, canonical_json(metadata), event_id),
    )
    connection.commit()


def _routing_decision(connection, event_id: str):
    row = connection.execute(
        """
        SELECT *
        FROM market_data_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()
    assert row is not None
    return row


def _sample_count(connection, event_id: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM market_tick_samples
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()["count"]
    )
