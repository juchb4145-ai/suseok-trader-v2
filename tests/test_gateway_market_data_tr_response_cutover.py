from __future__ import annotations

import json
from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_tr_response_event
from services.config import Settings, clear_settings_cache
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture


def test_tr_response_cutover_skips_gateway_inline_and_worker_applies(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "tr-cutover.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    connection = initialize_database(db_path)
    _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_tr_response_payload("evt_tr_cutover_skip", codes=("005930",)),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_tr_cutover_skip")
        assert response.status_code == 200
        statuses = response.json()["projection_statuses"]
        assert statuses["market_data"] == "SKIPPED_INLINE_APPEND_ONLY_TR_RESPONSE"
        assert statuses["market_data_effective_skip_inline"] == "TRUE"
        assert statuses["incremental_evaluation"] == (
            "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_TR_RESPONSE"
        )
        assert decision["effective_skip_inline"] == 1
        assert "TR_RESPONSE_EFFECTIVE_SKIP_ALLOWED" in decision[
            "blocked_reason_codes_json"
        ]
        assert _table_event_count(connection, "market_tr_snapshots", "evt_tr_cutover_skip") == 0

        result = process_projection_outbox_batch(
            connection,
            settings=_cutover_settings(),
            limit=1,
            apply_projection=True,
        )
        snapshot_count = _table_event_count(
            connection,
            "market_tr_snapshots",
            "evt_tr_cutover_skip",
        )
        outbox = _outbox_row(connection, "market_data:evt_tr_cutover_skip")
        metadata = json.loads(outbox["metadata_json"])
        queue_count = int(
            connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM incremental_evaluation_queue
                WHERE source_event_id = 'evt_tr_cutover_skip'
                """
            ).fetchone()["count"]
        )
    finally:
        connection.close()

    assert result.applied_by_worker_count == 1
    assert snapshot_count == 1
    assert outbox["status"] == "APPLIED"
    assert _metadata_path(metadata, "last_worker_evidence", "apply_result") == (
        "APPLIED_BY_WORKER"
    )
    assert _metadata_path(
        metadata,
        "last_worker_evidence",
        "append_only_cutover",
        "gateway_inline_skipped",
    ) is True
    assert _metadata_path(
        metadata,
        "last_worker_evidence",
        "post_apply_side_effects",
        "candidate_quote_refresh_enqueue_status",
    ) == "ENQUEUED"
    assert queue_count == 1


def test_tr_response_cutover_budget_exhaustion_falls_back_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "tr-cutover-budget.sqlite3"
    _configure_cutover_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE="1",
    )
    connection = initialize_database(db_path)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()

    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/gateway/events",
                json=_tr_response_payload("evt_tr_budget_1", codes=("005930",)),
                headers={"X-Local-Token": "test-token"},
            )
            second = client.post(
                "/api/gateway/events",
                json=_tr_response_payload("evt_tr_budget_2", codes=("000660",)),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        second_decision = _routing_decision(connection, "evt_tr_budget_2")
        second_snapshot_count = _table_event_count(
            connection,
            "market_tr_snapshots",
            "evt_tr_budget_2",
        )
    finally:
        connection.close()

    assert first.json()["projection_statuses"]["market_data"] == (
        "SKIPPED_INLINE_APPEND_ONLY_TR_RESPONSE"
    )
    assert second.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert second_decision["effective_skip_inline"] == 0
    assert "TR_RESPONSE_SKIP_BUDGET_EXHAUSTED" in second_decision[
        "blocked_reason_codes_json"
    ]
    assert second_snapshot_count == 1


def test_tr_response_cutover_blocks_when_worker_disabled_or_guard_fails(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-cutover-guards.sqlite3")
    event = GatewayEvent.from_dict(
        _tr_response_payload("evt_tr_worker_disabled", codes=("005930",))
    )
    from storage.event_store import append_gateway_event
    from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
    from services.runtime.gateway_projection_routing import (
        decide_market_data_projection_routing,
    )

    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)

    disabled = decide_market_data_projection_routing(
        connection,
        event,
        settings=_cutover_settings(
            projection_outbox_apply_projection_enabled=False,
            projection_outbox_market_data_apply_enabled=False,
        ),
        outbox_status=outbox.status,
    )
    connection.execute("DELETE FROM market_data_projection_reconcile_runs")
    _insert_reconcile_run(
        connection,
        status="PASS",
        append_only_ready=True,
        synthetic_child_event_issue_count=1,
    )
    guard_failed = decide_market_data_projection_routing(
        connection,
        event,
        settings=_cutover_settings(),
        outbox_status=outbox.status,
    )
    connection.close()

    assert disabled.effective_skip_inline is False
    assert "WORKER_APPLY_NOT_ENABLED" in disabled.blocked_reason_codes
    assert guard_failed.effective_skip_inline is False
    assert "TR_RESPONSE_SYNTHETIC_CHILD_GUARD_NOT_READY" in (
        guard_failed.blocked_reason_codes
    )


def test_condition_event_remains_inline_with_tr_response_cutover_flags(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "tr-cutover-condition-inline.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    connection = initialize_database(db_path)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()
    event = make_condition_event(source="test-gateway").to_dict()
    event["event_id"] = "evt_tr_cutover_condition"

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=event,
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_tr_cutover_condition")
        signal_count = _table_event_count(
            connection,
            "market_condition_signals",
            "evt_tr_cutover_condition",
        )
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert decision["effective_skip_inline"] == 0
    assert "CONDITION_EVENT_CUTOVER_DISABLED_IN_PR9" in decision[
        "blocked_reason_codes_json"
    ]
    assert signal_count == 1


def test_tr_response_cutover_too_many_rows_falls_back_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "tr-cutover-too-many-rows.sqlite3"
    _configure_cutover_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_ROWS_PER_EVENT="1",
    )
    connection = initialize_database(db_path)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.close()

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_tr_response_payload(
                    "evt_tr_too_many_rows",
                    codes=("005930", "000660"),
                ),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_tr_too_many_rows")
        snapshot_count = _table_event_count(
            connection,
            "market_tr_snapshots",
            "evt_tr_too_many_rows",
        )
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert decision["effective_skip_inline"] == 0
    assert "TR_RESPONSE_TOO_MANY_ROWS" in decision["blocked_reason_codes_json"]
    assert snapshot_count == 2


def _configure_cutover_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("CONDITION_FUSION_SWEEP_ENABLED", "false")
    values = {
        "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE": "10",
        "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED": "true",
        "PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED": "true",
        "PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC": "0",
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()


def _cutover_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": True,
        "gateway_market_data_append_only_tr_response_cutover_enabled": True,
        "gateway_market_data_append_only_tr_response_max_skip_per_minute": 10,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
        "projection_outbox_apply_min_age_sec": 0,
    }
    values.update(overrides)
    return Settings(**values)


def _tr_response_payload(event_id: str, *, codes: tuple[str, ...]) -> dict[str, object]:
    rows = [
        {
            "종목코드": f"A{code}",
            "종목명": f"종목{code}",
            "현재가": "+70000",
            "등락율": "+0.10",
            "거래량": "1000",
            "거래대금": "70000000",
            "고가": "+70100",
            "저가": "+69900",
        }
        for code in codes
    ]
    payload = make_tr_response_event(
        request_id=f"candidate_quote_refresh:2026-07-08:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        rows=rows,
        source="test-gateway",
    ).to_dict()
    payload["event_id"] = event_id
    return payload


def _insert_reconcile_run(
    connection,
    *,
    status: str,
    append_only_ready: bool,
    synthetic_child_event_issue_count: int = 0,
) -> None:
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
            summary_json,
            created_at
        )
        VALUES (?, ?, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, ?, 0, ?, '[]', ?, ?)
        """,
        (
            f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
            status,
            synthetic_child_event_issue_count,
            int(append_only_ready),
            canonical_json(
                {
                    "status": status,
                    "append_only_ready": append_only_ready,
                    "synthetic_child_event_issue_count": (
                        synthetic_child_event_issue_count
                    ),
                }
            ),
            datetime_to_wire(utc_now()),
        ),
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


def _outbox_row(connection, outbox_id: str):
    row = connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()
    assert row is not None
    return row


def _table_event_count(connection, table_name: str, event_id: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name} WHERE event_id = ?",
            (event_id,),
        ).fetchone()["count"]
    )


def _metadata_path(payload: dict, *path: str):
    current = payload
    for item in path:
        current = current[item]
    return current
