from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import (
    make_condition_event,
    make_price_tick_event,
    make_tr_response_event,
)
from services.config import clear_settings_cache
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database, open_connection


def test_price_tick_cutover_default_keeps_inline_projection(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-default.sqlite3"
    _configure_env(monkeypatch, tmp_path, db_path)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_default"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_price_cutover_default")
        sample_count = _sample_count(connection, "evt_price_cutover_default")
    finally:
        connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data_effective_skip_inline"] == "FALSE"
    assert statuses["market_data"] == "APPLIED"
    assert decision["effective_skip_inline"] == 0
    assert sample_count == 1


def test_price_tick_cutover_skips_inline_when_all_guards_pass(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-skip.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    _insert_reconcile_run(db_path, status="PASS", append_only_ready=True)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_skip"),
                headers={"X-Local-Token": "test-token"},
            )
            status_response = client.get(
                "/api/operator/market-data-append-only-routing/status"
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_price_cutover_skip")
        sample_count = _sample_count(connection, "evt_price_cutover_skip")
        outbox = _outbox_row(connection, "market_data:evt_price_cutover_skip")
    finally:
        connection.close()

    assert response.status_code == 200
    payload = response.json()
    statuses = payload["projection_statuses"]
    assert statuses["market_data"] == "SKIPPED_INLINE_APPEND_ONLY_PRICE_TICK"
    assert statuses["market_data_effective_skip_inline"] == "TRUE"
    assert (
        statuses["incremental_evaluation"]
        == "DEFERRED_TO_PROJECTION_OUTBOX_WORKER"
    )
    assert payload["market_data_append_only_routing"]["effective_skip_inline"] is True
    assert decision["would_skip_inline"] == 1
    assert decision["effective_skip_inline"] == 1
    assert decision["cutover_scope"] == "price_tick_only"
    assert decision["fallback_inline_projection_expected"] == 0
    assert "EFFECTIVE_SKIP_ALLOWED_PRICE_TICK" in decision["blocked_reason_codes_json"]
    assert sample_count == 0
    assert outbox["status"] == "PENDING"
    assert status_response.json()["effective_price_tick_skip_count"] == 1
    assert status_response.json()["condition_event_effective_skip_count"] == 0
    assert status_response.json()["tr_response_effective_skip_count"] == 0
    assert status_response.json()["invalid_effective_skip_count"] == 0


def test_condition_and_tr_response_remain_inline_even_with_cutover_flags(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-non-price-inline.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    _insert_reconcile_run(db_path, status="PASS", append_only_ready=True)
    condition = make_condition_event(metadata={"test": "price-cutover"}).to_dict()
    condition["event_id"] = "evt_price_cutover_condition"
    tr_response = _tr_response_payload("evt_price_cutover_tr")

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
        condition_decision = _routing_decision(connection, "evt_price_cutover_condition")
        tr_decision = _routing_decision(connection, "evt_price_cutover_tr")
        condition_signal_count = _table_event_count(
            connection,
            "market_condition_signals",
            "evt_price_cutover_condition",
        )
        tr_snapshot_count = _table_event_count(
            connection,
            "market_tr_snapshots",
            "evt_price_cutover_tr",
        )
    finally:
        connection.close()

    assert condition_response.status_code == 200
    assert tr_response_response.status_code == 200
    assert condition_response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert tr_response_response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert condition_decision["effective_skip_inline"] == 0
    assert tr_decision["effective_skip_inline"] == 0
    assert "CONDITION_EVENT_CUTOVER_DISABLED_IN_PR9" in (
        condition_decision["blocked_reason_codes_json"]
    )
    assert "TR_RESPONSE_CUTOVER_DISABLED" in (
        tr_decision["blocked_reason_codes_json"]
    )
    assert condition_signal_count == 1
    assert tr_snapshot_count == 1


def test_price_tick_cutover_falls_back_inline_when_reconcile_fails(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-reconcile-fail.sqlite3"
    _configure_cutover_env(monkeypatch, tmp_path, db_path)
    _insert_reconcile_run(db_path, status="FAIL", append_only_ready=False)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_reconcile_fail"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_price_cutover_reconcile_fail")
        sample_count = _sample_count(connection, "evt_price_cutover_reconcile_fail")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert response.json()["projection_statuses"]["market_data_effective_skip_inline"] == (
        "FALSE"
    )
    assert decision["effective_skip_inline"] == 0
    assert "MARKET_DATA_RECONCILE_NOT_PASS" in decision["blocked_reason_codes_json"]
    assert sample_count == 1


def test_price_tick_cutover_budget_exhaustion_falls_back_inline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-budget.sqlite3"
    _configure_cutover_env(
        monkeypatch,
        tmp_path,
        db_path,
        GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE="1",
    )
    _insert_reconcile_run(db_path, status="PASS", append_only_ready=True)

    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_budget_1"),
                headers={"X-Local-Token": "test-token"},
            )
            second = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_budget_2", price=70_100),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        first_decision = _routing_decision(connection, "evt_price_cutover_budget_1")
        second_decision = _routing_decision(connection, "evt_price_cutover_budget_2")
        first_sample_count = _sample_count(connection, "evt_price_cutover_budget_1")
        second_sample_count = _sample_count(connection, "evt_price_cutover_budget_2")
    finally:
        connection.close()

    assert first.json()["projection_statuses"]["market_data"] == (
        "SKIPPED_INLINE_APPEND_ONLY_PRICE_TICK"
    )
    assert second.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert first_decision["effective_skip_inline"] == 1
    assert second_decision["effective_skip_inline"] == 0
    assert "PRICE_TICK_SKIP_BUDGET_EXHAUSTED" in (
        second_decision["blocked_reason_codes_json"]
    )
    assert first_sample_count == 0
    assert second_sample_count == 1


def test_price_tick_cutover_requires_worker_apply_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "price-cutover-worker-disabled.sqlite3"
    _configure_cutover_env(
        monkeypatch,
        tmp_path,
        db_path,
        PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="false",
        PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="false",
    )
    _insert_reconcile_run(db_path, status="PASS", append_only_ready=True)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=_price_tick_payload("evt_price_cutover_worker_disabled"),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        decision = _routing_decision(connection, "evt_price_cutover_worker_disabled")
        sample_count = _sample_count(connection, "evt_price_cutover_worker_disabled")
    finally:
        connection.close()

    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert decision["effective_skip_inline"] == 0
    assert "WORKER_APPLY_NOT_ENABLED" in decision["blocked_reason_codes_json"]
    assert sample_count == 1


def _configure_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("CONDITION_FUSION_SWEEP_ENABLED", "false")
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()


def _configure_cutover_env(monkeypatch, tmp_path, db_path, **overrides: str) -> None:
    values = {
        "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED": "true",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_EVENT_TYPES": "price_tick",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE": "10",
        "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED": "true",
        "PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED": "true",
    }
    values.update(overrides)
    _configure_env(monkeypatch, tmp_path, db_path, **values)


def _price_tick_payload(
    event_id: str,
    *,
    price: int = 70_000,
) -> dict[str, object]:
    payload = make_price_tick_event(source="test-gateway", price=price).to_dict()
    payload["event_id"] = event_id
    return payload


def _tr_response_payload(event_id: str) -> dict[str, object]:
    payload = make_tr_response_event(
        request_id=f"candidate_quote_refresh:2026-06-26:{event_id}",
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
        source="test-gateway",
    ).to_dict()
    payload["event_id"] = event_id
    return payload


def _insert_reconcile_run(
    db_path,
    *,
    status: str,
    append_only_ready: bool,
) -> None:
    connection = initialize_database(db_path)
    try:
        _insert_reconcile_run_on_connection(
            connection,
            status=status,
            append_only_ready=append_only_ready,
        )
    finally:
        connection.close()


def _insert_reconcile_run_on_connection(
    connection,
    *,
    status: str,
    append_only_ready: bool,
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
        VALUES (?, ?, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, '[]', ?, ?)
        """,
        (
            f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
            status,
            int(append_only_ready),
            canonical_json(
                {
                    "status": status,
                    "append_only_ready": append_only_ready,
                    "test": "price_tick_cutover",
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


def _sample_count(connection, event_id: str) -> int:
    return _table_event_count(connection, "market_tick_samples", event_id)


def _table_event_count(connection, table_name: str, event_id: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name} WHERE event_id = ?",
            (event_id,),
        ).fetchone()["count"]
    )
