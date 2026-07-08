from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.core_api import app
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import datetime_to_wire
from fastapi.testclient import TestClient
from services.config import Settings, clear_settings_cache
from services.dashboard_service import build_dashboard_snapshot
from services.market_data_service import process_gateway_event
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
    run_market_data_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.projection_watermarks import advance_projection_watermark
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_reconcile_passes_when_market_data_artifacts_and_outbox_are_applied(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "reconcile-pass.sqlite3")
    settings = Settings()
    events = [
        _price_tick_event("evt_reconcile_price"),
        _condition_event("evt_reconcile_condition"),
        _tr_response_event("evt_reconcile_tr", row_count=1),
    ]
    for event in events:
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
        process_gateway_event(connection, event, settings=settings)
        _mark_market_data_outbox(connection, event.event_id, "APPLIED")

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
    )
    connection.close()

    assert result.status == "PASS"
    assert result.append_only_ready is True
    assert result.checked_price_tick_count == 1
    assert result.checked_condition_event_count == 1
    assert result.checked_tr_response_count == 1
    assert result.missing_projection_count == 0
    assert result.outbox_applied_count == 3


def test_reconcile_fails_when_price_tick_sample_and_error_are_missing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-missing-price.sqlite3")
    event = _price_tick_event("evt_reconcile_missing_price")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "FAIL"
    assert result.append_only_ready is False
    assert result.missing_projection_count == 1
    assert "MARKET_DATA_PRICE_TICK_PROJECTION_MISSING" in result.reason_codes


def test_reconcile_allows_price_tick_skipped_as_older_than_latest(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-older-price-skip.sqlite3")
    settings = Settings()
    older_event = _price_tick_event("evt_reconcile_older_price", ts=TS)
    latest_event = _price_tick_event(
        "evt_reconcile_latest_price",
        ts=TS + timedelta(seconds=1),
    )
    append_gateway_event(connection, older_event)
    enqueue_projection_jobs_for_gateway_event(connection, older_event)
    append_gateway_event(connection, latest_event)
    enqueue_projection_jobs_for_gateway_event(connection, latest_event)
    process_gateway_event(connection, latest_event, settings=settings)
    _mark_market_data_outbox(
        connection,
        older_event.event_id,
        "SKIPPED",
        reason="MARKET_DATA_PRICE_TICK_OLDER_THAN_LATEST",
    )
    _mark_market_data_outbox(connection, latest_event.event_id, "APPLIED")

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
    )
    connection.close()

    assert result.status == "PASS"
    assert result.append_only_ready is True
    assert result.checked_price_tick_count == 2
    assert result.missing_projection_count == 0
    assert result.outbox_skipped_count == 1
    assert "MARKET_DATA_PRICE_TICK_PROJECTION_MISSING" not in result.reason_codes


def test_reconcile_allows_empty_tr_response_with_skipped_outbox(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-empty-tr.sqlite3")
    event = _tr_response_event("evt_reconcile_empty_tr", row_count=0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_market_data_outbox(
        connection,
        event.event_id,
        "SKIPPED",
        reason="MARKET_DATA_TR_RESPONSE_NO_ROWS",
    )

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "PASS"
    assert result.checked_tr_response_count == 1
    assert result.missing_projection_count == 0
    assert result.outbox_skipped_count == 1


def test_reconcile_records_inline_projection_error_as_warn(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-inline-error.sqlite3")
    event = _price_tick_event("evt_reconcile_inline_error")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")
    connection.execute(
        """
        INSERT INTO market_projection_errors (
            event_id, event_type, code, error_message, payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            event.event_type,
            "005930",
            "INVALID_PRICE_TICK",
            canonical_json(event.payload),
        ),
    )
    connection.commit()

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "WARN"
    assert result.inline_projection_error_count == 1
    assert "MARKET_DATA_INLINE_PROJECTION_ERROR" in result.reason_codes


def test_reconcile_fails_when_outbox_job_is_missing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-missing-outbox.sqlite3")
    event = _price_tick_event("evt_reconcile_missing_outbox")
    append_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=Settings())

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "FAIL"
    assert "MARKET_DATA_OUTBOX_JOB_MISSING" in result.reason_codes


def test_reconcile_fails_on_dead_letter_outbox(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-dead-letter.sqlite3")
    event = _price_tick_event("evt_reconcile_dead_letter")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=Settings())
    _mark_market_data_outbox(connection, event.event_id, "DEAD_LETTER")

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "FAIL"
    assert result.outbox_dead_letter_count == 1
    assert "MARKET_DATA_OUTBOX_DEAD_LETTER" in result.reason_codes


def test_reconcile_fails_when_watermark_passed_missing_artifact(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-watermark.sqlite3")
    event = _price_tick_event("evt_reconcile_watermark")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")
    rowid = _event_rowid(connection, event.event_id)
    advance_projection_watermark(
        connection,
        "market_data",
        last_event_rowid=rowid,
        last_event_id=event.event_id,
        last_event_received_at=datetime_to_wire(TS),
        commit=True,
    )

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "FAIL"
    assert result.watermark_risk_count == 1
    assert "MARKET_DATA_WATERMARK_ADVANCED_WITH_MISSING_ARTIFACT" in result.reason_codes


def test_reconcile_fails_on_synthetic_child_event_id_regression(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-synthetic-regression.sqlite3")
    event = _tr_response_event("evt_reconcile_synthetic_parent", row_count=2)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _insert_tr_snapshot(connection, event)
    _insert_tick_sample(
        connection,
        event_id=event.event_id,
        code="005930",
        metadata={
            "parent_event_id": event.event_id,
            "synthetic_event": True,
            "row_index": 0,
        },
    )
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")

    result = run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)
    connection.close()

    assert result.status == "FAIL"
    assert result.synthetic_child_event_issue_count >= 1
    assert "SYNTHETIC_CHILD_EVENT_ID_REGRESSION" in result.reason_codes


def test_reconcile_latest_api_and_run_once_api(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "reconcile-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    clear_settings_cache()
    connection = initialize_database(db_path)
    event = _price_tick_event("evt_reconcile_api")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=Settings())
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")
    connection.close()

    try:
        with TestClient(app) as client:
            missing_token = client.post(
                "/api/operator/market-data-projection-reconcile/run-once?limit=1"
            )
            run_once = client.post(
                "/api/operator/market-data-projection-reconcile/run-once?limit=1",
                headers={"X-Local-Token": "test-token"},
            )
            latest = client.get("/api/operator/market-data-projection-reconcile/latest")
    finally:
        clear_settings_cache()

    assert missing_token.status_code == 401
    assert run_once.status_code == 200
    assert run_once.json()["status"] == "PASS"
    assert run_once.json()["read_only"] is True
    assert run_once.json()["no_trading_side_effects"] is True
    assert latest.status_code == 200
    assert latest.json()["latest_run"]["status"] == "PASS"


def test_dashboard_snapshot_includes_latest_reconcile_result(tmp_path) -> None:
    connection = initialize_database(tmp_path / "reconcile-dashboard.sqlite3")
    event = _price_tick_event("evt_reconcile_dashboard")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=Settings())
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")
    run_market_data_projection_reconcile(connection, settings=Settings(), limit=1)

    snapshot = build_dashboard_snapshot(connection, Settings())
    latest = get_latest_market_data_projection_reconcile(connection)
    connection.close()

    summary = snapshot["pipeline_summary"]["market_data_projection_reconcile"]
    assert latest["latest_run"]["status"] == "PASS"
    assert summary["latest_status"] == "PASS"
    assert summary["append_only_ready"] is True
    assert "Gateway inline projection remains enabled" in summary["warnings"]


def _price_tick_event(event_id: str, *, ts: datetime = TS):
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=70_000,
        change_rate=0.1,
        volume=1_000,
        trade_value=70_000_000,
        execution_strength=101.5,
        best_bid=69_900,
        best_ask=70_000,
        spread_ticks=1,
        day_high=70_500,
        day_low=69_500,
        trade_time=ts,
        ts=ts,
    )
    from domain.broker.events import GatewayEvent

    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
    )


def _condition_event(event_id: str):
    condition = BrokerConditionEvent(
        condition_id=f"cond-{event_id}",
        condition_name="Reconcile Guard",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=70_000,
        metadata={"test": "market_data_projection_reconcile"},
        ts=TS,
    )
    from domain.broker.events import GatewayEvent

    return GatewayEvent(
        event_id=event_id,
        event_type="condition_event",
        source="test-gateway",
        payload=condition.to_dict(),
        ts=TS,
    )


def _tr_response_event(event_id: str, *, row_count: int):
    rows = [
        {
            "종목코드": f"A{code}",
            "종목명": name,
            "현재가": price,
            "등락율": "+0.10",
            "거래량": "1000",
            "거래대금": "70000000",
            "고가": price,
            "저가": price,
        }
        for code, name, price in (
            ("005930", "삼성전자", "+70000"),
            ("000660", "SK하이닉스", "+120000"),
        )
    ][:row_count]
    response = BrokerTrResponse(
        request_id=f"candidate_quote_refresh:2026-06-26:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=rows,
        ts=TS,
    )
    from domain.broker.events import GatewayEvent

    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=TS,
    )


def _mark_market_data_outbox(
    connection,
    event_id: str,
    status: str,
    *,
    reason: str = "MARKET_DATA_RECONCILE_TEST",
) -> None:
    now = datetime_to_wire(TS)
    metadata = {
        "last_worker_evidence": {
            "verification_reason": reason,
            "apply_mode": "SHADOW_VERIFY_ONLY",
        }
    }
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?,
            updated_at = ?,
            processed_at = CASE WHEN ? IN ('APPLIED', 'SKIPPED', 'DEAD_LETTER')
                THEN ? ELSE processed_at END,
            metadata_json = ?,
            last_error = CASE WHEN ? IN ('ERROR', 'DEAD_LETTER') THEN ? ELSE NULL END
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (
            status,
            now,
            status,
            now,
            canonical_json(metadata),
            status,
            reason,
            event_id,
        ),
    )
    connection.commit()


def _event_rowid(connection, event_id: str) -> int:
    row = connection.execute(
        "SELECT rowid FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return int(row["rowid"])


def _insert_tr_snapshot(connection, event) -> None:
    row = event.payload["rows"][0]
    connection.execute(
        """
        INSERT INTO market_tr_snapshots (
            event_id, request_id, tr_code, request_name, code, row_json,
            event_ts, received_at, source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            event.payload["request_id"],
            event.payload["tr_code"],
            event.payload["request_name"],
            "005930",
            canonical_json(row),
            datetime_to_wire(TS),
            datetime_to_wire(TS),
            event.source,
        ),
    )
    connection.commit()


def _insert_tick_sample(
    connection,
    *,
    event_id: str,
    code: str,
    metadata: dict,
) -> None:
    connection.execute(
        """
        INSERT INTO market_tick_samples (
            event_id, code, exchange, session, price, cumulative_volume,
            cumulative_trade_value, volume_delta, trade_value_delta,
            execution_strength, event_ts, received_at, source, metadata_json
        )
        VALUES (?, ?, 'KRX', 'REGULAR', 70000, 1000, 70000000, 1000, 70000000,
            101.5, ?, ?, 'test-gateway', ?)
        """,
        (
            event_id,
            code,
            datetime_to_wire(TS),
            datetime_to_wire(TS),
            canonical_json(metadata),
        ),
    )
    connection.commit()
