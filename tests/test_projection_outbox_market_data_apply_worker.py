from __future__ import annotations

import json
from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.tr import BrokerTrResponse
from fastapi.testclient import TestClient
from services.config import Settings, clear_settings_cache
from services.market_data_service import process_gateway_event
from services.runtime import projection_outbox_worker
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_api_apply_request_is_noop_when_flags_disabled(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "projection-outbox-apply-disabled.sqlite3"
    connection = initialize_database(db_path)
    event = _price_tick_event("evt_apply_disabled")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    connection.close()

    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC", "0")
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC", "0")
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "false")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED", "false")
    clear_settings_cache()
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/operator/projection-outbox/run-once?limit=1&apply_projection=true",
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        sample_count = _count_rows(connection, "market_tick_samples")
        row = _outbox_row(connection, "market_data:evt_apply_disabled")
    finally:
        connection.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["apply_projection_requested"] is True
    assert payload["apply_projection_effective"] is False
    assert payload["projection_side_effects_allowed"] is False
    assert payload["read_only_projection"] is True
    assert payload["no_trading_side_effects"] is True
    assert payload["mutated_projection_names"] == []
    assert payload["skipped_apply_disabled_count"] == 1
    assert sample_count == 0
    assert row["status"] == "SKIPPED"
    assert "APPLY_DISABLED_BY_SETTINGS" in row["metadata_json"]


def test_api_default_run_once_stays_read_only_even_when_apply_flags_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "projection-outbox-apply-flags-query-required.sqlite3"
    connection = initialize_database(db_path)
    event = _price_tick_event("evt_apply_query_required")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    connection.close()

    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC", "0")
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC", "0")
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED", "true")
    clear_settings_cache()
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/operator/projection-outbox/run-once?limit=1",
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    try:
        sample_count = _count_rows(connection, "market_tick_samples")
        row = _outbox_row(connection, "market_data:evt_apply_query_required")
    finally:
        connection.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["apply_projection_requested"] is False
    assert payload["apply_projection_effective"] is False
    assert payload["projection_side_effects_allowed"] is False
    assert payload["read_only_projection"] is True
    assert payload["projection_apply_error_count"] == 0
    assert sample_count == 0
    assert row["status"] == "ERROR"
    assert "MARKET_DATA_PRICE_TICK_SAMPLE_MISSING" in row["last_error"]


def test_market_data_apply_skips_inline_applied_price_tick_without_mutation(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-inline.sqlite3")
    settings = _apply_settings()
    event = _price_tick_event("evt_inline_applied")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    before_counts = _projection_counts(connection)

    def fail_process_gateway_event(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AssertionError("process_gateway_event must not be called")

    monkeypatch.setattr(
        projection_outbox_worker,
        "process_gateway_event",
        fail_process_gateway_event,
    )

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )
    after_counts = _projection_counts(connection)
    row = _outbox_row(connection, "market_data:evt_inline_applied")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert result.applied_count == 1
    assert result.applied_by_verify_count == 1
    assert result.applied_by_worker_count == 0
    assert result.mutated_projection_names == ()
    assert after_counts == before_counts
    assert row["status"] == "APPLIED"
    assert (
        metadata["last_worker_evidence"]["verification_reason"]
        == "MARKET_DATA_ALREADY_APPLIED_BY_INLINE"
    )


def test_market_data_apply_price_tick_creates_sample_when_inline_missing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-price-apply.sqlite3")
    settings = _apply_settings()
    event = _price_tick_event("evt_apply_price")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )
    row = _outbox_row(connection, "market_data:evt_apply_price")
    sample_count = _count_rows(connection, "market_tick_samples")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert result.status == "COMPLETED"
    assert result.applied_count == 1
    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_data",)
    assert sample_count == 1
    assert row["status"] == "APPLIED"
    assert metadata["last_worker_evidence"]["apply_mode"] == "MARKET_DATA_APPLY"
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLIED_BY_WORKER"


def test_market_data_apply_tr_response_preserves_synthetic_child_event_id(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-tr-apply.sqlite3")
    settings = _apply_settings()
    event = _tr_response_event("evt_apply_tr")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )
    sample = connection.execute(
        """
        SELECT event_id, metadata_json
        FROM market_tick_samples
        WHERE code = '005930'
        """
    ).fetchone()
    row = _outbox_row(connection, "market_data:evt_apply_tr")
    connection.close()

    assert result.applied_by_worker_count == 1
    assert row["status"] == "APPLIED"
    assert sample["event_id"] == "evt_apply_tr:synthetic_price_tick:0:005930:KRX"
    metadata = json.loads(sample["metadata_json"])
    assert metadata["parent_event_id"] == "evt_apply_tr"
    assert metadata["synthetic_event"] is True


def test_market_data_apply_condition_event_creates_condition_signal(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-condition-apply.sqlite3")
    settings = _apply_settings()
    event = _condition_event("evt_apply_condition")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
    )
    market_data = _outbox_row(connection, "market_data:evt_apply_condition")
    condition_fusion = _outbox_row(connection, "condition_fusion:evt_apply_condition")
    signal_count = _count_rows(connection, "market_condition_signals")
    connection.close()

    assert result.applied_by_worker_count == 1
    assert result.skipped_apply_disabled_count == 1
    assert result.mutated_projection_names == ("market_data",)
    assert market_data["status"] == "APPLIED"
    assert condition_fusion["status"] == "SKIPPED"
    assert "APPLY_NOT_ENABLED_FOR_PROJECTION" in condition_fusion["metadata_json"]
    assert signal_count == 1


def test_apply_mode_skips_non_market_data_projection_jobs(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-non-market-data.sqlite3")
    settings = _apply_settings()
    index_event = _market_index_tick_event("evt_apply_index")
    symbols_event = _market_symbols_event("evt_apply_symbols")
    for event in (index_event, symbols_event):
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=3,
        apply_projection=True,
    )
    rows = {
        row["projection_name"]: row
        for row in connection.execute(
            "SELECT projection_name, status, metadata_json FROM projection_outbox"
        ).fetchall()
    }
    counts = _projection_counts(connection)
    symbol_membership_count = _count_rows(connection, "market_symbol_memberships")
    connection.close()

    assert result.applied_count == 0
    assert result.skipped_count == 3
    assert result.skipped_apply_disabled_count == 3
    assert result.mutated_projection_names == ()
    assert rows["market_index"]["status"] == "SKIPPED"
    assert rows["market_regime"]["status"] == "SKIPPED"
    assert rows["market_reference"]["status"] == "SKIPPED"
    assert all("APPLY_NOT_ENABLED_FOR_PROJECTION" in row["metadata_json"] for row in rows.values())
    assert counts["market_index_tick_samples"] == 0
    assert counts["market_regime_snapshots"] == 0
    assert symbol_membership_count == 0


def test_market_data_apply_exception_marks_outbox_error(tmp_path, monkeypatch) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-apply-error.sqlite3")
    settings = _apply_settings(projection_outbox_retry_limit=2)
    event = _price_tick_event("evt_apply_error")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    def raise_projection_error(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("forced projection apply error")

    monkeypatch.setattr(
        projection_outbox_worker,
        "process_gateway_event",
        raise_projection_error,
    )

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )
    row = _outbox_row(connection, "market_data:evt_apply_error")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.error_count == 1
    assert result.projection_apply_error_count == 1
    assert row["status"] == "ERROR"
    assert "MARKET_DATA_APPLY_EXCEPTION" in row["last_error"]
    assert metadata["last_worker_evidence"]["apply_mode"] == "MARKET_DATA_APPLY"
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLY_ERROR"


def _apply_settings(**overrides) -> Settings:
    values = {
        "projection_outbox_shadow_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _price_tick_event(event_id: str) -> GatewayEvent:
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
        trade_time=TS,
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=TS,
    )


def _tr_response_event(event_id: str) -> GatewayEvent:
    response = BrokerTrResponse(
        request_id=f"candidate_quote_refresh:2026-06-26:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70000",
                "등락율": "+0.10",
                "거래량": "1000",
                "거래대금": "70000000",
                "고가": "+70500",
                "저가": "-69500",
            }
        ],
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=TS,
    )


def _condition_event(event_id: str) -> GatewayEvent:
    condition = BrokerConditionEvent(
        condition_id="cond-apply",
        condition_name="Apply Guard",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=70_000,
        metadata={"test": "projection_outbox_market_data_apply"},
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="condition_event",
        source="test-gateway",
        payload=condition.to_dict(),
        ts=TS,
    )


def _market_index_tick_event(event_id: str) -> GatewayEvent:
    tick = BrokerMarketIndexTick(
        index_code="KOSPI",
        index_name="KOSPI",
        price=2_800.0,
        change_rate=0.1,
        change_value=2.8,
        trade_time=TS,
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=TS,
    )


def _market_symbols_event(event_id: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={
            "markets": {
                "KOSPI": [
                    {
                        "code": "005930",
                        "name": "삼성전자",
                    }
                ]
            }
        },
        ts=TS,
    )


def _outbox_row(connection, outbox_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()


def _projection_counts(connection) -> dict[str, int]:
    return {
        "market_ticks_latest": _count_rows(connection, "market_ticks_latest"),
        "market_tick_samples": _count_rows(connection, "market_tick_samples"),
        "market_tr_snapshots": _count_rows(connection, "market_tr_snapshots"),
        "market_condition_signals": _count_rows(connection, "market_condition_signals"),
        "market_index_tick_samples": _count_rows(connection, "market_index_tick_samples"),
        "market_regime_snapshots": _count_rows(connection, "market_regime_snapshots"),
    }


def _count_rows(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
