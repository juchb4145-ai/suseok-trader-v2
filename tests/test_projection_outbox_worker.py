from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.core_api import app
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.market_index_service import process_market_index_event
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import (
    claim_projection_outbox_jobs,
    enqueue_projection_jobs_for_gateway_event,
    mark_projection_outbox_applied,
    mark_projection_outbox_error,
    reset_stale_projection_outbox_processing,
)
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_worker_marks_price_tick_job_applied_without_projection_mutation(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-price.sqlite3")
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _price_tick_event("evt_worker_price_tick")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    before_counts = _projection_counts(connection)

    result = process_projection_outbox_batch(connection, settings=settings, limit=1)
    after_counts = _projection_counts(connection)
    row = _outbox_row(connection, "market_data:evt_worker_price_tick")
    connection.close()

    assert result.status == "COMPLETED"
    assert result.claimed_count == 1
    assert result.applied_count == 1
    assert result.skipped_count == 0
    assert row["status"] == "APPLIED"
    assert row["processed_at"] is not None
    assert after_counts == before_counts


def test_worker_skips_legacy_older_price_tick_missing_sample(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-older-price.sqlite3")
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    newer = _price_tick_event(
        "evt_worker_newer_price_tick",
        price=70_100,
        volume=1_010,
        trade_value=70_801_000,
        ts=TS + timedelta(seconds=10),
    )
    older = _price_tick_event(
        "evt_worker_older_price_tick",
        price=69_900,
        volume=1_000,
        trade_value=69_900_000,
        ts=TS + timedelta(seconds=5),
    )
    append_gateway_event(connection, newer)
    process_gateway_event(connection, newer, settings=settings)
    append_gateway_event(connection, older)
    enqueue_projection_jobs_for_gateway_event(connection, older)

    result = process_projection_outbox_batch(connection, settings=settings, limit=1)
    row = _outbox_row(connection, "market_data:evt_worker_older_price_tick")
    connection.close()

    assert result.status == "COMPLETED"
    assert result.claimed_count == 1
    assert result.applied_count == 0
    assert result.skipped_count == 1
    assert result.error_count == 0
    assert row["status"] == "SKIPPED"
    assert "MARKET_DATA_PRICE_TICK_OLDER_THAN_LATEST" in row["metadata_json"]


def test_worker_marks_tr_response_job_applied_from_inline_snapshot(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-tr.sqlite3")
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _tr_response_event("evt_worker_tr_response")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    result = process_projection_outbox_batch(connection, settings=settings, limit=1)
    row = _outbox_row(connection, "market_data:evt_worker_tr_response")
    snapshot_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()["count"]
    connection.close()

    assert result.applied_count == 1
    assert row["status"] == "APPLIED"
    assert snapshot_count == 1


def test_worker_skips_condition_fusion_when_incremental_disabled(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-condition.sqlite3")
    settings = Settings(
        condition_fusion_event_incremental_enabled=False,
        projection_outbox_shadow_min_age_sec=0,
    )
    event = _condition_event("evt_worker_condition")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    result = process_projection_outbox_batch(connection, settings=settings, limit=10)
    market_data = _outbox_row(connection, "market_data:evt_worker_condition")
    condition_fusion = _outbox_row(connection, "condition_fusion:evt_worker_condition")
    connection.close()

    assert result.claimed_count == 2
    assert result.applied_count == 1
    assert result.skipped_count == 1
    assert market_data["status"] == "APPLIED"
    assert condition_fusion["status"] == "SKIPPED"
    assert "CONDITION_FUSION_INCREMENTAL_DISABLED" in condition_fusion["metadata_json"]


def test_worker_marks_market_index_job_applied_and_regime_safe_skipped(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-index.sqlite3")
    settings = Settings(projection_outbox_shadow_min_age_sec=0)
    event = _market_index_tick_event("evt_worker_market_index")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_index_event(connection, event, settings=settings)
    before_counts = _projection_counts(connection)

    result = process_projection_outbox_batch(connection, settings=settings, limit=10)
    after_counts = _projection_counts(connection)
    market_index = _outbox_row(connection, "market_index:evt_worker_market_index")
    market_regime = _outbox_row(connection, "market_regime:evt_worker_market_index")
    connection.close()

    assert result.claimed_count == 2
    assert result.applied_count == 1
    assert result.skipped_count == 1
    assert market_index["status"] == "APPLIED"
    assert market_regime["status"] == "SKIPPED"
    assert "MARKET_REGIME_SHADOW_VERIFY_UNSAFE" in market_regime["metadata_json"]
    assert after_counts == before_counts


def test_worker_missing_source_event_moves_to_error_then_dead_letter(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-missing-source.sqlite3")
    settings = Settings(
        projection_outbox_shadow_min_age_sec=0,
        projection_outbox_retry_limit=1,
    )
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id,
            projection_name,
            event_id,
            event_type,
            status,
            created_at,
            updated_at,
            metadata_json
        )
        VALUES (
            'market_data:evt_missing_source',
            'market_data',
            'evt_missing_source',
            'price_tick',
            'PENDING',
            ?,
            ?,
            '{}'
        )
        """,
        (datetime_to_wire(utc_now() - timedelta(seconds=2)), datetime_to_wire(utc_now())),
    )
    connection.commit()

    result = process_projection_outbox_batch(connection, settings=settings, limit=1)
    row = _outbox_row(connection, "market_data:evt_missing_source")
    connection.close()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.dead_letter_count == 1
    assert row["status"] == "DEAD_LETTER"
    assert row["attempts"] == 1
    assert "SOURCE_GATEWAY_EVENT_MISSING" in row["last_error"]


def test_mark_requires_matching_owner_and_dead_letter_threshold(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-owner.sqlite3")
    event = _price_tick_event("evt_worker_owner_guard")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    claimed = claim_projection_outbox_jobs(
        connection,
        owner_id="owner-a",
        limit=1,
        processing_ttl_sec=60,
        min_age_sec=0,
    )

    mark_projection_outbox_applied(
        connection,
        claimed[0]["outbox_id"],
        owner_id="owner-b",
        evidence={"attempt": "wrong-owner"},
    )
    unchanged = _outbox_row(connection, claimed[0]["outbox_id"])
    mark_projection_outbox_error(
        connection,
        claimed[0]["outbox_id"],
        owner_id="owner-a",
        error_message="forced worker failure",
        retry_limit=1,
    )
    dead = _outbox_row(connection, claimed[0]["outbox_id"])
    connection.close()

    assert unchanged["status"] == "PROCESSING"
    assert unchanged["locked_by"] == "owner-a"
    assert dead["status"] == "DEAD_LETTER"
    assert dead["attempts"] == 1
    assert dead["locked_by"] is None


def test_reset_stale_projection_outbox_processing_only_resets_old_jobs(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-reset.sqlite3")
    old_event = _price_tick_event("evt_worker_stale_old")
    fresh_event = _price_tick_event("evt_worker_stale_fresh")
    for event in (old_event, fresh_event):
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
    claim_projection_outbox_jobs(
        connection,
        owner_id="owner-reset",
        limit=2,
        processing_ttl_sec=60,
        min_age_sec=0,
    )
    old_locked_at = datetime_to_wire(utc_now() - timedelta(seconds=120))
    fresh_locked_at = datetime_to_wire(utc_now())
    connection.execute(
        "UPDATE projection_outbox SET locked_at = ? WHERE outbox_id = ?",
        (old_locked_at, "market_data:evt_worker_stale_old"),
    )
    connection.execute(
        "UPDATE projection_outbox SET locked_at = ? WHERE outbox_id = ?",
        (fresh_locked_at, "market_data:evt_worker_stale_fresh"),
    )
    connection.commit()

    reset_count = reset_stale_projection_outbox_processing(connection, stale_sec=60)
    old_row = _outbox_row(connection, "market_data:evt_worker_stale_old")
    fresh_row = _outbox_row(connection, "market_data:evt_worker_stale_fresh")
    connection.close()

    assert reset_count == 1
    assert old_row["status"] == "PENDING"
    assert old_row["locked_by"] is None
    assert fresh_row["status"] == "PROCESSING"
    assert fresh_row["locked_by"] == "owner-reset"


def test_projection_outbox_run_once_api_requires_token_and_is_shadow_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "projection-outbox-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC", "0")
    event = _price_tick_event("evt_worker_api").to_dict()

    with TestClient(app) as client:
        post = client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )
        missing_token = client.post("/api/operator/projection-outbox/run-once")
        run_once = client.post(
            "/api/operator/projection-outbox/run-once",
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        row = _outbox_row(connection, "market_data:evt_worker_api")
    finally:
        connection.close()

    assert post.status_code == 200
    assert missing_token.status_code == 401
    assert run_once.status_code == 200
    assert run_once.json()["shadow_mode"] is True
    assert run_once.json()["apply_projection"] is False
    assert run_once.json()["apply_projection_requested"] is False
    assert run_once.json()["apply_projection_effective"] is False
    assert run_once.json()["projection_side_effects_allowed"] is False
    assert run_once.json()["read_only_projection"] is True
    assert run_once.json()["mutated_projection_names"] == []
    assert run_once.json()["no_trading_side_effects"] is True
    assert run_once.json()["claimed_count"] == 1
    assert run_once.json()["applied_count"] == 1
    assert row["status"] == "APPLIED"


def _price_tick_event(
    event_id: str,
    *,
    price: int = 70_000,
    volume: int = 1_000,
    trade_value: int = 70_000_000,
    ts: datetime = TS,
) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=price,
        change_rate=0.1,
        volume=volume,
        trade_value=trade_value,
        execution_strength=101.5,
        best_bid=max(price - 100, 1),
        best_ask=price,
        spread_ticks=1,
        day_high=price + 500,
        day_low=max(price - 500, 1),
        trade_time=ts,
        ts=ts,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
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
        condition_id="cond-worker",
        condition_name="Worker Guard",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=70_000,
        metadata={"test": "projection_outbox_worker"},
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


def _outbox_row(connection, outbox_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()


def _projection_counts(connection) -> dict[str, int]:
    return {
        "market_ticks_latest": _count_rows(connection, "market_ticks_latest"),
        "market_tick_samples": _count_rows(connection, "market_tick_samples"),
        "market_index_tick_samples": _count_rows(connection, "market_index_tick_samples"),
        "market_regime_snapshots": _count_rows(connection, "market_regime_snapshots"),
    }


def _count_rows(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
