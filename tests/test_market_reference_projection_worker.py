from __future__ import annotations

import json
from datetime import UTC, datetime

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.market_reference_service import process_market_symbols_event
from services.runtime import projection_outbox_worker
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_market_reference_worker_observes_inline_artifact_without_reapply(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-verify.sqlite3")
    event = _market_symbols_event("evt_ref_verify", "005930")
    append_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    def fail_reapply(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AssertionError("market_reference inline artifact should not be reapplied")

    monkeypatch.setattr(
        projection_outbox_worker,
        "process_market_symbols_event",
        fail_reapply,
    )
    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=_settings(
            projection_outbox_apply_projection_enabled=True,
            projection_outbox_market_reference_apply_enabled=True,
        ),
        limit=1,
        apply_projection=True,
    )
    row = _outbox_row(connection, "market_reference:evt_ref_verify")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert result.applied_by_verify_count == 1
    assert result.applied_by_worker_count == 0
    assert row["status"] == "APPLIED"
    assert metadata["last_worker_evidence"]["verification_reason"] == (
        "INLINE_ARTIFACT_OBSERVED"
    )


def test_market_reference_worker_apply_creates_memberships(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-apply.sqlite3")
    event = _market_symbols_event("evt_ref_apply", "005930")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=_settings(
            projection_outbox_apply_projection_enabled=True,
            projection_outbox_market_reference_apply_enabled=True,
        ),
        limit=1,
        apply_projection=True,
    )
    row = _outbox_row(connection, "market_reference:evt_ref_apply")
    membership_count = _count_rows(connection, "market_symbol_memberships")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_reference",)
    assert row["status"] == "APPLIED"
    assert membership_count == 1
    assert metadata["last_worker_evidence"]["apply_mode"] == "MARKET_REFERENCE_APPLY"
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLIED_BY_WORKER"


def test_market_reference_worker_skips_empty_symbols(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-empty.sqlite3")
    event = GatewayEvent(
        event_id="evt_ref_empty",
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {}},
        ts=TS,
    )
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=_settings(
            projection_outbox_apply_projection_enabled=True,
            projection_outbox_market_reference_apply_enabled=True,
        ),
        limit=1,
        apply_projection=True,
    )
    row = _outbox_row(connection, "market_reference:evt_ref_empty")
    connection.close()

    assert result.skipped_count == 1
    assert row["status"] == "SKIPPED"
    assert "MARKET_REFERENCE_NO_SYMBOLS" in row["metadata_json"]


def test_market_reference_worker_supports_dict_and_list_payload_shapes(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-shapes.sqlite3")
    dict_event = _market_symbols_event("evt_ref_dict", "005930")
    list_event = GatewayEvent(
        event_id="evt_ref_list",
        event_type="market_symbols",
        source="test-gateway",
        payload={
            "markets": [
                {
                    "market": "KOSDAQ",
                    "symbols": [{"code": "035420", "name": "NAVER"}],
                }
            ]
        },
        ts=TS,
    )
    for event in (dict_event, list_event):
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=_settings(
            projection_outbox_apply_projection_enabled=True,
            projection_outbox_market_reference_apply_enabled=True,
        ),
        limit=2,
        apply_projection=True,
    )
    rows = connection.execute(
        "SELECT code, market FROM market_symbol_memberships ORDER BY code"
    ).fetchall()
    connection.close()

    assert result.applied_by_worker_count == 2
    assert [(row["code"], row["market"]) for row in rows] == [
        ("005930", "KOSPI"),
        ("035420", "KOSDAQ"),
    ]


def test_market_reference_worker_filter_leaves_market_data_pending(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-reference-worker-filter.sqlite3")
    price_event = GatewayEvent(
        event_id="evt_ref_filter_price",
        event_type="price_tick",
        source="test-gateway",
        payload={"code": "005930"},
        ts=TS,
    )
    reference_event = _market_symbols_event("evt_ref_filter_reference", "005930")
    for event in (price_event, reference_event):
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=_settings(
            projection_outbox_apply_projection_enabled=True,
            projection_outbox_market_reference_apply_enabled=True,
        ),
        limit=1,
        apply_projection=True,
        projection_name="market_reference",
    )
    price_row = _outbox_row(connection, "market_data:evt_ref_filter_price")
    reference_row = _outbox_row(
        connection,
        "market_reference:evt_ref_filter_reference",
    )
    connection.close()

    assert result.projection_name_filter == "market_reference"
    assert result.claimed_count == 1
    assert result.applied_by_worker_count == 1
    assert price_row["status"] == "PENDING"
    assert reference_row["status"] == "APPLIED"


def _settings(**overrides) -> Settings:
    values = {
        "projection_outbox_shadow_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_market_reference_apply_min_age_sec": 0,
        "projection_outbox_market_data_apply_enabled": False,
    }
    values.update(overrides)
    return Settings(**values)


def _market_symbols_event(event_id: str, code: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {"KOSPI": [{"code": code, "name": "삼성전자"}]}},
        ts=TS,
    )


def _outbox_row(connection, outbox_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()


def _count_rows(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
