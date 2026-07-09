from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.runtime.projection_outbox_bulk_retire import (
    bulk_retire_projection_outbox,
)
from storage.sqlite import initialize_database


def test_bulk_retire_price_tick_artifact_marks_applied(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-price.sqlite3")
    _insert_gateway_event(connection, "evt_price", "price_tick")
    _insert_outbox(connection, "market_data", "evt_price", "price_tick")
    _insert_price_tick_sample(connection, "evt_price")

    dry_run = bulk_retire_projection_outbox(connection, dry_run=True, older_than_sec=60)
    apply = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    row = _outbox(connection, "market_data:evt_price")
    connection.close()

    assert dry_run.retired_count == 1
    assert apply.applied_count == 1
    assert row["status"] == "APPLIED"
    metadata = json.loads(row["metadata_json"])
    assert metadata["bulk_retired"] is True
    assert (
        metadata["bulk_retire_reason"]
        == "INLINE_MARKET_DATA_PRICE_TICK_ARTIFACT_OBSERVED"
    )


def test_bulk_retire_condition_event_artifact_marks_applied(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-condition.sqlite3")
    _insert_gateway_event(connection, "evt_condition", "condition_event")
    _insert_outbox(connection, "market_data", "evt_condition", "condition_event")
    _insert_condition_signal(connection, "evt_condition")

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    row = _outbox(connection, "market_data:evt_condition")
    connection.close()

    assert result.applied_count == 1
    assert row["status"] == "APPLIED"


def test_bulk_retire_tr_response_artifact_and_empty_rows(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-tr.sqlite3")
    _insert_gateway_event(
        connection,
        "evt_tr_artifact",
        "tr_response",
        payload={"rows": [{"code": "005930"}]},
    )
    _insert_outbox(connection, "market_data", "evt_tr_artifact", "tr_response")
    _insert_tr_snapshot(connection, "evt_tr_artifact")
    _insert_gateway_event(
        connection,
        "evt_tr_empty",
        "tr_response",
        payload={"rows": []},
    )
    _insert_outbox(connection, "market_data", "evt_tr_empty", "tr_response")

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    artifact = _outbox(connection, "market_data:evt_tr_artifact")
    empty = _outbox(connection, "market_data:evt_tr_empty")
    connection.close()

    assert result.applied_count == 1
    assert result.skipped_count == 1
    assert artifact["status"] == "APPLIED"
    assert empty["status"] == "SKIPPED"
    assert "INLINE_MARKET_DATA_TR_RESPONSE_ROWS_EMPTY" in empty["metadata_json"]


def test_bulk_retire_market_index_and_market_regime(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-index.sqlite3")
    _insert_gateway_event(connection, "evt_index", "market_index_tick")
    _insert_outbox(connection, "market_index", "evt_index", "market_index_tick")
    _insert_outbox(connection, "market_regime", "evt_index", "market_index_tick")
    _insert_market_index_sample(connection, "evt_index")

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    market_index = _outbox(connection, "market_index:evt_index")
    market_regime = _outbox(connection, "market_regime:evt_index")
    connection.close()

    assert result.applied_count == 1
    assert result.skipped_count == 1
    assert market_index["status"] == "APPLIED"
    assert market_regime["status"] == "SKIPPED"
    assert "MARKET_REGIME_SHADOW_RETIRE_UNSAFE_NOT_BLOCKING" in market_regime[
        "metadata_json"
    ]


def test_bulk_retire_condition_fusion_old_unverifiable_skipped_recent_not_eligible(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "bulk-fusion.sqlite3")
    _insert_gateway_event(connection, "evt_fusion_old", "condition_event")
    _insert_outbox(connection, "condition_fusion", "evt_fusion_old", "condition_event")
    _insert_condition_signal(connection, "evt_fusion_old")
    _insert_gateway_event(connection, "evt_fusion_recent", "condition_event")
    _insert_outbox(
        connection,
        "condition_fusion",
        "evt_fusion_recent",
        "condition_event",
        created_at=datetime_to_wire(utc_now()),
    )
    _insert_condition_signal(connection, "evt_fusion_recent", code="000660")

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    old_row = _outbox(connection, "condition_fusion:evt_fusion_old")
    recent_row = _outbox(connection, "condition_fusion:evt_fusion_recent")
    connection.close()

    assert result.skipped_count == 1
    assert old_row["status"] == "SKIPPED"
    assert recent_row["status"] == "PENDING"


def test_bulk_retire_excludes_effective_skip_and_missing_artifact(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-exclude.sqlite3")
    _insert_gateway_event(connection, "evt_skip", "price_tick")
    _insert_outbox(connection, "market_data", "evt_skip", "price_tick")
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
            event_id, event_type, projection_name, effective_skip_inline, decided_at
        )
        VALUES ('evt_skip', 'price_tick', 'market_data', 1, ?)
        """,
        (datetime_to_wire(utc_now()),),
    )
    _insert_gateway_event(connection, "evt_missing", "price_tick")
    _insert_outbox(connection, "market_data", "evt_missing", "price_tick")
    connection.commit()

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    skip = _outbox(connection, "market_data:evt_skip")
    missing = _outbox(connection, "market_data:evt_missing")
    connection.close()

    assert result.retired_count == 0
    assert result.not_eligible_count == 2
    assert skip["status"] == "PENDING"
    assert missing["status"] == "PENDING"


def test_bulk_retire_error_dead_letter_untouched(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bulk-terminal.sqlite3")
    _insert_gateway_event(connection, "evt_error", "price_tick")
    _insert_outbox(connection, "market_data", "evt_error", "price_tick", status="ERROR")
    _insert_gateway_event(connection, "evt_dead", "price_tick")
    _insert_outbox(
        connection,
        "market_data",
        "evt_dead",
        "price_tick",
        status="DEAD_LETTER",
    )

    result = bulk_retire_projection_outbox(connection, dry_run=False, older_than_sec=60)
    error = _outbox(connection, "market_data:evt_error")
    dead = _outbox(connection, "market_data:evt_dead")
    connection.close()

    assert result.scanned_count == 0
    assert error["status"] == "ERROR"
    assert dead["status"] == "DEAD_LETTER"


def _insert_gateway_event(
    connection,
    event_id: str,
    event_type: str,
    *,
    payload: dict | None = None,
    status: str = "ACCEPTED",
) -> None:
    ts = datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO gateway_events (
            event_id, event_type, source, event_ts, received_at, payload_json, status
        )
        VALUES (?, ?, 'test-gateway', ?, ?, ?, ?)
        """,
        (
            event_id,
            event_type,
            ts,
            ts,
            json.dumps(payload or {}, ensure_ascii=False),
            status,
        ),
    )
    connection.commit()


def _insert_outbox(
    connection,
    projection_name: str,
    event_id: str,
    event_type: str,
    *,
    status: str = "PENDING",
    created_at: str | None = None,
) -> None:
    wire = created_at or datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id, projection_name, event_id, event_type, status,
            created_at, updated_at, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}')
        """,
        (
            f"{projection_name}:{event_id}",
            projection_name,
            event_id,
            event_type,
            status,
            wire,
            wire,
        ),
    )
    connection.commit()


def _insert_price_tick_sample(connection, event_id: str) -> None:
    ts = datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO market_tick_samples (
            event_id, code, exchange, session, price, cumulative_volume,
            cumulative_trade_value, volume_delta, trade_value_delta,
            execution_strength, event_ts, received_at, source, metadata_json
        )
        VALUES (?, '005930', 'KRX', 'REGULAR', 70000, 1, 70000, 1, 70000,
            100.0, ?, ?, 'test', '{}')
        """,
        (event_id, ts, ts),
    )
    connection.commit()


def _insert_condition_signal(connection, event_id: str, *, code: str = "005930") -> None:
    ts = datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO market_condition_signals (
            event_id, condition_id, condition_name, code, name, action,
            price, event_ts, received_at, source, metadata_json
        )
        VALUES (?, 'cond', '조건', ?, '삼성전자', 'ENTER', 70000, ?, ?, 'test', '{}')
        """,
        (event_id, code, ts, ts),
    )
    connection.commit()


def _insert_tr_snapshot(connection, event_id: str) -> None:
    ts = datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO market_tr_snapshots (
            event_id, request_id, tr_code, request_name, code, row_json,
            event_ts, received_at, source
        )
        VALUES (?, 'req', 'OPT10001', 'test', '005930', '{}', ?, ?, 'test')
        """,
        (event_id, ts, ts),
    )
    connection.commit()


def _insert_market_index_sample(connection, event_id: str) -> None:
    ts = datetime_to_wire(utc_now() - timedelta(minutes=10))
    connection.execute(
        """
        INSERT INTO market_index_tick_samples (
            event_id, index_code, index_name, price, change_rate, change_value,
            trade_time, event_ts, received_at, source, metadata_json
        )
        VALUES (?, 'KOSPI', 'KOSPI', 3000.0, 0.1, 3.0, ?, ?, ?, 'test', '{}')
        """,
        (event_id, ts, ts, ts),
    )
    connection.commit()


def _outbox(connection, outbox_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()
