from __future__ import annotations

import json

from services.condition_fusion import list_condition_fusion
from services.config import Settings
from services.runtime.gateway_projection_routing import decide_market_data_projection_routing
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_gateway_market_data_condition_event_cutover import _insert_reconcile_run
from tests.test_market_data_condition_event_side_effects import _profile_condition_event


def test_worker_applies_effective_skipped_condition_event_and_refreshes_fusion(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-cutover-worker.sqlite3")
    settings = _cutover_settings()
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = _profile_condition_event("evt_condition_cutover_worker")
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    _prioritize_market_data_job(connection, event.event_id)

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )
    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    row = _outbox_row(connection, f"market_data:{event.event_id}")
    metadata = json.loads(row["metadata_json"])
    side_effects = metadata["last_worker_evidence"]["post_apply_side_effects"]
    fusion = list_condition_fusion(connection, settings=settings)
    candidate_source_count = _count_rows(connection, "candidate_sources_latest")
    signal_count = _table_event_count(
        connection,
        "market_condition_signals",
        event.event_id,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.applied_by_worker_count == 1
    assert row["status"] == "APPLIED"
    assert signal_count == 1
    assert fusion[0]["latest_event_id"] == event.event_id
    assert candidate_source_count == 0
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLIED_BY_WORKER"
    assert metadata["last_worker_evidence"]["append_only_cutover"][
        "gateway_inline_skipped"
    ] is True
    assert side_effects["condition_fusion_refresh_status"] == "APPLIED"
    assert side_effects["condition_fusion_processed_event_count"] == 1
    assert side_effects["condition_fusion_fused_code_count"] == 1
    assert side_effects["condition_code"] == "005930"
    assert side_effects["condition_action"] == "ENTER"
    assert side_effects["source"] == "projection_outbox_worker_condition_event"
    assert side_effects["deferred_from_gateway_path"] is True
    assert side_effects["candidate_ingest_executed"] is False
    assert side_effects["no_order_side_effects"] is True
    assert side_effects["no_trading_side_effects"] is True


def _cutover_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": True,
        "gateway_market_data_append_only_operating_mode": "CONDITION_EVENT_ONLY",
        "gateway_market_data_append_only_global_kill_switch": False,
        "gateway_market_data_append_only_global_max_skip_per_minute": 10,
        "gateway_market_data_append_only_condition_event_cutover_enabled": True,
        "gateway_market_data_append_only_condition_event_max_skip_per_minute": 10,
        "projection_outbox_shadow_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _prioritize_market_data_job(connection, event_id: str) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET priority = CASE WHEN projection_name = 'market_data' THEN 10 ELSE 0 END
        WHERE event_id = ?
        """,
        (event_id,),
    )
    connection.commit()


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


def _count_rows(connection, table_name: str) -> int:
    return int(
        connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()[
            "count"
        ]
    )
