from __future__ import annotations

import json

from services.condition_fusion import list_condition_fusion
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from services.market_data_service import process_gateway_event
from services.runtime.gateway_projection_routing import (
    decide_market_data_projection_routing,
    get_latest_market_data_append_only_routing_status,
)
from services.runtime.market_data_projection_side_effects import (
    refresh_condition_fusion_for_condition_event_projection,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_market_data_condition_event_side_effects import _profile_condition_event


def test_worker_apply_condition_event_runs_deferred_condition_fusion_refresh(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-worker-apply.sqlite3")
    settings = _condition_apply_settings(
        gateway_market_data_append_only_condition_event_dry_run_enabled=True,
    )
    event = _profile_condition_event("evt_condition_worker_apply")
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)
    _prioritize_market_data_job(connection, event.event_id)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
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
    routing = connection.execute(
        """
        SELECT post_apply_deferred_side_effects_json
        FROM market_data_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    routing_side_effects = json.loads(routing["post_apply_deferred_side_effects_json"])
    fusion = list_condition_fusion(connection, settings=settings)
    candidate_source_count = _count_rows(connection, "candidate_sources_latest")
    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    dashboard = build_dashboard_snapshot(connection, settings)
    connection.close()

    assert decision.effective_skip_inline is False
    assert "DRY_RUN_DISABLED" in decision.blocked_reason_codes
    assert result.applied_by_worker_count == 1
    assert row["status"] == "APPLIED"
    assert side_effects["condition_fusion_refresh_status"] == "APPLIED"
    assert side_effects["condition_fusion_processed_event_count"] == 1
    assert side_effects["condition_fusion_fused_code_count"] == 1
    assert side_effects["condition_code"] == "005930"
    assert side_effects["source"] == "projection_outbox_worker_condition_event"
    assert side_effects["deferred_from_gateway_path"] is True
    assert side_effects["candidate_ingest_executed"] is False
    assert side_effects["no_order_side_effects"] is True
    assert side_effects["no_trading_side_effects"] is True
    assert routing_side_effects["condition_fusion_refresh_status"] == "APPLIED"
    assert fusion[0]["latest_event_id"] == event.event_id
    assert candidate_source_count == 0
    assert status["condition_event_effective_skip_count"] == 0
    assert status["condition_event_deferred_side_effect_count"] == 1
    assert status["condition_event_deferred_side_effect_error_count"] == 0
    assert status["condition_event_candidate_ingest_executed_count"] == 0
    assert status["condition_event_side_effect_duplicate_count"] == 0
    assert status["condition_event_worker_side_effect_ready"] is True
    summary = dashboard["pipeline_summary"]["market_data_append_only_routing"]
    assert summary["condition_event_side_effect_migration_status"] == (
        "WORKER_DEFERRED_READY"
    )
    assert summary["condition_event_deferred_fusion_refresh_count"] == 1
    assert summary["condition_event_candidate_ingest_status"] == "NOT_IN_WORKER"
    assert "PR-11 condition_event limited cutover is feature-flagged" in (
        summary["warnings"]
    )


def test_worker_does_not_duplicate_condition_fusion_after_inline_apply(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-worker-inline.sqlite3")
    settings = _condition_apply_settings()
    event = _profile_condition_event("evt_condition_worker_inline")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _prioritize_market_data_job(connection, event.event_id)
    process_gateway_event(connection, event, settings=settings)
    inline_result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=settings,
        source="gateway_inline_condition_event",
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    row = _outbox_row(connection, f"market_data:{event.event_id}")
    metadata = json.loads(row["metadata_json"])
    connection.close()

    assert inline_result.status == "APPLIED"
    assert result.applied_by_verify_count == 1
    assert result.applied_by_worker_count == 0
    assert row["status"] == "APPLIED"
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLIED_BY_VERIFY"
    assert "post_apply_side_effects" not in metadata["last_worker_evidence"]


def test_worker_condition_event_side_effect_skips_when_fusion_disabled(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-worker-fusion-disabled.sqlite3")
    settings = _condition_apply_settings(
        condition_fusion_event_incremental_enabled=False,
        gateway_market_data_append_only_condition_event_dry_run_enabled=True,
    )
    event = _profile_condition_event("evt_condition_worker_disabled")
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)
    _prioritize_market_data_job(connection, event.event_id)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
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
    fusion = list_condition_fusion(connection, settings=Settings())
    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.effective_skip_inline is False
    assert "CONDITION_EVENT_FUSION_DISABLED" in decision.blocked_reason_codes
    assert result.applied_by_worker_count == 1
    assert row["status"] == "APPLIED"
    assert side_effects["condition_fusion_refresh_status"] == "SKIPPED"
    assert "CONDITION_FUSION_INCREMENTAL_DISABLED" in (
        side_effects["condition_fusion_reason_codes"]
    )
    assert side_effects["candidate_ingest_executed"] is False
    assert fusion == []
    assert status["condition_event_worker_side_effect_ready"] is False
    assert status["condition_event_deferred_side_effect_error_count"] == 0


def test_condition_event_effective_skip_requires_positive_skip_budget(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-effective-skip-pr10.sqlite3")
    settings = _condition_apply_settings(
        gateway_market_data_append_only_dry_run_enabled=True,
        gateway_market_data_append_only_cutover_enabled=True,
        gateway_market_data_append_only_condition_event_dry_run_enabled=True,
        gateway_market_data_append_only_condition_event_cutover_enabled=True,
        gateway_market_data_append_only_condition_event_require_backlog_ready=False,
        gateway_market_data_append_only_cutover_event_types=(
            "price_tick",
            "condition_event",
            "tr_response",
        ),
        gateway_market_data_append_only_require_reconcile_pass=False,
        gateway_market_data_append_only_require_latest_reconcile_pass=False,
    )
    event = _profile_condition_event("evt_condition_effective_skip_pr10")
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
    )
    projection = process_gateway_event(connection, event, settings=settings)
    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert projection.status == "APPLIED"
    assert "CONDITION_EVENT_SKIP_BUDGET_EXHAUSTED" in decision.blocked_reason_codes
    assert status["condition_event_effective_skip_count"] == 0
    assert status["invalid_effective_skip_count"] == 0


def _condition_apply_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_require_reconcile_pass": False,
        "gateway_market_data_append_only_require_latest_reconcile_pass": False,
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


def _count_rows(connection, table_name: str) -> int:
    return int(
        connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()[
            "count"
        ]
    )
