from __future__ import annotations

import json

from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime.market_data_projection_reconcile import (
    run_market_data_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_market_data_condition_event_side_effects import _profile_condition_event


def test_reconcile_passes_worker_applied_condition_event_side_effect(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-reconcile-pass.sqlite3")
    settings = _condition_apply_settings()
    event = _profile_condition_event("evt_condition_reconcile_worker")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    _prioritize_market_data_job(connection, event.event_id)
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert worker.applied_by_worker_count == 1
    assert result.status == "PASS"
    assert result.append_only_ready is True
    assert result.condition_event_worker_applied_count == 1
    assert result.condition_event_deferred_fusion_refresh_count == 1
    assert result.condition_event_deferred_fusion_refresh_error_count == 0
    assert result.condition_event_candidate_ingest_in_worker_count == 0
    assert result.condition_event_side_effect_duplicate_count == 0


def test_reconcile_warns_for_condition_event_effective_skip_pending_within_sla(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-reconcile-effective.sqlite3")
    settings = _condition_apply_settings(projection_outbox_processing_ttl_sec=300)
    event = _profile_condition_event("evt_condition_reconcile_effective_skip")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
            event_id,
            event_type,
            projection_name,
            dry_run_enabled,
            cutover_enabled,
            reconcile_required,
            append_only_ready,
            outbox_status,
            outbox_job_present,
            would_skip_inline,
            effective_skip_inline,
            cutover_scope,
            worker_apply_enabled,
            fallback_inline_projection_expected,
            blocked_reason_codes_json,
            evidence_json
        )
        VALUES (?, 'condition_event', 'market_data', 1, 1, 0, 1, 'ENQUEUED',
            1, 1, 1, 'price_tick_tr_response_condition_event', 1, 0, ?, ?)
        """,
        (
            event.event_id,
            json.dumps(["CONDITION_EVENT_EFFECTIVE_SKIP_ALLOWED"]),
            canonical_json({"test": "condition_event_effective_skip_pending"}),
        ),
    )
    connection.commit()

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "WARN"
    assert result.append_only_ready is False
    assert result.condition_event_effective_skip_count == 1
    assert result.condition_event_pending_within_sla_count == 1
    assert result.invalid_effective_skip_count == 0
    assert "MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_PENDING_WITHIN_SLA" in (
        result.reason_codes
    )


def test_reconcile_fails_if_worker_candidate_ingest_is_reported(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-reconcile-candidate.sqlite3")
    settings = _condition_apply_settings()
    event = _profile_condition_event("evt_condition_reconcile_candidate_ingest")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    _mark_market_data_outbox(
        connection,
        event.event_id,
        _worker_evidence(
            event.event_id,
            apply_result="APPLIED_BY_WORKER",
            candidate_ingest_executed=True,
        ),
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.condition_event_candidate_ingest_in_worker_count == 1
    assert "CONDITION_EVENT_CANDIDATE_INGEST_IN_WORKER_FORBIDDEN" in (
        result.reason_codes
    )


def test_reconcile_fails_on_inline_condition_event_duplicate_side_effect(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-reconcile-duplicate.sqlite3")
    settings = _condition_apply_settings()
    event = _profile_condition_event("evt_condition_reconcile_duplicate")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)
    _mark_market_data_outbox(
        connection,
        event.event_id,
        _worker_evidence(event.event_id, apply_result="APPLIED_BY_VERIFY"),
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.condition_event_side_effect_duplicate_count == 1
    assert "CONDITION_EVENT_SIDE_EFFECT_DUPLICATED" in result.reason_codes


def _condition_apply_settings(**overrides) -> Settings:
    values = {
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


def _worker_evidence(
    event_id: str,
    *,
    apply_result: str,
    candidate_ingest_executed: bool = False,
) -> dict[str, object]:
    return {
        "last_worker_evidence": {
            "apply_mode": "MARKET_DATA_APPLY",
            "apply_result": apply_result,
            "post_apply_side_effects": {
                "condition_fusion_refresh_status": "APPLIED",
                "condition_fusion_processed_event_count": 1,
                "condition_fusion_fused_code_count": 1,
                "condition_code": "005930",
                "source": "projection_outbox_worker_condition_event",
                "deferred_from_gateway_path": True,
                "candidate_ingest_executed": candidate_ingest_executed,
                "no_order_side_effects": True,
                "no_trading_side_effects": True,
                "evidence": {"event_id": event_id},
            },
        },
    }


def _mark_market_data_outbox(
    connection,
    event_id: str,
    metadata: dict[str, object],
) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'APPLIED',
            metadata_json = ?,
            last_error = NULL
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (canonical_json(metadata), event_id),
    )
    connection.commit()
