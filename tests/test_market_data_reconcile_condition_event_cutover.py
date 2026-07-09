from __future__ import annotations

from domain.broker.utils import datetime_to_wire, utc_now
from services.market_data_service import process_gateway_event
from services.runtime.gateway_projection_routing import decide_market_data_projection_routing
from services.runtime.market_data_projection_reconcile import (
    run_market_data_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_gateway_market_data_condition_event_cutover import _insert_reconcile_run
from tests.test_market_data_condition_event_side_effects import _profile_condition_event
from tests.test_projection_outbox_condition_event_cutover_worker import _cutover_settings


def test_reconcile_warns_for_effective_skipped_condition_event_pending_within_sla(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-cutover-reconcile-pending.sqlite3")
    settings = _cutover_settings(projection_outbox_processing_ttl_sec=300)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = _profile_condition_event("evt_condition_reconcile_pending")
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "WARN"
    assert result.append_only_ready is False
    assert result.condition_event_effective_skip_count == 1
    assert result.condition_event_pending_within_sla_count == 1
    assert result.missing_projection_count == 0
    assert "MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_PENDING_WITHIN_SLA" in (
        result.reason_codes
    )


def test_reconcile_fails_for_terminal_condition_event_missing_artifact(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-cutover-reconcile-missing.sqlite3")
    settings = _cutover_settings()
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = _profile_condition_event("evt_condition_reconcile_missing")
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )
    _mark_market_data_outbox(
        connection,
        event.event_id,
        status="APPLIED",
        metadata={
            "last_worker_evidence": {
                "apply_mode": "MARKET_DATA_APPLY",
                "apply_result": "APPLIED_BY_WORKER",
            }
        },
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "FAIL"
    assert result.condition_event_artifact_missing_after_worker_count == 1
    assert (
        "MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_ARTIFACT_MISSING_AFTER_WORKER"
        in result.reason_codes
    )


def test_reconcile_fails_for_condition_event_missing_deferred_fusion_evidence(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "condition-cutover-reconcile-fusion.sqlite3")
    settings = _cutover_settings()
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = _profile_condition_event("evt_condition_reconcile_fusion_missing")
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )
    process_gateway_event(connection, event, settings=settings)
    _mark_market_data_outbox(
        connection,
        event.event_id,
        status="APPLIED",
        metadata={
            "last_worker_evidence": {
                "apply_mode": "MARKET_DATA_APPLY",
                "apply_result": "APPLIED_BY_WORKER",
            }
        },
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "FAIL"
    assert "CONDITION_EVENT_DEFERRED_FUSION_REFRESH_MISSING" in result.reason_codes


def _mark_market_data_outbox(
    connection,
    event_id: str,
    *,
    status: str,
    metadata: dict[str, object],
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?,
            updated_at = ?,
            processed_at = ?,
            metadata_json = ?,
            last_error = NULL
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (status, now, now, canonical_json(metadata), event_id),
    )
    connection.commit()
