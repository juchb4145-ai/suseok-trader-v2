from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest
from services.pipeline_coherency import build_pipeline_coherency_rca_status
from services.pipeline_coherency_disposition import (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    ACTION_REVOKE,
    PipelineCoherencyDispositionError,
    _issue_orphan_apply_approval_context,
    preview_pipeline_coherency_disposition,
    record_pipeline_coherency_disposition,
    resolve_pipeline_coherency_dispositions,
    revoke_pipeline_coherency_disposition,
)
from services.pipeline_orphan_manual_evidence import (
    ORPHAN_EVIDENCE_CONTRACT,
    StableJsonDocument,
    build_orphan_apply_evidence_binding,
    validate_orphan_manual_evidence_document,
)
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import (
    _prepared_order_plan_connection,
)


def test_expired_plan_disposition_is_cas_guarded_append_only(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "expired-plan.sqlite3"
    )

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    recorded = record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-one"),
    )
    repeated = record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-one"),
    )
    status = build_pipeline_coherency_rca_status(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        disposition_resolver=lambda resolved_trade_date, subjects: (
            resolve_pipeline_coherency_dispositions(
                connection,
                resolved_trade_date,
                subjects,
            )
        ),
    )
    raw_stage_count = connection.execute(
        "SELECT COUNT(*) FROM entry_timing_evaluations"
    ).fetchone()[0]
    connection.close()

    assert preview["eligible"] is True
    assert recorded["action"] == ACTION_DISPOSE_EXPIRED_PLAN_READY
    assert repeated["disposition_id"] == recorded["disposition_id"]
    assert status["canonical_status"] == "FAIL"
    assert status["qualification_status"] == "PASS"
    assert status["disposition_pending_count"] == 0
    assert status["items"][0]["effective_disposition"]["effective"] is True
    assert raw_stage_count == 1


def test_disposition_late_downstream_evidence_invalidates_effective_cas(
    tmp_path,
) -> None:
    connection, candidate_id, trade_date, order_plan_id = _expired_closed_pipeline(
        tmp_path / "late-downstream.sqlite3"
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-late"),
    )
    plan = connection.execute(
        """
        SELECT strategy_observation_id, risk_observation_id, code, name
        FROM order_plan_drafts
        WHERE order_plan_id = ?
        """,
        (order_plan_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id, candidate_instance_id,
            strategy_observation_id, risk_observation_id, order_plan_id,
            trade_date, account_id, code, name, side, order_type, quantity,
            limit_price, notional, status, idempotency_key,
            live_sim_only, live_real_allowed, broker_order_sent,
            created_at, expires_at
        ) VALUES (
            'late-intent', ?, ?, ?, ?, ?, 'SIM_REDACTED', ?, ?, 'BUY',
            'LIMIT', 1, 1, 1, 'CREATED', 'late-intent-key', 1, 0, 0,
            '2026-07-15T00:00:00Z', '2026-07-15T00:10:00Z'
        )
        """,
        (
            candidate_id,
            plan["strategy_observation_id"],
            plan["risk_observation_id"],
            order_plan_id,
            trade_date,
            plan["code"],
            plan["name"],
        ),
    )
    connection.commit()

    status = build_pipeline_coherency_rca_status(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        disposition_resolver=lambda resolved_trade_date, subjects: (
            resolve_pipeline_coherency_dispositions(
                connection,
                resolved_trade_date,
                subjects,
            )
        ),
    )
    connection.close()

    effective = status["items"][0]["effective_disposition"]
    assert effective["effective"] is False
    assert effective["status"] == "INVALID_CAS"
    assert status["qualification_status"] == "BLOCKED"


def test_disposition_source_payload_change_invalidates_effective_cas(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "source-payload.sqlite3"
    )
    candidate = connection.execute(
        "SELECT code, name FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active,
            reason_codes_json, payload_json
        ) VALUES (
            'source-cas-event', ?, ?, ?, ?, 'TEST_SOURCE', 'source-cas',
            'REMOVE', '2026-07-14T00:00:00Z', '2026-07-14T00:00:00Z', 0,
            '[]', '{"version":1}'
        )
        """,
        (candidate_id, trade_date, candidate["code"], candidate["name"]),
    )
    connection.commit()
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-source-payload"),
    )
    connection.execute(
        """
        UPDATE candidate_source_events
        SET payload_json = '{"late":"material-change"}'
        WHERE source_event_id = (
            SELECT source_event_id FROM candidate_source_events
            WHERE candidate_instance_id = ?
            ORDER BY event_ts, source_event_id
            LIMIT 1
        )
        """,
        (candidate_id,),
    )
    connection.commit()

    status = _resolved_status(connection, candidate_id=candidate_id, trade_date=trade_date)
    connection.close()

    assert status["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert status["qualification_status"] == "BLOCKED"


def test_disposition_late_dry_run_order_and_execution_invalidate_cas(tmp_path) -> None:
    connection, candidate_id, trade_date, order_plan_id = _expired_closed_pipeline(
        tmp_path / "late-dry-order.sqlite3"
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-late-dry"),
    )
    plan = connection.execute(
        """
        SELECT strategy_observation_id, risk_observation_id, code, name
        FROM order_plan_drafts WHERE order_plan_id = ?
        """,
        (order_plan_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO dry_run_intents (
            dry_run_intent_id, candidate_instance_id, strategy_observation_id,
            risk_observation_id, trade_date, code, name, side, order_type,
            intended_price, quantity, notional, status, source, observe_only,
            dry_run_only, live_order_allowed, gateway_command_allowed, created_at
        ) VALUES (
            'late-dry-intent', ?, ?, ?, ?, ?, ?, 'BUY', 'LIMIT', 1, 1, 1,
            'CONVERTED_TO_DRY_RUN_ORDER', 'test', 1, 1, 0, 0,
            '2026-07-15T00:00:00Z'
        )
        """,
        (
            candidate_id,
            plan["strategy_observation_id"],
            plan["risk_observation_id"],
            trade_date,
            plan["code"],
            plan["name"],
        ),
    )
    connection.execute(
        """
        INSERT INTO dry_run_orders (
            dry_run_order_id, dry_run_intent_id, trade_date, code, name, side,
            order_type, quantity, requested_price, simulated_fill_price,
            filled_quantity, remaining_quantity, status, dry_run_only, created_at
        ) VALUES (
            'late-dry-order', 'late-dry-intent', ?, ?, ?, 'BUY', 'LIMIT', 1,
            1, 1, 1, 0, 'SIMULATED_FILLED', 1, '2026-07-15T00:00:01Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO dry_run_executions (
            dry_run_execution_id, dry_run_order_id, dry_run_intent_id,
            trade_date, code, side, quantity, price, notional, executed_at,
            dry_run_only
        ) VALUES (
            'late-dry-execution', 'late-dry-order', 'late-dry-intent', ?, ?,
            'BUY', 1, 1, 1, '2026-07-15T00:00:02Z', 1
        )
        """,
        (trade_date, plan["code"]),
    )
    connection.commit()

    status = _resolved_status(connection, candidate_id=candidate_id, trade_date=trade_date)
    connection.close()

    assert status["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert status["qualification_status"] == "BLOCKED"


@pytest.mark.parametrize("object_kind", ["trigger", "index"])
def test_disposition_schema_rejects_same_name_counterfeit_object(
    tmp_path,
    object_kind: str,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / f"counterfeit-{object_kind}.sqlite3"
    )
    if object_kind == "trigger":
        connection.execute("DROP TRIGGER trg_pipeline_coherency_dispositions_no_update")
        connection.execute(
            """
            CREATE TRIGGER trg_pipeline_coherency_dispositions_no_update
            BEFORE UPDATE ON pipeline_coherency_dispositions
            BEGIN SELECT 1; END
            """
        )
    else:
        connection.execute("DROP INDEX idx_pipeline_coherency_disposition_effective")
        connection.execute(
            """
            CREATE INDEX idx_pipeline_coherency_disposition_effective
            ON pipeline_coherency_dispositions (sequence_no, subject_key)
            """
        )
    connection.commit()

    with pytest.raises(PipelineCoherencyDispositionError) as raised:
        preview_pipeline_coherency_disposition(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
        )
    connection.close()

    assert raised.value.code == "PIPELINE_DISPOSITION_SCHEMA_NOT_READY"


def test_pipeline_disposition_request_conflict_and_revoke_chain(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(tmp_path / "revoke.sqlite3")
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-conflict"),
    )
    conflict = _record_kwargs(preview, request_id="pipeline-request-conflict")
    conflict["reason_code"] = "DIFFERENT_REASON"
    try:
        record_pipeline_coherency_disposition(connection, **conflict)
    except PipelineCoherencyDispositionError as exc:
        assert exc.code == "PIPELINE_DISPOSITION_REQUEST_CONFLICT"
    else:  # pragma: no cover - defensive contract assertion
        raise AssertionError("conflicting request_id must fail")

    revoke_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_REVOKE,
    )
    revoked = revoke_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(revoke_preview, request_id="pipeline-request-revoke"),
    )
    status = build_pipeline_coherency_rca_status(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        disposition_resolver=lambda resolved_trade_date, subjects: (
            resolve_pipeline_coherency_dispositions(
                connection,
                resolved_trade_date,
                subjects,
            )
        ),
    )
    connection.close()

    assert revoke_preview["eligible"] is True
    assert revoked["action"] == ACTION_REVOKE
    assert revoked["sequence_no"] == 2
    assert status["disposition_pending_count"] == 1
    assert status["qualification_status"] == "BLOCKED"


def test_orphan_and_stale_other_date_have_distinct_guarded_actions(tmp_path) -> None:
    orphan = initialize_database(tmp_path / "orphan.sqlite3")
    orphan.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (
            'orphan-candidate', 'orphan-risk', 'missing-strategy',
            '2026-07-15', '005930', 'fixture', '2026-07-15T00:00:00Z',
            'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0, '[]', 'test', 1
        )
        """
    )
    orphan.commit()
    orphan_preview = preview_pipeline_coherency_disposition(
        orphan,
        trade_date="2026-07-15",
        candidate_instance_id="orphan-candidate",
        action=ACTION_DISPOSE_ORPHAN,
    )
    orphan.close()

    stale, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "stale.sqlite3",
        state="STALE",
        candidate_trade_date="2026-06-26",
    )
    stale_preview = preview_pipeline_coherency_disposition(
        stale,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_STALE_OTHER_DATE,
    )
    stale.close()

    assert orphan_preview["eligible"] is True
    assert orphan_preview["expected_action"] == ACTION_DISPOSE_ORPHAN
    assert stale_preview["eligible"] is True
    assert stale_preview["expected_action"] == ACTION_DISPOSE_STALE_OTHER_DATE


def test_orphan_disposition_rejects_generic_evidence_binding(tmp_path) -> None:
    connection = initialize_database(tmp_path / "orphan-generic-evidence.sqlite3")
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (
            'orphan-generic', 'orphan-generic-risk', 'missing-strategy',
            '2026-07-15', '005930', 'fixture', '2026-07-15T00:00:00Z',
            'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0, '[]', 'test', 1
        )
        """
    )
    connection.commit()
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date="2026-07-15",
        candidate_instance_id="orphan-generic",
        action=ACTION_DISPOSE_ORPHAN,
    )

    with pytest.raises(PipelineCoherencyDispositionError) as exc_info:
        record_pipeline_coherency_disposition(
            connection,
            **_record_kwargs(preview, request_id="pipeline-request-orphan-generic"),
        )
    row_count = connection.execute(
        "SELECT COUNT(*) FROM pipeline_coherency_dispositions"
    ).fetchone()[0]
    connection.close()

    assert exc_info.value.code == "PIPELINE_ORPHAN_EVIDENCE_INVALID"
    assert row_count == 0


def test_orphan_disposition_rejects_new_append_without_cli_approval_context(tmp_path) -> None:
    connection = initialize_database(tmp_path / "orphan-no-approval-context.sqlite3")
    preview = _insert_orphan_subject(connection, candidate_id="orphan-no-approval-context")
    kwargs = _orphan_record_kwargs(
        preview,
        request_id="pipeline-request-orphan-no-approval-context",
    )
    kwargs.pop("orphan_approval_context")

    with pytest.raises(PipelineCoherencyDispositionError) as exc_info:
        record_pipeline_coherency_disposition(connection, **kwargs)

    assert exc_info.value.code == "PIPELINE_ORPHAN_APPROVAL_CONTEXT_REQUIRED"
    assert (
        connection.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0]
        == 0
    )
    connection.close()


def test_orphan_disposition_future_as_of_cannot_move_approval_ceiling(tmp_path) -> None:
    connection = initialize_database(tmp_path / "orphan-future-approval.sqlite3")
    preview = _insert_orphan_subject(connection, candidate_id="orphan-future-approval")
    kwargs = _orphan_record_kwargs(
        preview,
        request_id="pipeline-request-orphan-future-approval",
    )
    binding = kwargs["evidence_json"]["evidence_binding"]
    future_wrapper = build_orphan_apply_evidence_binding(
        binding,
        approved_preflight_report_sha256="3" * 64,
        approved_private_manifest_sha256="4" * 64,
        source_plan_report_sha256="5" * 64,
        source_reconciliation_report_sha256="6" * 64,
        approved_database_main_sha256="7" * 64,
        approved_database_main_size=1,
        broker_scope_sha256="2" * 64,
        preflight_generated_at="2099-01-01T00:00:00Z",
    )
    kwargs["evidence_json"] = future_wrapper
    kwargs["orphan_approval_context"] = _issue_orphan_apply_approval_context(
        future_wrapper,
        verified_preflight_report_sha256="3" * 64,
        verified_database_main_sha256="7" * 64,
    )
    kwargs["as_of"] = datetime(2099, 1, 1, 1, 0, tzinfo=UTC)

    with pytest.raises(PipelineCoherencyDispositionError) as exc_info:
        record_pipeline_coherency_disposition(connection, **kwargs)

    assert exc_info.value.code == "PIPELINE_ORPHAN_EVIDENCE_INVALID"
    assert exc_info.value.details["reason_codes"] == ["ORPHAN_APPLY_APPROVAL_TIME_INVALID"]
    assert (
        connection.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0]
        == 0
    )
    connection.close()


@pytest.mark.parametrize(
    ("state", "candidate_trade_date", "action"),
    (
        ("CLOSED", None, ACTION_DISPOSE_EXPIRED_PLAN_READY),
        ("STALE", "2026-06-26", ACTION_DISPOSE_STALE_OTHER_DATE),
    ),
)
def test_historical_disposition_requires_candidate_active_source_count_zero(
    tmp_path,
    state,
    candidate_trade_date,
    action,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / f"active-source-{state.lower()}.sqlite3",
        state=state,
        candidate_trade_date=candidate_trade_date,
    )
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    connection.commit()

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=action,
    )
    connection.close()

    assert preview["eligible"] is False
    assert "CANDIDATE_ACTIVE_SOURCE_COUNT_PRESENT" in preview["reason_codes"]


def test_source_snapshot_separates_historical_active_event_from_current_projection(
    tmp_path,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "historical-source-event.sqlite3"
    )
    _insert_candidate_source_projection(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
        latest_active=False,
    )

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    source = preview["source"]
    assert source["active_count"] == 1
    assert source["historical_event_active_count"] == 1
    assert source["latest_active_count"] == 0
    assert source["candidate_active_source_count"] == 0
    assert source["source_projection_consistent"] is True
    assert source["source_state_classification"] == "HISTORICAL_EVENT_ACTIVE_ONLY"
    assert preview["eligible"] is False
    assert "HISTORICAL_ACTIVE_SOURCE_PRESENT" in preview["reason_codes"]
    assert "LATEST_ACTIVE_SOURCE_PRESENT" not in preview["reason_codes"]
    assert "SOURCE_PROJECTION_INCONSISTENT" not in preview["reason_codes"]


def test_source_snapshot_reports_latest_active_source_as_current_blocker(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "latest-active-source.sqlite3"
    )
    _insert_candidate_source_projection(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
        latest_active=True,
    )
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    connection.commit()

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    source = preview["source"]
    assert source["historical_event_active_count"] == 1
    assert source["latest_active_count"] == 1
    assert source["candidate_active_source_count"] == 1
    assert source["source_projection_consistent"] is True
    assert source["source_state_classification"] == "LATEST_ACTIVE_SOURCE_PRESENT"
    assert preview["eligible"] is False
    assert "LATEST_ACTIVE_SOURCE_PRESENT" in preview["reason_codes"]
    assert "SOURCE_PROJECTION_INCONSISTENT" not in preview["reason_codes"]


def test_source_snapshot_blocks_candidate_latest_projection_mismatch(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "source-projection-mismatch.sqlite3"
    )
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    _insert_candidate_source_projection(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
        latest_active=False,
    )

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    source = preview["source"]
    assert source["historical_event_active_count"] == 1
    assert source["latest_active_count"] == 0
    assert source["candidate_active_source_count"] == 1
    assert source["source_projection_consistent"] is False
    assert source["source_state_classification"] == "SOURCE_PROJECTION_INCONSISTENT"
    assert preview["eligible"] is False
    assert "SOURCE_PROJECTION_INCONSISTENT" in preview["reason_codes"]
    assert "LATEST_ACTIVE_SOURCE_PRESENT" not in preview["reason_codes"]


def test_effective_stale_disposition_resolves_drift_and_tracks_candidate_date_downstream(
    tmp_path,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "stale-effective.sqlite3",
        state="STALE",
        candidate_trade_date="2026-06-26",
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_STALE_OTHER_DATE,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-stale-effective"),
    )

    effective = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    assert effective["current_source_drift_count"] == 1
    assert effective["current_source_drift_pending_count"] == 0
    assert effective["qualification_status"] == "PASS"

    candidate = connection.execute(
        "SELECT code, name FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO dry_run_positions (
            dry_run_position_id, trade_date, code, name, quantity, avg_price,
            invested_notional, status, opened_at, updated_at, dry_run_only
        ) VALUES (
            'late-stale-position', '2026-06-26', ?, ?, 1, 1,
            1, 'OPEN', '2026-06-26T00:00:00Z', '2026-06-26T00:00:00Z', 1
        )
        """,
        (candidate["code"], candidate["name"]),
    )
    connection.commit()
    invalidated = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    blocked_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_STALE_OTHER_DATE,
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["current_source_drift_pending_count"] == 1
    assert invalidated["qualification_status"] == "BLOCKED"
    assert blocked_preview["downstream"]["active_other_downstream_count"] == 1
    assert "ACTIVE_OTHER_DOWNSTREAM_PRESENT" in blocked_preview["reason_codes"]


def test_candidate_metadata_gateway_commands_invalidate_disposition_and_malformed_blocks(
    tmp_path,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "gateway-command-cas.sqlite3"
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-gateway-command"),
    )
    valid_payload = json.dumps(
        {"metadata": {"candidate_instance_id": candidate_id}},
        separators=(",", ":"),
        sort_keys=True,
    )
    malformed_payload = '{"candidate_instance_id":' + json.dumps(candidate_id) + ',"metadata":'
    unbound_payload = json.dumps(
        {
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_intent_id": "missing-live-sim-intent",
            "metadata": {
                "live_sim_intent_id": "missing-live-sim-intent",
                "live_sim_only": True,
                "live_real_allowed": False,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    connection.executemany(
        """
        INSERT INTO gateway_commands (
            command_id, command_type, source, status,
            payload_json, payload_hash, created_at
        ) VALUES (?, 'send_order', 'live_sim', ?, ?, ?, ?)
        """,
        (
            (
                "late-ghost-command",
                "QUEUED",
                valid_payload,
                "late-ghost-command-hash",
                "2026-07-15T00:00:00Z",
            ),
            (
                "late-malformed-command",
                "ACKED",
                malformed_payload,
                "late-malformed-command-hash",
                "2026-07-15T00:00:01Z",
            ),
            (
                "late-unbound-malformed-command",
                "ACKED",
                "{",
                "late-unbound-malformed-command-hash",
                "2026-07-15T00:00:02Z",
            ),
            (
                "late-valid-unbound-command",
                "QUEUED",
                unbound_payload,
                "late-valid-unbound-command-hash",
                "2026-07-15T00:00:03Z",
            ),
        ),
    )
    connection.commit()

    invalidated = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    blocked_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["qualification_status"] == "BLOCKED"
    assert blocked_preview["downstream"]["gateway_command_count"] == 4
    assert blocked_preview["downstream"]["unsafe_gateway_command_count"] == 2
    assert blocked_preview["downstream"]["malformed_gateway_command_payload_count"] == 2
    assert blocked_preview["downstream"]["malformed_unbound_order_command_count"] == 1
    assert blocked_preview["downstream"]["unbound_order_command_count"] == 2
    assert "UNSAFE_GATEWAY_COMMAND_PRESENT" in blocked_preview["reason_codes"]
    assert "MALFORMED_GATEWAY_COMMAND_PAYLOAD_PRESENT" in blocked_preview["reason_codes"]
    assert "MALFORMED_UNBOUND_ORDER_COMMAND_PRESENT" in blocked_preview["reason_codes"]
    assert "UNBOUND_ORDER_COMMAND_PRESENT" in blocked_preview["reason_codes"]


def test_unbound_active_order_artifacts_and_global_boundary_invalidate_disposition(
    tmp_path,
) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "unbound-active-artifacts.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-unbound-artifacts"),
    )
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, trade_date, account_id,
            code, name, side, order_type, quantity, limit_price, notional,
            status, filled_quantity, remaining_quantity, idempotency_key,
            created_at
        ) VALUES (
            'unbound-live-order', 'missing-live-intent', ?, 'SIM_REDACTED',
            ?, ?, 'BUY', 'LIMIT', 1, 1, 1, 'COMMAND_QUEUED', 0, 1,
            'unbound-live-order-key', '2026-07-15T00:00:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, trade_date, account_id,
            code, name, side, order_type, quantity, limit_price, notional,
            status, filled_quantity, remaining_quantity, idempotency_key,
            created_at
        ) VALUES (
            'unbound-live-order-terminal', 'missing-live-intent-terminal', ?,
            'SIM_REDACTED', ?, ?, 'BUY', 'LIMIT', 1, 1, 1, 'FILLED', 1, 0,
            'unbound-live-order-terminal-key', '2026-07-15T00:00:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id, live_sim_order_id, live_sim_intent_id,
            account_id, code, side, quantity, price, notional, executed_at
        ) VALUES (
            'unbound-live-execution', 'missing-execution-order',
            'missing-execution-intent', 'SIM_REDACTED', ?, 'BUY', 1, 1, 1,
            '2026-07-15T00:00:00Z'
        )
        """,
        (plan["code"],),
    )
    connection.execute(
        """
        INSERT INTO dry_run_orders (
            dry_run_order_id, dry_run_intent_id, trade_date, code, name,
            side, order_type, quantity, requested_price, status, created_at
        ) VALUES (
            'unbound-dry-order', 'missing-dry-intent', ?, ?, ?,
            'BUY', 'LIMIT', 1, 1, 'CREATED', '2026-07-15T00:00:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id, account_id, trade_date, code, name, quantity,
            available_quantity, avg_entry_price, total_entry_notional,
            opened_at, status, created_at, updated_at
        ) VALUES (
            'unbound-live-position', 'SIM_REDACTED', ?, ?, ?, 1,
            1, 1, 1, '2026-07-15T00:00:00Z', 'OPEN',
            '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_cancel_intents (
            cancel_intent_id, live_sim_order_id, code, cancel_quantity,
            reason, status, idempotency_key, created_at
        ) VALUES (
            'unbound-cancel', 'missing-order', ?, 1, 'TEST', 'CREATED',
            'unbound-cancel-key', '2026-07-15T00:00:00Z'
        )
        """,
        (plan["code"],),
    )
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundaries (
            command_id, idempotency_key, command_type, source, state,
            attempts, code, side, unconfirmed_at,
            pre_ack_payload_json, latest_payload_json, created_at, updated_at
        ) VALUES (
            'missing-boundary-command', 'missing-boundary-key', 'send_order',
            'live_sim', 'UNCONFIRMED', 1, ?, 'BUY',
            '2026-07-15T00:00:00Z', '{}', '{}',
            '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z'
        )
        """,
        (plan["code"],),
    )
    connection.commit()

    invalidated = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    blocked_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["qualification_status"] == "BLOCKED"
    assert blocked_preview["downstream"]["live_sim_order_count"] == 2
    assert blocked_preview["downstream"]["active_live_sim_order_count"] == 1
    assert blocked_preview["downstream"]["live_sim_execution_count"] == 1
    assert blocked_preview["downstream"]["dry_run_order_count"] == 1
    assert blocked_preview["downstream"]["active_other_downstream_count"] >= 3
    assert blocked_preview["downstream"]["unbound_active_order_artifact_count"] == 4
    assert blocked_preview["downstream"]["unbound_order_artifact_count"] == 6
    assert blocked_preview["downstream"]["unresolved_boundary_count"] == 1
    assert "UNBOUND_ACTIVE_ORDER_ARTIFACT_PRESENT" in blocked_preview["reason_codes"]
    assert "UNBOUND_ORDER_ARTIFACT_PRESENT" in blocked_preview["reason_codes"]
    assert "EFFECTIVE_BROKER_BOUNDARY_UNCONFIRMED" in blocked_preview["reason_codes"]


def test_unbound_open_positions_are_global_fail_closed_artifacts(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "unbound-open-positions.sqlite3"
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-unbound-positions"),
    )
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id, account_id, trade_date, code, name, quantity,
            available_quantity, avg_entry_price, total_entry_notional,
            opened_at, status, created_at, updated_at
        ) VALUES (
            'unbound-global-live-position', 'SIM_REDACTED', '2026-07-01',
            '000660', 'SYNTHETIC', 1, 1, 1, 1,
            '2026-07-01T00:00:00Z', 'OPEN',
            '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO dry_run_positions (
            dry_run_position_id, trade_date, code, name, quantity, avg_price,
            invested_notional, status, opened_at, updated_at
        ) VALUES (
            'unbound-global-dry-position', '2026-07-02', '035420',
            'SYNTHETIC', 1, 1, 1, 'OPEN',
            '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z'
        )
        """
    )
    connection.commit()

    invalidated = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    blocked_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["qualification_status"] == "BLOCKED"
    assert blocked_preview["eligible"] is False
    assert blocked_preview["downstream"]["unbound_active_order_artifact_count"] == 2
    assert blocked_preview["downstream"]["unbound_order_artifact_count"] == 2
    assert blocked_preview["downstream"]["active_other_downstream_count"] == 2
    assert "UNBOUND_ACTIVE_ORDER_ARTIFACT_PRESENT" in blocked_preview["reason_codes"]
    assert "UNBOUND_ORDER_ARTIFACT_PRESENT" in blocked_preview["reason_codes"]


def test_unknown_global_boundary_state_fails_closed(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "unknown-global-boundary.sqlite3"
    )
    connection.execute(
        """
        INSERT INTO gateway_order_broker_boundaries (
            command_id, idempotency_key, command_type, source, state,
            attempts, code, side, created_at, updated_at
        ) VALUES (
            'corrupt-boundary-command', 'corrupt-boundary-key', 'send_order',
            'live_sim', 'CORRUPT', 0, '005930', 'BUY',
            '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z'
        )
        """
    )
    connection.commit()

    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    assert preview["eligible"] is False
    assert preview["downstream"]["unknown_boundary_count"] == 1
    assert "BROKER_BOUNDARY_STATUS_UNKNOWN" in preview["reason_codes"]


def test_late_live_exit_order_and_execution_invalidate_disposition(tmp_path) -> None:
    connection, candidate_id, trade_date, order_plan_id = _expired_closed_pipeline(
        tmp_path / "late-exit-order.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id, candidate_instance_id,
            strategy_observation_id, risk_observation_id, order_plan_id,
            trade_date, account_id, code, name, side, order_type, quantity,
            limit_price, notional, status, idempotency_key, created_at
        ) VALUES (
            'entry-intent', ?, ?, ?, ?, ?, 'SIM_REDACTED', ?, ?, 'BUY',
            'LIMIT', 1, 1, 1, 'FILLED', 'entry-intent-key',
            '2026-07-15T00:00:00Z'
        )
        """,
        (
            candidate_id,
            plan["strategy_observation_id"],
            plan["risk_observation_id"],
            order_plan_id,
            trade_date,
            plan["code"],
            plan["name"],
        ),
    )
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, trade_date, account_id,
            code, name, side, order_type, quantity, limit_price, notional,
            status, filled_quantity, remaining_quantity, idempotency_key,
            created_at
        ) VALUES (
            'entry-order', 'entry-intent', ?, 'SIM_REDACTED', ?, ?, 'BUY',
            'LIMIT', 1, 1, 1, 'FILLED', 1, 0, 'entry-order-key',
            '2026-07-15T00:00:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id, account_id, trade_date, code, name, quantity,
            available_quantity, avg_entry_price, total_entry_notional,
            opened_at, closed_at, status, source_live_sim_order_id,
            source_live_sim_intent_id, created_at, updated_at
        ) VALUES (
            'closed-position', 'SIM_REDACTED', ?, ?, ?, 0, 0, 1, 1,
            '2026-07-15T00:00:00Z', '2026-07-15T00:01:00Z', 'CLOSED',
            'entry-order', 'entry-intent',
            '2026-07-15T00:00:00Z', '2026-07-15T00:01:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_exit_intents (
            exit_intent_id, position_id, live_sim_order_id, code, quantity,
            reason, status, idempotency_key, created_at
        ) VALUES (
            'exit-intent', 'closed-position', 'late-exit-order', ?, 1,
            'TEST', 'CLOSED', 'exit-intent-key', '2026-07-15T00:01:00Z'
        )
        """,
        (plan["code"],),
    )
    connection.commit()
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    assert preview["eligible"] is True
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-late-exit"),
    )

    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, trade_date, account_id,
            code, name, side, order_type, quantity, limit_price, notional,
            status, filled_quantity, remaining_quantity, idempotency_key,
            created_at
        ) VALUES (
            'late-exit-order', 'exit-intent', ?, 'SIM_REDACTED', ?, ?, 'SELL',
            'LIMIT', 1, 1, 1, 'FILLED', 1, 0, 'late-exit-order-key',
            '2026-07-15T00:02:00Z'
        )
        """,
        (trade_date, plan["code"], plan["name"]),
    )
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id, live_sim_order_id, live_sim_intent_id,
            account_id, code, side, quantity, price, notional, executed_at
        ) VALUES (
            'late-exit-execution', 'late-exit-order', 'exit-intent',
            'SIM_REDACTED', ?, 'SELL', 1, 1, 1, '2026-07-15T00:02:00Z'
        )
        """,
        (plan["code"],),
    )
    connection.commit()
    invalidated = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    blocked_preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["qualification_status"] == "BLOCKED"
    assert blocked_preview["downstream"]["live_sim_order_count"] == 2
    assert blocked_preview["downstream"]["live_sim_execution_count"] == 1


def test_effective_orphan_disposition_resolves_manual_blocker_and_late_position_invalidates(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "orphan-effective.sqlite3")
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (
            'orphan-effective', 'orphan-effective-risk', 'missing-strategy',
            '2026-07-15', '005930', 'fixture', '2026-07-15T00:00:00Z',
            'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0, '[]', 'test', 1
        )
        """
    )
    connection.commit()
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date="2026-07-15",
        candidate_instance_id="orphan-effective",
        action=ACTION_DISPOSE_ORPHAN,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_orphan_record_kwargs(preview, request_id="pipeline-request-orphan-effective"),
    )

    effective = _resolved_status(
        connection,
        candidate_id="orphan-effective",
        trade_date="2026-07-15",
    )
    assert effective["items"][0]["effective_disposition"]["effective"] is True
    assert effective["manual_review_count"] == 1
    assert effective["manual_review_pending_count"] == 0
    assert effective["current_source_drift_unknown_count"] == 1
    assert effective["current_source_drift_unknown_pending_count"] == 0
    assert effective["qualification_status"] == "PASS"

    connection.execute(
        """
        INSERT INTO dry_run_positions (
            dry_run_position_id, trade_date, code, name, quantity, avg_price,
            invested_notional, status, opened_at, updated_at, dry_run_only
        ) VALUES (
            'late-orphan-position', '2026-07-15', '005930', 'fixture', 1, 1,
            1, 'OPEN', '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z', 1
        )
        """
    )
    connection.commit()
    invalidated = _resolved_status(
        connection,
        candidate_id="orphan-effective",
        trade_date="2026-07-15",
    )
    connection.close()

    assert invalidated["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert invalidated["disposition_pending_count"] == 1
    assert invalidated["qualification_status"] == "BLOCKED"


@pytest.mark.parametrize(
    ("column", "duplicate_prefix"),
    (
        ("evidence_json", '"contract":"duplicate",'),
        ("safety_snapshot_json", '"trading_profile":"OBSERVE",'),
    ),
)
def test_stored_orphan_disposition_rejects_duplicate_json_keys(
    tmp_path,
    column: str,
    duplicate_prefix: str,
) -> None:
    connection = initialize_database(tmp_path / f"orphan-duplicate-{column}.sqlite3")
    preview = _insert_orphan_subject(connection, candidate_id=f"orphan-duplicate-{column}")
    record_pipeline_coherency_disposition(
        connection,
        **_orphan_record_kwargs(
            preview,
            request_id=f"pipeline-request-orphan-duplicate-{column}",
        ),
    )
    raw_json = str(
        connection.execute(f"SELECT {column} FROM pipeline_coherency_dispositions").fetchone()[
            column
        ]
    )
    _replace_disposition_column(
        connection,
        column=column,
        value="{" + duplicate_prefix + raw_json[1:],
    )

    status = _resolved_status(
        connection,
        candidate_id=preview["candidate_instance_id"],
        trade_date=preview["trade_date"],
    )
    connection.close()

    assert status["items"][0]["effective_disposition"]["status"] == "INVALID_CHAIN"
    assert status["disposition_pending_count"] == 1
    assert status["qualification_status"] == "BLOCKED"


def test_stored_legacy_generic_orphan_evidence_is_not_effective(tmp_path) -> None:
    connection = initialize_database(tmp_path / "orphan-legacy-generic.sqlite3")
    preview = _insert_orphan_subject(connection, candidate_id="orphan-legacy-generic")
    record_pipeline_coherency_disposition(
        connection,
        **_orphan_record_kwargs(
            preview,
            request_id="pipeline-request-orphan-legacy-generic",
        ),
    )
    _replace_disposition_column(
        connection,
        column="evidence_json",
        value=json.dumps({"redacted": True}, separators=(",", ":"), sort_keys=True),
    )

    status = _resolved_status(
        connection,
        candidate_id=preview["candidate_instance_id"],
        trade_date=preview["trade_date"],
    )
    connection.close()

    assert status["items"][0]["effective_disposition"]["status"] == "INVALID_CHAIN"
    assert status["disposition_pending_count"] == 1
    assert status["qualification_status"] == "BLOCKED"


def test_tampered_request_hash_or_safety_snapshot_invalidates_chain(tmp_path) -> None:
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(
        tmp_path / "tampered-ledger.sqlite3"
    )
    preview = preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    record_pipeline_coherency_disposition(
        connection,
        **_record_kwargs(preview, request_id="pipeline-request-tampered"),
    )
    trigger_sql = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'trigger'
          AND name = 'trg_pipeline_coherency_dispositions_no_update'
        """
    ).fetchone()["sql"]
    connection.execute("DROP TRIGGER trg_pipeline_coherency_dispositions_no_update")
    connection.execute(
        """
        UPDATE pipeline_coherency_dispositions
        SET request_hash = ?, safety_snapshot_json = ?
        WHERE request_id = 'pipeline-request-tampered'
        """,
        (
            "0" * 64,
            json.dumps(
                {
                    "trading_profile": "OBSERVE",
                    "trading_mode": "OBSERVE",
                    "live_sim_allowed": False,
                    "live_real_allowed": True,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )
    connection.execute(trigger_sql)
    connection.commit()

    status = _resolved_status(
        connection,
        candidate_id=candidate_id,
        trade_date=trade_date,
    )
    connection.close()

    assert status["schema_ready"] is True
    assert status["items"][0]["effective_disposition"]["status"] == "INVALID_CHAIN"
    assert status["disposition_pending_count"] == 1
    assert status["qualification_status"] == "BLOCKED"


def _expired_closed_pipeline(
    path,
    *,
    state: str = "CLOSED",
    candidate_trade_date: str | None = None,
):
    connection, order_plan_id = _prepared_order_plan_connection(path)
    plan = connection.execute(
        """
        SELECT candidate_instance_id, trade_date
        FROM order_plan_drafts
        WHERE order_plan_id = ?
        """,
        (order_plan_id,),
    ).fetchone()
    candidate_id = str(plan["candidate_instance_id"])
    trade_date = str(plan["trade_date"])
    connection.execute(
        """
        UPDATE candidates
        SET state = ?, trade_date = COALESCE(?, trade_date),
            active_source_count = 0,
            closed_at = CASE WHEN ? = 'CLOSED'
                THEN '2026-07-15T00:00:00Z' ELSE NULL END
        WHERE candidate_instance_id = ?
        """,
        (state, candidate_trade_date, state, candidate_id),
    )
    connection.execute(
        "UPDATE candidate_source_events SET active = 0 WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    for table in ("strategy_observations_latest", "risk_observations_latest"):
        connection.execute(
            f"""
            UPDATE {table}
            SET source_run_id = NULL, source_watermark = NULL,
                source_watermark_hash = NULL, source_event_id = NULL,
                source_observed_at = NULL, generated_by = NULL
            WHERE candidate_instance_id = ?
            """,
            (candidate_id,),
        )
    connection.execute(
        """
        UPDATE entry_timing_evaluations
        SET source_run_id = NULL, source_watermark = NULL,
            source_watermark_hash = NULL, source_event_id = NULL,
            source_observed_at = NULL, generated_by = NULL
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    for table in ("order_plan_drafts", "order_plan_drafts_latest"):
        connection.execute(
            f"""
            UPDATE {table}
            SET expires_at = '2020-01-01T00:00:00Z',
                source_run_id = NULL, source_watermark = NULL,
                source_watermark_hash = NULL, source_event_id = NULL,
                source_observed_at = NULL, generated_by = NULL
            WHERE candidate_instance_id = ?
            """,
            (candidate_id,),
        )
    connection.commit()
    return connection, candidate_id, trade_date, order_plan_id


def _insert_candidate_source_projection(
    connection,
    *,
    candidate_id: str,
    trade_date: str,
    latest_active: bool,
) -> None:
    candidate = connection.execute(
        "SELECT code, name FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    source_events = [
        (
            "source-enter",
            "ENTER",
            "2026-06-27T00:00:00Z",
            1,
        )
    ]
    if not latest_active:
        source_events.append(
            (
                "source-exit",
                "EXIT",
                "2026-06-27T00:01:00Z",
                0,
            )
        )
    connection.executemany(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active,
            reason_codes_json, payload_json
        ) VALUES (
            ?, ?, ?, ?, ?, 'TEST_SOURCE', 'source-fixture', ?, ?, ?, ?,
            '[]', '{}'
        )
        """,
        [
            (
                source_event_id,
                candidate_id,
                trade_date,
                candidate["code"],
                candidate["name"],
                action,
                event_ts,
                event_ts,
                active,
            )
            for source_event_id, action, event_ts, active in source_events
        ],
    )
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date, code, source_type, source_id, candidate_instance_id,
            name, active, first_seen_at, last_seen_at, last_event_id,
            payload_json
        ) VALUES (
            ?, ?, 'TEST_SOURCE', 'source-fixture', ?, ?, ?,
            '2026-06-27T00:00:00Z', ?, ?, '{}'
        )
        """,
        (
            trade_date,
            candidate["code"],
            candidate_id,
            candidate["name"],
            1 if latest_active else 0,
            source_events[-1][2],
            source_events[-1][0],
        ),
    )
    connection.commit()


def _resolved_status(connection, *, candidate_id: str, trade_date: str):
    return build_pipeline_coherency_rca_status(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        disposition_resolver=lambda resolved_trade_date, subjects: (
            resolve_pipeline_coherency_dispositions(
                connection,
                resolved_trade_date,
                subjects,
            )
        ),
    )


def _record_kwargs(preview, *, request_id: str):
    return {
        "trade_date": preview["trade_date"],
        "candidate_instance_id": preview["candidate_instance_id"],
        "action": preview["action"],
        "request_id": request_id,
        "expected_pipeline_fingerprint": preview["pipeline_fingerprint"],
        "expected_subject_version": preview["subject_version"],
        "expected_source_fingerprint": preview["source_fingerprint"],
        "expected_candidate_fingerprint": preview["candidate_fingerprint"],
        "expected_downstream_fingerprint": preview["downstream_fingerprint"],
        "expected_boundary_fingerprint": preview["boundary_fingerprint"],
        "reason_code": "EVIDENCED_HISTORICAL_PIPELINE",
        "operator_id": "test.operator",
        "evidence_type": "TEST_ARTIFACT",
        "evidence_ref": "reports/redacted/pipeline-evidence.json",
        "evidence_sha256": "a" * 64,
        "evidence_json": {"redacted": True},
        "safety_snapshot": {
            "trading_profile": "OBSERVE",
            "trading_mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
            "kill_switch_active": True,
            "incremental_worker_enabled": False,
            "enabled_command_producers": [],
            "explicit_env_file": True,
            "theme_refresh_queue_market_scan_commands": False,
            "not_order_intent": True,
            "order_commands_allowed": False,
        },
    }


def _orphan_record_kwargs(preview, *, request_id: str):
    target = {
        "trade_date": preview["trade_date"],
        "candidate_instance_id": preview["candidate_instance_id"],
        "classification": preview["classification"],
        "action": preview["action"],
        "pipeline_fingerprint": preview["pipeline_fingerprint"],
        "subject_version": preview["subject_version"],
        "source_fingerprint": preview["source_fingerprint"],
        "candidate_fingerprint": preview["candidate_fingerprint"],
        "downstream_fingerprint": preview["downstream_fingerprint"],
        "boundary_fingerprint": preview["boundary_fingerprint"],
    }
    payload = {
        "contract": ORPHAN_EVIDENCE_CONTRACT,
        "target_set_sha256": "f" * 64,
        "alias": "M001",
        "target": target,
        "determination": {
            "status": "AUTHORITATIVE",
            "terminal_orphan_confirmed": True,
            "candidate_present": False,
            "current_source_present": False,
            "order_or_broker_activity_present": False,
        },
        "provenance": {
            "source_type": "BROKER_ORDER_HISTORY_EXPORT",
            "source_ref": "broker/order-history/test-artifact",
            "artifact_sha256": "1" * 64,
            "artifact_size": 128,
            "coverage_trade_date": target["trade_date"],
            "coverage_status": "FINAL_COMPLETE",
            "account_scope_sha256": "2" * 64,
            "observed_at": "2026-07-15T15:00:00Z",
            "reviewed_at": "2026-07-15T15:01:00Z",
            "reviewer_id": "reviewer.safe",
        },
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    document = StableJsonDocument(
        payload=payload,
        sha256=hashlib.sha256(raw).hexdigest(),
        size=len(raw),
        device=0,
        inode=0,
        mtime_ns=0,
    )
    binding = validate_orphan_manual_evidence_document(
        document,
        expected_target_set_sha256="f" * 64,
        expected_alias="M001",
        expected_target=target,
        expected_artifact_sha256="1" * 64,
        expected_artifact_size=128,
        expected_account_scope_sha256="2" * 64,
        not_after=datetime(2026, 7, 16, 2, 0, tzinfo=UTC),
    )
    apply_binding = build_orphan_apply_evidence_binding(
        binding,
        approved_preflight_report_sha256="3" * 64,
        approved_private_manifest_sha256="4" * 64,
        source_plan_report_sha256="5" * 64,
        source_reconciliation_report_sha256="6" * 64,
        approved_database_main_sha256="7" * 64,
        approved_database_main_size=1,
        broker_scope_sha256="2" * 64,
        preflight_generated_at="2026-07-16T02:00:00Z",
    )
    result = _record_kwargs(preview, request_id=request_id)
    result["evidence_sha256"] = document.sha256
    result["evidence_json"] = apply_binding
    result["orphan_approval_context"] = _issue_orphan_apply_approval_context(
        apply_binding,
        verified_preflight_report_sha256="3" * 64,
        verified_database_main_sha256="7" * 64,
    )
    result["as_of"] = datetime(2026, 7, 16, 3, 0, tzinfo=UTC)
    return result


def _insert_orphan_subject(connection, *, candidate_id: str):
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (
            ?, ?, 'missing-strategy', '2026-07-15', '005930', 'fixture',
            '2026-07-15T00:00:00Z', 'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0,
            '[]', 'test', 1
        )
        """,
        (candidate_id, f"{candidate_id}-risk"),
    )
    connection.commit()
    return preview_pipeline_coherency_disposition(
        connection,
        trade_date="2026-07-15",
        candidate_instance_id=candidate_id,
        action=ACTION_DISPOSE_ORPHAN,
    )


def _replace_disposition_column(connection, *, column: str, value: str) -> None:
    assert column in {"evidence_json", "safety_snapshot_json"}
    trigger_sql = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'trigger'
          AND name = 'trg_pipeline_coherency_dispositions_no_update'
        """
    ).fetchone()["sql"]
    connection.execute("DROP TRIGGER trg_pipeline_coherency_dispositions_no_update")
    connection.execute(
        f"UPDATE pipeline_coherency_dispositions SET {column} = ?",
        (value,),
    )
    connection.execute(trigger_sql)
    connection.commit()
