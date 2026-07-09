from __future__ import annotations

from datetime import UTC, datetime

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime.market_data_append_only_controller import (
    build_market_data_append_only_controller_status,
    record_market_data_append_only_auto_rollback_event,
)
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database


def test_controller_mode_off_blocks_all_event_types(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-off.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)

    status = build_market_data_append_only_controller_status(
        connection,
        settings=Settings(),
    )

    connection.close()
    assert status.operating_mode == "OFF"
    assert status.effective_cutover_enabled is False
    for gate in (
        status.price_tick_gate,
        status.tr_response_gate,
        status.condition_event_gate,
    ):
        assert gate.effective_skip_allowed is False
        assert "MARKET_DATA_APPEND_ONLY_MODE_OFF" in gate.blocked_reason_codes


def test_controller_global_kill_switch_blocks_all_even_when_flags_pass(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-kill.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(
            gateway_market_data_append_only_global_kill_switch=True
        ),
    )

    connection.close()
    assert status.global_kill_switch is True
    assert status.price_tick_gate.effective_skip_allowed is False
    assert "MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH" in (
        status.price_tick_gate.blocked_reason_codes
    )


def test_controller_single_event_modes_allow_only_matching_gate(tmp_path) -> None:
    for mode, allowed in (
        ("PRICE_TICK_ONLY", "price_tick"),
        ("TR_RESPONSE_ONLY", "tr_response"),
        ("CONDITION_EVENT_ONLY", "condition_event"),
    ):
        connection = initialize_database(tmp_path / f"{mode}.sqlite3")
        _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
        status = build_market_data_append_only_controller_status(
            connection,
            settings=_healthy_settings(
                gateway_market_data_append_only_operating_mode=mode,
            ),
        )
        gates = {
            "price_tick": status.price_tick_gate,
            "tr_response": status.tr_response_gate,
            "condition_event": status.condition_event_gate,
        }
        connection.close()
        assert gates[allowed].effective_skip_allowed is True
        for event_type, gate in gates.items():
            if event_type != allowed:
                assert gate.effective_skip_allowed is False
                assert "MARKET_DATA_APPEND_ONLY_MODE_DOES_NOT_ALLOW_EVENT_TYPE" in (
                    gate.blocked_reason_codes
                )


def test_controller_market_data_limited_allows_all_three_when_healthy(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-limited.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(),
    )

    connection.close()
    assert status.status == "PASS"
    assert status.effective_cutover_enabled is True
    assert status.allowed_event_types == (
        "price_tick",
        "tr_response",
        "condition_event",
    )
    assert status.price_tick_gate.effective_skip_allowed is True
    assert status.tr_response_gate.effective_skip_allowed is True
    assert status.condition_event_gate.effective_skip_allowed is True


def test_controller_global_budget_exhaustion_blocks_all(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-budget.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    _insert_effective_skip_decision(connection, event_type="price_tick")

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(
            gateway_market_data_append_only_global_max_skip_per_minute=1
        ),
    )

    connection.close()
    assert status.global_skip_budget_remaining == 0
    assert "MARKET_DATA_APPEND_ONLY_GLOBAL_BUDGET_EXHAUSTED" in (
        status.tr_response_gate.blocked_reason_codes
    )


def test_controller_auto_rollback_records_event_for_outbox_error(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-rollback.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id, projection_name, event_id, event_type, source, status,
            priority, attempts, available_at, created_at, updated_at, metadata_json
        )
        VALUES (
            'market_data:evt_error', 'market_data', 'evt_error', 'price_tick',
            'test', 'ERROR', 0, 0, ?, ?, ?, '{}'
        )
        """,
        (datetime_to_wire(utc_now()),) * 3,
    )
    connection.commit()

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(),
    )
    event_id = record_market_data_append_only_auto_rollback_event(
        connection,
        status,
        settings=_healthy_settings(),
    )
    count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_data_append_only_auto_rollback_events"
    ).fetchone()["count"]

    connection.close()
    assert status.auto_rollback_required is True
    assert "PROJECTION_OUTBOX_ERROR_COUNT_EXCEEDED" in (
        status.auto_rollback_reason_codes
    )
    assert event_id is not None
    assert count == 1


def test_controller_reconcile_fail_and_side_effect_error_require_rollback(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "controller-side-effect.sqlite3")
    _insert_reconcile_run(connection, status="FAIL", append_only_ready=False)
    _insert_routing_decision_with_side_effect_error(connection)

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(),
    )

    connection.close()
    assert status.auto_rollback_required is True
    assert "MARKET_DATA_RECONCILE_FAIL" in status.auto_rollback_reason_codes
    assert "MARKET_DATA_APPEND_ONLY_SIDE_EFFECT_ERROR" in (
        status.auto_rollback_reason_codes
    )


def test_controller_dashboard_fast_fail_requires_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "controller-dashboard-fail.sqlite3")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)

    status = build_market_data_append_only_controller_status(
        connection,
        settings=_healthy_settings(),
        dashboard_fast_status="FAIL",
    )

    connection.close()
    assert status.dashboard_fast_status == "FAIL"
    assert status.auto_rollback_required is True
    assert "DASHBOARD_FAST_STATUS_FAIL" in status.auto_rollback_reason_codes


def _healthy_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": True,
        "gateway_market_data_append_only_operating_mode": "MARKET_DATA_LIMITED",
        "gateway_market_data_append_only_global_kill_switch": False,
        "gateway_market_data_append_only_global_max_skip_per_minute": 10,
        "gateway_market_data_append_only_price_tick_cutover_enabled": True,
        "gateway_market_data_append_only_price_tick_max_skip_per_minute": 10,
        "gateway_market_data_append_only_tr_response_cutover_enabled": True,
        "gateway_market_data_append_only_tr_response_max_skip_per_minute": 10,
        "gateway_market_data_append_only_condition_event_cutover_enabled": True,
        "gateway_market_data_append_only_condition_event_max_skip_per_minute": 10,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _insert_reconcile_run(connection, *, status: str, append_only_ready: bool) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_reconcile_runs (
            run_id, status, checked_event_count, checked_price_tick_count,
            checked_condition_event_count, checked_tr_response_count,
            outbox_job_count, outbox_pending_count, outbox_processing_count,
            outbox_applied_count, outbox_skipped_count, outbox_error_count,
            outbox_dead_letter_count, missing_projection_count,
            inline_projection_error_count, outbox_error_issue_count,
            duplicate_or_conflict_count, synthetic_child_event_issue_count,
            watermark_risk_count, append_only_ready, reason_codes_json,
            summary_json, created_at
        )
        VALUES (?, ?, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, '[]', ?, ?)
        """,
        (
            f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
            status,
            int(append_only_ready),
            canonical_json({"status": status, "append_only_ready": append_only_ready}),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()


def _insert_effective_skip_decision(connection, *, event_type: str) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            cutover_enabled, reconcile_required, append_only_ready,
            outbox_status, outbox_job_present, would_skip_inline,
            effective_skip_inline, worker_apply_enabled,
            fallback_inline_projection_expected, blocked_reason_codes_json,
            evidence_json, decided_at
        )
        VALUES (?, ?, 'market_data', 1, 1, 1, 1, 'ENQUEUED', 1, 1, 1, 1, 0, '[]', '{}', ?)
        """,
        (f"evt_{event_type}_skip", event_type, datetime_to_wire(utc_now())),
    )
    connection.commit()


def _insert_routing_decision_with_side_effect_error(connection) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            cutover_enabled, reconcile_required, append_only_ready,
            outbox_status, outbox_job_present, would_skip_inline,
            effective_skip_inline, worker_apply_enabled,
            fallback_inline_projection_expected, blocked_reason_codes_json,
            post_apply_deferred_side_effects_json, evidence_json, decided_at
        )
        VALUES (
            'evt_condition_error', 'condition_event', 'market_data',
            1, 1, 1, 1, 'ENQUEUED', 1, 1, 1, 1, 0, '[]',
            ?, '{}', ?
        )
        """,
        (
            canonical_json({"condition_fusion_error_count": 1}),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()
