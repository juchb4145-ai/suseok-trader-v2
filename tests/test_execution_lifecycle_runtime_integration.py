from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable

import pytest
import services.live_sim.live_sim_service as live_sim_service
import services.runtime.preflight as preflight_service
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus
from services.live_sim.live_sim_service import (
    create_live_sim_intent,
    evaluate_live_sim_eligibility,
    queue_live_sim_order_command,
)
from services.live_sim.order_plan_eligibility import (
    evaluate_live_sim_order_plan_eligibility,
)
from services.live_sim.order_plan_intent import (
    create_live_sim_intent_from_order_plan,
    queue_live_sim_order_command_from_order_plan,
)
from services.oms.dry_run_service import create_dry_run_intent
from services.runtime.preflight import OperatingMode, PreflightStatus, run_live_sim_preflight
from tests.test_live_sim import _live_sim_settings, _mark_gateway_ready
from tests.test_live_sim_operating_orchestrator import _operating_settings
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
)
from tests.test_oms_dry_run import _prepared_connection
from tests.test_oms_dry_run import _settings as _dry_run_settings


def test_new_buy_eligibility_uses_global_effective_lifecycle_blockers(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    def blocked_status(_connection: sqlite3.Connection, **kwargs: object) -> dict[str, object]:
        calls.append(dict(kwargs))
        return _classifier_status(effective_blocker_count=1)

    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        blocked_status,
    )

    candidate_connection, candidate_id = _prepared_connection(
        tmp_path / "candidate-global-lifecycle-block.sqlite3"
    )
    create_dry_run_intent(
        candidate_connection,
        candidate_id,
        settings=_dry_run_settings(),
    )
    _mark_gateway_ready(candidate_connection)
    candidate_eligibility = evaluate_live_sim_eligibility(
        candidate_connection,
        candidate_id,
        settings=_live_sim_settings(),
    )

    plan_connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "order-plan-global-lifecycle-block.sqlite3"
    )
    plan_eligibility = evaluate_live_sim_order_plan_eligibility(
        plan_connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    candidate_connection.close()
    plan_connection.close()

    assert LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value in (
        candidate_eligibility.reason_codes
    )
    assert LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value in (
        plan_eligibility.reason_codes
    )
    assert calls == [{}, {}]


def test_eligibility_fail_closes_on_missing_or_false_classifier_contract_flags(
    tmp_path,
    monkeypatch,
) -> None:
    candidate_connection, candidate_id = _prepared_connection(
        tmp_path / "candidate-invalid-lifecycle-contract.sqlite3"
    )
    create_dry_run_intent(
        candidate_connection,
        candidate_id,
        settings=_dry_run_settings(),
    )
    _mark_gateway_ready(candidate_connection)
    plan_connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "plan-invalid-lifecycle-contract.sqlite3"
    )

    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status_without(
            "read_only",
            sensitive_detail="not-persisted",
        ),
    )
    with pytest.raises(ValueError, match="read_only contract is invalid"):
        evaluate_live_sim_eligibility(
            candidate_connection,
            candidate_id,
            settings=_live_sim_settings(),
        )

    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(
            effective_blocker_count=0
        )
        | {"observe_only": False},
    )
    with pytest.raises(ValueError, match="observe_only contract is invalid"):
        evaluate_live_sim_order_plan_eligibility(
            plan_connection,
            order_plan_id,
            settings=_pilot_settings(),
        )
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(
            effective_blocker_count=0
        )
        | {"status": "pass", "qualification_status": "pass"},
    )
    with pytest.raises(ValueError, match="qualification status is invalid"):
        evaluate_live_sim_eligibility(
            candidate_connection,
            candidate_id,
            settings=_live_sim_settings(),
        )
    assert candidate_connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents"
    ).fetchone()["count"] == 0
    candidate_connection.close()
    plan_connection.close()


def test_existing_created_buy_intent_is_blocked_at_command_enqueue_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    connection, candidate_id = _prepared_connection(
        tmp_path / "created-intent-lifecycle-boundary.sqlite3"
    )
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(effective_blocker_count=1),
    )

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value,
    ):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    snapshot = _queue_boundary_snapshot(connection, intent.live_sim_intent_id)
    connection.close()

    assert snapshot["gateway_command_count"] == 0
    assert snapshot["live_sim_order_count"] == 0
    assert snapshot["intent_status"] == LiveSimIntentStatus.CREATED.value
    assert snapshot["connection_in_transaction"] is False
    assert snapshot["reason_codes"] == [
        LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value
    ]
    assert snapshot["gate"]["status"] == "BLOCKED"
    assert snapshot["gate"]["effective_blocker_count"] == 1
    assert snapshot["gate"]["classifier_fail_closed"] is False
    assert snapshot["gate"]["classifier_error_type"] is None


@pytest.mark.parametrize(
    ("classifier", "expected_error_type", "sensitive_detail"),
    [
        (
            lambda _connection, **_kwargs: (_ for _ in ()).throw(
                sqlite3.OperationalError("sensitive-sqlite-classifier-detail")
            ),
            "OperationalError",
            "sensitive-sqlite-classifier-detail",
        ),
        (
            lambda _connection, **_kwargs: {
                "status": "PASS",
                "qualification_status": "PASS",
                "effective_blocker_count": None,
                "sensitive": "sensitive-invalid-classifier-detail",
            },
            "ValueError",
            "sensitive-invalid-classifier-detail",
        ),
        (
            lambda _connection, **_kwargs: _classifier_status(
                effective_blocker_count=0
            )
            | {
                "read_only": False,
                "sensitive": "sensitive-false-contract-detail",
            },
            "ValueError",
            "sensitive-false-contract-detail",
        ),
        (
            lambda _connection, **_kwargs: _classifier_status_without(
                "read_only",
                sensitive_detail="sensitive-missing-contract-detail",
            ),
            "ValueError",
            "sensitive-missing-contract-detail",
        ),
        (
            lambda _connection, **_kwargs: _classifier_status(
                effective_blocker_count=0
            )
            | {
                "status": "pass",
                "qualification_status": "pass",
                "sensitive": "sensitive-lowercase-status-detail",
            },
            "ValueError",
            "sensitive-lowercase-status-detail",
        ),
    ],
)
def test_buy_enqueue_classifier_failures_reject_without_raw_detail_or_rows(
    tmp_path,
    monkeypatch,
    classifier,
    expected_error_type: str,
    sensitive_detail: str,
) -> None:
    connection, candidate_id = _prepared_connection(
        tmp_path / f"queue-classifier-{expected_error_type}.sqlite3"
    )
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        classifier,
    )

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value,
    ) as exc_info:
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    snapshot = _queue_boundary_snapshot(connection, intent.live_sim_intent_id)
    serialized = json.dumps(snapshot, sort_keys=True)
    connection.close()

    assert snapshot["gateway_command_count"] == 0
    assert snapshot["live_sim_order_count"] == 0
    assert snapshot["intent_status"] == LiveSimIntentStatus.CREATED.value
    assert snapshot["connection_in_transaction"] is False
    assert snapshot["gate"]["effective_blocker_count"] == 1
    assert snapshot["gate"]["classifier_fail_closed"] is True
    assert snapshot["gate"]["classifier_error_type"] == expected_error_type
    assert sensitive_detail not in serialized
    assert sensitive_detail not in str(exc_info.value)
    assert exc_info.value.__suppress_context__ is True
    assert exc_info.value.__context__ is None


def test_existing_order_plan_intent_is_rechecked_at_command_enqueue_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "order-plan-created-intent-boundary.sqlite3"
    )
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(effective_blocker_count=1),
    )

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value,
    ):
        queue_live_sim_order_command_from_order_plan(
            connection,
            order_plan_id,
            settings=settings,
        )

    snapshot = _queue_boundary_snapshot(connection, intent.live_sim_intent_id)
    connection.close()

    assert snapshot["gateway_command_count"] == 0
    assert snapshot["live_sim_order_count"] == 0
    assert snapshot["intent_status"] == LiveSimIntentStatus.CREATED.value
    assert snapshot["connection_in_transaction"] is False
    assert snapshot["gate"]["stage"] == "gateway_send_order_enqueue_boundary"
    assert snapshot["gate"]["scope"] == "GLOBAL"


@pytest.mark.parametrize("caller_transaction", [False, True])
def test_buy_lifecycle_check_holds_write_lock_until_command_enqueue(
    tmp_path,
    monkeypatch,
    caller_transaction: bool,
) -> None:
    path = tmp_path / "queue-lifecycle-write-lock.sqlite3"
    connection, candidate_id = _prepared_connection(path)
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    competing_writer_blocked = False

    def pass_while_probing_lock(
        checked_connection: sqlite3.Connection,
        **_kwargs,
    ) -> dict[str, object]:
        nonlocal competing_writer_blocked
        assert checked_connection.in_transaction is True
        competing = sqlite3.connect(path, timeout=0)
        try:
            competing.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            competing_writer_blocked = True
        else:
            competing.rollback()
        finally:
            competing.close()
        return _classifier_status(effective_blocker_count=0)

    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        pass_while_probing_lock,
    )
    if caller_transaction:
        connection.execute("BEGIN DEFERRED")

    order = queue_live_sim_order_command(
        connection,
        intent.live_sim_intent_id,
        settings=settings,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    assert connection.in_transaction is caller_transaction
    if caller_transaction:
        connection.commit()
    connection.close()

    assert competing_writer_blocked is True
    assert order.live_sim_intent_id == intent.live_sim_intent_id
    assert command_count == 1


def test_owned_buy_boundary_lock_rolls_back_when_enqueue_raises(
    tmp_path,
    monkeypatch,
) -> None:
    connection, candidate_id = _prepared_connection(
        tmp_path / "queue-boundary-enqueue-error.sqlite3"
    )
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(effective_blocker_count=0),
    )

    def raise_enqueue_error(*_args, **_kwargs):
        raise RuntimeError("synthetic enqueue failure")

    monkeypatch.setattr(live_sim_service, "enqueue_command", raise_enqueue_error)

    with pytest.raises(RuntimeError, match="synthetic enqueue failure"):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    assert connection.in_transaction is False
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"] == 0
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_orders"
    ).fetchone()["count"] == 0
    assert connection.execute(
        """
        SELECT status
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()["status"] == LiveSimIntentStatus.CREATED.value
    connection.close()


def test_caller_transaction_partial_enqueue_error_rolls_back_only_queue_writes(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "caller-transaction-partial-enqueue.sqlite3"
    connection, candidate_id = _prepared_connection(path)
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: _classifier_status(effective_blocker_count=0),
    )

    def insert_gateway_command_then_raise(
        enqueue_connection: sqlite3.Connection,
        command,
        **kwargs,
    ) -> None:
        assert kwargs["manage_transaction"] is False
        enqueue_connection.execute(
            """
            INSERT INTO gateway_commands (
                command_id,
                command_type,
                source,
                status,
                idempotency_key,
                payload_json,
                payload_hash
            )
            VALUES (?, ?, ?, 'QUEUED', ?, '{}', 'synthetic-payload-hash')
            """,
            (
                command.command_id,
                command.command_type,
                command.source,
                command.idempotency_key,
            ),
        )
        raise sqlite3.OperationalError("synthetic post-command-insert failure")

    monkeypatch.setattr(
        live_sim_service,
        "enqueue_command",
        insert_gateway_command_then_raise,
    )
    connection.execute("BEGIN DEFERRED")
    connection.execute(
        "INSERT INTO gateway_status (key, value) VALUES ('caller_tx_marker', 'kept')"
    )

    with pytest.raises(sqlite3.OperationalError, match="post-command-insert"):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    assert connection.in_transaction is True
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_status WHERE key = 'caller_tx_marker'"
    ).fetchone()["count"] == 1
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands WHERE command_type = 'send_order'"
    ).fetchone()["count"] == 0
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_command_dedupe_keys"
    ).fetchone()["count"] == 0
    assert connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_orders"
    ).fetchone()["count"] == 0
    assert connection.execute(
        """
        SELECT status
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()["status"] == LiveSimIntentStatus.CREATED.value

    connection.commit()
    connection.close()
    verification = sqlite3.connect(path)
    try:
        assert verification.execute(
            "SELECT COUNT(*) FROM gateway_status WHERE key = 'caller_tx_marker'"
        ).fetchone()[0] == 1
        assert verification.execute(
            "SELECT COUNT(*) FROM gateway_commands WHERE command_type = 'send_order'"
        ).fetchone()[0] == 0
        assert verification.execute(
            "SELECT COUNT(*) FROM gateway_command_dedupe_keys"
        ).fetchone()[0] == 0
        assert verification.execute(
            "SELECT COUNT(*) FROM live_sim_orders"
        ).fetchone()[0] == 0
    finally:
        verification.close()


def test_sell_command_queue_skips_new_buy_lifecycle_blocker(
    tmp_path,
    monkeypatch,
) -> None:
    connection, candidate_id = _prepared_connection(
        tmp_path / "sell-command-new-buy-gate.sqlite3"
    )
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(live_sim_allow_sell=True)
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    connection.execute(
        "UPDATE live_sim_intents SET side = 'SELL' WHERE live_sim_intent_id = ?",
        (intent.live_sim_intent_id,),
    )
    connection.commit()

    def unexpected_classifier_call(*_args, **_kwargs):
        raise AssertionError("SELL queue must not call the new-BUY lifecycle classifier")

    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        unexpected_classifier_call,
    )

    with pytest.raises(ValueError, match="close_only=true"):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )
    rejection = connection.execute(
        """
        SELECT reason_codes_json
        FROM live_sim_rejections
        ORDER BY rowid DESC
        LIMIT 1
        """
    ).fetchone()
    connection.close()

    assert LiveSimReasonCode.LIVE_SIM_LIFECYCLE_ERROR_BLOCK.value not in json.loads(
        rejection["reason_codes_json"]
    )


def test_preflight_passes_historical_only_lifecycle_audit(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "preflight-historical-lifecycle.sqlite3"
    )
    monkeypatch.setattr(
        preflight_service,
        "_execution_lifecycle_blocker_status",
        lambda _connection: _classifier_status(
            effective_blocker_count=0,
            historical_runtime_status_audit_count=3,
        ),
    )

    result = run_live_sim_preflight(
        connection,
        settings=_operating_settings(),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    check = _check(result, "lifecycle_error_count")
    connection.close()

    assert check.status is PreflightStatus.PASS
    assert check.details["effective_blocker_count"] == 0
    assert check.details["historical_runtime_status_audit_count"] == 3
    assert result.counts["lifecycle_error_count"] == 0
    assert result.counts["execution_lifecycle"]["classifier_fail_closed"] is False


@pytest.mark.parametrize(
    ("failure", "expected_reason_code", "expected_error_type"),
    [
        (
            lambda: sqlite3.OperationalError("sensitive-sqlite-detail"),
            "EXECUTION_LIFECYCLE_CLASSIFIER_SQLITE_ERROR",
            "OperationalError",
        ),
        (
            lambda: RuntimeError("sensitive-classifier-detail"),
            "EXECUTION_LIFECYCLE_CLASSIFIER_ERROR",
            "RuntimeError",
        ),
    ],
)
def test_preflight_classifier_errors_block_fail_closed_without_detail_leakage(
    tmp_path,
    monkeypatch,
    failure: Callable[[], Exception],
    expected_reason_code: str,
    expected_error_type: str,
) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / f"preflight-classifier-error-{expected_error_type}.sqlite3"
    )

    def raise_classifier_error(_connection: sqlite3.Connection) -> dict[str, object]:
        raise failure()

    monkeypatch.setattr(
        preflight_service,
        "_execution_lifecycle_blocker_status",
        raise_classifier_error,
    )

    result = run_live_sim_preflight(
        connection,
        settings=_operating_settings(),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    check = _check(result, "lifecycle_error_count")
    serialized = json.dumps(result.to_dict(), sort_keys=True)
    connection.close()

    assert check.status is PreflightStatus.BLOCK
    assert result.status is PreflightStatus.BLOCK
    assert result.counts["lifecycle_error_count"] == 1
    assert check.details["effective_blocker_count"] == 1
    assert check.details["manual_review_blocker_count"] == 1
    assert check.details["classifier_fail_closed"] is True
    assert check.details["classifier_failure_reason_code"] == expected_reason_code
    assert check.details["classifier_error_type"] == expected_error_type
    assert "sensitive-" not in serialized


@pytest.mark.parametrize(
    "classifier_result_factory",
    [
        lambda: {
            "status": "PASS",
            "qualification_status": "PASS",
            "effective_blocker_count": None,
        },
        lambda: _classifier_status_without(
            "observe_only",
            sensitive_detail="not-persisted",
        ),
        lambda: _classifier_status(effective_blocker_count=0)
        | {"no_order_side_effects": False},
        lambda: _classifier_status(effective_blocker_count=0)
        | {"status": "pass", "qualification_status": "pass"},
    ],
)
def test_preflight_invalid_classifier_result_blocks_with_minimum_one_blocker(
    tmp_path,
    monkeypatch,
    classifier_result_factory,
) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "preflight-invalid-classifier.sqlite3"
    )
    monkeypatch.setattr(
        live_sim_service,
        "build_live_sim_execution_lifecycle_status",
        lambda _connection, **_kwargs: classifier_result_factory(),
    )

    result = run_live_sim_preflight(
        connection,
        settings=_operating_settings(),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    check = _check(result, "lifecycle_error_count")
    connection.close()

    assert check.status is PreflightStatus.BLOCK
    assert result.status is PreflightStatus.BLOCK
    assert result.counts["lifecycle_error_count"] == 1
    assert check.details["classifier_failure_reason_code"] == (
        "EXECUTION_LIFECYCLE_CLASSIFIER_INVALID_RESULT"
    )


def _classifier_status(
    *,
    effective_blocker_count: int,
    historical_runtime_status_audit_count: int = 0,
) -> dict[str, object]:
    blocked = effective_blocker_count > 0
    qualification = "BLOCKED" if blocked else "PASS"
    return {
        "status": qualification,
        "qualification_status": qualification,
        "qualification_reason_codes": [],
        "canonical_status": "ACTIVE_LIFECYCLE_BLOCKER" if blocked else "PASS",
        "canonical_reason_codes": [],
        "classification_counts": {
            "ACTIVE_LIFECYCLE_BLOCKER": effective_blocker_count,
            "HISTORICAL_RUNTIME_STATUS_AUDIT": historical_runtime_status_audit_count,
            "MANUAL_REVIEW_BLOCKER": 0,
        },
        "raw_error_count": effective_blocker_count
        + historical_runtime_status_audit_count,
        "mirror_lifecycle_count": 0,
        "logical_subject_count": effective_blocker_count
        + historical_runtime_status_audit_count,
        "active_lifecycle_blocker_count": effective_blocker_count,
        "historical_runtime_status_audit_count": historical_runtime_status_audit_count,
        "manual_review_blocker_count": 0,
        "effective_blocker_count": effective_blocker_count,
        "mirror_consistent": True,
        "inventory_count_consistent": True,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def _queue_boundary_snapshot(
    connection: sqlite3.Connection,
    live_sim_intent_id: str,
) -> dict[str, object]:
    rejection = connection.execute(
        """
        SELECT reason_codes_json, evidence_json
        FROM live_sim_rejections
        ORDER BY rowid DESC
        LIMIT 1
        """
    ).fetchone()
    evidence = json.loads(rejection["evidence_json"])
    return {
        "connection_in_transaction": connection.in_transaction,
        "gateway_command_count": connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands"
        ).fetchone()["count"],
        "live_sim_order_count": connection.execute(
            "SELECT COUNT(*) AS count FROM live_sim_orders"
        ).fetchone()["count"],
        "intent_status": connection.execute(
            """
            SELECT status
            FROM live_sim_intents
            WHERE live_sim_intent_id = ?
            """,
            (live_sim_intent_id,),
        ).fetchone()["status"],
        "reason_codes": json.loads(rejection["reason_codes_json"]),
        "gate": evidence["execution_lifecycle_gate"],
    }


def _classifier_status_without(
    key: str,
    *,
    sensitive_detail: str,
) -> dict[str, object]:
    status = _classifier_status(effective_blocker_count=0)
    status.pop(key)
    status["sensitive"] = sensitive_detail
    return status


def _check(result, name: str):
    return next(check for check in result.checks if check.name == name)
