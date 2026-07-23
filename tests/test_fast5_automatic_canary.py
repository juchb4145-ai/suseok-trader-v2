from __future__ import annotations

import sqlite3

import pytest
from api.routes import live_sim as live_sim_routes
from services.config import Settings
from services.runtime import fast5_automatic_canary as fast5
from services.runtime.fast5_automatic_canary import (
    Fast5AutomaticCanaryGate,
    Fast5CanaryMode,
    Fast5CanaryPhase,
    build_fast5_bootstrap_approval,
    evaluate_fast5_automatic_canary_gate,
    run_fast5_automatic_canary_once,
)
from services.runtime.live_sim_operating_orchestrator import (
    LiveSimOperatingRunResult,
)
from services.runtime.preflight import (
    LiveSimPreflightResult,
    OperatingMode,
    PreflightStatus,
)
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import _pilot_settings

SHA = "a" * 64


def test_fast5_defaults_are_disabled_and_gate_is_read_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "fast5-defaults.sqlite3")
    before = _table_count(connection, "live_sim_operating_runs")

    gate = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=Settings(),
        trade_date="2026-07-20",
    )
    after = _table_count(connection, "live_sim_operating_runs")
    connection.close()

    assert gate.status is Fast5CanaryMode.PROTECT_ONLY
    assert "FAST5_AUTOMATIC_CANARY_DISABLED" in gate.reason_codes
    assert "FAST5_BOOTSTRAP_NOT_AUTHORIZED" in gate.reason_codes
    assert gate.to_dict()["ai_routing_effect"] == 0
    assert before == after == 0


def test_fast5_bootstrap_approval_normalizes_numeric_types() -> None:
    direct = build_fast5_bootstrap_approval(
        Settings(live_sim_fast5_bootstrap_approval_id="fast5-bootstrap-20260723-b"),
        trade_date="2026-07-23",
    )
    parsed_like = build_fast5_bootstrap_approval(
        Settings(
            live_sim_fast5_bootstrap_approval_id="fast5-bootstrap-20260723-b",
            live_sim_max_order_notional=100_000.0,
            live_sim_order_plan_max_notional=100_000.0,
            live_sim_fast5_max_order_notional=100_000.0,
        ),
        trade_date="2026-07-23",
    )

    assert direct["canonical_json"] == parsed_like["canonical_json"]
    assert direct["expected_sha256"] == parsed_like["expected_sha256"]
    assert '"max_order_notional":100000.0' in direct["canonical_json"]


def test_fast5_blocked_queue_request_is_rejected_without_persistent_latch(tmp_path) -> None:
    connection = initialize_database(tmp_path / "fast5-latch.sqlite3")
    settings = _pilot_settings(
        live_sim_fast5_automatic_canary_enabled=True,
        live_sim_fast5_auto_queue_enabled=True,
    )

    first = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    second = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    operating_count = _table_count(connection, "live_sim_operating_runs")
    command_count = _table_count(connection, "gateway_commands")
    connection.close()

    assert first.status == "PROTECT_ONLY"
    assert first.rollback_latched is False
    assert first.to_dict()["buy_command_count"] == 0
    assert second.status == "PROTECT_ONLY"
    assert second.gate.rollback_latch["latched_run_id"] is None
    assert operating_count == 0
    assert command_count == 0


def test_fast5_all_gates_pass_only_with_bound_evidence_and_dynamic_pass(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-ready.sqlite3")
    settings = _qualified_settings()
    _patch_dynamic_pass(monkeypatch)

    gate = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    request_disabled = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=_qualified_settings(
            live_sim_reconcile_request_broker_snapshot_enabled=False
        ),
        trade_date="2026-07-20",
        queue_commands=True,
    )
    monkeypatch.setattr(
        fast5,
        "get_order_broker_boundary_status",
        lambda connection: {
            "effective_status": "WARN",
            "raw_unconfirmed_count": 3,
            "effective_unconfirmed_count": 0,
            "active_order_command_count": 1,
            "effective_block_new_order_routing": False,
            "resolution_maintenance_fence_active": False,
            "reason_codes": [],
        },
    )
    active_order = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    _patch_dynamic_pass(monkeypatch)
    monkeypatch.setattr(fast5, "_pipeline_inventory_count", lambda *args: 501)
    oversized = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    connection.close()

    assert gate.status is Fast5CanaryMode.READY
    assert gate.reason_codes == ()
    assert gate.checks["broker_boundary"]["raw_unconfirmed_count"] == 3
    assert gate.checks["broker_boundary"]["effective_unconfirmed_count"] == 0
    assert gate.effective_limits["max_buy_commands_per_cycle"] == 1
    assert gate.effective_limits["max_daily_buy_count"] == 2
    assert gate.effective_limits["max_order_notional"] == 100_000
    assert "FAST5_BROKER_RECONCILE_NON_PASS" in request_disabled.reason_codes
    assert "FAST5_BROKER_BOUNDARY_BLOCKED" in active_order.reason_codes
    assert active_order.checks["broker_boundary"]["active_order_command_count"] == 1
    assert oversized.status is Fast5CanaryMode.PROTECT_ONLY
    assert "FAST5_PIPELINE_NON_PASS" in oversized.reason_codes
    assert oversized.checks["pipeline_coherency"]["full_inventory_count"] == 501


def test_fast5_pipeline_gate_blocks_coverage_mismatch_and_zero_plan_ready(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-pipeline-scope.sqlite3")
    settings = _qualified_settings()
    _patch_dynamic_pass(monkeypatch)
    monkeypatch.setattr(fast5, "_pipeline_inventory_count", lambda *args: 2)

    coverage_mismatch = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    monkeypatch.setattr(fast5, "_pipeline_inventory_count", lambda *args: 1)
    monkeypatch.setattr(
        fast5,
        "build_fast5_plan_ready_coherency_status",
        lambda *args, **kwargs: {
            "status": "FAIL",
            "reason_codes": ["NO_UNEXPIRED_PLAN_READY"],
            "latest_plan_ready_count": 0,
            "unexpired_plan_ready_count": 0,
            "coherent_plan_ready_count": 0,
            "non_pass_plan_ready_count": 0,
            "missing_lineage_plan_ready_count": 0,
            "stale_plan_ready_count": 0,
            "mismatch_plan_ready_count": 0,
            "invalid_expiry_plan_ready_count": 0,
            "selection_truncated": False,
            "generated_at": "2026-07-20T00:00:00Z",
        },
    )
    zero_plan_ready = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    connection.close()

    assert coverage_mismatch.status is Fast5CanaryMode.PROTECT_ONLY
    assert "FAST5_PIPELINE_NON_PASS" in coverage_mismatch.reason_codes
    assert coverage_mismatch.checks["pipeline_coherency"][
        "full_inventory_covered"
    ] is False
    assert "PIPELINE_INVENTORY_COVERAGE_MISMATCH" in coverage_mismatch.checks[
        "pipeline_coherency"
    ]["qualification_reason_codes"]
    assert zero_plan_ready.status is Fast5CanaryMode.PROTECT_ONLY
    assert "FAST5_PIPELINE_NON_PASS" in zero_plan_ready.reason_codes
    assert zero_plan_ready.checks["pipeline_coherency"][
        "unexpired_plan_ready_count"
    ] == 0


@pytest.mark.parametrize(
    "caller_transaction",
    [False, True],
    ids=["owned-snapshot", "caller-snapshot"],
)
def test_fast5_pipeline_reads_share_one_snapshot_and_preserve_caller_transaction(
    tmp_path,
    monkeypatch,
    caller_transaction: bool,
) -> None:
    db_path = tmp_path / f"fast5-snapshot-{caller_transaction}.sqlite3"
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    writer = sqlite3.connect(db_path)
    writer.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE fast5_snapshot_probe (probe_id INTEGER PRIMARY KEY)"
    )
    connection.execute("INSERT INTO fast5_snapshot_probe DEFAULT VALUES")
    connection.commit()
    if caller_transaction:
        connection.execute("BEGIN DEFERRED")

    traced_statements: list[str] = []
    observed_reads: list[tuple[str, bool, int]] = []
    captured: dict = {}
    connection.set_trace_callback(traced_statements.append)

    def read_probe(stage: str) -> int:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM fast5_snapshot_probe"
        ).fetchone()
        count = int(row["count"] if row else 0)
        observed_reads.append((stage, connection.in_transaction, count))
        return count

    def inventory_count(actual_connection, trade_date: str) -> int:
        assert actual_connection is connection
        assert trade_date == "2026-07-20"
        count = read_probe("inventory")
        writer.execute("INSERT INTO fast5_snapshot_probe DEFAULT VALUES")
        writer.commit()
        return count

    def diagnostic_status(actual_connection, **kwargs) -> dict:
        assert actual_connection is connection
        assert kwargs["trade_date"] == "2026-07-20"
        count = read_probe("diagnostic")
        return {
            "status": "PASS",
            "reason_codes": [],
            "candidate_count": count,
            "coherent_count": count,
            "mismatch_count": 0,
            "missing_lineage_count": 0,
            "stale_count": 0,
        }

    def qualification_status(
        actual_connection,
        *,
        pipeline_status,
        **kwargs,
    ) -> dict:
        assert actual_connection is connection
        assert pipeline_status["candidate_count"] == 1
        assert kwargs["trade_date"] == "2026-07-20"
        count = read_probe("qualification")
        return {
            "status": "PASS",
            "reason_codes": [],
            "latest_plan_ready_count": count,
            "unexpired_plan_ready_count": count,
            "coherent_plan_ready_count": count,
            "non_pass_plan_ready_count": 0,
            "missing_lineage_plan_ready_count": 0,
            "stale_plan_ready_count": 0,
            "mismatch_plan_ready_count": 0,
            "invalid_expiry_plan_ready_count": 0,
            "selection_truncated": False,
            "generated_at": "2026-07-20T00:00:00Z",
        }

    def capture(name, passed, reason_code, details, **kwargs) -> None:
        captured.update(
            name=name,
            passed=passed,
            reason_code=reason_code,
            details=details,
            kwargs=kwargs,
        )

    monkeypatch.setattr(fast5, "_pipeline_inventory_count", inventory_count)
    monkeypatch.setattr(fast5, "build_pipeline_coherency_status", diagnostic_status)
    monkeypatch.setattr(
        fast5,
        "build_fast5_plan_ready_coherency_status",
        qualification_status,
    )

    try:
        fast5._add_pipeline_check(
            capture,
            connection,
            Settings(),
            "2026-07-20",
        )
        caller_transaction_preserved = connection.in_transaction
        row = writer.execute(
            "SELECT COUNT(*) AS count FROM fast5_snapshot_probe"
        ).fetchone()
        writer_count = int(row["count"] if row else 0)
    finally:
        connection.set_trace_callback(None)
        if connection.in_transaction:
            connection.rollback()
        writer.close()
        connection.close()

    transaction_statements = [
        statement.strip().upper()
        for statement in traced_statements
        if statement.strip()
        .upper()
        .startswith(("BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"))
    ]
    assert observed_reads == [
        ("inventory", True, 1),
        ("diagnostic", True, 1),
        ("qualification", True, 1),
    ]
    assert writer_count == 2
    assert captured["passed"] is True
    assert captured["details"]["read_snapshot_consistent"] is True
    assert caller_transaction_preserved is caller_transaction
    if caller_transaction:
        assert transaction_statements == [
            "SAVEPOINT FAST5_PIPELINE_READ_SNAPSHOT",
            "ROLLBACK TO SAVEPOINT FAST5_PIPELINE_READ_SNAPSHOT",
            "RELEASE SAVEPOINT FAST5_PIPELINE_READ_SNAPSHOT",
        ]
    else:
        assert transaction_statements == ["BEGIN DEFERRED", "ROLLBACK"]


def test_fast5_pipeline_gate_fails_closed_when_read_snapshot_ends(
    tmp_path,
    monkeypatch,
) -> None:
    connection = sqlite3.connect(tmp_path / "fast5-snapshot-ended.sqlite3")
    captured: dict = {}
    qualification_called = False

    def inventory_count(*args) -> int:
        assert connection.in_transaction
        return 1

    def diagnostic_status(*args, **kwargs) -> dict:
        assert connection.in_transaction
        connection.commit()
        return {
            "status": "PASS",
            "reason_codes": [],
            "candidate_count": 1,
            "coherent_count": 1,
            "mismatch_count": 0,
            "missing_lineage_count": 0,
            "stale_count": 0,
        }

    def qualification_status(*args, **kwargs) -> dict:
        nonlocal qualification_called
        qualification_called = True
        return {}

    def capture(name, passed, reason_code, details, **kwargs) -> None:
        captured.update(
            name=name,
            passed=passed,
            reason_code=reason_code,
            details=details,
        )

    monkeypatch.setattr(fast5, "_pipeline_inventory_count", inventory_count)
    monkeypatch.setattr(fast5, "build_pipeline_coherency_status", diagnostic_status)
    monkeypatch.setattr(
        fast5,
        "build_fast5_plan_ready_coherency_status",
        qualification_status,
    )

    fast5._add_pipeline_check(capture, connection, Settings(), "2026-07-20")
    connection.close()

    assert qualification_called is False
    assert captured == {
        "name": "pipeline_coherency",
        "passed": False,
        "reason_code": "FAST5_PIPELINE_NON_PASS",
        "details": {
            "classifier_error_type": "RuntimeError",
            "read_snapshot_consistent": False,
        },
    }


def test_fast5_ready_run_forces_hard_limits_and_disables_ai_and_reprice(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-strict.sqlite3")
    settings = _qualified_settings(
        live_sim_max_daily_order_count=3,
        live_sim_max_daily_notional=30_000_000,
        live_sim_max_order_notional=3_000_000,
        live_sim_order_plan_min_notional=10_000,
        live_sim_order_plan_default_notional=3_000_000,
        live_sim_order_plan_max_notional=3_000_000,
        entry_timing_default_notional=3_000_000,
        entry_timing_max_notional=3_000_000,
        live_sim_reprice_enabled=True,
        live_sim_operating_include_ai=True,
        live_sim_operating_write_runs=False,
    )
    gate = Fast5AutomaticCanaryGate(
        status=Fast5CanaryMode.READY,
        phase=Fast5CanaryPhase.QUALIFIED,
        trade_date="2026-07-20",
        queue_commands_requested=True,
    )
    preflight = _passing_preflight()
    monkeypatch.setattr(fast5, "_evaluate_gate", lambda *args, **kwargs: (gate, preflight))
    captured = {}

    def fake_operating_run(connection, **kwargs):
        captured.update(kwargs)
        return LiveSimOperatingRunResult(
            run_id="operating-placeholder",
            trade_date="2026-07-20",
            mode=OperatingMode.PILOT_BUY_ONLY,
            queue_commands=True,
            preflight=preflight,
            status="COMPLETED",
            buy_command_count=1,
        )

    monkeypatch.setattr(fast5, "run_live_sim_operating_cycle_once", fake_operating_run)

    result = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    connection.close()

    strict = captured["settings"]
    assert result.status == "COMPLETED"
    assert result.rollback_latched is False
    assert captured["mode"] is OperatingMode.PILOT_BUY_ONLY
    assert captured["queue_commands"] is True
    assert captured["include_ai"] is False
    assert strict.live_sim_max_daily_order_count == 2
    assert strict.live_sim_max_order_notional == 100_000
    assert strict.live_sim_order_plan_min_notional == 10_000
    assert strict.live_sim_order_plan_default_notional == 100_000
    assert strict.live_sim_order_plan_max_notional == 100_000
    assert strict.entry_timing_default_notional == 100_000
    assert strict.entry_timing_max_notional == 100_000
    assert strict.live_sim_max_active_orders == 1
    assert strict.live_sim_max_active_positions == 1
    assert strict.live_sim_operating_max_buy_commands_per_cycle == 1
    assert strict.live_sim_reprice_enabled is False
    assert strict.live_sim_reconcile_enabled is False
    assert strict.live_sim_position_allow_scale_in is False


def test_fast5_bootstrap_uses_date_bound_approval_and_advisory_alpha_shadow(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-bootstrap-gate.sqlite3")
    base = _bootstrap_settings()
    approval = build_fast5_bootstrap_approval(base, trade_date="2026-07-20")
    settings = _bootstrap_settings(
        live_sim_fast5_bootstrap_status="PENDING",
        live_sim_fast5_bootstrap_approval_sha256=approval["expected_sha256"],
    )
    _patch_dynamic_pass(monkeypatch)

    gate = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    wrong_date = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-21",
        queue_commands=True,
    )
    reserved, _ = fast5._reserve_bootstrap_once(
        connection,
        settings=settings,
        gate=gate,
        preflight=_passing_preflight(),
    )
    consumed = evaluate_fast5_automatic_canary_gate(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    connection.close()

    assert gate.status is Fast5CanaryMode.READY
    assert gate.phase is Fast5CanaryPhase.BOOTSTRAP
    assert gate.effective_limits["max_daily_buy_count"] == 1
    assert gate.checks["alpha"]["status"] == "WARN"
    assert gate.checks["parallel_shadow"]["status"] == "WARN"
    assert "FAST5_ALPHA_NOT_QUALIFIED" not in gate.reason_codes
    assert gate.bootstrap_approval["matches"] is True
    assert "FAST5_BOOTSTRAP_APPROVAL_MISMATCH" in wrong_date.reason_codes
    assert reserved is True
    assert "FAST5_BOOTSTRAP_ALREADY_CONSUMED" in consumed.reason_codes


def test_fast5_bootstrap_reserves_once_then_runs_protect_only_lifecycle(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-bootstrap-once.sqlite3")
    base = _bootstrap_settings()
    approval = build_fast5_bootstrap_approval(base, trade_date="2026-07-20")
    settings = _bootstrap_settings(
        live_sim_fast5_bootstrap_status="PENDING",
        live_sim_fast5_bootstrap_approval_sha256=approval["expected_sha256"],
    )
    preflight = _passing_preflight()
    ready_gate = Fast5AutomaticCanaryGate(
        status=Fast5CanaryMode.READY,
        phase=Fast5CanaryPhase.BOOTSTRAP,
        trade_date="2026-07-20",
        queue_commands_requested=True,
        bootstrap_approval=approval,
    )
    monkeypatch.setattr(
        fast5,
        "_evaluate_gate",
        lambda *args, **kwargs: (ready_gate, preflight),
    )
    calls = []

    def fake_operating_run(connection, **kwargs):
        calls.append(kwargs)
        mode = kwargs["mode"]
        return LiveSimOperatingRunResult(
            run_id="operating-placeholder",
            trade_date="2026-07-20",
            mode=mode,
            queue_commands=True,
            preflight=preflight,
            status="COMPLETED",
            buy_command_count=1 if mode is OperatingMode.PILOT_BUY_ONLY else 0,
            cancel_command_count=1 if mode is OperatingMode.PROTECT_ONLY else 0,
        )

    monkeypatch.setattr(fast5, "run_live_sim_operating_cycle_once", fake_operating_run)
    first = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    usage = fast5._bootstrap_usage(
        connection,
        settings.live_sim_fast5_bootstrap_approval_sha256,
    )
    reserved_again, existing = fast5._reserve_bootstrap_once(
        connection,
        settings=settings,
        gate=ready_gate,
        preflight=preflight,
    )
    consumed_gate = Fast5AutomaticCanaryGate(
        status=Fast5CanaryMode.PROTECT_ONLY,
        phase=Fast5CanaryPhase.BOOTSTRAP,
        trade_date="2026-07-20",
        queue_commands_requested=True,
        reason_codes=("FAST5_BOOTSTRAP_ALREADY_CONSUMED",),
        bootstrap_approval=approval,
    )
    monkeypatch.setattr(
        fast5,
        "_evaluate_gate",
        lambda *args, **kwargs: (consumed_gate, preflight),
    )
    second = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    monkeypatch.setattr(
        fast5,
        "run_live_sim_operating_cycle_once",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("protect failure")),
    )
    failed_protection = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    connection.close()

    assert first.status == "BOOTSTRAP_QUEUED"
    assert first.to_dict()["buy_command_count"] == 1
    assert calls[0]["settings"].live_sim_max_daily_order_count == 1
    assert usage["consumed"] is True
    assert reserved_again is False
    assert existing == usage["reservation_run_id"]
    assert second.mode is OperatingMode.PROTECT_ONLY
    assert second.to_dict()["buy_command_count"] == 0
    assert second.to_dict()["cancel_command_count"] == 1
    assert calls[1]["mode"] is OperatingMode.PROTECT_ONLY
    assert failed_protection.rollback_latched is True
    assert failed_protection.to_dict()["side_effects_unknown"] is True
    assert failed_protection.to_dict()["no_order_side_effects"] is False
    assert failed_protection.operating_run["errors"] == [
        {
            "stage": "fast5_bootstrap_protect_only_cycle",
            "error_type": "RuntimeError",
        }
    ]


def test_fast5_bootstrap_cycle_exception_is_audited_and_latched(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "fast5-bootstrap-error.sqlite3")
    base = _bootstrap_settings()
    approval = build_fast5_bootstrap_approval(base, trade_date="2026-07-20")
    settings = _bootstrap_settings(
        live_sim_fast5_bootstrap_status="PENDING",
        live_sim_fast5_bootstrap_approval_sha256=approval["expected_sha256"],
    )
    preflight = _passing_preflight()
    gate = Fast5AutomaticCanaryGate(
        status=Fast5CanaryMode.READY,
        phase=Fast5CanaryPhase.BOOTSTRAP,
        trade_date="2026-07-20",
        queue_commands_requested=True,
        bootstrap_approval=approval,
    )
    monkeypatch.setattr(fast5, "_evaluate_gate", lambda *args, **kwargs: (gate, preflight))
    monkeypatch.setattr(
        fast5,
        "run_live_sim_operating_cycle_once",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("test failure")),
    )

    result = run_fast5_automatic_canary_once(
        connection,
        settings=settings,
        trade_date="2026-07-20",
        queue_commands=True,
    )
    latch = fast5._latest_unacknowledged_rollback(connection, settings)
    operating_count = _table_count(connection, "live_sim_operating_runs")
    connection.close()

    assert result.status == "PROTECT_ONLY"
    assert result.rollback_latched is True
    assert result.to_dict()["side_effects_unknown"] is True
    assert result.to_dict()["no_order_side_effects"] is False
    assert result.operating_run["errors"] == [
        {"stage": "fast5_operating_cycle", "error_type": "RuntimeError"}
    ]
    assert latch["latched_run_id"] == result.run_id
    assert operating_count == 2


def test_fast5_v3_preserves_unacknowledged_v1_rollback_latch(tmp_path) -> None:
    connection = initialize_database(tmp_path / "fast5-v1-latch.sqlite3")
    run = LiveSimOperatingRunResult(
        run_id="fast5-v1-failure",
        trade_date="2026-07-19",
        mode=OperatingMode.PROTECT_ONLY,
        queue_commands=False,
        preflight=_passing_preflight(),
        status="PROTECT_ONLY",
        reason_summary={
            "policy": "fast5-guarded-automatic-canary.v1",
            "rollback_latched": True,
        },
    )
    fast5.save_live_sim_operating_run(connection, run)
    connection.commit()

    latch = fast5._latest_unacknowledged_rollback(connection, _pilot_settings())
    connection.close()

    assert latch["latched_run_id"] == "fast5-v1-failure"
    assert latch["acknowledgement_required"] is True


def test_fast5_config_requires_sha_binding_and_hard_caps() -> None:
    with pytest.raises(ValueError, match="BOOTSTRAP_EVIDENCE_SHA256 is required"):
        _pilot_settings(live_sim_fast5_bootstrap_status="PASS")
    with pytest.raises(ValueError, match="BOOTSTRAP_APPROVAL_SHA256 is required"):
        _pilot_settings(
            live_sim_fast5_bootstrap_status="PENDING",
            live_sim_fast5_bootstrap_trade_date="2026-07-20",
            live_sim_fast5_bootstrap_approval_id="approval-1",
        )
    with pytest.raises(ValueError, match="MAX_DAILY_BUY_COUNT"):
        _pilot_settings(live_sim_fast5_max_daily_buy_count=3)
    with pytest.raises(ValueError, match="MAX_ORDER_NOTIONAL"):
        _pilot_settings(live_sim_fast5_max_order_notional=100_001)


def test_fast5_status_route_is_read_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fast5-route.sqlite3"
    connection = initialize_database(db_path)
    connection.close()
    settings = _pilot_settings(trading_db_path=db_path)
    monkeypatch.setattr(live_sim_routes, "load_settings", lambda: settings)

    response = live_sim_routes.live_sim_automatic_canary_status(
        trade_date="2026-07-20"
    )
    connection = initialize_database(db_path)
    order_artifact_counts = {
        table: _table_count(connection, table)
        for table in (
            "live_sim_operating_runs",
            "live_sim_intents",
            "live_sim_orders",
            "gateway_commands",
        )
    }
    connection.close()

    assert response["read_only"] is True
    assert response["no_order_side_effects"] is True
    assert response["automatic_canary"]["status"] == "PROTECT_ONLY"
    assert order_artifact_counts == {
        "live_sim_operating_runs": 0,
        "live_sim_intents": 0,
        "live_sim_orders": 0,
        "gateway_commands": 0,
    }


def _qualified_settings(**overrides) -> Settings:
    values = {
        "live_sim_fast5_automatic_canary_enabled": True,
        "live_sim_fast5_auto_queue_enabled": True,
        "live_sim_fast5_bootstrap_status": "PASS",
        "live_sim_fast5_bootstrap_evidence_sha256": SHA,
        "live_sim_fast5_alpha_status": "ALPHA_QUALIFIED",
        "live_sim_fast5_alpha_evidence_sha256": SHA,
        "live_sim_fast5_shadow_status": "PASS",
        "live_sim_fast5_shadow_evidence_sha256": SHA,
        "live_sim_pilot_auto_queue_command": True,
        "live_sim_reconcile_request_broker_snapshot_enabled": True,
    }
    values.update(overrides)
    return _pilot_settings(**values)


def _bootstrap_settings(**overrides) -> Settings:
    values = {
        "live_sim_fast5_automatic_canary_enabled": True,
        "live_sim_fast5_auto_queue_enabled": True,
        "live_sim_fast5_bootstrap_status": "BLOCKED",
        "live_sim_fast5_bootstrap_trade_date": "2026-07-20",
        "live_sim_fast5_bootstrap_approval_id": "fast5-bootstrap-20260720-a",
        "live_sim_pilot_auto_queue_command": True,
        "live_sim_reconcile_request_broker_snapshot_enabled": True,
    }
    values.update(overrides)
    return _pilot_settings(**values)


def _patch_dynamic_pass(monkeypatch) -> None:
    preflight = _passing_preflight()
    monkeypatch.setattr(fast5, "run_live_sim_preflight", lambda *args, **kwargs: preflight)
    monkeypatch.setattr(
        fast5,
        "build_pipeline_coherency_status",
        lambda *args, **kwargs: {
            "status": "PASS",
            "reason_codes": [],
            "candidate_count": 1,
            "coherent_count": 1,
            "mismatch_count": 0,
            "missing_lineage_count": 0,
            "stale_count": 0,
        },
    )
    monkeypatch.setattr(
        fast5,
        "build_fast5_plan_ready_coherency_status",
        lambda *args, **kwargs: {
            "status": "PASS",
            "reason_codes": [],
            "latest_plan_ready_count": 1,
            "unexpired_plan_ready_count": 1,
            "coherent_plan_ready_count": 1,
            "non_pass_plan_ready_count": 0,
            "missing_lineage_plan_ready_count": 0,
            "stale_plan_ready_count": 0,
            "mismatch_plan_ready_count": 0,
            "invalid_expiry_plan_ready_count": 0,
            "selection_truncated": False,
            "generated_at": "2026-07-20T00:00:00Z",
        },
    )
    monkeypatch.setattr(fast5, "_pipeline_inventory_count", lambda *args: 1)
    monkeypatch.setattr(
        fast5,
        "get_order_broker_boundary_status",
        lambda connection: {
            "effective_status": "PASS",
            "raw_unconfirmed_count": 3,
            "effective_unconfirmed_count": 0,
            "effective_block_new_order_routing": False,
            "resolution_maintenance_fence_active": False,
            "reason_codes": [],
        },
    )
    monkeypatch.setattr(
        fast5,
        "build_live_sim_execution_lifecycle_status",
        lambda connection: {
            "qualification_status": "PASS",
            "effective_blocker_count": 0,
            "classifier_fail_closed": False,
            "reason_codes": [],
        },
    )
    monkeypatch.setattr(
        fast5,
        "get_latest_live_sim_reconcile",
        lambda connection: _fresh_reconcile(),
    )


def _passing_preflight() -> LiveSimPreflightResult:
    return LiveSimPreflightResult(
        status=PreflightStatus.PASS,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
    )


def _fresh_reconcile() -> dict:
    return {
        "reconcile_id": "reconcile-fast5",
        "status": "OK",
        "mismatch_count": 0,
        "blocking_new_buy": False,
        "snapshot_json": {
            "broker_snapshot": {
                "snapshot_id": "broker-fast5",
                "snapshot_status": "COMPLETE",
                "complete": True,
                "snapshot_at": fast5.datetime_to_wire(fast5.utc_now()),
                "stale_after_sec": 120,
                "trade_date": "2026-07-20",
            }
        },
    }


def _table_count(connection, table_name: str) -> int:
    return int(
        connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()[
            "count"
        ]
    )
