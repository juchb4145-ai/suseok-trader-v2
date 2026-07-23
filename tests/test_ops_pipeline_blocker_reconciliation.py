from __future__ import annotations

import copy
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.pipeline_coherency_disposition import (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
)
from storage.sqlite import initialize_database
from tests.test_ops_fast0_blocker_resolution_plan import _write_fast0_baseline
from tests.test_pipeline_coherency_disposition import _expired_closed_pipeline
from tools import ops_fast0_blocker_resolution_plan as plan_tool
from tools import ops_pipeline_blocker_reconciliation as tool

_OBSERVED_AT = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)


def test_expired_plan_reconciliation_is_aggregate_only_and_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "expired.sqlite3"
    connection, candidate_id, _, _ = _expired_closed_pipeline(db_path)
    connection.close()
    plan_path = _build_plan_report(tmp_path, db_path, monkeypatch=monkeypatch)

    report = tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        out_dir=tmp_path / "reconciliation",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")
    reconciliation = report["pipeline_reconciliation"]

    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["verdict"]["reconciliation_status"] == "COMPLETE"
    assert reconciliation["target_count"] == 1
    assert reconciliation["expected_action_counts"] == {
        ACTION_DISPOSE_EXPIRED_PLAN_READY: 1,
        ACTION_DISPOSE_ORPHAN: 0,
    }
    assert reconciliation["source_state_counts"]["NO_ACTIVE_SOURCE"] == 1
    assert reconciliation["downstream_state_counts"]["SAFE_DB_PROVEN"] == 1
    assert reconciliation["outcome_counts"]["DB_PREVIEW_ELIGIBLE"] == 1
    assert reconciliation["preview_eligible_count"] == 1
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert report["database_write_performed"] is False
    assert report["eligibility_changed"] is False
    assert report["apply_authorized"] is False
    assert candidate_id not in raw
    assert '"candidate_instance_id"' not in raw
    assert str(tmp_path) not in raw


def test_orphan_is_never_promoted_without_authoritative_manual_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "orphan.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (
            'orphan-private-id', 'orphan-risk', 'missing-strategy',
            '2026-07-15', '005930', 'fixture', '2026-07-15T00:00:00Z',
            'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0, '[]', 'test', 1
        )
        """
    )
    connection.commit()
    connection.close()
    plan_path = _build_plan_report(tmp_path, db_path, monkeypatch=monkeypatch)

    report = tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        out_dir=tmp_path / "reconciliation",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")
    reconciliation = report["pipeline_reconciliation"]

    assert report["verdict"]["evidence_status"] == "PASS"
    assert reconciliation["expected_action_counts"] == {
        ACTION_DISPOSE_EXPIRED_PLAN_READY: 0,
        ACTION_DISPOSE_ORPHAN: 1,
    }
    assert reconciliation["source_state_counts"]["CANDIDATE_ABSENT"] == 1
    assert reconciliation["outcome_counts"]["MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED"] == 1
    assert reconciliation["preview_eligible_count"] == 1
    assert reconciliation["manual_evidence_required_count"] == 1
    assert (
        "PIPELINE_MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED"
        in report["verdict"]["preparation_requirements"]
    )
    assert "orphan-private-id" not in raw


def test_source_and_downstream_classification_fail_closed() -> None:
    assert (
        tool._source_state(
            {
                "historical_event_active_count": 2,
                "latest_active_count": 0,
                "source_projection_consistent": True,
            },
            candidate={"present": True, "active_source_count": 0},
        )
        == "HISTORICAL_EVENT_ONLY"
    )
    assert (
        tool._source_state(
            {
                "historical_event_active_count": 1,
                "latest_active_count": 0,
                "source_projection_consistent": False,
            },
            candidate={"present": True, "active_source_count": 1},
        )
        == "PROJECTION_INCONSISTENT"
    )
    assert (
        tool._source_state(
            {
                "historical_event_active_count": 0,
                "latest_active_count": True,
                "source_projection_consistent": True,
            },
            candidate={"present": True, "active_source_count": 1},
        )
        == "UNKNOWN"
    )
    assert (
        tool._source_state(
            {
                "historical_event_active_count": 1,
                "latest_active_count": 1,
                "source_projection_consistent": False,
            },
            candidate={"present": False, "active_source_count": None},
        )
        == "ORPHAN_ACTIVE_SOURCE_PRESENT"
    )
    assert tool._downstream_state(_safe_downstream(unknown_boundary_count=1)) == (
        "MANUAL_BROKER_EVIDENCE_REQUIRED"
    )
    assert (
        tool._downstream_state(_safe_downstream(malformed_gateway_command_payload_count=1))
        == "REPAIR_REQUIRED"
    )
    assert tool._downstream_state({}) == "UNKNOWN"
    assert tool._downstream_state(_safe_downstream(unexpired_plan_count=-1)) == "UNKNOWN"
    assert tool._downstream_state(_safe_downstream(unexpired_plan_count=True)) == "UNKNOWN"


def test_preview_semantics_reject_internal_contradictions() -> None:
    eligible = {
        "status": "ELIGIBLE",
        "eligible": True,
        "reason_codes": [],
        "action": ACTION_DISPOSE_EXPIRED_PLAN_READY,
        "expected_action": ACTION_DISPOSE_EXPIRED_PLAN_READY,
    }
    assert tool._preview_semantics_safe(
        eligible,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
        reasons=[],
        reasons_valid=True,
        source_state="NO_ACTIVE_SOURCE",
        downstream_state="SAFE_DB_PROVEN",
    )

    contradictions = [
        ({**eligible, "reason_codes": ["BLOCKED"]}, ["BLOCKED"], True),
        ({"status": "BLOCKED", "eligible": False, "reason_codes": []}, [], True),
        ({**eligible, "status": "BLOCKED"}, [], True),
        ({**eligible, "reason_codes": "not-a-list"}, [], False),
    ]
    for preview, reasons, reasons_valid in contradictions:
        assert not tool._preview_semantics_safe(
            preview,
            action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
            reasons=reasons,
            reasons_valid=reasons_valid,
            source_state="NO_ACTIVE_SOURCE",
            downstream_state="SAFE_DB_PROVEN",
        )
    assert not tool._preview_semantics_safe(
        eligible,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
        reasons=[],
        reasons_valid=True,
        source_state="CURRENT_ACTIVE",
        downstream_state="SAFE_DB_PROVEN",
    )
    assert not tool._preview_semantics_safe(
        eligible,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
        reasons=[],
        reasons_valid=True,
        source_state="NO_ACTIVE_SOURCE",
        downstream_state="UNKNOWN",
    )
    assert not tool._preview_semantics_safe(
        eligible,
        action=ACTION_DISPOSE_EXPIRED_PLAN_READY,
        reasons=[],
        reasons_valid=True,
        source_state="CANDIDATE_ABSENT",
        downstream_state="SAFE_DB_PROVEN",
    )
    assert not tool._preview_semantics_safe(
        {
            **eligible,
            "action": ACTION_DISPOSE_ORPHAN,
            "expected_action": ACTION_DISPOSE_EXPIRED_PLAN_READY,
        },
        action=ACTION_DISPOSE_ORPHAN,
        reasons=[],
        reasons_valid=True,
        source_state="CANDIDATE_ABSENT",
        downstream_state="SAFE_DB_PROVEN",
    )


def test_verdict_rejects_plan_target_drift_and_any_write_contract() -> None:
    report = _minimal_report()
    assert tool.evaluate_report(report)["evidence_status"] == "PASS"

    drifted = copy.deepcopy(report)
    drifted["pipeline_reconciliation"]["expected_action_counts"][
        ACTION_DISPOSE_EXPIRED_PLAN_READY
    ] = 2
    verdict = tool.evaluate_report(drifted)
    assert verdict["evidence_status"] == "FAIL"
    assert "PIPELINE_TARGET_COUNT_BASELINE_MISMATCH" in verdict["evidence_failures"]

    write_claim = copy.deepcopy(report)
    write_claim["database_write_performed"] = True
    verdict = tool.evaluate_report(write_claim)
    assert "READ_ONLY_RECONCILIATION_CONTRACT_INVALID" in verdict["evidence_failures"]

    invalid = copy.deepcopy(report)
    invalid["pipeline_reconciliation"]["outcome_counts"] = {"INVALID_EVIDENCE": 1}
    verdict = tool.evaluate_report(invalid)
    assert verdict["evidence_status"] == "FAIL"
    assert "PIPELINE_RECONCILIATION_EVIDENCE_INVALID" in verdict["evidence_failures"]

    eligible_drift = copy.deepcopy(report)
    eligible_drift["pipeline_reconciliation"]["preview_eligible_action_counts"] = {
        ACTION_DISPOSE_EXPIRED_PLAN_READY: 0,
        ACTION_DISPOSE_ORPHAN: 1,
    }
    verdict = tool.evaluate_report(eligible_drift)
    assert "PIPELINE_RECONCILIATION_COUNT_CONSERVATION_INVALID" in verdict["evidence_failures"]


def test_verdict_rejects_tampered_read_only_privacy_and_identity_contracts() -> None:
    mutations = (
        lambda value: value.__setitem__("apply_authorized", True),
        lambda value: value.__setitem__("apply_authorized", 0),
        lambda value: value.__setitem__("read_only", 1),
        lambda value: value.__setitem__("observe_only", False),
        lambda value: value.__setitem__("raw_rows_recorded", True),
        lambda value: value.__setitem__("raw_payloads_recorded", True),
        lambda value: value.__setitem__("live_sim_allowed", True),
        lambda value: value["tool_actions"].__setitem__("order_operation_invoked", True),
        lambda value: value["tool_actions"].__setitem__("order_operation_invoked", 0),
        lambda value: value["pipeline_reconciliation"].__setitem__("apply_authorized", True),
        lambda value: value["pipeline_reconciliation"].__setitem__("apply_authorized", 0),
    )
    for mutate in mutations:
        report = _minimal_report()
        mutate(report)
        verdict = tool.evaluate_report(report)
        assert verdict["evidence_status"] == "FAIL"
        assert "READ_ONLY_RECONCILIATION_CONTRACT_INVALID" in verdict["evidence_failures"]

    report = _minimal_report()
    report["database"]["connection"]["mode"] = "rw"
    assert (
        "STRICT_READ_ONLY_CONNECTION_INVALID" in tool.evaluate_report(report)["evidence_failures"]
    )

    report = _minimal_report()
    report["database"]["connection"]["query_only"] = 1
    assert (
        "STRICT_READ_ONLY_CONNECTION_INVALID" in tool.evaluate_report(report)["evidence_failures"]
    )

    report = _minimal_report()
    report["database"]["identity"]["app_name_value_valid"] = False
    assert "DATABASE_APP_IDENTITY_INVALID" in tool.evaluate_report(report)["evidence_failures"]

    report = _minimal_report()
    report["database"]["identity"]["app_name_row_count"] = True
    assert "DATABASE_APP_IDENTITY_INVALID" in tool.evaluate_report(report)["evidence_failures"]

    report = _minimal_report()
    report["database"]["identity_pin"]["held_across_snapshot"] = 1
    assert "DATABASE_IDENTITY_PIN_FAILED" in tool.evaluate_report(report)["evidence_failures"]

    report = _minimal_report()
    report["database"]["database_list"][0]["file_present"] = 1
    assert (
        "DATABASE_ATTACHMENT_CONTRACT_INVALID" in tool.evaluate_report(report)["evidence_failures"]
    )


def _build_plan_report(
    tmp_path: Path,
    db_path: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    fast0_baseline = _write_fast0_baseline(tmp_path, db_path)
    monkeypatch.setattr(plan_tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)
    report = plan_tool.run_report(
        db_path=db_path,
        fast0_report=fast0_baseline,
        expected_fast0_report_sha256=_sha256_file(fast0_baseline),
        out_dir=tmp_path / f"{db_path.stem}-plan",
        observed_at=_OBSERVED_AT,
    )
    return Path(report["report_paths"]["raw_json"])


def _minimal_report() -> dict:
    fingerprint = {
        "exists": True,
        "size": 1,
        "mtime_ns": 1,
        "sha256": "a" * 64,
    }
    probe = {
        "status": "PASS",
        "no_other_open_handles": True,
    }
    actions = {
        ACTION_DISPOSE_EXPIRED_PLAN_READY: 1,
        ACTION_DISPOSE_ORPHAN: 1,
    }
    source_counts = {state: 0 for state in tool._SOURCE_STATES}
    source_counts["CANDIDATE_ABSENT"] = 1
    source_counts["NO_ACTIVE_SOURCE"] = 1
    downstream_counts = {state: 0 for state in tool._DOWNSTREAM_STATES}
    downstream_counts["SAFE_DB_PROVEN"] = 2
    outcome_counts = {state: 0 for state in tool._OUTCOMES}
    outcome_counts["DB_PREVIEW_ELIGIBLE"] = 1
    outcome_counts["MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED"] = 1
    return {
        "source_blocker_plan": {
            "contract": tool._EXPECTED_PLAN_CONTRACT,
            "report_sha256": "c" * 64,
            "evidence_status": "PASS",
            "plan_status": "COMPLETE",
            "database_main_matches": True,
            "path_recorded": False,
            "pipeline_expected": {
                "full_count": 2,
                "collected_count": 2,
                "inventory_digest": "b" * 64,
                "expected_action_counts": dict(actions),
            },
        },
        "database": {
            "identity": {
                "app_name": tool.APP_NAME,
                "app_name_row_count": 1,
                "app_name_value_valid": True,
                "schema_version": "63",
                "schema_version_row_count": 1,
                "schema_version_value_valid": True,
            },
            "identity_pin": {
                "status": "PASS",
                "method": "WINDOWS_READ_HANDLE_DENY_WRITE_DELETE",
                "held_across_snapshot": True,
                "path_identity_stable": True,
                "raw_identity_recorded": False,
            },
            "connection": {
                "mode": "ro",
                "query_only": True,
                "immutable": True,
                "single_deferred_snapshot": True,
            },
            "database_list": [{"seq": 0, "name": "main", "file_present": True}],
            "quick_check": ["ok"],
            "files_before": {"main": fingerprint},
            "files_after": {"main": fingerprint},
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": probe,
            "writer_probe_after": probe,
            "whole_window_writer_absence_proven": True,
            "runtime_lock_count": 0,
        },
        "schema_manifest": {"ready": True, "invalid_object_count": 0},
        "pipeline_reconciliation": {
            "inventory_count_consistent": True,
            "inventory_digest": "b" * 64,
            "inventory_end_digest": "b" * 64,
            "full_count": 2,
            "collected_count": 2,
            "expected_action_counts": dict(actions),
            "target_count": 2,
            "source_state_counts": source_counts,
            "downstream_state_counts": downstream_counts,
            "outcome_counts": outcome_counts,
            "preview_eligible_count": 1,
            "preview_eligible_action_counts": {
                ACTION_DISPOSE_EXPIRED_PLAN_READY: 1,
                ACTION_DISPOSE_ORPHAN: 0,
            },
            "manual_evidence_required_count": 1,
            "repair_required_count": 0,
            "invalid_item_count": 0,
            "read_only": True,
            "observe_only": True,
            "identifiers_recorded": False,
            "raw_rows_recorded": False,
            "raw_payloads_recorded": False,
            "database_write_performed": False,
            "apply_authorized": False,
            "eligibility_changed": False,
        },
        "contract": tool._CONTRACT,
        "read_only": True,
        "observe_only": True,
        "database_write_performed": False,
        "apply_authorized": False,
        "eligibility_changed": False,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "tool_actions": dict(tool._TOOL_ACTIONS),
        "side_effect_scope": "THIS_TOOL_EXECUTION_ONLY",
        "external_process_state_verified": False,
        "external_broker_state_verified": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


def _safe_downstream(**overrides: int) -> dict[str, int]:
    counts = {key: 0 for key in tool._DOWNSTREAM_COUNT_KEYS}
    counts.update(overrides)
    return counts


def _passing_probe(_path: Path) -> dict:
    return {
        "status": "PASS",
        "method": "TEST",
        "no_other_open_handles": True,
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
