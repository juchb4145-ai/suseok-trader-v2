from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.pipeline_coherency_disposition import (
    ACTION_DISPOSE_ORPHAN,
    preview_pipeline_coherency_disposition,
)
from services.pipeline_orphan_manual_evidence import (
    ORPHAN_EVIDENCE_CONTRACT,
    PRIVATE_TARGET_SET_CONTRACT,
    private_target_set_sha256,
)
from storage.sqlite import initialize_database
from tests.test_ops_pipeline_blocker_reconciliation import _build_plan_report
from tools import ops_pipeline_blocker_reconciliation as reconciliation_tool
from tools import ops_pipeline_orphan_evidence_preflight as tool

_OBSERVED_AT = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)
_REAL_PINNED_DATABASE_IDENTITY = tool.fast0_tool._PinnedDatabaseIdentity


class _CrossPlatformTestPinnedIdentity:
    """Exercise the real pin while projecting the Windows-only deny-write proof."""

    def __init__(self, path: Path) -> None:
        self._inner = _REAL_PINNED_DATABASE_IDENTITY(path)
        self.sqlite_source = str(path)
        self.method = "WINDOWS_READ_HANDLE_DENY_WRITE_DELETE"

    def __enter__(self):
        self._inner.__enter__()
        self.sqlite_source = self._inner.sqlite_source
        return self

    def __exit__(self, exc_type, exc, traceback):
        return self._inner.__exit__(exc_type, exc, traceback)

    def assert_path_identity(self) -> None:
        self._inner.assert_path_identity()

    def fingerprint(self) -> dict:
        return self._inner.fingerprint()

    def public_status(self) -> dict:
        status = dict(self._inner.public_status())
        status["method"] = self.method
        return status


def test_orphan_evidence_preflight_is_strict_read_only_private_and_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)

    report = tool.run_report(
        **case,
        out_dir=tmp_path / "preflight",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["verdict"]["preflight_status"] == "COMPLETE"
    assert report["verdict"]["execution_readiness"] == "PREPARATION_REQUIRED"
    assert report["private_target_manifest"]["target_count"] == 1
    assert report["preview_binding"]["status"] == "ELIGIBLE"
    assert report["preview_binding"]["source_state"] == "CANDIDATE_ABSENT"
    assert report["preview_binding"]["downstream_state"] == "SAFE_DB_PROVEN"
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert report["database_write_performed"] is False
    assert report["apply_authorized"] is False
    assert report["external_broker_artifact_bound"] is True
    assert report["orphan_ledger_audit"]["ready"] is True
    assert report["orphan_ledger_audit"]["row_count"] == 0
    assert "orphan-private-id" not in raw
    assert "broker/order-history/private-ref" not in raw
    assert "reviewer.safe" not in raw
    assert str(tmp_path) not in raw


def test_orphan_evidence_preflight_rejects_manifest_semantic_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    manifest_path = case["private_target_manifest"]
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["items"][0]["alias"] = "M002"
    _write_json(manifest_path, payload)
    case["expected_private_target_manifest_sha256"] = _sha256_file(manifest_path)

    with pytest.raises(
        tool.PipelineOrphanEvidencePreflightError,
        match="TARGET_MANIFEST_ALIAS_SEQUENCE_INVALID",
    ):
        tool.run_report(
            **case,
            out_dir=tmp_path / "invalid-preflight",
            observed_at=_OBSERVED_AT,
        )


def test_orphan_preflight_requires_exact_r9_handoff_before_database_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    case["predecessor_report"] = tmp_path / "missing-r9-handoff.json"
    opened = False

    def forbidden_database_open(*_args, **_kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("database must not open before predecessor validation")

    monkeypatch.setattr(tool.fast0_tool, "_validated_database_path", forbidden_database_open)
    with pytest.raises(tool.PipelineOrphanEvidencePreflightError):
        tool.run_report(
            **case,
            out_dir=tmp_path / "missing-predecessor",
            observed_at=_OBSERVED_AT,
        )
    assert opened is False


def test_orphan_evidence_preflight_blocks_unrelated_legacy_orphan_ledger_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch, legacy_orphan_row=True)

    report = tool.run_report(
        **case,
        out_dir=tmp_path / "legacy-ledger-preflight",
        observed_at=_OBSERVED_AT,
    )

    assert report["orphan_ledger_audit"]["row_count"] == 1
    assert report["orphan_ledger_audit"]["legacy_generic_row_count"] == 1
    assert report["orphan_ledger_audit"]["invalid_row_count"] == 1
    assert report["orphan_ledger_audit"]["invalid_chain_subject_count"] == 1
    assert report["verdict"]["evidence_status"] == "FAIL"
    assert report["verdict"]["preflight_status"] == "BLOCKED"
    assert "ORPHAN_LEDGER_GLOBAL_AUDIT_INVALID" in report["verdict"]["evidence_failures"]


def test_orphan_evidence_preflight_verdict_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    report = tool.run_report(
        **case,
        out_dir=tmp_path / "preflight",
        observed_at=_OBSERVED_AT,
    )

    for mutation in (
        lambda value: value.__setitem__("contract", "fast0-pipeline-orphan-evidence-preflight.v1"),
        lambda value: value.__setitem__("apply_authorized", True),
        lambda value: value.__setitem__("database_write_performed", True),
        lambda value: value.__setitem__("identifiers_recorded", True),
        lambda value: value.__setitem__("external_broker_artifact_bound", False),
        lambda value: value["database"]["identity"].__setitem__("app_name_row_count", True),
        lambda value: value["database"].__setitem__("runtime_lock_count", False),
        lambda value: value["database"]["database_list"][0].__setitem__("seq", False),
        lambda value: value["schema_manifest"].__setitem__("invalid_object_count", False),
        lambda value: value["preview_binding"].__setitem__("cas_exact", False),
        lambda value: value["preview_binding"].__setitem__("reason_count", False),
        lambda value: value["evidence"].__setitem__("target_sha256", "0" * 64),
        lambda value: value["evidence"].__setitem__("coverage_status", "PARTIAL"),
        lambda value: value["evidence"].__setitem__(
            "provenance_source_type", "OPERATING_DATABASE_AUDIT_EXPORT"
        ),
        lambda value: value["tool_actions"].__setitem__("future_write_invoked", False),
        lambda value: value.__setitem__("side_effect_scope", "EXTERNAL_STATE"),
        lambda value: value.__setitem__("external_process_state_verified", True),
        lambda value: value.__setitem__("external_broker_state_verified", True),
        lambda value: value.pop("orphan_ledger_audit"),
        lambda value: value.pop("orphan_campaign_audit"),
        lambda value: value["campaign_predecessor"].__setitem__("validated", False),
    ):
        tampered = copy.deepcopy(report)
        mutation(tampered)
        verdict = tool.evaluate_report(tampered)
        assert verdict["evidence_status"] == "FAIL"
        assert verdict["preflight_status"] == "BLOCKED"


def test_orphan_evidence_preflight_requires_independently_approved_account_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    case["expected_account_scope_sha256"] = "3" * 64

    with pytest.raises(
        tool.PipelineOrphanEvidencePreflightError,
        match="ORPHAN_EVIDENCE_ACCOUNT_SCOPE_SHA256_MISMATCH",
    ):
        tool.run_report(
            **case,
            out_dir=tmp_path / "wrong-account-scope-preflight",
            observed_at=_OBSERVED_AT,
        )


def test_orphan_evidence_preflight_recomputes_plan_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    plan_path = case["blocker_plan_report"]
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["pipeline"]["invalid_item_count"] = 1
    _write_json(plan_path, payload)
    case["expected_blocker_plan_report_sha256"] = _sha256_file(plan_path)

    with pytest.raises(
        tool.PipelineOrphanEvidencePreflightError,
        match="PLAN_REPORT_CONTRACT_INVALID",
    ):
        tool.run_report(
            **case,
            out_dir=tmp_path / "tampered-plan-preflight",
            observed_at=_OBSERVED_AT,
        )


def test_orphan_evidence_preflight_recomputes_reconciliation_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    reconciliation_path = case["reconciliation_report"]
    payload = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    payload["source_blocker_plan"]["database_main_matches"] = False
    _write_json(reconciliation_path, payload)
    case["expected_reconciliation_report_sha256"] = _sha256_file(reconciliation_path)

    with pytest.raises(
        tool.PipelineOrphanEvidencePreflightError,
        match="RECONCILIATION_REPORT_CONTRACT_INVALID",
    ):
        tool.run_report(
            **case,
            out_dir=tmp_path / "tampered-reconciliation-preflight",
            observed_at=_OBSERVED_AT,
        )


def test_orphan_apply_validator_rejects_pre_global_audit_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch=monkeypatch)
    report = tool.run_report(
        **case,
        out_dir=tmp_path / "preflight",
        observed_at=_OBSERVED_AT,
    )
    payload = copy.deepcopy(report)
    payload.pop("report_paths")
    payload.pop("orphan_ledger_audit")

    with pytest.raises(
        tool.PipelineOrphanEvidencePreflightError,
        match="APPROVED_ORPHAN_PREFLIGHT_CONTRACT_INVALID",
    ):
        tool.validate_apply_preflight_report(
            payload,
            report_sha256="a" * 64,
            expected_private_manifest_sha256=case["expected_private_target_manifest_sha256"],
            expected_evidence_sha256=case["expected_evidence_file_sha256"],
            expected_artifact_sha256=case["expected_authoritative_artifact_sha256"],
            expected_artifact_size=case["authoritative_artifact"].stat().st_size,
            expected_target_set_sha256=report["evidence"]["target_set_sha256"],
            expected_alias="M001",
            expected_evidence_preview_sha256=report["evidence"]["evidence_preview_sha256"],
        )


def _case(
    tmp_path: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
    legacy_orphan_row: bool = False,
    target_count: int = 1,
    selected_alias: str = "M001",
) -> dict:
    monkeypatch.setattr(
        tool.fast0_tool,
        "_PinnedDatabaseIdentity",
        _CrossPlatformTestPinnedIdentity,
    )
    db_path = tmp_path / "orphan.sqlite3"
    connection = initialize_database(db_path)
    private_ids = [
        "orphan-private-id" if target_count == 1 else f"orphan-private-id-{ordinal:03d}"
        for ordinal in range(1, target_count + 1)
    ]
    previews = []
    for ordinal, private_id in enumerate(private_ids, start=1):
        connection.execute(
            """
            INSERT INTO risk_observations_latest (
                candidate_instance_id, risk_observation_id,
                strategy_observation_id, trade_date, code, name, evaluated_at,
                overall_status, max_severity, blocked_count, caution_count,
                pass_count, reason_codes_json, config_version, observe_only
            ) VALUES (
                ?, ?, 'missing-strategy',
                '2026-07-15', '005930', 'fixture', '2026-07-15T00:00:00Z',
                'OBSERVE_BLOCK', 'BLOCK', 1, 0, 0, '[]', 'test', 1
            )
            """,
            (private_id, f"orphan-risk-{ordinal:03d}"),
        )
    if legacy_orphan_row:
        _insert_legacy_orphan_row(connection)
    connection.commit()
    for private_id in private_ids:
        previews.append(
            preview_pipeline_coherency_disposition(
                connection,
                trade_date="2026-07-15",
                candidate_instance_id=private_id,
                action=ACTION_DISPOSE_ORPHAN,
                as_of=_OBSERVED_AT,
            )
        )
    connection.close()

    plan_path = _build_plan_report(tmp_path, db_path, monkeypatch=monkeypatch)
    monkeypatch.setattr(
        reconciliation_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    reconciliation = reconciliation_tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        out_dir=tmp_path / "reconciliation",
        observed_at=_OBSERVED_AT,
    )
    reconciliation_path = Path(reconciliation["report_paths"]["raw_json"])

    targets = [
        {
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
        for preview in previews
    ]
    items = [
        {**target, "alias": f"M{ordinal:03d}"} for ordinal, target in enumerate(targets, start=1)
    ]
    target = targets[int(selected_alias[1:]) - 1]
    manifest_path = _write_json(
        tmp_path / "private-manifest.json",
        {"contract": PRIVATE_TARGET_SET_CONTRACT, "items": items},
    )
    plan_payload = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan_payload["pipeline"]["manual_evidence_campaign_sha256"] == (
        private_target_set_sha256(items)
    )
    predecessor_path = _write_json(
        tmp_path / "r9-handoff.json",
        {"contract": "fixture-r9-handoff"},
    )

    def validate_fixture_handoff(_payload, **kwargs):
        return {
            "apply_chain_sha256": "a" * 64,
            "final_database_main": dict(kwargs["expected_source_database_main"]),
            "generated_at": "2026-07-16T01:00:00Z",
        }

    monkeypatch.setattr(
        tool.r9_handoff_tool,
        "validate_handoff_report",
        validate_fixture_handoff,
    )

    artifact_path = tmp_path / "private-broker-order-history.csv"
    artifact_path.write_bytes(b"synthetic,broker,order,history\n")
    evidence_path = _write_json(
        tmp_path / "private-evidence.json",
        {
            "contract": ORPHAN_EVIDENCE_CONTRACT,
            "target_set_sha256": private_target_set_sha256(items),
            "alias": selected_alias,
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
                "source_ref": "broker/order-history/private-ref",
                "artifact_sha256": _sha256_file(artifact_path),
                "artifact_size": artifact_path.stat().st_size,
                "coverage_trade_date": "2026-07-15",
                "coverage_status": "FINAL_COMPLETE",
                "account_scope_sha256": "2" * 64,
                "observed_at": "2026-07-15T15:00:00Z",
                "reviewed_at": "2026-07-15T15:01:00Z",
                "reviewer_id": "reviewer.safe",
            },
        },
    )
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)
    return {
        "db_path": db_path,
        "blocker_plan_report": plan_path,
        "expected_blocker_plan_report_sha256": _sha256_file(plan_path),
        "reconciliation_report": reconciliation_path,
        "expected_reconciliation_report_sha256": _sha256_file(reconciliation_path),
        "private_target_manifest": manifest_path,
        "expected_private_target_manifest_sha256": _sha256_file(manifest_path),
        "evidence_file": evidence_path,
        "expected_evidence_file_sha256": _sha256_file(evidence_path),
        "authoritative_artifact": artifact_path,
        "expected_authoritative_artifact_sha256": _sha256_file(artifact_path),
        "expected_account_scope_sha256": "2" * 64,
        "alias": selected_alias,
        "predecessor_report": predecessor_path,
        "expected_predecessor_report_sha256": _sha256_file(predecessor_path),
    }


def _passing_probe(_path: Path) -> dict:
    return {
        "status": "PASS",
        "method": "TEST",
        "no_other_open_handles": True,
    }


def _insert_legacy_orphan_row(connection) -> None:
    safety = json.dumps(
        {
            "enabled_command_producers": [],
            "explicit_env_file": True,
            "incremental_worker_enabled": False,
            "kill_switch_active": True,
            "live_real_allowed": False,
            "live_sim_allowed": False,
            "not_order_intent": True,
            "order_commands_allowed": False,
            "theme_refresh_queue_market_scan_commands": False,
            "trading_mode": "OBSERVE",
            "trading_profile": "OBSERVE",
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    evidence = json.dumps(
        {
            "content_embedded": False,
            "contract": "fast0-pipeline-disposition-evidence.v1",
            "file_sha256": "3" * 64,
            "file_size": 1,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    connection.execute(
        """
        INSERT INTO pipeline_coherency_dispositions (
            disposition_id, request_id, request_hash, candidate_instance_id,
            subject_key, trade_date, order_plan_id, sequence_no, action,
            supersedes_disposition_id, reason_code, operator_id,
            expected_pipeline_fingerprint, expected_subject_version,
            expected_source_fingerprint, expected_candidate_fingerprint,
            expected_downstream_fingerprint, expected_boundary_fingerprint,
            evidence_type, evidence_ref, evidence_sha256, evidence_json,
            safety_snapshot_json, created_at, observe_only, live_sim_allowed,
            live_real_allowed, order_commands_allowed, not_order_intent,
            no_order_side_effects, auto_run_evaluation
        ) VALUES (
            'legacy-orphan-disposition', 'legacy-orphan-request', ?,
            'unrelated-private-orphan', '2026-07-14:unrelated-private-orphan',
            '2026-07-14', NULL, 1, ?, NULL, 'LEGACY_FIXTURE', 'fixture.operator',
            ?, ?, ?, ?, ?, ?, 'LEGACY_GENERIC', 'fixture/legacy', ?, ?, ?,
            '2026-07-16T00:00:00Z', 1, 0, 0, 0, 1, 1, 0
        )
        """,
        (
            "4" * 64,
            ACTION_DISPOSE_ORPHAN,
            "5" * 64,
            "6" * 64,
            "7" * 64,
            "8" * 64,
            "9" * 64,
            "a" * 64,
            "3" * 64,
            evidence,
            safety,
        ),
    )


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
