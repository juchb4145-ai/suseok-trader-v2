from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.pipeline_coherency import build_pipeline_coherency_rca_status
from services.pipeline_coherency_disposition import (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    resolve_pipeline_coherency_dispositions,
)
from services.pipeline_legacy_evidence import (
    MANIFEST_CONTRACT,
    _current_subject_evidence,
    _sha256_json,
    legacy_target_set_sha256,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (
    build_incremental_evaluation_dead_letter_effective_status,
)
from storage.sqlite import initialize_database
from tests.test_pipeline_coherency_disposition import _expired_closed_pipeline
from tests.test_pipeline_coherency_rca import (
    TRADE_DATE,
    _insert_candidate,
    _insert_pipeline_observations,
)
from tests.test_resolve_incremental_evaluation_dead_letter_tool import (
    _seed_closed_dead_letter,
)
from tools import ops_fast0_blocker_resolution_plan as tool

_OBSERVED_AT = datetime(2026, 7, 16, 1, 0, tzinfo=UTC)


def test_incremental_plan_is_aggregate_only_and_preserves_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "incremental.sqlite3"
    seeded = _seed_closed_dead_letter(db_path)
    baseline = _write_fast0_baseline(tmp_path, db_path)
    malformed_baseline = json.loads(baseline.read_text(encoding="utf-8"))
    malformed_baseline["incremental_evaluation"]["raw_dead_letter_status_count"] = True
    with pytest.raises(
        tool.Fast0BlockerResolutionPlanError,
        match="FAST0_REPORT_CONTRACT_INVALID",
    ):
        tool._validate_fast0_baseline(malformed_baseline, report_sha256="a" * 64)
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.run_report(
        db_path=db_path,
        fast0_report=baseline,
        expected_fast0_report_sha256=_sha256_file(baseline),
        out_dir=tmp_path / "evidence",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    incremental = report["incremental_dead_letter"]
    assert report["verdict"]["evidence_status"] == "PASS"
    assert incremental["disposition_target_count"] == 1
    assert incremental["preview_eligible_count"] == 1
    assert incremental["preview_blocked_count"] == 0
    assert incremental["campaign_alias_contract"]["first_alias"] == "U01"
    assert incremental["campaign_alias_contract"]["last_alias"] == "U01"
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert report["database_write_performed"] is False
    assert report["apply_authorized"] is False
    assert report["tool_actions"] == {
        "core_start_invoked": False,
        "gateway_start_invoked": False,
        "worker_start_invoked": False,
        "order_operation_invoked": False,
        "broker_operation_invoked": False,
    }
    assert report["side_effect_scope"] == "THIS_TOOL_EXECUTION_ONLY"
    assert report["external_process_state_verified"] is False
    assert report["external_broker_state_verified"] is False
    assert str(seeded["dead_letter_id"]) not in raw
    assert str(seeded["candidate_instance_id"]) not in raw
    assert '"dead_letter_id"' not in raw
    assert '"candidate_instance_id"' not in raw
    assert str(tmp_path) not in raw

    blocked = copy.deepcopy(report)
    blocked["database"]["writer_probe_after"] = {
        "status": "BLOCKED",
        "no_other_open_handles": False,
    }
    verdict = tool.evaluate_report(blocked)
    assert verdict["evidence_status"] == "FAIL"
    assert "OTHER_DATABASE_OPEN_HANDLE_PRESENT" in verdict["evidence_failures"]

    quiescence_pending = copy.deepcopy(report)
    quiescence_pending["database"]["whole_window_writer_absence_proven"] = False
    verdict = tool.evaluate_report(quiescence_pending)
    assert "PRE_APPLY_WHOLE_WINDOW_WRITER_QUIESCENCE_REQUIRED" in verdict[
        "preparation_requirements"
    ]


def test_expired_pipeline_plan_separates_applyable_disposition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "pipeline.sqlite3"
    connection, candidate_id, _, _ = _expired_closed_pipeline(db_path)
    connection.close()
    baseline = _write_fast0_baseline(tmp_path, db_path)
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.run_report(
        db_path=db_path,
        fast0_report=baseline,
        expected_fast0_report_sha256=_sha256_file(baseline),
        out_dir=tmp_path / "evidence",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    pipeline = report["pipeline"]
    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["verdict"]["execution_readiness"] == "PREPARATION_REQUIRED"
    assert "PRE_APPLY_BYTE_IDENTICAL_BACKUP_REQUIRED" in report["verdict"][
        "preparation_requirements"
    ]
    assert pipeline["disposition_required_count"] == 1
    assert pipeline["expected_action_counts"][ACTION_DISPOSE_EXPIRED_PLAN_READY] == 1
    assert pipeline["preview_eligible_count"] == 1
    assert pipeline["automatic_historical_disposition_count"] == 1
    assert pipeline["legacy_evidence_target_count"] == 0
    assert pipeline["manual_evidence_required_count"] == 0
    assert pipeline["active_current_target_count"] == 0
    assert candidate_id not in raw

    baseline_drift = copy.deepcopy(report)
    baseline_drift["source_fast0_report"]["pipeline_expected"][
        "disposition_required_count"
    ] = 2
    verdict = tool.evaluate_report(baseline_drift)
    assert verdict["evidence_status"] == "FAIL"
    assert "PIPELINE_INVENTORY_BASELINE_MISMATCH" in verdict["evidence_failures"]


def test_legacy_manifest_qualifies_exact_targets_without_recording_identifiers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "legacy-plan.sqlite3"
    connection = initialize_database(db_path)
    _insert_candidate(
        connection,
        "private-legacy-subject",
        state="CLOSED",
        closed_at="2026-07-10T01:00:00Z",
    )
    _insert_pipeline_observations(connection, "private-legacy-subject")
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        ("private-legacy-subject",),
    )
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active, payload_json
        ) VALUES ('private-event-inactive', 'private-legacy-subject', ?, '005930', 'fixture',
                  'TEST', 'private-source-1', 'CLOSED', '2026-07-10T00:00:00Z',
                  '2026-07-10T00:00:00Z', 0, '{}')
        """,
        (TRADE_DATE,),
    )
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date, code, source_type, source_id, candidate_instance_id,
            name, active, first_seen_at, last_seen_at, last_event_id, payload_json
        ) VALUES (?, '005930', 'TEST', 'private-source-1', 'private-legacy-subject',
                  'fixture', 0, '2026-07-10T00:00:00Z', '2026-07-10T00:00:00Z',
                  'private-event-inactive', '{}')
        """,
        (TRADE_DATE,),
    )
    connection.commit()
    item = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="private-legacy-subject",
        as_of=_OBSERVED_AT,
    )["items"][0]
    cutover = {
        "completed_at": "2026-07-11T00:00:00Z",
        "migration_report_sha256": "1" * 64,
        "source_database_sha256": "2" * 64,
        "migrated_database_sha256": "3" * 64,
    }
    evidence = _current_subject_evidence(
        connection,
        candidate_instance_id="private-legacy-subject",
        trade_date=TRADE_DATE,
        cutoff=datetime(2026, 7, 11, tzinfo=UTC),
    )
    approved = {
        "alias": "L001",
        "candidate_instance_id": "private-legacy-subject",
        "pipeline_fingerprint": item["pipeline_fingerprint"],
        "subject_version": item["subject_version"],
        "source_fingerprint": evidence["source_fingerprint"],
        "closure_fingerprint": evidence["closure_fingerprint"],
        "pipeline_stage_fingerprint": evidence["pipeline_stage_fingerprint"],
        "provenance_sha256": _sha256_json(cutover),
    }
    manifest_payload = {
        "contract": MANIFEST_CONTRACT,
        "trade_date": TRADE_DATE,
        "target_set_sha256": legacy_target_set_sha256([approved]),
        "schema59_cutover": cutover,
        "items": [approved],
    }
    manifest_path = tmp_path / "private-legacy-manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")
    connection.close()
    baseline = _write_fast0_baseline(tmp_path, db_path)
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.run_report(
        db_path=db_path,
        fast0_report=baseline,
        expected_fast0_report_sha256=_sha256_file(baseline),
        legacy_evidence_manifest=manifest_path,
        expected_legacy_evidence_manifest_sha256=_sha256_file(manifest_path),
        out_dir=tmp_path / "evidence",
        observed_at=_OBSERVED_AT,
    )
    raw = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["pipeline"]["legacy_evidence_target_count"] == 1
    assert report["pipeline"]["legacy_evidence_eligible_count"] == 1
    assert report["pipeline"]["legacy_evidence_contract_blocked_count"] == 0
    assert report["execution_phases"][3]["status"] == "COMPLETE"
    assert report["legacy_evidence_input"]["configured"] is True
    assert "private-legacy-subject" not in raw
    assert str(tmp_path) not in raw


def test_pipeline_pagination_fails_closed_on_inventory_contract_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = iter(
        [
            _pipeline_page(offset=0, digest="a" * 64, has_more=True),
            _pipeline_page(offset=1, digest="b" * 64, has_more=False),
        ]
    )
    monkeypatch.setattr(
        tool,
        "build_pipeline_coherency_rca_status",
        lambda *_args, **_kwargs: next(pages),
    )
    connection = sqlite3.connect(":memory:")

    with pytest.raises(
        tool.Fast0BlockerResolutionPlanError,
        match="PIPELINE_PAGE_CONTRACT_DRIFT",
    ):
        tool._read_pipeline_pages(connection, observed_at=_OBSERVED_AT)
    connection.close()


def test_pipeline_pagination_fails_closed_on_duplicate_subject(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = iter(
        [
            _pipeline_page(
                offset=0,
                digest="a" * 64,
                has_more=True,
                candidate_id="candidate-duplicate",
            ),
            _pipeline_page(
                offset=1,
                digest="a" * 64,
                has_more=False,
                candidate_id="candidate-duplicate",
            ),
        ]
    )
    monkeypatch.setattr(
        tool,
        "build_pipeline_coherency_rca_status",
        lambda *_args, **_kwargs: next(pages),
    )
    connection = sqlite3.connect(":memory:")

    with pytest.raises(
        tool.Fast0BlockerResolutionPlanError,
        match="PIPELINE_PAGINATION_DUPLICATE_SUBJECT",
    ):
        tool._read_pipeline_pages(connection, observed_at=_OBSERVED_AT)
    connection.close()


def test_pipeline_pagination_fails_closed_when_page_items_do_not_match_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = iter(
        [
            _pipeline_page(offset=0, digest="a" * 64, has_more=True),
            _pipeline_page(offset=1, digest="a" * 64, has_more=False),
        ]
    )
    monkeypatch.setattr(
        tool,
        "build_pipeline_coherency_rca_status",
        lambda *_args, **_kwargs: next(pages),
    )
    connection = sqlite3.connect(":memory:")

    with pytest.raises(
        tool.Fast0BlockerResolutionPlanError,
        match="PIPELINE_PAGINATION_INVENTORY_MISMATCH",
    ):
        tool._read_pipeline_pages(connection, observed_at=_OBSERVED_AT)
    connection.close()


def test_incremental_campaign_excludes_unsafe_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "unsafe-preview.sqlite3"
    _seed_closed_dead_letter(db_path)
    baseline = _write_fast0_baseline(tmp_path, db_path)
    real_preview = tool.preview_incremental_evaluation_dead_letter_disposition

    def unsafe_preview(*args, **kwargs):
        preview = dict(real_preview(*args, **kwargs))
        preview["live_sim_allowed"] = True
        return preview

    monkeypatch.setattr(
        tool,
        "preview_incremental_evaluation_dead_letter_disposition",
        unsafe_preview,
    )
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.run_report(
        db_path=db_path,
        fast0_report=baseline,
        expected_fast0_report_sha256=_sha256_file(baseline),
        out_dir=tmp_path / "evidence",
        observed_at=_OBSERVED_AT,
    )

    incremental = report["incremental_dead_letter"]
    assert incremental["disposition_target_count"] == 1
    assert incremental["preview_eligible_count"] == 0
    assert incremental["preview_blocked_count"] == 1
    assert incremental["preview_blocked_reason_counts"] == {
        "INCREMENTAL_PREVIEW_SAFETY_CONTRACT_INVALID": 1,
    }


@pytest.mark.parametrize(
    ("classification", "latest_plan", "expected"),
    [
        (
            "HISTORICAL_CLOSED",
            {"status": "PLAN_READY", "unexpired": False},
            ACTION_DISPOSE_EXPIRED_PLAN_READY,
        ),
        ("HISTORICAL_CLOSED", {"status": "DATA_WAIT", "unexpired": False}, None),
        ("MISSING_CANDIDATE_MANUAL_REVIEW", {}, ACTION_DISPOSE_ORPHAN),
        ("STALE_OTHER_DATE_MANUAL_REVIEW", {}, ACTION_DISPOSE_STALE_OTHER_DATE),
        ("ACTIVE_CURRENT", {"status": "PLAN_READY", "unexpired": False}, None),
    ],
)
def test_expected_pipeline_action_routes_only_supported_subjects(
    classification: str,
    latest_plan: dict,
    expected: str | None,
) -> None:
    assert (
        tool._expected_pipeline_action(
            {"classification": classification, "latest_plan": latest_plan}
        )
        == expected
    )


def test_pipeline_preview_requires_exact_cas_match() -> None:
    expected = {
        "candidate_instance_id": "candidate-alpha",
        "trade_date": "2026-07-15",
        "action": ACTION_DISPOSE_EXPIRED_PLAN_READY,
        **{key: "a" * 64 for key in tool._PIPELINE_CAS_KEYS},
    }
    preview = {
        **expected,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "auto_run_evaluation": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "chain_valid": True,
    }
    preview["source_fingerprint"] = "b" * 64

    assert tool._pipeline_preview_safe(preview, expected=expected) is False


def test_pipeline_plan_rejects_item_outside_fixed_trade_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = {
        "trade_date": "2026-07-15",
        "full_count": 1,
        "canonical_status": "FAIL",
        "qualification_status": "BLOCKED",
        "qualification_reason_codes": [],
        "inventory_digest": "a" * 64,
        "inventory_end_digest": "a" * 64,
        "inventory_count_consistent": True,
        "schema_ready": True,
    }
    item = {
        "candidate_instance_id": "candidate-alpha",
        "trade_date": "2026-07-14",
        "classification": "HISTORICAL_CLOSED",
        "pipeline_fingerprint": "b" * 64,
        "subject_version": "c" * 64,
    }
    monkeypatch.setattr(
        tool,
        "_read_pipeline_pages",
        lambda *_args, **_kwargs: (first, [item], []),
    )
    connection = sqlite3.connect(":memory:")

    result = tool._plan_pipeline(connection, observed_at=_OBSERVED_AT)
    connection.close()

    assert result["invalid_item_count"] == 1
    assert result["classification_counts"]["HISTORICAL_CLOSED"] == 0


def test_aliases_are_target_ordered_and_cannot_be_overridden() -> None:
    result = tool._with_aliases(
        [
            {"dead_letter_id": "b", "alias": "INJECTED"},
            {"dead_letter_id": "a", "alias": "INJECTED"},
        ],
        prefix="U",
        width=2,
        ordering_key="dead_letter_id",
    )

    assert [item["dead_letter_id"] for item in result] == ["a", "b"]
    assert [item["alias"] for item in result] == ["U01", "U02"]


def test_cli_does_not_echo_sensitive_exception_detail(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "ghp_" + "a" * 36

    def raise_sensitive(**_kwargs):
        raise RuntimeError(f"token={token}")

    monkeypatch.setattr(tool, "run_report", raise_sensitive)
    monkeypatch.setattr(
        tool.sys,
        "argv",
        [
            "ops_fast0_blocker_resolution_plan",
            "--db",
            "missing.sqlite3",
            "--fast0-report",
            "missing.json",
            "--fast0-report-sha256",
            "a" * 64,
        ],
    )

    assert tool.main() == 2
    captured = capsys.readouterr()
    assert "error_type=RuntimeError" in captured.err
    assert token not in captured.err


@pytest.mark.parametrize(
    ("report", "error"),
    [
        ({"dead_letter_id": "raw-id-must-not-be-written"}, "FORBIDDEN_FIELD"),
        ({"dead_letter_ids": ["raw-id-must-not-be-written"]}, "FORBIDDEN_FIELD"),
        ({"note": r"\\server\share\private.json"}, "ABSOLUTE_PATH"),
        ({"note": "//server/share/private.json"}, "ABSOLUTE_PATH"),
    ],
)
def test_report_writer_rejects_identifier_and_absolute_path_before_output(
    tmp_path: Path,
    report: dict,
    error: str,
) -> None:
    out_dir = tmp_path / "evidence"

    with pytest.raises(
        tool.Fast0BlockerResolutionPlanError,
        match=f"REPORT_CONTAINS_{error}",
    ):
        tool.write_report(report, out_dir=out_dir)

    assert out_dir.exists() is False


def _write_fast0_baseline(tmp_path: Path, db_path: Path) -> Path:
    connection = tool.fast0_tool._open_strict_read_only(db_path)
    incremental = build_incremental_evaluation_dead_letter_effective_status(connection)
    pipeline = build_pipeline_coherency_rca_status(
        connection,
        max_age_sec=tool._PIPELINE_MAX_AGE_SEC,
        limit=tool._PAGE_LIMIT,
        disposition_resolver=lambda trade_date, subjects: (
            resolve_pipeline_coherency_dispositions(
                connection,
                trade_date,
                subjects,
                as_of=_OBSERVED_AT,
            )
        ),
        as_of=_OBSERVED_AT,
    )
    active_current_non_pass_count = sum(
        item.get("active_recovery_required") is True for item in pipeline["items"]
    )
    connection.close()
    main = tool.fast0_tool._file_fingerprints(db_path)["main"]
    bucket_counts = {
        output_key: int(incremental[output_key])
        for output_key in tool._INCREMENTAL_BUCKET_OUTPUT_KEYS.values()
    }
    classification_counts = {
        key: int(pipeline["classification_counts"].get(key, 0))
        for key in tool._PIPELINE_CLASSIFICATIONS
    }
    payload = {
        "contract": tool._EXPECTED_FAST0_CONTRACT,
        "verdict": {"evidence_status": "PASS", "fast0_status": "BLOCKED"},
        "read_only": True,
        "identifiers_recorded": False,
        "database": {
            "identity": {
                "schema_version": tool._EXPECTED_SCHEMA_VERSION,
                "schema_version_value_valid": True,
            },
            "identity_pin": {"status": "PASS"},
            "files_after": {"main": main},
        },
        "incremental_evaluation": {
            "bucket_conservation_valid": True,
            "raw_dead_letter_status_count": incremental["raw_dead_letter_count"],
            "effective_dead_letter_count": incremental["effective_dead_letter_count"],
            "bucket_counts": bucket_counts,
        },
        "pipeline_coherency": {
            "full_count": pipeline["full_count"],
            "inventory_digest": pipeline["inventory_digest"],
            "classification_counts": classification_counts,
            "disposition_required_count": pipeline["disposition_required_count"],
            "independent_counts": {
                "active_current_non_pass_count": active_current_non_pass_count,
            },
        },
    }
    path = tmp_path / f"{db_path.stem}-fast0.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _pipeline_page(
    *,
    offset: int,
    digest: str,
    has_more: bool,
    candidate_id: str | None = None,
) -> dict:
    return {
        "trade_date": "2026-07-15",
        "full_count": 2,
        "inventory_digest": digest,
        "inventory_end_digest": digest,
        "inventory_count_consistent": True,
        "items": [{"candidate_instance_id": candidate_id or f"candidate-{offset}"}],
        "offset": offset,
        "returned_count": 1,
        "has_more": has_more,
        "next_offset": offset + 1 if has_more else None,
    }


def _passing_probe(_path: Path) -> dict:
    return {
        "status": "PASS",
        "method": "TEST",
        "no_other_open_handles": True,
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
