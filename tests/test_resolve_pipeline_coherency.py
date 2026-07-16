from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.pipeline_orphan_campaign import (
    PipelineOrphanCampaignError,
    validate_orphan_campaign_apply_report,
)
from storage.sqlite import initialize_database
from tests.test_ops_pipeline_orphan_evidence_preflight import (
    _OBSERVED_AT as _ORPHAN_OBSERVED_AT,
)
from tests.test_ops_pipeline_orphan_evidence_preflight import (
    _case as _build_orphan_preflight_case,
)
from tests.test_pipeline_coherency_disposition import _expired_closed_pipeline
from tools import resolve_pipeline_coherency as tool


def test_pipeline_disposition_cli_preview_is_strict_read_only(tmp_path) -> None:
    db_path = tmp_path / "preview.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    connection.close()

    report = tool.preview_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        out_dir=tmp_path / "preview-evidence",
    )

    assert report["verdict"]["status"] == "ELIGIBLE"
    assert report["verdict"]["database_files_unchanged"] is True
    assert report["preview"]["eligible"] is True
    assert report["read_only"] is True
    assert report["no_order_side_effects"] is True


def test_pipeline_disposition_cli_orphan_preview_never_publishes_private_identifier(
    tmp_path,
    monkeypatch,
) -> None:
    case = _build_orphan_preflight_case(tmp_path, monkeypatch=monkeypatch)
    private_candidate = "orphan-private-id"

    report = tool.preview_disposition(
        db_path=case["db_path"],
        trade_date="2026-07-15",
        candidate_instance_id=private_candidate,
        action=tool.ACTION_DISPOSE_ORPHAN,
        out_dir=tmp_path / "orphan-private-preview",
    )

    preview = report["preview"]
    artifacts = "\n".join(
        Path(path).read_text(encoding="utf-8") for path in report["report_paths"].values()
    )
    assert (
        preview["candidate_instance_id_sha256"]
        == hashlib.sha256(private_candidate.encode("utf-8")).hexdigest()
    )
    assert "candidate_instance_id" not in preview
    assert "subject_key" not in preview
    assert private_candidate not in artifacts
    assert f"2026-07-15:{private_candidate}" not in artifacts
    assert str(tmp_path) not in artifacts


def test_pipeline_disposition_cli_apply_is_one_row_and_idempotent(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "apply.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    direct_preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    entry_count_before = connection.execute(
        "SELECT COUNT(*) FROM entry_timing_evaluations"
    ).fetchone()[0]
    command_count_before = connection.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()[0]
    connection.close()

    safe_env = tmp_path / "pipeline-safe.env"
    producer_lines = [f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES]
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *producer_lines,
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    kwargs = {
        "db_path": db_path,
        "trade_date": trade_date,
        "candidate_instance_id": candidate_id,
        "action": tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        "request_id": "fast0r3.pipeline.request-one",
        "expected_pipeline_fingerprint": direct_preview["pipeline_fingerprint"],
        "expected_subject_version": direct_preview["subject_version"],
        "expected_source_fingerprint": direct_preview["source_fingerprint"],
        "expected_candidate_fingerprint": direct_preview["candidate_fingerprint"],
        "expected_downstream_fingerprint": direct_preview["downstream_fingerprint"],
        "expected_boundary_fingerprint": direct_preview["boundary_fingerprint"],
        "reason_code": "FAST0R3_EVIDENCED_HISTORICAL_PIPELINE",
        "operator_id": "fast0.operator",
        "evidence_type": "FAST0R3_PIPELINE_RCA",
        "evidence_ref": "fast0r3/pipeline-rca-redacted",
        "evidence_file": evidence_file,
        "acknowledge": tool.APPLY_ACK,
    }
    first = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-one",
    )
    second = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-two",
    )

    write_report = tool._write_report

    def fail_evidence_write(*_args, **_kwargs):
        raise OSError("injected evidence volume failure")

    monkeypatch.setattr(tool, "_write_report", fail_evidence_write)
    evidence_failed = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-failure",
    )

    check = tool._open_strict_read_only(db_path)
    try:
        ledger_count = check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions"
        ).fetchone()[0]
        entry_count_after = check.execute(
            "SELECT COUNT(*) FROM entry_timing_evaluations"
        ).fetchone()[0]
        command_count_after = check.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()[0]
        safety_json = check.execute(
            "SELECT safety_snapshot_json FROM pipeline_coherency_dispositions"
        ).fetchone()[0]
    finally:
        check.close()

    assert first["verdict"]["status"] == "APPLIED"
    assert second["verdict"]["status"] == "IDEMPOTENT"
    assert evidence_failed["verdict"] == {
        "status": "COMMITTED_EVIDENCE_WRITE_FAILED",
        "failures": ["EVIDENCE_WRITE_FAILED"],
        "committed": True,
        "evidence_written": False,
        "operator_action_required": True,
        "retry_with_same_request_id": "fast0r3.pipeline.request-one",
        "error_type": "OSError",
    }
    assert ledger_count == 1
    assert entry_count_after == entry_count_before
    assert command_count_after == command_count_before
    assert hashlib.sha256(safe_env.read_bytes()).hexdigest() in safety_json

    monkeypatch.setattr(tool, "_write_report", write_report)
    write = tool._open_existing_read_write(db_path)
    plan = write.execute(
        "SELECT * FROM order_plan_drafts WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    write.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id, candidate_instance_id,
            strategy_observation_id, risk_observation_id, order_plan_id,
            trade_date, account_id, code, name, side, order_type, quantity,
            limit_price, notional, status, idempotency_key,
            live_sim_only, live_real_allowed, broker_order_sent,
            created_at, expires_at
        ) VALUES (
            'late-cli-intent', ?, ?, ?, ?, ?, 'SIM_REDACTED', ?, ?, 'BUY',
            'LIMIT', 1, 1, 1, 'CREATED', 'late-cli-intent-key', 1, 0, 0,
            '2026-07-15T00:00:00Z', '2026-07-15T00:10:00Z'
        )
        """,
        (
            candidate_id,
            plan["strategy_observation_id"],
            plan["risk_observation_id"],
            plan["order_plan_id"],
            trade_date,
            plan["code"],
            plan["name"],
        ),
    )
    write.close()

    with pytest.raises(tool.PipelineCoherencyDispositionError) as exc_info:
        tool.apply_disposition(
            **kwargs,
            out_dir=tmp_path / "apply-evidence-stale-idempotent",
        )
    assert exc_info.value.code == "PIPELINE_DISPOSITION_IDEMPOTENT_STALE"


def test_pipeline_disposition_cli_orphan_rejects_legacy_nonempty_evidence_before_rw_open(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, evidence_file = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-invalid",
    )
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    kwargs["expected_evidence_sha256"] = hashlib.sha256(evidence_file.read_bytes()).hexdigest()
    opened = False

    def fail_if_opened(_path):
        nonlocal opened
        opened = True
        raise AssertionError("RW connection must not open for invalid orphan evidence")

    monkeypatch.setattr(tool, "_open_existing_read_write", fail_if_opened)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="APPROVED_ORPHAN_PREFLIGHT_CONTRACT_INVALID",
    ):
        tool.apply_disposition(
            **kwargs,
            out_dir=tmp_path / "orphan-invalid-report",
        )

    assert opened is False
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_requires_approved_preflight_inputs(
    tmp_path,
    monkeypatch,
) -> None:
    _db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-missing-preflight",
    )
    kwargs["orphan_preflight_report"] = None

    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="APPLY_ARGUMENT_REQUIRED:ORPHAN_PREFLIGHT_REPORT",
    ):
        tool.apply_disposition(**kwargs, out_dir=tmp_path / "missing-preflight-report")


def test_pipeline_disposition_cli_orphan_apply_binds_evidence_and_is_idempotent(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-valid",
    )

    first = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "orphan-first-report",
    )
    second = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "orphan-second-report",
    )

    check = tool._open_strict_read_only(db_path)
    try:
        row = check.execute("SELECT evidence_json FROM pipeline_coherency_dispositions").fetchone()
        row_count = check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions"
        ).fetchone()[0]
    finally:
        check.close()
    stored = json.loads(row["evidence_json"])
    serialized = json.dumps(stored, sort_keys=True)

    assert first["verdict"]["status"] == "APPLIED"
    assert second["verdict"]["status"] == "IDEMPOTENT"
    assert row_count == 1
    assert stored["contract"] == "fast0-pipeline-orphan-apply-binding.v2"
    assert stored["campaign"]["predecessor_kind"] == ("INCREMENTAL_DEAD_LETTER_CAMPAIGN_HANDOFF")
    assert first["campaign"]["chain_sha256"] == second["campaign"]["chain_sha256"]
    assert first["database"]["checkpoint_complete"] is True
    assert len(first["database"]["main_after"]["sha256"]) == 64
    first_raw = Path(first["report_paths"]["raw_json"])
    validate_orphan_campaign_apply_report(
        first,
        report_sha256=hashlib.sha256(first_raw.read_bytes()).hexdigest(),
        expected_alias="M001",
        source_plan_report_sha256=first["campaign"]["source_plan_report_sha256"],
        source_reconciliation_report_sha256=first["campaign"][
            "source_reconciliation_report_sha256"
        ],
        target_set_sha256=first["campaign"]["target_set_sha256"],
    )
    assert stored["evidence_binding"]["contract"] == ("fast0-pipeline-orphan-evidence-binding.v1")
    assert stored["evidence_binding"]["determination_status"] == "AUTHORITATIVE"
    assert stored["evidence_binding"]["content_embedded"] is False
    assert (
        stored["approval"]["approved_preflight_report_sha256"]
        == kwargs["expected_orphan_preflight_report_sha256"]
    )
    assert len(stored["approval_binding_sha256"]) == 64
    assert "orphan-private-id" not in serialized
    assert "broker/order-history/private-ref" not in serialized
    assert "reviewer.safe" not in serialized
    public_artifacts = "\n".join(
        Path(report["report_paths"][key]).read_text(encoding="utf-8")
        for report in (first, second)
        for key in ("raw_json", "summary_md", "commands_txt")
    )
    for private_value in (
        "orphan-private-id",
        "fast0r8.pipeline.orphan-valid",
        "fast0.operator",
        "fast0r8/orphan-evidence-M001",
        "broker/order-history/private-ref",
        "reviewer.safe",
        str(tmp_path),
    ):
        assert private_value not in public_artifacts


def test_orphan_campaign_m001_to_m002_requires_exact_predecessor_chain(
    tmp_path,
    monkeypatch,
) -> None:
    case = _build_orphan_preflight_case(
        tmp_path,
        monkeypatch=monkeypatch,
        target_count=2,
        selected_alias="M001",
    )
    first_preflight = tool.orphan_preflight_tool.run_report(
        **case,
        out_dir=tmp_path / "m001-preflight",
        observed_at=_ORPHAN_OBSERVED_AT,
    )
    _, first_kwargs, _ = _orphan_apply_kwargs(
        tmp_path,
        monkeypatch,
        request_id="fast0r10.pipeline.m001",
        preflight_case=case,
        preflight=first_preflight,
    )
    first_apply = tool.apply_disposition(
        **first_kwargs,
        out_dir=tmp_path / "m001-apply",
    )
    first_apply_path = Path(first_apply["report_paths"]["raw_json"])

    manifest = json.loads(case["private_target_manifest"].read_text(encoding="utf-8"))
    second_item = dict(manifest["items"][1])
    second_target = dict(second_item)
    second_target.pop("alias")
    second_evidence = json.loads(case["evidence_file"].read_text(encoding="utf-8"))
    second_evidence["alias"] = "M002"
    second_evidence["target"] = second_target
    second_evidence_path = tmp_path / "m002-private-evidence.json"
    second_evidence_path.write_text(
        json.dumps(second_evidence, sort_keys=True),
        encoding="utf-8",
    )
    second_case = {
        **case,
        "alias": "M002",
        "evidence_file": second_evidence_path,
        "expected_evidence_file_sha256": hashlib.sha256(
            second_evidence_path.read_bytes()
        ).hexdigest(),
        "predecessor_report": first_apply_path,
        "expected_predecessor_report_sha256": hashlib.sha256(
            first_apply_path.read_bytes()
        ).hexdigest(),
    }
    observed_at = datetime.now(UTC)

    skipped = {**second_case, "predecessor_report": case["predecessor_report"]}
    skipped["expected_predecessor_report_sha256"] = hashlib.sha256(
        skipped["predecessor_report"].read_bytes()
    ).hexdigest()
    with pytest.raises(tool.orphan_preflight_tool.PipelineOrphanEvidencePreflightError):
        tool.orphan_preflight_tool.run_report(
            **skipped,
            out_dir=tmp_path / "m002-skipped-predecessor",
            observed_at=observed_at,
        )

    second_preflight = tool.orphan_preflight_tool.run_report(
        **second_case,
        out_dir=tmp_path / "m002-preflight",
        observed_at=observed_at,
    )
    assert second_preflight["verdict"]["evidence_status"] == "PASS"
    assert second_preflight["campaign_predecessor"]["kind"] == ("PIPELINE_ORPHAN_APPLY_REPORT")
    assert second_preflight["orphan_campaign_audit"]["ledger_row_count"] == 1
    assert second_preflight["orphan_campaign_audit"]["chain_matches_predecessor"] is True

    _, second_kwargs, _ = _orphan_apply_kwargs(
        tmp_path,
        monkeypatch,
        request_id="fast0r10.pipeline.m002",
        preflight_case=second_case,
        preflight=second_preflight,
    )
    second_apply = tool.apply_disposition(
        **second_kwargs,
        out_dir=tmp_path / "m002-apply",
    )
    assert second_apply["verdict"]["status"] == "APPLIED"
    assert (
        second_apply["campaign"]["link"]["predecessor_report_sha256"]
        == (second_case["expected_predecessor_report_sha256"])
    )
    assert (
        second_apply["campaign"]["link"]["predecessor_chain_sha256"]
        == (first_apply["campaign"]["chain_sha256"])
    )


@pytest.mark.parametrize("private_evidence_ref", ["fast0", "M001"])
def test_pipeline_disposition_cli_orphan_public_value_collision_is_not_a_privacy_failure(
    tmp_path,
    monkeypatch,
    private_evidence_ref: str,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id=f"fast0r8.pipeline.collision-{private_evidence_ref.lower()}",
    )
    kwargs["evidence_ref"] = private_evidence_ref

    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / f"collision-{private_evidence_ref.lower()}",
    )

    assert report["verdict"]["status"] == "APPLIED"
    assert report["verdict"]["committed"] is True
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 1
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_postcommit_privacy_failure_returns_safe_report(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.postcommit-privacy-failure",
    )

    def fail_privacy_assertion(*_args, **_kwargs) -> None:
        raise tool.PipelineDispositionCliError("INJECTED_PRIVACY_FAILURE")

    monkeypatch.setattr(tool, "_assert_orphan_apply_report_private", fail_privacy_assertion)
    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "postcommit-privacy-failure",
    )

    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert "ORPHAN_REPORT_PRIVACY_ASSERTION_FAILED" in report["verdict"]["failures"]
    assert report["report_paths"]
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 1
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_checkpoint_failure_blocks_next_predecessor(
    tmp_path,
    monkeypatch,
) -> None:
    _, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r10.pipeline.checkpoint-failure",
    )

    def fail_checkpoint(_path) -> None:
        raise sqlite3.OperationalError("checkpoint-fault")

    monkeypatch.setattr(tool, "_checkpoint_committed_database", fail_checkpoint)
    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "checkpoint-failure-report",
    )

    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert report["database"]["checkpoint_complete"] is False
    with pytest.raises(PipelineOrphanCampaignError):
        validate_orphan_campaign_apply_report(
            report,
            report_sha256="f" * 64,
            expected_alias="M001",
            source_plan_report_sha256=report["campaign"]["source_plan_report_sha256"],
            source_reconciliation_report_sha256=report["campaign"][
                "source_reconciliation_report_sha256"
            ],
            target_set_sha256=report["campaign"]["target_set_sha256"],
        )


def test_pipeline_disposition_cli_orphan_rechecks_evidence_under_exclusive_lock(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, evidence_file = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-race",
    )
    original_open = tool._open_existing_read_write

    def mutate_then_open(path):
        evidence_file.write_text('{"redacted":true}', encoding="utf-8")
        return original_open(path)

    monkeypatch.setattr(tool, "_open_existing_read_write", mutate_then_open)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="DOCUMENT_SHA256_MISMATCH",
    ):
        tool.apply_disposition(
            **kwargs,
            out_dir=tmp_path / "orphan-race-report",
        )

    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_rejects_forged_preflight_before_rw_open(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-forged-preflight",
    )
    preflight_path = kwargs["orphan_preflight_report"]
    payload = json.loads(preflight_path.read_text(encoding="utf-8"))
    payload["apply_authorized"] = True
    preflight_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    kwargs["expected_orphan_preflight_report_sha256"] = hashlib.sha256(
        preflight_path.read_bytes()
    ).hexdigest()
    opened = False

    def fail_if_opened(_path):
        nonlocal opened
        opened = True
        raise AssertionError("RW connection must not open for forged preflight")

    monkeypatch.setattr(tool, "_open_existing_read_write", fail_if_opened)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="APPROVED_ORPHAN_PREFLIGHT_CONTRACT_INVALID",
    ):
        tool.apply_disposition(**kwargs, out_dir=tmp_path / "forged-preflight-report")
    assert opened is False
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_rejects_preflight_broker_scope_drift_before_rw(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-broker-scope-drift",
    )
    preflight_path = kwargs["orphan_preflight_report"]
    payload = json.loads(preflight_path.read_text(encoding="utf-8"))
    payload["evidence"]["broker_scope_sha256"] = "3" * 64
    payload["verdict"] = tool.orphan_preflight_tool.evaluate_report(payload)
    preflight_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    kwargs["expected_orphan_preflight_report_sha256"] = hashlib.sha256(
        preflight_path.read_bytes()
    ).hexdigest()
    opened = False

    def fail_if_opened(_path):
        nonlocal opened
        opened = True
        raise AssertionError("RW connection must not open for broker scope drift")

    monkeypatch.setattr(tool, "_open_existing_read_write", fail_if_opened)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="ORPHAN_EVIDENCE_ACCOUNT_SCOPE_SHA256_MISMATCH",
    ):
        tool.apply_disposition(**kwargs, out_dir=tmp_path / "broker-scope-drift-report")
    assert opened is False
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_rejects_stale_preflight_database(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-stale-preflight",
    )
    write = initialize_database(db_path)
    write.execute(
        "UPDATE risk_observations_latest SET name = 'changed-after-preflight' "
        "WHERE candidate_instance_id = 'orphan-private-id'"
    )
    write.commit()
    write.close()

    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="ORPHAN_PREFLIGHT_DATABASE_MAIN_FINGERPRINT_MISMATCH",
    ):
        tool.apply_disposition(**kwargs, out_dir=tmp_path / "stale-preflight-report")
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_rejects_committed_wal_race(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-wal-race",
    )
    original_sidecar_check = tool._assert_quiescent_sidecars
    held_readers: list[sqlite3.Connection] = []
    sidecar_check_count = 0

    def check_then_commit_unrelated_wal_write(path):
        nonlocal sidecar_check_count
        original_sidecar_check(path)
        sidecar_check_count += 1
        if sidecar_check_count != 2:
            return
        reader = sqlite3.connect(path, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM app_metadata").fetchone()
        held_readers.append(reader)
        writer = sqlite3.connect(path)
        try:
            writer.execute(
                "INSERT INTO app_metadata(key, value) VALUES (?, ?)",
                ("unrelated_phase_b_write", "committed"),
            )
            writer.commit()
        finally:
            writer.close()
        assert Path(f"{path}-wal").stat().st_size > 0

    monkeypatch.setattr(
        tool,
        "_assert_quiescent_sidecars",
        check_then_commit_unrelated_wal_write,
    )
    try:
        with pytest.raises(
            tool.PipelineDispositionCliError,
            match="ORPHAN_APPLY_COMMITTED_WAL_FRAMES_PRESENT",
        ):
            tool.apply_disposition(**kwargs, out_dir=tmp_path / "wal-race-report")
    finally:
        for reader in held_readers:
            reader.rollback()
            reader.close()

    check = sqlite3.connect(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
        check.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        check.close()


def test_pipeline_disposition_cli_orphan_rejects_same_byte_artifact_replacement(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs, _ = _orphan_apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r8.pipeline.orphan-artifact-race",
    )
    artifact = kwargs["authoritative_artifact"]
    replacement = artifact.with_name("replacement-artifact.bin")
    replacement.write_bytes(artifact.read_bytes())
    original_open = tool._open_existing_read_write

    def replace_then_open(path):
        os.replace(replacement, artifact)
        return original_open(path)

    monkeypatch.setattr(tool, "_open_existing_read_write", replace_then_open)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="ORPHAN_PRIVATE_INPUT_IDENTITY_CHANGED_DURING_APPLY",
    ):
        tool.apply_disposition(**kwargs, out_dir=tmp_path / "artifact-race-report")
    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


@pytest.mark.parametrize(
    "value",
    (
        "ghp_" + "a" * 36,
        "github_pat_" + "b" * 30,
        "acct9876-5432",
        "account:98765432",
    ),
)
def test_pipeline_disposition_evidence_rejects_and_redacts_sensitive_values(value) -> None:
    with pytest.raises(tool.PipelineDispositionCliError, match="UNSAFE_LABEL"):
        tool._safe_label("evidence_ref", value)

    redacted = tool._redact(
        {
            "evidence_ref": value,
            "candidate_instance_id": value,
            "nested": [value],
        }
    )
    summary = tool.render_markdown_summary(
        {
            "generated_at": "2026-07-15T00:00:00Z",
            "mode": "PREVIEW",
            "preview": {"candidate_instance_id": value, "action": "REVOKE"},
            "verdict": {"status": "BLOCKED", "failures": []},
        }
    )

    assert value not in str(redacted)
    assert value not in summary
    assert "[REDACTED]" in str(redacted)
    assert "[REDACTED]" in summary


def test_pipeline_disposition_redaction_preserves_iso_dates() -> None:
    label = "fast0/2026-07-15/evidence"
    payload = {
        "trade_date": "2026-07-15",
        "subject_key": "2026-07-15:candidate-safe",
    }

    assert tool._safe_label("evidence_ref", label) == label
    assert tool._redact(payload) == payload


def test_pipeline_disposition_cli_apply_requires_explicit_safe_env(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "unsafe.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text("{}", encoding="utf-8")
    monkeypatch.delenv("TRADING_ENV_FILE", raising=False)

    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="EXPLICIT_TRADING_ENV_FILE_REQUIRED",
    ):
        tool.apply_disposition(
            db_path=db_path,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
            request_id="fast0r3.pipeline.unsafe",
            expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
            expected_subject_version=preview["subject_version"],
            expected_source_fingerprint=preview["source_fingerprint"],
            expected_candidate_fingerprint=preview["candidate_fingerprint"],
            expected_downstream_fingerprint=preview["downstream_fingerprint"],
            expected_boundary_fingerprint=preview["boundary_fingerprint"],
            reason_code="FAST0R3_EVIDENCE",
            operator_id="fast0.operator",
            evidence_type="FAST0R3_PIPELINE_RCA",
            evidence_ref="fast0r3/pipeline-rca-redacted",
            evidence_file=evidence_file,
            acknowledge=tool.APPLY_ACK,
            out_dir=tmp_path / "unsafe-evidence",
        )


def test_pipeline_disposition_cli_rechecks_runtime_lease_under_exclusive_lock(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "lease-race.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "lease-race-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "lease-race-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    def injected_lease_count(connection) -> int:
        assert connection.in_transaction is True
        return 1

    monkeypatch.setattr(tool, "_runtime_lease_count", injected_lease_count)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="RUNTIME_EXECUTION_LEASE_PRESENT",
    ):
        tool.apply_disposition(
            db_path=db_path,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
            request_id="fast0r3.pipeline.lease-race",
            expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
            expected_subject_version=preview["subject_version"],
            expected_source_fingerprint=preview["source_fingerprint"],
            expected_candidate_fingerprint=preview["candidate_fingerprint"],
            expected_downstream_fingerprint=preview["downstream_fingerprint"],
            expected_boundary_fingerprint=preview["boundary_fingerprint"],
            reason_code="FAST0R3_EVIDENCE",
            operator_id="fast0.operator",
            evidence_type="FAST0R3_PIPELINE_RCA",
            evidence_ref="fast0r3/pipeline-rca-redacted",
            evidence_file=evidence_file,
            acknowledge=tool.APPLY_ACK,
            out_dir=tmp_path / "lease-race-report",
        )

    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0] == 0
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_reconciles_durable_commit_exception(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "commit-reconcile.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "commit-reconcile-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "commit-reconcile-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    original_commit = tool._commit_connection

    def durable_commit_then_raise(connection) -> None:
        original_commit(connection)
        raise OSError("injected post-commit acknowledgement failure")

    monkeypatch.setattr(tool, "_commit_connection", durable_commit_then_raise)
    report = tool.apply_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        request_id="fast0r3.pipeline.commit-reconcile",
        expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
        expected_subject_version=preview["subject_version"],
        expected_source_fingerprint=preview["source_fingerprint"],
        expected_candidate_fingerprint=preview["candidate_fingerprint"],
        expected_downstream_fingerprint=preview["downstream_fingerprint"],
        expected_boundary_fingerprint=preview["boundary_fingerprint"],
        reason_code="FAST0R3_EVIDENCE",
        operator_id="fast0.operator",
        evidence_type="FAST0R3_PIPELINE_RCA",
        evidence_ref="fast0r3/pipeline-rca-redacted",
        evidence_file=evidence_file,
        acknowledge=tool.APPLY_ACK,
        out_dir=tmp_path / "commit-reconcile-report",
    )

    assert report["verdict"]["status"] == "APPLIED_RECONCILED_WITH_WARNING"
    assert report["verdict"]["committed"] is True
    assert report["invariants"]["commit_reconciliation"] == "COMMITTED"
    assert (
        tool._reconcile_committed_disposition(
            db_path,
            request_id="fast0r3.pipeline.commit-reconcile",
            applied=report["result"],
        )
        == "COMMITTED"
    )


def test_pipeline_disposition_cli_reports_postcommit_file_state_failure(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "postcommit-file-state.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "postcommit-file-state-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "postcommit-file-state-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    original_file_state = tool._file_state
    calls = 0

    def fail_second_file_state(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected postcommit file-state failure")
        return original_file_state(path)

    monkeypatch.setattr(tool, "_file_state", fail_second_file_state)
    report = tool.apply_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        request_id="fast0r3.pipeline.postcommit-file-state",
        expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
        expected_subject_version=preview["subject_version"],
        expected_source_fingerprint=preview["source_fingerprint"],
        expected_candidate_fingerprint=preview["candidate_fingerprint"],
        expected_downstream_fingerprint=preview["downstream_fingerprint"],
        expected_boundary_fingerprint=preview["boundary_fingerprint"],
        reason_code="FAST0R3_EVIDENCE",
        operator_id="fast0.operator",
        evidence_type="FAST0R3_PIPELINE_RCA",
        evidence_ref="fast0r3/pipeline-rca-redacted",
        evidence_file=evidence_file,
        acknowledge=tool.APPLY_ACK,
        out_dir=tmp_path / "postcommit-file-state-report",
    )

    check = tool._open_strict_read_only(db_path)
    try:
        row_count = check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions WHERE request_id = ?",
            ("fast0r3.pipeline.postcommit-file-state",),
        ).fetchone()[0]
    finally:
        check.close()

    assert row_count == 1
    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert report["verdict"]["retry_with_same_request_id"] == (
        "fast0r3.pipeline.postcommit-file-state"
    )


def test_pipeline_disposition_cli_preserves_unknown_commit_outcome_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs = _apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r3.pipeline.outcome-unknown",
    )
    original_commit = tool._commit_connection

    def durable_commit_then_raise(connection) -> None:
        original_commit(connection)
        raise OSError("injected ambiguous commit signal")

    monkeypatch.setattr(tool, "_commit_connection", durable_commit_then_raise)
    monkeypatch.setattr(
        tool,
        "_reconcile_committed_disposition",
        lambda *_args, **_kwargs: "UNKNOWN",
    )

    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "outcome-unknown-report",
    )

    assert report["verdict"]["status"] == "OUTCOME_UNKNOWN"
    assert report["verdict"]["committed"] is None
    assert report["verdict"]["operator_action_required"] is True
    assert report["verdict"]["retry_with_same_request_id"] == ("fast0r3.pipeline.outcome-unknown")
    assert "PIPELINE_DISPOSITION_COMMIT_OUTCOME_UNKNOWN" in report["verdict"]["failures"]
    assert report["result"]["request_id"] == "fast0r3.pipeline.outcome-unknown"
    assert (
        report["result"]["expected_pipeline_fingerprint"] == kwargs["expected_pipeline_fingerprint"]
    )
    assert report["invariants"]["commit_reconciliation"] == "UNKNOWN"
    assert report["report_paths"]["raw_json"]
    assert "committed=unknown" in tool.render_console_summary(report)

    check = tool._open_strict_read_only(db_path)
    try:
        assert (
            check.execute(
                "SELECT COUNT(*) FROM pipeline_coherency_dispositions WHERE request_id = ?",
                ("fast0r3.pipeline.outcome-unknown",),
            ).fetchone()[0]
            == 1
        )
    finally:
        check.close()


def test_pipeline_disposition_cli_treats_close_only_failure_as_committed(
    tmp_path,
    monkeypatch,
) -> None:
    _, kwargs = _apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r3.pipeline.close-only",
    )
    original_close = tool._close_connection

    def close_then_raise(connection) -> None:
        original_close(connection)
        raise OSError("injected close acknowledgement failure")

    monkeypatch.setattr(tool, "_close_connection", close_then_raise)
    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "close-only-report",
    )

    assert report["verdict"]["status"] == "APPLIED_RECONCILED_WITH_WARNING"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is False
    assert report["invariants"]["commit_reconciliation"] == ("COMMITTED_BY_SUCCESSFUL_COMMIT")
    assert "CLOSE_RAISED_AFTER_DURABLE_WRITE" in report["verdict"]["failures"]


def _apply_case(tmp_path, monkeypatch, *, request_id: str):
    db_path = tmp_path / f"{request_id.rsplit('.', 1)[-1]}.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / f"{request_id.rsplit('.', 1)[-1]}.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / f"{request_id.rsplit('.', 1)[-1]}-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    return db_path, {
        "db_path": db_path,
        "trade_date": trade_date,
        "candidate_instance_id": candidate_id,
        "action": tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        "request_id": request_id,
        "expected_pipeline_fingerprint": preview["pipeline_fingerprint"],
        "expected_subject_version": preview["subject_version"],
        "expected_source_fingerprint": preview["source_fingerprint"],
        "expected_candidate_fingerprint": preview["candidate_fingerprint"],
        "expected_downstream_fingerprint": preview["downstream_fingerprint"],
        "expected_boundary_fingerprint": preview["boundary_fingerprint"],
        "reason_code": "FAST0R3_EVIDENCE",
        "operator_id": "fast0.operator",
        "evidence_type": "FAST0R3_PIPELINE_RCA",
        "evidence_ref": "fast0r3/pipeline-rca-redacted",
        "evidence_file": evidence_file,
        "acknowledge": tool.APPLY_ACK,
    }


def _orphan_apply_case(tmp_path, monkeypatch, *, request_id: str):
    preflight_case = _build_orphan_preflight_case(tmp_path, monkeypatch=monkeypatch)
    preflight = tool.orphan_preflight_tool.run_report(
        **preflight_case,
        out_dir=tmp_path / f"{request_id.rsplit('.', 1)[-1]}-orphan-preflight",
        observed_at=_ORPHAN_OBSERVED_AT,
    )
    return _orphan_apply_kwargs(
        tmp_path,
        monkeypatch,
        request_id=request_id,
        preflight_case=preflight_case,
        preflight=preflight,
    )


def _orphan_apply_kwargs(
    tmp_path,
    monkeypatch,
    *,
    request_id: str,
    preflight_case: dict,
    preflight: dict,
):
    preflight_path = Path(preflight["report_paths"]["raw_json"])
    db_path = preflight_case["db_path"]
    manifest_payload = json.loads(
        preflight_case["private_target_manifest"].read_text(encoding="utf-8")
    )
    selected_alias = str(preflight_case["alias"])
    manifest_item = next(
        dict(item) for item in manifest_payload["items"] if item["alias"] == selected_alias
    )
    alias = str(manifest_item.pop("alias"))
    target_set_sha256 = str(preflight["private_target_manifest"]["target_set_sha256"])
    preview = manifest_item
    safe_env = tmp_path / f"{request_id.rsplit('.', 1)[-1]}-orphan.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = preflight_case["evidence_file"]
    evidence_sha256 = hashlib.sha256(evidence_file.read_bytes()).hexdigest()
    document = tool.read_stable_json_document(
        evidence_file,
        expected_sha256=evidence_sha256,
    )
    binding = tool.validate_orphan_manual_evidence_document(
        document,
        expected_target_set_sha256=target_set_sha256,
        expected_alias=alias,
        expected_target=preview,
        expected_artifact_sha256=preflight_case["expected_authoritative_artifact_sha256"],
        expected_artifact_size=preflight_case["authoritative_artifact"].stat().st_size,
        expected_account_scope_sha256=preflight_case["expected_account_scope_sha256"],
        not_after=_ORPHAN_OBSERVED_AT,
    )
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    return (
        db_path,
        {
            "db_path": db_path,
            "trade_date": preview["trade_date"],
            "candidate_instance_id": preview["candidate_instance_id"],
            "action": tool.ACTION_DISPOSE_ORPHAN,
            "request_id": request_id,
            "expected_pipeline_fingerprint": preview["pipeline_fingerprint"],
            "expected_subject_version": preview["subject_version"],
            "expected_source_fingerprint": preview["source_fingerprint"],
            "expected_candidate_fingerprint": preview["candidate_fingerprint"],
            "expected_downstream_fingerprint": preview["downstream_fingerprint"],
            "expected_boundary_fingerprint": preview["boundary_fingerprint"],
            "reason_code": "FAST0R8_AUTHORITATIVE_ORPHAN_EVIDENCE",
            "operator_id": "fast0.operator",
            "evidence_type": "FAST0R8_ORPHAN_EVIDENCE",
            "evidence_ref": f"fast0r8/orphan-evidence-{alias}",
            "evidence_file": evidence_file,
            "acknowledge": tool.APPLY_ACK,
            "expected_evidence_sha256": evidence_sha256,
            "expected_evidence_preview_sha256": binding["evidence_preview_sha256"],
            "evidence_target_set_sha256": target_set_sha256,
            "evidence_alias": alias,
            "orphan_preflight_report": preflight_path,
            "expected_orphan_preflight_report_sha256": hashlib.sha256(
                preflight_path.read_bytes()
            ).hexdigest(),
            "private_target_manifest": preflight_case["private_target_manifest"],
            "expected_private_target_manifest_sha256": preflight_case[
                "expected_private_target_manifest_sha256"
            ],
            "authoritative_artifact": preflight_case["authoritative_artifact"],
            "expected_authoritative_artifact_sha256": preflight_case[
                "expected_authoritative_artifact_sha256"
            ],
        },
        evidence_file,
    )
