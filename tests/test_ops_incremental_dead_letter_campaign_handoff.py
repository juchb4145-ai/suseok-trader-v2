from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from tests.test_ops_incremental_dead_letter_campaign_preflight import (
    _fixture,
    _passing_probe,
)
from tests.test_resolve_incremental_evaluation_dead_letter_tool import _write_safe_env
from tools import apply_incremental_dead_letter_campaign as apply_tool
from tools import ops_incremental_dead_letter_campaign_handoff as tool
from tools import ops_incremental_dead_letter_campaign_preflight as preflight_tool


def test_full_u01_u38_chain_seals_private_read_only_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(preflight_tool, "_EXPECTED_TARGET_SET_SHA256", target_set_sha256)
    monkeypatch.setattr(
        preflight_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    monkeypatch.setattr(
        apply_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "private-evidence.json"
    evidence.write_text('{"classification":"obsolete-closed"}', encoding="utf-8")
    start = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)
    predecessor: Path | None = None
    apply_paths: list[Path] = []

    for ordinal in range(1, 39):
        alias = f"U{ordinal:02d}"
        preflight_out = tmp_path / f"preflight-{alias}"
        preflight = preflight_tool.run_report(
            db_path=db_path,
            blocker_plan_report=plan_path,
            expected_blocker_plan_report_sha256=_sha256_file(plan_path),
            private_target_manifest=manifest_path,
            expected_private_target_manifest_sha256=_sha256_file(manifest_path),
            base_backup=backup_path,
            expected_base_backup_sha256=_sha256_file(backup_path),
            alias=alias,
            predecessor_apply_report=predecessor,
            expected_predecessor_apply_report_sha256=(
                None if predecessor is None else _sha256_file(predecessor)
            ),
            out_dir=preflight_out,
            observed_at=start + timedelta(minutes=ordinal * 2 - 2),
        )
        assert preflight["verdict"]["evidence_status"] == "PASS"
        preflight_path = next(preflight_out.glob("*/raw.json"))
        apply_out = tmp_path / f"apply-{alias}"
        applied = apply_tool.apply_alias(
            db_path=db_path,
            blocker_plan_report=plan_path,
            expected_blocker_plan_report_sha256=_sha256_file(plan_path),
            preflight_report=preflight_path,
            expected_preflight_report_sha256=_sha256_file(preflight_path),
            private_target_manifest=manifest_path,
            expected_private_target_manifest_sha256=_sha256_file(manifest_path),
            alias=alias,
            request_id=f"fast0r9.request.{alias.lower()}",
            operator_id="fast0r9.operator",
            evidence_ref=f"fast0r9.{alias.lower()}.evidence",
            evidence_file=evidence,
            expected_evidence_file_sha256=_sha256_file(evidence),
            acknowledgement=apply_tool.APPLY_ACKNOWLEDGEMENT,
            out_dir=apply_out,
            observed_at=start + timedelta(minutes=ordinal * 2 - 1),
        )
        assert applied["verdict"]["status"] == "VERIFIED"
        predecessor = next(apply_out.glob("*/raw.json"))
        apply_paths.append(predecessor)

    handoff = tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        apply_reports=apply_paths,
        expected_apply_report_sha256s=[_sha256_file(path) for path in apply_paths],
        out_dir=tmp_path / "handoff",
        observed_at=start + timedelta(minutes=80),
    )
    raw_path = next((tmp_path / "handoff").glob("*/raw.json"))
    raw = raw_path.read_text(encoding="utf-8")

    assert handoff["verdict"]["evidence_status"] == "PASS"
    assert handoff["verdict"]["handoff_status"] == "COMPLETE"
    assert handoff["campaign"]["completed_count"] == 38
    assert handoff["campaign"]["effective_dead_letter_count"] == 0
    assert handoff["campaign"]["historical_disposed_dead_letter_count"] == 38
    assert len(handoff["apply_chain"]) == 38
    assert '"dead_letter_id"' not in raw
    assert '"operator_id"' not in raw
    assert str(tmp_path) not in raw

    validated = tool.validate_handoff_report(
        json.loads(raw),
        report_sha256=_sha256_file(raw_path),
        expected_source_plan_report_sha256=_sha256_file(plan_path),
        expected_source_database_main=handoff["source"]["database_main"],
        expected_target_set_sha256=target_set_sha256,
    )
    assert validated["target_count"] == 38
    assert validated["final_database_main"] == handoff["database"]["files_after"]["main"]


def test_handoff_rejects_missing_apply_report_without_database_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    def forbidden(*_args, **_kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("database should not be opened")

    monkeypatch.setattr(tool.fast0_tool, "_validated_database_path", forbidden)
    with pytest.raises(
        tool.IncrementalDeadLetterCampaignHandoffError,
        match="APPLY_REPORT_CHAIN_COUNT_MISMATCH",
    ):
        tool.run_report(
            db_path=tmp_path / "missing.sqlite3",
            blocker_plan_report=tmp_path / "plan.json",
            expected_blocker_plan_report_sha256="a" * 64,
            private_target_manifest=tmp_path / "manifest.json",
            expected_private_target_manifest_sha256="b" * 64,
            apply_reports=[],
            expected_apply_report_sha256s=[],
            out_dir=tmp_path / "handoff",
        )
    assert opened is False


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
