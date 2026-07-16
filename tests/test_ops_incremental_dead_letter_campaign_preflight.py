from __future__ import annotations

import hashlib
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.incremental_dead_letter_campaign import (
    CAMPAIGN_ACTION,
    CAMPAIGN_BUCKET,
    canonical_json,
    private_target_set_sha256,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (
    list_incremental_evaluation_dead_letter_effective_rows,
)
from storage.sqlite import initialize_database
from tests.test_incremental_evaluation_dead_letter_resolution import (
    _insert_closed_legacy_dead_letter,
)
from tests.test_ops_fast0_blocker_resolution_plan import _write_fast0_baseline
from tools import ops_fast0_blocker_resolution_plan as plan_tool
from tools import ops_incremental_dead_letter_campaign_preflight as tool

_OBSERVED_AT = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)


def test_u01_preflight_is_strict_read_only_private_and_not_apply_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    before = _sha256_file(db_path)
    monkeypatch.setattr(tool, "_EXPECTED_TARGET_SET_SHA256", target_set_sha256)
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        base_backup=backup_path,
        expected_base_backup_sha256=_sha256_file(backup_path),
        alias="U01",
        predecessor_apply_report=None,
        expected_predecessor_apply_report_sha256=None,
        out_dir=tmp_path / "r9-preflight",
        observed_at=_OBSERVED_AT,
    )
    evidence = list((tmp_path / "r9-preflight").glob("*/raw.json"))
    raw = evidence[0].read_text(encoding="utf-8")

    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["verdict"]["execution_readiness"] == "PREPARATION_REQUIRED"
    assert report["apply_authorized"] is False
    assert report["campaign"]["completed_count"] == 0
    assert report["campaign"]["pending_count"] == 38
    assert report["campaign"]["progress_valid"] is True
    assert report["selected_target"]["alias"] == "U01"
    assert report["selected_target"]["eligible"] is True
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert _sha256_file(db_path) == before
    assert '"dead_letter_id"' not in raw
    assert '"candidate_instance_id"' not in raw
    assert str(tmp_path) not in raw


def test_u02_preflight_requires_exact_predecessor_before_database_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(tool, "_EXPECTED_TARGET_SET_SHA256", target_set_sha256)
    opened = False

    def forbidden_database_open(*_args, **_kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("database should not be opened")

    monkeypatch.setattr(tool.fast0_tool, "_open_strict_read_only", forbidden_database_open)

    with pytest.raises(
        tool.IncrementalDeadLetterCampaignPreflightError,
        match="PREDECESSOR_APPLY_REPORT_REQUIRED",
    ):
        tool.run_report(
            db_path=db_path,
            blocker_plan_report=plan_path,
            expected_blocker_plan_report_sha256=_sha256_file(plan_path),
            private_target_manifest=manifest_path,
            expected_private_target_manifest_sha256=_sha256_file(manifest_path),
            base_backup=backup_path,
            expected_base_backup_sha256=_sha256_file(backup_path),
            alias="U02",
            predecessor_apply_report=None,
            expected_predecessor_apply_report_sha256=None,
            out_dir=tmp_path / "r9-preflight",
            observed_at=_OBSERVED_AT,
        )

    assert opened is False


def _fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, Path, str]:
    db_path = tmp_path / "campaign.sqlite3"
    connection = initialize_database(db_path)
    for ordinal in range(1, 39):
        _insert_closed_legacy_dead_letter(
            connection,
            candidate_id=f"CAND-2026-07-03-005930-{ordinal:03d}",
            dead_letter_id=f"incremental-dead-letter-{ordinal:03d}",
        )
    connection.close()
    baseline = _write_fast0_baseline(tmp_path, db_path)
    monkeypatch.setattr(
        plan_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    plan = plan_tool.run_report(
        db_path=db_path,
        fast0_report=baseline,
        expected_fast0_report_sha256=_sha256_file(baseline),
        out_dir=tmp_path / "r6-plan",
        observed_at=datetime(2026, 7, 16, 1, 0, tzinfo=UTC),
    )
    plan_path = Path(plan["report_paths"]["raw_json"])

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    rows = sorted(
        list_incremental_evaluation_dead_letter_effective_rows(connection, limit=500),
        key=lambda value: str(value["dead_letter_id"]),
    )
    connection.close()
    items = [
        {
            "action": CAMPAIGN_ACTION,
            "dead_letter_id": row["dead_letter_id"],
            "dead_letter_fingerprint": row["dead_letter_fingerprint"],
            "candidate_version": row["candidate_version"],
            "bucket": CAMPAIGN_BUCKET,
            "alias": f"U{ordinal:02d}",
        }
        for ordinal, row in enumerate(rows, start=1)
    ]
    target_set_sha256 = private_target_set_sha256(items)
    assert plan["incremental_dead_letter"]["campaign_manifest_sha256"] == target_set_sha256
    manifest_path = tmp_path / "private-manifest.json"
    manifest_path.write_text(
        canonical_json({"contract": "fast0-private-target-set.v1", "items": items}),
        encoding="utf-8",
    )
    backup_path = tmp_path / "campaign-base-backup.sqlite3"
    shutil.copy2(db_path, backup_path)
    return db_path, plan_path, manifest_path, backup_path, target_set_sha256


def _passing_probe(_path: Path) -> dict[str, object]:
    return {
        "status": "PASS",
        "method": "TEST_EXCLUSIVE_HANDLE",
        "no_other_open_handles": True,
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
