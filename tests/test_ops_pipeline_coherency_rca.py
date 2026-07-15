from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

import pytest
from storage.sqlite import initialize_database
from tools import ops_pipeline_coherency_rca as tool


def test_offline_pipeline_rca_preserves_database_files(tmp_path) -> None:
    db_path = tmp_path / "pipeline-rca.sqlite3"
    initialize_database(db_path).close()

    report = tool.run_report(
        db_path=db_path,
        trade_date=None,
        candidate_instance_id=None,
        max_age_sec=60.0,
        run_quick_check=True,
        out_dir=tmp_path / "evidence",
    )

    assert report["database"]["quick_check"] == ["ok"]
    assert report["verdict"]["status"] == "BLOCKED"
    assert report["verdict"]["database_files_unchanged"] is True
    assert report["pipeline_rca"]["full_count"] == 0
    assert report["pipeline_rca"]["collected_count"] == 0
    assert report["read_only"] is True
    assert report["no_order_side_effects"] is True
    assert report["report_paths"]["raw_json"].endswith("raw.json")


def test_offline_pipeline_rca_collects_every_stable_page(monkeypatch) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    all_ids = [f"candidate-{index:04d}" for index in range(503)]
    digest = "a" * 64

    def fake_builder(_connection, *, offset, limit, **_kwargs):
        page_ids = all_ids[offset : offset + limit]
        return {
            "qualification_status": "BLOCKED",
            "qualification_reason_codes": ["TEST"],
            "canonical_status": "FAIL",
            "schema_ready": True,
            "full_count": len(all_ids),
            "offset": offset,
            "returned_count": len(page_ids),
            "has_more": offset + len(page_ids) < len(all_ids),
            "next_offset": offset + len(page_ids),
            "inventory_count_consistent": True,
            "inventory_digest": digest,
            "inventory_end_digest": digest,
            "items": [{"candidate_instance_id": candidate_id} for candidate_id in page_ids],
        }

    monkeypatch.setattr(tool, "build_pipeline_coherency_rca_status", fake_builder)
    connection.execute("BEGIN DEFERRED")
    try:
        report = tool._read_all_pages(
            connection,
            trade_date="2026-07-15",
            candidate_instance_id=None,
            max_age_sec=60.0,
            as_of=datetime(2026, 7, 15, tzinfo=UTC),
        )
    finally:
        connection.rollback()
        connection.close()

    assert report["collected_count"] == 503
    assert report["page_count"] == 2
    assert [item["candidate_instance_id"] for item in report["items"]] == all_ids


def test_offline_pipeline_rca_verdict_fails_closed_on_page_drift() -> None:
    digest = "b" * 64
    report = {
        "read_only": True,
        "no_order_side_effects": True,
        "database": {
            "files_before": {"main": {"sha256": "c" * 64}},
            "files_after": {"main": {"sha256": "c" * 64}},
            "quick_check": ["ok"],
        },
        "pipeline_rca": {
            "qualification_status": "PASS",
            "canonical_status": "FAIL",
            "schema_ready": True,
            "inventory_count_consistent": True,
            "inventory_digest": digest,
            "inventory_end_digest": digest,
            "full_count": 1,
            "collected_count": 1,
            "items": [{"candidate_instance_id": "candidate-1"}],
            "pages": [
                {
                    "full_count": 2,
                    "inventory_digest": "d" * 64,
                    "inventory_end_digest": "d" * 64,
                    "inventory_count_consistent": True,
                }
            ],
        },
    }

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "BLOCKED"
    assert "PIPELINE_PAGE_SNAPSHOT_MISMATCH" in verdict["failures"]


def test_offline_pipeline_rca_rejects_skipping_quick_check(tmp_path) -> None:
    db_path = tmp_path / "mandatory-quick-check.sqlite3"
    initialize_database(db_path).close()

    with pytest.raises(
        tool.PipelineCoherencyRcaError,
        match="DATABASE_QUICK_CHECK_IS_MANDATORY",
    ):
        tool.run_report(
            db_path=db_path,
            trade_date=None,
            candidate_instance_id=None,
            max_age_sec=60.0,
            run_quick_check=False,
            out_dir=tmp_path / "evidence",
        )


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_offline_pipeline_rca_rejects_even_empty_sidecar(tmp_path, suffix: str) -> None:
    db_path = tmp_path / "sidecar.sqlite3"
    initialize_database(db_path).close()
    db_path.with_name(f"{db_path.name}{suffix}").write_bytes(b"")

    with pytest.raises(
        tool.PipelineCoherencyRcaError,
        match="STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE",
    ):
        tool.run_report(
            db_path=db_path,
            trade_date=None,
            candidate_instance_id=None,
            max_age_sec=60.0,
            run_quick_check=True,
            out_dir=tmp_path / "evidence",
        )


def test_pipeline_rca_redaction_covers_serialized_json_and_sensitive_values() -> None:
    token = "ghp_" + "a" * 36
    account = "9876-5432"
    redacted = tool._redact(
        {
            "payload_json": json.dumps(
                {"account_id": account, "nested": {"token": token}}
            ),
            "candidate_instance_id": f"acct{account}",
            "database": {"path": "C:/evidence/98765432.sqlite3"},
            "trade_date": "2026-07-15",
            "malformed_json": "{",
        }
    )
    serialized = json.dumps(redacted, ensure_ascii=False)

    assert token not in serialized
    assert account not in serialized
    assert "98765432" not in serialized
    assert redacted["trade_date"] == "2026-07-15"
    assert redacted["malformed_json"] == "[REDACTED]"
