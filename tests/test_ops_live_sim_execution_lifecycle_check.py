from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from storage.sqlite import initialize_database
from tools import ops_live_sim_execution_lifecycle_check as tool


def test_strict_lifecycle_check_passes_empty_schema_62_without_raw_rows(tmp_path) -> None:
    db_path = tmp_path / "lifecycle.sqlite3"
    initialize_database(db_path).close()

    report = tool.run_report(
        db_path=db_path,
        code=None,
        run_quick_check=True,
        out_dir=tmp_path / "evidence",
    )
    raw = json.loads(
        Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")
    )

    assert report["verdict"]["status"] == "PASS"
    assert report["database"]["quick_check"] == ["ok"]
    assert report["database"]["identity"]["schema_version"] == "62"
    assert report["database"]["connection"] == {
        "mode": "ro",
        "immutable": True,
        "query_only": True,
    }
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert report["execution_lifecycle"]["collected_count"] == 0
    assert report["execution_lifecycle"]["raw_rows_recorded"] is False
    assert "items" not in raw["execution_lifecycle"]
    assert raw["raw_rows_recorded"] is False


def test_strict_lifecycle_check_qualifies_historical_pair_without_recording_rows(
    tmp_path,
) -> None:
    db_path = tmp_path / "historical-lifecycle.sqlite3"
    connection = initialize_database(db_path)
    created_at = "2026-07-06T00:00:00Z"
    source_event_id = "evt-synthetic-historical-runtime-status"
    payload = {
        "event_type": "heartbeat",
        "event_id": source_event_id,
        "source": "gateway",
        "ts": created_at,
        "payload": {"live_sim_only": True, "state": "IDLE"},
    }
    connection.execute(
        """
        INSERT INTO live_sim_errors (
            run_id, live_sim_intent_id, live_sim_order_id, code,
            error_message, payload_json, created_at
        ) VALUES (?, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            "run-synthetic-historical",
            "UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
            json.dumps(payload, sort_keys=True),
            created_at,
        ),
    )
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id, event_type, entity_type, entity_id,
            live_sim_order_id, position_id, status, reason, evidence_json,
            created_at, live_sim_only, live_real_allowed
        ) VALUES (?, 'LIFECYCLE_ERROR', 'LIVE_SIM_ERROR', NULL,
                  NULL, NULL, 'ERROR', ?, ?, ?, 1, 0)
        """,
        (
            "lifecycle-synthetic-historical",
            "UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
            json.dumps(
                    {
                        "run_id": "run-synthetic-historical",
                        "code": None,
                        "payload": payload,
                    },
                sort_keys=True,
            ),
            created_at,
        ),
    )
    connection.commit()
    connection.close()

    report = tool.run_report(
        db_path=db_path,
        code=None,
        run_quick_check=True,
        out_dir=tmp_path / "evidence",
    )
    raw_text = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    assert report["verdict"]["status"] == "PASS"
    assert report["verdict"]["canonical_status"] == "BLOCKED"
    assert report["verdict"]["qualification_status"] == "PASS"
    assert report["verdict"]["historical_runtime_status_audit_count"] == 1
    assert report["execution_lifecycle"]["full_count"] == 1
    assert report["execution_lifecycle"]["collected_count"] == 1
    assert "items" not in report["execution_lifecycle"]
    assert source_event_id not in raw_text


def test_strict_lifecycle_check_qualifies_clean_historical_reconcile_evidence(
    tmp_path,
) -> None:
    db_path = tmp_path / "historical-reconcile.sqlite3"
    connection = initialize_database(db_path)
    created_at = "2026-07-15T00:00:00Z"
    lifecycle_event_id = "reconcile-event-private-id"
    reconcile_id = "reconcile-private-id"
    clean_reconcile_id = "reconcile-clean-private-id"
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id, event_type, entity_type, entity_id, status,
            reason, evidence_json, created_at, live_sim_only, live_real_allowed
        )
        VALUES (?, 'RECONCILE_MISMATCH', 'RECONCILE', ?,
                'RECONCILE_MISMATCH', 'RECONCILE_MISMATCH', ?, ?, 1, 0)
        """,
        (
            lifecycle_event_id,
            reconcile_id,
            json.dumps(
                {
                    "mismatches": [{"reason": "synthetic_mismatch"}],
                    "blocking_new_buy": True,
                },
                sort_keys=True,
            ),
            created_at,
        ),
    )
    snapshot_contract = {
        "broker_snapshot": {},
        "open_orders": [],
        "positions": [],
        "broker_snapshot_available": False,
        "broker_snapshot_status": "BROKER_SNAPSHOT_UNAVAILABLE",
        "allow_exit": True,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id, account_id, trade_date, mismatch_count, status,
            snapshot_json, created_at, blocking_new_buy, allow_exit, live_sim_only
        )
        VALUES (?, 'SIMULATION-TEST', '2026-07-15', 1, 'RECONCILE_MISMATCH',
                ?, ?, 1, 1, 1)
        """,
        (
            reconcile_id,
            json.dumps(
                {
                    **snapshot_contract,
                    "mismatches": [{"reason": "synthetic_mismatch"}],
                    "blocking_new_buy": True,
                },
                sort_keys=True,
            ),
            "2026-07-14T23:59:00Z",
        ),
    )
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id, account_id, trade_date, mismatch_count, status,
            snapshot_json, created_at, blocking_new_buy, allow_exit, live_sim_only
        )
        VALUES (?, 'SIMULATION-TEST', '2026-07-15', 0, 'OK', ?, ?, 0, 1, 1)
        """,
        (
            clean_reconcile_id,
            json.dumps(
                {
                    **snapshot_contract,
                    "mismatches": [],
                    "blocking_new_buy": False,
                },
                sort_keys=True,
            ),
            "2026-07-15T00:01:00Z",
        ),
    )
    connection.commit()

    public = tool.build_live_sim_execution_lifecycle_status(connection)
    item = public["items"][0]
    assert item["classification"] == "HISTORICAL_RUNTIME_STATUS_AUDIT"
    assert item["mirror_status"] == "RECONCILE_EVIDENCE"
    assert item["event_metadata_consistent"] is False
    assert item["identifier_free"] is False
    assert set(item) == set(tool._PUBLIC_ITEM_KEYS)
    assert {"entity_id", "reconcile_id", "lifecycle_event_id"}.isdisjoint(item)
    assert tool._valid_public_item(item) is True
    assert tool._valid_public_item({**item, "reconcile_id": reconcile_id}) is False
    connection.close()

    report = tool.run_report(
        db_path=db_path,
        code=None,
        run_quick_check=True,
        out_dir=tmp_path / "evidence",
    )
    raw_text = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    assert report["verdict"]["status"] == "PASS"
    assert report["verdict"]["canonical_status"] == "BLOCKED"
    assert report["verdict"]["qualification_status"] == "PASS"
    assert report["verdict"]["historical_runtime_status_audit_count"] == 1
    assert report["execution_lifecycle"]["invalid_item_count"] == 0
    assert lifecycle_event_id not in raw_text
    assert reconcile_id not in raw_text
    assert clean_reconcile_id not in raw_text


def test_strict_lifecycle_check_collects_all_pages_without_returning_items(
    monkeypatch,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    subject_ids = [f"{index:064x}" for index in range(503)]
    digest = "a" * 64

    def fake_builder(_connection, *, limit, offset, code):
        assert limit == 500
        assert code is None
        page_ids = subject_ids[offset : offset + limit]
        returned_count = len(page_ids)
        return {
            **_qualification_contract(digest=digest, full_count=len(subject_ids)),
            "limit": limit,
            "offset": offset,
            "returned_count": returned_count,
            "has_more": offset + returned_count < len(subject_ids),
            "next_offset": offset + returned_count,
            "items": [_public_item(subject_id) for subject_id in page_ids],
        }

    monkeypatch.setattr(tool, "build_live_sim_execution_lifecycle_status", fake_builder)

    result = tool._read_all_pages(connection, code=None)
    connection.close()

    assert result["collected_count"] == 503
    assert result["unique_subject_count"] == 503
    assert result["page_count"] == 2
    assert result["raw_rows_recorded"] is False
    assert "items" not in result


def test_strict_lifecycle_check_detects_item_classification_count_mismatch(
    monkeypatch,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    digest = "a" * 64

    def fake_builder(_connection, *, limit, offset, code):
        status = _qualification_contract(digest=digest, full_count=1)
        status["classification_counts"] = {
            "ACTIVE_LIFECYCLE_BLOCKER": 1,
            "HISTORICAL_RUNTIME_STATUS_AUDIT": 0,
            "MANUAL_REVIEW_BLOCKER": 0,
        }
        return {
            **status,
            "limit": limit,
            "offset": offset,
            "returned_count": 1,
            "has_more": False,
            "next_offset": None,
            "items": [_public_item("a" * 64)],
        }

    monkeypatch.setattr(tool, "build_live_sim_execution_lifecycle_status", fake_builder)

    result = tool._read_all_pages(connection, code=None)
    connection.close()

    assert result["item_classification_counts_consistent"] is False


def test_strict_lifecycle_check_detects_code_filter_changing_global_contract(
    monkeypatch,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    digest = "a" * 64

    def fake_builder(_connection, *, limit, offset, code):
        status = _qualification_contract(digest=digest, full_count=1)
        if code is not None:
            status["raw_error_count"] = 0
        return {
            **status,
            "code_filter": code,
            "limit": limit,
            "offset": offset,
            "returned_count": 0,
            "full_count": 0,
            "has_more": False,
            "next_offset": None,
            "items": [],
        }

    monkeypatch.setattr(tool, "build_live_sim_execution_lifecycle_status", fake_builder)

    result = tool._read_all_pages(connection, code="005930")
    connection.close()

    assert result["requested_code_filter"] == "005930"
    assert result["filter_global_contract_consistent"] is False


def test_strict_lifecycle_verdict_fails_closed_on_page_digest_drift() -> None:
    digest = "b" * 64
    lifecycle = {
        **_qualification_contract(digest=digest, full_count=0),
        "collected_count": 0,
        "unique_subject_count": 0,
        "invalid_item_count": 0,
        "page_count": 1,
        "pages": [
            {
                "offset": 0,
                "returned_count": 0,
                "full_count": 0,
                "has_more": False,
                "next_offset": 0,
                "inventory_count_consistent": True,
                "inventory_digest": "c" * 64,
                "scanned_inventory_digest": "c" * 64,
                "ending_inventory_digest": "c" * 64,
                "qualification_status": "PASS",
                "effective_blocker_count": 0,
            }
        ],
        "raw_rows_recorded": False,
    }
    report = _report(lifecycle)

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "BLOCKED"
    assert "LIFECYCLE_PAGE_SNAPSHOT_MISMATCH" in verdict["failures"]


def test_strict_lifecycle_verdict_rejects_classification_alias_mismatch() -> None:
    lifecycle = _verdict_lifecycle(full_count=1)
    lifecycle["classification_counts"] = {
        "ACTIVE_LIFECYCLE_BLOCKER": 1,
        "HISTORICAL_RUNTIME_STATUS_AUDIT": 0,
        "MANUAL_REVIEW_BLOCKER": 0,
    }

    verdict = tool.evaluate_report(_report(lifecycle))

    assert verdict["status"] == "BLOCKED"
    assert "LIFECYCLE_CLASSIFICATION_ALIAS_MISMATCH" in verdict["failures"]


def test_strict_lifecycle_verdict_rejects_historical_reconcile_outside_subset() -> None:
    lifecycle = _verdict_lifecycle(full_count=1)
    lifecycle["historical_reconcile_event_count"] = 2

    verdict = tool.evaluate_report(_report(lifecycle))

    assert verdict["status"] == "BLOCKED"
    assert (
        "LIFECYCLE_RECONCILE_CLASSIFICATION_SUBSET_MISMATCH"
        in verdict["failures"]
    )


def test_strict_lifecycle_check_rejects_skipping_quick_check(tmp_path) -> None:
    db_path = tmp_path / "mandatory-quick-check.sqlite3"
    initialize_database(db_path).close()

    with pytest.raises(
        tool.LiveSimExecutionLifecycleCheckError,
        match="DATABASE_QUICK_CHECK_IS_MANDATORY",
    ):
        tool.run_report(
            db_path=db_path,
            code=None,
            run_quick_check=False,
            out_dir=tmp_path / "evidence",
        )


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_strict_lifecycle_check_rejects_sidecars(tmp_path, suffix: str) -> None:
    db_path = tmp_path / "sidecar.sqlite3"
    initialize_database(db_path).close()
    db_path.with_name(f"{db_path.name}{suffix}").write_bytes(b"")

    with pytest.raises(
        tool.LiveSimExecutionLifecycleCheckError,
        match="STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE",
    ):
        tool.run_report(
            db_path=db_path,
            code=None,
            run_quick_check=True,
            out_dir=tmp_path / "evidence",
        )


def test_strict_lifecycle_report_redacts_payload_account_and_token() -> None:
    token = "ghp_" + "a" * 36
    account = "9876-5432"

    redacted = tool._redact(
        {
            "payload": {"token": token, "account_id": account},
            "database": {"path": f"C:/evidence/{account}.sqlite3"},
            "generated_at": "2026-07-16T00:00:00Z",
        }
    )
    serialized = json.dumps(redacted, ensure_ascii=False)

    assert token not in serialized
    assert account not in serialized
    assert redacted["payload"] == "[REDACTED]"
    assert redacted["generated_at"] == "2026-07-16T00:00:00Z"


def test_strict_lifecycle_cli_hides_exception_detail(monkeypatch, capsys) -> None:
    token = "ghp_" + "a" * 36

    def raise_sensitive(**_kwargs):
        raise RuntimeError(f"sensitive token={token}")

    monkeypatch.setattr(tool, "run_report", raise_sensitive)
    monkeypatch.setattr(
        tool.sys,
        "argv",
        ["ops_live_sim_execution_lifecycle_check", "--db", "missing.sqlite3"],
    )

    exit_code = tool.main()
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "error_type=RuntimeError" in captured.err
    assert token not in captured.err
    assert "sensitive token" not in captured.err


def _qualification_contract(*, digest: str, full_count: int) -> dict:
    return {
        "status": "PASS",
        "qualification_status": "PASS",
        "qualification_reason_codes": [],
        "canonical_status": "FAIL" if full_count else "PASS",
        "canonical_reason_codes": [],
        "canonical": {},
        "classification_counts": {
            "ACTIVE_LIFECYCLE_BLOCKER": 0,
            "HISTORICAL_RUNTIME_STATUS_AUDIT": full_count,
            "MANUAL_REVIEW_BLOCKER": 0,
        },
        "raw_error_count": full_count,
        "mirror_lifecycle_count": full_count,
        "logical_subject_count": full_count,
        "active_lifecycle_blocker_count": 0,
        "historical_runtime_status_audit_count": full_count,
        "manual_review_blocker_count": 0,
        "active_reconcile_blocker_count": 0,
        "historical_reconcile_event_count": 0,
        "reconcile_manual_review_count": 0,
        "effective_blocker_count": 0,
        "mirrored_pair_count": full_count,
        "mirror_consistent": True,
        "reconcile": {"status": "NOT_PRESENT"},
        "code_filter": None,
        "full_count": full_count,
        "inventory_count_consistent": True,
        "inventory_digest": digest,
        "scanned_inventory_digest": digest,
        "ending_inventory_digest": digest,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def _public_item(subject_id: str) -> dict:
    return {
        "subject_id": subject_id,
        "subject_fingerprint": subject_id,
        "classification": "HISTORICAL_RUNTIME_STATUS_AUDIT",
        "reason_codes": ["CLOSED_RUNTIME_STATUS_AUDIT"],
        "mirror_status": "CLOSED",
        "error_surface_count": 1,
        "lifecycle_surface_count": 1,
        "created_at": "2026-07-16T00:00:00Z",
        "code": "005930",
        "inner_event_type": "runtime_status_audit",
        "payload_sha256": "e" * 64,
        "event_metadata_consistent": True,
        "identifier_free": True,
    }


def _verdict_lifecycle(*, full_count: int) -> dict:
    digest = "b" * 64
    lifecycle = {
        **_qualification_contract(digest=digest, full_count=full_count),
        "collected_count": full_count,
        "unique_subject_count": full_count,
        "invalid_item_count": 0,
        "item_classification_counts_consistent": True,
        "filter_global_contract_consistent": True,
        "page_count": 1,
        "pages": [
            {
                "offset": 0,
                "returned_count": full_count,
                "full_count": full_count,
                "has_more": False,
                "next_offset": None,
                "inventory_count_consistent": True,
                "inventory_digest": digest,
                "scanned_inventory_digest": digest,
                "ending_inventory_digest": digest,
                "qualification_status": "PASS",
                "effective_blocker_count": 0,
            }
        ],
        "raw_rows_recorded": False,
    }
    lifecycle["canonical_status"] = "BLOCKED" if full_count else "PASS"
    return lifecycle


def _report(lifecycle: dict) -> dict:
    fingerprint = {"main": {"exists": True, "sha256": "d" * 64}}
    return {
        "database": {
            "identity": {
                "app_name": "suseok-trader-v2",
                "app_name_row_count": 1,
                "schema_version": "62",
                "schema_version_row_count": 1,
            },
            "files_before": fingerprint,
            "files_after": fingerprint,
            "quick_check": ["ok"],
            "connection": {"mode": "ro", "immutable": True, "query_only": True},
        },
        "execution_lifecycle": lifecycle,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "raw_rows_recorded": False,
    }
