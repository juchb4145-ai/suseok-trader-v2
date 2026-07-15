from __future__ import annotations

import json
import sqlite3

from services.operator import operator_status as operator_module


def test_operator_lifecycle_surface_whitelists_safe_classifier_fields(monkeypatch) -> None:
    token = "ghp_" + "a" * 36
    account = "9876-5432"
    digest = "a" * 64

    def fake_builder(_connection, *, limit, offset, code):
        return {
            "status": "PASS",
            "qualification_status": "PASS",
            "qualification_reason_codes": [],
            "canonical_status": "FAIL",
            "canonical_reason_codes": ["HISTORICAL_RUNTIME_STATUS_AUDIT"],
            "canonical": {
                "count": 1,
                "payload": {"token": token},
                "account_id": account,
            },
            "classification_counts": {"HISTORICAL_RUNTIME_STATUS_AUDIT": 1},
            "raw_error_count": 1,
            "mirror_lifecycle_count": 1,
            "logical_subject_count": 1,
            "active_lifecycle_blocker_count": 0,
            "historical_runtime_status_audit_count": 1,
            "manual_review_blocker_count": 0,
            "active_reconcile_blocker_count": 0,
            "historical_reconcile_event_count": 0,
            "reconcile_manual_review_count": 0,
            "effective_blocker_count": 0,
            "mirrored_pair_count": 1,
            "mirror_consistent": True,
            "reconcile": {"status": "NOT_PRESENT"},
            "code_filter": code,
            "code_filter_diagnostic_only": True,
            "limit": limit,
            "offset": offset,
            "returned_count": 1,
            "full_count": 1,
            "has_more": False,
            "next_offset": None,
            "pagination": {"limit": limit, "offset": offset},
            "inventory_count_consistent": True,
            "inventory_digest": digest,
            "scanned_inventory_digest": digest,
            "ending_inventory_digest": digest,
            "items": [
                {
                    "subject_id": "b" * 64,
                    "subject_fingerprint": "c" * 64,
                    "classification": "HISTORICAL_RUNTIME_STATUS_AUDIT",
                    "reason_codes": ["CLOSED_RUNTIME_STATUS_AUDIT"],
                    "mirror_status": "CLOSED",
                    "error_surface_count": 1,
                    "lifecycle_surface_count": 1,
                    "created_at": "2026-07-16T00:00:00Z",
                    "code": "005930",
                    "inner_event_type": "runtime_status_audit",
                    "payload_sha256": "d" * 64,
                    "event_metadata_consistent": True,
                    "identifier_free": True,
                    "raw_payload": {"token": token},
                    "account_id": account,
                }
            ],
            "raw_payload": {"token": token},
            "token": token,
            "account_id": account,
            "read_only": True,
            "observe_only": True,
            "no_order_side_effects": True,
            "real_order_allowed": False,
        }

    monkeypatch.setattr(
        operator_module,
        "build_live_sim_execution_lifecycle_status",
        fake_builder,
    )
    connection = sqlite3.connect(":memory:")

    status = operator_module.build_live_sim_execution_lifecycle_public_status(
        connection,
        limit=25,
        offset=5,
        code="005930",
    )
    connection.close()
    serialized = json.dumps(status, ensure_ascii=False)

    assert status["qualification_status"] == "PASS"
    assert status["limit"] == 25
    assert status["offset"] == 5
    assert status["code_filter"] == "005930"
    assert status["items"][0]["created_at"] == "2026-07-16T00:00:00Z"
    assert status["items"][0]["payload_sha256"] == "d" * 64
    assert "raw_payload" not in status
    assert "raw_payload" not in status["items"][0]
    assert token not in serialized
    assert account not in serialized
    assert status["raw_payload_exposed"] is False
    assert status["account_identifier_exposed"] is False
    assert status["token_exposed"] is False


def test_operator_lifecycle_surface_drops_unrecognized_inner_event_type(
    monkeypatch,
) -> None:
    secret_assignment = "token=short-secret"
    digest = "a" * 64

    def fake_builder(_connection, *, limit, offset, code):
        return {
            "status": "BLOCKED",
            "qualification_status": "BLOCKED",
            "qualification_reason_codes": ["LIVE_SIM_ACTIVE_LIFECYCLE_BLOCKER_PRESENT"],
            "canonical_status": "BLOCKED",
            "canonical_reason_codes": ["LIVE_SIM_RAW_LIFECYCLE_ERRORS_PRESENT"],
            "canonical": {},
            "classification_counts": {
                "HISTORICAL_RUNTIME_STATUS_AUDIT": 0,
                "ACTIVE_LIFECYCLE_BLOCKER": 1,
                "MANUAL_REVIEW_BLOCKER": 0,
            },
            "raw_error_count": 1,
            "mirror_lifecycle_count": 1,
            "logical_subject_count": 1,
            "active_lifecycle_blocker_count": 1,
            "historical_runtime_status_audit_count": 0,
            "manual_review_blocker_count": 0,
            "active_reconcile_blocker_count": 0,
            "historical_reconcile_event_count": 0,
            "reconcile_manual_review_count": 0,
            "effective_blocker_count": 1,
            "mirrored_pair_count": 1,
            "mirror_consistent": True,
            "reconcile": {},
            "code_filter": code,
            "code_filter_diagnostic_only": True,
            "limit": limit,
            "offset": offset,
            "returned_count": 1,
            "full_count": 1,
            "has_more": False,
            "next_offset": None,
            "pagination": {},
            "inventory_count_consistent": True,
            "inventory_digest": digest,
            "scanned_inventory_digest": digest,
            "ending_inventory_digest": digest,
            "items": [
                {
                    "subject_id": "b" * 64,
                    "subject_fingerprint": "c" * 64,
                    "classification": "ACTIVE_LIFECYCLE_BLOCKER",
                    "reason_codes": ["LIVE_SIM_ACTIVE_LIFECYCLE_ERROR"],
                    "mirror_status": "EXACT_1_TO_1",
                    "error_surface_count": 1,
                    "lifecycle_surface_count": 1,
                    "created_at": "2026-07-16T00:00:00Z",
                    "code": None,
                    "inner_event_type": secret_assignment,
                    "payload_sha256": "d" * 64,
                    "event_metadata_consistent": False,
                    "identifier_free": True,
                }
            ],
            "read_only": True,
            "observe_only": True,
            "no_order_side_effects": True,
            "real_order_allowed": False,
        }

    monkeypatch.setattr(
        operator_module,
        "build_live_sim_execution_lifecycle_status",
        fake_builder,
    )
    connection = sqlite3.connect(":memory:")
    status = operator_module.build_live_sim_execution_lifecycle_public_status(connection)
    connection.close()

    serialized = json.dumps(status, ensure_ascii=False)
    assert status["items"][0]["inner_event_type"] is None
    assert secret_assignment not in serialized
