from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime

import services.pipeline_coherency as pipeline_coherency
from services.pipeline_coherency import (
    build_pipeline_coherency_rca_status,
    build_pipeline_coherency_status,
)
from storage.sqlite import initialize_database

TRADE_DATE = "2026-07-10"
EVALUATED_AT = "2026-07-10T00:00:00Z"
AS_OF = datetime(2026, 7, 15, tzinfo=UTC)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def test_pipeline_rca_exact_inventory_classification_and_pagination(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca.sqlite3")
    _seed_rca_inventory(connection)
    canonical_before = build_pipeline_coherency_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=500,
    )
    changes_before = connection.total_changes

    report = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=2,
        offset=1,
        as_of=AS_OF,
    )
    canonical_after = build_pipeline_coherency_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=500,
    )

    assert connection.total_changes == changes_before
    assert report["full_count"] == 5
    assert report["returned_count"] == 2
    assert report["has_more"] is True
    assert report["next_offset"] == 3
    assert report["pagination"] == {
        "limit": 2,
        "offset": 1,
        "returned_count": 2,
        "full_count": 5,
        "has_more": True,
        "next_offset": 3,
    }
    assert report["classification_counts"] == {
        "HISTORICAL_CLOSED": 2,
        "MISSING_CANDIDATE_MANUAL_REVIEW": 1,
        "STALE_OTHER_DATE_MANUAL_REVIEW": 1,
        "ACTIVE_CURRENT": 1,
    }
    assert [item["candidate_instance_id"] for item in report["items"]] == [
        "candidate-closed-legacy",
        "candidate-closed-plan",
    ]

    legacy, plan_ready = report["items"]
    assert legacy["classification"] == "HISTORICAL_CLOSED"
    assert legacy["latest_plan"] == {
        "present": False,
        "order_plan_id": None,
        "status": None,
        "created_at": None,
        "expires_at": None,
        "unexpired": False,
        "expiration_status": "ABSENT",
        "entry_timing_evaluation_id": None,
        "strategy_observation_id": None,
        "risk_observation_id": None,
        "source_run_id": None,
        "source_watermark_hash": None,
        "source_event_id": None,
        "source_observed_at": None,
        "generated_by": None,
    }
    assert legacy["current_source_drift"] is False
    assert legacy["legacy_warn_eligible"] is True
    assert legacy["legacy_warn_candidate"] is False
    assert legacy["disposition_required"] is True
    assert legacy["legacy_warn_evidence"] == {
        "status": "PENDING_AUTHORITATIVE_EVIDENCE",
        "authoritative": False,
        "pre_schema59": False,
        "cas_valid": False,
    }

    assert plan_ready["latest_plan"]["status"] == "PLAN_READY"
    assert plan_ready["latest_plan"]["unexpired"] is True
    assert plan_ready["legacy_warn_candidate"] is False
    assert plan_ready["disposition_required"] is True
    for item in report["items"]:
        assert SHA256_RE.fullmatch(item["pipeline_fingerprint"])
        assert SHA256_RE.fullmatch(item["subject_version"])
        assert item["effective_disposition"]["status"] == "PENDING_SCHEMA"

    assert report["schema_ready"] is False
    assert report["legacy_evidence_ready"] is False
    assert report["legacy_warn_candidate_count"] == 0
    assert report["qualification_status"] == "BLOCKED"
    assert "PIPELINE_DISPOSITION_SCHEMA_NOT_READY" in report[
        "qualification_reason_codes"
    ]
    assert report["canonical_status"] == canonical_before["status"]
    assert report["canonical_reason_codes"] == canonical_before["reason_codes"]
    assert report["canonical"]["candidate_count"] == canonical_before["candidate_count"]
    assert canonical_after["status"] == canonical_before["status"]
    assert canonical_after["reason_codes"] == canonical_before["reason_codes"]
    assert canonical_after["candidate_count"] == canonical_before["candidate_count"]

    def evidence_resolver(_trade_date, subjects):
        subject = subjects[0]
        return {
            "status": "READY",
            "items": {
                str(subject["candidate_instance_id"]): {
                    "status": "AUTHORITATIVE",
                    "authoritative": True,
                    "pre_schema59": True,
                    "pipeline_fingerprint": subject["pipeline_fingerprint"],
                    "subject_version": subject["subject_version"],
                }
            },
        }

    proven_legacy = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-legacy",
        legacy_evidence_resolver=evidence_resolver,
        as_of=AS_OF,
    )
    proven_item = proven_legacy["items"][0]
    assert proven_legacy["legacy_evidence_ready"] is True
    assert proven_item["legacy_warn_candidate"] is True
    assert proven_item["legacy_warn_evidence"]["authoritative"] is True
    assert proven_item["disposition_required"] is False
    connection.close()


def test_pipeline_rca_exact_filter_and_fingerprints_track_subject_changes(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca-filter.sqlite3")
    _seed_rca_inventory(connection)

    filtered = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=1000,
        offset=0,
        candidate_instance_id="candidate-closed-plan",
        as_of=AS_OF,
    )
    first = filtered["items"][0]

    assert filtered["limit"] == 500
    assert filtered["full_count"] == 1
    assert filtered["returned_count"] == 1
    assert filtered["has_more"] is False
    assert filtered["next_offset"] is None
    assert filtered["candidate_instance_id_filter"] == "candidate-closed-plan"
    assert first["latest_plan"]["status"] == "PLAN_READY"
    assert first["latest_plan"]["unexpired"] is True

    connection.execute(
        """
        UPDATE order_plan_drafts_latest
        SET status = 'WAIT_RETRY'
        WHERE candidate_instance_id = 'candidate-closed-plan'
        """
    )
    connection.commit()
    changed = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        as_of=AS_OF,
    )["items"][0]

    assert changed["pipeline_fingerprint"] != first["pipeline_fingerprint"]
    assert changed["subject_version"] != first["subject_version"]

    connection.execute(
        "UPDATE order_plan_drafts_latest SET expires_at = 'not-a-timestamp' "
        "WHERE candidate_instance_id = 'candidate-closed-plan'"
    )
    connection.commit()
    unknown_expiry = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        as_of=AS_OF,
    )
    unknown_item = unknown_expiry["items"][0]
    assert unknown_item["pipeline_fingerprint"] == changed["pipeline_fingerprint"]
    assert unknown_item["subject_version"] != changed["subject_version"]
    assert unknown_item["latest_plan"]["unexpired"] is None
    assert unknown_item["latest_plan"]["expiration_status"] == "INVALID"
    assert unknown_expiry["plan_expiry_unknown_count"] == 1
    assert "PIPELINE_PLAN_EXPIRY_UNKNOWN" in unknown_expiry[
        "qualification_reason_codes"
    ]

    missing = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        candidate_instance_id="candidate-does-not-exist",
        as_of=AS_OF,
    )
    canonical = build_pipeline_coherency_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=500,
    )
    assert missing["full_count"] == 0
    assert missing["returned_count"] == 0
    assert missing["canonical_status"] == "WARN"
    assert canonical["candidate_count"] == 5
    connection.close()


def test_pipeline_rca_full_count_and_offset_cross_canonical_500_row_limit(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca-large-page.sqlite3")
    candidate_ids = [f"candidate-{index:04d}" for index in range(503)]
    connection.executemany(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, '005930', 'fixture', ?, 'MATCHED_OBSERVATION', 1, 1, '[]', 'test', 1)
        """,
        [
            (candidate_id, f"strategy-{candidate_id}", TRADE_DATE, EVALUATED_AT)
            for candidate_id in candidate_ids
        ],
    )
    connection.commit()
    changes_before = connection.total_changes

    report = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=1000,
        offset=500,
        as_of=AS_OF,
    )
    canonical = build_pipeline_coherency_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        limit=1000,
    )

    assert connection.total_changes == changes_before
    assert report["limit"] == 500
    assert report["offset"] == 500
    assert report["full_count"] == 503
    assert report["returned_count"] == 3
    assert report["has_more"] is False
    assert report["next_offset"] is None
    assert report["inventory_count_consistent"] is True
    assert SHA256_RE.fullmatch(report["inventory_digest"])
    assert report["inventory_end_digest"] == report["inventory_digest"]
    assert report["inventory_duplicate_key_count"] == 0
    assert report["classification_counts"] == {
        "HISTORICAL_CLOSED": 0,
        "MISSING_CANDIDATE_MANUAL_REVIEW": 503,
        "STALE_OTHER_DATE_MANUAL_REVIEW": 0,
        "ACTIVE_CURRENT": 0,
    }
    assert [item["candidate_instance_id"] for item in report["items"]] == (
        candidate_ids[500:]
    )
    assert canonical["candidate_count"] == 500
    connection.close()


def test_pipeline_rca_disposition_hook_requires_matching_cas_hashes(tmp_path) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca-hook.sqlite3")
    _seed_rca_inventory(connection)

    def resolver(
        _trade_date: str,
        subjects,
    ):
        return {
            "schema_ready": True,
            "status": "READY",
            "items": {
                str(subject["candidate_instance_id"]): {
                    "status": "EFFECTIVE",
                    "effective": True,
                    "pipeline_fingerprint": subject["pipeline_fingerprint"],
                    "subject_version": subject["subject_version"],
                }
                for subject in subjects
            },
        }

    valid = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        disposition_resolver=resolver,
        as_of=AS_OF,
    )

    def stale_resolver(_trade_date, subjects):
        subject = subjects[0]
        return {
            "schema_ready": True,
            "status": "READY",
            "items": {
                str(subject["candidate_instance_id"]): {
                    "status": "EFFECTIVE",
                    "effective": True,
                    "pipeline_fingerprint": "0" * 64,
                    "subject_version": subject["subject_version"],
                }
            },
        }

    stale = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        disposition_resolver=stale_resolver,
        as_of=AS_OF,
    )

    assert valid["schema_ready"] is True
    assert valid["items"][0]["effective_disposition"]["effective"] is True
    assert valid["disposition_pending_count"] == 0
    assert stale["items"][0]["effective_disposition"]["effective"] is False
    assert stale["items"][0]["effective_disposition"]["status"] == "INVALID_CAS"
    assert stale["disposition_pending_count"] == 1

    resolver_not_ready = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-legacy",
        disposition_resolver=lambda *_args: {
            "schema_ready": True,
            "status": "DEGRADED",
            "items": {},
        },
        as_of=AS_OF,
    )
    invalid_result = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-legacy",
        disposition_resolver=lambda *_args: {
            "schema_ready": True,
            "items": [],
        },
        as_of=AS_OF,
    )
    reported_issue = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-legacy",
        disposition_resolver=lambda *_args: {
            "schema_ready": True,
            "status": "READY",
            "reason_codes": ["LEDGER_READ_INCOMPLETE"],
            "items": {},
        },
        as_of=AS_OF,
    )
    assert resolver_not_ready["qualification_status"] == "BLOCKED"
    assert "PIPELINE_DISPOSITION_RESOLVER_NOT_READY" in resolver_not_ready[
        "qualification_reason_codes"
    ]
    assert invalid_result["schema_ready"] is True
    assert invalid_result["disposition"]["status"] == "INVALID_RESOLVER_RESULT"
    assert invalid_result["qualification_status"] == "BLOCKED"
    assert reported_issue["disposition"]["resolver_ready"] is False
    assert "PIPELINE_DISPOSITION_RESOLVER_NOT_READY" in reported_issue[
        "qualification_reason_codes"
    ]

    def contradictory_item_status(_trade_date, subjects):
        subject = subjects[0]
        return {
            "schema_ready": True,
            "status": "READY",
            "items": {
                str(subject["candidate_instance_id"]): {
                    "status": "PENDING",
                    "effective": True,
                    "pipeline_fingerprint": subject["pipeline_fingerprint"],
                    "subject_version": subject["subject_version"],
                }
            },
        }

    contradictory = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        disposition_resolver=contradictory_item_status,
        as_of=AS_OF,
    )
    assert contradictory["items"][0]["effective_disposition"] == {
        "status": "INVALID_STATUS",
        "effective": False,
        "pipeline_fingerprint": contradictory["items"][0][
            "pipeline_fingerprint"
        ],
        "subject_version": contradictory["items"][0]["subject_version"],
        "cas_valid": True,
    }
    assert contradictory["disposition_pending_count"] == 1
    connection.close()


def test_pipeline_rca_resolver_is_detached_and_write_attempt_is_fail_closed(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca-resolver-safety.sqlite3")
    _seed_rca_inventory(connection)

    def mutating_resolver(_trade_date, subjects):
        subject = subjects[0]
        subject["canonical_status"] = "PASS"
        subject["classification"] = "HISTORICAL_CLOSED"
        subject["manual_review_required"] = False
        subject["disposition_required"] = False
        return {
            "schema_ready": True,
            "status": "READY",
            "items": {
                str(subject["candidate_instance_id"]): {
                    "status": "EFFECTIVE",
                    "effective": True,
                    "pipeline_fingerprint": subject["pipeline_fingerprint"],
                    "subject_version": subject["subject_version"],
                }
            },
        }

    detached = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-missing",
        disposition_resolver=mutating_resolver,
        as_of=AS_OF,
    )
    detached_item = detached["items"][0]
    assert detached_item["canonical_status"] != "PASS"
    assert detached_item["classification"] == "MISSING_CANDIDATE_MANUAL_REVIEW"
    assert detached_item["manual_review_required"] is True
    assert detached_item["disposition_required"] is True
    assert detached["manual_review_count"] == 1
    assert detached["manual_review_pending_count"] == 0
    assert detached_item["effective_disposition"]["effective"] is True
    assert "PIPELINE_MANUAL_REVIEW_PRESENT" not in detached[
        "qualification_reason_codes"
    ]

    changes_before = connection.total_changes

    def writing_resolver(_trade_date, _subjects):
        connection.execute("PRAGMA query_only = OFF")
        connection.execute(
            "UPDATE candidates SET state = 'READY' "
            "WHERE candidate_instance_id = 'candidate-closed-plan'"
        )
        return {"schema_ready": True, "status": "READY", "items": {}}

    blocked_write = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-closed-plan",
        disposition_resolver=writing_resolver,
        as_of=AS_OF,
    )
    persisted_state = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        ("candidate-closed-plan",),
    ).fetchone()["state"]
    assert connection.total_changes == changes_before + 1
    assert persisted_state == "CLOSED"
    assert blocked_write["schema_ready"] is False
    assert blocked_write["disposition"]["status"] == "READ_ONLY_VIOLATION"
    assert blocked_write["disposition"]["reason_codes"] == [
        "PIPELINE_DISPOSITION_RESOLVER_READ_ONLY_VIOLATION"
    ]
    assert int(connection.execute("PRAGMA query_only").fetchone()[0]) == 0
    assert connection.in_transaction is False

    connection.execute(
        "UPDATE candidates SET name = 'pending-user-change' "
        "WHERE candidate_instance_id = 'candidate-active'"
    )
    assert connection.in_transaction is True
    nested = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-active",
        as_of=AS_OF,
    )
    pending_name = connection.execute(
        "SELECT name FROM candidates WHERE candidate_instance_id = ?",
        ("candidate-active",),
    ).fetchone()["name"]
    assert nested["full_count"] == 1
    assert connection.in_transaction is True
    assert pending_name == "pending-user-change"
    connection.rollback()
    connection.close()


def test_pipeline_rca_same_count_inventory_identity_drift_is_blocked(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "pipeline-rca-drift.sqlite3")
    _seed_rca_inventory(connection)
    original_inventory_ids = pipeline_coherency._pipeline_rca_inventory_ids
    calls = 0

    def drifting_inventory_ids(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original_inventory_ids(*args, **kwargs)
        if calls == 2:
            result[-1] = result[0]
        return result

    monkeypatch.setattr(
        pipeline_coherency,
        "_pipeline_rca_inventory_ids",
        drifting_inventory_ids,
    )
    report = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        as_of=AS_OF,
    )

    assert report["full_count"] == 5
    assert report["inventory_count_consistent"] is False
    assert report["inventory_duplicate_key_count"] == 1
    assert report["inventory_digest"] != report["inventory_end_digest"]
    assert "PIPELINE_INVENTORY_CHANGED_DURING_READ" in report[
        "qualification_reason_codes"
    ]
    connection.close()


def _seed_rca_inventory(connection: sqlite3.Connection) -> None:
    _insert_candidate(connection, "candidate-active", state="READY")
    _insert_candidate(
        connection,
        "candidate-closed-legacy",
        state="CLOSED",
        closed_at="2026-07-10T01:00:00Z",
    )
    _insert_candidate(
        connection,
        "candidate-closed-plan",
        state="CLOSED",
        closed_at="2026-07-10T01:00:00Z",
    )
    _insert_candidate(
        connection,
        "candidate-stale-other",
        state="STALE",
        trade_date="2026-07-09",
    )
    for candidate_id in (
        "candidate-active",
        "candidate-closed-legacy",
        "candidate-closed-plan",
        "candidate-missing",
        "candidate-stale-other",
    ):
        _insert_pipeline_observations(connection, candidate_id)
    _insert_plan_ready(connection, "candidate-closed-plan")
    connection.commit()


def _insert_candidate(
    connection: sqlite3.Connection,
    candidate_id: str,
    *,
    state: str,
    trade_date: str = TRADE_DATE,
    closed_at: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            detected_at,
            last_seen_at,
            state_updated_at,
            closed_at,
            primary_source_type,
            primary_source_id,
            source_count,
            active_source_count,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, '005930', 'fixture', 1, ?, ?, ?, ?, ?, 'TEST', ?, 1, 0, '[]', '{}')
        """,
        (
            candidate_id,
            trade_date,
            state,
            EVALUATED_AT,
            EVALUATED_AT,
            EVALUATED_AT,
            closed_at,
            f"source-{candidate_id}",
        ),
    )


def _insert_pipeline_observations(
    connection: sqlite3.Connection,
    candidate_id: str,
) -> None:
    strategy_id = f"strategy-{candidate_id}"
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, '005930', 'fixture', ?, 'MATCHED_OBSERVATION', 1, 1, '[]', 'test', 1)
        """,
        (candidate_id, strategy_id, TRADE_DATE, EVALUATED_AT),
    )
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id,
            risk_observation_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (
            ?, ?, ?, ?, '005930', 'fixture', ?, 'OBSERVE_PASS', 'INFO',
            0, 0, 1, '[]', 'test', 1
        )
        """,
        (
            candidate_id,
            f"risk-{candidate_id}",
            strategy_id,
            TRADE_DATE,
            EVALUATED_AT,
        ),
    )


def _insert_plan_ready(connection: sqlite3.Connection, candidate_id: str) -> None:
    connection.execute(
        """
        INSERT INTO order_plan_drafts_latest (
            idempotency_key,
            order_plan_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            side,
            status,
            setup_type,
            entry_timing_state,
            price_location_state,
            current_price,
            limit_price,
            limit_price_source,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent,
            created_at
        )
        VALUES (
            ?, ?, ?, ?, '005930', 'fixture', 'BUY', 'PLAN_READY', 'BREAKOUT',
            'READY', 'NEAR_VWAP', 100, 100, 'FIXTURE', 1, 100, 100, 'FIXTURE',
            '2099-01-01T00:00:00Z', '[]', '{}', 1, 1, ?
        )
        """,
        (
            f"idem-{candidate_id}",
            f"plan-{candidate_id}",
            TRADE_DATE,
            candidate_id,
            EVALUATED_AT,
        ),
    )
