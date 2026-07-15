from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from services.live_sim.execution_lifecycle_status import (
    build_live_sim_execution_lifecycle_status,
)
from storage.sqlite import initialize_database


def _event(
    suffix: str,
    *,
    event_type: str = "heartbeat",
    payload_extra: dict[str, Any] | None = None,
    envelope_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event_id": f"evt-{suffix}",
        "event_type": event_type,
        "source": "kiwoom_gateway",
        "ts": "2026-07-15T00:00:00Z",
        "payload": {
            "mode": "LIVE_SIM",
            "live_real_allowed": False,
            **dict(payload_extra or {}),
        },
        **dict(envelope_extra or {}),
    }


def _insert_error(
    connection,
    *,
    event: dict[str, Any] | str,
    created_at: str,
    reason: str = "UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
    code: str | None = None,
    run_id: str | None = None,
    intent_id: str | None = None,
    order_id: str | None = None,
) -> None:
    payload_json = event if isinstance(event, str) else json.dumps(event, sort_keys=True)
    connection.execute(
        """
        INSERT INTO live_sim_errors (
            run_id, live_sim_intent_id, live_sim_order_id, code,
            error_message, payload_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, intent_id, order_id, code, reason, payload_json, created_at),
    )


def _insert_lifecycle(
    connection,
    *,
    suffix: str,
    event: dict[str, Any] | None,
    created_at: str,
    reason: str = "UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
    code: str | None = None,
    run_id: str | None = None,
    order_id: str | None = None,
    evidence_json: str | None = None,
    entity_type: str = "LIVE_SIM_ERROR",
    entity_id: str | None = None,
    status: str = "ERROR",
    live_sim_only: int = 1,
    live_real_allowed: int = 0,
    event_type: str = "LIFECYCLE_ERROR",
) -> None:
    evidence = (
        evidence_json
        if evidence_json is not None
        else json.dumps(
            {"run_id": run_id, "code": code, "payload": event},
            sort_keys=True,
        )
    )
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id, event_type, entity_type, entity_id,
            live_sim_order_id, position_id, status, reason, evidence_json,
            created_at, live_sim_only, live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"lifecycle-{suffix}",
            event_type,
            entity_type,
            entity_id,
            order_id,
            status,
            reason,
            evidence,
            created_at,
            live_sim_only,
            live_real_allowed,
        ),
    )


def _insert_pair(
    connection,
    *,
    suffix: str,
    event_type: str = "heartbeat",
    reason: str = "UNKNOWN_LIVE_SIM_GATEWAY_EVENT",
    lifecycle_reason: str | None = None,
    code: str | None = None,
    created_at: str | None = None,
    payload_extra: dict[str, Any] | None = None,
    envelope_extra: dict[str, Any] | None = None,
    intent_id: str | None = None,
) -> None:
    event_payload = dict(payload_extra or {})
    if code is not None:
        event_payload["code"] = code
    event = _event(
        suffix,
        event_type=event_type,
        payload_extra=event_payload,
        envelope_extra=envelope_extra,
    )
    at = created_at or f"2026-07-15T00:00:{int(suffix) % 60:02d}Z"
    _insert_error(
        connection,
        event=event,
        created_at=at,
        reason=reason,
        code=code,
        intent_id=intent_id,
    )
    _insert_lifecycle(
        connection,
        suffix=suffix,
        event=event,
        created_at=at,
        reason=lifecycle_reason or reason,
        code=code,
    )


def _insert_reconcile_event(
    connection,
    *,
    suffix: str,
    created_at: str,
    evidence_json: str = (
        '{"blocking_new_buy":true,'
        '"mismatches":[{"reason":"synthetic_mismatch"}]}'
    ),
    entity_type: str = "RECONCILE",
    entity_id: str | None = "reconcile-source",
    status: str = "RECONCILE_MISMATCH",
    reason: str = "RECONCILE_MISMATCH",
    live_sim_only: int = 1,
    live_real_allowed: int = 0,
    order_id: str | None = None,
    position_id: str | None = None,
    event_type: str = "RECONCILE_MISMATCH",
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id, event_type, entity_type, entity_id,
            live_sim_order_id, position_id, status, reason, evidence_json,
            created_at, live_sim_only, live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"reconcile-event-{suffix}",
            event_type,
            entity_type,
            entity_id,
            order_id,
            position_id,
            status,
            reason,
            evidence_json,
            created_at,
            live_sim_only,
            live_real_allowed,
        ),
    )


def _insert_reconcile_snapshot(
    connection,
    *,
    suffix: str,
    created_at: str,
    mismatch_count: int = 0,
    blocking_new_buy: int = 0,
    status: str = "OK",
    snapshot_json: str | None = None,
    allow_exit: int = 1,
    live_sim_only: int = 1,
) -> None:
    snapshot = snapshot_json or json.dumps(
        _reconcile_snapshot_payload(
            mismatch_count=mismatch_count,
            blocking_new_buy=bool(blocking_new_buy),
            allow_exit=bool(allow_exit),
        ),
        sort_keys=True,
    )
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id, account_id, trade_date, mismatch_count, status,
            snapshot_json, created_at, blocking_new_buy, allow_exit, live_sim_only
        )
        VALUES (?, 'SIMULATION-TEST', '2026-07-15', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"reconcile-{suffix}",
            mismatch_count,
            status,
            snapshot,
            created_at,
            blocking_new_buy,
            allow_exit,
            live_sim_only,
        ),
    )


def _reconcile_snapshot_payload(
    *,
    mismatch_count: int = 0,
    blocking_new_buy: bool = False,
    allow_exit: bool = True,
) -> dict[str, Any]:
    return {
        "broker_snapshot": {},
        "open_orders": [],
        "positions": [],
        "mismatches": (
            [{"reason": "synthetic_mismatch"}] if mismatch_count else []
        ),
        "broker_snapshot_available": False,
        "broker_snapshot_status": "BROKER_SNAPSHOT_UNAVAILABLE",
        "blocking_new_buy": blocking_new_buy,
        "allow_exit": allow_exit,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }


def _normalized_broker_snapshot() -> dict[str, Any]:
    return {
        "account_id": "SIMULATION-TEST",
        "trade_date": "2026-07-15",
        "snapshot_at": "2026-07-15T00:00:00Z",
        "open_orders": [
            {
                "broker_order_no": "SIM-BROKER-ORDER",
                "code": "005930",
                "side": "BUY",
                "quantity": 1,
                "remaining_quantity": 1,
                "price": 70000.0,
                "raw": {"code": "005930"},
            }
        ],
        "positions": [
            {
                "code": "005930",
                "name": "SYNTHETIC",
                "quantity": 1,
                "available_quantity": 1,
                "avg_entry_price": 70000.0,
                "raw": {"code": "005930"},
            }
        ],
        "source": "broker_snapshot",
        "complete": True,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }


def _connection(path: Path):
    return initialize_database(path)


def test_exact_runtime_status_pair_is_historical_without_lowering_canonical(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "historical.sqlite3")
    _insert_pair(
        connection,
        suffix="1",
        payload_extra={"account": "9876-5432", "token": "never-expose-this"},
    )
    connection.commit()
    before_changes = connection.total_changes

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["canonical_status"] == "BLOCKED"
    assert report["qualification_status"] == report["status"] == "PASS"
    assert report["raw_error_count"] == report["raw_error_row_count"] == 1
    assert report["mirror_lifecycle_count"] == report["raw_lifecycle_error_count"] == 1
    assert report["logical_subject_count"] == 1
    assert report["mirrored_pair_count"] == 1
    assert report["historical_runtime_status_audit_count"] == 1
    assert report["active_lifecycle_blocker_count"] == 0
    assert report["manual_review_blocker_count"] == 0
    assert report["effective_blocker_count"] == 0
    assert report["mirror_consistent"] is True
    assert report["inventory_count_consistent"] is True
    assert report["inventory_digest"] == report["scanned_inventory_digest"]
    assert report["inventory_digest"] == report["ending_inventory_digest"]
    assert re.fullmatch(r"[0-9a-f]{64}", report["items"][0]["subject_id"])
    public = json.dumps(report, sort_keys=True)
    assert "9876-5432" not in public
    assert "never-expose-this" not in public
    assert connection.total_changes == before_changes
    assert connection.in_transaction is False
    connection.close()


def test_runtime_status_exact_boolean_safety_flags_remain_historical(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "exact-safety-flags.sqlite3")
    _insert_pair(
        connection,
        suffix="41",
        payload_extra={
            "live_sim_only": True,
            "live_real_allowed": False,
            "metadata": {
                "live_sim_only": True,
                "live_real_allowed": False,
            },
        },
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["qualification_status"] == report["status"] == "PASS"
    assert report["historical_runtime_status_audit_count"] == 1
    assert report["manual_review_blocker_count"] == 0
    connection.close()


@pytest.mark.parametrize(
    "payload_extra",
    [
        {"live_real_allowed": "true"},
        {"live_sim_only": 1},
        {"metadata": {"live_real_allowed": "false"}},
        {"metadata": {"live_sim_only": 1}},
    ],
)
def test_runtime_status_safety_flags_require_exact_booleans(
    tmp_path: Path,
    payload_extra: dict[str, Any],
) -> None:
    connection = _connection(
        tmp_path / f"invalid-safety-flags-{len(str(payload_extra))}.sqlite3"
    )
    _insert_pair(connection, suffix="42", payload_extra=payload_extra)
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["historical_runtime_status_audit_count"] == 0
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_MIRROR_EVENT_METADATA_MISMATCH" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


@pytest.mark.parametrize("event_type", ["price_tick", "Heartbeat"])
def test_unknown_non_exact_lowercase_runtime_type_remains_active(
    tmp_path: Path,
    event_type: str,
) -> None:
    connection = _connection(tmp_path / f"active-{event_type}.sqlite3")
    _insert_pair(connection, suffix="2", event_type=event_type)
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["active_lifecycle_blocker_count"] == 1
    assert report["classification_counts"]["ACTIVE_LIFECYCLE_BLOCKER"] == 1
    assert report["items"][0]["mirror_status"] == "EXACT_1_TO_1"
    assert report["items"][0]["inner_event_type"] is None
    connection.close()


def test_non_whitelisted_inner_event_type_is_not_exposed(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "non-whitelisted-inner-event.sqlite3")
    secret_event_type = "token=short-secret"
    _insert_pair(connection, suffix="43", event_type=secret_event_type)
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["items"][0]["inner_event_type"] is None
    assert secret_event_type not in json.dumps(report, sort_keys=True)
    connection.close()


def test_command_identifier_keeps_exact_pair_active_and_is_not_exposed(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "identifier.sqlite3")
    _insert_pair(
        connection,
        suffix="3",
        envelope_extra={"command_id": "secret-command-id"},
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["mirrored_pair_count"] == 1
    assert report["active_lifecycle_blocker_count"] == 1
    assert report["items"][0]["identifier_free"] is False
    assert "secret-command-id" not in json.dumps(report, sort_keys=True)
    connection.close()


@pytest.mark.parametrize("identifier_key", ["client_order_id", "order_no"])
def test_runtime_status_with_generic_order_identifier_is_not_historical(
    tmp_path: Path,
    identifier_key: str,
) -> None:
    connection = _connection(tmp_path / f"generic-{identifier_key}.sqlite3")
    _insert_pair(
        connection,
        suffix="31",
        payload_extra={identifier_key: "sensitive-order-reference"},
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["historical_runtime_status_audit_count"] == 0
    assert report["active_lifecycle_blocker_count"] == 1
    assert "sensitive-order-reference" not in json.dumps(report, sort_keys=True)
    connection.close()


def test_runtime_status_with_unparseable_event_timestamp_is_manual(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "bad-event-timestamp.sqlite3")
    _insert_pair(
        connection,
        suffix="32",
        envelope_extra={"ts": "garbage"},
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["historical_runtime_status_audit_count"] == 0
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_MIRROR_EVENT_METADATA_MISMATCH" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


@pytest.mark.parametrize("created_at", ["garbage", b"2026-07-15T00:00:00Z"])
def test_runtime_status_with_invalid_storage_timestamp_is_manual(
    tmp_path: Path,
    created_at: str | bytes,
) -> None:
    connection = _connection(tmp_path / "bad-storage-timestamp.sqlite3")
    _insert_pair(connection, suffix="33")
    connection.execute("UPDATE live_sim_errors SET created_at = ?", (created_at,))
    connection.execute(
        "UPDATE live_sim_lifecycle_events SET created_at = ?",
        (created_at,),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)
    reasons = report["items"][0]["reason_codes"]

    assert report["status"] == "BLOCKED"
    assert report["historical_runtime_status_audit_count"] == 0
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_MIRROR_ERROR_FIELDS_INVALID" in reasons
    assert "LIVE_SIM_MIRROR_LIFECYCLE_FIELDS_INVALID" in reasons
    connection.close()


@pytest.mark.parametrize("corruption", ["error_id", "lifecycle_event_id", "code"])
def test_runtime_status_requires_valid_storage_identity_and_code(
    tmp_path: Path,
    corruption: str,
) -> None:
    connection = _connection(tmp_path / f"bad-storage-{corruption}.sqlite3")
    code = "A005930" if corruption == "code" else None
    _insert_pair(connection, suffix="34", code=code)
    if corruption == "error_id":
        connection.execute("UPDATE live_sim_errors SET id = 0")
    elif corruption == "lifecycle_event_id":
        connection.execute(
            "UPDATE live_sim_lifecycle_events SET lifecycle_event_id = ''"
        )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["historical_runtime_status_audit_count"] == 0
    assert report["manual_review_blocker_count"] == 1
    assert report["items"][0]["code"] is None
    connection.close()


def test_orphan_duplicate_malformed_and_field_mismatch_require_manual_review(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "manual.sqlite3")
    orphan_event = _event("orphan")
    _insert_error(
        connection,
        event=orphan_event,
        created_at="2026-07-15T00:01:00Z",
    )
    _insert_pair(
        connection,
        suffix="4",
        created_at="2026-07-15T00:02:00Z",
        lifecycle_reason="DIFFERENT_REASON",
    )
    duplicate_event = _event("duplicate")
    for suffix in ("duplicate-a", "duplicate-b"):
        _insert_error(
            connection,
            event=duplicate_event,
            created_at="2026-07-15T00:03:00Z",
        )
        _insert_lifecycle(
            connection,
            suffix=suffix,
            event=duplicate_event,
            created_at="2026-07-15T00:03:00Z",
        )
    _insert_error(
        connection,
        event="{not-json",
        created_at="2026-07-15T00:04:00Z",
    )
    _insert_lifecycle(
        connection,
        suffix="orphan-lifecycle",
        event=_event("lone-lifecycle"),
        created_at="2026-07-15T00:05:00Z",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)
    reasons = {
        reason for item in report["items"] for reason in item["reason_codes"]
    }

    assert report["status"] == "BLOCKED"
    assert report["manual_review_blocker_count"] == 5
    assert report["mirror_consistent"] is False
    assert "LIVE_SIM_MIRROR_ORPHAN_ERROR" in reasons
    assert "LIVE_SIM_MIRROR_ORPHAN_LIFECYCLE" in reasons
    assert "LIVE_SIM_MIRROR_DUPLICATE_FINGERPRINT" in reasons
    assert "LIVE_SIM_MIRROR_FIELD_MISMATCH" in reasons
    assert "LIVE_SIM_MIRROR_ERROR_PAYLOAD_JSON_INVALID" in reasons
    connection.close()


def test_intent_metadata_missing_or_conflicting_is_manual(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "intent-metadata.sqlite3")
    _insert_pair(
        connection,
        suffix="5",
        intent_id="intent-stored-but-not-in-payload",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["manual_review_blocker_count"] == 1
    assert report["items"][0]["mirror_status"] == "FIELD_MISMATCH"
    assert "LIVE_SIM_MIRROR_INTENT_METADATA_MISMATCH" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


def test_code_filter_is_diagnostic_only(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "code-filter.sqlite3")
    _insert_pair(connection, suffix="6", code="005930")
    _insert_pair(
        connection,
        suffix="7",
        code="000660",
        event_type="price_tick",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(
        connection,
        code="005930",
    )

    assert report["code_filter_diagnostic_only"] is True
    assert report["returned_count"] == report["full_count"] == 1
    assert report["items"][0]["code"] == "005930"
    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 2
    assert report["active_lifecycle_blocker_count"] == 1
    with pytest.raises(ValueError):
        build_live_sim_execution_lifecycle_status(connection, code="bad-code")
    connection.close()


def test_reconcile_requires_latest_clean_snapshot_before_history_is_nonblocking(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_event(
        connection,
        suffix="1",
        created_at="2026-07-15T00:00:01Z",
        entity_id="reconcile-historical",
    )
    connection.commit()

    active = build_live_sim_execution_lifecycle_status(connection)
    assert active["status"] == "BLOCKED"
    assert active["active_reconcile_blocker_count"] == 1
    assert active["active_lifecycle_blocker_count"] == 1

    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    connection.commit()
    clean = build_live_sim_execution_lifecycle_status(connection)
    assert clean["qualification_status"] == "PASS"
    assert clean["canonical_status"] == "BLOCKED"
    assert clean["active_reconcile_blocker_count"] == 0
    assert clean["historical_reconcile_event_count"] == 1
    assert clean["classification_counts"]["HISTORICAL_RUNTIME_STATUS_AUDIT"] == 1
    assert clean["reconcile"]["snapshot_inventory_count"] == 2

    _insert_reconcile_snapshot(
        connection,
        suffix="malformed",
        created_at="2026-07-15T00:02:00Z",
        snapshot_json="{not-json",
    )
    connection.commit()
    malformed = build_live_sim_execution_lifecycle_status(connection)
    assert malformed["status"] == "BLOCKED"
    assert malformed["reconcile_manual_review_count"] == 2
    assert malformed["manual_review_blocker_count"] == 2
    connection.close()


def test_historical_reconcile_mismatch_snapshot_without_event_is_manual(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "orphan-historical-reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="orphan",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 1
    assert report["active_reconcile_blocker_count"] == 0
    assert report["reconcile_manual_review_count"] == 1
    assert "LIVE_SIM_RECONCILE_MISMATCH_SNAPSHOT_EVENT_MISSING" in report[
        "items"
    ][0]["reason_codes"]
    connection.close()


def test_historical_invalid_reconcile_snapshot_without_event_is_manual(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "invalid-historical-reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="invalid",
        created_at="2026-07-15T00:00:00Z",
        snapshot_json="{not-json",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 1
    assert report["reconcile_manual_review_count"] == 1
    assert "LIVE_SIM_RECONCILE_SNAPSHOT_MANUAL_REVIEW_REQUIRED" in report[
        "items"
    ][0]["reason_codes"]
    connection.close()


def test_reconcile_non_text_timestamp_is_manual_and_public_json_safe(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "non-text-reconcile-timestamp.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="non-text-time",
        created_at="2026-07-15T00:00:00Z",
    )
    connection.execute(
        "UPDATE live_sim_reconcile_snapshots SET created_at = ?",
        (b"2026-07-15T00:00:00Z",),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile_manual_review_count"] == 1
    assert report["reconcile"]["created_at"] is None
    assert report["items"][0]["created_at"] is None
    json.dumps(report, sort_keys=True)
    connection.close()


def test_duplicate_reconcile_events_for_one_snapshot_are_manual(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "duplicate-reconcile-events.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    for suffix in ("duplicate-a", "duplicate-b"):
        _insert_reconcile_event(
            connection,
            suffix=suffix,
            created_at="2026-07-15T00:00:01Z",
            entity_id="reconcile-historical",
        )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 1
    assert report["reconcile_manual_review_count"] == 1
    assert report["historical_reconcile_event_count"] == 0
    assert all(
        "LIVE_SIM_RECONCILE_SNAPSHOT_EVENT_NOT_1_TO_1" in item["reason_codes"]
        for item in report["items"]
    )
    connection.close()


def test_latest_active_snapshot_missing_event_keeps_current_active_and_manual(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "latest-active-missing-event.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_event(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:01Z",
        entity_id="reconcile-historical",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="current",
        created_at="2026-07-15T00:01:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["active_reconcile_blocker_count"] == 1
    assert report["reconcile_manual_review_count"] == 1
    assert report["historical_reconcile_event_count"] == 1
    assert report["effective_blocker_count"] == 2
    assert report["logical_subject_count"] == 3
    connection.close()


@pytest.mark.parametrize("integrity_failure", ["duplicate", "evidence_mismatch"])
def test_latest_active_snapshot_integrity_failure_keeps_active_and_manual(
    tmp_path: Path,
    integrity_failure: str,
) -> None:
    connection = _connection(tmp_path / f"latest-active-{integrity_failure}.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="current",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    if integrity_failure == "duplicate":
        for suffix in ("duplicate-a", "duplicate-b"):
            _insert_reconcile_event(
                connection,
                suffix=suffix,
                created_at="2026-07-15T00:00:01Z",
                entity_id="reconcile-current",
            )
    else:
        _insert_reconcile_event(
            connection,
            suffix="evidence-mismatch",
            created_at="2026-07-15T00:00:01Z",
            entity_id="reconcile-current",
            evidence_json=json.dumps(
                {"blocking_new_buy": True, "mismatches": []},
                sort_keys=True,
            ),
        )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["active_reconcile_blocker_count"] == 1
    assert report["reconcile_manual_review_count"] == 1
    assert report["effective_blocker_count"] == 2
    assert report["logical_subject_count"] == 2
    connection.close()


def test_active_reconcile_snapshot_is_a_global_active_subject(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "active-reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="active",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_event(
        connection,
        suffix="active",
        created_at="2026-07-15T00:00:01Z",
        entity_id="reconcile-active",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["logical_subject_count"] == 1
    assert report["active_reconcile_blocker_count"] == 1
    assert report["active_lifecycle_blocker_count"] == 1
    assert report["effective_blocker_count"] == 1
    assert report["items"][0]["classification"] == "ACTIVE_LIFECYCLE_BLOCKER"
    connection.close()


def test_corrupt_reconcile_event_is_manual_even_with_latest_clean_snapshot(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "corrupt-reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_event(
        connection,
        suffix="corrupt",
        created_at="2026-07-15T00:00:00Z",
        entity_type="LIVE_SIM_ERROR",
        entity_id="reconcile-historical",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile_manual_review_count"] == 1
    assert report["manual_review_blocker_count"] == 1
    assert report["historical_reconcile_event_count"] == 0
    connection.close()


@pytest.mark.parametrize(
    "evidence",
    [
        {"blocking_new_buy": True, "mismatches": []},
        {
            "blocking_new_buy": False,
            "mismatches": [{"reason": "synthetic_mismatch"}],
        },
    ],
)
def test_reconcile_event_evidence_must_match_linked_snapshot(
    tmp_path: Path,
    evidence: dict[str, Any],
) -> None:
    connection = _connection(tmp_path / "reconcile-evidence-mismatch.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    _insert_reconcile_event(
        connection,
        suffix="evidence-mismatch",
        created_at="2026-07-15T00:00:01Z",
        entity_id="reconcile-historical",
        evidence_json=json.dumps(evidence, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_RECONCILE_EVENT_SNAPSHOT_EVIDENCE_MISMATCH" in report[
        "items"
    ][0]["reason_codes"]
    connection.close()


@pytest.mark.parametrize("linkage", ["order", "position"])
def test_reconcile_event_must_not_reference_order_or_position(
    tmp_path: Path,
    linkage: str,
) -> None:
    connection = _connection(tmp_path / f"reconcile-{linkage}-link.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    _insert_reconcile_event(
        connection,
        suffix=f"{linkage}-link",
        created_at="2026-07-15T00:00:01Z",
        entity_id="reconcile-historical",
        order_id="order-should-be-null" if linkage == "order" else None,
        position_id="position-should-be-null" if linkage == "position" else None,
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["manual_review_blocker_count"] == 1
    assert report["historical_reconcile_event_count"] == 0
    connection.close()


def test_unknown_latest_reconcile_status_is_manual(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "unknown-reconcile-status.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="unknown",
        created_at="2026-07-15T00:00:00Z",
        status="SOMETHING_NEW",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile"]["status"] == "LATEST_SNAPSHOT_INVALID"
    assert report["manual_review_blocker_count"] == 1
    connection.close()


@pytest.mark.parametrize("corruption", ["safety", "mismatches", "types"])
def test_corrupt_reconcile_snapshot_contract_is_manual(
    tmp_path: Path,
    corruption: str,
) -> None:
    connection = _connection(tmp_path / f"corrupt-snapshot-{corruption}.sqlite3")
    payload = _reconcile_snapshot_payload()
    if corruption == "safety":
        payload["live_real_allowed"] = True
    elif corruption == "mismatches":
        payload["mismatches"] = [{"reason": "not_clean"}]
    else:
        payload["open_orders"] = {}
    _insert_reconcile_snapshot(
        connection,
        suffix=corruption,
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile"]["latest_snapshot_clean"] is False
    assert report["manual_review_blocker_count"] == 1
    connection.close()


def test_safe_legacy_reconcile_snapshot_without_broker_snapshot_is_clean(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "legacy-reconcile-snapshot.sqlite3")
    payload = _reconcile_snapshot_payload()
    payload.pop("broker_snapshot")
    _insert_reconcile_snapshot(
        connection,
        suffix="legacy-safe",
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["qualification_status"] == report["status"] == "PASS"
    assert report["reconcile"]["latest_snapshot_clean"] is True
    assert report["reconcile_manual_review_count"] == 0
    assert report["effective_blocker_count"] == 0
    connection.close()


def test_unavailable_broker_snapshot_must_be_absent_or_exactly_empty(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "unavailable-non-empty-snapshot.sqlite3")
    payload = _reconcile_snapshot_payload()
    payload["broker_snapshot"] = {"unexpected": "payload"}
    _insert_reconcile_snapshot(
        connection,
        suffix="unavailable-non-empty",
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile_manual_review_count"] == 1
    assert "LIVE_SIM_RECONCILE_SNAPSHOT_CONTRACT_INVALID" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


def test_applied_normalized_broker_snapshot_is_clean(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "applied-normalized-snapshot.sqlite3")
    payload = _reconcile_snapshot_payload()
    payload["broker_snapshot_available"] = True
    payload["broker_snapshot_status"] = "BROKER_SNAPSHOT_APPLIED"
    payload["broker_snapshot"] = _normalized_broker_snapshot()
    _insert_reconcile_snapshot(
        connection,
        suffix="applied-normalized",
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["qualification_status"] == report["status"] == "PASS"
    assert report["reconcile"]["latest_snapshot_clean"] is True
    assert report["reconcile_manual_review_count"] == 0
    connection.close()


@pytest.mark.parametrize(
    "corruption",
    [
        "empty",
        "missing_root_field",
        "root_bool_type",
        "open_order_int_type",
        "position_int_type",
        "extra_root_field",
    ],
)
def test_applied_broker_snapshot_requires_exact_normalized_writer_contract(
    tmp_path: Path,
    corruption: str,
) -> None:
    connection = _connection(tmp_path / f"applied-corrupt-{corruption}.sqlite3")
    broker_snapshot = _normalized_broker_snapshot()
    if corruption == "empty":
        broker_snapshot = {}
    elif corruption == "missing_root_field":
        broker_snapshot.pop("source")
    elif corruption == "root_bool_type":
        broker_snapshot["complete"] = 1
    elif corruption == "open_order_int_type":
        broker_snapshot["open_orders"][0]["quantity"] = True
    elif corruption == "position_int_type":
        broker_snapshot["positions"][0]["available_quantity"] = True
    else:
        broker_snapshot["unexpected"] = "field"
    payload = _reconcile_snapshot_payload()
    payload["broker_snapshot_available"] = True
    payload["broker_snapshot_status"] = "BROKER_SNAPSHOT_APPLIED"
    payload["broker_snapshot"] = broker_snapshot
    _insert_reconcile_snapshot(
        connection,
        suffix=corruption,
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile"]["latest_snapshot_clean"] is False
    assert report["reconcile_manual_review_count"] == 1
    assert "LIVE_SIM_RECONCILE_SNAPSHOT_CONTRACT_INVALID" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


@pytest.mark.parametrize(
    "corruption",
    ["missing_snapshot", "contradictory_status"],
)
def test_available_broker_snapshot_requires_mapping_and_applied_status(
    tmp_path: Path,
    corruption: str,
) -> None:
    connection = _connection(tmp_path / f"broker-snapshot-{corruption}.sqlite3")
    payload = _reconcile_snapshot_payload()
    payload["broker_snapshot_available"] = True
    payload["broker_snapshot_status"] = "BROKER_SNAPSHOT_APPLIED"
    if corruption == "missing_snapshot":
        payload.pop("broker_snapshot")
    else:
        payload["broker_snapshot_status"] = "BROKER_SNAPSHOT_UNAVAILABLE"
    _insert_reconcile_snapshot(
        connection,
        suffix=corruption,
        created_at="2026-07-15T00:00:00Z",
        snapshot_json=json.dumps(payload, sort_keys=True),
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["reconcile"]["latest_snapshot_clean"] is False
    assert report["reconcile_manual_review_count"] == 1
    assert report["items"][0]["classification"] == "MANUAL_REVIEW_BLOCKER"
    assert "LIVE_SIM_RECONCILE_SNAPSHOT_CONTRACT_INVALID" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


def test_unlinked_reconcile_event_is_manual(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "unlinked-reconcile.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    _insert_reconcile_event(
        connection,
        suffix="unlinked",
        created_at="2026-07-15T00:00:00Z",
        entity_id="reconcile-does-not-exist",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_RECONCILE_EVENT_SNAPSHOT_UNLINKED" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


def test_corrupt_lifecycle_error_candidate_is_not_omitted(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "corrupt-lifecycle-candidate.sqlite3")
    _insert_lifecycle(
        connection,
        suffix="corrupt-candidate",
        event=_event("corrupt-candidate"),
        created_at="2026-07-15T00:00:00Z",
        event_type=" lifecycle_error ",
        entity_type="BROKEN",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 1
    assert report["manual_review_blocker_count"] == 1
    assert "LIVE_SIM_MIRROR_LIFECYCLE_FIELDS_INVALID" in report["items"][0][
        "reason_codes"
    ]
    connection.close()


def test_corrupt_reconcile_candidate_is_not_omitted(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "corrupt-reconcile-candidate.sqlite3")
    _insert_reconcile_event(
        connection,
        suffix="corrupt-candidate",
        created_at="2026-07-15T00:00:00Z",
        event_type=" reconcile_mismatch ",
        entity_type="BROKEN",
        reason="BROKEN",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["logical_subject_count"] == 1
    assert report["reconcile_manual_review_count"] == 1
    connection.close()


def test_unrelated_lifecycle_event_is_not_included(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "unrelated-lifecycle-event.sqlite3")
    _insert_lifecycle(
        connection,
        suffix="position-event",
        event=_event("position-event"),
        created_at="2026-07-15T00:00:00Z",
        event_type="POSITION_OPENED",
        entity_type="POSITION",
        status="OPEN",
        reason="POSITION_OPENED",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "PASS"
    assert report["logical_subject_count"] == 0
    connection.close()


def test_reconcile_event_newer_than_latest_clean_snapshot_is_active(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "newer-reconcile-event.sqlite3")
    _insert_reconcile_snapshot(
        connection,
        suffix="historical",
        created_at="2026-07-15T00:00:00Z",
        mismatch_count=1,
        blocking_new_buy=1,
        status="RECONCILE_MISMATCH",
    )
    _insert_reconcile_snapshot(
        connection,
        suffix="clean",
        created_at="2026-07-15T00:01:00Z",
    )
    _insert_reconcile_event(
        connection,
        suffix="newer",
        created_at="2026-07-15T00:02:00Z",
        entity_id="reconcile-historical",
    )
    connection.commit()

    report = build_live_sim_execution_lifecycle_status(connection)

    assert report["status"] == "BLOCKED"
    assert report["active_reconcile_blocker_count"] == 1
    assert report["historical_reconcile_event_count"] == 0
    assert "LIVE_SIM_RECONCILE_EVENT_NEWER_THAN_CLEAN_SNAPSHOT" in report[
        "items"
    ][0]["reason_codes"]
    connection.close()


def test_full_pagination_and_global_digests_cover_more_than_500_pairs(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "pagination.sqlite3")
    for index in range(503):
        _insert_pair(
            connection,
            suffix=str(1000 + index),
            created_at=f"2026-07-15T00:{index // 60:02d}:{index % 60:02d}Z",
        )
    connection.commit()

    first = build_live_sim_execution_lifecycle_status(connection, limit=1000)
    second = build_live_sim_execution_lifecycle_status(connection, limit=10, offset=500)

    assert first["limit"] == 500
    assert first["returned_count"] == 500
    assert first["full_count"] == first["logical_subject_count"] == 503
    assert first["has_more"] is True
    assert first["next_offset"] == 500
    assert second["returned_count"] == 3
    assert second["has_more"] is False
    assert len({item["subject_id"] for item in [*first["items"], *second["items"]]}) == 503
    assert first["inventory_digest"] == second["inventory_digest"]
    assert first["inventory_digest"] == first["ending_inventory_digest"]
    assert first["status"] == "PASS"
    connection.close()
