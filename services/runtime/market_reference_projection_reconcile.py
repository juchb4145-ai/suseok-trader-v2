from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_reference_service import (
    extract_market_symbol_memberships,
    market_symbols_payload_shape,
)

PROJECTION_NAME_MARKET_REFERENCE = "market_reference"
MARKET_REFERENCE_EVENT_TYPE = "market_symbols"
_STATUS_KEYS = {
    "PENDING": "outbox_pending_count",
    "PROCESSING": "outbox_processing_count",
    "APPLIED": "outbox_applied_count",
    "SKIPPED": "outbox_skipped_count",
    "ERROR": "outbox_error_count",
    "DEAD_LETTER": "outbox_dead_letter_count",
}
_SEVERITY_RANK = {"INFO": 0, "WARN": 1, "FAIL": 2}


@dataclass(frozen=True, kw_only=True)
class MarketReferenceProjectionReconcileIssue:
    issue_id: str
    severity: str
    reason_code: str
    message: str
    event_id: str | None = None
    event_type: str | None = MARKET_REFERENCE_EVENT_TYPE
    code: str | None = None
    market: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "code": self.code,
            "market": self.market,
            "message": self.message,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketReferenceProjectionReconcileRunResult:
    run_id: str
    status: str
    checked_event_count: int
    checked_symbol_count: int
    stored_membership_count: int
    missing_membership_count: int
    missing_membership_ratio: float
    outbox_job_count: int
    outbox_pending_count: int
    outbox_processing_count: int
    outbox_applied_count: int
    outbox_skipped_count: int
    outbox_error_count: int
    outbox_dead_letter_count: int
    payload_shape_counts: Mapping[str, int]
    latest_event_id: str | None
    latest_event_ts: str | None
    append_only_ready: bool
    reason_codes: Sequence[str]
    issues: Sequence[MarketReferenceProjectionReconcileIssue]
    created_at: str
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "checked_event_count": self.checked_event_count,
            "checked_symbol_count": self.checked_symbol_count,
            "stored_membership_count": self.stored_membership_count,
            "missing_membership_count": self.missing_membership_count,
            "missing_membership_ratio": self.missing_membership_ratio,
            "outbox_job_count": self.outbox_job_count,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_applied_count": self.outbox_applied_count,
            "outbox_skipped_count": self.outbox_skipped_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "payload_shape_counts": dict(self.payload_shape_counts),
            "latest_event_id": self.latest_event_id,
            "latest_event_ts": self.latest_event_ts,
            "append_only_ready": self.append_only_ready,
            "reason_codes": list(self.reason_codes),
            "issues": [issue.to_dict() for issue in self.issues],
            "created_at": self.created_at,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_market_reference_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 100,
    persist: bool = True,
) -> MarketReferenceProjectionReconcileRunResult:
    resolved_settings = settings or load_settings()
    bounded_limit = min(max(int(limit), 1), 5000)
    run_id = new_message_id("market_reference_projection_reconcile")
    created_at = datetime_to_wire(utc_now())
    events = _select_market_reference_events(connection, limit=bounded_limit)
    latest_event = events[-1] if events else None
    latest_event_id = None if latest_event is None else str(latest_event["event_id"])
    latest_event_ts = None if latest_event is None else str(latest_event["event_ts"])

    issues: list[MarketReferenceProjectionReconcileIssue] = []
    counters: Counter[str] = Counter()
    outbox_counts: Counter[str] = Counter()
    payload_shape_counts: Counter[str] = Counter()
    memberships = _market_reference_memberships(connection)
    membership_count = len(memberships)
    outbox_rollout_rowid = _market_reference_outbox_rollout_rowid(connection)

    if not events:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REFERENCE_EVENT_MISSING",
            message="no accepted market_symbols gateway event was found",
            evidence={"stored_membership_count": membership_count},
        )

    for row in events:
        event_id = str(row["event_id"])
        payload = _json_object(row["payload_json"])
        payload_shape = market_symbols_payload_shape(payload)
        payload_shape_counts[payload_shape] += 1
        counters["checked_event_count"] += 1
        outbox = _market_reference_outbox_job(connection, event_id)
        if outbox is None:
            event_rowid = int(row["rowid"])
            is_legacy_event = bool(
                event_id != latest_event_id
                and outbox_rollout_rowid is not None
                and event_rowid < outbox_rollout_rowid
            )
            if is_legacy_event:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="INFO",
                    reason_code="MARKET_REFERENCE_LEGACY_EVENT_WITHOUT_OUTBOX",
                    event_id=event_id,
                    message="market_symbols event predates market_reference outbox rollout",
                    evidence={
                        "event_rowid": event_rowid,
                        "outbox_rollout_rowid": outbox_rollout_rowid,
                    },
                )
            else:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="FAIL",
                    reason_code="MARKET_REFERENCE_OUTBOX_JOB_MISSING",
                    event_id=event_id,
                    message="accepted market_symbols event has no market_reference outbox job",
                    evidence={
                        "event_rowid": event_rowid,
                        "outbox_rollout_rowid": outbox_rollout_rowid,
                    },
                )
        else:
            counters["outbox_job_count"] += 1
            outbox_status = str(outbox["status"]).upper()
            outbox_counts[outbox_status] += 1
            _check_outbox_status(
                outbox,
                event=row,
                settings=resolved_settings,
                run_id=run_id,
                issues=issues,
            )

        try:
            symbols = extract_market_symbol_memberships(payload)
        except Exception as exc:
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_REFERENCE_PAYLOAD_PARSE_ERROR",
                event_id=event_id,
                message="market_symbols payload could not be parsed",
                evidence={"error": str(exc), "payload_shape": payload_shape},
            )
            continue

        if not symbols:
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_REFERENCE_EMPTY_SYMBOLS",
                event_id=event_id,
                message="market_symbols payload contains no symbols",
                evidence={"payload_shape": payload_shape},
            )
            continue

        counters["checked_symbol_count"] += len(symbols)
        _check_symbol_coverage(
            event_id=event_id,
            symbols=symbols,
            latest_event_id=latest_event_id,
            memberships=memberships,
            run_id=run_id,
            issues=issues,
            counters=counters,
            outbox=outbox,
        )

    if membership_count <= 0 and counters["checked_symbol_count"] > 0:
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="MARKET_REFERENCE_MEMBERSHIP_COUNT_ZERO",
            message="market_symbol_memberships is empty",
            evidence={"checked_symbol_count": int(counters["checked_symbol_count"])},
        )
    min_membership_count = int(
        resolved_settings.gateway_market_reference_append_only_min_membership_count
    )
    if (
        counters["checked_event_count"] > 0
        and membership_count < min_membership_count
        and membership_count > 0
    ):
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REFERENCE_MEMBERSHIP_COUNT_BELOW_MIN",
            message="stored market reference membership count is below configured minimum",
            evidence={
                "stored_membership_count": membership_count,
                "min_membership_count": min_membership_count,
            },
        )
    _check_latest_membership_freshness(
        connection,
        latest_event_id=latest_event_id,
        latest_event_ts=latest_event_ts,
        run_id=run_id,
        issues=issues,
    )

    checked_symbol_count = int(counters["checked_symbol_count"])
    missing_count = int(counters["missing_membership_count"])
    missing_ratio = missing_count / checked_symbol_count if checked_symbol_count else 0.0
    reason_codes = sorted({issue.reason_code for issue in issues})
    status = _run_status(issues, checked_event_count=int(counters["checked_event_count"]))
    append_only_ready = bool(
        status == "PASS"
        and int(counters["checked_event_count"]) > 0
        and membership_count >= max(min_membership_count, 1)
        and missing_count == 0
        and int(outbox_counts["ERROR"]) == 0
        and int(outbox_counts["DEAD_LETTER"]) == 0
    )
    result = MarketReferenceProjectionReconcileRunResult(
        run_id=run_id,
        status=status,
        checked_event_count=int(counters["checked_event_count"]),
        checked_symbol_count=checked_symbol_count,
        stored_membership_count=membership_count,
        missing_membership_count=missing_count,
        missing_membership_ratio=round(missing_ratio, 6),
        outbox_job_count=int(counters["outbox_job_count"]),
        outbox_pending_count=int(outbox_counts["PENDING"]),
        outbox_processing_count=int(outbox_counts["PROCESSING"]),
        outbox_applied_count=int(outbox_counts["APPLIED"]),
        outbox_skipped_count=int(outbox_counts["SKIPPED"]),
        outbox_error_count=int(outbox_counts["ERROR"]),
        outbox_dead_letter_count=int(outbox_counts["DEAD_LETTER"]),
        payload_shape_counts=dict(payload_shape_counts),
        latest_event_id=latest_event_id,
        latest_event_ts=latest_event_ts,
        append_only_ready=append_only_ready,
        reason_codes=tuple(reason_codes),
        issues=tuple(issues),
        created_at=created_at,
    )
    if persist:
        persist_market_reference_projection_reconcile_result(connection, result)
    return result


def persist_market_reference_projection_reconcile_result(
    connection: sqlite3.Connection,
    result: MarketReferenceProjectionReconcileRunResult,
) -> None:
    payload = result.to_dict()
    summary = {
        key: value
        for key, value in payload.items()
        if key not in {"issues", "reason_codes"}
    }
    connection.execute(
        """
        INSERT INTO market_reference_projection_reconcile_runs (
            run_id,
            status,
            checked_event_count,
            checked_symbol_count,
            stored_membership_count,
            missing_membership_count,
            missing_membership_ratio,
            outbox_job_count,
            outbox_pending_count,
            outbox_processing_count,
            outbox_applied_count,
            outbox_skipped_count,
            outbox_error_count,
            outbox_dead_letter_count,
            latest_event_id,
            latest_event_ts,
            append_only_ready,
            reason_codes_json,
            summary_json,
            created_at,
            no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.status,
            result.checked_event_count,
            result.checked_symbol_count,
            result.stored_membership_count,
            result.missing_membership_count,
            result.missing_membership_ratio,
            result.outbox_job_count,
            result.outbox_pending_count,
            result.outbox_processing_count,
            result.outbox_applied_count,
            result.outbox_skipped_count,
            result.outbox_error_count,
            result.outbox_dead_letter_count,
            result.latest_event_id,
            result.latest_event_ts,
            1 if result.append_only_ready else 0,
            json.dumps(list(result.reason_codes), ensure_ascii=False, sort_keys=True),
            canonical_json(summary),
            result.created_at,
            1 if result.no_trading_side_effects else 0,
        ),
    )
    for issue in result.issues:
        connection.execute(
            """
            INSERT INTO market_reference_projection_reconcile_issues (
                run_id,
                severity,
                reason_code,
                event_id,
                event_type,
                code,
                market,
                message,
                evidence_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                issue.severity,
                issue.reason_code,
                issue.event_id,
                issue.event_type,
                issue.code,
                issue.market,
                issue.message,
                canonical_json(issue.evidence),
                result.created_at,
            ),
        )
    connection.commit()


def get_latest_market_reference_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    issue_limit: int = 20,
) -> dict[str, Any]:
    run = connection.execute(
        """
        SELECT *
        FROM market_reference_projection_reconcile_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if run is None:
        return {
            "latest_run": None,
            "issues": [],
            "issue_summary": {},
            "read_only": True,
            "no_trading_side_effects": True,
        }
    issues = [
        _issue_row_to_dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM market_reference_projection_reconcile_issues
            WHERE run_id = ?
            ORDER BY
                CASE severity WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
                id ASC
            LIMIT ?
            """,
            (run["run_id"], min(max(int(issue_limit), 1), 200)),
        ).fetchall()
    ]
    summary_rows = connection.execute(
        """
        SELECT severity, reason_code, COUNT(*) AS count
        FROM market_reference_projection_reconcile_issues
        WHERE run_id = ?
        GROUP BY severity, reason_code
        ORDER BY severity, reason_code
        """,
        (run["run_id"],),
    ).fetchall()
    return {
        "latest_run": _run_row_to_dict(run),
        "issues": issues,
        "issue_summary": {
            f"{row['severity']}:{row['reason_code']}": int(row["count"])
            for row in summary_rows
        },
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _select_market_reference_events(
    connection: sqlite3.Connection,
    *,
    limit: int,
) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT rowid, event_id, event_type, source, event_ts, received_at, payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED' AND event_type = 'market_symbols'
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return sorted(rows, key=lambda row: int(row["rowid"]))


def _check_symbol_coverage(
    *,
    event_id: str,
    symbols: Sequence[Mapping[str, Any]],
    latest_event_id: str | None,
    memberships: Mapping[str, Mapping[str, Any]],
    run_id: str,
    issues: list[MarketReferenceProjectionReconcileIssue],
    counters: Counter[str],
    outbox: Mapping[str, Any] | None,
) -> None:
    terminal_status = _outbox_terminal_status(outbox)
    superseded_symbol_count = 0
    historical_missing_symbol_count = 0
    superseding_event_ids: Counter[str] = Counter()
    for symbol in symbols:
        code = str(symbol.get("code") or "")
        market = str(symbol.get("market") or "")
        row = memberships.get(code)
        if row is None:
            if event_id != latest_event_id:
                historical_missing_symbol_count += 1
                continue
            counters["missing_membership_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_REFERENCE_MEMBERSHIP_MISSING",
                event_id=event_id,
                code=code,
                market=market,
                message="market_symbols payload code is absent from membership table",
                evidence={"outbox_status": terminal_status},
            )
            continue
        stored_market = str(row["market"])
        stored_event_id = str(row["event_id"])
        if event_id != latest_event_id and stored_event_id != event_id:
            superseded_symbol_count += 1
            superseding_event_ids[stored_event_id] += 1
            continue
        if stored_market != market:
            counters["missing_membership_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_REFERENCE_MEMBERSHIP_MARKET_MISMATCH",
                event_id=event_id,
                code=code,
                market=market,
                message="stored membership market does not match payload market",
                evidence={
                    "stored_market": stored_market,
                    "stored_event_id": stored_event_id,
                },
            )
        if stored_event_id != event_id:
            counters["missing_membership_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_REFERENCE_MEMBERSHIP_EVENT_ID_MISMATCH",
                event_id=event_id,
                code=code,
                market=market,
                message="latest market_symbols event is not reflected by event_id",
                evidence={
                    "stored_event_id": stored_event_id,
                    "outbox_status": terminal_status,
                },
            )
        if terminal_status in {"APPLIED", "SKIPPED"} and stored_event_id != event_id:
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_REFERENCE_ARTIFACT_MISSING_AFTER_TERMINAL_OUTBOX",
                event_id=event_id,
                code=code,
                market=market,
                message="terminal market_reference outbox state has no matching artifact",
                evidence={"outbox_status": terminal_status},
            )

    if event_id != latest_event_id and (
        superseded_symbol_count or historical_missing_symbol_count
    ):
        _add_issue(
            issues,
            run_id=run_id,
            severity="INFO",
            reason_code="MARKET_REFERENCE_MEMBERSHIP_SUPERSEDED",
            event_id=event_id,
            message="older market_symbols membership snapshot was superseded",
            evidence={
                "superseded_symbol_count": superseded_symbol_count,
                "historical_missing_symbol_count": historical_missing_symbol_count,
                "superseding_event_ids": [
                    stored_event_id
                    for stored_event_id, _ in superseding_event_ids.most_common(5)
                ],
            },
        )


def _check_outbox_status(
    outbox: Mapping[str, Any],
    *,
    event: Mapping[str, Any],
    settings: Settings,
    run_id: str,
    issues: list[MarketReferenceProjectionReconcileIssue],
) -> None:
    status = str(outbox["status"]).upper()
    if status in {"PENDING", "PROCESSING"}:
        age = _age_seconds(outbox["created_at"] if status == "PENDING" else outbox["updated_at"])
        max_age = int(settings.gateway_market_reference_append_only_reconcile_max_age_sec)
        severity = "FAIL" if age is not None and age > max_age else "WARN"
        _add_issue(
            issues,
            run_id=run_id,
            severity=severity,
            reason_code=(
                "MARKET_REFERENCE_OUTBOX_STALE"
                if severity == "FAIL"
                else "MARKET_REFERENCE_OUTBOX_PENDING_WITHIN_SLA"
            ),
            event_id=str(event["event_id"]),
            message="market_reference projection_outbox job is not terminal",
            evidence={"outbox": _row_to_plain_dict(outbox), "age_sec": age},
        )
    elif status == "ERROR":
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="MARKET_REFERENCE_OUTBOX_ERROR",
            event_id=str(event["event_id"]),
            message="market_reference projection_outbox job is ERROR",
            evidence={"outbox": _row_to_plain_dict(outbox)},
        )
    elif status == "DEAD_LETTER":
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="MARKET_REFERENCE_OUTBOX_DEAD_LETTER",
            event_id=str(event["event_id"]),
            message="market_reference projection_outbox job is DEAD_LETTER",
            evidence={"outbox": _row_to_plain_dict(outbox)},
        )


def _check_latest_membership_freshness(
    connection: sqlite3.Connection,
    *,
    latest_event_id: str | None,
    latest_event_ts: str | None,
    run_id: str,
    issues: list[MarketReferenceProjectionReconcileIssue],
) -> None:
    if latest_event_id is None or latest_event_ts is None:
        return
    row = connection.execute(
        """
        SELECT MAX(event_ts) AS latest_membership_event_ts
        FROM market_symbol_memberships
        """
    ).fetchone()
    if row is None or row["latest_membership_event_ts"] is None:
        return
    membership_ts = str(row["latest_membership_event_ts"])
    if _timestamp_before(membership_ts, latest_event_ts):
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REFERENCE_MEMBERSHIP_FRESHNESS_BEHIND",
            event_id=latest_event_id,
            message="latest membership event_ts is behind latest market_symbols event_ts",
            evidence={
                "latest_event_ts": latest_event_ts,
                "latest_membership_event_ts": membership_ts,
            },
        )


def _market_reference_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, projection_name, event_id, event_type, status, created_at,
            updated_at, last_error, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_reference' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _market_reference_outbox_rollout_rowid(
    connection: sqlite3.Connection,
) -> int | None:
    row = connection.execute(
        """
        SELECT MIN(event_rowid) AS rollout_rowid
        FROM projection_outbox
        WHERE projection_name = 'market_reference' AND event_rowid IS NOT NULL
        """
    ).fetchone()
    if row is None or row["rollout_rowid"] is None:
        return None
    return int(row["rollout_rowid"])


def _market_reference_memberships(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT code, market, event_id, event_ts, updated_at
        FROM market_symbol_memberships
        """
    ).fetchall()
    return {str(row["code"]): dict(row) for row in rows}


def _outbox_terminal_status(outbox: Mapping[str, Any] | None) -> str | None:
    if outbox is None:
        return None
    status = str(outbox.get("status") or "").upper()
    return status if status in {"APPLIED", "SKIPPED", "ERROR", "DEAD_LETTER"} else None


def _age_seconds(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(str(value), "timestamp")
    except ValueError:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _timestamp_before(left: str, right: str) -> bool:
    try:
        return parse_timestamp(left, "left_ts") < parse_timestamp(right, "right_ts")
    except ValueError:
        return left < right


def _add_issue(
    issues: list[MarketReferenceProjectionReconcileIssue],
    *,
    run_id: str,
    severity: str,
    reason_code: str,
    message: str,
    event_id: str | None = None,
    event_type: str | None = MARKET_REFERENCE_EVENT_TYPE,
    code: str | None = None,
    market: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    normalized_severity = severity.upper()
    if normalized_severity not in _SEVERITY_RANK:
        normalized_severity = "WARN"
    issues.append(
        MarketReferenceProjectionReconcileIssue(
            issue_id=f"{run_id}:{len(issues) + 1}",
            severity=normalized_severity,
            reason_code=reason_code,
            event_id=event_id,
            event_type=event_type,
            code=code,
            market=market,
            message=message,
            evidence=dict(evidence or {}),
        )
    )


def _run_status(
    issues: Sequence[MarketReferenceProjectionReconcileIssue],
    *,
    checked_event_count: int,
) -> str:
    if checked_event_count <= 0:
        return "WARN"
    if any(issue.severity == "FAIL" for issue in issues):
        return "FAIL"
    if any(issue.severity == "WARN" for issue in issues):
        return "WARN"
    return "PASS"


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_array(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _row_to_plain_dict(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {str(key): row[key] for key in row.keys()}


def _run_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["append_only_ready"] = bool(payload["append_only_ready"])
    payload["no_trading_side_effects"] = bool(payload["no_trading_side_effects"])
    payload["reason_codes"] = _json_array(payload.pop("reason_codes_json", "[]"))
    payload["summary"] = _json_object(payload.pop("summary_json", "{}"))
    payload["payload_shape_counts"] = dict(
        payload.get("summary", {}).get("payload_shape_counts") or {}
    )
    return payload


def _issue_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": str(row["id"]),
        "severity": row["severity"],
        "reason_code": row["reason_code"],
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "code": row["code"],
        "market": row["market"],
        "message": row["message"],
        "evidence": _json_object(row["evidence_json"]),
        "created_at": row["created_at"],
    }
