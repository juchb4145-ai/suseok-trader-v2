from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_scan_service import inspect_market_scan_event

PROJECTION_NAME_MARKET_SCAN = "market_scan"
_STATUS_KEYS = {
    "PENDING": "outbox_pending_count",
    "PROCESSING": "outbox_processing_count",
    "APPLIED": "outbox_applied_count",
    "SKIPPED": "outbox_skipped_count",
    "ERROR": "outbox_error_count",
    "DEAD_LETTER": "outbox_dead_letter_count",
}


@dataclass(frozen=True, kw_only=True)
class MarketScanProjectionReconcileIssue:
    issue_id: str
    severity: str
    reason_code: str
    message: str
    event_id: str | None = None
    scan_type: str | None = None
    market: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "message": self.message,
            "event_id": self.event_id,
            "scan_type": self.scan_type,
            "market": self.market,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketScanProjectionReconcileRunResult:
    run_id: str
    status: str
    checked_event_count: int
    source_row_count: int
    projected_row_count: int
    projection_error_row_count: int
    event_covered_count: int
    parser_verified_event_count: int
    data_usable_event_count: int
    market_data_dependency_ready_count: int
    outbox_job_count: int
    outbox_pending_count: int
    outbox_processing_count: int
    outbox_applied_count: int
    outbox_skipped_count: int
    outbox_error_count: int
    outbox_dead_letter_count: int
    latest_event_id: str | None
    latest_event_ts: str | None
    latest_event_covered: bool
    append_only_ready: bool
    reason_codes: Sequence[str]
    issues: Sequence[MarketScanProjectionReconcileIssue]
    created_at: str
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "checked_event_count": self.checked_event_count,
            "source_row_count": self.source_row_count,
            "projected_row_count": self.projected_row_count,
            "projection_error_row_count": self.projection_error_row_count,
            "event_covered_count": self.event_covered_count,
            "parser_verified_event_count": self.parser_verified_event_count,
            "data_usable_event_count": self.data_usable_event_count,
            "market_data_dependency_ready_count": self.market_data_dependency_ready_count,
            "outbox_job_count": self.outbox_job_count,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_applied_count": self.outbox_applied_count,
            "outbox_skipped_count": self.outbox_skipped_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "latest_event_id": self.latest_event_id,
            "latest_event_ts": self.latest_event_ts,
            "latest_event_covered": self.latest_event_covered,
            "append_only_ready": self.append_only_ready,
            "reason_codes": list(self.reason_codes),
            "issues": [issue.to_dict() for issue in self.issues],
            "created_at": self.created_at,
            "read_only_projection": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_market_scan_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 100,
    persist: bool = True,
) -> MarketScanProjectionReconcileRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("market_scan_projection_reconcile")
    created_at = datetime_to_wire(utc_now())
    events = _select_events(connection, limit=min(max(int(limit), 1), 5000))
    latest_event = events[-1] if events else None
    inspected = [
        (event, inspect_market_scan_event(_gateway_event(event), settings=resolved_settings))
        for event in events
    ]
    latest_stream_events: dict[tuple[str | None, str | None], str] = {}
    for event, readiness in inspected:
        latest_stream_events[(readiness.scan_type, readiness.market)] = str(event["event_id"])

    issues: list[MarketScanProjectionReconcileIssue] = []
    counts: Counter[str] = Counter()
    source_row_count = 0
    projected_row_count = 0
    projection_error_row_count = 0
    event_covered_count = 0
    parser_verified_event_count = 0
    data_usable_event_count = 0
    market_data_dependency_ready_count = 0
    outbox_job_count = 0
    rollout_rowid = _outbox_rollout_rowid(connection)

    if not events:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_SCAN_SOURCE_EVENT_MISSING",
            message="no accepted market-scan TR response was found",
        )

    latest_event_covered = False
    for event, readiness in inspected:
        event_id = str(event["event_id"])
        request_id = readiness.request_id or ""
        source_row_count += readiness.row_count
        snapshots = _snapshot_count(connection, event_id=event_id, request_id=request_id)
        errors = _error_count(connection, event_id=event_id)
        projected_row_count += snapshots
        projection_error_row_count += errors
        covered = bool(
            (readiness.row_count > 0 and snapshots + errors >= readiness.row_count)
            or (not readiness.response_success and errors > 0)
        )
        if covered:
            event_covered_count += 1
        if readiness.parser_verified:
            parser_verified_event_count += 1
        if readiness.data_usable and errors == 0:
            data_usable_event_count += 1
        dependency_ready = _market_data_dependency_ready(
            connection,
            event_id=event_id,
            expected_rows=readiness.row_count,
        )
        if dependency_ready:
            market_data_dependency_ready_count += 1
        if latest_event is not None and event_id == str(latest_event["event_id"]):
            latest_event_covered = covered

        outbox = _outbox_job(connection, event_id)
        if outbox is None:
            if rollout_rowid is None or int(event["event_rowid"]) >= rollout_rowid:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="ERROR",
                    reason_code="MARKET_SCAN_OUTBOX_MISSING",
                    message="market_scan outbox job is missing",
                    event_id=event_id,
                    scan_type=readiness.scan_type,
                    market=readiness.market,
                )
        else:
            outbox_job_count += 1
            outbox_status = str(outbox["status"]).upper()
            counts[outbox_status] += 1
            if outbox_status in {"ERROR", "DEAD_LETTER"}:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="ERROR",
                    reason_code=f"MARKET_SCAN_OUTBOX_{outbox_status}",
                    message=f"market_scan outbox is {outbox_status}",
                    event_id=event_id,
                    scan_type=readiness.scan_type,
                    market=readiness.market,
                )
            elif outbox_status in {"PENDING", "PROCESSING", "SKIPPED"}:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="WARN",
                    reason_code=f"MARKET_SCAN_OUTBOX_{outbox_status}",
                    message=f"market_scan outbox is {outbox_status}",
                    event_id=event_id,
                    scan_type=readiness.scan_type,
                    market=readiness.market,
                )

        is_latest_stream = latest_stream_events.get(
            (readiness.scan_type, readiness.market)
        ) == event_id
        if not is_latest_stream:
            continue
        if not readiness.recognized:
            _add_issue(
                issues,
                run_id=run_id,
                severity="ERROR",
                reason_code="MARKET_SCAN_EVENT_NOT_RECOGNIZED",
                message="latest scan stream event is not recognized",
                event_id=event_id,
            )
        if not readiness.parser_verified:
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_SCAN_PARSER_UNVERIFIED",
                message="latest scan stream parser is not KOA Studio verified",
                event_id=event_id,
                scan_type=readiness.scan_type,
                market=readiness.market,
                evidence={"parser_status": readiness.parser_status},
            )
        if not readiness.data_usable:
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_SCAN_DATA_UNUSABLE",
                message="latest scan stream response is empty or unsuccessful",
                event_id=event_id,
                scan_type=readiness.scan_type,
                market=readiness.market,
                evidence={"readiness": readiness.to_dict()},
            )
        if not covered:
            _add_issue(
                issues,
                run_id=run_id,
                severity="ERROR",
                reason_code="MARKET_SCAN_EVENT_COVERAGE_MISSING",
                message="latest scan stream rows are not fully terminal",
                event_id=event_id,
                scan_type=readiness.scan_type,
                market=readiness.market,
                evidence={
                    "source_row_count": readiness.row_count,
                    "snapshot_count": snapshots,
                    "error_count": errors,
                },
            )
        if errors:
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_SCAN_ROW_PROJECTION_ERROR",
                message="latest scan stream contains row projection errors",
                event_id=event_id,
                scan_type=readiness.scan_type,
                market=readiness.market,
                evidence={"error_count": errors},
            )
        if not dependency_ready:
            _add_issue(
                issues,
                run_id=run_id,
                severity="ERROR",
                reason_code="MARKET_SCAN_MARKET_DATA_DEPENDENCY_MISSING",
                message="market_data sibling projection is not ready",
                event_id=event_id,
                scan_type=readiness.scan_type,
                market=readiness.market,
            )

    status = _status_from_issues(issues)
    reason_codes = tuple(dict.fromkeys(issue.reason_code for issue in issues))
    append_only_ready = bool(events and status == "PASS" and latest_event_covered)
    result = MarketScanProjectionReconcileRunResult(
        run_id=run_id,
        status=status,
        checked_event_count=len(events),
        source_row_count=source_row_count,
        projected_row_count=projected_row_count,
        projection_error_row_count=projection_error_row_count,
        event_covered_count=event_covered_count,
        parser_verified_event_count=parser_verified_event_count,
        data_usable_event_count=data_usable_event_count,
        market_data_dependency_ready_count=market_data_dependency_ready_count,
        outbox_job_count=outbox_job_count,
        outbox_pending_count=counts["PENDING"],
        outbox_processing_count=counts["PROCESSING"],
        outbox_applied_count=counts["APPLIED"],
        outbox_skipped_count=counts["SKIPPED"],
        outbox_error_count=counts["ERROR"],
        outbox_dead_letter_count=counts["DEAD_LETTER"],
        latest_event_id=None if latest_event is None else str(latest_event["event_id"]),
        latest_event_ts=None if latest_event is None else str(latest_event["event_ts"]),
        latest_event_covered=latest_event_covered,
        append_only_ready=append_only_ready,
        reason_codes=reason_codes,
        issues=tuple(issues),
        created_at=created_at,
    )
    if persist:
        persist_market_scan_projection_reconcile_result(connection, result)
    return result


def persist_market_scan_projection_reconcile_result(
    connection: sqlite3.Connection,
    result: MarketScanProjectionReconcileRunResult,
) -> None:
    payload = result.to_dict()
    connection.execute(
        """
        INSERT INTO market_scan_projection_reconcile_runs (
            run_id, status, checked_event_count, source_row_count,
            projected_row_count, projection_error_row_count, event_covered_count,
            parser_verified_event_count, data_usable_event_count,
            market_data_dependency_ready_count, outbox_job_count,
            outbox_pending_count, outbox_processing_count, outbox_applied_count,
            outbox_skipped_count, outbox_error_count, outbox_dead_letter_count,
            latest_event_id, latest_event_ts, latest_event_covered,
            append_only_ready, reason_codes_json, summary_json, created_at,
            no_trading_side_effects
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.status,
            result.checked_event_count,
            result.source_row_count,
            result.projected_row_count,
            result.projection_error_row_count,
            result.event_covered_count,
            result.parser_verified_event_count,
            result.data_usable_event_count,
            result.market_data_dependency_ready_count,
            result.outbox_job_count,
            result.outbox_pending_count,
            result.outbox_processing_count,
            result.outbox_applied_count,
            result.outbox_skipped_count,
            result.outbox_error_count,
            result.outbox_dead_letter_count,
            result.latest_event_id,
            result.latest_event_ts,
            int(result.latest_event_covered),
            int(result.append_only_ready),
            json.dumps(
                list(result.reason_codes),
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            canonical_json(payload),
            result.created_at,
            int(result.no_trading_side_effects),
        ),
    )
    for issue in result.issues:
        connection.execute(
            """
            INSERT INTO market_scan_projection_reconcile_issues (
                run_id, severity, reason_code, event_id, scan_type, market,
                message, evidence_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                issue.severity,
                issue.reason_code,
                issue.event_id,
                issue.scan_type,
                issue.market,
                issue.message,
                canonical_json(dict(issue.evidence)),
                result.created_at,
            ),
        )
    connection.commit()


def get_latest_market_scan_projection_reconcile(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT * FROM market_scan_projection_reconcile_runs
        ORDER BY created_at DESC, rowid DESC LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "status": "NOT_RUN",
            "latest_run": None,
            "issues": [],
            "read_only": True,
            "no_trading_side_effects": True,
        }
    latest = _run_row(row)
    issue_rows = connection.execute(
        """
        SELECT * FROM market_scan_projection_reconcile_issues
        WHERE run_id = ? ORDER BY id ASC
        """,
        (latest["run_id"],),
    ).fetchall()
    return {
        "status": latest["status"],
        "latest_run": latest,
        "issues": [_issue_row(issue) for issue in issue_rows],
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _select_events(connection: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT rowid AS event_rowid, *
        FROM gateway_events
        WHERE status = 'ACCEPTED'
          AND lower(event_type) = 'tr_response'
          AND (
                lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
                    LIKE 'market_scan:%'
             OR lower(COALESCE(json_extract(payload_json, '$.request_name'), ''))
                    LIKE 'market_scan_%'
             OR lower(COALESCE(json_extract(payload_json, '$.metadata.source'), ''))
                    IN ('market_scan', 'market_scan_service')
          )
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return list(reversed(rows))


def _gateway_event(row: Mapping[str, Any]) -> GatewayEvent:
    return GatewayEvent(
        event_id=str(row["event_id"]),
        event_type=str(row["event_type"]),
        source=str(row["source"]),
        command_id=row["command_id"],
        idempotency_key=row["idempotency_key"],
        ts=parse_timestamp(row["event_ts"], "event_ts"),
        payload=json.loads(str(row["payload_json"])),
    )


def _snapshot_count(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    request_id: str,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count FROM market_scan_snapshots
        WHERE source_event_id = ?
           OR (
                (source_event_id IS NULL OR source_event_id = '')
                AND (
                    request_id = ?
                    OR json_extract(metadata_json, '$.request_id') = ?
                )
           )
        """,
        (event_id, request_id, request_id),
    ).fetchone()
    return int(row["count"] if row else 0)


def _error_count(connection: sqlite3.Connection, *, event_id: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_scan_errors WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return int(row["count"] if row else 0)


def _market_data_dependency_ready(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    expected_rows: int,
) -> bool:
    artifact = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if expected_rows > 0 and int(artifact["count"] if artifact else 0) >= expected_rows:
        return True
    outbox = connection.execute(
        """
        SELECT status FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    return outbox is not None and str(outbox["status"]).upper() == "APPLIED"


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT * FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()


def _outbox_rollout_rowid(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        """
        SELECT MIN(event_rowid) AS event_rowid FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_rowid IS NOT NULL
        """
    ).fetchone()
    return None if row is None or row["event_rowid"] is None else int(row["event_rowid"])


def _add_issue(
    issues: list[MarketScanProjectionReconcileIssue],
    *,
    run_id: str,
    severity: str,
    reason_code: str,
    message: str,
    event_id: str | None = None,
    scan_type: str | None = None,
    market: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    issues.append(
        MarketScanProjectionReconcileIssue(
            issue_id=f"{run_id}:{len(issues) + 1}",
            severity=severity,
            reason_code=reason_code,
            message=message,
            event_id=event_id,
            scan_type=scan_type,
            market=market,
            evidence=dict(evidence or {}),
        )
    )


def _status_from_issues(issues: Sequence[MarketScanProjectionReconcileIssue]) -> str:
    if any(issue.severity == "ERROR" for issue in issues):
        return "FAIL"
    if issues:
        return "WARN"
    return "PASS"


def _run_row(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["latest_event_covered"] = bool(data["latest_event_covered"])
    data["append_only_ready"] = bool(data["append_only_ready"])
    data["no_trading_side_effects"] = bool(data["no_trading_side_effects"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["summary"] = json.loads(data.pop("summary_json"))
    return data


def _issue_row(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["evidence"] = json.loads(data.pop("evidence_json"))
    return data
