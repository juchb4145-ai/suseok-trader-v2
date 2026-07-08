from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from storage.gateway_command_store import canonical_json

from services.candidate_quote_refresh import (
    candidate_quote_refresh_tick_payloads_from_tr_response,
)
from services.config import Settings, load_settings
from services.market_data_service import (
    MARKET_DATA_EVENT_TYPES,
    QUOTE_ONLY_REAL_TYPES,
    normalize_market_data_exchange,
)

MARKET_DATA_RECONCILE_PROJECTION_NAME = "market_data"
MARKET_DATA_RECONCILE_EVENT_TYPES = tuple(sorted(MARKET_DATA_EVENT_TYPES))
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
class MarketDataProjectionReconcileIssue:
    issue_id: str
    severity: str
    reason_code: str
    event_id: str | None = None
    event_type: str | None = None
    event_rowid: int | None = None
    projection_name: str | None = MARKET_DATA_RECONCILE_PROJECTION_NAME
    message: str
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_rowid": self.event_rowid,
            "projection_name": self.projection_name,
            "message": self.message,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketDataProjectionReconcileRunResult:
    run_id: str
    status: str
    checked_event_count: int
    checked_price_tick_count: int
    checked_condition_event_count: int
    checked_tr_response_count: int
    outbox_job_count: int
    outbox_pending_count: int
    outbox_processing_count: int
    outbox_applied_count: int
    outbox_skipped_count: int
    outbox_error_count: int
    outbox_dead_letter_count: int
    missing_projection_count: int
    inline_projection_error_count: int
    outbox_error_issue_count: int
    duplicate_or_conflict_count: int
    synthetic_child_event_issue_count: int
    watermark_risk_count: int
    event_rowid_min: int | None
    event_rowid_max: int | None
    reason_codes: Sequence[str]
    issues: Sequence[MarketDataProjectionReconcileIssue]
    created_at: str
    no_trading_side_effects: bool = True
    append_only_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "checked_event_count": self.checked_event_count,
            "checked_price_tick_count": self.checked_price_tick_count,
            "checked_condition_event_count": self.checked_condition_event_count,
            "checked_tr_response_count": self.checked_tr_response_count,
            "outbox_job_count": self.outbox_job_count,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_applied_count": self.outbox_applied_count,
            "outbox_skipped_count": self.outbox_skipped_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "missing_projection_count": self.missing_projection_count,
            "inline_projection_error_count": self.inline_projection_error_count,
            "outbox_error_issue_count": self.outbox_error_issue_count,
            "duplicate_or_conflict_count": self.duplicate_or_conflict_count,
            "synthetic_child_event_issue_count": self.synthetic_child_event_issue_count,
            "watermark_risk_count": self.watermark_risk_count,
            "event_rowid_min": self.event_rowid_min,
            "event_rowid_max": self.event_rowid_max,
            "append_only_ready": self.append_only_ready,
            "reason_codes": list(self.reason_codes),
            "issues": [issue.to_dict() for issue in self.issues],
            "created_at": self.created_at,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_market_data_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int | None = None,
    min_event_rowid: int | None = None,
    max_event_rowid: int | None = None,
    persist: bool = True,
) -> MarketDataProjectionReconcileRunResult:
    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(
        limit or resolved_settings.market_data_projection_reconcile_limit
    )
    run_id = new_message_id("market_data_projection_reconcile")
    created_at = datetime_to_wire(utc_now())
    events = _select_market_data_events(
        connection,
        limit=bounded_limit,
        min_event_rowid=min_event_rowid,
        max_event_rowid=max_event_rowid,
    )
    issues: list[MarketDataProjectionReconcileIssue] = []
    counters: Counter[str] = Counter()
    outbox_counts: Counter[str] = Counter()
    watermark = _market_data_watermark_rowid(connection)

    for row in events:
        event_id = str(row["event_id"])
        event_type = str(row["event_type"])
        event_rowid = int(row["rowid"])
        payload = _json_object(row["payload_json"])
        counters["checked_event_count"] += 1
        counters[f"checked_{event_type}_count"] += 1

        inline_errors = _projection_errors(connection, event_id)
        if inline_errors:
            counters["inline_projection_error_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity=_inline_error_severity(inline_errors),
                reason_code="MARKET_DATA_INLINE_PROJECTION_ERROR",
                event_id=event_id,
                event_type=event_type,
                event_rowid=event_rowid,
                message="market_data inline projection recorded an error",
                evidence={"errors": inline_errors},
            )

        outbox = _market_data_outbox_job(connection, event_id)
        artifact_exists = _market_data_artifact_exists(
            connection,
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            source_event=row,
            issues=issues,
            run_id=run_id,
        )
        if not artifact_exists and not inline_errors:
            if _missing_artifact_is_allowed_by_outbox_skip(outbox):
                pass
            elif _missing_artifact_is_allowed(event_type, payload):
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="WARN",
                    reason_code=_allowed_missing_reason(event_type, payload),
                    event_id=event_id,
                    event_type=event_type,
                    event_rowid=event_rowid,
                    message="market_data projection artifact is absent but allowed",
                    evidence={"event_type": event_type},
                )
            else:
                counters["missing_projection_count"] += 1
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="FAIL",
                    reason_code=_missing_projection_reason(event_type),
                    event_id=event_id,
                    event_type=event_type,
                    event_rowid=event_rowid,
                    message="accepted market_data event has no projection artifact or error",
                    evidence={"watermark_rowid": watermark},
                )
                if watermark is not None and event_rowid <= watermark:
                    counters["watermark_risk_count"] += 1
                    _add_issue(
                        issues,
                        run_id=run_id,
                        severity="FAIL",
                        reason_code="MARKET_DATA_WATERMARK_ADVANCED_WITH_MISSING_ARTIFACT",
                        event_id=event_id,
                        event_type=event_type,
                        event_rowid=event_rowid,
                        message=(
                            "market_data watermark passed an event without an artifact "
                            "or recorded projection error"
                        ),
                        evidence={"watermark_rowid": watermark},
                    )

        if outbox is None:
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_DATA_OUTBOX_JOB_MISSING",
                event_id=event_id,
                event_type=event_type,
                event_rowid=event_rowid,
                message="accepted market_data event has no market_data projection_outbox job",
                evidence={},
            )
        else:
            counters["outbox_job_count"] += 1
            outbox_status = str(outbox["status"]).upper()
            outbox_counts[outbox_status] += 1
            _check_outbox_status(
                outbox,
                row,
                settings=resolved_settings,
                run_id=run_id,
                issues=issues,
            )

        if event_type == "tr_response":
            before_count = len(issues)
            _check_synthetic_child_events(
                connection,
                row,
                payload=payload,
                run_id=run_id,
                issues=issues,
            )
            synthetic_issues = [
                issue
                for issue in issues[before_count:]
                if issue.reason_code.startswith("SYNTHETIC_CHILD_EVENT")
            ]
            counters["synthetic_child_event_issue_count"] += len(synthetic_issues)
            counters["duplicate_or_conflict_count"] += sum(
                1
                for issue in synthetic_issues
                if issue.reason_code == "SYNTHETIC_CHILD_EVENT_ID_REGRESSION"
            )

    _check_non_market_mutation_evidence(
        connection,
        min_event_rowid=_event_rowid_min(events),
        max_event_rowid=_event_rowid_max(events),
        run_id=run_id,
        issues=issues,
    )
    counters["outbox_error_issue_count"] = sum(
        1
        for issue in issues
        if issue.reason_code
        in {
            "MARKET_DATA_OUTBOX_ERROR",
            "MARKET_DATA_OUTBOX_DEAD_LETTER",
            "MARKET_DATA_OUTBOX_STALE_PROCESSING",
        }
    )
    counters["duplicate_or_conflict_count"] += sum(
        1
        for issue in issues
        if issue.reason_code == "NON_MARKET_PROJECTION_MUTATION_EVIDENCE"
    )
    reason_codes = sorted({issue.reason_code for issue in issues})
    status = _run_status(issues, checked_event_count=int(counters["checked_event_count"]))
    result = MarketDataProjectionReconcileRunResult(
        run_id=run_id,
        status=status,
        checked_event_count=int(counters["checked_event_count"]),
        checked_price_tick_count=int(counters["checked_price_tick_count"]),
        checked_condition_event_count=int(counters["checked_condition_event_count"]),
        checked_tr_response_count=int(counters["checked_tr_response_count"]),
        outbox_job_count=int(counters["outbox_job_count"]),
        outbox_pending_count=int(outbox_counts["PENDING"]),
        outbox_processing_count=int(outbox_counts["PROCESSING"]),
        outbox_applied_count=int(outbox_counts["APPLIED"]),
        outbox_skipped_count=int(outbox_counts["SKIPPED"]),
        outbox_error_count=int(outbox_counts["ERROR"]),
        outbox_dead_letter_count=int(outbox_counts["DEAD_LETTER"]),
        missing_projection_count=int(counters["missing_projection_count"]),
        inline_projection_error_count=int(counters["inline_projection_error_count"]),
        outbox_error_issue_count=int(counters["outbox_error_issue_count"]),
        duplicate_or_conflict_count=int(counters["duplicate_or_conflict_count"]),
        synthetic_child_event_issue_count=int(counters["synthetic_child_event_issue_count"]),
        watermark_risk_count=int(counters["watermark_risk_count"]),
        event_rowid_min=_event_rowid_min(events),
        event_rowid_max=_event_rowid_max(events),
        append_only_ready=status == "PASS",
        reason_codes=tuple(reason_codes),
        issues=tuple(issues),
        created_at=created_at,
    )
    if persist:
        persist_market_data_projection_reconcile_result(connection, result)
    return result


def persist_market_data_projection_reconcile_result(
    connection: sqlite3.Connection,
    result: MarketDataProjectionReconcileRunResult,
) -> None:
    payload = result.to_dict()
    summary = {
        key: value
        for key, value in payload.items()
        if key not in {"issues", "reason_codes"}
    }
    connection.execute(
        """
        INSERT INTO market_data_projection_reconcile_runs (
            run_id,
            status,
            checked_event_count,
            checked_price_tick_count,
            checked_condition_event_count,
            checked_tr_response_count,
            outbox_job_count,
            outbox_pending_count,
            outbox_processing_count,
            outbox_applied_count,
            outbox_skipped_count,
            outbox_error_count,
            outbox_dead_letter_count,
            missing_projection_count,
            inline_projection_error_count,
            outbox_error_issue_count,
            duplicate_or_conflict_count,
            synthetic_child_event_issue_count,
            watermark_risk_count,
            event_rowid_min,
            event_rowid_max,
            append_only_ready,
            reason_codes_json,
            summary_json,
            created_at,
            no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.status,
            result.checked_event_count,
            result.checked_price_tick_count,
            result.checked_condition_event_count,
            result.checked_tr_response_count,
            result.outbox_job_count,
            result.outbox_pending_count,
            result.outbox_processing_count,
            result.outbox_applied_count,
            result.outbox_skipped_count,
            result.outbox_error_count,
            result.outbox_dead_letter_count,
            result.missing_projection_count,
            result.inline_projection_error_count,
            result.outbox_error_issue_count,
            result.duplicate_or_conflict_count,
            result.synthetic_child_event_issue_count,
            result.watermark_risk_count,
            result.event_rowid_min,
            result.event_rowid_max,
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
            INSERT INTO market_data_projection_reconcile_issues (
                run_id,
                severity,
                reason_code,
                event_id,
                event_type,
                event_rowid,
                projection_name,
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
                issue.event_rowid,
                issue.projection_name,
                issue.message,
                canonical_json(issue.evidence),
                result.created_at,
            ),
        )
    connection.commit()


def get_latest_market_data_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    issue_limit: int = 20,
) -> dict[str, Any]:
    run = connection.execute(
        """
        SELECT *
        FROM market_data_projection_reconcile_runs
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
            FROM market_data_projection_reconcile_issues
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
        FROM market_data_projection_reconcile_issues
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


def _select_market_data_events(
    connection: sqlite3.Connection,
    *,
    limit: int,
    min_event_rowid: int | None,
    max_event_rowid: int | None,
) -> list[sqlite3.Row]:
    clauses = [
        "status = 'ACCEPTED'",
        "event_type IN ('price_tick', 'condition_event', 'tr_response')",
    ]
    params: list[Any] = []
    if min_event_rowid is not None:
        clauses.append("rowid >= ?")
        params.append(int(min_event_rowid))
    if max_event_rowid is not None:
        clauses.append("rowid <= ?")
        params.append(int(max_event_rowid))
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT rowid, event_id, event_type, source, event_ts, received_at, payload_json
        FROM gateway_events
        WHERE {" AND ".join(clauses)}
        ORDER BY rowid DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return sorted(rows, key=lambda row: int(row["rowid"]))


def _market_data_artifact_exists(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    source_event: Mapping[str, Any],
    issues: list[MarketDataProjectionReconcileIssue],
    run_id: str,
) -> bool:
    if event_type == "price_tick":
        sample = connection.execute(
            "SELECT event_id FROM market_tick_samples WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if sample is None:
            return False
        _check_latest_tick(
            connection,
            payload=payload,
            source_event=source_event,
            run_id=run_id,
            issues=issues,
        )
        return True
    if event_type == "condition_event":
        return (
            connection.execute(
                "SELECT 1 FROM market_condition_signals WHERE event_id = ? LIMIT 1",
                (event_id,),
            ).fetchone()
            is not None
        )
    if event_type == "tr_response":
        if _tr_response_rows(payload):
            return (
                connection.execute(
                    "SELECT 1 FROM market_tr_snapshots WHERE event_id = ? LIMIT 1",
                    (event_id,),
                ).fetchone()
                is not None
            )
        return True
    return False


def _check_latest_tick(
    connection: sqlite3.Connection,
    *,
    payload: Mapping[str, Any],
    source_event: Mapping[str, Any],
    run_id: str,
    issues: list[MarketDataProjectionReconcileIssue],
) -> None:
    try:
        code = validate_stock_code(payload.get("code"))
        exchange = _payload_exchange(payload)
    except ValueError:
        return
    latest = connection.execute(
        """
        SELECT event_id, event_ts
        FROM market_ticks_latest
        WHERE code = ? AND exchange = ?
        """,
        (code, exchange),
    ).fetchone()
    if latest is None:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_DATA_LATEST_TICK_MISSING",
            event_id=str(source_event["event_id"]),
            event_type=str(source_event["event_type"]),
            event_rowid=int(source_event["rowid"]),
            message="market_tick_samples exists but market_ticks_latest row is missing",
            evidence={"code": code, "exchange": exchange},
        )
        return
    if _timestamp_before(str(latest["event_ts"]), str(source_event["event_ts"])):
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_DATA_LATEST_TICK_BEHIND_EVENT",
            event_id=str(source_event["event_id"]),
            event_type=str(source_event["event_type"]),
            event_rowid=int(source_event["rowid"]),
            message="market_ticks_latest timestamp is behind the accepted price_tick event",
            evidence={
                "code": code,
                "exchange": exchange,
                "latest_event_id": latest["event_id"],
                "latest_event_ts": latest["event_ts"],
                "event_ts": source_event["event_ts"],
            },
        )


def _check_outbox_status(
    outbox: Mapping[str, Any],
    event: Mapping[str, Any],
    *,
    settings: Settings,
    run_id: str,
    issues: list[MarketDataProjectionReconcileIssue],
) -> None:
    status = str(outbox["status"]).upper()
    if status == "PENDING":
        age = _age_seconds(outbox["created_at"])
        severity = (
            "FAIL"
            if age is not None and age >= settings.projection_outbox_processing_ttl_sec
            else "WARN"
        )
        _add_issue(
            issues,
            run_id=run_id,
            severity=severity,
            reason_code="MARKET_DATA_OUTBOX_BACKLOG",
            event_id=str(event["event_id"]),
            event_type=str(event["event_type"]),
            event_rowid=int(event["rowid"]),
            message="market_data projection_outbox job is still pending",
            evidence={"outbox_id": outbox["outbox_id"], "age_sec": age},
        )
    elif status == "PROCESSING":
        age = _age_seconds(outbox["locked_at"])
        severity = (
            "FAIL"
            if age is not None and age >= settings.projection_outbox_processing_ttl_sec
            else "WARN"
        )
        _add_issue(
            issues,
            run_id=run_id,
            severity=severity,
            reason_code=(
                "MARKET_DATA_OUTBOX_STALE_PROCESSING"
                if severity == "FAIL"
                else "MARKET_DATA_OUTBOX_PROCESSING"
            ),
            event_id=str(event["event_id"]),
            event_type=str(event["event_type"]),
            event_rowid=int(event["rowid"]),
            message="market_data projection_outbox job is processing",
            evidence={"outbox_id": outbox["outbox_id"], "locked_age_sec": age},
        )
    elif status == "ERROR":
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="MARKET_DATA_OUTBOX_ERROR",
            event_id=str(event["event_id"]),
            event_type=str(event["event_type"]),
            event_rowid=int(event["rowid"]),
            message="market_data projection_outbox job is ERROR",
            evidence={"outbox_id": outbox["outbox_id"], "last_error": outbox["last_error"]},
        )
    elif status == "DEAD_LETTER":
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="MARKET_DATA_OUTBOX_DEAD_LETTER",
            event_id=str(event["event_id"]),
            event_type=str(event["event_type"]),
            event_rowid=int(event["rowid"]),
            message="market_data projection_outbox job is DEAD_LETTER",
            evidence={"outbox_id": outbox["outbox_id"], "last_error": outbox["last_error"]},
        )
    elif status == "SKIPPED" and not _outbox_skip_is_justified(outbox):
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_DATA_OUTBOX_SKIPPED",
            event_id=str(event["event_id"]),
            event_type=str(event["event_type"]),
            event_rowid=int(event["rowid"]),
            message="market_data projection_outbox job is skipped",
            evidence={
                "outbox_id": outbox["outbox_id"],
                "metadata": _json_object(outbox["metadata_json"]),
            },
        )


def _check_synthetic_child_events(
    connection: sqlite3.Connection,
    event: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    run_id: str,
    issues: list[MarketDataProjectionReconcileIssue],
) -> None:
    event_id = str(event["event_id"])
    tick_payloads = candidate_quote_refresh_tick_payloads_from_tr_response(
        payload,
        event_ts=str(event["event_ts"]),
    )
    if len(tick_payloads) < 2:
        return
    parent_sample = connection.execute(
        "SELECT event_id FROM market_tick_samples WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    if parent_sample is not None:
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="SYNTHETIC_CHILD_EVENT_ID_REGRESSION",
            event_id=event_id,
            event_type="tr_response",
            event_rowid=int(event["rowid"]),
            message="synthetic price_tick sample reused the parent tr_response event_id",
            evidence={"parent_event_id": event_id},
        )
    expected_ids = _expected_synthetic_child_event_ids(event_id, tick_payloads)
    samples = [
        dict(row)
        for row in connection.execute(
            """
            SELECT event_id, metadata_json
            FROM market_tick_samples
            WHERE json_extract(metadata_json, '$.parent_event_id') = ?
            """,
            (event_id,),
        ).fetchall()
    ]
    sample_ids = [str(row["event_id"]) for row in samples]
    if len(sample_ids) != len(set(sample_ids)):
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="SYNTHETIC_CHILD_EVENT_ID_REGRESSION",
            event_id=event_id,
            event_type="tr_response",
            event_rowid=int(event["rowid"]),
            message="synthetic price_tick child event ids are not unique",
            evidence={"sample_event_ids": sample_ids},
        )
    missing = sorted(set(expected_ids) - set(sample_ids))
    if missing:
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="SYNTHETIC_CHILD_EVENT_SAMPLE_MISSING",
            event_id=event_id,
            event_type="tr_response",
            event_rowid=int(event["rowid"]),
            message="candidate quote refresh tr_response is missing synthetic child samples",
            evidence={"expected_event_ids": expected_ids, "missing_event_ids": missing},
        )
    for sample in samples:
        metadata = _json_object(sample["metadata_json"])
        if (
            metadata.get("parent_event_id") != event_id
            or metadata.get("synthetic_event") is not True
            or metadata.get("row_index") is None
        ):
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="SYNTHETIC_CHILD_EVENT_METADATA_MISSING",
                event_id=event_id,
                event_type="tr_response",
                event_rowid=int(event["rowid"]),
                message="synthetic price_tick metadata is missing required parent fields",
                evidence={"sample_event_id": sample["event_id"], "metadata": metadata},
            )


def _check_non_market_mutation_evidence(
    connection: sqlite3.Connection,
    *,
    min_event_rowid: int | None,
    max_event_rowid: int | None,
    run_id: str,
    issues: list[MarketDataProjectionReconcileIssue],
) -> None:
    if min_event_rowid is None or max_event_rowid is None:
        return
    rows = connection.execute(
        """
        SELECT outbox_id, projection_name, event_id, event_type, event_rowid, metadata_json
        FROM projection_outbox
        WHERE event_rowid BETWEEN ? AND ?
            AND json_extract(metadata_json, '$.last_worker_evidence.mutated_projection_name')
                IS NOT NULL
            AND json_extract(metadata_json, '$.last_worker_evidence.mutated_projection_name')
                != 'market_data'
        LIMIT 20
        """,
        (min_event_rowid, max_event_rowid),
    ).fetchall()
    for row in rows:
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code="NON_MARKET_PROJECTION_MUTATION_EVIDENCE",
            event_id=row["event_id"],
            event_type=row["event_type"],
            event_rowid=row["event_rowid"],
            projection_name=row["projection_name"],
            message="outbox worker evidence shows a non-market_data projection mutation",
            evidence={
                "outbox_id": row["outbox_id"],
                "metadata": _json_object(row["metadata_json"]),
            },
        )


def _projection_errors(connection: sqlite3.Connection, event_id: str) -> list[dict[str, Any]]:
    return [
        {
            "id": int(row["id"]),
            "event_type": row["event_type"],
            "code": row["code"],
            "error_message": row["error_message"],
            "created_at": row["created_at"],
        }
        for row in connection.execute(
            """
            SELECT id, event_type, code, error_message, created_at
            FROM market_projection_errors
            WHERE event_id = ?
            ORDER BY id ASC
            """,
            (event_id,),
        ).fetchall()
    ]


def _market_data_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()


def _market_data_watermark_rowid(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        """
        SELECT last_event_rowid
        FROM projection_watermarks
        WHERE projection_name = 'market_data'
        """
    ).fetchone()
    if row is None:
        return None
    return int(row["last_event_rowid"])


def _expected_synthetic_child_event_ids(
    parent_event_id: str,
    tick_payloads: Sequence[Mapping[str, Any]],
) -> list[str]:
    ids: list[str] = []
    for row_index, payload in enumerate(tick_payloads):
        try:
            code = validate_stock_code(payload.get("code"))
        except ValueError:
            continue
        ids.append(
            f"{parent_event_id}:synthetic_price_tick:"
            f"{row_index}:{code}:{_payload_exchange(payload)}"
        )
    return ids


def _missing_artifact_is_allowed(event_type: str, payload: Mapping[str, Any]) -> bool:
    if event_type == "price_tick":
        return _payload_real_type(payload) in QUOTE_ONLY_REAL_TYPES
    if event_type == "tr_response":
        return not _tr_response_rows(payload)
    return False


def _allowed_missing_reason(event_type: str, payload: Mapping[str, Any]) -> str:
    if event_type == "price_tick":
        return "MARKET_DATA_PRICE_TICK_QUOTE_ONLY_SKIPPED"
    if event_type == "tr_response":
        return "MARKET_DATA_TR_RESPONSE_EMPTY_ROWS_SKIPPED"
    return "MARKET_DATA_PROJECTION_ARTIFACT_MISSING_ALLOWED"


def _missing_artifact_is_allowed_by_outbox_skip(
    outbox: Mapping[str, Any] | None,
) -> bool:
    if outbox is None:
        return False
    if str(outbox["status"]).upper() != "SKIPPED":
        return False
    return _outbox_skip_is_justified(outbox)


def _missing_projection_reason(event_type: str) -> str:
    return {
        "price_tick": "MARKET_DATA_PRICE_TICK_PROJECTION_MISSING",
        "condition_event": "MARKET_DATA_CONDITION_EVENT_PROJECTION_MISSING",
        "tr_response": "MARKET_DATA_TR_RESPONSE_PROJECTION_MISSING",
    }.get(event_type, "MARKET_DATA_PROJECTION_MISSING")


def _inline_error_severity(errors: Sequence[Mapping[str, Any]]) -> str:
    messages = " ".join(str(item.get("error_message") or "") for item in errors).upper()
    if "UNIQUE CONSTRAINT" in messages or "PRIMARY KEY" in messages:
        return "FAIL"
    return "WARN"


def _outbox_skip_is_justified(outbox: Mapping[str, Any]) -> bool:
    metadata = _json_object(outbox["metadata_json"])
    evidence = metadata.get("last_worker_evidence")
    if not isinstance(evidence, Mapping):
        return False
    reason = str(
        evidence.get("verification_reason") or evidence.get("reason") or ""
    ).upper()
    return any(
        token in reason
        for token in (
            "NO_ROWS",
            "QUOTE_ONLY",
            "OLDER_THAN_LATEST",
            "SKIPPED_SOURCE_NOT_ACCEPTED",
        )
    )


def _payload_exchange(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("exchange") is not None:
        return normalize_market_data_exchange(metadata.get("exchange"))
    if payload.get("exchange") is not None:
        return normalize_market_data_exchange(payload.get("exchange"))
    return "KRX"


def _payload_real_type(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return ""
    return str(metadata.get("real_type") or "")


def _tr_response_rows(payload: Mapping[str, Any]) -> list[Any]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return rows
    row = payload.get("row")
    return [row] if isinstance(row, Mapping) else []


def _timestamp_before(left: str, right: str) -> bool:
    try:
        return parse_timestamp(left, "left") < parse_timestamp(right, "right")
    except ValueError:
        return str(left) < str(right)


def _age_seconds(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return max((utc_now() - parse_timestamp(str(value), "timestamp")).total_seconds(), 0.0)
    except ValueError:
        return None


def _add_issue(
    issues: list[MarketDataProjectionReconcileIssue],
    *,
    run_id: str,
    severity: str,
    reason_code: str,
    message: str,
    event_id: str | None = None,
    event_type: str | None = None,
    event_rowid: int | None = None,
    projection_name: str | None = MARKET_DATA_RECONCILE_PROJECTION_NAME,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    normalized_severity = severity.upper()
    if normalized_severity not in _SEVERITY_RANK:
        normalized_severity = "WARN"
    issues.append(
        MarketDataProjectionReconcileIssue(
            issue_id=f"{run_id}:{len(issues) + 1}",
            severity=normalized_severity,
            reason_code=reason_code,
            event_id=event_id,
            event_type=event_type,
            event_rowid=event_rowid,
            projection_name=projection_name,
            message=message,
            evidence=normalize_payload(dict(evidence or {})),
        )
    )


def _run_status(
    issues: Sequence[MarketDataProjectionReconcileIssue],
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


def _bounded_limit(value: int) -> int:
    return min(max(int(value), 1), 5000)


def _event_rowid_min(rows: Sequence[Mapping[str, Any]]) -> int | None:
    if not rows:
        return None
    return min(int(row["rowid"]) for row in rows)


def _event_rowid_max(rows: Sequence[Mapping[str, Any]]) -> int | None:
    if not rows:
        return None
    return max(int(row["rowid"]) for row in rows)


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


def _run_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["append_only_ready"] = bool(payload["append_only_ready"])
    payload["no_trading_side_effects"] = bool(payload["no_trading_side_effects"])
    payload["reason_codes"] = _json_array(payload.pop("reason_codes_json", "[]"))
    payload["summary"] = _json_object(payload.pop("summary_json", "{}"))
    return payload


def _issue_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": str(row["id"]),
        "severity": row["severity"],
        "reason_code": row["reason_code"],
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "event_rowid": row["event_rowid"],
        "projection_name": row["projection_name"],
        "message": row["message"],
        "evidence": _json_object(row["evidence_json"]),
        "created_at": row["created_at"],
    }
