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
from services.market_index_service import (
    MARKET_INDEX_SOURCE_REALTIME,
    MARKET_INDEX_SOURCE_TR_BOOTSTRAP,
    classify_market_index_data_source,
    market_index_parser_status,
    market_index_parser_verified,
    market_index_payload_usability,
)
from services.market_index_tr_bootstrap import (
    MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION,
    MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
    bootstrap_event_from_row,
    inspect_market_index_tr_bootstrap_event,
)

PROJECTION_NAME_MARKET_INDEX = "market_index"
MARKET_INDEX_EVENT_TYPE = "market_index_tick"
REQUIRED_INDEX_CODES = frozenset({"KOSPI", "KOSDAQ"})
_STATUS_KEYS = {
    "PENDING": "outbox_pending_count",
    "PROCESSING": "outbox_processing_count",
    "APPLIED": "outbox_applied_count",
    "SKIPPED": "outbox_skipped_count",
    "ERROR": "outbox_error_count",
    "DEAD_LETTER": "outbox_dead_letter_count",
}


@dataclass(frozen=True, kw_only=True)
class MarketIndexProjectionReconcileIssue:
    issue_id: str
    severity: str
    reason_code: str
    message: str
    event_id: str | None = None
    index_code: str | None = None
    parser_status: str | None = None
    data_source: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "event_id": self.event_id,
            "index_code": self.index_code,
            "parser_status": self.parser_status,
            "data_source": self.data_source,
            "message": self.message,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketIndexProjectionReconcileRunResult:
    run_id: str
    status: str
    checked_event_count: int
    sample_count: int
    missing_sample_count: int
    projection_error_count: int
    outbox_job_count: int
    outbox_pending_count: int
    outbox_processing_count: int
    outbox_applied_count: int
    outbox_skipped_count: int
    outbox_error_count: int
    outbox_dead_letter_count: int
    parser_verified_count: int
    parser_unverified_count: int
    data_usable_count: int
    data_unusable_count: int
    realtime_source_count: int
    tr_bootstrap_source_count: int
    unknown_source_count: int
    observed_index_codes: Sequence[str]
    latest_event_id: str | None
    latest_event_ts: str | None
    data_usability_ready: bool
    parser_confidence_ready: bool
    append_only_ready: bool
    reason_codes: Sequence[str]
    issues: Sequence[MarketIndexProjectionReconcileIssue]
    created_at: str
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "checked_event_count": self.checked_event_count,
            "sample_count": self.sample_count,
            "missing_sample_count": self.missing_sample_count,
            "projection_error_count": self.projection_error_count,
            "outbox_job_count": self.outbox_job_count,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_applied_count": self.outbox_applied_count,
            "outbox_skipped_count": self.outbox_skipped_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "parser_verified_count": self.parser_verified_count,
            "parser_unverified_count": self.parser_unverified_count,
            "data_usable_count": self.data_usable_count,
            "data_unusable_count": self.data_unusable_count,
            "realtime_source_count": self.realtime_source_count,
            "tr_bootstrap_source_count": self.tr_bootstrap_source_count,
            "unknown_source_count": self.unknown_source_count,
            "observed_index_codes": list(self.observed_index_codes),
            "latest_event_id": self.latest_event_id,
            "latest_event_ts": self.latest_event_ts,
            "data_usability_ready": self.data_usability_ready,
            "parser_confidence_ready": self.parser_confidence_ready,
            "append_only_ready": self.append_only_ready,
            "reason_codes": list(self.reason_codes),
            "issues": [issue.to_dict() for issue in self.issues],
            "created_at": self.created_at,
            "read_only_projection": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_market_index_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 100,
    persist: bool = True,
) -> MarketIndexProjectionReconcileRunResult:
    resolved_settings = settings or load_settings()
    bounded_limit = min(max(int(limit), 1), 5000)
    run_id = new_message_id("market_index_projection_reconcile")
    created_at = datetime_to_wire(utc_now())
    events = _select_events(connection, limit=bounded_limit)
    latest_event = events[-1] if events else None
    issues: list[MarketIndexProjectionReconcileIssue] = []
    counters: Counter[str] = Counter()
    outbox_counts: Counter[str] = Counter()
    observed_index_codes: set[str] = set()
    outbox_rollout_rowid = _outbox_rollout_rowid(connection)

    if not events:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_INDEX_EVENT_MISSING",
            message="no accepted market-index realtime or TR bootstrap event was found",
        )

    for row in events:
        counters["checked_event_count"] += 1
        event_id = str(row["event_id"])
        stored_payload = _json_object(row["payload_json"])
        payload = stored_payload
        bootstrap_inspection = None
        if str(row["event_type"]).lower() == "tr_response":
            bootstrap_event = bootstrap_event_from_row(dict(row))
            bootstrap_inspection = inspect_market_index_tr_bootstrap_event(
                bootstrap_event,
                settings=resolved_settings,
            )
            if bootstrap_inspection.valid and bootstrap_inspection.tick is not None:
                payload = bootstrap_inspection.tick.to_dict()
        parser_status = market_index_parser_status(payload)
        parser_verified = market_index_parser_verified(payload)
        data_source = classify_market_index_data_source(payload)
        usability = market_index_payload_usability(payload)
        index_code = _string_or_none(usability.get("index_code"))
        if index_code:
            observed_index_codes.add(index_code)

        if parser_verified:
            counters["parser_verified_count"] += 1
        else:
            counters["parser_unverified_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="INFO",
                reason_code="MARKET_INDEX_PARSER_UNVERIFIED",
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                message="parser confidence is not VERIFIED; data usability is evaluated separately",
            )

        if data_source == MARKET_INDEX_SOURCE_REALTIME:
            counters["realtime_source_count"] += 1
        elif data_source == MARKET_INDEX_SOURCE_TR_BOOTSTRAP:
            counters["tr_bootstrap_source_count"] += 1
            lineage_reasons = _tr_bootstrap_lineage_reasons(
                event_type=str(row["event_type"]),
                payload=payload,
                inspection=bootstrap_inspection,
            )
            for reason_code in lineage_reasons:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="FAIL",
                    reason_code=reason_code,
                    event_id=event_id,
                    index_code=index_code,
                    parser_status=parser_status,
                    data_source=data_source,
                    message="TR bootstrap event does not satisfy the v1 source lineage contract",
                    evidence={"stored_payload": stored_payload},
                )
        else:
            counters["unknown_source_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_INDEX_SOURCE_UNKNOWN",
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                message="market index event source is not explicitly REALTIME or TR_BOOTSTRAP",
            )

        outbox = _outbox_job(connection, event_id)
        if outbox is None:
            is_legacy = bool(
                outbox_rollout_rowid is not None
                and int(row["rowid"]) < outbox_rollout_rowid
            )
            _add_issue(
                issues,
                run_id=run_id,
                severity="INFO" if is_legacy else "FAIL",
                reason_code=(
                    "MARKET_INDEX_LEGACY_EVENT_WITHOUT_OUTBOX"
                    if is_legacy
                    else "MARKET_INDEX_OUTBOX_JOB_MISSING"
                ),
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                message="market_index projection_outbox job is missing",
                evidence={
                    "event_rowid": int(row["rowid"]),
                    "outbox_rollout_rowid": outbox_rollout_rowid,
                },
            )
        else:
            counters["outbox_job_count"] += 1
            outbox_status = str(outbox["status"]).upper()
            outbox_counts[outbox_status] += 1
            _check_outbox(
                issues,
                run_id=run_id,
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                outbox=outbox,
                max_age_sec=resolved_settings.gateway_market_index_append_only_reconcile_max_age_sec,
            )

        projection_error = _projection_error(connection, event_id)
        sample = _sample(connection, event_id)
        if projection_error is not None:
            counters["projection_error_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_INDEX_PROJECTION_ERROR",
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                message="market index projection recorded an error",
                evidence=dict(projection_error),
            )
        if sample is None:
            counters["missing_sample_count"] += 1
            if projection_error is None:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="FAIL",
                    reason_code="MARKET_INDEX_SAMPLE_MISSING",
                    event_id=event_id,
                    index_code=index_code,
                    parser_status=parser_status,
                    data_source=data_source,
                    message="accepted event has no market_index_tick_samples artifact",
                )
        else:
            counters["sample_count"] += 1
            if index_code and str(sample["index_code"]) != index_code:
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="FAIL",
                    reason_code="MARKET_INDEX_SAMPLE_CODE_MISMATCH",
                    event_id=event_id,
                    index_code=index_code,
                    parser_status=parser_status,
                    data_source=data_source,
                    message="sample index_code differs from the source event",
                    evidence={"sample_index_code": sample["index_code"]},
                )

        data_usable = bool(usability.get("data_usable")) and sample is not None
        if data_usable:
            counters["data_usable_count"] += 1
        else:
            counters["data_unusable_count"] += 1
            _add_issue(
                issues,
                run_id=run_id,
                severity="FAIL",
                reason_code="MARKET_INDEX_DATA_NOT_USABLE",
                event_id=event_id,
                index_code=index_code,
                parser_status=parser_status,
                data_source=data_source,
                message="market index payload or projection artifact is not usable",
                evidence=usability,
            )

    missing_codes = sorted(REQUIRED_INDEX_CODES - observed_index_codes)
    if events and missing_codes:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_INDEX_REQUIRED_CODE_COVERAGE_MISSING",
            message="reconcile window does not contain every required KRX index code",
            evidence={"missing_index_codes": missing_codes},
        )

    checked_count = int(counters["checked_event_count"])
    data_usability_ready = bool(
        checked_count > 0
        and int(counters["data_usable_count"]) == checked_count
        and int(counters["data_unusable_count"]) == 0
        and not missing_codes
    )
    parser_confidence_ready = bool(
        checked_count > 0
        and int(counters["parser_verified_count"]) == checked_count
        and int(counters["parser_unverified_count"]) == 0
    )
    status = _run_status(issues, checked_event_count=checked_count)
    append_only_ready = bool(
        status == "PASS"
        and int(counters["missing_sample_count"]) == 0
        and int(counters["projection_error_count"]) == 0
        and int(outbox_counts["ERROR"]) == 0
        and int(outbox_counts["DEAD_LETTER"]) == 0
        and int(outbox_counts["SKIPPED"]) == 0
        and data_usability_ready
        and (
            parser_confidence_ready
            or not resolved_settings.gateway_market_index_append_only_require_parser_verified
        )
    )
    result = MarketIndexProjectionReconcileRunResult(
        run_id=run_id,
        status=status,
        checked_event_count=checked_count,
        sample_count=int(counters["sample_count"]),
        missing_sample_count=int(counters["missing_sample_count"]),
        projection_error_count=int(counters["projection_error_count"]),
        outbox_job_count=int(counters["outbox_job_count"]),
        outbox_pending_count=int(outbox_counts["PENDING"]),
        outbox_processing_count=int(outbox_counts["PROCESSING"]),
        outbox_applied_count=int(outbox_counts["APPLIED"]),
        outbox_skipped_count=int(outbox_counts["SKIPPED"]),
        outbox_error_count=int(outbox_counts["ERROR"]),
        outbox_dead_letter_count=int(outbox_counts["DEAD_LETTER"]),
        parser_verified_count=int(counters["parser_verified_count"]),
        parser_unverified_count=int(counters["parser_unverified_count"]),
        data_usable_count=int(counters["data_usable_count"]),
        data_unusable_count=int(counters["data_unusable_count"]),
        realtime_source_count=int(counters["realtime_source_count"]),
        tr_bootstrap_source_count=int(counters["tr_bootstrap_source_count"]),
        unknown_source_count=int(counters["unknown_source_count"]),
        observed_index_codes=tuple(sorted(observed_index_codes)),
        latest_event_id=None if latest_event is None else str(latest_event["event_id"]),
        latest_event_ts=None if latest_event is None else str(latest_event["event_ts"]),
        data_usability_ready=data_usability_ready,
        parser_confidence_ready=parser_confidence_ready,
        append_only_ready=append_only_ready,
        reason_codes=tuple(sorted({issue.reason_code for issue in issues})),
        issues=tuple(issues),
        created_at=created_at,
    )
    if persist:
        persist_market_index_projection_reconcile_result(connection, result)
    return result


def persist_market_index_projection_reconcile_result(
    connection: sqlite3.Connection,
    result: MarketIndexProjectionReconcileRunResult,
) -> None:
    payload = result.to_dict()
    summary = {
        key: value
        for key, value in payload.items()
        if key not in {"issues", "reason_codes"}
    }
    connection.execute(
        """
        INSERT INTO market_index_projection_reconcile_runs (
            run_id, status, checked_event_count, sample_count, missing_sample_count,
            projection_error_count, outbox_job_count, outbox_pending_count,
            outbox_processing_count, outbox_applied_count, outbox_skipped_count,
            outbox_error_count, outbox_dead_letter_count, parser_verified_count,
            parser_unverified_count, data_usable_count, data_unusable_count,
            realtime_source_count, tr_bootstrap_source_count, unknown_source_count,
            latest_event_id, latest_event_ts, data_usability_ready,
            parser_confidence_ready, append_only_ready, reason_codes_json,
            summary_json, created_at, no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.status,
            result.checked_event_count,
            result.sample_count,
            result.missing_sample_count,
            result.projection_error_count,
            result.outbox_job_count,
            result.outbox_pending_count,
            result.outbox_processing_count,
            result.outbox_applied_count,
            result.outbox_skipped_count,
            result.outbox_error_count,
            result.outbox_dead_letter_count,
            result.parser_verified_count,
            result.parser_unverified_count,
            result.data_usable_count,
            result.data_unusable_count,
            result.realtime_source_count,
            result.tr_bootstrap_source_count,
            result.unknown_source_count,
            result.latest_event_id,
            result.latest_event_ts,
            int(result.data_usability_ready),
            int(result.parser_confidence_ready),
            int(result.append_only_ready),
            json.dumps(list(result.reason_codes), ensure_ascii=False, sort_keys=True),
            canonical_json(summary),
            result.created_at,
            int(result.no_trading_side_effects),
        ),
    )
    for issue in result.issues:
        connection.execute(
            """
            INSERT INTO market_index_projection_reconcile_issues (
                run_id, severity, reason_code, event_id, index_code, parser_status,
                data_source, message, evidence_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                issue.severity,
                issue.reason_code,
                issue.event_id,
                issue.index_code,
                issue.parser_status,
                issue.data_source,
                issue.message,
                canonical_json(issue.evidence),
                result.created_at,
            ),
        )
    connection.commit()


def get_latest_market_index_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    issue_limit: int = 20,
) -> dict[str, Any]:
    run = connection.execute(
        """
        SELECT *
        FROM market_index_projection_reconcile_runs
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
    issues = connection.execute(
        """
        SELECT *
        FROM market_index_projection_reconcile_issues
        WHERE run_id = ?
        ORDER BY CASE severity WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, id ASC
        LIMIT ?
        """,
        (run["run_id"], min(max(int(issue_limit), 1), 200)),
    ).fetchall()
    summary_rows = connection.execute(
        """
        SELECT severity, reason_code, COUNT(*) AS count
        FROM market_index_projection_reconcile_issues
        WHERE run_id = ?
        GROUP BY severity, reason_code
        ORDER BY severity, reason_code
        """,
        (run["run_id"],),
    ).fetchall()
    return {
        "latest_run": _run_row_to_dict(run),
        "issues": [_issue_row_to_dict(row) for row in issues],
        "issue_summary": {
            f"{row['severity']}:{row['reason_code']}": int(row["count"])
            for row in summary_rows
        },
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _select_events(connection: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT rowid, event_id, event_type, event_ts, received_at, source,
               command_id, idempotency_key, payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED'
          AND (
              event_type = 'market_index_tick'
              OR (
                  event_type = 'tr_response'
                  AND lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
                      LIKE 'market_index_tr_bootstrap:%'
              )
          )
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return sorted(rows, key=lambda row: int(row["rowid"]))


def _tr_bootstrap_lineage_reasons(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    inspection: Any,
) -> list[str]:
    if str(event_type).lower() == "tr_response":
        if inspection is None or not bool(inspection.valid):
            return ["MARKET_INDEX_TR_BOOTSTRAP_LINEAGE_INVALID"]
        return []
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return ["MARKET_INDEX_TR_BOOTSTRAP_LINEAGE_INVALID"]
    source = str(
        metadata.get("projection_source") or metadata.get("source") or ""
    ).strip().upper()
    contract = str(metadata.get("tr_bootstrap_contract_version") or "").strip().lower()
    parent_event_id = str(metadata.get("parent_event_id") or "").strip()
    request_id = str(metadata.get("request_id") or "").strip().lower()
    if (
        source != MARKET_INDEX_TR_BOOTSTRAP_SOURCE
        or contract != MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION
        or not parent_event_id
        or not request_id.startswith("market_index_tr_bootstrap:")
    ):
        return ["MARKET_INDEX_TR_BOOTSTRAP_LINEAGE_INVALID"]
    return []


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, created_at, updated_at, last_error, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _outbox_rollout_rowid(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        """
        SELECT MIN(event_rowid) AS rollout_rowid
        FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_rowid IS NOT NULL
        """
    ).fetchone()
    if row is None or row["rollout_rowid"] is None:
        return None
    return int(row["rollout_rowid"])


def _sample(connection: sqlite3.Connection, event_id: str) -> Mapping[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM market_index_tick_samples WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _projection_error(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT reason_code, error_message, created_at
        FROM market_index_projection_errors
        WHERE event_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _check_outbox(
    issues: list[MarketIndexProjectionReconcileIssue],
    *,
    run_id: str,
    event_id: str,
    index_code: str | None,
    parser_status: str,
    data_source: str,
    outbox: Mapping[str, Any],
    max_age_sec: int,
) -> None:
    status = str(outbox["status"]).upper()
    if status in {"PENDING", "PROCESSING"}:
        age = _age_seconds(outbox["created_at"] if status == "PENDING" else outbox["updated_at"])
        severity = "FAIL" if age is not None and age > max_age_sec else "WARN"
        _add_issue(
            issues,
            run_id=run_id,
            severity=severity,
            reason_code=(
                "MARKET_INDEX_OUTBOX_STALE"
                if severity == "FAIL"
                else "MARKET_INDEX_OUTBOX_PENDING_WITHIN_SLA"
            ),
            event_id=event_id,
            index_code=index_code,
            parser_status=parser_status,
            data_source=data_source,
            message="market_index outbox job is not terminal",
            evidence={"status": status, "age_sec": age},
        )
    elif status in {"ERROR", "DEAD_LETTER", "SKIPPED"}:
        _add_issue(
            issues,
            run_id=run_id,
            severity="FAIL",
            reason_code=f"MARKET_INDEX_OUTBOX_{status}",
            event_id=event_id,
            index_code=index_code,
            parser_status=parser_status,
            data_source=data_source,
            message=f"market_index outbox job is {status}",
            evidence={"last_error": outbox.get("last_error")},
        )


def _add_issue(
    issues: list[MarketIndexProjectionReconcileIssue],
    *,
    run_id: str,
    severity: str,
    reason_code: str,
    message: str,
    event_id: str | None = None,
    index_code: str | None = None,
    parser_status: str | None = None,
    data_source: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    issues.append(
        MarketIndexProjectionReconcileIssue(
            issue_id=f"{run_id}:{len(issues) + 1}",
            severity=severity.upper(),
            reason_code=reason_code,
            event_id=event_id,
            index_code=index_code,
            parser_status=parser_status,
            data_source=data_source,
            message=message,
            evidence=dict(evidence or {}),
        )
    )


def _run_status(
    issues: Sequence[MarketIndexProjectionReconcileIssue],
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


def _age_seconds(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(str(value), "timestamp")
    except ValueError:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        loaded = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_array(value: object) -> list[Any]:
    try:
        loaded = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _string_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _run_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "data_usability_ready",
        "parser_confidence_ready",
        "append_only_ready",
        "no_trading_side_effects",
    ):
        payload[key] = bool(payload[key])
    payload["reason_codes"] = _json_array(payload.pop("reason_codes_json", "[]"))
    payload["summary"] = _json_object(payload.pop("summary_json", "{}"))
    payload["observed_index_codes"] = list(
        payload["summary"].get("observed_index_codes") or []
    )
    return payload


def _issue_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": str(row["id"]),
        "severity": row["severity"],
        "reason_code": row["reason_code"],
        "event_id": row["event_id"],
        "index_code": row["index_code"],
        "parser_status": row["parser_status"],
        "data_source": row["data_source"],
        "message": row["message"],
        "evidence": _json_object(row["evidence_json"]),
        "created_at": row["created_at"],
    }
