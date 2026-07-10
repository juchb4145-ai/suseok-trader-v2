from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_context_service import get_market_context_status

PROJECTION_NAME_MARKET_REGIME = "market_regime"
MARKET_INDEX_EVENT_TYPE = "market_index_tick"
REQUIRED_INDEX_CODES: tuple[str, ...] = ("KOSPI", "KOSDAQ")
_STATUS_KEYS = {
    "PENDING": "outbox_pending_count",
    "PROCESSING": "outbox_processing_count",
    "APPLIED": "outbox_applied_count",
    "SKIPPED": "outbox_skipped_count",
    "ERROR": "outbox_error_count",
    "DEAD_LETTER": "outbox_dead_letter_count",
}


@dataclass(frozen=True, kw_only=True)
class MarketRegimeProjectionReconcileIssue:
    issue_id: str
    severity: str
    reason_code: str
    message: str
    event_id: str | None = None
    index_code: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "message": self.message,
            "event_id": self.event_id,
            "index_code": self.index_code,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketRegimeProjectionReconcileRunResult:
    run_id: str
    status: str
    checked_event_count: int
    observed_index_count: int
    outbox_job_count: int
    outbox_pending_count: int
    outbox_processing_count: int
    outbox_applied_count: int
    outbox_skipped_count: int
    outbox_error_count: int
    outbox_dead_letter_count: int
    event_linked_regime_count: int
    context_snapshot_count: int
    latest_event_id: str | None
    latest_event_ts: str | None
    latest_context_watermark_hash: str | None
    latest_context_regime_snapshot_id: str | None
    latest_event_covered: bool
    context_ready: bool
    append_only_ready: bool
    reason_codes: Sequence[str]
    issues: Sequence[MarketRegimeProjectionReconcileIssue]
    context_status: Mapping[str, Any]
    created_at: str
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "checked_event_count": self.checked_event_count,
            "observed_index_count": self.observed_index_count,
            "outbox_job_count": self.outbox_job_count,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_applied_count": self.outbox_applied_count,
            "outbox_skipped_count": self.outbox_skipped_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "event_linked_regime_count": self.event_linked_regime_count,
            "context_snapshot_count": self.context_snapshot_count,
            "latest_event_id": self.latest_event_id,
            "latest_event_ts": self.latest_event_ts,
            "latest_context_watermark_hash": self.latest_context_watermark_hash,
            "latest_context_regime_snapshot_id": (
                self.latest_context_regime_snapshot_id
            ),
            "latest_event_covered": self.latest_event_covered,
            "context_ready": self.context_ready,
            "append_only_ready": self.append_only_ready,
            "reason_codes": list(self.reason_codes),
            "issues": [issue.to_dict() for issue in self.issues],
            "context_status": dict(self.context_status),
            "created_at": self.created_at,
            "read_only_projection": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_market_regime_projection_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 100,
    persist: bool = True,
) -> MarketRegimeProjectionReconcileRunResult:
    resolved_settings = settings or load_settings()
    bounded_limit = min(max(int(limit), 1), 5000)
    run_id = new_message_id("market_regime_projection_reconcile")
    created_at = datetime_to_wire(utc_now())
    events = _select_events(connection, limit=bounded_limit)
    latest_event = events[-1] if events else None
    latest_by_code = _latest_events_by_code(events)
    outbox_rollout_rowid = _outbox_rollout_rowid(connection)
    issues: list[MarketRegimeProjectionReconcileIssue] = []
    outbox_counts: Counter[str] = Counter()
    outbox_job_count = 0
    event_linked_regime_count = 0

    if not events:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REGIME_SOURCE_EVENT_MISSING",
            message="no accepted market_index_tick event was found",
        )

    for event in events:
        event_id = str(event["event_id"])
        index_code = _event_index_code(event)
        if not _market_index_sample_exists(connection, event_id):
            _add_issue(
                issues,
                run_id=run_id,
                severity="ERROR",
                reason_code="MARKET_REGIME_INDEX_DEPENDENCY_MISSING",
                message="market index sample is missing for regime source event",
                event_id=event_id,
                index_code=index_code,
            )
        if _event_linked_regime_exists(connection, event_id):
            event_linked_regime_count += 1
        outbox = _outbox_job(connection, event_id)
        if outbox is None:
            if (
                outbox_rollout_rowid is None
                or int(event["event_rowid"]) >= outbox_rollout_rowid
            ):
                _add_issue(
                    issues,
                    run_id=run_id,
                    severity="ERROR",
                    reason_code="MARKET_REGIME_OUTBOX_MISSING",
                    message="market_regime outbox job is missing",
                    event_id=event_id,
                    index_code=index_code,
                )
            continue
        outbox_job_count += 1
        outbox_status = str(outbox["status"])
        outbox_counts[outbox_status] += 1
        if outbox_status in {"ERROR", "DEAD_LETTER"}:
            _add_issue(
                issues,
                run_id=run_id,
                severity="ERROR",
                reason_code=f"MARKET_REGIME_OUTBOX_{outbox_status}",
                message=f"market_regime outbox is {outbox_status}",
                event_id=event_id,
                index_code=index_code,
            )
        elif outbox_status == "SKIPPED":
            _add_issue(
                issues,
                run_id=run_id,
                severity="WARN",
                reason_code="MARKET_REGIME_OUTBOX_SKIPPED",
                message="market_regime outbox was skipped",
                event_id=event_id,
                index_code=index_code,
            )

    missing_codes = [
        code for code in REQUIRED_INDEX_CODES if code not in latest_by_code
    ]
    if missing_codes:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REGIME_REQUIRED_INDEX_MISSING",
            message="required KOSPI/KOSDAQ source coverage is incomplete",
            evidence={"missing_index_codes": missing_codes},
        )

    context_status = get_market_context_status(connection, settings=resolved_settings)
    latest_context = _mapping(context_status.get("latest"))
    latest_event_covered = _latest_events_covered(latest_by_code, latest_context)
    structural_context_ready = _structural_context_ready(context_status)
    context_ready = bool(
        structural_context_ready and context_status.get("status") == "PASS"
    )
    if not structural_context_ready:
        _add_issue(
            issues,
            run_id=run_id,
            severity="ERROR",
            reason_code="MARKET_REGIME_CONTEXT_STRUCTURE_INVALID",
            message="common market context pair or source-regime lineage is invalid",
            evidence={"context_status": context_status},
        )
    elif not context_ready:
        _add_issue(
            issues,
            run_id=run_id,
            severity="WARN",
            reason_code="MARKET_REGIME_CONTEXT_NOT_TRADING_READY",
            message="common market context is stale, unverified, or data-unusable",
            evidence={
                "stale_markets": context_status.get("stale_markets") or [],
                "parser_unverified_markets": (
                    context_status.get("parser_unverified_markets") or []
                ),
                "data_unusable_markets": context_status.get("data_unusable_markets")
                or [],
            },
        )
    if latest_by_code and not latest_event_covered:
        _add_issue(
            issues,
            run_id=run_id,
            severity="ERROR",
            reason_code="MARKET_REGIME_CONTEXT_WATERMARK_BEHIND",
            message="latest common context does not cover latest index events",
            evidence={
                "latest_event_ids": {
                    code: event["event_id"] for code, event in latest_by_code.items()
                },
            },
        )

    status = _status_from_issues(issues)
    append_only_ready = bool(
        status == "PASS"
        and context_ready
        and latest_event_covered
        and len(latest_by_code) == len(REQUIRED_INDEX_CODES)
        and not outbox_counts["ERROR"]
        and not outbox_counts["DEAD_LETTER"]
    )
    first_context = next(
        (value for value in latest_context.values() if isinstance(value, Mapping)),
        {},
    )
    result = MarketRegimeProjectionReconcileRunResult(
        run_id=run_id,
        status=status,
        checked_event_count=len(events),
        observed_index_count=len(latest_by_code),
        outbox_job_count=outbox_job_count,
        outbox_pending_count=outbox_counts["PENDING"],
        outbox_processing_count=outbox_counts["PROCESSING"],
        outbox_applied_count=outbox_counts["APPLIED"],
        outbox_skipped_count=outbox_counts["SKIPPED"],
        outbox_error_count=outbox_counts["ERROR"],
        outbox_dead_letter_count=outbox_counts["DEAD_LETTER"],
        event_linked_regime_count=event_linked_regime_count,
        context_snapshot_count=int(context_status.get("snapshot_count") or 0),
        latest_event_id=None if latest_event is None else str(latest_event["event_id"]),
        latest_event_ts=None if latest_event is None else str(latest_event["event_ts"]),
        latest_context_watermark_hash=(
            str(first_context.get("source_watermark_hash"))
            if first_context.get("source_watermark_hash")
            else None
        ),
        latest_context_regime_snapshot_id=(
            str(first_context.get("source_regime_snapshot_id"))
            if first_context.get("source_regime_snapshot_id")
            else None
        ),
        latest_event_covered=latest_event_covered,
        context_ready=context_ready,
        append_only_ready=append_only_ready,
        reason_codes=tuple(dict.fromkeys(issue.reason_code for issue in issues)),
        issues=tuple(issues),
        context_status=context_status,
        created_at=created_at,
    )
    if persist:
        _persist_result(connection, result)
    return result


def get_latest_market_regime_projection_reconcile(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT * FROM market_regime_projection_reconcile_runs
        ORDER BY created_at DESC, rowid DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "latest_run": None,
            "issues": [],
            "read_only": True,
            "no_trading_side_effects": True,
        }
    run = _run_row_to_dict(row)
    issue_rows = connection.execute(
        """
        SELECT * FROM market_regime_projection_reconcile_issues
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run["run_id"],),
    ).fetchall()
    return {
        "latest_run": run,
        "issues": [_issue_row_to_dict(item) for item in issue_rows],
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _select_events(
    connection: sqlite3.Connection, *, limit: int
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT rowid AS event_rowid, event_id, event_ts, received_at, payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED' AND event_type = ?
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (MARKET_INDEX_EVENT_TYPE, limit),
    ).fetchall()
    return [_row_to_dict(row) for row in reversed(rows)]


def _latest_events_by_code(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for event in events:
        code = _event_index_code(event)
        if code in REQUIRED_INDEX_CODES:
            latest[code] = event
    return latest


def _event_index_code(event: Mapping[str, Any]) -> str:
    payload = _json_object(event.get("payload_json"))
    return str(payload.get("index_code") or "").strip().upper()


def _latest_events_covered(
    latest_by_code: Mapping[str, Mapping[str, Any]],
    latest_context: Mapping[str, Any],
) -> bool:
    if set(latest_by_code) != set(REQUIRED_INDEX_CODES):
        return False
    for code in REQUIRED_INDEX_CODES:
        context = _mapping(latest_context.get(code))
        watermark = _mapping(context.get("source_watermark"))
        index_watermark = _mapping(watermark.get(code))
        if str(index_watermark.get("event_id") or "") != str(
            latest_by_code[code].get("event_id") or ""
        ):
            return False
    return True


def _structural_context_ready(status: Mapping[str, Any]) -> bool:
    latest = _mapping(status.get("latest"))
    return bool(
        isinstance(latest.get("KOSPI"), Mapping)
        and isinstance(latest.get("KOSDAQ"), Mapping)
        and status.get("latest_watermark_coherent") is True
        and status.get("latest_regime_coherent") is True
        and int(status.get("regime_reference_missing_count") or 0) == 0
        and int(status.get("candidate_missing_snapshot_count") or 0) == 0
    )


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT * FROM projection_outbox
        WHERE projection_name = ? AND event_id = ?
        LIMIT 1
        """,
        (PROJECTION_NAME_MARKET_REGIME, event_id),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


def _outbox_rollout_rowid(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        """
        SELECT MIN(event.rowid) AS event_rowid
        FROM projection_outbox AS outbox
        JOIN gateway_events AS event ON event.event_id = outbox.event_id
        WHERE outbox.projection_name = ?
        """,
        (PROJECTION_NAME_MARKET_REGIME,),
    ).fetchone()
    return (
        None if row is None or row["event_rowid"] is None else int(row["event_rowid"])
    )


def _market_index_sample_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM market_index_tick_samples WHERE event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
        is not None
    )


def _event_linked_regime_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM market_regime_snapshots WHERE source_event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
        is not None
    )


def _persist_result(
    connection: sqlite3.Connection,
    result: MarketRegimeProjectionReconcileRunResult,
) -> None:
    summary = result.to_dict()
    connection.execute(
        """
        INSERT INTO market_regime_projection_reconcile_runs (
            run_id, status, checked_event_count, observed_index_count,
            outbox_job_count, outbox_pending_count, outbox_processing_count,
            outbox_applied_count, outbox_skipped_count, outbox_error_count,
            outbox_dead_letter_count, event_linked_regime_count,
            context_snapshot_count, latest_event_id, latest_event_ts,
            latest_context_watermark_hash, latest_context_regime_snapshot_id,
            latest_event_covered, context_ready, append_only_ready,
            reason_codes_json, summary_json, created_at, no_trading_side_effects
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.status,
            result.checked_event_count,
            result.observed_index_count,
            result.outbox_job_count,
            result.outbox_pending_count,
            result.outbox_processing_count,
            result.outbox_applied_count,
            result.outbox_skipped_count,
            result.outbox_error_count,
            result.outbox_dead_letter_count,
            result.event_linked_regime_count,
            result.context_snapshot_count,
            result.latest_event_id,
            result.latest_event_ts,
            result.latest_context_watermark_hash,
            result.latest_context_regime_snapshot_id,
            int(result.latest_event_covered),
            int(result.context_ready),
            int(result.append_only_ready),
            json.dumps(
                list(result.reason_codes),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            canonical_json(summary),
            result.created_at,
            int(result.no_trading_side_effects),
        ),
    )
    for issue in result.issues:
        connection.execute(
            """
            INSERT INTO market_regime_projection_reconcile_issues (
                run_id, severity, reason_code, event_id, index_code,
                message, evidence_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                issue.severity,
                issue.reason_code,
                issue.event_id,
                issue.index_code,
                issue.message,
                canonical_json(issue.evidence),
                result.created_at,
            ),
        )
    connection.commit()


def _add_issue(
    issues: list[MarketRegimeProjectionReconcileIssue],
    *,
    run_id: str,
    severity: str,
    reason_code: str,
    message: str,
    event_id: str | None = None,
    index_code: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    issues.append(
        MarketRegimeProjectionReconcileIssue(
            issue_id=f"{run_id}:{len(issues) + 1}",
            severity=severity,
            reason_code=reason_code,
            message=message,
            event_id=event_id,
            index_code=index_code,
            evidence=evidence or {},
        )
    )


def _status_from_issues(issues: Sequence[MarketRegimeProjectionReconcileIssue]) -> str:
    severities = {issue.severity for issue in issues}
    if "ERROR" in severities:
        return "FAIL"
    if "WARN" in severities:
        return "WARN"
    return "PASS"


def _run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["summary"] = json.loads(data.pop("summary_json"))
    for key in (
        "latest_event_covered",
        "context_ready",
        "append_only_ready",
        "no_trading_side_effects",
    ):
        data[key] = bool(data[key])
    return data


def _issue_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["evidence"] = json.loads(data.pop("evidence_json"))
    return data


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
