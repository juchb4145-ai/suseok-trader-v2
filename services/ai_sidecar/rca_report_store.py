from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from domain.ai_sidecar.policy import assert_no_trading_action
from domain.ai_sidecar.rca import (
    AIRCAReport,
    AIRCAReportType,
    parse_report_status,
    parse_report_type,
)
from domain.broker.utils import normalize_payload, normalize_value, optional_non_empty_str


def save_rca_report(connection: sqlite3.Connection, report: AIRCAReport) -> str:
    if not report.observe_only or not report.no_trading_side_effects:
        raise ValueError("RCA report must remain observe_only with no trading side effects")

    data = report.to_dict()
    assert_no_trading_action(data)
    metadata = dict(data["metadata"])
    metadata["schema_version"] = data["schema_version"]
    if report.ai_summary:
        metadata["ai_summary"] = report.ai_summary

    try:
        connection.execute("BEGIN")
        connection.execute("DELETE FROM ai_rca_sections WHERE report_id = ?", (report.report_id,))
        connection.execute(
            "DELETE FROM ai_rca_report_links WHERE report_id = ?",
            (report.report_id,),
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO ai_rca_reports (
                report_id,
                report_type,
                trade_date,
                related_entity_type,
                related_entity_id,
                title,
                summary,
                status,
                severity,
                root_cause_category,
                root_cause,
                context_id,
                ai_request_id,
                ai_insight_id,
                suggested_checks_json,
                warnings_json,
                metadata_json,
                observe_only,
                no_trading_side_effects,
                generated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report.report_id,
                report.report_type.value,
                report.trade_date,
                report.related_entity_type,
                report.related_entity_id,
                report.title,
                report.summary,
                report.status.value,
                report.severity.value,
                report.root_cause_category.value,
                report.root_cause,
                report.context_id,
                report.ai_request_id,
                report.ai_insight_id,
                _json_dumps(data["suggested_checks"]),
                _json_dumps(data["warnings"]),
                _json_dumps(metadata),
                1,
                1,
                report.generated_at,
            ),
        )
        for section in report.deterministic_sections:
            section_data = section.to_dict()
            connection.execute(
                """
                INSERT INTO ai_rca_sections (
                    report_id,
                    section_name,
                    status,
                    severity,
                    summary,
                    reason_codes_json,
                    evidence_json,
                    source_refs_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.report_id,
                    section.section_name,
                    section.status,
                    section.severity.value,
                    section.summary,
                    _json_dumps(section_data["reason_codes"]),
                    _json_dumps(section_data["evidence_json"]),
                    _json_dumps(section_data["source_refs"]),
                ),
            )
        _insert_default_links(connection, report)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return report.report_id


def get_rca_report(
    connection: sqlite3.Connection,
    report_id: str,
    *,
    include_sections: bool = True,
    include_links: bool = True,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_rca_reports
        WHERE report_id = ?
        """,
        (optional_non_empty_str(report_id, "report_id"),),
    ).fetchone()
    if row is None:
        return None
    report = _report_row_to_dict(row)
    if include_sections:
        report["deterministic_sections"] = _list_sections(connection, report_id)
    if include_links:
        report["links"] = _list_links(connection, report_id)
    return report


def list_rca_reports(
    connection: sqlite3.Connection,
    *,
    report_type: AIRCAReportType | str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if report_type is not None:
        clauses.append("report_type = ?")
        params.append(parse_report_type(report_type).value)
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(optional_non_empty_str(trade_date, "trade_date"))
    if related_entity_type is not None:
        clauses.append("related_entity_type = ?")
        params.append(optional_non_empty_str(related_entity_type, "related_entity_type"))
    if related_entity_id is not None:
        clauses.append("related_entity_id = ?")
        params.append(optional_non_empty_str(related_entity_id, "related_entity_id"))
    if status is not None:
        clauses.append("status = ?")
        params.append(parse_report_status(status).value)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_rca_reports
        {where_sql}
        ORDER BY generated_at DESC, report_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_report_row_to_dict(row) for row in rows]


def save_rca_report_error(
    connection: sqlite3.Connection,
    *,
    report_type: AIRCAReportType | str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    error_message: str,
    payload: Mapping[str, Any] | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO ai_rca_report_errors (
            report_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            None if report_type is None else parse_report_type(report_type).value,
            trade_date,
            related_entity_type,
            related_entity_id,
            str(error_message),
            _json_dumps(normalize_payload(payload or {})),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def list_rca_report_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_rca_report_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    errors = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["payload"] = json.loads(item.pop("payload_json"))
        errors.append(item)
    return errors


def count_rca_reports(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM ai_rca_reports").fetchone()
    return int(row["count"])


def _insert_default_links(connection: sqlite3.Connection, report: AIRCAReport) -> None:
    links: list[tuple[str, str, str, Mapping[str, Any]]] = []
    if report.related_entity_type and report.related_entity_id:
        links.append(
            (
                "related_entity",
                report.related_entity_type,
                report.related_entity_id,
                {},
            )
        )
    if report.context_id:
        links.append(("context_packet", "ai_context_packet", report.context_id, {}))
    if report.ai_request_id:
        links.append(
            (
                "ai_request",
                "ai_request",
                report.ai_request_id,
                {"status": report.status.value},
            )
        )
    if report.ai_insight_id:
        links.append(("ai_insight", "ai_insight", report.ai_insight_id, {}))

    for link_type, entity_type, entity_id, metadata in links:
        connection.execute(
            """
            INSERT INTO ai_rca_report_links (
                report_id,
                link_type,
                related_entity_type,
                related_entity_id,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                report.report_id,
                link_type,
                entity_type,
                entity_id,
                _json_dumps(metadata),
            ),
        )


def _list_sections(connection: sqlite3.Connection, report_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_rca_sections
        WHERE report_id = ?
        ORDER BY id ASC
        """,
        (report_id,),
    ).fetchall()
    return [_section_row_to_dict(row) for row in rows]


def _list_links(connection: sqlite3.Connection, report_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_rca_report_links
        WHERE report_id = ?
        ORDER BY id ASC
        """,
        (report_id,),
    ).fetchall()
    links = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["metadata"] = json.loads(item.pop("metadata_json"))
        links.append(item)
    return links


def _report_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    metadata = json.loads(row["metadata_json"])
    return {
        "report_id": row["report_id"],
        "report_type": row["report_type"],
        "trade_date": row["trade_date"],
        "related_entity_type": row["related_entity_type"],
        "related_entity_id": row["related_entity_id"],
        "title": row["title"],
        "summary": row["summary"],
        "status": row["status"],
        "severity": row["severity"],
        "root_cause_category": row["root_cause_category"],
        "root_cause": row["root_cause"],
        "context_id": row["context_id"],
        "ai_request_id": row["ai_request_id"],
        "ai_insight_id": row["ai_insight_id"],
        "deterministic_sections": [],
        "ai_summary": metadata.get("ai_summary"),
        "suggested_checks": json.loads(row["suggested_checks_json"]),
        "warnings": json.loads(row["warnings_json"]),
        "metadata": metadata,
        "observe_only": bool(row["observe_only"]),
        "no_trading_side_effects": bool(row["no_trading_side_effects"]),
        "generated_at": row["generated_at"],
        "schema_version": metadata.get("schema_version", "ai-rca-report.v1"),
    }


def _section_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "report_id": row["report_id"],
        "section_name": row["section_name"],
        "status": row["status"],
        "severity": row["severity"],
        "summary": row["summary"],
        "reason_codes": json.loads(row["reason_codes_json"]),
        "evidence_json": json.loads(row["evidence_json"]),
        "source_refs": json.loads(row["source_refs_json"]),
        "created_at": row["created_at"],
    }


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
