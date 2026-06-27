from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from domain.ai_sidecar.codex_prompt import (
    AICodexPromptDraft,
    AICodexPromptDraftStatus,
    AICodexPromptSourceType,
    AICodexPromptTargetArea,
    ensure_codex_prompt_policy,
    parse_prompt_source_type,
    parse_prompt_status,
    parse_prompt_target_area,
)
from domain.broker.utils import normalize_payload, normalize_value, optional_non_empty_str


def save_codex_prompt_draft(
    connection: sqlite3.Connection,
    draft: AICodexPromptDraft,
) -> str:
    _validate_safe_draft(draft)
    data = draft.to_dict()
    metadata = dict(data["metadata"])
    metadata["schema_version"] = data["schema_version"]
    try:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM ai_codex_prompt_sections WHERE draft_id = ?",
            (draft.draft_id,),
        )
        connection.execute(
            "DELETE FROM ai_codex_prompt_links WHERE draft_id = ?",
            (draft.draft_id,),
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO ai_codex_prompt_drafts (
                draft_id,
                title,
                source_type,
                target_area,
                status,
                trade_date,
                related_entity_type,
                related_entity_id,
                rca_report_id,
                context_id,
                ai_request_id,
                ai_insight_id,
                summary,
                prompt_text,
                safety_notes_json,
                acceptance_criteria_json,
                forbidden_scope_json,
                test_plan_json,
                warnings_json,
                metadata_json,
                generated_by,
                run_ai,
                observe_only,
                human_review_required,
                auto_apply_allowed,
                github_write_allowed,
                codex_execution_allowed,
                no_trading_side_effects,
                generated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                draft.draft_id,
                draft.title,
                draft.source_type.value,
                draft.target_area.value,
                draft.status.value,
                draft.trade_date,
                draft.related_entity_type,
                draft.related_entity_id,
                draft.rca_report_id,
                draft.context_id,
                draft.ai_request_id,
                draft.ai_insight_id,
                draft.summary,
                draft.prompt_text,
                _json_dumps(data["safety_notes"]),
                _json_dumps(data["acceptance_criteria"]),
                _json_dumps(data["forbidden_scope"]),
                _json_dumps(data["test_plan"]),
                _json_dumps(data["warnings"]),
                _json_dumps(metadata),
                draft.generated_by,
                1 if draft.run_ai else 0,
                1 if draft.observe_only else 0,
                1 if draft.human_review_required else 0,
                1 if draft.auto_apply_allowed else 0,
                1 if draft.github_write_allowed else 0,
                1 if draft.codex_execution_allowed else 0,
                1 if draft.no_trading_side_effects else 0,
                draft.generated_at,
            ),
        )
        for section in draft.sections:
            section_data = section.to_dict()
            connection.execute(
                """
                INSERT INTO ai_codex_prompt_sections (
                    draft_id,
                    section_name,
                    title,
                    body,
                    source_refs_json,
                    required,
                    order_index
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    draft.draft_id,
                    section.section_name,
                    section.title,
                    section.body,
                    _json_dumps(section_data["source_refs"]),
                    1 if section.required else 0,
                    section.order_index,
                ),
            )
        _insert_default_links(connection, draft)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return draft.draft_id


def get_codex_prompt_draft(
    connection: sqlite3.Connection,
    draft_id: str,
    *,
    include_sections: bool = True,
    include_links: bool = True,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_codex_prompt_drafts
        WHERE draft_id = ?
        """,
        (optional_non_empty_str(draft_id, "draft_id"),),
    ).fetchone()
    if row is None:
        return None
    draft = _draft_row_to_dict(row)
    if include_sections:
        draft["sections"] = _list_sections(connection, draft_id)
    if include_links:
        draft["links"] = _list_links(connection, draft_id)
    return draft


def list_codex_prompt_drafts(
    connection: sqlite3.Connection,
    *,
    source_type: AICodexPromptSourceType | str | None = None,
    target_area: AICodexPromptTargetArea | str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    status: AICodexPromptDraftStatus | str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if source_type is not None:
        clauses.append("source_type = ?")
        params.append(parse_prompt_source_type(source_type).value)
    if target_area is not None:
        clauses.append("target_area = ?")
        params.append(parse_prompt_target_area(target_area).value)
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
        params.append(parse_prompt_status(status).value)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_codex_prompt_drafts
        {where_sql}
        ORDER BY generated_at DESC, draft_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_draft_row_to_dict(row) for row in rows]


def save_codex_prompt_error(
    connection: sqlite3.Connection,
    *,
    source_type: AICodexPromptSourceType | str | None = None,
    target_area: AICodexPromptTargetArea | str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    error_message: str,
    payload: Mapping[str, Any] | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO ai_codex_prompt_errors (
            source_type,
            target_area,
            trade_date,
            related_entity_type,
            related_entity_id,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            None if source_type is None else parse_prompt_source_type(source_type).value,
            None if target_area is None else parse_prompt_target_area(target_area).value,
            trade_date,
            related_entity_type,
            related_entity_id,
            str(error_message),
            _json_dumps(normalize_payload(payload or {})),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def list_codex_prompt_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_codex_prompt_errors
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


def count_codex_prompt_drafts(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM ai_codex_prompt_drafts").fetchone()
    return int(row["count"])


def _validate_safe_draft(draft: AICodexPromptDraft) -> None:
    if not draft.observe_only:
        raise ValueError("Codex prompt draft must remain observe_only")
    if not draft.human_review_required:
        raise ValueError("Codex prompt draft must require human review")
    if draft.auto_apply_allowed:
        raise ValueError("Codex prompt draft cannot allow auto apply")
    if draft.github_write_allowed:
        raise ValueError("Codex prompt draft cannot allow GitHub writes")
    if draft.codex_execution_allowed:
        raise ValueError("Codex prompt draft cannot allow Codex execution")
    if not draft.no_trading_side_effects:
        raise ValueError("Codex prompt draft must have no trading side effects")
    ensure_codex_prompt_policy(draft.prompt_text)


def _insert_default_links(connection: sqlite3.Connection, draft: AICodexPromptDraft) -> None:
    links: list[tuple[str, str, str, Mapping[str, Any]]] = []
    if draft.related_entity_type and draft.related_entity_id:
        links.append(
            (
                "related_entity",
                draft.related_entity_type,
                draft.related_entity_id,
                {},
            )
        )
    if draft.rca_report_id:
        links.append(("rca_report", "ai_rca_report", draft.rca_report_id, {}))
    if draft.context_id:
        links.append(("context_packet", "ai_context_packet", draft.context_id, {}))
    if draft.ai_request_id:
        links.append(
            (
                "ai_request",
                "ai_request",
                draft.ai_request_id,
                {"status": draft.status.value},
            )
        )
    if draft.ai_insight_id:
        links.append(("ai_insight", "ai_insight", draft.ai_insight_id, {}))
    for link_type, entity_type, entity_id, metadata in links:
        connection.execute(
            """
            INSERT INTO ai_codex_prompt_links (
                draft_id,
                link_type,
                related_entity_type,
                related_entity_id,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                draft.draft_id,
                link_type,
                entity_type,
                entity_id,
                _json_dumps(metadata),
            ),
        )


def _list_sections(connection: sqlite3.Connection, draft_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_codex_prompt_sections
        WHERE draft_id = ?
        ORDER BY order_index ASC, id ASC
        """,
        (draft_id,),
    ).fetchall()
    return [_section_row_to_dict(row) for row in rows]


def _list_links(connection: sqlite3.Connection, draft_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_codex_prompt_links
        WHERE draft_id = ?
        ORDER BY id ASC
        """,
        (draft_id,),
    ).fetchall()
    links = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["metadata"] = json.loads(item.pop("metadata_json"))
        links.append(item)
    return links


def _draft_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    metadata = json.loads(row["metadata_json"])
    return {
        "draft_id": row["draft_id"],
        "title": row["title"],
        "source_type": row["source_type"],
        "target_area": row["target_area"],
        "status": row["status"],
        "trade_date": row["trade_date"],
        "related_entity_type": row["related_entity_type"],
        "related_entity_id": row["related_entity_id"],
        "rca_report_id": row["rca_report_id"],
        "context_id": row["context_id"],
        "ai_request_id": row["ai_request_id"],
        "ai_insight_id": row["ai_insight_id"],
        "summary": row["summary"],
        "prompt_text": row["prompt_text"],
        "safety_notes": json.loads(row["safety_notes_json"]),
        "acceptance_criteria": json.loads(row["acceptance_criteria_json"]),
        "forbidden_scope": json.loads(row["forbidden_scope_json"]),
        "test_plan": json.loads(row["test_plan_json"]),
        "warnings": json.loads(row["warnings_json"]),
        "metadata": metadata,
        "generated_by": row["generated_by"],
        "run_ai": bool(row["run_ai"]),
        "observe_only": bool(row["observe_only"]),
        "human_review_required": bool(row["human_review_required"]),
        "auto_apply_allowed": bool(row["auto_apply_allowed"]),
        "github_write_allowed": bool(row["github_write_allowed"]),
        "codex_execution_allowed": bool(row["codex_execution_allowed"]),
        "no_trading_side_effects": bool(row["no_trading_side_effects"]),
        "generated_at": row["generated_at"],
        "schema_version": metadata.get("schema_version", "ai-codex-prompt-draft.v1"),
        "sections": [],
    }


def _section_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "draft_id": row["draft_id"],
        "section_name": row["section_name"],
        "title": row["title"],
        "body": row["body"],
        "source_refs": json.loads(row["source_refs_json"]),
        "required": bool(row["required"]),
        "order_index": row["order_index"],
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
