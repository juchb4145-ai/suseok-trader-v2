from __future__ import annotations

import json
import sqlite3
from typing import Any

from services.theme_leadership.models import ThemeUniverseMember


class ThemeUniverseBuilder:
    """Builds an active theme universe from v2 Theme Service membership tables."""

    def build(
        self,
        connection: sqlite3.Connection,
        *,
        active_only: bool = True,
    ) -> list[ThemeUniverseMember]:
        where = "WHERE t.active = 1 AND m.active = 1" if active_only else ""
        rows = connection.execute(
            f"""
            SELECT
                t.theme_id,
                t.theme_name,
                m.code,
                m.name,
                m.weight,
                m.source_type,
                m.source_name,
                m.active,
                m.metadata_json
            FROM theme_members AS m
            JOIN themes AS t ON t.theme_id = m.theme_id
            {where}
            ORDER BY t.theme_name ASC, m.weight DESC, m.code ASC
            """
        ).fetchall()
        return [_row_to_member(row) for row in rows]


def _row_to_member(row: sqlite3.Row) -> ThemeUniverseMember:
    metadata: dict[str, Any]
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except json.JSONDecodeError:
        metadata = {"metadata_decode_error": True}
    return ThemeUniverseMember(
        theme_id=row["theme_id"],
        theme_name=row["theme_name"],
        code=row["code"],
        name=row["name"],
        weight=float(row["weight"] or 0.0),
        source_type=row["source_type"],
        source_name=row["source_name"],
        active=bool(row["active"]),
        metadata=metadata,
    )
