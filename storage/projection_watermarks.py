from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now


@dataclass(frozen=True, kw_only=True)
class ProjectionWatermark:
    projection_name: str
    last_event_rowid: int
    last_event_id: str | None = None
    last_event_received_at: str | None = None
    last_processed_at: str | None = None
    updated_at: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection_name": self.projection_name,
            "last_event_rowid": self.last_event_rowid,
            "last_event_id": self.last_event_id,
            "last_event_received_at": self.last_event_received_at,
            "last_processed_at": self.last_processed_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata or {},
        }


def get_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    row = connection.execute(
        """
        SELECT *
        FROM projection_watermarks
        WHERE projection_name = ?
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        return ProjectionWatermark(
            projection_name=normalized,
            last_event_rowid=0,
            metadata={},
        )
    return _row_to_watermark(row)


def advance_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    last_event_rowid: int,
    last_event_id: str | None = None,
    last_event_received_at: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    commit: bool = False,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    normalized_rowid = max(int(last_event_rowid), 0)
    current = get_projection_watermark(connection, normalized)
    if normalized_rowid <= current.last_event_rowid:
        return current

    now = datetime_to_wire(utc_now())
    metadata_json = json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True)
    connection.execute(
        """
        INSERT INTO projection_watermarks (
            projection_name,
            last_event_rowid,
            last_event_id,
            last_event_received_at,
            last_processed_at,
            updated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(projection_name) DO UPDATE SET
            last_event_rowid = excluded.last_event_rowid,
            last_event_id = excluded.last_event_id,
            last_event_received_at = excluded.last_event_received_at,
            last_processed_at = excluded.last_processed_at,
            updated_at = excluded.updated_at,
            metadata_json = excluded.metadata_json
        """,
        (
            normalized,
            normalized_rowid,
            last_event_id,
            last_event_received_at,
            now,
            now,
            metadata_json,
        ),
    )
    if commit:
        connection.commit()
    return get_projection_watermark(connection, normalized)


def reset_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    commit: bool = False,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    connection.execute(
        "DELETE FROM projection_watermarks WHERE projection_name = ?",
        (normalized,),
    )
    if commit:
        connection.commit()
    return get_projection_watermark(connection, normalized)


def _normalize_projection_name(projection_name: str) -> str:
    normalized = str(projection_name or "").strip().lower()
    if not normalized:
        raise ValueError("projection_name must not be empty")
    return normalized


def _row_to_watermark(row: sqlite3.Row) -> ProjectionWatermark:
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}
    return ProjectionWatermark(
        projection_name=str(row["projection_name"]),
        last_event_rowid=int(row["last_event_rowid"]),
        last_event_id=row["last_event_id"],
        last_event_received_at=row["last_event_received_at"],
        last_processed_at=row["last_processed_at"],
        updated_at=row["updated_at"],
        metadata=metadata,
    )
