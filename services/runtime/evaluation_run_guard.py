from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

EVALUATION_PIPELINE_LOCK = "evaluation_pipeline"
DEFAULT_EVALUATION_LOCK_TTL_SEC = 900


class EvaluationRunLockError(RuntimeError):
    def __init__(
        self,
        *,
        lock_name: str,
        owner_id: str,
        expires_at: str,
    ) -> None:
        super().__init__(
            f"evaluation run lock is already held: {lock_name} owner={owner_id}"
        )
        self.lock_name = lock_name
        self.owner_id = owner_id
        self.expires_at = expires_at

    def to_dict(self) -> dict[str, str]:
        return {
            "error": "EVALUATION_RUN_LOCKED",
            "lock_name": self.lock_name,
            "owner_id": self.owner_id,
            "expires_at": self.expires_at,
        }


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    started = not connection.in_transaction
    if started:
        connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        if started:
            connection.rollback()
        raise
    else:
        if started:
            connection.commit()


@contextmanager
def runtime_execution_lock(
    connection: sqlite3.Connection,
    lock_name: str = EVALUATION_PIPELINE_LOCK,
    *,
    owner_id: str | None = None,
    ttl_sec: int = DEFAULT_EVALUATION_LOCK_TTL_SEC,
    details: Mapping[str, Any] | None = None,
    manage_lock: bool = True,
) -> Iterator[str | None]:
    if not manage_lock:
        yield owner_id
        return

    resolved_owner_id = owner_id or new_message_id("eval_lock")
    _acquire_lock(
        connection,
        lock_name=lock_name,
        owner_id=resolved_owner_id,
        ttl_sec=ttl_sec,
        details=details or {},
    )
    try:
        yield resolved_owner_id
    finally:
        _release_lock(connection, lock_name=lock_name, owner_id=resolved_owner_id)


def _acquire_lock(
    connection: sqlite3.Connection,
    *,
    lock_name: str,
    owner_id: str,
    ttl_sec: int,
    details: Mapping[str, Any],
) -> None:
    now_dt = utc_now()
    now = datetime_to_wire(now_dt)
    expires_at = datetime_to_wire(now_dt + timedelta(seconds=max(int(ttl_sec), 1)))
    started = not connection.in_transaction
    if started:
        connection.execute("BEGIN IMMEDIATE")
    try:
        row = connection.execute(
            """
            SELECT owner_id, expires_at
            FROM runtime_execution_locks
            WHERE lock_name = ?
            """,
            (lock_name,),
        ).fetchone()
        if row is not None and parse_timestamp(row["expires_at"], "expires_at") > now_dt:
            raise EvaluationRunLockError(
                lock_name=lock_name,
                owner_id=str(row["owner_id"]),
                expires_at=str(row["expires_at"]),
            )
        connection.execute(
            """
            INSERT INTO runtime_execution_locks (
                lock_name,
                owner_id,
                acquired_at,
                expires_at,
                detail_json
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(lock_name) DO UPDATE SET
                owner_id = excluded.owner_id,
                acquired_at = excluded.acquired_at,
                expires_at = excluded.expires_at,
                detail_json = excluded.detail_json
            """,
            (lock_name, owner_id, now, expires_at, canonical_json(dict(details))),
        )
        if started:
            connection.commit()
    except Exception:
        if started:
            connection.rollback()
        raise


def _release_lock(
    connection: sqlite3.Connection,
    *,
    lock_name: str,
    owner_id: str,
) -> None:
    started = not connection.in_transaction
    if started:
        connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            DELETE FROM runtime_execution_locks
            WHERE lock_name = ? AND owner_id = ?
            """,
            (lock_name, owner_id),
        )
        if started:
            connection.commit()
    except Exception:
        if started:
            connection.rollback()
        raise
