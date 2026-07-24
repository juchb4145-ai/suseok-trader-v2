from __future__ import annotations

import contextvars
import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json
from storage.sqlite_locking import coordinated_sqlite_writer

EVALUATION_PIPELINE_LOCK = "evaluation_pipeline"
DEFAULT_EVALUATION_LOCK_TTL_SEC = 120
_BEGIN_IMMEDIATE_RETRY_DELAYS_SEC = (0.05, 0.1, 0.2, 0.4)


class EvaluationRunLockError(RuntimeError):
    def __init__(
        self,
        *,
        lock_name: str,
        owner_id: str,
        expires_at: str,
        process_id: int | None = None,
        thread_id: int | None = None,
        heartbeat_at: str | None = None,
        fencing_token: int | None = None,
        reason: str = "LEASE_ACTIVE",
    ) -> None:
        super().__init__(
            f"evaluation run lock is already held: {lock_name} owner={owner_id}"
        )
        self.lock_name = lock_name
        self.owner_id = owner_id
        self.expires_at = expires_at
        self.process_id = process_id
        self.thread_id = thread_id
        self.heartbeat_at = heartbeat_at
        self.fencing_token = fencing_token
        self.reason = reason

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "EVALUATION_RUN_LOCKED",
            "lock_name": self.lock_name,
            "owner_id": self.owner_id,
            "expires_at": self.expires_at,
            "process_id": self.process_id,
            "thread_id": self.thread_id,
            "heartbeat_at": self.heartbeat_at,
            "fencing_token": self.fencing_token,
            "reason": self.reason,
        }


class EvaluationRunFenceError(EvaluationRunLockError):
    def __init__(
        self,
        *,
        lock_name: str,
        owner_id: str,
        fencing_token: int,
        current_owner_id: str | None,
        current_fencing_token: int | None,
    ) -> None:
        RuntimeError.__init__(
            self,
            "runtime execution fence was lost: "
            f"{lock_name} owner={owner_id} token={fencing_token}"
        )
        self.lock_name = lock_name
        self.owner_id = owner_id
        self.fencing_token = fencing_token
        self.current_owner_id = current_owner_id
        self.current_fencing_token = current_fencing_token

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "EVALUATION_RUN_FENCE_LOST",
            "lock_name": self.lock_name,
            "owner_id": self.owner_id,
            "fencing_token": self.fencing_token,
            "current_owner_id": self.current_owner_id,
            "current_fencing_token": self.current_fencing_token,
        }


@dataclass
class RuntimeExecutionLease:
    lock_name: str
    owner_id: str
    fencing_token: int
    process_id: int
    thread_id: int
    acquired_at: str
    heartbeat_at: str
    expires_at: str
    ttl_sec: int
    heartbeat_interval_sec: float
    database_path: str | None = None
    _stop_event: threading.Event = field(
        default_factory=threading.Event,
        init=False,
        repr=False,
    )
    _heartbeat_thread: threading.Thread | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _lost_error: EvaluationRunFenceError | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "lock_name": self.lock_name,
            "owner_id": self.owner_id,
            "fencing_token": self.fencing_token,
            "process_id": self.process_id,
            "thread_id": self.thread_id,
            "acquired_at": self.acquired_at,
            "heartbeat_at": self.heartbeat_at,
            "expires_at": self.expires_at,
            "ttl_sec": self.ttl_sec,
            "heartbeat_interval_sec": self.heartbeat_interval_sec,
            "heartbeat_running": bool(
                self._heartbeat_thread is not None
                and self._heartbeat_thread.is_alive()
            ),
            "fence_lost": self._lost_error is not None,
        }

    def assert_current(
        self,
        connection: sqlite3.Connection,
        *,
        renew: bool = True,
    ) -> None:
        if self._lost_error is not None:
            raise self._lost_error
        if renew:
            heartbeat_at, expires_at = _renew_lock(connection, self)
            self.heartbeat_at = heartbeat_at
            self.expires_at = expires_at
            return
        _assert_lock_row_matches(connection, self)


_ACTIVE_RUNTIME_EXECUTION_LEASE: contextvars.ContextVar[
    RuntimeExecutionLease | None
] = contextvars.ContextVar("active_runtime_execution_lease", default=None)


@contextmanager
def immediate_transaction(
    connection: sqlite3.Connection,
    *,
    lease: RuntimeExecutionLease | None = None,
) -> Iterator[None]:
    started = not connection.in_transaction
    if not started:
        assert_runtime_execution_fence(connection, lease=lease, renew=True)
        yield
        return

    with coordinated_sqlite_writer():
        _begin_immediate_with_retry(connection)
        try:
            assert_runtime_execution_fence(connection, lease=lease, renew=True)
            yield
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise


@contextmanager
def runtime_execution_lock(
    connection: sqlite3.Connection,
    lock_name: str = EVALUATION_PIPELINE_LOCK,
    *,
    owner_id: str | None = None,
    ttl_sec: int = DEFAULT_EVALUATION_LOCK_TTL_SEC,
    heartbeat_interval_sec: float | None = None,
    details: Mapping[str, Any] | None = None,
    manage_lock: bool = True,
) -> Iterator[RuntimeExecutionLease | None]:
    if not manage_lock:
        yield _ACTIVE_RUNTIME_EXECUTION_LEASE.get()
        return

    resolved_owner_id = owner_id or new_message_id("eval_lock")
    resolved_ttl_sec = max(int(ttl_sec), 1)
    resolved_heartbeat_interval = (
        max(min(resolved_ttl_sec / 3.0, 30.0), 0.25)
        if heartbeat_interval_sec is None
        else max(float(heartbeat_interval_sec), 0.0)
    )
    process_id = os.getpid()
    thread_id = int(threading.get_ident())
    acquired = _acquire_lock(
        connection,
        lock_name=lock_name,
        owner_id=resolved_owner_id,
        process_id=process_id,
        thread_id=thread_id,
        ttl_sec=resolved_ttl_sec,
        details=details or {},
    )
    lease = RuntimeExecutionLease(
        lock_name=lock_name,
        owner_id=resolved_owner_id,
        fencing_token=acquired["fencing_token"],
        process_id=process_id,
        thread_id=thread_id,
        acquired_at=acquired["acquired_at"],
        heartbeat_at=acquired["heartbeat_at"],
        expires_at=acquired["expires_at"],
        ttl_sec=resolved_ttl_sec,
        heartbeat_interval_sec=resolved_heartbeat_interval,
        database_path=_database_path(connection),
    )
    context_token = _ACTIVE_RUNTIME_EXECUTION_LEASE.set(lease)
    _start_lease_heartbeat(lease)
    body_failed = False
    try:
        yield lease
    except Exception:
        body_failed = True
        raise
    finally:
        _stop_lease_heartbeat(lease)
        try:
            if not body_failed:
                lease.assert_current(connection, renew=False)
            _release_lock(connection, lease=lease)
        except Exception:
            if not body_failed:
                raise
        finally:
            _ACTIVE_RUNTIME_EXECUTION_LEASE.reset(context_token)


def assert_runtime_execution_fence(
    connection: sqlite3.Connection,
    *,
    lease: RuntimeExecutionLease | None = None,
    renew: bool = True,
) -> RuntimeExecutionLease | None:
    resolved_lease = lease or _ACTIVE_RUNTIME_EXECUTION_LEASE.get()
    if resolved_lease is None:
        return None
    resolved_lease.assert_current(connection, renew=renew)
    return resolved_lease


def clear_runtime_execution_locks(
    connection: sqlite3.Connection,
    *,
    process_id: int | None = None,
) -> int:
    current_process_id = os.getpid() if process_id is None else int(process_id)
    now_dt = utc_now()
    started = not connection.in_transaction
    if started:
        _begin_immediate_with_retry(connection)
    try:
        rows = connection.execute(
            """
            SELECT lock_name, owner_id, process_id, thread_id, expires_at,
                   fencing_token
            FROM runtime_execution_locks
            """
        ).fetchall()
        deleted_count = 0
        for row in rows:
            owner_process_id = int(row["process_id"] or 0)
            self_owned = owner_process_id > 0 and owner_process_id == current_process_id
            expired = parse_timestamp(row["expires_at"], "expires_at") <= now_dt
            owner_alive = _lock_owner_is_alive(
                process_id=owner_process_id,
                thread_id=int(row["thread_id"] or 0),
            )
            if not self_owned and not (expired and not owner_alive):
                continue
            cursor = connection.execute(
                """
                DELETE FROM runtime_execution_locks
                WHERE lock_name = ? AND owner_id = ? AND fencing_token = ?
                """,
                (
                    row["lock_name"],
                    row["owner_id"],
                    int(row["fencing_token"] or 0),
                ),
            )
            deleted_count += max(int(cursor.rowcount), 0)
        if started:
            connection.commit()
        return deleted_count
    except Exception:
        if started:
            connection.rollback()
        raise


def get_runtime_execution_lock_status(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    now_dt = utc_now()
    rows = connection.execute(
        """
        SELECT l.*, COALESCE(f.last_fencing_token, l.fencing_token, 0)
            AS last_fencing_token
        FROM runtime_execution_locks AS l
        LEFT JOIN runtime_execution_lock_fences AS f
            ON f.lock_name = l.lock_name
        ORDER BY l.lock_name
        """
    ).fetchall()
    locks: list[dict[str, Any]] = []
    for row in rows:
        expires_at = str(row["expires_at"])
        heartbeat_at = row["heartbeat_at"] or row["acquired_at"]
        expired = parse_timestamp(expires_at, "expires_at") <= now_dt
        owner_alive = _lock_owner_is_alive(
            process_id=int(row["process_id"] or 0),
            thread_id=int(row["thread_id"] or 0),
        )
        if not expired:
            state = "ACTIVE"
        elif owner_alive:
            state = "EXPIRED_OWNER_ALIVE"
        else:
            state = "STALE_EXPIRED"
        locks.append(
            {
                "lock_name": str(row["lock_name"]),
                "owner_id": str(row["owner_id"]),
                "process_id": int(row["process_id"] or 0),
                "thread_id": int(row["thread_id"] or 0),
                "fencing_token": int(row["fencing_token"] or 0),
                "last_fencing_token": int(row["last_fencing_token"] or 0),
                "acquired_at": row["acquired_at"],
                "heartbeat_at": heartbeat_at,
                "expires_at": expires_at,
                "heartbeat_age_sec": _age_seconds(heartbeat_at, now_dt=now_dt),
                "owner_alive": owner_alive,
                "state": state,
                "detail": _json_object(row["detail_json"]),
            }
        )
    return {
        "status": (
            "FAIL"
            if any(lock["state"] == "STALE_EXPIRED" for lock in locks)
            else "WARN"
            if any(lock["state"] == "EXPIRED_OWNER_ALIVE" for lock in locks)
            else "PASS"
        ),
        "lock_count": len(locks),
        "active_count": sum(lock["state"] == "ACTIVE" for lock in locks),
        "expired_owner_alive_count": sum(
            lock["state"] == "EXPIRED_OWNER_ALIVE" for lock in locks
        ),
        "stale_expired_count": sum(
            lock["state"] == "STALE_EXPIRED" for lock in locks
        ),
        "locks": locks,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _acquire_lock(
    connection: sqlite3.Connection,
    *,
    lock_name: str,
    owner_id: str,
    process_id: int,
    thread_id: int,
    ttl_sec: int,
    details: Mapping[str, Any],
) -> dict[str, Any]:
    now_dt = utc_now()
    now = datetime_to_wire(now_dt)
    expires_at = datetime_to_wire(now_dt + timedelta(seconds=ttl_sec))
    started = not connection.in_transaction
    if started:
        _begin_immediate_with_retry(connection)
    try:
        row = connection.execute(
            """
            SELECT owner_id, process_id, thread_id, heartbeat_at, expires_at,
                   fencing_token
            FROM runtime_execution_locks
            WHERE lock_name = ?
            """,
            (lock_name,),
        ).fetchone()
        if row is not None:
            expired = parse_timestamp(row["expires_at"], "expires_at") <= now_dt
            owner_alive = _lock_owner_is_alive(
                process_id=int(row["process_id"] or 0),
                thread_id=int(row["thread_id"] or 0),
            )
            if not expired or owner_alive:
                raise EvaluationRunLockError(
                    lock_name=lock_name,
                    owner_id=str(row["owner_id"]),
                    expires_at=str(row["expires_at"]),
                    process_id=int(row["process_id"] or 0),
                    thread_id=int(row["thread_id"] or 0),
                    heartbeat_at=row["heartbeat_at"],
                    fencing_token=int(row["fencing_token"] or 0),
                    reason=(
                        "OWNER_ALIVE_AFTER_TTL" if expired else "LEASE_ACTIVE"
                    ),
                )
        fencing_token = _next_fencing_token(connection, lock_name)
        connection.execute(
            """
            INSERT INTO runtime_execution_locks (
                lock_name,
                owner_id,
                acquired_at,
                expires_at,
                process_id,
                thread_id,
                heartbeat_at,
                fencing_token,
                detail_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lock_name) DO UPDATE SET
                owner_id = excluded.owner_id,
                acquired_at = excluded.acquired_at,
                expires_at = excluded.expires_at,
                process_id = excluded.process_id,
                thread_id = excluded.thread_id,
                heartbeat_at = excluded.heartbeat_at,
                fencing_token = excluded.fencing_token,
                detail_json = excluded.detail_json
            """,
            (
                lock_name,
                owner_id,
                now,
                expires_at,
                process_id,
                thread_id,
                now,
                fencing_token,
                canonical_json(dict(details)),
            ),
        )
        if started:
            connection.commit()
        return {
            "fencing_token": fencing_token,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": expires_at,
        }
    except Exception:
        if started:
            connection.rollback()
        raise


def _next_fencing_token(connection: sqlite3.Connection, lock_name: str) -> int:
    connection.execute(
        """
        INSERT INTO runtime_execution_lock_fences (
            lock_name,
            last_fencing_token,
            updated_at
        )
        VALUES (?, 1, ?)
        ON CONFLICT(lock_name) DO UPDATE SET
            last_fencing_token = runtime_execution_lock_fences.last_fencing_token + 1,
            updated_at = excluded.updated_at
        """,
        (lock_name, datetime_to_wire(utc_now())),
    )
    row = connection.execute(
        """
        SELECT last_fencing_token
        FROM runtime_execution_lock_fences
        WHERE lock_name = ?
        """,
        (lock_name,),
    ).fetchone()
    return int(row["last_fencing_token"])


def _renew_lock(
    connection: sqlite3.Connection,
    lease: RuntimeExecutionLease,
) -> tuple[str, str]:
    now_dt = utc_now()
    heartbeat_at = datetime_to_wire(now_dt)
    expires_at = datetime_to_wire(now_dt + timedelta(seconds=lease.ttl_sec))
    started = not connection.in_transaction
    if started:
        _begin_immediate_with_retry(connection)
    try:
        cursor = connection.execute(
            """
            UPDATE runtime_execution_locks
            SET heartbeat_at = ?, expires_at = ?
            WHERE lock_name = ? AND owner_id = ? AND fencing_token = ?
            """,
            (
                heartbeat_at,
                expires_at,
                lease.lock_name,
                lease.owner_id,
                lease.fencing_token,
            ),
        )
        if cursor.rowcount != 1:
            raise _fence_error(connection, lease)
        if started:
            connection.commit()
        return heartbeat_at, expires_at
    except Exception:
        if started:
            connection.rollback()
        raise


def _assert_lock_row_matches(
    connection: sqlite3.Connection,
    lease: RuntimeExecutionLease,
) -> None:
    row = connection.execute(
        """
        SELECT owner_id, fencing_token
        FROM runtime_execution_locks
        WHERE lock_name = ?
        """,
        (lease.lock_name,),
    ).fetchone()
    if (
        row is None
        or str(row["owner_id"]) != lease.owner_id
        or int(row["fencing_token"] or 0) != lease.fencing_token
    ):
        raise _fence_error(connection, lease, row=row)


def _fence_error(
    connection: sqlite3.Connection,
    lease: RuntimeExecutionLease,
    *,
    row: Mapping[str, Any] | None = None,
) -> EvaluationRunFenceError:
    current = row
    if current is None:
        current = connection.execute(
            """
            SELECT owner_id, fencing_token
            FROM runtime_execution_locks
            WHERE lock_name = ?
            """,
            (lease.lock_name,),
        ).fetchone()
    return EvaluationRunFenceError(
        lock_name=lease.lock_name,
        owner_id=lease.owner_id,
        fencing_token=lease.fencing_token,
        current_owner_id=None if current is None else str(current["owner_id"]),
        current_fencing_token=(
            None if current is None else int(current["fencing_token"] or 0)
        ),
    )


def _release_lock(
    connection: sqlite3.Connection,
    *,
    lease: RuntimeExecutionLease,
) -> None:
    started = not connection.in_transaction
    if started:
        _begin_immediate_with_retry(connection)
    try:
        connection.execute(
            """
            DELETE FROM runtime_execution_locks
            WHERE lock_name = ? AND owner_id = ? AND fencing_token = ?
            """,
            (lease.lock_name, lease.owner_id, lease.fencing_token),
        )
        if started:
            connection.commit()
    except Exception:
        if started:
            connection.rollback()
        raise


def _start_lease_heartbeat(lease: RuntimeExecutionLease) -> None:
    if lease.heartbeat_interval_sec <= 0 or not lease.database_path:
        return

    def heartbeat_loop() -> None:
        from storage.sqlite import open_connection

        connection = open_connection(lease.database_path)
        try:
            while not lease._stop_event.wait(lease.heartbeat_interval_sec):
                try:
                    heartbeat_at, expires_at = _renew_lock(connection, lease)
                    lease.heartbeat_at = heartbeat_at
                    lease.expires_at = expires_at
                except EvaluationRunFenceError as exc:
                    lease._lost_error = exc
                    return
                except sqlite3.OperationalError as exc:
                    if not _is_database_locked_error(exc):
                        return
        finally:
            connection.close()

    lease._heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        name=f"runtime-lock-heartbeat:{lease.lock_name}",
        daemon=True,
    )
    lease._heartbeat_thread.start()


def _stop_lease_heartbeat(lease: RuntimeExecutionLease) -> None:
    lease._stop_event.set()
    if lease._heartbeat_thread is not None:
        lease._heartbeat_thread.join(timeout=max(lease.heartbeat_interval_sec * 2, 1.0))


def _lock_owner_is_alive(*, process_id: int, thread_id: int) -> bool:
    if process_id <= 0:
        return False
    if process_id == os.getpid():
        if thread_id <= 0:
            return True
        return any(
            thread.ident == thread_id and thread.is_alive()
            for thread in threading.enumerate()
        )
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _database_path(connection: sqlite3.Connection) -> str | None:
    rows = connection.execute("PRAGMA database_list").fetchall()
    for row in rows:
        name = row[1] if not isinstance(row, sqlite3.Row) else row["name"]
        path = row[2] if not isinstance(row, sqlite3.Row) else row["file"]
        if name == "main" and path not in (None, "", ":memory:"):
            return str(path)
    return None


def _age_seconds(value: object, *, now_dt) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(str(value), "timestamp")
    except Exception:
        return None
    return max((now_dt - parsed).total_seconds(), 0.0)


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


def _begin_immediate_with_retry(connection: sqlite3.Connection) -> None:
    for delay_sec in (*_BEGIN_IMMEDIATE_RETRY_DELAYS_SEC, None):
        try:
            connection.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            if not _is_database_locked_error(exc) or delay_sec is None:
                raise
            time.sleep(delay_sec)
    raise RuntimeError("unreachable BEGIN IMMEDIATE retry state")


def _is_database_locked_error(exc: sqlite3.OperationalError) -> bool:
    return "database is locked" in str(exc).lower()
