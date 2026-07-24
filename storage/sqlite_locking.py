from __future__ import annotations

import random
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

T = TypeVar("T")

_SQLITE_LOCK_MESSAGES = (
    "database is locked",
    "database table is locked",
    "database is busy",
)
PROCESS_SQLITE_WRITER_COORDINATOR = threading.RLock()


@contextmanager
def coordinated_sqlite_writer(
    *,
    timeout_sec: float | None = None,
) -> Iterator[bool]:
    """Coordinate short SQLite write transactions within this process.

    A timed caller receives ``False`` without entering the writer slot. Blocking
    callers omit ``timeout_sec`` and always receive ``True``. The coordinator is
    reentrant so a service transaction can safely run inside a Gateway write
    request on the same thread.
    """

    if timeout_sec is None:
        PROCESS_SQLITE_WRITER_COORDINATOR.acquire()
        acquired = True
    else:
        acquired = PROCESS_SQLITE_WRITER_COORDINATOR.acquire(
            timeout=max(float(timeout_sec), 0.0)
        )
    try:
        yield acquired
    finally:
        if acquired:
            PROCESS_SQLITE_WRITER_COORDINATOR.release()


def configure_sqlite_busy_timeout(
    connection: sqlite3.Connection,
    *,
    timeout_ms: int,
) -> None:
    bounded_timeout_ms = min(max(int(timeout_ms), 1), 60_000)
    connection.execute(f"PRAGMA busy_timeout={bounded_timeout_ms}")


def is_sqlite_locked_error(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return any(fragment in message for fragment in _SQLITE_LOCK_MESSAGES)


def retry_sqlite_locked(
    operation: Callable[[], T],
    *,
    attempts: int,
    base_sleep_sec: float,
    max_sleep_sec: float,
    jitter: bool = True,
    on_retry: Callable[[BaseException, int], None] | None = None,
) -> T:
    bounded_attempts = max(int(attempts), 1)
    sleep_base = max(float(base_sleep_sec), 0.0)
    sleep_max = max(float(max_sleep_sec), sleep_base)
    for attempt in range(1, bounded_attempts + 1):
        try:
            return operation()
        except BaseException as exc:
            if not is_sqlite_locked_error(exc) or attempt >= bounded_attempts:
                raise
            if on_retry is not None:
                on_retry(exc, attempt)
            sleep_sec = min(sleep_base * (2 ** (attempt - 1)), sleep_max)
            if jitter and sleep_sec > 0:
                sleep_sec *= random.uniform(0.8, 1.2)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
    raise RuntimeError("unreachable sqlite lock retry state")


def sqlite_lock_retry_metadata(
    exc: BaseException,
    *,
    attempts: int,
    elapsed_ms: float,
) -> dict[str, object]:
    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "attempts": max(int(attempts), 1),
        "elapsed_ms": round(float(elapsed_ms), 3),
        "retryable": True,
        "reason_codes": ["SQLITE_DATABASE_LOCKED"],
    }
