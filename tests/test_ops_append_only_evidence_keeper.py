from __future__ import annotations

from pathlib import Path

from tools.ops_append_only_evidence_keeper import _run_reconcile, run_keeper_cycle


def _wrapped(data):
    return {"ok": True, "status_code": 200, "data": data}


def _fetcher(db_path: Path, *, candidate_ingest: int = 0):
    calls: list[tuple[str, str, float]] = []

    def fake(url, *, token, method, timeout_sec):
        del token
        calls.append((url, method, timeout_sec))
        route = url.split("?", 1)[0]
        if route.endswith("/api/status"):
            return _wrapped(
                {
                    "profile": "OBSERVE",
                    "mode": "OBSERVE",
                    "live_sim_allowed": False,
                    "live_real_allowed": False,
                    "database_path": str(db_path),
                }
            )
        if route.endswith("/api/gateway/commands/status"):
            return _wrapped({"counts": {"ACKED": 3}, "order_command_count": 0})
        if route.endswith("/api/operator/projection-outbox/run-once"):
            return _wrapped(
                {
                    "status": "COMPLETED",
                    "remaining_pending_count": 0,
                    "error_count": 0,
                    "dead_letter_count": 0,
                }
            )
        if route.endswith("/api/operator/live-sim/lifecycle-consumer/run-once"):
            return _wrapped({"status": "IDLE"})
        if route.endswith("projection-reconcile/run-once"):
            return _wrapped({"status": "PASS", "append_only_ready": True})
        if route.endswith("/api/operator/market-data-append-only/controller/snapshot"):
            return _wrapped({"status": "PASS", "snapshot_id": "snapshot-1"})
        if route.endswith("/api/operator/market-data-append-only/controller/status"):
            return _wrapped({"status": "PASS", "auto_rollback_required": False})
        if route.endswith("/api/operator/market-data-append-only-routing/status"):
            return _wrapped(
                {
                    "condition_event_effective_skip_count": 1,
                    "condition_event_candidate_ingest_executed_count": candidate_ingest,
                    "invalid_effective_skip_count": 0,
                }
            )
        if route.endswith("/api/operator/projection-outbox/status"):
            return _wrapped({"error_count": 0, "dead_letter_count": 0})
        raise AssertionError(f"unexpected route: {route}")

    return fake, calls


def _run(tmp_path: Path, fetcher):
    db_path = tmp_path / "evidence.sqlite3"
    db_path.touch()
    return run_keeper_cycle(
        core_url="http://127.0.0.1:8040",
        token="test-token",
        expected_db_path=str(db_path),
        trade_date="2026-07-14",
        reconcile_limit=500,
        outbox_limit=100,
        outbox_max_batches=5,
        timeout_sec=30.0,
        fetcher=fetcher,
    )


def test_keeper_cycle_refreshes_controller_before_remaining_reconciles(tmp_path) -> None:
    db_path = tmp_path / "evidence.sqlite3"
    fetcher, calls = _fetcher(db_path)

    report = _run(tmp_path, fetcher)

    snapshot_index = next(
        index for index, call in enumerate(calls) if call[0].endswith("controller/snapshot")
    )
    reference_index = next(
        index
        for index, call in enumerate(calls)
        if "market-reference-projection-reconcile" in call[0]
    )
    scan_call = next(
        call for call in calls if "market-scan-projection-reconcile" in call[0]
    )
    assert report["verdict"]["status"] == "PASS"
    assert snapshot_index < reference_index
    assert scan_call[1] == "POST"
    assert scan_call[2] == 120.0
    assert report["verdict"]["order_command_count_delta"] == 0


def test_keeper_cycle_fails_closed_before_post_when_core_is_not_observe(tmp_path) -> None:
    db_path = tmp_path / "evidence.sqlite3"
    db_path.touch()
    calls = []

    def fetcher(url, *, token, method, timeout_sec):
        del token, timeout_sec
        calls.append((url, method))
        if url.endswith("/api/status"):
            return _wrapped(
                {
                    "profile": "LIVE",
                    "mode": "LIVE",
                    "live_sim_allowed": True,
                    "live_real_allowed": False,
                    "database_path": str(db_path),
                }
            )
        return _wrapped({"counts": {}, "order_command_count": 0})

    report = _run(tmp_path, fetcher)

    assert report["verdict"]["status"] == "FAIL_SAFETY"
    assert "CORE_NOT_OBSERVE" in report["verdict"]["failures"]
    assert all(method == "GET" for _, method in calls)


def test_keeper_cycle_blocks_candidate_ingest_side_effect(tmp_path) -> None:
    db_path = tmp_path / "evidence.sqlite3"
    fetcher, _ = _fetcher(db_path, candidate_ingest=1)

    report = _run(tmp_path, fetcher)

    assert report["verdict"]["status"] == "FAIL_SAFETY"
    assert "CONDITION_EVENT_CANDIDATE_INGEST_EXECUTED" in report["verdict"][
        "failures"
    ]


def test_keeper_reconcile_falls_back_to_read_only_after_sqlite_lock() -> None:
    calls: list[str] = []

    def fetcher(url, *, token, method, timeout_sec):
        del token, method, timeout_sec
        calls.append(url)
        if "persist=true" in url:
            return {
                "ok": False,
                "status_code": 409,
                "data": {
                    "detail": {
                        "status": "LOCKED_RETRYABLE",
                        "retryable": True,
                        "reason_codes": ["SQLITE_DATABASE_LOCKED"],
                    }
                },
            }
        return _wrapped({"status": "PASS", "append_only_ready": True})

    result = _run_reconcile(
        fetcher=fetcher,
        base_url="http://127.0.0.1:8040",
        token="test-token",
        component="market_scan",
        limit=500,
        timeout_sec=30,
    )

    assert result["ok"] is True
    assert result["data"]["status"] == "PASS"
    assert result["persist_fallback"] == {
        "used": True,
        "reason_code": "SQLITE_DATABASE_LOCKED",
        "primary_status_code": 409,
    }
    assert "persist=true" in calls[0]
    assert "persist=false" in calls[1]
