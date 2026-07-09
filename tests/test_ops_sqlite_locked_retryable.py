from __future__ import annotations

import io
import json
import urllib.error

from tools import ops_market_data_condition_event_side_effect_check as condition_tool
from tools import ops_market_data_tr_response_side_effect_check as http_tool


def test_ops_fetch_json_retries_locked_retryable_then_succeeds(monkeypatch) -> None:
    calls = {"count": 0}
    monkeypatch.setenv("OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC", "0")

    def fake_urlopen(request, timeout):
        del request, timeout
        calls["count"] += 1
        if calls["count"] <= 2:
            body = json.dumps(
                {
                    "detail": {
                        "status": "LOCKED_RETRYABLE",
                        "retryable": True,
                        "reason_codes": ["SQLITE_DATABASE_LOCKED"],
                    }
                }
            ).encode("utf-8")
            raise urllib.error.HTTPError(
                url="http://127.0.0.1/locked",
                code=409,
                msg="Conflict",
                hdrs={},
                fp=io.BytesIO(body),
            )
        return _Response({"status": "PASS"})

    monkeypatch.setattr(http_tool.urllib.request, "urlopen", fake_urlopen)

    payload = http_tool.fetch_json(
        "http://127.0.0.1/test",
        token="secret-token",
        method="POST",
        timeout_sec=1,
    )

    assert calls["count"] == 3
    assert payload["ok"] is True
    assert payload["status_code"] == 200
    assert payload["locked_retry_count"] == 2
    assert payload["locked_retry_exhausted"] is False


def test_condition_event_verdict_warns_and_blocks_next_pr_for_run_once_lock() -> None:
    report = {
        "run_once": {
            "projection_outbox": {
                "ok": False,
                "status_code": 409,
                "data": {
                    "detail": {
                        "status": "LOCKED_RETRYABLE",
                        "retryable": True,
                        "reason_codes": ["SQLITE_DATABASE_LOCKED"],
                    }
                },
                "locked_retry_exhausted": True,
            }
        },
        "routing_status": {
            "ok": True,
            "data": {
                "condition_event_worker_side_effect_ready": True,
                "condition_event_fusion_enabled": True,
                "condition_event_would_skip_inline_count": 1,
                "condition_event_effective_skip_count": 0,
                "condition_event_deferred_fusion_refresh_count": 1,
                "condition_event_deferred_fusion_refresh_error_count": 0,
                "condition_event_candidate_ingest_executed_count": 0,
                "condition_event_side_effect_duplicate_count": 0,
                "invalid_effective_skip_count": 0,
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        },
        "latest_reconcile": {
            "ok": True,
            "data": {"latest_run": {"status": "PASS"}},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_routing": {
                        "condition_event_effective_skip_count": 0,
                    }
                }
            },
        },
    }

    verdict = condition_tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "PROJECTION_OUTBOX_LOCKED_RETRYABLE" in verdict["warnings"]
    assert verdict["block_next_pr"] is True


class _Response:
    status = 200

    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")
