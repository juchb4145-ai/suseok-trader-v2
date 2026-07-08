from __future__ import annotations

from tools import ops_market_data_append_only_routing_check as tool


def test_ops_market_data_append_only_routing_check_writes_pass_summary(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_fetch_json(url: str, *, token: str, method: str, timeout_sec: float):
        calls.append((method, url))
        if url.endswith("/api/operator/market-data-append-only-routing/status"):
            return {
                "ok": True,
                "status_code": 200,
                "url": url,
                "data": {
                    "dry_run_enabled": True,
                    "cutover_enabled": False,
                    "append_only_ready": True,
                    "total_decision_count": 3,
                    "would_skip_inline_count": 2,
                    "effective_skip_inline_count": 0,
                    "blocked_count": 1,
                    "blocked_reason_code_counts": {},
                },
            }
        if "/api/operator/market-data-append-only-routing/decisions" in url:
            return {
                "ok": True,
                "status_code": 200,
                "url": url,
                "data": {"decisions": []},
            }
        if url.endswith("/api/operator/market-data-projection-reconcile/latest"):
            return {
                "ok": True,
                "status_code": 200,
                "url": url,
                "data": {
                    "latest_run": {
                        "status": "PASS",
                        "append_only_ready": True,
                        "checked_event_count": 3,
                    }
                },
            }
        if url.endswith("/api/operator/projection-outbox/status"):
            return {
                "ok": True,
                "status_code": 200,
                "url": url,
                "data": {
                    "pending_count": 0,
                    "error_count": 0,
                    "dead_letter_count": 0,
                },
            }
        return {
            "ok": True,
            "status_code": 200,
            "url": url,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_routing": {
                        "effective_skip_inline_count": 0
                    }
                }
            },
        }

    monkeypatch.setattr(tool, "fetch_json", fake_fetch_json)

    report = tool.run_routing_report(
        core_url="http://127.0.0.1:8000",
        token="test-token",
        limit=3,
        timeout_sec=1,
        out_dir=tmp_path,
    )

    summary_file = next(tmp_path.glob("*/summary.md"))
    assert report["verdict"]["status"] == "PASS"
    assert report["report_paths"]["summary_md"] == str(summary_file)
    assert "verdict: `PASS`" in summary_file.read_text(encoding="utf-8")
    assert calls[0][0] == "GET"
    assert "market-data-append-only-routing/status" in calls[0][1]


def test_ops_market_data_append_only_routing_check_fails_on_effective_skip() -> None:
    report = {
        "routing_status": {
            "ok": True,
            "data": {
                "dry_run_enabled": True,
                "cutover_enabled": True,
                "append_only_ready": True,
                "total_decision_count": 1,
                "would_skip_inline_count": 1,
                "effective_skip_inline_count": 1,
            },
        },
        "routing_decisions": {"ok": True, "data": {"decisions": []}},
        "latest_reconcile": {
            "ok": True,
            "data": {
                "latest_run": {
                    "status": "PASS",
                    "append_only_ready": True,
                    "checked_event_count": 1,
                }
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_routing": {
                        "effective_skip_inline_count": 1
                    }
                }
            },
        },
    }

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "EFFECTIVE_SKIP_INLINE_OCCURRED_IN_PR6" in verdict["failures"]


def test_ops_market_data_append_only_routing_check_fails_on_would_skip_without_ready() -> None:
    report = {
        "routing_status": {
            "ok": True,
            "data": {
                "dry_run_enabled": True,
                "cutover_enabled": False,
                "append_only_ready": False,
                "total_decision_count": 1,
                "would_skip_inline_count": 1,
                "effective_skip_inline_count": 0,
            },
        },
        "routing_decisions": {"ok": True, "data": {"decisions": []}},
        "latest_reconcile": {
            "ok": True,
            "data": {
                "latest_run": {
                    "status": "WARN",
                    "append_only_ready": False,
                    "checked_event_count": 1,
                }
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_routing": {
                        "effective_skip_inline_count": 0
                    }
                }
            },
        },
    }

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "WOULD_SKIP_WITHOUT_APPEND_ONLY_READY" in verdict["failures"]
