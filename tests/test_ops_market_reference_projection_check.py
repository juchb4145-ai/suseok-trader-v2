from __future__ import annotations

from tools.ops_market_reference_projection_check import evaluate_report


def test_ops_market_reference_projection_check_passes_ready_report() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_pr14"] is False


def test_ops_market_reference_projection_check_fails_missing_membership() -> None:
    report = _report()
    report["latest_reconcile"]["data"]["latest_run"]["status"] = "FAIL"
    report["latest_reconcile"]["data"]["latest_run"]["missing_membership_count"] = 1
    report["market_reference_status"]["data"]["missing_membership_count"] = 1

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_REFERENCE_MEMBERSHIP_MISSING" in verdict["failures"]
    assert verdict["block_pr14"] is True


def _report() -> dict:
    latest_run = {
        "status": "PASS",
        "append_only_ready": True,
        "stored_membership_count": 120,
        "missing_membership_count": 0,
        "outbox_pending_count": 0,
        "outbox_error_count": 0,
        "outbox_dead_letter_count": 0,
        "reason_codes": [],
    }
    return {
        "reconcile_run": {"ok": True, "data": latest_run},
        "latest_reconcile": {"ok": True, "data": {"latest_run": dict(latest_run)}},
        "routing_status": {
            "ok": True,
            "data": {
                "dry_run_enabled": True,
                "effective_skip_inline_count": 0,
                "would_skip_inline_count": 1,
            },
        },
        "market_reference_status": {
            "ok": True,
            "data": {
                "membership_count": 120,
                "missing_membership_count": 0,
                "effective_skip_inline_count": 0,
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_reference": {
                        "error_count": 0,
                        "dead_letter_count": 0,
                    }
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "market_reference": {
                    "effective_skip_inline_count": 0,
                }
            },
        },
    }
