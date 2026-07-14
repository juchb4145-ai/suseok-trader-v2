from __future__ import annotations

from tools.ops_market_scan_projection_check import evaluate_report, write_report


def test_market_scan_ops_report_passes_pr20_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["effective_skip_inline_count"] == 0
    assert verdict["candidate_ingest_executed_count"] == 0


def test_market_scan_ops_report_fails_on_effective_skip_and_dead_letter(tmp_path) -> None:
    report = _report()
    report["routing_status"]["data"]["effective_skip_inline_count"] = 1
    report["projection_outbox"]["data"]["by_projection_name"]["market_scan"][
        "dead_letter_count"
    ] = 1

    verdict = evaluate_report(report)
    report["verdict"] = verdict
    paths = write_report(report, out_dir=tmp_path)

    assert verdict["status"] == "FAIL"
    assert "MARKET_SCAN_EFFECTIVE_SKIP_FORBIDDEN_IN_PR20" in verdict["failures"]
    assert "MARKET_SCAN_OUTBOX_DEAD_LETTER_PRESENT" in verdict["failures"]
    assert paths["raw_json"].exists()
    assert paths["summary_md"].exists()


def test_market_scan_ops_report_requires_would_skip_when_expected() -> None:
    report = _report()
    report["expect_dry_run_ready"] = True
    report["routing_status"]["data"]["would_skip_inline_count"] = 0

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_SCAN_DRY_RUN_READY_EVIDENCE_MISSING" in verdict["failures"]


def test_market_scan_ops_report_passes_pr21_limited_cutover_contract() -> None:
    report = _report()
    report["expect_effective_skip"] = True
    report["routing_status"]["data"].update(
        {
            "cutover_enabled": True,
            "global_kill_switch": False,
            "effective_skip_disabled_in_pr20": False,
            "controller_status": "PASS",
            "rollback_required": False,
            "effective_skip_inline_count": 1,
            "effective_skip_health": {
                "pending_worker_count": 0,
                "worker_error_count": 0,
                "worker_apply_evidence_missing_count": 0,
                "artifact_missing_count": 0,
                "worker_lineage_missing_count": 0,
            },
        }
    )
    report["scan_worker_run"] = {
        "ok": True,
        "data": {
            "status": "COMPLETED",
            "market_scan_apply_enabled": True,
            "applied_by_worker_count": 1,
            "error_count": 0,
            "mutated_projection_names": ["market_scan"],
        },
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["effective_skip_inline_count"] == 1


def _report() -> dict:
    reconcile = {
        "status": "PASS",
        "append_only_ready": True,
        "no_trading_side_effects": True,
    }
    routing = {
        "status": "PASS",
        "effective_skip_disabled_in_pr20": True,
        "would_skip_inline_count": 1,
        "effective_skip_inline_count": 0,
        "no_trading_side_effects": True,
    }
    return {
        "run_worker": True,
        "expect_dry_run_ready": True,
        "core_status": {
            "ok": True,
            "data": {
                "mode": "OBSERVE",
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        },
        "command_status_before": {
            "ok": True,
            "data": {
                "counts": {"QUEUED": 0},
                "order_command_count": 0,
                "order_commands_allowed": False,
            },
        },
        "worker_run": {
            "ok": True,
            "data": {
                "status": "COMPLETED",
                "market_scan_apply_enabled": True,
                "applied_by_worker_count": 1,
                "error_count": 0,
                "mutated_projection_names": ["market_scan"],
            },
        },
        "scan_worker_run": {"ok": True, "data": {"status": "NOT_RUN"}},
        "reconcile_run": {"ok": True, "data": reconcile},
        "latest_reconcile": {
            "ok": True,
            "data": {"latest_run": reconcile, "issues": []},
        },
        "routing_status": {"ok": True, "data": routing},
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_scan": {"error_count": 0, "dead_letter_count": 0}
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "market_scan_projection_reconcile": {"latest_run": reconcile},
                "market_scan_append_only_routing": routing,
            },
        },
        "command_status_after": {
            "ok": True,
            "data": {
                "counts": {"QUEUED": 0},
                "order_command_count": 0,
                "order_commands_allowed": False,
            },
        },
    }
