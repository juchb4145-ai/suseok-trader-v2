from __future__ import annotations

from tools.ops_market_regime_projection_check import evaluate_report, write_report


def test_market_regime_ops_report_passes_observe_safe_pr18_contract() -> None:
    report = _report()

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []
    assert verdict["effective_skip_inline_count"] == 0
    assert verdict["command_count_delta"] == 0


def test_market_regime_ops_report_warns_when_context_is_not_ready() -> None:
    report = _report()
    report["reconcile_run"]["data"]["status"] = "WARN"
    report["reconcile_run"]["data"]["append_only_ready"] = False
    report["market_context_status"]["data"]["status"] = "WARN"

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "MARKET_REGIME_RECONCILE_NOT_PASS" in verdict["warnings"]
    assert "MARKET_CONTEXT_NOT_PASS" in verdict["warnings"]
    assert verdict["block_pr18"] is False


def test_market_regime_ops_report_fails_on_skip_outbox_or_safety_gap(tmp_path) -> None:
    report = _report()
    report["core_status"]["data"]["mode"] = "LIVE_SIM"
    report["routing_status"]["data"]["effective_skip_inline_count"] = 1
    report["projection_outbox"]["data"]["by_projection_name"]["market_regime"][
        "dead_letter_count"
    ] = 1

    verdict = evaluate_report(report)
    report["verdict"] = verdict
    paths = write_report(report, out_dir=tmp_path)

    assert verdict["status"] == "FAIL"
    assert "CORE_NOT_OBSERVE" in verdict["failures"]
    assert "MARKET_REGIME_EFFECTIVE_SKIP_FORBIDDEN_IN_PR18" in verdict["failures"]
    assert "MARKET_REGIME_OUTBOX_DEAD_LETTER_PRESENT" in verdict["failures"]
    assert paths["raw_json"].exists()
    assert paths["summary_md"].exists()


def _report() -> dict:
    reconcile = {
        "run_id": "reconcile-1",
        "status": "PASS",
        "append_only_ready": True,
        "checked_event_count": 2,
        "outbox_error_count": 0,
        "outbox_dead_letter_count": 0,
        "no_trading_side_effects": True,
    }
    routing = {
        "status": "PASS",
        "effective_skip_disabled_in_pr18": True,
        "would_skip_inline_count": 1,
        "effective_skip_inline_count": 0,
        "no_trading_side_effects": True,
    }
    return {
        "run_worker": True,
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
                "projection_name_filter": "market_regime",
                "market_regime_apply_enabled": True,
                "mutated_projection_names": ["market_regime", "market_context"],
                "no_trading_side_effects": True,
            },
        },
        "reconcile_run": {"ok": True, "data": reconcile},
        "latest_reconcile": {
            "ok": True,
            "data": {"latest_run": dict(reconcile), "issues": []},
        },
        "routing_status": {"ok": True, "data": routing},
        "market_context_status": {
            "ok": True,
            "data": {
                "status": "PASS",
                "latest_watermark_coherent": True,
                "latest_regime_coherent": True,
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_regime": {"error_count": 0, "dead_letter_count": 0}
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "market_regime_projection_reconcile": {"latest_run": reconcile},
                "market_regime_append_only_routing": routing,
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
