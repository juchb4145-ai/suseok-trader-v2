from tools.ops_market_index_tr_bootstrap_check import evaluate_report


def test_ops_bootstrap_passes_implemented_verified_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["event_count"] == 2
    assert verdict["sample_count"] == 2
    assert verdict["command_count_delta"] == 0


def test_ops_bootstrap_warns_for_safe_disabled_default() -> None:
    report = _report(status="DISABLED", event_count=0, sample_count=0)
    report["plan_only"]["status"] = "DISABLED"

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["warnings"] == [
        "MARKET_INDEX_TR_BOOTSTRAP_DISABLED_SAFE_DEFAULT"
    ]


def test_ops_bootstrap_fails_missing_projection_sample() -> None:
    verdict = evaluate_report(_report(event_count=2, sample_count=1))

    assert verdict["status"] == "FAIL"
    assert "MARKET_INDEX_TR_BOOTSTRAP_SAMPLE_GAP" in verdict["failures"]


def _report(
    *,
    status: str = "READY",
    event_count: int = 2,
    sample_count: int = 2,
) -> dict:
    bootstrap = {
        "status": status,
        "adapter_status": "IMPLEMENTED",
        "event_count": event_count,
        "sample_count": sample_count,
        "nxt_is_not_valid_market_index_evidence": True,
    }
    return {
        "expect_events": event_count > 0,
        "core_status": {
            "mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
        },
        "command_status_before": {
            "counts": {"ACKED": 2},
            "order_command_count": 0,
        },
        "command_status_after": {
            "counts": {"ACKED": 2},
            "order_command_count": 0,
        },
        "bootstrap_status": bootstrap,
        "plan_only": {
            "status": "PLAN_ONLY",
            "command_count": 0,
            "no_order_side_effects": True,
        },
        "market_index_status": {
            "source_contract": {"tr_bootstrap_adapter_status": "IMPLEMENTED"}
        },
        "latest_reconcile": {"latest_run": {"status": "PASS"}},
        "dashboard_snapshot": {
            "market_index_tr_bootstrap": bootstrap,
            "pipeline_summary": {"market_index_tr_bootstrap": bootstrap},
        },
    }
