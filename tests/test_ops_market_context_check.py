from __future__ import annotations

from tools.ops_market_context_check import evaluate_report, write_report


def test_market_context_ops_report_passes_coherent_observe_safe_contract() -> None:
    report = _report()

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0


def test_market_context_ops_report_warns_for_unverified_or_missing_market() -> None:
    report = _report()
    context = report["market_context_status"]["data"]
    context["status"] = "WARN"
    context["latest"]["KOSDAQ"] = None
    context["snapshot_count"] = 1
    context["parser_unverified_markets"] = ["KOSDAQ"]
    context["data_unusable_markets"] = ["KOSDAQ"]

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "MARKET_CONTEXT_LATEST_PAIR_MISSING" in verdict["warnings"]
    assert "MARKET_CONTEXT_PARSER_UNVERIFIED" in verdict["warnings"]
    assert verdict["block_next_pr"] is False


def test_market_context_ops_report_fails_on_safety_or_coherency_gap(tmp_path) -> None:
    report = _report()
    report["core_status"]["data"]["mode"] = "LIVE_SIM"
    report["market_context_status"]["data"]["latest_watermark_coherent"] = False
    report["projection_outbox"]["data"]["by_projection_name"]["market_index"][
        "dead_letter_count"
    ] = 1

    verdict = evaluate_report(report)
    report["verdict"] = verdict
    paths = write_report(report, out_dir=tmp_path)

    assert verdict["status"] == "FAIL"
    assert "CORE_NOT_OBSERVE" in verdict["failures"]
    assert "MARKET_CONTEXT_WATERMARK_INCOHERENT" in verdict["failures"]
    assert "PROJECTION_OUTBOX_DEAD_LETTER_PRESENT" in verdict["failures"]
    assert paths["raw_json"].exists()
    assert paths["summary_md"].exists()


def _report() -> dict:
    context = {
        "status": "PASS",
        "snapshot_count": 2,
        "latest": {
            "KOSPI": {"snapshot_id": "ctx-kospi", "source_watermark_hash": "hash"},
            "KOSDAQ": {"snapshot_id": "ctx-kosdaq", "source_watermark_hash": "hash"},
        },
        "latest_watermark_coherent": True,
        "latest_regime_coherent": True,
        "regime_reference_missing_count": 0,
        "stale_markets": [],
        "parser_unverified_markets": [],
        "data_unusable_markets": [],
        "candidate_reference_count": 2,
        "candidate_unreferenced_count": 0,
        "candidate_missing_snapshot_count": 0,
        "no_trading_side_effects": True,
    }
    return {
        "run_rebuild": True,
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
        "rebuild": {
            "ok": True,
            "data": {
                "status": "APPLIED_BY_VERIFY",
                "observe_safe": True,
                "no_trading_side_effects": True,
            },
        },
        "market_context_status": {"ok": True, "data": context},
        "market_index_status": {"ok": True, "data": {"latest_tick_count": 2}},
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_index": {"error_count": 0, "dead_letter_count": 0},
                    "market_regime": {"error_count": 0, "dead_letter_count": 0},
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {"market_context": dict(context)},
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
