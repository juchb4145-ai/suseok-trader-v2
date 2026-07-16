from __future__ import annotations

import copy

import pytest
from services.pipeline_orphan_campaign import (
    ORPHAN_APPLY_PREDECESSOR,
    R9_HANDOFF_PREDECESSOR,
    PipelineOrphanCampaignError,
    build_orphan_campaign_link,
    orphan_campaign_chain_sha256,
    validate_orphan_campaign_apply_report,
    validate_orphan_campaign_link,
)


def test_m001_root_and_m002_predecessor_links_are_exact() -> None:
    root_report = "1" * 64
    root_chain = "2" * 64
    m001 = build_orphan_campaign_link(
        alias="M001",
        root_handoff_report_sha256=root_report,
        root_handoff_chain_sha256=root_chain,
        predecessor_kind=R9_HANDOFF_PREDECESSOR,
        predecessor_report_sha256=root_report,
        predecessor_chain_sha256=root_chain,
        predecessor_database_main_sha256="5" * 64,
        predecessor_database_main_size=100,
    )
    m001_chain = _chain(m001, request="3" * 64, disposition="4" * 64)
    m002 = build_orphan_campaign_link(
        alias="M002",
        root_handoff_report_sha256=root_report,
        root_handoff_chain_sha256=root_chain,
        predecessor_kind=ORPHAN_APPLY_PREDECESSOR,
        predecessor_report_sha256="5" * 64,
        predecessor_chain_sha256=m001_chain,
        predecessor_database_main_sha256="6" * 64,
        predecessor_database_main_size=101,
    )

    assert m001["ordinal"] == 1
    assert m002["ordinal"] == 2
    assert m002["predecessor_chain_sha256"] == m001_chain

    skipped = dict(m002)
    skipped["alias"] = "M003"
    with pytest.raises(PipelineOrphanCampaignError):
        validate_orphan_campaign_link(skipped)


def test_apply_report_recomputes_chain_and_rejects_tamper() -> None:
    link = build_orphan_campaign_link(
        alias="M001",
        root_handoff_report_sha256="1" * 64,
        root_handoff_chain_sha256="2" * 64,
        predecessor_kind=R9_HANDOFF_PREDECESSOR,
        predecessor_report_sha256="1" * 64,
        predecessor_chain_sha256="2" * 64,
        predecessor_database_main_sha256="5" * 64,
        predecessor_database_main_size=100,
    )
    report = _report(link)
    validated = validate_orphan_campaign_apply_report(
        report,
        report_sha256="9" * 64,
        expected_alias="M001",
        source_plan_report_sha256="a" * 64,
        source_reconciliation_report_sha256="b" * 64,
        target_set_sha256="c" * 64,
    )
    assert validated["chain_sha256"] == report["campaign"]["chain_sha256"]

    tampered = copy.deepcopy(report)
    tampered["database"]["checkpoint_complete"] = False
    with pytest.raises(PipelineOrphanCampaignError):
        validate_orphan_campaign_apply_report(
            tampered,
            report_sha256="9" * 64,
            expected_alias="M001",
            source_plan_report_sha256="a" * 64,
            source_reconciliation_report_sha256="b" * 64,
            target_set_sha256="c" * 64,
        )


def _chain(link: dict, *, request: str, disposition: str) -> str:
    return orphan_campaign_chain_sha256(
        source_plan_report_sha256="a" * 64,
        source_reconciliation_report_sha256="b" * 64,
        target_set_sha256="c" * 64,
        campaign_link=link,
        approval_binding_sha256="d" * 64,
        request_id_sha256=request,
        disposition_id_sha256=disposition,
    )


def _report(link: dict) -> dict:
    request = "3" * 64
    disposition = "4" * 64
    return {
        "contract": "fast0-pipeline-disposition-apply.v1",
        "generated_at": "2026-07-16T02:00:00Z",
        "mode": "APPLY",
        "campaign": {
            "source_plan_report_sha256": "a" * 64,
            "source_reconciliation_report_sha256": "b" * 64,
            "target_set_sha256": "c" * 64,
            "link": link,
            "chain_sha256": _chain(link, request=request, disposition=disposition),
        },
        "database": {
            "main_before": _fingerprint("5"),
            "main_after": _fingerprint("6"),
            "checkpoint_complete": True,
        },
        "result": {
            "action": "DISPOSE_ORPHAN_PIPELINE_OBSERVATION",
            "alias": "M001",
            "request_id_sha256": request,
            "disposition_id_sha256": disposition,
        },
        "evidence": {
            "alias": "M001",
            "approval_binding_sha256": "d" * 64,
        },
        "append_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": "APPLIED",
            "failures": [],
            "committed": True,
            "evidence_written": True,
            "operator_action_required": False,
        },
    }


def _fingerprint(digit: str) -> dict:
    return {
        "exists": True,
        "size": 100,
        "mtime_ns": 1,
        "sha256": digit * 64,
    }
