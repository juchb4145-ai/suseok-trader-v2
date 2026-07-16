from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

ORPHAN_CAMPAIGN_LINK_CONTRACT = "fast0-pipeline-orphan-campaign-link.v1"
ORPHAN_CAMPAIGN_CHAIN_CONTRACT = "fast0-pipeline-orphan-campaign-chain.v1"
ORPHAN_APPLY_REPORT_CONTRACT = "fast0-pipeline-disposition-apply.v1"
R9_HANDOFF_PREDECESSOR = "INCREMENTAL_DEAD_LETTER_CAMPAIGN_HANDOFF"
ORPHAN_APPLY_PREDECESSOR = "PIPELINE_ORPHAN_APPLY_REPORT"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALIAS_RE = re.compile(r"^M(?:00[1-9]|0[1-9][0-9]|[1-9][0-9]{2})$")
_LINK_KEYS = frozenset(
    {
        "contract",
        "ordinal",
        "alias",
        "root_handoff_report_sha256",
        "root_handoff_chain_sha256",
        "predecessor_kind",
        "predecessor_report_sha256",
        "predecessor_chain_sha256",
        "predecessor_database_main_sha256",
        "predecessor_database_main_size",
    }
)


class PipelineOrphanCampaignError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def build_orphan_campaign_link(
    *,
    alias: str,
    root_handoff_report_sha256: str,
    root_handoff_chain_sha256: str,
    predecessor_kind: str,
    predecessor_report_sha256: str,
    predecessor_chain_sha256: str,
    predecessor_database_main_sha256: str,
    predecessor_database_main_size: int,
) -> dict[str, Any]:
    ordinal = alias_ordinal(alias)
    return validate_orphan_campaign_link(
        {
            "contract": ORPHAN_CAMPAIGN_LINK_CONTRACT,
            "ordinal": ordinal,
            "alias": alias,
            "root_handoff_report_sha256": root_handoff_report_sha256,
            "root_handoff_chain_sha256": root_handoff_chain_sha256,
            "predecessor_kind": predecessor_kind,
            "predecessor_report_sha256": predecessor_report_sha256,
            "predecessor_chain_sha256": predecessor_chain_sha256,
            "predecessor_database_main_sha256": predecessor_database_main_sha256,
            "predecessor_database_main_size": predecessor_database_main_size,
        }
    )


def validate_orphan_campaign_link(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _LINK_KEYS:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_LINK_CONTRACT_INVALID")
    if value.get("contract") != ORPHAN_CAMPAIGN_LINK_CONTRACT:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_LINK_CONTRACT_INVALID")
    alias = require_alias(value.get("alias"))
    ordinal = alias_ordinal(alias)
    if type(value.get("ordinal")) is not int or int(value["ordinal"]) != ordinal:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_ORDINAL_INVALID")
    root_report = require_sha256(
        "ORPHAN_ROOT_HANDOFF_REPORT_SHA256", value.get("root_handoff_report_sha256")
    )
    root_chain = require_sha256(
        "ORPHAN_ROOT_HANDOFF_CHAIN_SHA256", value.get("root_handoff_chain_sha256")
    )
    predecessor_report = require_sha256(
        "ORPHAN_PREDECESSOR_REPORT_SHA256", value.get("predecessor_report_sha256")
    )
    predecessor_chain = require_sha256(
        "ORPHAN_PREDECESSOR_CHAIN_SHA256", value.get("predecessor_chain_sha256")
    )
    predecessor_main_sha256 = require_sha256(
        "ORPHAN_PREDECESSOR_DATABASE_MAIN_SHA256",
        value.get("predecessor_database_main_sha256"),
    )
    predecessor_main_size = value.get("predecessor_database_main_size")
    if type(predecessor_main_size) is not int or int(predecessor_main_size) <= 0:
        raise PipelineOrphanCampaignError("ORPHAN_PREDECESSOR_DATABASE_MAIN_SIZE_INVALID")
    expected_kind = R9_HANDOFF_PREDECESSOR if ordinal == 1 else ORPHAN_APPLY_PREDECESSOR
    if value.get("predecessor_kind") != expected_kind:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_PREDECESSOR_KIND_INVALID")
    if ordinal == 1 and (predecessor_report != root_report or predecessor_chain != root_chain):
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_ROOT_PREDECESSOR_INVALID")
    return {
        "contract": ORPHAN_CAMPAIGN_LINK_CONTRACT,
        "ordinal": ordinal,
        "alias": alias,
        "root_handoff_report_sha256": root_report,
        "root_handoff_chain_sha256": root_chain,
        "predecessor_kind": expected_kind,
        "predecessor_report_sha256": predecessor_report,
        "predecessor_chain_sha256": predecessor_chain,
        "predecessor_database_main_sha256": predecessor_main_sha256,
        "predecessor_database_main_size": int(predecessor_main_size),
    }


def orphan_campaign_chain_sha256(
    *,
    source_plan_report_sha256: str,
    source_reconciliation_report_sha256: str,
    target_set_sha256: str,
    campaign_link: Mapping[str, Any],
    approval_binding_sha256: str,
    request_id_sha256: str,
    disposition_id_sha256: str,
) -> str:
    link = validate_orphan_campaign_link(campaign_link)
    payload = {
        "contract": ORPHAN_CAMPAIGN_CHAIN_CONTRACT,
        "source_plan_report_sha256": require_sha256(
            "SOURCE_PLAN_REPORT_SHA256", source_plan_report_sha256
        ),
        "source_reconciliation_report_sha256": require_sha256(
            "SOURCE_RECONCILIATION_REPORT_SHA256", source_reconciliation_report_sha256
        ),
        "target_set_sha256": require_sha256("TARGET_SET_SHA256", target_set_sha256),
        "campaign_link": link,
        "approval_binding_sha256": require_sha256(
            "APPROVAL_BINDING_SHA256", approval_binding_sha256
        ),
        "request_id_sha256": require_sha256("REQUEST_ID_SHA256", request_id_sha256),
        "disposition_id_sha256": require_sha256("DISPOSITION_ID_SHA256", disposition_id_sha256),
    }
    return canonical_sha256(payload)


def validate_orphan_campaign_apply_report(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    expected_alias: str,
    source_plan_report_sha256: str,
    source_reconciliation_report_sha256: str,
    target_set_sha256: str,
) -> dict[str, Any]:
    alias = require_alias(expected_alias)
    campaign = _mapping(payload.get("campaign"))
    result = _mapping(payload.get("result"))
    evidence = _mapping(payload.get("evidence"))
    database = _mapping(payload.get("database"))
    verdict = _mapping(payload.get("verdict"))
    main_before = _mapping(database.get("main_before"))
    main_after = _mapping(database.get("main_after"))
    link = validate_orphan_campaign_link(_mapping(campaign.get("link")))
    expected_chain = orphan_campaign_chain_sha256(
        source_plan_report_sha256=source_plan_report_sha256,
        source_reconciliation_report_sha256=source_reconciliation_report_sha256,
        target_set_sha256=target_set_sha256,
        campaign_link=link,
        approval_binding_sha256=str(evidence.get("approval_binding_sha256") or ""),
        request_id_sha256=str(result.get("request_id_sha256") or ""),
        disposition_id_sha256=str(result.get("disposition_id_sha256") or ""),
    )
    valid = bool(
        payload.get("contract") == ORPHAN_APPLY_REPORT_CONTRACT
        and payload.get("mode") == "APPLY"
        and link["alias"] == alias
        and campaign.get("source_plan_report_sha256") == source_plan_report_sha256
        and campaign.get("source_reconciliation_report_sha256")
        == source_reconciliation_report_sha256
        and campaign.get("target_set_sha256") == target_set_sha256
        and campaign.get("chain_sha256") == expected_chain
        and result.get("alias") == alias
        and result.get("action") == "DISPOSE_ORPHAN_PIPELINE_OBSERVATION"
        and is_sha256(result.get("request_id_sha256"))
        and is_sha256(result.get("disposition_id_sha256"))
        and evidence.get("alias") == alias
        and is_sha256(evidence.get("approval_binding_sha256"))
        and _fingerprint_valid(main_before)
        and _fingerprint_valid(main_after)
        and main_before.get("sha256") == link["predecessor_database_main_sha256"]
        and main_before.get("size") == link["predecessor_database_main_size"]
        and database.get("checkpoint_complete") is True
        and verdict.get("status") in {"APPLIED", "IDEMPOTENT"}
        and verdict.get("failures") == []
        and verdict.get("committed") is True
        and verdict.get("evidence_written") is True
        and verdict.get("operator_action_required") is False
        and payload.get("append_only") is True
        and payload.get("observe_only") is True
        and payload.get("no_order_side_effects") is True
    )
    if not valid:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_APPLY_REPORT_INVALID")
    return {
        "contract": ORPHAN_APPLY_REPORT_CONTRACT,
        "report_sha256": require_sha256("APPLY_REPORT_SHA256", report_sha256),
        "alias": alias,
        "ordinal": link["ordinal"],
        "root_handoff_report_sha256": link["root_handoff_report_sha256"],
        "root_handoff_chain_sha256": link["root_handoff_chain_sha256"],
        "chain_sha256": expected_chain,
        "database_main_before": dict(main_before),
        "database_main_after": dict(main_after),
        "generated_at": payload.get("generated_at"),
    }


def alias_ordinal(value: object) -> int:
    alias = require_alias(value)
    return int(alias[1:])


def require_alias(value: object) -> str:
    alias = str(value or "")
    if _ALIAS_RE.fullmatch(alias) is None:
        raise PipelineOrphanCampaignError("ORPHAN_CAMPAIGN_ALIAS_INVALID")
    return alias


def require_sha256(name: str, value: object) -> str:
    normalized = str(value or "")
    if _SHA256_RE.fullmatch(normalized) is None:
        raise PipelineOrphanCampaignError(f"{name}_INVALID")
    return normalized


def is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _fingerprint_valid(value: Mapping[str, Any]) -> bool:
    return bool(
        value.get("exists") is True
        and type(value.get("size")) is int
        and int(value["size"]) > 0
        and type(value.get("mtime_ns")) is int
        and is_sha256(value.get("sha256"))
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
