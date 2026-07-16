from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PRIVATE_TARGET_SET_CONTRACT = "fast0-private-target-set.v1"
CAMPAIGN_APPLY_EVIDENCE_BINDING_CONTRACT = "fast0-incremental-dead-letter-campaign-apply-binding.v1"
CAMPAIGN_CHAIN_LINK_CONTRACT = "fast0-incremental-dead-letter-campaign-chain-link.v1"
CAMPAIGN_ACTION = "DISPOSE_OBSOLETE_CLOSED_CANDIDATE"
CAMPAIGN_BUCKET = "HISTORICAL_PENDING_DISPOSITION"
CAMPAIGN_PREDECESSOR_KIND = "CAMPAIGN_APPLY_REPORT"
CAMPAIGN_TARGET_COUNT = 38

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALIAS_RE = re.compile(r"^U(?:0[1-9]|[12][0-9]|3[0-8])$")
_UTC_Z_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z$"
)

_PRIVATE_TARGET_KEYS = frozenset(
    {
        "action",
        "dead_letter_id",
        "dead_letter_fingerprint",
        "candidate_version",
        "bucket",
        "alias",
    }
)
_PRIVACY_SAFE_TARGET_KEYS = frozenset(
    {
        "alias",
        "ordinal",
        "action",
        "bucket",
        "dead_letter_id_sha256",
        "dead_letter_fingerprint",
        "candidate_version",
    }
)
_APPLY_BINDING_KEYS = frozenset(
    {
        "contract",
        *_PRIVACY_SAFE_TARGET_KEYS,
        "source_plan_report_sha256",
        "private_manifest_sha256",
        "target_set_sha256",
        "approved_preflight_report_sha256",
        "approved_database_main_sha256",
        "approved_database_main_size",
        "predecessor_kind",
        "predecessor_report_sha256",
        "predecessor_chain_sha256",
        "evidence_sha256",
        "generated_at",
        "preflight_generated_at",
        "approval_binding_sha256",
    }
)


class IncrementalDeadLetterCampaignError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


@dataclass(frozen=True)
class StableJsonDocument:
    payload: Mapping[str, Any]
    sha256: str
    size: int
    device: int
    inode: int
    mtime_ns: int


def read_stable_json_document(
    path: Path,
    *,
    expected_sha256: str | None = None,
    max_bytes: int = 1_048_576,
) -> StableJsonDocument:
    if type(max_bytes) is not int or max_bytes <= 0:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_MAX_BYTES_INVALID")
    expected = (
        None
        if expected_sha256 is None
        else require_sha256("EXPECTED_DOCUMENT_SHA256", expected_sha256)
    )
    candidate = Path(path)
    try:
        before = os.lstat(candidate)
    except OSError as exc:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_NOT_FOUND") from exc
    if stat.S_ISLNK(before.st_mode):
        raise IncrementalDeadLetterCampaignError("DOCUMENT_SYMLINK_FORBIDDEN")
    if not stat.S_ISREG(before.st_mode):
        raise IncrementalDeadLetterCampaignError("DOCUMENT_REGULAR_FILE_REQUIRED")
    if int(before.st_nlink) != 1:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_HARDLINK_FORBIDDEN")

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_CHANGED_DURING_READ") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise IncrementalDeadLetterCampaignError("DOCUMENT_REGULAR_FILE_REQUIRED")
        if int(opened.st_nlink) != 1:
            raise IncrementalDeadLetterCampaignError("DOCUMENT_HARDLINK_FORBIDDEN")
        if _file_identity(before) != _file_identity(opened):
            raise IncrementalDeadLetterCampaignError("DOCUMENT_CHANGED_DURING_READ")
        if int(opened.st_size) <= 0:
            raise IncrementalDeadLetterCampaignError("DOCUMENT_EMPTY")
        if int(opened.st_size) > max_bytes:
            raise IncrementalDeadLetterCampaignError("DOCUMENT_TOO_LARGE")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(max_bytes + 1)
    finally:
        os.close(descriptor)

    try:
        current = os.lstat(candidate)
    except OSError as exc:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_CHANGED_DURING_READ") from exc
    if (
        stat.S_ISLNK(current.st_mode)
        or _file_identity(opened) != _file_identity(current)
        or len(raw) != int(opened.st_size)
    ):
        raise IncrementalDeadLetterCampaignError("DOCUMENT_CHANGED_DURING_READ")

    document_sha256 = hashlib.sha256(raw).hexdigest()
    if expected is not None and document_sha256 != expected:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_SHA256_MISMATCH")

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_constant,
            parse_float=_parse_finite_float,
        )
    except (UnicodeError, ValueError) as exc:
        raise IncrementalDeadLetterCampaignError("DOCUMENT_JSON_INVALID") from exc
    if not isinstance(payload, Mapping):
        raise IncrementalDeadLetterCampaignError("DOCUMENT_ROOT_OBJECT_REQUIRED")
    return StableJsonDocument(
        payload=payload,
        sha256=document_sha256,
        size=len(raw),
        device=int(opened.st_dev),
        inode=int(opened.st_ino),
        mtime_ns=int(opened.st_mtime_ns),
    )


def validate_private_target_manifest(
    document: StableJsonDocument,
    *,
    expected_semantic_sha256: str,
) -> dict[str, Any]:
    expected_digest = require_sha256("EXPECTED_TARGET_SET_SHA256", expected_semantic_sha256)
    _require_exact_keys(document.payload, {"contract", "items"}, "TARGET_MANIFEST")
    if document.payload.get("contract") != PRIVATE_TARGET_SET_CONTRACT:
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_CONTRACT_INVALID")
    raw_items = document.payload.get("items")
    if not isinstance(raw_items, list) or len(raw_items) != CAMPAIGN_TARGET_COUNT:
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_COUNT_MISMATCH")

    items: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    for ordinal, raw_item in enumerate(raw_items, start=1):
        if not isinstance(raw_item, Mapping):
            raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_ITEM_INVALID")
        item = normalize_private_target(raw_item)
        if item["alias"] != _alias_for_ordinal(ordinal):
            raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_ALIAS_SEQUENCE_INVALID")
        identifier = item["dead_letter_id"]
        if identifier in identifiers:
            raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_DUPLICATE_DEAD_LETTER_ID")
        identifiers.add(identifier)
        items.append(item)
    ordered_identifiers = [item["dead_letter_id"] for item in items]
    if ordered_identifiers != sorted(ordered_identifiers):
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_ORDER_INVALID")

    target_set_sha256 = private_target_set_sha256(items)
    if target_set_sha256 != expected_digest:
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_DIGEST_MISMATCH")
    return {
        "contract": PRIVATE_TARGET_SET_CONTRACT,
        "manifest_file_sha256": document.sha256,
        "manifest_file_size": document.size,
        "target_set_sha256": target_set_sha256,
        "count": len(items),
        "items": items,
    }


def normalize_private_target(value: Mapping[str, Any]) -> dict[str, Any]:
    _require_exact_keys(value, _PRIVATE_TARGET_KEYS, "TARGET_MANIFEST_ITEM")
    alias = require_alias(value.get("alias"))
    dead_letter_id = _require_private_identifier(value.get("dead_letter_id"))
    if value.get("action") != CAMPAIGN_ACTION:
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_ACTION_INVALID")
    if value.get("bucket") != CAMPAIGN_BUCKET:
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_BUCKET_INVALID")
    return {
        "action": CAMPAIGN_ACTION,
        "dead_letter_id": dead_letter_id,
        "dead_letter_fingerprint": require_sha256(
            "DEAD_LETTER_FINGERPRINT", value.get("dead_letter_fingerprint")
        ),
        "candidate_version": require_sha256("CANDIDATE_VERSION", value.get("candidate_version")),
        "bucket": CAMPAIGN_BUCKET,
        "alias": alias,
    }


def privacy_safe_target_projection(target: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_private_target(target)
    return {
        "alias": normalized["alias"],
        "ordinal": _ordinal_for_alias(normalized["alias"]),
        "action": CAMPAIGN_ACTION,
        "bucket": CAMPAIGN_BUCKET,
        "dead_letter_id_sha256": hashlib.sha256(
            normalized["dead_letter_id"].encode("utf-8")
        ).hexdigest(),
        "dead_letter_fingerprint": normalized["dead_letter_fingerprint"],
        "candidate_version": normalized["candidate_version"],
    }


def build_campaign_apply_evidence_binding(
    target: Mapping[str, Any],
    *,
    source_plan_report_sha256: str,
    private_manifest_sha256: str,
    target_set_sha256: str,
    approved_preflight_report_sha256: str,
    approved_database_main_sha256: str,
    approved_database_main_size: int,
    predecessor_kind: str | None,
    predecessor_report_sha256: str | None,
    predecessor_chain_sha256: str | None,
    evidence_sha256: str,
    generated_at: str,
    preflight_generated_at: str,
) -> dict[str, Any]:
    projection = privacy_safe_target_projection(target)
    ordinal = int(projection["ordinal"])
    normalized_predecessor = _normalize_predecessor(
        ordinal=ordinal,
        kind=predecessor_kind,
        report_sha256=predecessor_report_sha256,
        chain_sha256=predecessor_chain_sha256,
    )
    if type(approved_database_main_size) is not int or approved_database_main_size <= 0:
        raise IncrementalDeadLetterCampaignError("APPROVED_DATABASE_MAIN_SIZE_INVALID")
    generated = require_utc_z_timestamp("GENERATED_AT", generated_at)
    preflight_generated = require_utc_z_timestamp("PREFLIGHT_GENERATED_AT", preflight_generated_at)
    if preflight_generated > generated:
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_TIME_ORDER_INVALID")

    binding: dict[str, Any] = {
        "contract": CAMPAIGN_APPLY_EVIDENCE_BINDING_CONTRACT,
        **projection,
        "source_plan_report_sha256": require_sha256(
            "SOURCE_PLAN_REPORT_SHA256", source_plan_report_sha256
        ),
        "private_manifest_sha256": require_sha256(
            "PRIVATE_MANIFEST_SHA256", private_manifest_sha256
        ),
        "target_set_sha256": require_sha256("TARGET_SET_SHA256", target_set_sha256),
        "approved_preflight_report_sha256": require_sha256(
            "APPROVED_PREFLIGHT_REPORT_SHA256", approved_preflight_report_sha256
        ),
        "approved_database_main_sha256": require_sha256(
            "APPROVED_DATABASE_MAIN_SHA256", approved_database_main_sha256
        ),
        "approved_database_main_size": approved_database_main_size,
        **normalized_predecessor,
        "evidence_sha256": require_sha256("EVIDENCE_SHA256", evidence_sha256),
        "generated_at": generated_at,
        "preflight_generated_at": preflight_generated_at,
    }
    binding["approval_binding_sha256"] = canonical_sha256(binding)
    return binding


def validate_campaign_apply_evidence_binding(
    binding: Mapping[str, Any] | None,
    *,
    expected_alias: str,
    dead_letter_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    evidence_sha256: str,
    not_after: datetime,
) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_REQUIRED")
    _require_exact_keys(binding, _APPLY_BINDING_KEYS, "APPLY_BINDING")
    if binding.get("contract") != CAMPAIGN_APPLY_EVIDENCE_BINDING_CONTRACT:
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_CONTRACT_INVALID")

    alias = require_alias(expected_alias)
    ordinal = _ordinal_for_alias(alias)
    expected_projection = {
        "alias": alias,
        "ordinal": ordinal,
        "action": CAMPAIGN_ACTION,
        "bucket": CAMPAIGN_BUCKET,
        "dead_letter_id_sha256": hashlib.sha256(
            _require_private_identifier(dead_letter_id).encode("utf-8")
        ).hexdigest(),
        "dead_letter_fingerprint": require_sha256(
            "EXPECTED_DEAD_LETTER_FINGERPRINT", expected_dead_letter_fingerprint
        ),
        "candidate_version": require_sha256(
            "EXPECTED_CANDIDATE_VERSION", expected_candidate_version
        ),
    }
    for key, expected in expected_projection.items():
        if type(binding.get(key)) is not type(expected) or binding.get(key) != expected:
            raise IncrementalDeadLetterCampaignError(f"APPLY_BINDING_TARGET_MISMATCH:{key.upper()}")

    normalized: dict[str, Any] = {
        "contract": CAMPAIGN_APPLY_EVIDENCE_BINDING_CONTRACT,
        **expected_projection,
    }
    for key in (
        "source_plan_report_sha256",
        "private_manifest_sha256",
        "target_set_sha256",
        "approved_preflight_report_sha256",
        "approved_database_main_sha256",
    ):
        normalized[key] = require_sha256(key.upper(), binding.get(key))
    database_size = binding.get("approved_database_main_size")
    if type(database_size) is not int or database_size <= 0:
        raise IncrementalDeadLetterCampaignError("APPROVED_DATABASE_MAIN_SIZE_INVALID")
    normalized["approved_database_main_size"] = database_size
    normalized.update(
        _normalize_predecessor(
            ordinal=ordinal,
            kind=binding.get("predecessor_kind"),
            report_sha256=binding.get("predecessor_report_sha256"),
            chain_sha256=binding.get("predecessor_chain_sha256"),
        )
    )
    normalized_evidence_sha256 = require_sha256("EVIDENCE_SHA256", evidence_sha256)
    if binding.get("evidence_sha256") != normalized_evidence_sha256:
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_EVIDENCE_MISMATCH")
    normalized["evidence_sha256"] = normalized_evidence_sha256

    generated_at = require_utc_z_timestamp("GENERATED_AT", binding.get("generated_at"))
    preflight_generated_at = require_utc_z_timestamp(
        "PREFLIGHT_GENERATED_AT", binding.get("preflight_generated_at")
    )
    if not_after.tzinfo is None or not_after.utcoffset() is None:
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_NOT_AFTER_INVALID")
    if not (preflight_generated_at <= generated_at <= not_after.astimezone(UTC)):
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_TIME_ORDER_INVALID")
    normalized["generated_at"] = str(binding.get("generated_at"))
    normalized["preflight_generated_at"] = str(binding.get("preflight_generated_at"))

    actual_digest = require_sha256(
        "APPROVAL_BINDING_SHA256", binding.get("approval_binding_sha256")
    )
    if actual_digest != canonical_sha256(normalized):
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_DIGEST_INVALID")
    normalized["approval_binding_sha256"] = actual_digest
    return normalized


def private_target_set_sha256(items: Sequence[Mapping[str, Any]]) -> str:
    normalized = [normalize_private_target(item) for item in items]
    return canonical_sha256({"contract": PRIVATE_TARGET_SET_CONTRACT, "items": normalized})


def campaign_chain_sha256(
    *,
    source_plan_report_sha256: str,
    target_set_sha256: str,
    alias: str,
    predecessor_chain_sha256: str | None,
    approval_binding_sha256: str,
    request_id_sha256: str,
    disposition_id_sha256: str,
) -> str:
    normalized_alias = require_alias(alias)
    ordinal = _ordinal_for_alias(normalized_alias)
    if ordinal == 1:
        if predecessor_chain_sha256 is not None:
            raise IncrementalDeadLetterCampaignError("CAMPAIGN_CHAIN_U01_PREDECESSOR_FORBIDDEN")
        normalized_predecessor = None
    else:
        normalized_predecessor = require_sha256(
            "PREDECESSOR_CHAIN_SHA256", predecessor_chain_sha256
        )
    return canonical_sha256(
        {
            "contract": CAMPAIGN_CHAIN_LINK_CONTRACT,
            "source_plan_report_sha256": require_sha256(
                "SOURCE_PLAN_REPORT_SHA256", source_plan_report_sha256
            ),
            "target_set_sha256": require_sha256("TARGET_SET_SHA256", target_set_sha256),
            "alias": normalized_alias,
            "ordinal": ordinal,
            "predecessor_chain_sha256": normalized_predecessor,
            "approval_binding_sha256": require_sha256(
                "APPROVAL_BINDING_SHA256", approval_binding_sha256
            ),
            "request_id_sha256": require_sha256("REQUEST_ID_SHA256", request_id_sha256),
            "disposition_id_sha256": require_sha256("DISPOSITION_ID_SHA256", disposition_id_sha256),
        }
    )


def canonical_json(value: object) -> str:
    _require_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise IncrementalDeadLetterCampaignError("CANONICAL_JSON_INVALID") from exc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def require_sha256(name: str, value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise IncrementalDeadLetterCampaignError(f"INVALID_SHA256:{name}")
    return value


def require_alias(value: object) -> str:
    if not isinstance(value, str) or _ALIAS_RE.fullmatch(value) is None:
        raise IncrementalDeadLetterCampaignError("CAMPAIGN_ALIAS_INVALID")
    return value


def require_utc_z_timestamp(name: str, value: object) -> datetime:
    if not isinstance(value, str) or _UTC_Z_RE.fullmatch(value) is None:
        raise IncrementalDeadLetterCampaignError(f"INVALID_UTC_TIMESTAMP:{name}")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise IncrementalDeadLetterCampaignError(f"INVALID_UTC_TIMESTAMP:{name}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise IncrementalDeadLetterCampaignError(f"INVALID_UTC_TIMESTAMP:{name}")
    return parsed


def _normalize_predecessor(
    *,
    ordinal: int,
    kind: object,
    report_sha256: object,
    chain_sha256: object,
) -> dict[str, Any]:
    if ordinal == 1:
        if kind is not None or report_sha256 is not None or chain_sha256 is not None:
            raise IncrementalDeadLetterCampaignError("APPLY_BINDING_U01_PREDECESSOR_FORBIDDEN")
        return {
            "predecessor_kind": None,
            "predecessor_report_sha256": None,
            "predecessor_chain_sha256": None,
        }
    if kind != CAMPAIGN_PREDECESSOR_KIND:
        raise IncrementalDeadLetterCampaignError("APPLY_BINDING_PREDECESSOR_KIND_INVALID")
    return {
        "predecessor_kind": CAMPAIGN_PREDECESSOR_KIND,
        "predecessor_report_sha256": require_sha256("PREDECESSOR_REPORT_SHA256", report_sha256),
        "predecessor_chain_sha256": require_sha256("PREDECESSOR_CHAIN_SHA256", chain_sha256),
    }


def _alias_for_ordinal(ordinal: int) -> str:
    return f"U{ordinal:02d}"


def _ordinal_for_alias(alias: str) -> int:
    return int(require_alias(alias)[1:])


def _require_private_identifier(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or value != value.strip()
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise IncrementalDeadLetterCampaignError("TARGET_MANIFEST_DEAD_LETTER_ID_INVALID")
    return value


def _require_exact_keys(
    value: Mapping[str, Any], expected: set[str] | frozenset[str], name: str
) -> None:
    if set(value) != set(expected):
        raise IncrementalDeadLetterCampaignError(f"{name}_KEYS_INVALID")


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    # Windows can refresh ctime metadata independently of file bytes while a
    # scanner opens a newly-created file.  Device/inode/mode/link-count/size and
    # mtime still pin both the object and its content without that false race.
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_mode),
        int(value.st_nlink),
        int(value.st_size),
        int(value.st_mtime_ns),
    )


def _reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number: {value}")
    return parsed


def _require_json_value(value: object) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise IncrementalDeadLetterCampaignError("CANONICAL_JSON_NONFINITE")
        return
    if isinstance(value, list):
        for item in value:
            _require_json_value(item)
        return
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise IncrementalDeadLetterCampaignError("CANONICAL_JSON_KEY_INVALID")
        for item in value.values():
            _require_json_value(item)
        return
    raise IncrementalDeadLetterCampaignError("CANONICAL_JSON_TYPE_INVALID")
