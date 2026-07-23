from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import datetime_to_wire, normalize_value, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.pipeline_coherency import (
    assess_order_plan_lineage,
    resolve_candidate_source_lineage,
)

ORDER_PLAN_BINDING_CONTRACT = "live-sim-order-plan-binding.v1"

_BINDING_ID_FIELDS = (
    "order_plan_id",
    "idempotency_key",
    "trade_date",
    "candidate_instance_id",
    "entry_timing_evaluation_id",
    "strategy_observation_id",
    "risk_observation_id",
    "source_run_id",
    "source_watermark_hash",
)
_ORDER_PLAN_SNAPSHOT_FIELDS = (
    "order_plan_id",
    "idempotency_key",
    "trade_date",
    "candidate_instance_id",
    "code",
    "name",
    "side",
    "status",
    "setup_type",
    "entry_timing_state",
    "price_location_state",
    "theme_id",
    "theme_name",
    "theme_state",
    "theme_rank",
    "stock_role",
    "priority_score",
    "current_price",
    "limit_price",
    "limit_price_source",
    "limit_price_offset_ticks",
    "suggested_quantity",
    "suggested_notional",
    "max_notional",
    "risk_budget_source",
    "expires_at",
    "observe_only",
    "not_order_intent",
    "created_at",
    "entry_timing_evaluation_id",
    "strategy_observation_id",
    "risk_observation_id",
    "source_run_id",
    "source_watermark_hash",
    "source_event_id",
    "source_observed_at",
    "data_age_sec",
    "generated_by",
)
_ROW_BINDING_FIELDS = (
    "order_plan_id",
    "idempotency_key",
    "trade_date",
    "candidate_instance_id",
    "entry_timing_evaluation_id",
    "strategy_observation_id",
    "risk_observation_id",
    "source_run_id",
    "source_watermark_hash",
    "expires_at",
)
_PIPELINE_BINDING_FIELDS = (
    "trade_date",
    "candidate_instance_id",
    "strategy_observation_id",
    "risk_observation_id",
    "source_run_id",
    "source_watermark_hash",
)


@dataclass(frozen=True, kw_only=True)
class OrderPlanBindingValidationResult:
    passed: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    binding: Mapping[str, Any] = field(default_factory=dict)
    order_plan: Mapping[str, Any] = field(default_factory=dict)
    lineage: Mapping[str, Any] = field(default_factory=dict)
    checked_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": bool(self.passed),
            "status": "PASS" if self.passed else "FAIL",
            "reason_codes": list(self.reason_codes),
            "binding": normalize_value(dict(self.binding)),
            "order_plan": normalize_value(dict(self.order_plan)),
            "lineage": normalize_value(dict(self.lineage)),
            "checked_at": self.checked_at,
            "read_only": True,
            "no_order_side_effects": True,
        }


def build_order_plan_binding(plan: Mapping[str, Any] | sqlite3.Row) -> dict[str, Any]:
    """Build an immutable execution binding for one exact persisted order plan."""

    row = _row_dict(plan)
    if not row:
        raise ValueError("order plan is required")
    for field_name in _BINDING_ID_FIELDS:
        _required_text(row.get(field_name), field_name)
    source_watermark_hash = str(row["source_watermark_hash"])
    if not _is_sha256(source_watermark_hash):
        raise ValueError("source_watermark_hash must be a lowercase SHA-256")

    expires_at = datetime_to_wire(parse_timestamp(row.get("expires_at"), "expires_at"))
    snapshot = _canonical_order_plan_snapshot(row)
    snapshot_sha256 = _sha256_mapping(snapshot)
    binding_payload = {
        "contract": ORDER_PLAN_BINDING_CONTRACT,
        **{field_name: str(row[field_name]) for field_name in _BINDING_ID_FIELDS},
        "expires_at": expires_at,
        "order_plan_snapshot_sha256": snapshot_sha256,
    }
    return {
        **binding_payload,
        "binding_sha256": _sha256_mapping(binding_payload),
    }


def validate_order_plan_binding(
    connection: sqlite3.Connection,
    binding: Mapping[str, Any],
    *,
    max_age_sec: float,
    expected_trade_date: str | None = None,
    expected_intent: Mapping[str, Any] | sqlite3.Row | None = None,
) -> OrderPlanBindingValidationResult:
    """Revalidate a gate-selected plan with fixed, keyed, read-only lookups."""

    reasons: list[str] = []
    normalized_binding = _normalize_binding(binding, reasons)
    order_plan: dict[str, Any] = {}
    lineage: dict[str, Any] = {}

    try:
        bounded_max_age = float(max_age_sec)
        if not math.isfinite(bounded_max_age) or bounded_max_age < 0:
            raise ValueError
    except (TypeError, ValueError):
        bounded_max_age = 0.0
        reasons.append("ORDER_PLAN_BINDING_MAX_AGE_INVALID")

    order_plan_id = str(normalized_binding.get("order_plan_id") or "")
    idempotency_key = str(normalized_binding.get("idempotency_key") or "")
    entry_evaluation_id = str(
        normalized_binding.get("entry_timing_evaluation_id") or ""
    )
    strategy_observation_id = str(
        normalized_binding.get("strategy_observation_id") or ""
    )
    risk_observation_id = str(normalized_binding.get("risk_observation_id") or "")
    candidate_id = str(normalized_binding.get("candidate_instance_id") or "")

    base_row = _keyed_row(
        connection,
        table_name="order_plan_drafts",
        key_name="order_plan_id",
        key_value=order_plan_id,
        missing_reason="ORDER_PLAN_BINDING_ORDER_PLAN_NOT_FOUND",
        reasons=reasons,
    )
    latest_row = _keyed_row(
        connection,
        table_name="order_plan_drafts_latest",
        key_name="idempotency_key",
        key_value=idempotency_key,
        missing_reason="ORDER_PLAN_BINDING_LATEST_NOT_FOUND",
        reasons=reasons,
    )
    evaluation_row = _keyed_row(
        connection,
        table_name="entry_timing_evaluations",
        key_name="entry_timing_evaluation_id",
        key_value=entry_evaluation_id,
        missing_reason="ORDER_PLAN_BINDING_ENTRY_TIMING_NOT_FOUND",
        reasons=reasons,
    )
    strategy_row = _keyed_row(
        connection,
        table_name="strategy_observations",
        key_name="strategy_observation_id",
        key_value=strategy_observation_id,
        missing_reason="ORDER_PLAN_BINDING_STRATEGY_NOT_FOUND",
        reasons=reasons,
    )
    risk_row = _keyed_row(
        connection,
        table_name="risk_observations",
        key_name="risk_observation_id",
        key_value=risk_observation_id,
        missing_reason="ORDER_PLAN_BINDING_RISK_NOT_FOUND",
        reasons=reasons,
    )
    strategy_latest = _keyed_row(
        connection,
        table_name="strategy_observations_latest",
        key_name="candidate_instance_id",
        key_value=candidate_id,
        missing_reason="ORDER_PLAN_BINDING_STRATEGY_LATEST_NOT_FOUND",
        reasons=reasons,
    )
    risk_latest = _keyed_row(
        connection,
        table_name="risk_observations_latest",
        key_name="candidate_instance_id",
        key_value=candidate_id,
        missing_reason="ORDER_PLAN_BINDING_RISK_LATEST_NOT_FOUND",
        reasons=reasons,
    )

    if base_row:
        try:
            order_plan = _public_order_plan(base_row)
        except (TypeError, ValueError, json.JSONDecodeError):
            reasons.append("ORDER_PLAN_BINDING_ORDER_PLAN_SNAPSHOT_INVALID")
            order_plan = {
                key: normalize_value(value)
                for key, value in base_row.items()
                if key not in {"evidence_json", "source_watermark", "reason_codes_json"}
            }
        _check_order_plan_row(
            base_row,
            normalized_binding,
            prefix="ORDER_PLAN_BINDING_ORDER_PLAN",
            reasons=reasons,
        )
    if latest_row:
        _check_order_plan_row(
            latest_row,
            normalized_binding,
            prefix="ORDER_PLAN_BINDING_LATEST",
            reasons=reasons,
        )
    if base_row and latest_row:
        _check_same_values(
            base_row,
            latest_row,
            _ROW_BINDING_FIELDS,
            reason="ORDER_PLAN_NOT_LATEST",
            reasons=reasons,
        )

    if evaluation_row:
        _check_same_values(
            evaluation_row,
            normalized_binding,
            (
                "entry_timing_evaluation_id",
                *_PIPELINE_BINDING_FIELDS,
            ),
            reason="ORDER_PLAN_BINDING_ENTRY_TIMING_MISMATCH",
            reasons=reasons,
        )
        if str(evaluation_row.get("order_plan_id") or "") != order_plan_id:
            reasons.append("ORDER_PLAN_BINDING_ENTRY_TIMING_ORDER_PLAN_MISMATCH")
        _check_ready_observe_row(
            evaluation_row,
            prefix="ORDER_PLAN_BINDING_ENTRY_TIMING",
            reasons=reasons,
            require_not_order_intent=True,
        )

    if strategy_row:
        _check_same_values(
            strategy_row,
            normalized_binding,
            (
                "strategy_observation_id",
                "trade_date",
                "candidate_instance_id",
                "source_run_id",
                "source_watermark_hash",
            ),
            reason="ORDER_PLAN_BINDING_STRATEGY_MISMATCH",
            reasons=reasons,
        )
        if not _strict_true(strategy_row.get("observe_only")):
            reasons.append("ORDER_PLAN_BINDING_STRATEGY_NOT_OBSERVE_ONLY")
        if str(strategy_row.get("overall_status") or "") != "MATCHED_OBSERVATION":
            reasons.append("ORDER_PLAN_STRATEGY_NOT_MATCHED")

    if risk_row:
        _check_same_values(
            risk_row,
            normalized_binding,
            (
                "risk_observation_id",
                "strategy_observation_id",
                "trade_date",
                "candidate_instance_id",
                "source_run_id",
                "source_watermark_hash",
            ),
            reason="ORDER_PLAN_BINDING_RISK_MISMATCH",
            reasons=reasons,
        )
        if not _strict_true(risk_row.get("observe_only")):
            reasons.append("ORDER_PLAN_BINDING_RISK_NOT_OBSERVE_ONLY")
        if str(risk_row.get("overall_status") or "") != "OBSERVE_PASS":
            reasons.append("ORDER_PLAN_RISK_NOT_PASS")

    if strategy_latest:
        _check_same_values(
            strategy_latest,
            normalized_binding,
            (
                "strategy_observation_id",
                "trade_date",
                "candidate_instance_id",
                "source_run_id",
                "source_watermark_hash",
            ),
            reason="ORDER_PLAN_BINDING_STRATEGY_LATEST_MISMATCH",
            reasons=reasons,
        )
    if risk_latest:
        _check_same_values(
            risk_latest,
            normalized_binding,
            (
                "risk_observation_id",
                "strategy_observation_id",
                "trade_date",
                "candidate_instance_id",
                "source_run_id",
                "source_watermark_hash",
            ),
            reason="ORDER_PLAN_BINDING_RISK_LATEST_MISMATCH",
            reasons=reasons,
        )

    if expected_trade_date is not None:
        expected_date = str(expected_trade_date).strip()
        if not expected_date or normalized_binding.get("trade_date") != expected_date:
            reasons.append("ORDER_PLAN_BINDING_TRADE_DATE_MISMATCH")

    if expected_intent is not None:
        _check_expected_intent(
            expected_intent,
            normalized_binding,
            base_row,
            reasons=reasons,
        )

    if base_row and evaluation_row and bounded_max_age >= 0:
        try:
            lineage = dict(
                assess_order_plan_lineage(
                    connection,
                    base_row,
                    evaluation_row,
                    max_age_sec=bounded_max_age,
                )
            )
        except Exception:
            reasons.append("ORDER_PLAN_BINDING_LINEAGE_CLASSIFIER_ERROR")
        else:
            if lineage.get("status") != "PASS":
                reasons.append("ORDER_PLAN_LINEAGE_INVALID")
                reasons.extend(str(reason) for reason in lineage.get("reason_codes", []))
        try:
            current_lineage = resolve_candidate_source_lineage(
                connection,
                candidate_id,
                source_run_id=str(normalized_binding.get("source_run_id") or ""),
                generated_by="order_plan_binding_terminal",
                fallback_trade_date=str(
                    normalized_binding.get("trade_date") or ""
                ),
            )
        except Exception:
            reasons.append("ORDER_PLAN_BINDING_CURRENT_SOURCE_CLASSIFIER_ERROR")
        else:
            lineage["current_source"] = current_lineage
            if not current_lineage.get("candidate_present"):
                reasons.append("ORDER_PLAN_BINDING_CURRENT_SOURCE_MISSING")
            if (
                current_lineage.get("source_watermark_hash")
                != normalized_binding.get("source_watermark_hash")
            ):
                reasons.append("ORDER_PLAN_BINDING_CURRENT_SOURCE_WATERMARK_MISMATCH")
            if (
                current_lineage.get("trade_date")
                != normalized_binding.get("trade_date")
            ):
                reasons.append("ORDER_PLAN_BINDING_CURRENT_SOURCE_TRADE_DATE_MISMATCH")

    reasons = _dedupe(reasons)
    return OrderPlanBindingValidationResult(
        passed=not reasons,
        reason_codes=tuple(reasons),
        binding=normalized_binding,
        order_plan=order_plan,
        lineage=lineage,
    )


def _normalize_binding(
    binding: Mapping[str, Any],
    reasons: list[str],
) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        reasons.append("ORDER_PLAN_BINDING_INVALID")
        return {}
    item = {str(key): value for key, value in binding.items()}
    if item.get("contract") != ORDER_PLAN_BINDING_CONTRACT:
        reasons.append("ORDER_PLAN_BINDING_CONTRACT_MISMATCH")
    for field_name in _BINDING_ID_FIELDS:
        value = item.get(field_name)
        if not isinstance(value, str) or not value.strip():
            reasons.append(f"ORDER_PLAN_BINDING_{field_name.upper()}_MISSING")
        else:
            item[field_name] = value.strip()
    for field_name in (
        "source_watermark_hash",
        "order_plan_snapshot_sha256",
        "binding_sha256",
    ):
        if not _is_sha256(item.get(field_name)):
            reasons.append(f"ORDER_PLAN_BINDING_{field_name.upper()}_INVALID")
    try:
        item["expires_at"] = datetime_to_wire(
            parse_timestamp(item.get("expires_at"), "expires_at")
        )
    except (TypeError, ValueError):
        reasons.append("ORDER_PLAN_BINDING_EXPIRES_AT_INVALID")

    binding_sha256 = item.get("binding_sha256")
    payload = {
        "contract": item.get("contract"),
        **{field_name: item.get(field_name) for field_name in _BINDING_ID_FIELDS},
        "expires_at": item.get("expires_at"),
        "order_plan_snapshot_sha256": item.get("order_plan_snapshot_sha256"),
    }
    if _is_sha256(binding_sha256):
        try:
            expected_sha256 = _sha256_mapping(payload)
        except (TypeError, ValueError):
            reasons.append("ORDER_PLAN_BINDING_INVALID")
        else:
            if expected_sha256 != binding_sha256:
                reasons.append("ORDER_PLAN_BINDING_HASH_MISMATCH")
    return item


def _check_order_plan_row(
    row: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    prefix: str,
    reasons: list[str],
) -> None:
    _check_same_values(
        row,
        binding,
        _ROW_BINDING_FIELDS,
        reason=f"{prefix}_MISMATCH",
        reasons=reasons,
    )
    try:
        snapshot_sha256 = _sha256_mapping(_canonical_order_plan_snapshot(row))
    except (TypeError, ValueError, json.JSONDecodeError):
        reasons.append(f"{prefix}_SNAPSHOT_INVALID")
    else:
        if snapshot_sha256 != binding.get("order_plan_snapshot_sha256"):
            reasons.append(f"{prefix}_SNAPSHOT_MISMATCH")

    if str(row.get("status") or "") != "PLAN_READY":
        reasons.append("ORDER_PLAN_NOT_READY")
    if str(row.get("side") or "").upper() != "BUY":
        reasons.append("ORDER_PLAN_NOT_BUY")
    if not _strict_true(row.get("observe_only")) or not _strict_true(
        row.get("not_order_intent")
    ):
        reasons.append(f"{prefix}_NOT_OBSERVE_ONLY")
    try:
        expiry = parse_timestamp(row.get("expires_at"), "expires_at")
    except (TypeError, ValueError):
        reasons.append("ORDER_PLAN_BINDING_EXPIRES_AT_INVALID")
    else:
        if expiry <= utc_now():
            reasons.append("ORDER_PLAN_EXPIRED")


def _check_ready_observe_row(
    row: Mapping[str, Any],
    *,
    prefix: str,
    reasons: list[str],
    require_not_order_intent: bool,
) -> None:
    if str(row.get("status") or "") != "PLAN_READY":
        reasons.append(f"{prefix}_NOT_READY")
    if not _strict_true(row.get("observe_only")):
        reasons.append(f"{prefix}_NOT_OBSERVE_ONLY")
    if require_not_order_intent and not _strict_true(row.get("not_order_intent")):
        reasons.append(f"{prefix}_ORDER_INTENT_FLAG_INVALID")


def _check_expected_intent(
    expected_intent: Mapping[str, Any] | sqlite3.Row,
    binding: Mapping[str, Any],
    order_plan: Mapping[str, Any],
    *,
    reasons: list[str],
) -> None:
    intent = _row_dict(expected_intent)
    if not intent:
        reasons.append("ORDER_PLAN_BINDING_INTENT_INVALID")
        return
    _check_same_values(
        intent,
        binding,
        (
            "order_plan_id",
            "trade_date",
            "candidate_instance_id",
            "strategy_observation_id",
            "risk_observation_id",
        ),
        reason="ORDER_PLAN_BINDING_INTENT_MISMATCH",
        reasons=reasons,
    )
    for field_name in ("code", "side"):
        if str(intent.get(field_name) or "") != str(order_plan.get(field_name) or ""):
            reasons.append("ORDER_PLAN_BINDING_INTENT_MISMATCH")
            break
    try:
        evidence = _json_object(
            intent.get("evidence_json"),
            field_name="intent.evidence_json",
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        reasons.append("ORDER_PLAN_BINDING_INTENT_BINDING_INVALID")
        return
    nested = evidence.get("order_plan_binding")
    if not isinstance(nested, Mapping):
        reasons.append("ORDER_PLAN_BINDING_INTENT_BINDING_MISSING")
    elif nested.get("binding_sha256") != binding.get("binding_sha256"):
        reasons.append("ORDER_PLAN_BINDING_INTENT_BINDING_MISMATCH")


def _keyed_row(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    key_name: str,
    key_value: str,
    missing_reason: str,
    reasons: list[str],
) -> dict[str, Any]:
    if not key_value:
        reasons.append(missing_reason)
        return {}
    try:
        row = connection.execute(
            f"SELECT * FROM {table_name} WHERE {key_name} = ?",
            (key_value,),
        ).fetchone()
    except sqlite3.Error:
        reasons.append("ORDER_PLAN_BINDING_DATABASE_ERROR")
        return {}
    if row is None:
        reasons.append(missing_reason)
        return {}
    return _row_dict(row)


def _check_same_values(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    field_names: Sequence[str],
    *,
    reason: str,
    reasons: list[str],
) -> None:
    if any(_comparable(left.get(name)) != _comparable(right.get(name)) for name in field_names):
        reasons.append(reason)


def _canonical_order_plan_snapshot(plan: Mapping[str, Any]) -> dict[str, Any]:
    row = _row_dict(plan)
    snapshot = {field_name: row.get(field_name) for field_name in _ORDER_PLAN_SNAPSHOT_FIELDS}
    snapshot["expires_at"] = datetime_to_wire(
        parse_timestamp(snapshot.get("expires_at"), "expires_at")
    )
    for field_name in ("created_at", "source_observed_at"):
        if snapshot.get(field_name) is not None:
            snapshot[field_name] = datetime_to_wire(
                parse_timestamp(snapshot[field_name], field_name)
            )
    snapshot["observe_only"] = _strict_bool(
        snapshot.get("observe_only"),
        "observe_only",
    )
    snapshot["not_order_intent"] = _strict_bool(
        snapshot.get("not_order_intent"),
        "not_order_intent",
    )
    snapshot["reason_codes"] = _reason_codes(row)
    snapshot["evidence_json"] = _json_object(
        row.get("evidence_json"),
        field_name="evidence_json",
    )
    snapshot["source_watermark"] = _json_object(
        row.get("source_watermark"),
        field_name="source_watermark",
    )
    return snapshot


def _public_order_plan(row: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["observe_only"] = _strict_true(item.get("observe_only"))
    item["not_order_intent"] = _strict_true(item.get("not_order_intent"))
    item["reason_codes"] = _reason_codes(item)
    item["evidence_json"] = _json_object(
        item.get("evidence_json"),
        field_name="evidence_json",
    )
    item["source_watermark"] = _json_object(
        item.get("source_watermark"),
        field_name="source_watermark",
    )
    item.pop("reason_codes_json", None)
    return item


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    value = row.get("reason_codes")
    if value is None:
        value = row.get("reason_codes_json")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    if value is None:
        return []
    loaded = json.loads(str(value))
    if not isinstance(loaded, list):
        raise ValueError("reason_codes must be a JSON array")
    return [str(item) for item in loaded]


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if value is None or value == "":
        return {}
    loaded = json.loads(str(value))
    if not isinstance(loaded, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return loaded


def _row_dict(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    keys = getattr(value, "keys", None)
    getitem = getattr(value, "__getitem__", None)
    if callable(keys) and callable(getitem):
        return {str(key): getitem(key) for key in keys()}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, Mapping):
            return {str(key): item for key, item in result.items()}
    return {}


def _strict_bool(value: object, field_name: str) -> bool:
    if value is True or value == 1:
        return True
    if value is False or value == 0:
        return False
    raise ValueError(f"{field_name} must be boolean")


def _strict_true(value: object) -> bool:
    return value is True or value == 1


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _comparable(value: object) -> object:
    if value is None:
        return None
    return str(value)


def _sha256_mapping(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]
