from __future__ import annotations

from typing import Any

ADVISORY_SCHEMA_VERSION = "ai-candidate-scorer-advisory.v1"

REQUIRED_OUTPUT_FIELDS = (
    "selected",
    "analysis",
    "score",
    "confidence",
    "risk_reward",
    "candidate_flags",
    "avoid",
    "summary",
    "no_trade_reason",
)

FORBIDDEN_OUTPUT_TOKENS = (
    "BUY_NOW",
    "SELL_NOW",
    "SEND_ORDER",
    "CANCEL_ORDER",
    "MODIFY_ORDER",
    "ORDERINTENT",
    "GATEWAYCOMMAND",
    "LIVE_SIM_INTENT",
    "LIVESIMINTENT",
    "KILL_SWITCH",
    "ACCOUNT_ID",
    "POSITION_SIZING",
)

MAX_SUMMARY_CHARS = 1000
MAX_ANALYSIS_CHARS = 500
MAX_AVOID_CHARS = 400
MAX_FLAG_CHARS = 64


def get_candidate_scorer_output_schema() -> dict[str, Any]:
    risk_reward_schema = {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "stop_loss_pct",
                "take_profit_pct",
                "trailing_stop_pct",
                "max_hold_sec",
            ],
            "properties": {
                "stop_loss_pct": {"type": "number"},
                "take_profit_pct": {"type": "number"},
                "trailing_stop_pct": {"type": "number"},
                "max_hold_sec": {"type": "integer"},
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_OUTPUT_FIELDS),
        "properties": {
            "selected": {"type": "array", "items": {"type": "string"}},
            "analysis": {"type": "object", "additionalProperties": {"type": "string"}},
            "score": {"type": "object", "additionalProperties": {"type": "number"}},
            "confidence": {"type": "object", "additionalProperties": {"type": "number"}},
            "risk_reward": risk_reward_schema,
            "candidate_flags": {
                "type": "object",
                "additionalProperties": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "avoid": {"type": "object", "additionalProperties": {"type": "string"}},
            "summary": {"type": "string"},
            "no_trade_reason": {"type": ["string", "null"]},
        },
    }

