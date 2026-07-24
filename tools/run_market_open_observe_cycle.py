from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "market_open_observe_cycle"
_REDACTED_VALUE = "[REDACTED]"
_SENSITIVE_REPORT_KEYS = frozenset(
    {
        "account_id",
        "account_ids",
        "account_number",
        "account_numbers",
        "account_no",
        "account_nos",
        "acct_id",
        "acct_ids",
        "acct_no",
        "acct_nos",
        "account",
        "broker_account_id",
        "token",
        "tokens",
        "access_token",
        "refresh_token",
        "api_key",
        "password",
        "passwords",
        "secret",
        "secrets",
        "authorization",
        "bearer",
        "credential",
        "credentials",
    }
)
_SENSITIVE_REPORT_KEY_SUFFIXES = (
    "_account_id",
    "_account_ids",
    "_account_number",
    "_account_numbers",
    "_account_no",
    "_account_nos",
    "_acct_id",
    "_acct_ids",
    "_acct_no",
    "_acct_nos",
    "_token",
    "_tokens",
    "_password",
    "_passwords",
    "_secret",
    "_secrets",
    "_api_key",
    "_authorization",
    "_bearer",
    "_credential",
    "_credentials",
)
_SENSITIVE_REPORT_CANONICAL_KEYS = frozenset(
    key.replace("_", "") for key in _SENSITIVE_REPORT_KEYS
)
_SENSITIVE_KOREAN_REPORT_KEYS = frozenset(
    {
        "계좌",
        "계좌id",
        "계좌번호",
        "비밀번호",
        "비밀키",
        "인증",
        "인증정보",
        "토큰",
    }
)
_HUMAN_TEXT_REPORT_KEYS = frozenset(
    {
        "error",
        "errors",
        "message",
        "messages",
        "summary",
        "warning",
        "warnings",
        "detail",
        "description",
        "exception",
        "traceback",
    }
)
_HUMAN_TEXT_REPORT_KEY_SUFFIXES = (
    "_error",
    "_errors",
    "_message",
    "_messages",
    "_summary",
    "_warning",
    "_warnings",
    "_description",
    "_exception",
)
_HUMAN_TEXT_REPORT_CONTAINER_KEYS = frozenset(
    {
        "error",
        "errors",
        "message",
        "messages",
        "warning",
        "warnings",
        "detail",
        "exception",
        "traceback",
    }
)
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_ACRONYM_CASE_BOUNDARY_RE = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")
_AUTHORIZATION_SCHEME_RE = re.compile(
    r"(?i)\b(bearer|basic|token)\s+[A-Za-z0-9._~+/=-]{4,}"
)
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"""(?ix)
    \b(
        account(?:[\s_-]?(?:id|number))?
        |[A-Za-z0-9_-]*(?:
            token
            |credentials?
            |authorization
            |bearer
            |password
            |secret
        )
        |[A-Za-z0-9_-]*api[\s_-]?key
        |계좌(?:\s*(?:id|번호))?
        |토큰
        |비밀번호
        |비밀키
    )\b
    (\s*[:=]\s*)
    (
        "[^"]*"
        |'[^']*'
        |\S+
    )
    """
)
_FORMATTED_ACCOUNT_NUMBER_RE = re.compile(
    r"(?<!\d)\d{4}(?:[- ]\d{4}){1,2}(?:[- ]\d{2,4})?(?!\d)"
)
_CONTIGUOUS_ACCOUNT_NUMBER_RE = re.compile(r"(?<!\d)\d{8,14}(?!\d)")
_KNOWN_TOKEN_RE = re.compile(
    r"""(?ix)
    \b(
        sk-[A-Za-z0-9_-]{8,}
        |gh[pousr]_[A-Za-z0-9]{20,}
        |github_pat_[A-Za-z0-9_]{20,}
        |eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}
    )\b
    """
)


def write_observe_cycle_report(
    payload: Mapping[str, object],
    *,
    report_root: str | Path = DEFAULT_REPORT_ROOT,
) -> dict[str, Path]:
    safe_payload = sanitize_observe_cycle_payload(payload)
    trade_date = str(
        safe_payload.get("trade_date") or datetime.now(tz=UTC).date().isoformat()
    )
    created_at = str(safe_payload.get("created_at") or datetime.now(tz=UTC).isoformat())
    timestamp = _safe_timestamp(created_at)
    output_dir = Path(report_root) / trade_date
    output_dir.mkdir(parents=True, exist_ok=True)

    run_json = output_dir / f"run_{timestamp}.json"
    run_md = output_dir / f"run_{timestamp}.md"
    run_json.write_text(
        json.dumps(safe_payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    run_md.write_text(_render_markdown(safe_payload), encoding="utf-8")
    return {"run_json": run_json, "run_md": run_md}


def _sanitize_report_payload(
    value: object,
    *,
    key_context: object | None = None,
    human_text_context: bool = False,
) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): (
                _REDACTED_VALUE
                if _is_sensitive_report_key(key)
                else _sanitize_report_payload(
                    item,
                    key_context=key,
                    human_text_context=(
                        human_text_context
                        or _is_human_text_report_container_key(key)
                    ),
                )
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _sanitize_report_payload(
                item,
                key_context=key_context,
                human_text_context=human_text_context,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            _sanitize_report_payload(
                item,
                key_context=key_context,
                human_text_context=human_text_context,
            )
            for item in value
        ]
    if isinstance(value, str):
        return _sanitize_report_text(
            value,
            redact_bare_account=(
                human_text_context or _is_human_text_report_key(key_context)
            ),
        )
    return value


def sanitize_observe_cycle_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    safe_payload = _sanitize_report_payload(payload)
    if not isinstance(safe_payload, dict):
        raise TypeError("observe cycle report payload must be a mapping")
    return safe_payload


def _is_sensitive_report_key(key: object) -> bool:
    normalized = _normalize_report_key(key)
    canonical = normalized.replace("_", "")
    compact_raw = re.sub(r"\s+", "", str(key).strip().lower())
    return (
        normalized in _SENSITIVE_REPORT_KEYS
        or canonical in _SENSITIVE_REPORT_CANONICAL_KEYS
        or normalized.endswith(_SENSITIVE_REPORT_KEY_SUFFIXES)
        or compact_raw in _SENSITIVE_KOREAN_REPORT_KEYS
    )


def _is_human_text_report_key(key: object | None) -> bool:
    if key is None:
        return False
    normalized = _normalize_report_key(key)
    return normalized in _HUMAN_TEXT_REPORT_KEYS or normalized.endswith(
        _HUMAN_TEXT_REPORT_KEY_SUFFIXES
    )


def _is_human_text_report_container_key(key: object) -> bool:
    return _normalize_report_key(key) in _HUMAN_TEXT_REPORT_CONTAINER_KEYS


def _normalize_report_key(key: object) -> str:
    normalized = _ACRONYM_CASE_BOUNDARY_RE.sub("_", str(key).strip())
    normalized = _CAMEL_CASE_BOUNDARY_RE.sub("_", normalized)
    return re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()


def _sanitize_report_text(
    value: str,
    *,
    redact_bare_account: bool,
) -> str:
    sanitized = _AUTHORIZATION_SCHEME_RE.sub(
        lambda match: f"{match.group(1)} {_REDACTED_VALUE}",
        value,
    )
    sanitized = _SENSITIVE_ASSIGNMENT_RE.sub(
        lambda match: (
            f"{match.group(1)}{match.group(2)}{_REDACTED_VALUE}"
        ),
        sanitized,
    )
    sanitized = _KNOWN_TOKEN_RE.sub(_REDACTED_VALUE, sanitized)
    if redact_bare_account:
        sanitized = _FORMATTED_ACCOUNT_NUMBER_RE.sub(
            _REDACTED_VALUE,
            sanitized,
        )
        sanitized = _CONTIGUOUS_ACCOUNT_NUMBER_RE.sub(
            _REDACTED_VALUE,
            sanitized,
        )
    return sanitized


def _render_markdown(payload: Mapping[str, object]) -> str:
    lines = [
        "# Market Open Observe Cycle",
        "",
        f"- run_id: `{_cell(payload.get('run_id'))}`",
        f"- trade_date: `{_cell(payload.get('trade_date'))}`",
        f"- status: `{_cell(payload.get('status'))}`",
        f"- observe_only: `{_cell(payload.get('observe_only'))}`",
        f"- not_order_intent: `{_cell(payload.get('not_order_intent'))}`",
        f"- live_real_allowed: `{_cell(payload.get('live_real_allowed'))}`",
        f"- queue_commands: `{_cell(payload.get('queue_commands'))}`",
        f"- send_order_delta: `{_cell(payload.get('send_order_delta'))}`",
        "",
        "## Stage Summary",
        "",
        "| Stage | Status | Reason codes | Counts | Summary |",
        "| --- | --- | --- | --- | --- |",
    ]
    stage_summary = payload.get("stage_summary")
    stages = stage_summary if isinstance(stage_summary, Mapping) else {}
    for stage_name, raw_stage in stages.items():
        stage = raw_stage if isinstance(raw_stage, Mapping) else {}
        lines.append(
            "| {stage} | {status} | {reasons} | {counts} | {summary} |".format(
                stage=_md_cell(stage_name),
                status=_md_cell(stage.get("status")),
                reasons=_md_cell(", ".join(str(item) for item in stage.get("reason_codes") or [])),
                counts=_md_cell(_json_excerpt(stage.get("counts"))),
                summary=_md_cell(stage.get("summary")),
            )
        )
    lines.extend(
        [
            "",
            "## Command Safety",
            "",
            f"- send_order_count_before: `{_cell(payload.get('send_order_count_before'))}`",
            f"- send_order_count_after: `{_cell(payload.get('send_order_count_after'))}`",
            f"- send_order_delta: `{_cell(payload.get('send_order_delta'))}`",
            f"- no_order_side_effects: `{_cell(payload.get('no_order_side_effects'))}`",
            f"- real_order_allowed: `{_cell(payload.get('real_order_allowed'))}`",
            f"- order_controls_available: `{_cell(payload.get('order_controls_available'))}`",
            "",
            "## Warnings",
            "",
        ]
    )
    warnings = payload.get("warnings")
    warning_items = warnings if isinstance(warnings, list) else []
    if warning_items:
        lines.extend(f"- `{_cell(item)}`" for item in warning_items)
    else:
        lines.append("- None")
    lines.extend(["", "## Errors", ""])
    errors = payload.get("errors")
    error_items = errors if isinstance(errors, list) else []
    if error_items:
        lines.extend(f"- `{_cell(_json_excerpt(item))}`" for item in error_items)
    else:
        lines.append("- None")
    lines.append("")
    return "\n".join(lines)


def _safe_timestamp(value: str) -> str:
    return (
        value.replace(":", "")
        .replace("-", "")
        .replace(".", "")
        .replace("+", "p")
        .replace("Z", "z")
    )


def _json_excerpt(value: object, *, max_chars: int = 220) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        rendered = str(value)
    return rendered if len(rendered) <= max_chars else f"{rendered[: max_chars - 3]}..."


def _cell(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


def _md_cell(value: object) -> str:
    return _cell(value).replace("|", "\\|").replace("\n", " ")


def main() -> int:
    from services.config import load_settings
    from services.runtime.market_open_observe_cycle import (
        run_market_open_observe_cycle_once,
    )
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Run one observe-only market-open pipeline cycle."
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_REPORT_ROOT),
        help="Directory root for run_<timestamp>.json/.md reports.",
    )
    parser.add_argument(
        "--no-write-run",
        action="store_true",
        help="Run the cycle without persisting market_open_observe_cycle_runs.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = run_market_open_observe_cycle_once(
            connection,
            settings=settings,
            trade_date=args.trade_date,
            limit=args.limit,
            write_run=not args.no_write_run,
        )
        payload = result.to_dict()
    finally:
        connection.close()
    report_paths = write_observe_cycle_report(payload, report_root=args.out_dir)
    safe_payload = sanitize_observe_cycle_payload(payload)
    safe_payload["report_paths"] = {
        key: str(path) for key, path in report_paths.items()
    }
    print(json.dumps(safe_payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
