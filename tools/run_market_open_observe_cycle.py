from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "market_open_observe_cycle"


def write_observe_cycle_report(
    payload: Mapping[str, object],
    *,
    report_root: str | Path = DEFAULT_REPORT_ROOT,
) -> dict[str, Path]:
    trade_date = str(payload.get("trade_date") or datetime.now(tz=UTC).date().isoformat())
    created_at = str(payload.get("created_at") or datetime.now(tz=UTC).isoformat())
    timestamp = _safe_timestamp(created_at)
    output_dir = Path(report_root) / trade_date
    output_dir.mkdir(parents=True, exist_ok=True)

    run_json = output_dir / f"run_{timestamp}.json"
    run_md = output_dir / f"run_{timestamp}.md"
    run_json.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    run_md.write_text(_render_markdown(payload), encoding="utf-8")
    return {"run_json": run_json, "run_md": run_md}


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
    payload["report_paths"] = {key: str(path) for key, path in report_paths.items()}
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
