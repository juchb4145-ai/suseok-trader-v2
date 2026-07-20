# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import new_message_id
from services.champion_challenger import (
    CHAMPION_CHALLENGER_REPORT_FORMAT,
    evaluate_experiment,
    load_experiment_bundle,
)

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "champion_challenger"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate one file-bound FAST-6 Champion and one or two Challengers. "
            "The tool is review-only and cannot promote, order, call a broker, or open a database."
        )
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))
    args = parser.parse_args()

    bundle = load_experiment_bundle(args.manifest)
    result = evaluate_experiment(bundle)
    report = build_champion_challenger_report(
        result.to_dict(),
        run_id=new_message_id("champion-challenger"),
    )
    paths = write_champion_challenger_report(report, out_dir=Path(args.out_dir))
    print(render_console_summary(report, report_paths=paths))
    return 2 if result.status.startswith("BLOCKED") else 0


def build_champion_challenger_report(
    result: Mapping[str, Any],
    *,
    run_id: str,
) -> dict[str, Any]:
    safety = _mapping(result.get("safety"), "safety")
    return {
        "format": CHAMPION_CHALLENGER_REPORT_FORMAT,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "run_id": run_id,
        "verdict": {
            "status": result.get("status"),
            "blocker_reasons": list(result.get("blocker_reasons") or []),
            "warnings": list(result.get("warnings") or []),
            "no_trading_side_effects": bool(safety.get("no_trading_side_effects")),
        },
        "champion_challenger": dict(result),
        "safety": {
            "file_only": True,
            "observe_only": True,
            "review_only": True,
            "operational_db_opened": False,
            "operational_db_writes_allowed": False,
            "gateway_command_writes_allowed": False,
            "live_sim_writes_allowed": False,
            "broker_calls_available": False,
            "automatic_promotion_available": False,
            "live_real_allowed": False,
        },
    }


def write_champion_challenger_report(
    report: Mapping[str, Any],
    *,
    out_dir: Path,
) -> dict[str, Path]:
    run_id = str(report.get("run_id") or "run")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir.expanduser().resolve() / f"{stamp}_{run_id[-8:]}"
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"), "verdict")
    result = _mapping(report.get("champion_challenger"), "champion_challenger")
    identity = _mapping(result.get("identity"), "identity")
    promotion = _mapping(result.get("promotion"), "promotion")
    comparisons = result.get("comparisons") or []
    lines = [
        "# FAST-6 Champion / Challenger",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- experiment_id: `{identity.get('experiment_id')}`",
        f"- status: `{verdict.get('status')}`",
        f"- changed_axis: `{identity.get('changed_axis')}`",
        f"- result_sha256: `{identity.get('result_sha256')}`",
        f"- selected_challenger_id: `{promotion.get('selected_challenger_id')}`",
        f"- promotion_mode: `{promotion.get('mode')}`",
        f"- promotion_applied: `{promotion.get('applied')}`",
        f"- no_trading_side_effects: `{verdict.get('no_trading_side_effects')}`",
        "",
        "## Comparisons",
        "",
    ]
    for item in comparisons:
        comparison = _mapping(item, "comparison")
        lines.append(
            f"- `{comparison.get('candidate_id')}`: `{comparison.get('verdict')}` "
            f"(OOS improvement `{comparison.get('oos_improvement_ratio')}`)"
        )
    lines.extend(
        [
            "",
            "## Blockers",
            "",
            *[f"- `{item}`" for item in verdict.get("blocker_reasons") or ["NONE"]],
            "",
            "This report is advisory only. It does not promote a strategy or activate LIVE_SIM.",
            "",
        ]
    )
    return "\n".join(lines)


def render_console_summary(
    report: Mapping[str, Any],
    *,
    report_paths: Mapping[str, Path],
) -> str:
    verdict = _mapping(report.get("verdict"), "verdict")
    result = _mapping(report.get("champion_challenger"), "champion_challenger")
    promotion = _mapping(result.get("promotion"), "promotion")
    return (
        "FAST-6 Champion/Challenger: "
        f"status={verdict.get('status')} "
        f"selected={promotion.get('selected_challenger_id') or 'NONE'} "
        f"promotion_applied={promotion.get('applied')} "
        f"report={report_paths['raw_json']}"
    )


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
