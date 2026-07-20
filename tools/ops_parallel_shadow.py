# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import new_message_id
from services.parallel_shadow.engine import load_parallel_shadow_frame, run_parallel_shadow
from services.parallel_shadow.models import PARALLEL_SHADOW_REPORT_FORMAT
from services.profit_lab.models import ProfitLabConfig

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "parallel_shadow"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run file-only FAST-3 parallel shadow execution and compare optional "
            "LIVE_SIM evidence without order, broker, or database access."
        )
    )
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--config-file")
    parser.add_argument("--commit-sha")
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))
    args = parser.parse_args()

    frame = load_parallel_shadow_frame(args.input_file)
    result = run_parallel_shadow(
        frame,
        config=_load_config(args.config_file),
        commit_sha=args.commit_sha or current_commit_sha(),
    )
    report = build_parallel_shadow_report(result.to_dict(), run_id=new_message_id("shadow"))
    paths = write_parallel_shadow_report(report, out_dir=Path(args.out_dir))
    print(render_console_summary(report, report_paths=paths))
    return 2 if result.status == "BLOCKED" else 0


def current_commit_sha() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT_DIR,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"
    return completed.stdout.strip() or "UNKNOWN"


def build_parallel_shadow_report(
    result: Mapping[str, Any],
    *,
    run_id: str,
) -> dict[str, Any]:
    safety = _mapping(result.get("safety"))
    return {
        "format": PARALLEL_SHADOW_REPORT_FORMAT,
        "generated_at": _now(),
        "run_id": run_id,
        "verdict": {
            "status": result.get("status"),
            "blocker_reasons": list(result.get("blocker_reasons") or []),
            "warnings": list(result.get("warnings") or []),
            "no_trading_side_effects": bool(safety.get("no_trading_side_effects")),
        },
        "parallel_shadow": dict(result),
        "safety": {
            "file_only": True,
            "observe_only": True,
            "operational_db_opened": False,
            "operational_db_writes_allowed": False,
            "gateway_command_writes_allowed": False,
            "live_sim_writes_allowed": False,
            "broker_calls_available": False,
            "live_real_allowed": False,
        },
    }


def write_parallel_shadow_report(
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
    verdict = _mapping(report.get("verdict"))
    shadow = _mapping(report.get("parallel_shadow"))
    metrics = _mapping(shadow.get("metrics"))
    identity = _mapping(shadow.get("identity"))
    return "\n".join(
        [
            "# FAST-3 Parallel Shadow Execution",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- status: `{verdict.get('status')}`",
            f"- result_sha256: `{shadow.get('result_sha256')}`",
            f"- input_sha256: `{identity.get('input_sha256')}`",
            f"- source_plan_count: `{metrics.get('source_plan_count')}`",
            f"- coherent_plan_ready_count: `{metrics.get('coherent_plan_ready_count')}`",
            f"- shadow_execution_count: `{metrics.get('shadow_execution_count')}`",
            f"- shadow_fill_count: `{metrics.get('shadow_fill_count')}`",
            f"- live_canary_plan_count: `{metrics.get('live_canary_plan_count')}`",
            f"- fill_disagreement_count: `{metrics.get('fill_disagreement_count')}`",
            "- comparison_linkage_gap_count: "
            f"`{metrics.get('comparison_linkage_gap_count')}`",
            "- live_buy_count_when_blocked: "
            f"`{metrics.get('live_buy_count_when_blocked')}`",
            "- blocker_reasons: "
            f"`{', '.join(verdict.get('blocker_reasons') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "## Safety",
            "",
            "- input and output: JSON/Markdown files only",
            "- operational database: not opened",
            "- GatewayCommand/LIVE_SIM writes: `0`",
            "- order and broker calls: unavailable",
            "- LIVE_REAL: forbidden",
            "- AI: advisory-only and excluded from selection/execution",
        ]
    )


def render_console_summary(
    report: Mapping[str, Any],
    *,
    report_paths: Mapping[str, Path],
) -> str:
    verdict = _mapping(report.get("verdict"))
    shadow = _mapping(report.get("parallel_shadow"))
    metrics = _mapping(shadow.get("metrics"))
    return (
        "FAST-3 Parallel Shadow: "
        f"{verdict.get('status')} plans={metrics.get('coherent_plan_ready_count')} "
        f"shadow={metrics.get('shadow_execution_count')} "
        f"live={metrics.get('live_canary_plan_count')} "
        f"disagreements={metrics.get('fill_disagreement_count')} "
        f"report={report_paths['summary_md']}"
    )


def _load_config(path_value: str | None) -> ProfitLabConfig:
    if not path_value:
        return ProfitLabConfig()
    path = Path(path_value).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"parallel shadow config is missing: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("parallel shadow config must be a JSON object")
    return ProfitLabConfig.from_mapping(value)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
