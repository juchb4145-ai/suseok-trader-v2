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
from services.profit_lab.engine import load_profit_lab_signals, run_profit_lab
from services.profit_lab.models import PROFIT_LAB_REPORT_FORMAT, ProfitLabConfig

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "profit_lab"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate point-in-time replay signals with conservative fill, exit, and "
            "cost models. The tool is file-only and cannot create orders or DB writes."
        )
    )
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--alpha-report", required=True)
    parser.add_argument("--signals-file")
    parser.add_argument("--config-file")
    parser.add_argument("--commit-sha")
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))
    args = parser.parse_args()

    config = _load_config(args.config_file)
    manifest = load_profit_lab_signals(args.signals_file) if args.signals_file else None
    result = run_profit_lab(
        bundle_dir=args.bundle_dir,
        alpha_replay_report=args.alpha_report,
        signal_manifest=manifest,
        config=config,
        commit_sha=args.commit_sha or current_commit_sha(),
    )
    report = build_profit_lab_report(result.to_dict(), run_id=new_message_id("profit_lab"))
    paths = write_profit_lab_report(report, out_dir=Path(args.out_dir))
    print(render_console_summary(report, report_paths=paths))
    return 0 if result.status in {"PASS", "WARN"} else 2


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


def build_profit_lab_report(result: Mapping[str, Any], *, run_id: str) -> dict[str, Any]:
    return {
        "format": PROFIT_LAB_REPORT_FORMAT,
        "generated_at": _now(),
        "run_id": run_id,
        "verdict": {
            "status": result.get("status"),
            "qualification": result.get("qualification"),
            "qualification_reasons": list(result.get("qualification_reasons") or []),
            "warnings": list(result.get("warnings") or []),
            "no_trading_side_effects": bool(
                _mapping(result.get("safety")).get("no_trading_side_effects")
            ),
        },
        "profit_lab": dict(result),
        "safety": {
            "file_only": True,
            "observe_only": True,
            "operational_db_opened": False,
            "production_db_writes_allowed": False,
            "gateway_command_writes_allowed": False,
            "live_sim_writes_allowed": False,
            "dry_run_writes_allowed": False,
            "broker_calls_available": False,
        },
    }


def write_profit_lab_report(
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
    lab = _mapping(report.get("profit_lab"))
    metrics = _mapping(lab.get("metrics"))
    identity = _mapping(lab.get("identity"))
    lines = [
        "# FAST-2B Conservative Profit Lab",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- qualification: `{verdict.get('qualification')}`",
        f"- result_sha256: `{lab.get('result_sha256')}`",
        f"- alpha_replay_result_sha256: `{identity.get('alpha_replay_result_sha256')}`",
        f"- signal_count: `{lab.get('signal_count')}`",
        f"- tick_count: `{lab.get('tick_count')}`",
        f"- entry_fill_count: `{metrics.get('entry_fill_count')}`",
        f"- closed_trade_count: `{metrics.get('closed_trade_count')}`",
        f"- fill_rate: `{metrics.get('fill_rate')}`",
        f"- net_pnl: `{metrics.get('net_pnl')}`",
        f"- net_expectancy: `{metrics.get('net_expectancy')}`",
        f"- expectancy_r: `{metrics.get('expectancy_r')}`",
        f"- profit_factor: `{metrics.get('profit_factor')}`",
        f"- max_drawdown_r: `{metrics.get('max_drawdown_r')}`",
        f"- cost_model_complete: `{lab.get('cost_model_complete')}`",
        "- qualification_reasons: "
        f"`{', '.join(verdict.get('qualification_reasons') or []) or '-'}`",
        f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
        "",
        "## Safety",
        "",
        "- input: validated FAST-2A bundle/report and optional signal/config JSON",
        "- operational database: not opened",
        "- GatewayCommand/LIVE_SIM/DRY_RUN writes: `0`",
        "- order and broker calls: unavailable",
        "- BUY/SELL fills require future ticks after configured latency",
        "- STOP exits use the first later tick and preserve gap-down loss",
    ]
    return "\n".join(lines)


def render_console_summary(
    report: Mapping[str, Any],
    *,
    report_paths: Mapping[str, Path],
) -> str:
    verdict = _mapping(report.get("verdict"))
    lab = _mapping(report.get("profit_lab"))
    metrics = _mapping(lab.get("metrics"))
    return (
        "FAST-2B Profit Lab: "
        f"{verdict.get('status')} qualification={verdict.get('qualification')} "
        f"signals={lab.get('signal_count')} fills={metrics.get('entry_fill_count')} "
        f"closed={metrics.get('closed_trade_count')} "
        f"report={report_paths['summary_md']}"
    )


def _load_config(path_value: str | None) -> ProfitLabConfig:
    if not path_value:
        return ProfitLabConfig()
    path = Path(path_value).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Profit Lab config is missing: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("Profit Lab config must be a JSON object")
    return ProfitLabConfig.from_mapping(value)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
