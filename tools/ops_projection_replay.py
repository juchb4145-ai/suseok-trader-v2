# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.runtime.projection_replay import (
    DEFAULT_REPORT_ROOT,
    DEFAULT_WORK_ROOT,
    REPORT_FORMAT,
    export_replay_bundle,
    get_projection_replay_status,
    import_replay_bundle,
    run_projection_replay_parity,
    validate_replay_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export/import accepted Gateway events and compare isolated inline/worker "
            "market-data projections without trading side effects."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--source-db", required=True)
    export_parser.add_argument("--bundle-dir", required=True)
    export_parser.add_argument("--trade-date")
    export_parser.add_argument(
        "--event-types",
        default="price_tick,condition_event,tr_response",
    )

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--bundle-dir", required=True)

    import_parser = subparsers.add_parser("import")
    import_parser.add_argument("--bundle-dir", required=True)
    import_parser.add_argument("--target-db", required=True)
    import_parser.add_argument("--operational-db")

    parity_parser = subparsers.add_parser("parity")
    parity_parser.add_argument("--bundle-dir", required=True)
    parity_parser.add_argument("--work-root", default=str(DEFAULT_WORK_ROOT))
    parity_parser.add_argument("--operational-db")
    parity_parser.add_argument("--batch-size", type=int, default=50)
    parity_parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))

    args = parser.parse_args()
    if args.command == "export":
        result = export_replay_bundle(
            source_db_path=args.source_db,
            bundle_dir=args.bundle_dir,
            trade_date=args.trade_date,
            event_types=_event_types(args.event_types),
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.command == "validate":
        result = validate_replay_bundle(args.bundle_dir)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.command == "import":
        result = import_replay_bundle(
            bundle_dir=args.bundle_dir,
            target_db_path=args.target_db,
            operational_db_path=args.operational_db,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.command == "parity":
        result = run_projection_replay_parity(
            bundle_dir=args.bundle_dir,
            work_root=args.work_root,
            operational_db_path=args.operational_db,
            batch_size=args.batch_size,
        )
        report = build_projection_replay_report(result.to_dict())
        paths = write_projection_replay_report(report, out_dir=Path(args.out_dir))
        report["report_paths"] = {key: str(path) for key, path in paths.items()}
        print(render_console_summary(report))
        return 0 if result.status in {"PASS", "WARN"} else 2
    status = get_projection_replay_status(args.out_dir)
    print(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status["status"] in {"PASS", "WARN", "NOT_RUN"} else 2


def build_projection_replay_report(parity: Mapping[str, Any]) -> dict[str, Any]:
    failures = list(parity.get("failures") or [])
    warnings = list(parity.get("warnings") or [])
    status = str(parity.get("status") or "FAIL")
    return {
        "format": REPORT_FORMAT,
        "generated_at": _now(),
        "verdict": {
            "status": status,
            "failures": failures,
            "warnings": warnings,
            "block_next_pr": status == "FAIL",
            "projection_hash_match": bool(parity.get("projection_hash_match")),
            "no_order_side_effects": bool(parity.get("no_order_side_effects")),
            "no_trading_side_effects": bool(parity.get("no_trading_side_effects")),
        },
        "parity": dict(parity),
        "safety": {
            "observe_only": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
            "production_db_writes_allowed": False,
            "replay_databases_isolated": True,
            "operator_api_can_start_replay": False,
        },
    }


def write_projection_replay_report(
    report: Mapping[str, Any],
    *,
    out_dir: Path,
) -> dict[str, Path]:
    parity = report.get("parity")
    parity = parity if isinstance(parity, Mapping) else {}
    run_id = str(parity.get("run_id") or "run")
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
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    parity = report.get("parity")
    parity = parity if isinstance(parity, Mapping) else {}
    bundle = parity.get("bundle")
    bundle = bundle if isinstance(bundle, Mapping) else {}
    inline = parity.get("inline")
    inline = inline if isinstance(inline, Mapping) else {}
    worker = parity.get("worker")
    worker = worker if isinstance(worker, Mapping) else {}
    lines = [
        "# Projection Replay Parity",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- run_id: `{parity.get('run_id')}`",
        f"- trade_date: `{bundle.get('trade_date')}`",
        f"- event_count: `{bundle.get('event_count')}`",
        f"- venue_counts: `{_compact(bundle.get('venue_counts') or {})}`",
        f"- event_order_sha256: `{bundle.get('event_order_sha256')}`",
        f"- projection_hash_match: `{parity.get('projection_hash_match')}`",
        f"- mismatched_tables: `{', '.join(parity.get('mismatched_tables') or []) or '-'}`",
        f"- inline_reconcile: `{inline.get('reconcile_status')}`",
        f"- worker_reconcile: `{worker.get('reconcile_status')}`",
        f"- inline_outbox: `{_compact(inline.get('market_data_outbox_counts') or {})}`",
        f"- worker_outbox: `{_compact(worker.get('market_data_outbox_counts') or {})}`",
        f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
        f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
        "",
        "## Safety Evidence",
        "",
        f"- no_order_side_effects: `{verdict.get('no_order_side_effects')}`",
        f"- no_trading_side_effects: `{verdict.get('no_trading_side_effects')}`",
        "- production_db_writes_allowed: `false`",
        "- replay databases: isolated new files",
        "- API execution control: unavailable (read-only status only)",
        "",
        "KRX and NXT event/projection counts are reported separately. A condition_event "
        "is KRX evidence and is never promoted to NXT condition evidence.",
    ]
    return "\n".join(lines)


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    parity = report.get("parity")
    parity = parity if isinstance(parity, Mapping) else {}
    bundle = parity.get("bundle")
    bundle = bundle if isinstance(bundle, Mapping) else {}
    return (
        "Projection replay: "
        f"{verdict.get('status')} events={bundle.get('event_count')} "
        f"hash_match={verdict.get('projection_hash_match')} "
        f"side_effects={not bool(verdict.get('no_trading_side_effects'))}"
    )


def _event_types(value: str) -> Sequence[str]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
