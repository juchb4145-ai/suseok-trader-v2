from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.config import load_settings
    from services.exit_engine import get_exit_evaluation, list_exit_evaluations, list_exit_signals
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Inspect DRY_RUN exit observations.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--dry-run-position-id")
    target.add_argument("--exit-evaluation-id")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.exit_evaluation_id:
            payload = get_exit_evaluation(
                connection,
                args.exit_evaluation_id,
                include_signals=True,
            )
        else:
            payload = {
                "evaluations": list_exit_evaluations(
                    connection,
                    dry_run_position_id=args.dry_run_position_id,
                    limit=20,
                ),
                "signals": list_exit_signals(
                    connection,
                    dry_run_position_id=args.dry_run_position_id,
                    limit=50,
                ),
            }
    finally:
        connection.close()
    if payload is None:
        print("dry-run exit record not found")
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
