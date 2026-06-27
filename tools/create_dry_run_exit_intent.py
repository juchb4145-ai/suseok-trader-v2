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
    from services.exit_engine import create_dry_run_exit_intent
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Create an internal DRY_RUN exit intent.")
    parser.add_argument("--dry-run-position-id", required=True)
    parser.add_argument("--exit-evaluation-id")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        intent = create_dry_run_exit_intent(
            connection,
            args.dry_run_position_id,
            exit_evaluation_id=args.exit_evaluation_id,
            settings=settings,
        )
        payload = intent.to_dict()
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
