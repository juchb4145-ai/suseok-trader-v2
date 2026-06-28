from __future__ import annotations

import argparse
import json
from typing import Any

from domain.broker.utils import normalize_value
from services.ai_advisory.storage import get_run, list_latest_scores, list_runs
from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect advisory-only AI candidate scores.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest", action="store_true", help="Show latest AI candidate scores.")
    group.add_argument("--run-id", help="Show a specific AI scoring run.")
    group.add_argument("--runs", action="store_true", help="List recent AI scoring runs.")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.latest:
            payload = list_latest_scores(connection, limit=args.limit)
        elif args.run_id:
            payload = get_run(connection, args.run_id) or {"error": "run not found"}
        else:
            payload = {"runs": list_runs(connection, limit=args.limit)}
    finally:
        connection.close()
    print(_json(payload))


def _json(value: Any) -> str:
    return json.dumps(normalize_value(value), ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()

