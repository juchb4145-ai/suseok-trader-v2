from __future__ import annotations

import argparse
import json

from services.candidate_service import get_candidate
from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect an observe-only candidate episode.")
    parser.add_argument("--candidate-instance-id", required=True)
    parser.add_argument("--include-context", action="store_true")
    parser.add_argument("--include-sources", action="store_true")
    parser.add_argument("--include-transitions", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        candidate = get_candidate(
            connection,
            args.candidate_instance_id,
            include_context=args.include_context,
            include_sources=args.include_sources,
            include_transitions=args.include_transitions,
        )
    finally:
        connection.close()

    if candidate is None:
        print("candidate not found")
        return 1
    print(json.dumps(candidate, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
