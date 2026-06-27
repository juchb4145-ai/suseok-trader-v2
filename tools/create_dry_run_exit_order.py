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
    from services.exit_engine import convert_exit_intent_to_dry_run_order
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Create an internal DRY_RUN exit order.")
    parser.add_argument("--dry-run-exit-intent-id", required=True)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        try:
            order = convert_exit_intent_to_dry_run_order(
                connection,
                args.dry_run_exit_intent_id,
                settings=settings,
            )
            payload = order.to_dict()
        except ValueError as exc:
            payload = _error_payload(str(exc))
            return_code = 1
        else:
            return_code = 0
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return return_code


def _error_payload(message: str) -> dict[str, object]:
    return {
        "status": "REJECTED",
        "error": message,
        "dry_run_only": True,
        "close_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
    }


if __name__ == "__main__":
    raise SystemExit(main())
