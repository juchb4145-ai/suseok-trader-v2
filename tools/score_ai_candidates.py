from __future__ import annotations

import argparse
import json
from typing import Any

from domain.broker.utils import normalize_value
from services.ai_advisory.service import score_ai_candidates
from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> None:
    parser = argparse.ArgumentParser(description="Run advisory-only AI candidate scoring.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview context/prompt without DB save.",
    )
    parser.add_argument("--trade-date", help="Trade date filter, YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=None, help="Candidate limit.")
    parser.add_argument(
        "--provider",
        choices=("mock", "external", "external_http", "openai"),
        default=None,
        help="Provider override. External providers still require --allow-external and env flags.",
    )
    parser.add_argument(
        "--allow-external",
        action="store_true",
        help="Allow an external provider call when all AI_EXTERNAL_LLM flags are enabled.",
    )
    parser.add_argument(
        "--store-raw-response",
        action="store_true",
        help="Store redacted raw response only when explicit storage flags are also enabled.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = score_ai_candidates(
            connection,
            trade_date=args.trade_date,
            limit=args.limit,
            dry_run=args.dry_run,
            provider_name=args.provider,
            allow_external=args.allow_external,
            store_raw_response=args.store_raw_response,
            settings=settings,
        )
    finally:
        connection.close()
    print(_json(result.to_dict()))


def _json(value: Any) -> str:
    return json.dumps(normalize_value(value), ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
