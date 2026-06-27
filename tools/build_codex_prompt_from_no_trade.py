from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.codex_prompt_generator import build_codex_prompt_from_no_trade
    from services.config import candidate_timezone, load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Build a no-trade Codex prompt draft.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--run-ai", action="store_true")
    parser.add_argument("--preview-chars", type=int, default=1000)
    args = parser.parse_args()

    settings = load_settings()
    timezone = candidate_timezone(settings.candidate_trade_date_timezone)
    trade_date = args.trade_date or _today(timezone)
    connection = initialize_database(settings.trading_db_path)
    try:
        result = build_codex_prompt_from_no_trade(
            connection,
            trade_date,
            run_ai=args.run_ai,
            settings=settings,
        )
    finally:
        connection.close()

    print(json.dumps(_payload(result, args.preview_chars), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def _today(timezone) -> str:
    from datetime import datetime

    return datetime.now(timezone).date().isoformat()


def _payload(result, preview_chars: int) -> dict[str, object]:
    if result.draft is None:
        return result.to_dict()
    return {
        "ok": result.ok,
        "draft_id": result.draft.draft_id,
        "status": result.draft.status.value,
        "title": result.draft.title,
        "target_area": result.draft.target_area.value,
        "run_ai": result.draft.run_ai,
        "ai_request_id": result.draft.ai_request_id,
        "ai_insight_id": result.draft.ai_insight_id,
        "prompt_preview": result.draft.prompt_text[: max(int(preview_chars), 1)],
        "auto_apply_allowed": False,
        "github_write_allowed": False,
        "codex_execution_allowed": False,
        "no_trading_side_effects": True,
    }


if __name__ == "__main__":
    raise SystemExit(main())
