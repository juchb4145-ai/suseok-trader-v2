from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.codex_prompt_generator import build_safety_review_prompt
    from services.config import load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Build the PR10 safety-review Codex prompt.")
    parser.add_argument("--preview-chars", type=int, default=1000)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = build_safety_review_prompt(connection, settings=settings)
    finally:
        connection.close()

    if result.draft is None:
        payload = result.to_dict()
    else:
        payload = {
            "ok": result.ok,
            "draft_id": result.draft.draft_id,
            "status": result.draft.status.value,
            "title": result.draft.title,
            "target_area": result.draft.target_area.value,
            "prompt_preview": result.draft.prompt_text[: max(int(args.preview_chars), 1)],
            "auto_apply_allowed": False,
            "github_write_allowed": False,
            "codex_execution_allowed": False,
            "no_trading_side_effects": True,
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
