from __future__ import annotations

import json

from tools.validate_condition_profiles import validate_condition_profiles_file


def test_validate_condition_profiles_accepts_market_open_pack() -> None:
    result = validate_condition_profiles_file(
        "configs/condition_profiles/market_open_profiles.json"
    )

    assert result.valid is True
    assert result.profile_count == 5
    assert result.enabled_count == 5
    assert any("placeholder" in warning for warning in result.warnings)
    assert result.profiles[0]["role"] == "DISCOVERY"


def test_validate_condition_profiles_rejects_bad_profile(tmp_path) -> None:
    path = tmp_path / "bad_profiles.json"
    path.write_text(
        json.dumps(
            {
                "profiles": [
                    {
                        "profile_id": "bad",
                        "condition_name": "",
                        "role": "NOT_A_ROLE",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = validate_condition_profiles_file(path)

    assert result.valid is False
    assert result.errors
