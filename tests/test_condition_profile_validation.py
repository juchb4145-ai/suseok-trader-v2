from __future__ import annotations

import json

from tools.validate_condition_profiles import validate_condition_profiles_file


def test_validate_condition_profiles_accepts_market_open_pack() -> None:
    result = validate_condition_profiles_file(
        "configs/condition_profiles/market_open_profiles.json"
    )

    assert result.valid is True
    assert result.profile_count == 6
    assert result.enabled_count == 5
    assert result.warnings == ()
    by_id = {profile["profile_id"]: profile for profile in result.profiles}
    assert by_id["theme_spread_follower"]["enabled"] is False
    assert by_id["risk_overheat_block"]["price_subscribe_policy"] == "none"
    assert by_id["risk_overheat_block"]["role"] == "RISK_BLOCK"
    enabled_positive = [
        profile
        for profile in result.profiles
        if profile["enabled"] and profile["role"] != "RISK_BLOCK"
    ]
    assert enabled_positive
    assert {profile["price_subscribe_policy"] for profile in enabled_positive} == {"batch"}


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


def test_validate_condition_profiles_warns_on_enabled_immediate_initial(tmp_path) -> None:
    path = tmp_path / "immediate_profiles.json"
    path.write_text(
        json.dumps(
            {
                "profiles": [
                    {
                        "profile_id": "manual_fast",
                        "condition_name": "ManualFast",
                        "role": "LEADER",
                        "price_subscribe_policy": "immediate",
                        "max_initial": 1,
                        "enabled": True,
                        "screen_no": "7600",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = validate_condition_profiles_file(path)

    assert result.valid is True
    assert any(
        "immediate" in warning and "max_initial > 0" in warning for warning in result.warnings
    )


def test_validate_condition_profiles_rejects_duplicate_screen_no(tmp_path) -> None:
    path = tmp_path / "duplicate_screen_profiles.json"
    path.write_text(
        json.dumps(
            {
                "profiles": [
                    {
                        "profile_id": "one",
                        "condition_name": "One",
                        "role": "DISCOVERY",
                        "screen_no": "7600",
                    },
                    {
                        "profile_id": "two",
                        "condition_name": "Two",
                        "role": "LEADER",
                        "screen_no": "7600",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = validate_condition_profiles_file(path)

    assert result.valid is False
    assert "duplicate screen_no: 7600" in result.errors
