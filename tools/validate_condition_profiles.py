from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from domain.broker.condition_profiles import (
    ConditionProfile,
    ConditionRole,
    PriceSubscribePolicy,
    parse_condition_profiles,
)


@dataclass(frozen=True, kw_only=True)
class ConditionProfileValidationResult:
    valid: bool
    profile_count: int = 0
    enabled_count: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    profiles: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "profile_count": self.profile_count,
            "enabled_count": self.enabled_count,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "profiles": list(self.profiles),
        }


def validate_condition_profiles_file(path: str | Path) -> ConditionProfileValidationResult:
    target = Path(path)
    errors: list[str] = []
    warnings: list[str] = []
    if not target.exists():
        return ConditionProfileValidationResult(
            valid=False,
            errors=(f"profile file not found: {target}",),
        )
    try:
        raw = target.read_text(encoding="utf-8")
    except OSError as exc:
        return ConditionProfileValidationResult(valid=False, errors=(str(exc),))

    try:
        profiles = parse_condition_profiles(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        return ConditionProfileValidationResult(valid=False, errors=(str(exc),))

    profile_ids: set[str] = set()
    condition_names: set[str] = set()
    screen_numbers: set[str] = set()
    normalized_profiles: list[dict[str, Any]] = []
    for profile in profiles:
        normalized_profiles.append(profile.to_dict())
        if profile.profile_id in profile_ids:
            errors.append(f"duplicate profile_id: {profile.profile_id}")
        profile_ids.add(profile.profile_id)
        if profile.condition_name in condition_names:
            warnings.append(f"duplicate condition_name: {profile.condition_name}")
        condition_names.add(profile.condition_name)
        if profile.screen_no:
            if profile.screen_no in screen_numbers:
                errors.append(f"duplicate screen_no: {profile.screen_no}")
            screen_numbers.add(profile.screen_no)
        _add_operational_warnings(profile, warnings)

    if not profiles:
        errors.append("at least one condition profile is required")
    enabled_count = sum(1 for profile in profiles if profile.enabled)
    if profiles and enabled_count <= 0:
        errors.append("at least one enabled condition profile is required")

    return ConditionProfileValidationResult(
        valid=not errors,
        profile_count=len(profiles),
        enabled_count=enabled_count,
        errors=tuple(errors),
        warnings=tuple(dict.fromkeys(warnings)),
        profiles=tuple(normalized_profiles),
    )


def _add_operational_warnings(profile: ConditionProfile, warnings: list[str]) -> None:
    if profile.condition_name.upper().startswith("REPLACE_WITH_"):
        warnings.append(
            f"{profile.profile_id}: condition_name is a placeholder and must be replaced"
        )
    if profile.price_subscribe_policy.value == "none" and profile.max_initial > 0:
        warnings.append(
            f"{profile.profile_id}: max_initial is ignored when price_subscribe_policy=none"
        )
    if (
        profile.enabled
        and profile.role is not ConditionRole.RISK_BLOCK
        and profile.price_subscribe_policy is PriceSubscribePolicy.IMMEDIATE
        and profile.max_initial > 0
    ):
        warnings.append(
            f"{profile.profile_id}: enabled positive profile uses immediate with "
            "max_initial > 0; use only for manual/special profiles"
        )
    if (
        profile.role is ConditionRole.RISK_BLOCK
        and profile.price_subscribe_policy is not PriceSubscribePolicy.NONE
    ):
        warnings.append(f"{profile.profile_id}: RISK_BLOCK should use price_subscribe_policy=none")
    if profile.role is ConditionRole.RISK_BLOCK and profile.priority > 0:
        warnings.append(f"{profile.profile_id}: RISK_BLOCK priority is usually <= 0")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Kiwoom condition profile JSON.")
    parser.add_argument("--file", required=True, help="Path to condition profile JSON file.")
    args = parser.parse_args(argv)

    result = validate_condition_profiles_file(args.file)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
