from __future__ import annotations

from fastapi import Header, HTTPException, status
from services.config import load_settings


def require_local_token(
    x_local_token: str | None = Header(default=None, alias="X-Local-Token"),
    x_core_token: str | None = Header(default=None, alias="X-Core-Token"),
) -> None:
    expected_token = load_settings().trading_core_token
    if not expected_token:
        return

    supplied_token = x_local_token or x_core_token
    if supplied_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Gateway token is required",
        )
    if supplied_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Gateway token is invalid",
        )
