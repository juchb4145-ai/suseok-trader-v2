from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable

from fastapi import Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from services.config import load_settings

AUTH_REQUIRED_DETAIL = "Gateway token is required"
AUTH_INVALID_DETAIL = "Gateway token is invalid"
AUTH_NOT_CONFIGURED_DETAIL = "TRADING_CORE_TOKEN must be configured"

_PUBLIC_EXACT_PATHS = {
    "/",
    "/dashboard",
    "/docs",
    "/health",
    "/openapi.json",
    "/redoc",
    "/api/status",
}
_PUBLIC_PREFIXES = ("/static/",)
_PROTECTED_READ_PATHS = {
    "/api/gateway/auth/probe",
    "/api/gateway/commands",
}


def ensure_auth_token_configured() -> None:
    if not _configured_token():
        raise RuntimeError(AUTH_NOT_CONFIGURED_DETAIL)


async def enforce_local_token_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if _is_read_only_whitelisted(request):
        return await call_next(request)

    supplied_token = _header_token(request)
    try:
        verify_local_token(supplied_token)
    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
        )
    return await call_next(request)


def require_local_token(
    x_local_token: str | None = Header(default=None, alias="X-Local-Token"),
    x_core_token: str | None = Header(default=None, alias="X-Core-Token"),
) -> None:
    verify_local_token(x_local_token or x_core_token)


def verify_local_token(supplied_token: str | None) -> None:
    expected_token = _configured_token()
    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=AUTH_NOT_CONFIGURED_DETAIL,
        )
    if supplied_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=AUTH_REQUIRED_DETAIL,
        )
    if not secrets.compare_digest(supplied_token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=AUTH_INVALID_DETAIL,
        )


def _configured_token() -> str:
    return load_settings().trading_core_token.strip()


def _header_token(request: Request) -> str | None:
    return request.headers.get("x-local-token") or request.headers.get("x-core-token")


def _is_read_only_whitelisted(request: Request) -> bool:
    normalized_method = request.method.upper()
    path = request.url.path
    if normalized_method == "OPTIONS":
        return True
    if path in _PUBLIC_EXACT_PATHS or any(path.startswith(prefix) for prefix in _PUBLIC_PREFIXES):
        return True
    if normalized_method not in {"GET", "HEAD"}:
        return False
    if path in _PROTECTED_READ_PATHS:
        return False
    if path == "/api/ai-sidecar/context/preview" and _truthy_query_flag(
        request.query_params.get("persist")
    ):
        return False
    return path.startswith("/api/")


def _truthy_query_flag(value: str | None) -> bool:
    return value is not None and value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
