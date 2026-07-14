from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def iter_app_routes(app: Any) -> Iterator[Any]:
    """Iterate concrete routes across eager and lazy FastAPI router inclusion."""
    try:
        from fastapi.routing import iter_route_contexts
    except ImportError:
        route_contexts = (
            context
            for route in app.routes
            for context in _legacy_route_contexts(route)
        )
    else:
        route_contexts = iter_route_contexts(app.routes)

    for route in route_contexts:
        if isinstance(getattr(route, "path", None), str) and getattr(
            route, "methods", None
        ) is not None:
            yield route


def _legacy_route_contexts(route: Any) -> Iterator[Any]:
    effective_route_contexts = getattr(route, "effective_route_contexts", None)
    if callable(effective_route_contexts):
        yield from effective_route_contexts()
    else:
        yield route
