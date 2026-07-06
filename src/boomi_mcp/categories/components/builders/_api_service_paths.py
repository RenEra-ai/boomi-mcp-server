"""Issue #133 (M6.1) — shared WSS/ASC endpoint formula helpers.

Pure functions shared by the ApiServiceBuilder (duplicate-route check),
``analyze_component`` (effective-path extraction), ``listener_verify``
orchestration (collision scan + probe), and the ``wss_listen`` primitive /
listener archetypes (endpoint metadata). They live HERE — in the builders
layer, below both ``patterns`` and ``categories.deployment`` — so every
consumer imports downward and no patterns↔builders cycle forms;
``patterns.primitives.wss_listen`` re-exports them for pattern-layer callers.

Live grounding (#133 recon, 2026-07-04, renera):

* ASC routes are served under ``/ws/rest/<base>/<objectName>/<urlPath>`` with
  EMPTY segments omitted and casing VERBATIM (WSS-op objectName
  ``generalListener`` served ``POST /ws/rest/generalListener`` -> 200) —
  unlike bare ``/ws/simple`` paths, which sentence-case the objectName.
* Route ``<overrides>`` attributes use empty string = "inherit from the
  linked WSS Listen operation" (per-attribute).
* The HTTP method is never stored on the WSS operation — it derives from the
  effective input type (``none`` -> GET, else POST) unless the ASC route
  overrides it explicitly.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def wss_http_method(input_type: Any) -> str:
    """HTTP method Boomi derives from the listener input type.

    ``none`` -> GET; every document-bearing input type -> POST. The method is
    never set on the WSS operation component (companion fixture, #12).
    """
    return "GET" if str(input_type or "").strip().lower() == "none" else "POST"


def normalize_api_service_path_segment(value: Any) -> str:
    """One ASC path segment: strip whitespace and leading/trailing slashes,
    preserve interior slashes and case verbatim; blank -> ``""``."""
    if value is None:
        return ""
    return str(value).strip().strip("/")


def compute_asc_endpoint(base_url_path: Any, object_name: Any, url_path: Any) -> str:
    """API Service Component (ASC) endpoint path:
    ``/ws/rest/<base>/<objectName>/<urlPath>`` with EMPTY segments omitted and
    casing preserved verbatim (never ``sentence_case_object_name`` — that is a
    ``/ws/simple``-only transformation; live-settled 2026-07-04, #133 recon).

    Live-confirmed shape: empty ASC base + all-inherit route resolves to
    ``/ws/rest/{WSS-op objectName}``.
    """
    segments = [
        normalize_api_service_path_segment(part)
        for part in (base_url_path, object_name, url_path)
    ]
    tail = "/".join(seg for seg in segments if seg)
    return f"/ws/rest/{tail}" if tail else "/ws/rest"


def api_service_http_method(route_http_method: Any, input_type: Any) -> str:
    """Effective HTTP method for an ASC route: the route's explicit
    ``httpMethod`` override (upper-cased) when non-empty, else derived from
    the effective WSS input type exactly like bare WSS (none -> GET, else
    POST). Empty string on the override means INHERIT (per-attribute route
    override semantics, live capture ``api_service_minimal.xml``)."""
    explicit = str(route_http_method or "").strip()
    if explicit:
        return explicit.upper()
    return wss_http_method(input_type)


def effective_api_service_route(
    base_url_path: Any,
    route_overrides: Dict[str, Any],
    wss_operation_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Resolve an ASC route's EFFECTIVE method + served path.

    ``route_overrides`` uses the builder's snake_case route keys
    (``http_method`` / ``url_path`` / ``object_name`` / ``input_type`` /
    ``output_type``) where empty string means "inherit from the linked WSS
    Listen operation" (per-attribute; live capture 2026-07-04). The WSS
    operation contributes ``object_name`` / ``input_type`` / ``output_type``;
    it has no urlPath, so an inherit (empty) route ``url_path`` contributes no
    path suffix. Returns ``{method, path, object_name, input_type,
    output_type}`` with the path computed by :func:`compute_asc_endpoint`.
    """
    op = wss_operation_config or {}

    def _inherit(route_key: str, op_key: str) -> str:
        value = str(route_overrides.get(route_key) or "").strip()
        if value:
            return value
        return str(op.get(op_key) or "").strip()

    object_name = _inherit("object_name", "object_name")
    input_type = _inherit("input_type", "input_type")
    output_type = _inherit("output_type", "output_type")
    url_path = str(route_overrides.get("url_path") or "").strip()
    method = api_service_http_method(route_overrides.get("http_method"), input_type)
    return {
        "method": method,
        "path": compute_asc_endpoint(base_url_path, object_name, url_path),
        "object_name": object_name,
        "input_type": input_type,
        "output_type": output_type,
    }


__all__ = [
    "wss_http_method",
    "normalize_api_service_path_segment",
    "compute_asc_endpoint",
    "api_service_http_method",
    "effective_api_service_route",
]
