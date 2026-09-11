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
* The HTTP method is never stored on the WSS operation, and the two routes
  derive it differently (#158 QA-158-r2-01, measured live 2026-09-11 over all
  seven operation types x {none, singlejson} x five methods — evidence
  ``docs/architecture/evidence/issue-158/captures/cap158-r3-wss-method-matrix/``):

  - bare ``/ws/simple`` ignores the operation type: ``none`` input answers
    GET/POST/PUT/DELETE, a document-bearing input answers POST/PUT/DELETE and
    refuses GET, so :func:`wss_http_method` (``none`` -> GET, else POST) is a
    method the runtime always accepts;
  - an all-inherit API Service route serves exactly ONE method, decided by the
    operation type alone (:data:`ASC_METHOD_BY_OPERATION_TYPE`). Deriving it
    from the input type — the bare rule, copied here by #133 from EXECUTE-only
    captures — served the wrong method for 9 of the 14 measured cells.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

# The measured facts live in a leaf module (importable without this package);
# applied here and re-exported.
from ..wss_route_methods import (
    ASC_BODY_METHOD_CHOICES,
    ASC_GET_WITH_INPUT_RULE,
    ASC_METHOD_BY_OPERATION_TYPE,
    ASC_METHOD_RULE,
    BARE_METHOD_RULE,
    DEFAULT_WSS_OPERATION_TYPE,
)


def wss_http_method(input_type: Any) -> str:
    """The method a bare ``/ws/simple`` listener route is called with.

    ``none`` -> GET; every document-bearing input type -> POST — a method the
    runtime accepts on that route for every operation type (measured, #158).
    Bare routes ONLY: an API Service route's method comes from the operation
    type (:func:`api_service_http_method`). The method is never set on the WSS
    operation component (companion fixture, #12).
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


def api_service_http_method(route_http_method: Any, *, operation_type: Any) -> str:
    """Effective HTTP method for an ASC route: the route's explicit
    ``httpMethod`` override (upper-cased) when non-empty, else the method the
    platform serves for the linked operation's ``operationType``
    (:data:`ASC_METHOD_BY_OPERATION_TYPE`; a blank type is the builder's default).
    Empty string on the override means INHERIT (per-attribute route override
    semantics, live capture ``api_service_minimal.xml``).

    Returns ``""`` for an operation type outside the platform's vocabulary: the
    served method is then unknown, and no caller may substitute a guess.
    """
    explicit = str(route_http_method or "").strip()
    if explicit:
        return explicit.upper()
    token = str(operation_type or "").strip().upper() or DEFAULT_WSS_OPERATION_TYPE
    return ASC_METHOD_BY_OPERATION_TYPE.get(token, "")


def route_refuses_its_input(http_method: Any, input_type: Any) -> bool:
    """True when a route serves GET for an operation that expects input.

    The runtime refuses every such request ("Cannot make GET request on
    operation ... which expects input", HTTP 405) on BOTH routes, and an
    all-inherit API Service route serves no other method for a GET or QUERY
    operation — so that route cannot be called at all (measured, #158 capture).
    """
    return (
        str(http_method or "").strip().upper() == "GET"
        and str(input_type or "").strip().lower() not in ("", "none")
    )


def uncallable_route_remedies(
    *,
    explicit_method: Any,
    operation_type: Any,
    input_type: Any,
    method_field: str,
    input_none_available: bool = True,
) -> Tuple[Tuple[str, Dict[str, str]], ...]:
    """The changes that make a GET-with-input route callable, each with its text.

    #158 (QA-158-r6-01): hand-written advice told a caller whose route pinned GET
    over a GET/QUERY operation to clear the pin — which inherits GET again. So no
    remedy is written here as advice: each CANDIDATE change is applied to the
    route's state and offered only if :func:`route_refuses_its_input` then passes
    it. ``method_field`` names the caller's own field for the route's method;
    ``input_none_available`` is False where the caller cannot choose ``none``.
    """
    state = {
        "explicit_method": str(explicit_method or "").strip(),
        "operation_type": str(operation_type or "").strip().upper(),
        "input_type": str(input_type or "").strip().lower(),
    }
    inherited = api_service_http_method("", operation_type=state["operation_type"])
    candidates = []
    if state["explicit_method"]:
        candidates.append((
            "set {0} to another method, such as POST".format(method_field),
            {"explicit_method": "POST"},
        ))
        candidates.append((
            "remove {0} so the route inherits the operation type's ({1} -> {2})".format(
                method_field, state["operation_type"] or DEFAULT_WSS_OPERATION_TYPE, inherited
            ),
            {"explicit_method": ""},
        ))
    else:
        candidates.append((
            "choose an operation type whose route takes a request body ("
            + ASC_BODY_METHOD_CHOICES + ")",
            {"operation_type": "EXECUTE"},
        ))
        candidates.append((
            "set {0} to another method, such as POST".format(method_field),
            {"explicit_method": "POST"},
        ))
    if input_none_available:
        candidates.append((
            "give the Listen operation input type 'none'",
            {"input_type": "none"},
        ))
    offered = []
    for text, change in candidates:
        applied = dict(state, **change)
        method = api_service_http_method(
            applied["explicit_method"], operation_type=applied["operation_type"]
        )
        if method and not route_refuses_its_input(method, applied["input_type"]):
            offered.append((text, change))
    return tuple(offered)


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
    operation contributes ``object_name`` / ``operation_type`` /
    ``input_type`` / ``output_type``; it has no urlPath, so an inherit (empty)
    route ``url_path`` contributes no path suffix. An inherited method comes
    from the operation TYPE (#158), never from the input type. Returns
    ``{method, path, object_name, input_type, output_type}`` with the path
    computed by :func:`compute_asc_endpoint`.
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
    method = api_service_http_method(
        route_overrides.get("http_method"), operation_type=op.get("operation_type")
    )
    return {
        "method": method,
        "path": compute_asc_endpoint(base_url_path, object_name, url_path),
        "object_name": object_name,
        "input_type": input_type,
        "output_type": output_type,
    }


__all__ = [
    "ASC_BODY_METHOD_CHOICES",
    "ASC_GET_WITH_INPUT_RULE",
    "ASC_METHOD_BY_OPERATION_TYPE",
    "ASC_METHOD_RULE",
    "BARE_METHOD_RULE",
    "DEFAULT_WSS_OPERATION_TYPE",
    "wss_http_method",
    "normalize_api_service_path_segment",
    "compute_asc_endpoint",
    "api_service_http_method",
    "route_refuses_its_input",
    "uncallable_route_remedies",
    "effective_api_service_route",
]
