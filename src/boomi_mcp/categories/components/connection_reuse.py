"""suggest_connection_reuse — read-only connection-reuse discovery (M7.3, issue #83).

Ranks EXISTING ``connector-settings`` components for safe reuse so an agent can
wire a reused connection (keeping credentials out of the conversation) instead of
authoring a new one. Read-only: queries component metadata and reads component XML
only to extract non-secret endpoint context. Never mutates Boomi, never echoes
credential material.

Every response carries ``read_only=True`` / ``boomi_mutation=False`` /
``raw_xml_exposed=False`` (mirrors ``_IMPORT_FLAGS`` in ``integration_import.py``)
so the advertised contract holds on success AND error.

Candidates are returned with IntegrationSpecV1-compatible reuse bindings:
``reference_only=True`` connections (resolved by component_id) plus an exact-name
fallback paired with ``conflict_policy='reuse'`` — the two reuse surfaces the
build path already understands (see ``integration_builder`` reference_only
resolution). Only whitelisted, non-secret endpoint fields are ever echoed, and the
response is scanned with the existing redaction/secret-shape helpers before return.
"""

from __future__ import annotations

import difflib
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
    ComponentMetadataGroupingExpression,
    ComponentMetadataGroupingExpressionOperator,
)

from ._shared import (
    component_get_xml,
    paginate_metadata,
    _extract_api_error_msg,
    ComponentGetDeadlineExceeded,
)
from .builders.connector_builder import (
    _resolve_rest_connector_type,
    _resolve_soap_client_connector_type,
    REST_CLIENT_SUBTYPE,
    SOAP_CLIENT_SUBTYPE,
    DatabaseConnectorBuilder,
)
from ...patterns.primitives._helpers import value_looks_secret, _key_looks_secret

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

_REUSE_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}

CONNECTION_REUSE_QUERY_FAILED = "CONNECTION_REUSE_QUERY_FAILED"

# top_k clamp — a discovery tool never needs to return the whole account.
_TOP_K_MIN = 1
_TOP_K_MAX = 25

# Whitelisted, non-secret endpoint fields echoed as match context.
_DB_SAFE_ATTRS = ("host", "port", "dbname", "driverId", "urlFormat")
# GenericConnectionConfig field ids that are safe URL context (REST base URL,
# SOAP WSDL/endpoint URL). Everything else (username/password/oauth/token/…) is
# never read.
_URL_SAFE_FIELD_IDS = ("url", "endpoint")

# Cap connector-action metadata attached per candidate.
_MAX_PAIRED_ACTIONS = 3

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SEGMENT_RE = re.compile(r"[\\/]+")
_SHARED_FOLDER_SEGMENTS = frozenset({"common", "shared", "library", "lib"})


# ---------------------------------------------------------------------------
# Small text/host helpers
# ---------------------------------------------------------------------------

def _tokens(text: Optional[str]) -> set:
    """Lowercase alphanumeric tokens of length >= 3 (drops noise words)."""
    if not text:
        return set()
    return {t for t in _TOKEN_RE.findall(text.lower()) if len(t) >= 3}


def _host_of(value: Optional[str]) -> str:
    """Extract a bare lowercase host from a URL, host:port, or bare host."""
    if not value:
        return ""
    v = value.strip()
    if "://" in v:
        return (urlparse(v).hostname or "").lower()
    v = v.split("/", 1)[0]
    v = v.split(":", 1)[0]
    return v.lower()


def _localname(tag: str) -> str:
    """Strip an ``{namespace}`` prefix from an ElementTree tag."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


# ---------------------------------------------------------------------------
# Subtype resolution
# ---------------------------------------------------------------------------

def _resolve_subtype(connector_type: str) -> str:
    """Resolve caller connector_type to the canonical Boomi subType stored on
    components. REST/SOAP aliases resolve to their canonical subtypes; anything
    else (``database``, ``sftp``, …) is treated as an exact raw subtype."""
    rest = _resolve_rest_connector_type(connector_type)
    if rest is not None:
        return rest
    soap = _resolve_soap_client_connector_type(connector_type)
    if soap is not None:
        return soap
    return connector_type


def _connector_family(resolved_subtype: str) -> str:
    """Map a resolved subtype to the reference_only ``connector_type`` family
    label the build path uses (matches the primitives' reuse configs)."""
    if resolved_subtype == REST_CLIENT_SUBTYPE:
        return "rest"
    if resolved_subtype == SOAP_CLIENT_SUBTYPE:
        return "soap_client"
    if resolved_subtype.lower() == "database":
        return "database"
    return resolved_subtype


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _folder_score(
    folder: Optional[str], purpose: Optional[str], endpoint_hint: Optional[str]
) -> Tuple[int, List[str]]:
    """Up to 15: shared/common placement + purpose/endpoint token overlap."""
    if not folder:
        return 0, []
    reasons: List[str] = []
    score = 0
    seg_lower = [s.lower() for s in _SEGMENT_RE.split(folder) if s]
    if any(s == "#common" for s in seg_lower):
        score = 15
        reasons.append("in #Common shared folder")
    elif any(s in _SHARED_FOLDER_SEGMENTS for s in seg_lower):
        score = 10
        reasons.append("in a shared/common folder")
    wanted = _tokens(purpose) | _tokens(endpoint_hint)
    folder_tokens: set = set()
    for s in seg_lower:
        folder_tokens |= _tokens(s)
    overlap = wanted & folder_tokens
    if overlap:
        score = min(15, score + 5)
        reasons.append("folder matches purpose (" + ", ".join(sorted(overlap)) + ")")
    return score, reasons


def _name_score(
    name: Optional[str], purpose: Optional[str], endpoint_hint: Optional[str]
) -> Tuple[int, List[str]]:
    """Up to 15: max(SequenceMatcher ratio, token-overlap) vs purpose+hint."""
    target = " ".join(x for x in (purpose, endpoint_hint) if x).strip()
    if not name or not target:
        return 0, []
    ratio = difflib.SequenceMatcher(None, name.lower(), target.lower()).ratio()
    name_tokens = _tokens(name)
    target_tokens = _tokens(target)
    token_overlap = (
        len(name_tokens & target_tokens) / len(target_tokens)
        if target_tokens
        else 0.0
    )
    score = round(max(ratio, token_overlap) * 15)
    if score <= 0:
        return 0, []
    common = name_tokens & target_tokens
    if common:
        return score, ["name matches purpose (" + ", ".join(sorted(common)) + ")"]
    return score, ["name is similar to the requested purpose"]


def _endpoint_score(
    endpoint_hint: Optional[str], endpoint_values: List[str]
) -> Tuple[int, List[str]]:
    """Up to 30 for host match against extracted endpoint context.

    30 exact host, 20 subdomain/suffix host, 10 normalized substring, else 0.
    """
    if not endpoint_hint:
        return 0, []
    hint_host = _host_of(endpoint_hint)
    hint_norm = endpoint_hint.strip().lower()
    best = 0
    reason: Optional[str] = None
    for raw in endpoint_values:
        if not raw:
            continue
        cand_host = _host_of(raw)
        raw_low = raw.strip().lower()
        if hint_host and cand_host:
            if hint_host == cand_host:
                if best < 30:
                    best, reason = 30, f"exact host match ({cand_host})"
                continue
            if cand_host.endswith("." + hint_host) or hint_host.endswith("." + cand_host):
                if best < 20:
                    best, reason = 20, f"subdomain/suffix host match ({cand_host})"
                continue
        if best < 10 and hint_norm and (
            hint_norm in raw_low or (hint_host and hint_host in raw_low)
        ):
            best, reason = 10, "endpoint substring match"
    return best, ([reason] if reason else [])


# ---------------------------------------------------------------------------
# Safe endpoint-context extraction (XML → non-secret fields only)
# ---------------------------------------------------------------------------

def _extract_safe_context(raw_xml: str) -> Tuple[Dict[str, Any], List[str]]:
    """Parse connector-settings XML and return ONLY whitelisted endpoint fields
    plus the list of endpoint strings usable for host matching.

    Reads DatabaseConnectionSettings safe attrs and GenericConnectionConfig
    ``url``/``endpoint`` fields. Never reads username/password/oauth/token/
    encrypted values (they are simply not in the whitelist)."""
    context: Dict[str, Any] = {}
    endpoint_values: List[str] = []
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError:
        return context, endpoint_values

    for el in root.iter():
        local = _localname(el.tag)
        if local == "DatabaseConnectionSettings":
            for attr in _DB_SAFE_ATTRS:
                val = el.get(attr)
                if val:
                    context[attr] = val
            if context.get("host"):
                endpoint_values.append(context["host"])
            if context.get("urlFormat"):
                endpoint_values.append(context["urlFormat"])
        elif local == "field":
            field_id = el.get("id")
            if field_id in _URL_SAFE_FIELD_IDS:
                val = el.get("value")
                if val:
                    context[field_id] = val
                    endpoint_values.append(val)
    return context, endpoint_values


# ---------------------------------------------------------------------------
# Secret backstop
# ---------------------------------------------------------------------------

def _scrub_secrets(node: Any) -> None:
    """Defensive in-place scrub: redact forbidden-keyed values, then drop any
    secret-shaped key or secret-looking value that slipped through. We only ever
    populate whitelisted fields, so this should be a no-op — it exists so a
    future field-whitelist change can never leak credential material."""
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(node)
    if isinstance(node, dict):
        for key in list(node.keys()):
            value = node[key]
            if isinstance(key, str) and _key_looks_secret(key):
                node.pop(key, None)
                continue
            if value_looks_secret(value):
                node[key] = "[REDACTED]"
            else:
                _scrub_secrets(value)
    elif isinstance(node, list):
        for item in node:
            _scrub_secrets(item)


# ---------------------------------------------------------------------------
# Reference bindings
# ---------------------------------------------------------------------------

def _build_reference(component_id: str, name: str, family: str) -> Dict[str, Any]:
    """Emit IntegrationSpecV1-compatible reuse bindings for a candidate."""
    key = "reused_connection"
    return {
        # Archetype/build binding keyed on the stable component id.
        "archetype_binding": {"mode": "reuse", "component_id": component_id},
        # config shape the build path resolves as reference_only (by id).
        "reference_only_config": {
            "reference_only": True,
            "connector_type": family,
            "component_id": component_id,
        },
        # A ready-to-drop IntegrationSpecV1 component (reference_only by id).
        "integration_spec_component_example": {
            "key": key,
            "type": "connector-settings",
            "action": "create",
            "name": name,
            "config": {
                "reference_only": True,
                "connector_type": family,
                "component_id": component_id,
            },
        },
        # Exact-name fallback: no id, resolved by name under conflict_policy=reuse.
        "exact_name_fallback": {
            "conflict_policy": "reuse",
            "component": {
                "key": key,
                "type": "connector-settings",
                "action": "create",
                "name": name,
                "config": {
                    "reference_only": True,
                    "connector_type": family,
                    "component_name": name,
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Paired connector-action metadata (optional, best-effort, metadata-only)
# ---------------------------------------------------------------------------

def _query_subtype_metadata(
    boomi_client: Boomi, component_type: str, subtype: str
) -> List[Dict[str, Any]]:
    """Metadata query for TYPE == component_type AND SUBTYPE == subtype."""
    type_expr = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.TYPE,
        argument=[component_type],
    )
    subtype_expr = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.SUBTYPE,
        argument=[subtype],
    )
    root_expr = ComponentMetadataGroupingExpression(
        operator=ComponentMetadataGroupingExpressionOperator.AND,
        nested_expression=[type_expr, subtype_expr],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=root_expr)
    query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
    return paginate_metadata(boomi_client, query_config)


def _pair_actions(
    candidate_folder: str,
    candidate_name: str,
    actions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach up to _MAX_PAIRED_ACTIONS same-subtype actions, preferring same
    folder then name-token overlap. Metadata only, marked non-authoritative."""
    if not actions:
        return []
    name_tokens = _tokens(candidate_name)

    def _rank(action: Dict[str, Any]) -> Tuple[int, int]:
        same_folder = 1 if action.get("folder_name") == candidate_folder and candidate_folder else 0
        overlap = len(name_tokens & _tokens(action.get("name")))
        return (same_folder, overlap)

    ranked = sorted(actions, key=_rank, reverse=True)
    paired: List[Dict[str, Any]] = []
    for action in ranked:
        score = _rank(action)
        if score == (0, 0):
            break  # no locality signal at all — don't pad with noise
        paired.append({
            "component_id": action.get("component_id"),
            "name": action.get("name"),
            "folder": action.get("folder_name"),
            "component_type": "connector-action",
            "authoritative": False,
        })
        if len(paired) >= _MAX_PAIRED_ACTIONS:
            break
    return paired


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def suggest_connection_reuse_action(
    boomi_client: Boomi,
    profile: str,
    connector_type: str,
    purpose: Optional[str] = None,
    endpoint_hint: Optional[str] = None,
    top_k: int = 5,
) -> Dict[str, Any]:
    """Rank existing connector-settings components for safe reuse.

    Read-only: queries component metadata and reads candidate XML only to pull
    non-secret endpoint context. Returns reference_only / conflict_policy='reuse'
    bindings and never echoes credential material.
    """
    try:
        if not connector_type or not str(connector_type).strip():
            return {
                "_success": False,
                "error": "connector_type is required",
                "error_code": CONNECTION_REUSE_QUERY_FAILED,
                "profile": profile,
                **_REUSE_FLAGS,
            }

        try:
            top_k_int = int(top_k)
        except (TypeError, ValueError):
            top_k_int = 5
        top_k_int = min(max(top_k_int, _TOP_K_MIN), _TOP_K_MAX)

        resolved_subtype = _resolve_subtype(connector_type.strip())
        family = _connector_family(resolved_subtype)

        # --- Query connector-settings of this subtype ---
        settings = _query_subtype_metadata(
            boomi_client, "connector-settings", resolved_subtype
        )

        base = {
            "_success": True,
            "profile": profile,
            "connector_type": connector_type,
            "resolved_subtype": resolved_subtype,
            "connector_family": family,
            "top_k": top_k_int,
            **_REUSE_FLAGS,
        }

        if not settings:
            return {**base, "total_matched": 0, "candidates": []}

        # --- Cheap scoring from metadata (folder + name) ---
        scored: List[Dict[str, Any]] = []
        for comp in settings:
            folder = comp.get("folder_name", "")
            name = comp.get("name", "")
            folder_pts, folder_reasons = _folder_score(folder, purpose, endpoint_hint)
            name_pts, name_reasons = _name_score(name, purpose, endpoint_hint)
            scored.append({
                "component_id": comp.get("component_id", ""),
                "name": name,
                "folder": folder,
                "_cheap": 40 + folder_pts + name_pts,  # subtype match is always 40
                "_folder_pts": folder_pts,
                "_name_pts": name_pts,
                "_reasons": folder_reasons + name_reasons,
            })

        # --- Bounded XML enrichment (endpoint context) for the top window ---
        scored.sort(key=lambda c: (c["_cheap"], c["name"]), reverse=True)
        working_cap = min(len(scored), max(top_k_int * 4, 20))
        enrichment_capped = working_cap < len(scored)

        for cand in scored[:working_cap]:
            endpoint_pts = 0
            endpoint_reasons: List[str] = []
            safe_context: Dict[str, Any] = {}
            try:
                comp_xml = component_get_xml(boomi_client, cand["component_id"])
                safe_context, endpoint_values = _extract_safe_context(
                    comp_xml.get("xml", "")
                )
                endpoint_pts, endpoint_reasons = _endpoint_score(
                    endpoint_hint, endpoint_values
                )
            except ComponentGetDeadlineExceeded:
                endpoint_reasons = ["endpoint context unavailable (component read timed out)"]
            except Exception:
                endpoint_reasons = ["endpoint context unavailable (component read failed)"]
            cand["_endpoint_pts"] = endpoint_pts
            cand["_reasons"] = cand["_reasons"] + endpoint_reasons
            cand["_safe_context"] = safe_context

        # --- Optional paired connector-action metadata (best-effort) ---
        actions: List[Dict[str, Any]] = []
        try:
            actions = _query_subtype_metadata(
                boomi_client, "connector-action", resolved_subtype
            )
        except Exception:
            actions = []

        # --- Assemble final candidates ---
        assembled: List[Dict[str, Any]] = []
        for cand in scored[:working_cap]:
            total = cand["_cheap"] + cand.get("_endpoint_pts", 0)
            why = ["connector subtype match (" + resolved_subtype + ")"] + cand["_reasons"]
            safe_context = cand.get("_safe_context", {})
            _scrub_secrets(safe_context)
            candidate = {
                "component_id": cand["component_id"],
                "name": cand["name"],
                "folder": cand["folder"],
                "component_type": "connector-settings",
                "subtype": resolved_subtype,
                "score": total,
                "why_matched": why,
                "safe_context": safe_context,
                "paired_actions": _pair_actions(cand["folder"], cand["name"], actions),
                "reference": _build_reference(
                    cand["component_id"], cand["name"], family
                ),
            }
            assembled.append(candidate)

        assembled.sort(key=lambda c: (c["score"], c["name"]), reverse=True)
        result_candidates = assembled[:top_k_int]

        # --- Final belt-and-suspenders scrub over the whole payload ---
        _scrub_secrets(result_candidates)

        return {
            **base,
            "total_matched": len(settings),
            "candidates_scanned": working_cap,
            "enrichment_capped": enrichment_capped,
            "candidates": result_candidates,
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to query reusable connections: {_extract_api_error_msg(e)}",
            "error_code": CONNECTION_REUSE_QUERY_FAILED,
            "exception_type": type(e).__name__,
            "profile": profile,
            **_REUSE_FLAGS,
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to query reusable connections: {str(e)}",
            "error_code": CONNECTION_REUSE_QUERY_FAILED,
            "exception_type": type(e).__name__,
            "profile": profile,
            **_REUSE_FLAGS,
        }


__all__ = ["suggest_connection_reuse_action"]
