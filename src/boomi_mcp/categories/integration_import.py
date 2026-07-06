"""Existing-integration import → pipeline / preset / IntegrationSpecV1 drafts (M7.2, issue #48).

Read-only migration discovery: converts a STRUCTURED migration description
(``generic_integration_description``) or a source-tool export summary
(``source_tool_export_summary``) into

- a semantic ``pipeline_draft`` (a validated :class:`PipelineSpec` dump),
- the closest EXISTING archetype preset (``selected_preset``) with derived
  ``preset_parameters``, and
- an ``integration_spec_draft`` — emitted through the existing
  ``build_from_archetype_action`` ONLY when zero blocking gaps remain, so the
  tool never produces a broken build input.

Never calls Boomi, constructs an SDK client, or reads credentials. Every
response carries ``read_only=True`` / ``boomi_mutation=False`` /
``raw_xml_exposed=False``. Product/version/tool identifiers from export
summaries are preserved under ``input_provenance`` only — they never influence
normalization or preset selection (no product-specific preset forks).

Gap codes: the six ``MIGRATION_IMPORT_*`` constants below are the import's own
surface; gaps produced by delegated layers pass through their existing codes
(``PROFILE_INFERENCE_*`` from profile inference, ``PARAM_VALIDATION_FAILED`` /
builder codes from archetype validation) plus the delegated
``PROFILE_INFERENCE_CONFIRMATION_REQUIRED`` marker for schemas inferred from
samples that still need field confirmation.

Secret safety: artifact VALUES are never echoed in gap messages or details —
a value that failed vocabulary validation (protocol, transform kind, auth
mode, method) is arbitrary caller content and is reported by field path plus
the SUPPORTED vocabulary only; a value is named only when it matches known
vocabulary. Schema leaf paths (the identifiers ``infer_profile_fields``
already surfaces) and component ids remain reportable. Plaintext
secret-shaped auth keys are rejected as gaps and never copied into
``preset_parameters``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError

from ..models.pipeline_models import PipelineSpec
from ..patterns.archetypes.database_to_api_sync import (
    JSONPayloadProfile,
    _flatten_payload_profile_leaves,
    _scan_for_secret_shaped_keys,
)
from .integration_authoring import (
    build_from_archetype_action,
    infer_profile_fields_action,
)

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

# Every response carries these flags so the advertised read-only contract
# holds on success AND error (same pattern as _INFERENCE_FLAGS, issue #47).
_IMPORT_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}

MIGRATION_IMPORT_INVALID_INPUT = "MIGRATION_IMPORT_INVALID_INPUT"
MIGRATION_IMPORT_MISSING_CREDENTIAL = "MIGRATION_IMPORT_MISSING_CREDENTIAL"
MIGRATION_IMPORT_UNKNOWN_PROTOCOL = "MIGRATION_IMPORT_UNKNOWN_PROTOCOL"
MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM = "MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM"
MIGRATION_IMPORT_AMBIGUOUS_MAPPING = "MIGRATION_IMPORT_AMBIGUOUS_MAPPING"
MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED = "MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED"

# Delegated-layer marker for inferred schemas that need field confirmation
# before the draft may feed a build (ready_for_builder=False upstream).
PROFILE_INFERENCE_CONFIRMATION_REQUIRED = "PROFILE_INFERENCE_CONFIRMATION_REQUIRED"

_SUPPORTED_SOURCE_TYPES = [
    "generic_integration_description",
    "source_tool_export_summary",
]

# Provenance keys lifted OUT of a source_tool_export_summary before
# normalization so product identity can never steer the semantic flow.
_PROVENANCE_KEYS = ("product", "vendor", "tool", "version", "export_format", "exported_at")

# Semantic protocol vocabulary → endpoint kind. Deliberately generic: engine /
# product names are NOT vocabulary (they stay provenance), only transport
# families are.
_REST_PROTOCOLS = {"rest", "http", "https", "api", "rest_api", "web_api"}
_DB_PROTOCOLS = {"database", "db", "jdbc", "sql"}
_LISTENER_PROTOCOLS = {
    "http_listener",
    "listener",
    "webhook",
    "inbound_http",
    "web_service",
    "wss",
}

_TRIGGER_SCHEDULED = {"scheduled", "schedule", "cron", "timer", "poll", "polling"}
_TRIGGER_LISTENER = {"listener", "event", "webhook", "realtime", "api_call", "inbound"}
_TRIGGER_MANUAL = {"manual", "on_demand", "none"}

# (source_kind, target_kind) → (existing archetype name, pipeline stage plan).
# Listener-triggered REST sources fold into source_kind='http_listener' before
# lookup. Only the five existing generic archetypes appear here — the import
# NEVER mints a new preset (anti-template invariant).
_PRESET_TABLE: Dict[Tuple[str, str], Tuple[str, List[Tuple[str, str]]]] = {
    ("database", "rest"): (
        "database_to_api_sync",
        [("read", "db_read"), ("map", ""), ("send", "rest_send")],
    ),
    ("rest", "rest"): (
        "api_to_api_sync",
        [("fetch", "rest_fetch"), ("map", ""), ("send", "rest_send")],
    ),
    ("rest", "database"): (
        "api_to_database_sync",
        [("fetch", "rest_fetch"), ("map", ""), ("write", "db_write")],
    ),
    ("http_listener", "rest"): (
        "http_listener_to_rest",
        [("listener", "wss_listen"), ("map", ""), ("send", "rest_send")],
    ),
    ("http_listener", "database"): (
        "http_listener_to_db",
        [("listener", "wss_listen"), ("map", ""), ("write", "db_write")],
    ),
}

# Transform kinds the import can express as archetype transform operations.
_SUPPORTED_TRANSFORM_KINDS = {"field_mapping", "mapping", "direct"}

# REST auth-mode vocabulary (RestCreateSettings enum). Values outside it are
# arbitrary caller content and are never echoed in messages.
_KNOWN_AUTH_MODES = {"basic", "bearer_token", "oauth2_client_credentials"}

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

_NON_PREFIX_RE = re.compile(r"[^A-Za-z0-9]+")


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def _import_error_envelope(
    message: str,
    *,
    field: Optional[str] = None,
    hint: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Malformed TOOL INPUT envelope (_success=False). Analysis findings are
    never reported this way — they become gaps on a _success=True response."""
    env: Dict[str, Any] = {
        "_success": False,
        **_IMPORT_FLAGS,
        # ``code`` is the legacy key; ``error_code`` the taxonomy-standard one
        # (#10) — same dual-key convention as _inference_error_envelope.
        "code": MIGRATION_IMPORT_INVALID_INPUT,
        "error_code": MIGRATION_IMPORT_INVALID_INPUT,
        "error": message,
    }
    if field is not None:
        env["field"] = field
    if hint is not None:
        env["hint"] = hint
    if details is not None:
        env["details"] = details
    return env


def _gap(
    code: str,
    field: str,
    message: str,
    *,
    severity: str = "blocking",
    hint: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    gap: Dict[str, Any] = {
        "code": code,
        "severity": severity,
        "field": field,
        "message": message,
    }
    if hint is not None:
        gap["hint"] = hint
    if details is not None:
        gap["details"] = details
    return gap


def _fact(statement: str, source: str) -> Dict[str, str]:
    return {"statement": statement, "source": source}


def _normalize_options(options: Any) -> Dict[str, Any]:
    if options is None:
        return {}
    if isinstance(options, dict):
        return options
    if isinstance(options, str):
        try:
            parsed = json.loads(options)
        except json.JSONDecodeError as exc:
            raise ValueError(f"options must be valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                "options JSON must be an object, not " + type(parsed).__name__
            )
        return parsed
    raise TypeError(
        f"options must be dict, JSON string, or None; got {type(options).__name__}"
    )


def _normalize_artifact(artifact: Any) -> Dict[str, Any]:
    if isinstance(artifact, dict):
        return artifact
    if isinstance(artifact, str):
        try:
            parsed = json.loads(artifact)
        except json.JSONDecodeError as exc:
            # Free text is rejected — never echo the content back.
            raise ValueError(
                "artifact must be a structured JSON object (free text is not "
                f"supported): {type(exc).__name__} at position {exc.pos}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                "artifact JSON must be an object, not " + type(parsed).__name__
            )
        return parsed
    raise TypeError(
        f"artifact must be dict or JSON-object string; got {type(artifact).__name__}"
    )


# ---------------------------------------------------------------------------
# Semantic normalization
# ---------------------------------------------------------------------------


def _classify_protocol(protocol: Any) -> Optional[str]:
    if not isinstance(protocol, str):
        return None
    token = protocol.strip().lower()
    if token in _REST_PROTOCOLS:
        return "rest"
    if token in _DB_PROTOCOLS:
        return "database"
    if token in _LISTENER_PROTOCOLS:
        return "http_listener"
    return None


def _classify_trigger(trigger: Any) -> Tuple[str, bool]:
    """Return (trigger_kind, was_declared)."""
    if isinstance(trigger, dict):
        raw = trigger.get("kind") or trigger.get("type")
    else:
        raw = trigger
    if not isinstance(raw, str) or not raw.strip():
        return "manual", False
    token = raw.strip().lower()
    if token in _TRIGGER_SCHEDULED:
        return "scheduled", True
    if token in _TRIGGER_LISTENER:
        return "listener", True
    if token in _TRIGGER_MANUAL:
        return "manual", True
    return "manual", False


def _derive_component_prefix(name: str) -> str:
    token = _NON_PREFIX_RE.sub(" ", name).strip()
    if not token:
        return "IMP"
    initials = "".join(word[0] for word in token.split()).upper()
    return (initials or "IMP")[:8]


class _EndpointAnalysis:
    """Per-endpoint (source/target) normalization result."""

    def __init__(self, role: str) -> None:
        self.role = role  # 'source' | 'target'
        self.kind: Optional[str] = None
        self.raw_protocol: Optional[str] = None
        self.profile_tree: Optional[Dict[str, Any]] = None
        self.leaves: List[str] = []
        self.schema_status: str = "absent"  # inline|inferred|by_reference|absent|error
        self.auth_mode: Optional[str] = None
        self.credential_ref: Optional[str] = None


def _analyze_schema(
    endpoint: Dict[str, Any],
    analysis: _EndpointAnalysis,
    facts: List[Dict[str, str]],
    assumptions: List[Dict[str, str]],
    gaps: List[Dict[str, Any]],
) -> None:
    """Resolve the endpoint's ``schema`` block into a profile tree + leaf paths."""
    role = analysis.role
    schema = endpoint.get("schema")
    if schema is None:
        return
    if not isinstance(schema, dict):
        gaps.append(
            _gap(
                MIGRATION_IMPORT_INVALID_INPUT,
                f"{role}.schema",
                "schema must be an object with one of: profile (inline JSON "
                "profile tree), infer (profile-inference request), or "
                "profile_component_id (existing profile reference)",
            )
        )
        analysis.schema_status = "error"
        return

    inline = schema.get("profile")
    infer_req = schema.get("infer")
    component_id = schema.get("profile_component_id")

    if inline is not None:
        try:
            model = JSONPayloadProfile.model_validate(inline)
        except ValidationError as exc:
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_INVALID_INPUT,
                    f"{role}.schema.profile",
                    f"inline profile tree failed validation with "
                    f"{exc.error_count()} error(s)",
                    hint=(
                        "Supply a JSONPayloadProfile tree: {format: 'json', "
                        "root: {name, kind: 'object', children: [...]}}"
                    ),
                )
            )
            analysis.schema_status = "error"
            return
        analysis.profile_tree = model.model_dump(mode="json", exclude_none=True)
        analysis.leaves = list(_flatten_payload_profile_leaves(model))
        analysis.schema_status = "inline"
        facts.append(
            _fact(
                f"{role} schema supplied inline with "
                f"{len(analysis.leaves)} mappable leaf path(s)",
                f"artifact:{role}.schema.profile",
            )
        )
        return

    if infer_req is not None:
        if not isinstance(infer_req, dict) or not isinstance(
            infer_req.get("source_type"), str
        ):
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_INVALID_INPUT,
                    f"{role}.schema.infer",
                    "infer must be an object with source_type and artifact "
                    "(see infer_profile_fields)",
                )
            )
            analysis.schema_status = "error"
            return
        result = infer_profile_fields_action(
            infer_req["source_type"],
            infer_req.get("artifact"),
            options=infer_req.get("options"),
        )
        if not result.get("_success"):
            gaps.append(
                _gap(
                    str(result.get("error_code") or result.get("code")),
                    f"{role}.schema.infer",
                    str(result.get("error", "profile inference failed")),
                    hint=result.get("hint"),
                )
            )
            analysis.schema_status = "error"
            return
        analysis.profile_tree = result["profile_config"]
        analysis.leaves = list(result.get("mappable_paths", []))
        analysis.schema_status = "inferred"
        assumptions.append(
            _fact(
                f"{role} schema inferred from "
                f"{result.get('generation_mode')} sample/metadata "
                f"({len(analysis.leaves)} mappable leaf path(s))",
                f"inferred:{result.get('generation_mode')}",
            )
        )
        if result.get("ready_for_builder") is False:
            unconfirmed = sorted(
                f["path"]
                for f in result.get("fields", [])
                if f.get("confirmation_required")
            )
            gaps.append(
                _gap(
                    PROFILE_INFERENCE_CONFIRMATION_REQUIRED,
                    f"{role}.schema.infer",
                    f"inferred {role} schema has "
                    f"{len(unconfirmed)} field(s) requiring confirmation before "
                    "the draft may feed a build",
                    hint=(
                        "Confirm the flagged fields (run infer_profile_fields "
                        "directly to inspect them) and re-import with an inline "
                        "schema.profile tree."
                    ),
                    details={"unconfirmed_paths": unconfirmed},
                )
            )
        return

    if component_id is not None:
        index = schema.get("field_index")
        produced_by = index.get("produced_by") if isinstance(index, dict) else None
        if produced_by != "index_profile_component":
            # #95 contract: NEVER invent map keys for an existing live profile.
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED,
                    f"{role}.schema.profile_component_id",
                    f"{role} references an existing profile component by id; a "
                    "live field index produced by index_profile_component "
                    "(issue #95) is required — the import never invents map keys",
                    hint=(
                        "Supply schema.field_index produced by "
                        "index_profile_component (with produced_by="
                        "'index_profile_component'), or provide the schema "
                        "inline via schema.profile / schema.infer."
                    ),
                    details={"component_id": str(component_id)},
                )
            )
            analysis.schema_status = "error"
            return
        paths = index.get("mappable_paths")
        if not isinstance(paths, list):
            index_by_path = index.get("field_index_by_path")
            paths = sorted(index_by_path) if isinstance(index_by_path, dict) else []
        analysis.leaves = [p for p in paths if isinstance(p, str)]
        analysis.schema_status = "by_reference"
        facts.append(
            _fact(
                f"{role} schema is an existing profile component (referenced by "
                f"id) with a #95 index covering {len(analysis.leaves)} path(s)",
                f"artifact:{role}.schema.field_index",
            )
        )
        return

    gaps.append(
        _gap(
            MIGRATION_IMPORT_INVALID_INPUT,
            f"{role}.schema",
            "schema object carries none of: profile, infer, profile_component_id",
        )
    )
    analysis.schema_status = "error"


def _analyze_endpoint(
    flow: Dict[str, Any],
    role: str,
    facts: List[Dict[str, str]],
    assumptions: List[Dict[str, str]],
    gaps: List[Dict[str, Any]],
) -> _EndpointAnalysis:
    analysis = _EndpointAnalysis(role)
    endpoint = flow.get(role)
    if not isinstance(endpoint, dict):
        gaps.append(
            _gap(
                MIGRATION_IMPORT_INVALID_INPUT,
                role,
                f"{role} must be an object describing the {role} endpoint "
                "(protocol, endpoint details, auth, schema)",
            )
        )
        return analysis

    protocol = endpoint.get("protocol") or endpoint.get("type")
    analysis.raw_protocol = protocol if isinstance(protocol, str) else None
    analysis.kind = _classify_protocol(protocol)
    if analysis.kind is None:
        # The rejected value is NOT echoed — it failed vocabulary validation,
        # so it is arbitrary caller content (secret-safe contract).
        gaps.append(
            _gap(
                MIGRATION_IMPORT_UNKNOWN_PROTOCOL,
                f"{role}.protocol",
                f"{role} protocol is not mappable to the supported semantic "
                "vocabulary (rest / database / http_listener)",
                hint=(
                    "Supported protocol tokens: "
                    + ", ".join(
                        sorted(_REST_PROTOCOLS | _DB_PROTOCOLS | _LISTENER_PROTOCOLS)
                    )
                    + ". Unsupported transports need a hand-authored "
                    "IntegrationSpecV1 (get_schema_template)."
                ),
            )
        )
    else:
        facts.append(
            _fact(
                f"{role} protocol classified as '{analysis.kind}'",
                f"artifact:{role}.protocol",
            )
        )

    auth = endpoint.get("auth")
    if isinstance(auth, dict):
        mode = auth.get("mode") or auth.get("type")
        analysis.auth_mode = mode.strip().lower() if isinstance(mode, str) else None
        ref = auth.get("credential_ref")
        analysis.credential_ref = ref.strip() if isinstance(ref, str) and ref.strip() else None
        if _scan_for_secret_shaped_keys(auth):
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_MISSING_CREDENTIAL,
                    f"{role}.auth",
                    f"{role} auth carries a plaintext secret-shaped key; the "
                    "import never stores or forwards plaintext secrets",
                    hint=(
                        "Remove the plaintext value and supply an opaque "
                        "credential_ref that resolves in the secret store at "
                        "execution time."
                    ),
                )
            )
        elif analysis.auth_mode and analysis.auth_mode != "none" and not analysis.credential_ref:
            # The mode value is only named when it is known vocabulary; an
            # unrecognized mode is arbitrary caller content and never echoed.
            mode_label = (
                f"'{analysis.auth_mode}'"
                if analysis.auth_mode in _KNOWN_AUTH_MODES
                else "a non-'none' value"
            )
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_MISSING_CREDENTIAL,
                    f"{role}.auth.credential_ref",
                    f"{role} auth mode is {mode_label} but declares no "
                    "credential reference; the import never invents credentials",
                    hint=(
                        "Supply auth.credential_ref (an opaque secret-store "
                        "reference) or an existing connection to reuse via "
                        "connection_ref."
                    ),
                )
            )
        elif analysis.auth_mode and analysis.auth_mode != "none":
            mode_label = (
                f"'{analysis.auth_mode}'"
                if analysis.auth_mode in _KNOWN_AUTH_MODES
                else "a non-'none' mode"
            )
            facts.append(
                _fact(
                    f"{role} auth mode {mode_label} with credential by "
                    "reference",
                    f"artifact:{role}.auth",
                )
            )
    else:
        assumptions.append(
            _fact(
                f"{role} declares no auth block; assuming an unauthenticated "
                "endpoint (auth mode 'none')",
                "inferred:default_auth_none",
            )
        )

    _analyze_schema(endpoint, analysis, facts, assumptions, gaps)
    return analysis


def _analyze_transforms(
    flow: Dict[str, Any],
    facts: List[Dict[str, str]],
    gaps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Validate transform entries; return declared mapping dicts from all
    field-mapping transforms plus the top-level ``mappings`` list."""
    mappings: List[Dict[str, Any]] = []

    raw_mappings = flow.get("mappings")
    if isinstance(raw_mappings, list):
        mappings.extend(m for m in raw_mappings if isinstance(m, dict))

    transforms = flow.get("transforms")
    if isinstance(transforms, list):
        for idx, entry in enumerate(transforms):
            if not isinstance(entry, dict):
                gaps.append(
                    _gap(
                        MIGRATION_IMPORT_INVALID_INPUT,
                        f"transforms[{idx}]",
                        "transform entries must be objects with a kind",
                    )
                )
                continue
            kind = entry.get("kind") or entry.get("type")
            token = kind.strip().lower() if isinstance(kind, str) else ""
            if token in _SUPPORTED_TRANSFORM_KINDS:
                entry_mappings = entry.get("mappings")
                if isinstance(entry_mappings, list):
                    mappings.extend(m for m in entry_mappings if isinstance(m, dict))
                facts.append(
                    _fact(
                        f"transforms[{idx}] is a declarative field mapping",
                        f"artifact:transforms[{idx}].kind",
                    )
                )
            else:
                # The rejected kind is not echoed (arbitrary caller content);
                # the supported vocabulary in the message localizes the fix.
                gaps.append(
                    _gap(
                        MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM,
                        f"transforms[{idx}].kind",
                        "transform kind is not importable; only declarative "
                        "leaf-to-leaf field mappings are supported "
                        f"({', '.join(sorted(_SUPPORTED_TRANSFORM_KINDS))})",
                        hint=(
                            "Re-express the transform as field mappings, or "
                            "hand-author the map (map_script is available via "
                            "build_from_archetype transform operations, XSLT is "
                            "not — see issue #42)."
                        ),
                    )
                )
    return mappings


def _resolve_mapping_side(
    value: Any,
    leaves: List[str],
    side: str,
    idx: int,
    assumptions: List[Dict[str, str]],
    gaps: List[Dict[str, Any]],
) -> Optional[str]:
    """Resolve one side of a mapping to a leaf path (or pass a full path through)."""
    if not isinstance(value, str) or not value.strip():
        gaps.append(
            _gap(
                MIGRATION_IMPORT_INVALID_INPUT,
                f"mappings[{idx}].{side}",
                f"mapping {side} must be a non-empty string leaf name or path",
            )
        )
        return None
    token = value.strip()
    if _UUID_RE.match(token):
        # A literal profile/component UUID inside a mapping is the #95 case:
        # never invent map keys for it.
        gaps.append(
            _gap(
                MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED,
                f"mappings[{idx}].{side}",
                f"mapping {side} references a component UUID; resolve existing "
                "profiles through a live index_profile_component index "
                "(issue #95) — the import never invents map keys",
                hint=(
                    "Reference the endpoint profile via schema."
                    "profile_component_id + schema.field_index (produced_by="
                    "'index_profile_component') and map by leaf path."
                ),
            )
        )
        return None
    if "/" in token:
        return token  # full path — archetype validation checks existence
    candidates = sorted(p for p in leaves if p.rsplit("/", 1)[-1] == token)
    if len(candidates) > 1:
        gaps.append(
            _gap(
                MIGRATION_IMPORT_AMBIGUOUS_MAPPING,
                f"mappings[{idx}].{side}",
                f"mapping {side} leaf name matches {len(candidates)} schema "
                "paths; confirm which path is meant",
                hint="Use the full leaf path in the mapping to disambiguate.",
                details={"leaf": token, "candidates": candidates},
            )
        )
        return None
    if len(candidates) == 1:
        if candidates[0] != token:
            assumptions.append(
                _fact(
                    f"mapping {side} leaf '{token}' resolved to unique schema "
                    f"path '{candidates[0]}'",
                    "inferred:unique_leaf_resolution",
                )
            )
        return candidates[0]
    return token  # unknown — archetype validation reports it against the profile


# ---------------------------------------------------------------------------
# Preset parameter derivation
# ---------------------------------------------------------------------------


def _headers_dict(endpoint: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Return declared static request headers ('request_headers' or 'headers')."""
    headers = endpoint.get("request_headers")
    if not isinstance(headers, dict):
        headers = endpoint.get("headers")
    if isinstance(headers, dict) and headers:
        return {str(k): str(v) for k, v in headers.items()}
    return None


def _derive_binding(
    endpoint: Dict[str, Any], analysis: _EndpointAnalysis
) -> Optional[Dict[str, Any]]:
    conn = endpoint.get("connection_ref") or endpoint.get("connection")
    if isinstance(conn, dict):
        if conn.get("component_id"):
            return {"mode": "reuse", "component_id": str(conn["component_id"])}
        if conn.get("component_name"):
            return {"mode": "reuse", "component_name": str(conn["component_name"])}
    base_url = endpoint.get("base_url") or endpoint.get("url")
    if isinstance(base_url, str) and base_url.strip():
        settings: Dict[str, Any] = {"base_url": base_url.strip()}
        auth_mode = analysis.auth_mode or "none"
        # Only known vocabulary is propagated into the draft; an unrecognized
        # mode is arbitrary caller content — omitted, so archetype validation
        # reports the missing auth_mode instead of echoing the junk value.
        if auth_mode == "none" or auth_mode in _KNOWN_AUTH_MODES:
            settings["auth_mode"] = auth_mode
        if analysis.credential_ref and settings.get("auth_mode") not in (None, "none"):
            settings["credential_ref"] = analysis.credential_ref
        return {"mode": "create", "settings": settings}
    return None


def _derive_preset_parameters(
    preset: str,
    flow: Dict[str, Any],
    source: _EndpointAnalysis,
    target: _EndpointAnalysis,
    operations: List[Dict[str, Any]],
    naming: Dict[str, Any],
) -> Dict[str, Any]:
    """Best-effort semantic mapping onto the selected archetype's contract.

    Slots the import cannot derive are OMITTED — build_from_archetype's own
    validation then reports them as per-field gaps, so missing required values
    always surface as blocking gaps naming the parameter field.
    """
    params: Dict[str, Any] = {"naming": naming}
    source_ep = flow.get("source") if isinstance(flow.get("source"), dict) else {}
    target_ep = flow.get("target") if isinstance(flow.get("target"), dict) else {}

    # --- source slot -------------------------------------------------------
    if preset in ("api_to_api_sync", "api_to_database_sync"):
        src: Dict[str, Any] = {}
        binding = _derive_binding(source_ep, source)
        if binding is not None:
            src["binding"] = binding
        fetch: Dict[str, Any] = {}
        if isinstance(source_ep.get("path"), str):
            fetch["path"] = source_ep["path"]
        qp = source_ep.get("query_parameters")
        if isinstance(qp, dict):
            fetch["query_parameters"] = {str(k): str(v) for k, v in qp.items()}
        headers = _headers_dict(source_ep)
        if headers is not None:
            fetch["request_headers"] = headers
        if fetch:
            src["fetch_request"] = fetch
        if source.profile_tree is not None:
            src["response_profile"] = source.profile_tree
        params["source"] = src
    elif preset == "database_to_api_sync":
        src = {}
        binding = _derive_binding(source_ep, source)
        if binding is not None:
            src["binding"] = binding
        params["source"] = src
    elif preset in ("http_listener_to_rest", "http_listener_to_db"):
        listener: Dict[str, Any] = {}
        if isinstance(source_ep.get("object_name"), str):
            listener["object_name"] = source_ep["object_name"]
        if source.profile_tree is not None:
            listener["payload_profile"] = source.profile_tree
        params["listener"] = listener

    # --- target slot -------------------------------------------------------
    if preset in ("api_to_api_sync", "http_listener_to_rest", "database_to_api_sync"):
        tgt: Dict[str, Any] = {}
        binding = _derive_binding(target_ep, target)
        if binding is not None:
            tgt["binding"] = binding
        send: Dict[str, Any] = {}
        method = target_ep.get("method")
        send["method"] = (
            method.strip().upper()
            if isinstance(method, str) and method.strip()
            else "POST"
        )
        if isinstance(target_ep.get("path"), str):
            send["path"] = target_ep["path"]
        # Declared request metadata is never silently dropped: it is copied
        # into the archetype's own send-request vocabulary, and any shape the
        # selected preset cannot carry fails its contract validation → an
        # honest blocking gap instead of a semantics-changing draft.
        qp = target_ep.get("query_parameters")
        if isinstance(qp, dict):
            if preset == "database_to_api_sync":
                # RestSendRequest takes a typed literal list, not a dict.
                send["query_parameters"] = [
                    {
                        "name": str(k),
                        "value_source": "literal",
                        "literal_value": str(v),
                    }
                    for k, v in qp.items()
                ]
            else:
                send["query_parameters"] = {str(k): str(v) for k, v in qp.items()}
        headers = _headers_dict(target_ep)
        if headers is not None:
            if preset == "database_to_api_sync" and binding is not None and binding.get("mode") == "create":
                # RestSendRequest has no request_headers; create-mode carries
                # them as connection default_headers (applied to every send).
                binding["settings"]["default_headers"] = headers
            else:
                send["request_headers"] = headers
        tgt["send_request"] = send
        if target.profile_tree is not None:
            tgt["payload_profile"] = target.profile_tree
        params["target"] = tgt
    else:
        # DbTarget slots (api_to_database_sync / http_listener_to_db) have no
        # semantic derivation yet — omitted so validation names the fields.
        params["target"] = {}

    # --- transform slot ----------------------------------------------------
    if operations:
        if preset == "database_to_api_sync":
            # Legacy TransformConfig maps by DB result FIELD NAME, not path.
            params["transform"] = {
                "operations": [
                    {
                        "operation_type": "direct",
                        "source_field": op["source_path"].rsplit("/", 1)[-1],
                        "target_path": op["target_path"],
                    }
                    for op in operations
                ]
            }
        else:
            params["transform"] = {"operations": operations}

    return params


def _convert_build_failure_to_gaps(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    code = str(result.get("error_code") or "ARCHETYPE_BUILD_VALIDATION_FAILED")
    field_errors = result.get("field_errors")
    if isinstance(field_errors, list) and field_errors:
        # Each per-field error becomes its own gap, so the delegated envelope's
        # "Inspect field_errors[]" suggestion would dangle here — this response
        # carries gaps[], not field_errors[]. Use a hint that stands alone.
        return [
            _gap(
                code,
                f"preset_parameters.{fe.get('field_path', '?')}",
                str(fe.get("message", "invalid value")),
                hint=(
                    "Supply the missing/corrected value at this "
                    "preset_parameters path, then re-run "
                    "import_integration_draft or call build_from_archetype "
                    "directly."
                ),
            )
            for fe in field_errors
        ]
    context = result.get("context")
    field = "preset_parameters"
    if isinstance(context, dict) and isinstance(context.get("field"), str):
        field = f"preset_parameters.{context['field']}"
    return [
        _gap(
            code,
            field,
            str(result.get("error", "archetype build validation failed")),
            hint=result.get("suggestion"),
        )
    ]


# ---------------------------------------------------------------------------
# Pipeline draft
# ---------------------------------------------------------------------------

_STAGE_SIDE_EFFECTS = {
    "read": "read",
    "fetch": "read",
    "listener": "read",
    "map": "none",
    "send": "write",
    "write": "write",
}


def _build_pipeline_draft(
    stage_plan: List[Tuple[str, str]],
    has_map: bool,
) -> Dict[str, Any]:
    stages: List[Dict[str, Any]] = []
    for kind, primitive in stage_plan:
        if kind == "map" and not has_map:
            continue
        stage: Dict[str, Any] = {
            "key": kind,
            "kind": kind,
            "config": {"primitive": primitive} if primitive else {},
            "side_effect": _STAGE_SIDE_EFFECTS.get(kind),
        }
        if kind == "map":
            stage["cardinality"] = "1:1"
        stages.append(stage)
    dependencies = [
        {"from_stage": stages[i]["key"], "to_stage": stages[i + 1]["key"]}
        for i in range(len(stages) - 1)
    ]
    spec = PipelineSpec.model_validate(
        {"stages": stages, "dependencies": dependencies}
    )
    return spec.model_dump(mode="json", exclude_none=True)


# ---------------------------------------------------------------------------
# Public action
# ---------------------------------------------------------------------------


def import_integration_draft_action(
    source_type: str,
    artifact: Any,
    options: Optional[Any] = None,
) -> Dict[str, Any]:
    """Convert a structured migration description into reviewable drafts.

    Read-only analysis (M7.2, issue #48): returns ``_success=True`` for every
    COMPLETED analysis — including ones full of blocking gaps — and
    ``_success=False`` only for malformed tool input. Blocking gaps suppress
    ``integration_spec_draft`` so the tool never emits a broken build input.
    """
    try:
        opts = _normalize_options(options)
    except (ValueError, TypeError) as exc:
        return _import_error_envelope(
            str(exc),
            field="options",
            hint="Provide options as a JSON object (dict) or JSON-encoded string.",
        )

    if source_type not in _SUPPORTED_SOURCE_TYPES:
        return _import_error_envelope(
            f"unknown source_type {source_type!r}",
            field="source_type",
            hint="Use one of the supported migration source types.",
            details={"supported_source_types": _SUPPORTED_SOURCE_TYPES},
        )

    try:
        artifact_dict = _normalize_artifact(artifact)
    except (ValueError, TypeError) as exc:
        return _import_error_envelope(
            str(exc),
            field="artifact",
            hint=(
                "Provide the migration description as a structured JSON object "
                "(dict or JSON-object string); free-form text is out of scope."
            ),
        )

    # ---- provenance lift (anti-template invariant) -------------------------
    input_provenance: Dict[str, Any] = {"source_type": source_type}
    flow = artifact_dict
    if source_type == "source_tool_export_summary":
        nested = artifact_dict.get("flow")
        if isinstance(nested, dict):
            flow = nested
        for key in _PROVENANCE_KEYS:
            value = artifact_dict.get(key)
            if isinstance(value, (str, int, float)):
                input_provenance[key] = value

    facts: List[Dict[str, str]] = []
    assumptions: List[Dict[str, str]] = []
    gaps: List[Dict[str, Any]] = []
    next_steps: List[str] = []

    # ---- endpoints, trigger, transforms ------------------------------------
    source = _analyze_endpoint(flow, "source", facts, assumptions, gaps)
    target = _analyze_endpoint(flow, "target", facts, assumptions, gaps)

    trigger_kind, trigger_declared = _classify_trigger(flow.get("trigger"))
    if trigger_declared:
        facts.append(_fact(f"trigger classified as '{trigger_kind}'", "artifact:trigger"))
    else:
        assumptions.append(
            _fact(
                "no recognizable trigger declared; assuming manual/on-demand "
                "execution",
                "inferred:default_trigger_manual",
            )
        )

    raw_mappings = _analyze_transforms(flow, facts, gaps)

    for block in ("error_handling", "retry", "deployment"):
        if isinstance(flow.get(block), dict) and flow[block]:
            facts.append(
                _fact(
                    f"{block} declared with keys: "
                    + ", ".join(sorted(str(k) for k in flow[block])),
                    f"artifact:{block}",
                )
            )

    # ---- mapping resolution -------------------------------------------------
    operations: List[Dict[str, Any]] = []
    for idx, mapping in enumerate(raw_mappings):
        from_value = mapping.get("from") or mapping.get("source") or mapping.get("source_path")
        to_value = mapping.get("to") or mapping.get("target") or mapping.get("target_path")
        resolved_from = _resolve_mapping_side(
            from_value, source.leaves, "from", idx, assumptions, gaps
        )
        resolved_to = _resolve_mapping_side(
            to_value, target.leaves, "to", idx, assumptions, gaps
        )
        if resolved_from and resolved_to:
            operations.append(
                {
                    "operation_type": "direct",
                    "source_path": resolved_from,
                    "target_path": resolved_to,
                }
            )

    # ---- preset selection ----------------------------------------------------
    source_kind = source.kind
    if source_kind == "rest" and trigger_kind == "listener":
        source_kind = "http_listener"
        assumptions.append(
            _fact(
                "REST source with a listener trigger treated as an inbound "
                "http_listener flow",
                "inferred:listener_trigger_folds_source",
            )
        )

    # A REST fetch source is GET-only (rest_fetch primitive; runtime request
    # bodies are out of scope). A non-GET source request cannot be represented,
    # so it blocks instead of silently changing the imported semantics.
    if source_kind == "rest":
        source_ep = flow.get("source") if isinstance(flow.get("source"), dict) else {}
        method = source_ep.get("method")
        if isinstance(method, str) and method.strip() and method.strip().upper() != "GET":
            method_label = (
                f"'{method.strip().upper()}'"
                if method.strip().upper() in ("POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
                else "a non-GET value"
            )
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_UNKNOWN_PROTOCOL,
                    "source.method",
                    f"source request method is {method_label}; a REST source is "
                    "imported as a GET fetch and non-GET source requests are an "
                    "unsupported source construct in the preset vocabulary",
                    hint=(
                        "Use a GET-readable source endpoint, or hand-author the "
                        "integration via get_schema_template("
                        "schema_name='IntegrationSpecV1')."
                    ),
                )
            )

    selected_preset: Optional[str] = None
    stage_plan: Optional[List[Tuple[str, str]]] = None
    if source_kind and target.kind:
        entry = _PRESET_TABLE.get((source_kind, target.kind))
        if entry is not None:
            selected_preset, stage_plan = entry

    # ---- pipeline draft -------------------------------------------------------
    # The map stage appears whenever the flow declares ANY transform intent —
    # even one the import cannot express (its specifics live in gaps[]) — so
    # the draft never hides that a transformation happens between endpoints.
    transforms_declared = isinstance(flow.get("transforms"), list) and bool(
        flow["transforms"]
    )
    has_map = bool(operations) or bool(raw_mappings) or transforms_declared
    pipeline_draft: Optional[Dict[str, Any]] = None
    if stage_plan is not None:
        pipeline_draft = _build_pipeline_draft(stage_plan, has_map=has_map)

    # ---- naming + preset parameters -------------------------------------------
    # Provenance is honest per origin: artifact values → 'artifact:' facts,
    # caller options → 'options:' facts, defaults → 'inferred:' assumptions.
    artifact_name = flow.get("name")
    artifact_name = (
        artifact_name.strip()
        if isinstance(artifact_name, str) and artifact_name.strip()
        else None
    )
    option_name = opts.get("integration_name")
    option_name = (
        option_name.strip()
        if isinstance(option_name, str) and option_name.strip()
        else None
    )
    if artifact_name:
        name = artifact_name
        facts.append(_fact(f"integration name '{name}'", "artifact:name"))
    elif option_name:
        name = option_name
        facts.append(
            _fact(
                f"integration name '{name}' supplied via options",
                "options:integration_name",
            )
        )
    else:
        name = "Imported Integration"
        assumptions.append(
            _fact(
                "no integration name supplied; defaulting to 'Imported "
                "Integration'",
                "inferred:default_integration_name",
            )
        )
    option_prefix = opts.get("component_prefix")
    option_prefix = (
        option_prefix.strip()
        if isinstance(option_prefix, str) and option_prefix.strip()
        else None
    )
    if option_prefix:
        prefix = option_prefix
        facts.append(
            _fact(
                f"component_prefix '{prefix}' supplied via options",
                "options:component_prefix",
            )
        )
    else:
        prefix = _derive_component_prefix(name)
        assumptions.append(
            _fact(
                f"component_prefix '{prefix}' derived from the integration name",
                "inferred:prefix_from_name",
            )
        )
    naming = {"integration_name": name, "component_prefix": prefix}

    preset_parameters: Optional[Dict[str, Any]] = None
    if selected_preset is not None:
        preset_parameters = _derive_preset_parameters(
            selected_preset, flow, source, target, operations, naming
        )

    # ---- gap ordering + build attempt -------------------------------------------
    gaps.sort(key=lambda g: (g["severity"], g["code"], g["field"]))
    blocking = [g for g in gaps if g["severity"] == "blocking"]

    integration_spec_draft: Optional[Dict[str, Any]] = None
    ready_for_build = False
    if selected_preset is not None and preset_parameters is not None and not blocking:
        build = build_from_archetype_action(selected_preset, preset_parameters)
        if build.get("_success"):
            integration_spec_draft = build["integration_spec"]
            ready_for_build = True
        else:
            gaps.extend(_convert_build_failure_to_gaps(build))
            gaps.sort(key=lambda g: (g["severity"], g["code"], g["field"]))
            blocking = [g for g in gaps if g["severity"] == "blocking"]

    # ---- next steps -----------------------------------------------------------
    if ready_for_build:
        next_steps.append(
            "Review the drafts, then pass integration_spec_draft to "
            "build_integration(action='plan', config=...) to preview build "
            "steps before applying."
        )
    elif selected_preset is not None:
        next_steps.append(
            f"Resolve the {len(blocking)} blocking gap(s), then re-run "
            "import_integration_draft or call build_from_archetype("
            f"name='{selected_preset}', parameters=preset_parameters) with the "
            "completed values."
        )
    else:
        next_steps.append(
            "No existing archetype preset matches this source/trigger/target "
            "shape. Compose the flow from parts via compose_archetypes, or "
            "hand-author an IntegrationSpecV1 "
            "(get_schema_template(schema_name='IntegrationSpecV1'))."
        )
    if "by_reference" in (source.schema_status, target.schema_status):
        next_steps.append(
            "Referenced existing profile components are mapped through their "
            "#95 field index for mapping validation only; supply an inline "
            "schema.profile tree to make the preset parameters build-ready."
        )

    response: Dict[str, Any] = {
        "_success": True,
        **_IMPORT_FLAGS,
        "source_type": source_type,
        "input_provenance": input_provenance,
        "ready_for_build": ready_for_build,
        "confirmed_facts": facts,
        "inferred_assumptions": assumptions,
        "gaps": gaps,
        "pipeline_draft": pipeline_draft,
        "selected_preset": selected_preset,
        "preset_parameters": preset_parameters,
        "next_steps": next_steps,
    }
    if integration_spec_draft is not None:
        response["integration_spec_draft"] = integration_spec_draft
    return response
