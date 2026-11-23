"""Existing-integration import → pipeline / preset / canonical authoring-request drafts (M7.2, issue #48).

Read-only migration discovery: converts a STRUCTURED migration description
(``generic_integration_description``) or a source-tool export summary
(``source_tool_export_summary``) into

- a semantic ``pipeline_draft`` (a validated :class:`PipelineSpec` dump),
- the closest EXISTING archetype preset (``selected_preset``) with derived
  ``preset_parameters``, and
- an ``authoring_request_draft`` (#159 S3) — a canonical ``AuthoringRequestV1``
  payload: one pinned typed-recipe invocation for the recipe-backed presets, or
  one direct ProcessIR unit for REST -> REST — emitted ONLY when zero blocking
  gaps remain and the shared component-material authorities and the
  account-free authoring normalization accept it, so the tool never produces a
  broken build input.

Never calls Boomi, constructs an SDK client, or reads credentials. Every
response carries ``read_only=True`` / ``boomi_mutation=False`` /
``raw_xml_exposed=False``. Product/version/tool identifiers from export
summaries are preserved under ``input_provenance`` only — they never influence
normalization or preset selection (no product-specific preset forks).

Gap codes: the six ``MIGRATION_IMPORT_*`` constants below are the import's own
surface; gaps produced by delegated layers pass through their existing codes
(``PROFILE_INFERENCE_*`` from profile inference, the recipe / authoring
diagnostic codes from canonical draft validation) plus the delegated
``PROFILE_INFERENCE_CONFIRMATION_REQUIRED`` marker for schemas inferred from
samples that still need field confirmation.

Secret safety: artifact VALUES are never echoed in gap messages or details —
a value that failed vocabulary validation (protocol, transform kind, auth
mode, method) is arbitrary caller content and is reported by field path plus
the SUPPORTED vocabulary only; a value is named only when it matches known
vocabulary. Schema leaf paths (the identifiers ``infer_profile_fields``
already surfaces) and component ids remain reportable. Plaintext
secret-shaped auth keys are rejected as gaps and never copied into
``preset_parameters``; the same key-shape scan (``_scan_for_secret_shaped_keys``
— names like ``authorization`` / ``api_key`` / ``bearer`` / ``token``) guards
request headers and query parameters before they are copied into a draft, so a
credential smuggled through ``request_headers``/``query_parameters`` is gapped,
never echoed. (Consistent with the rest of the codebase, the scan is key-name
based, not a deep value inspection.)
"""

from __future__ import annotations

import json
import re

from boomi_mcp.connector_replay.ids import BOOMI_COMPONENT_ID_RE
from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError

from ..models.pipeline_models import PipelineSpec
from ..patterns.archetype_parameters import (
    JSONPayloadProfile,
    _flatten_payload_profile_leaves,
    _scan_for_secret_shaped_keys,
)
from .integration_authoring import infer_profile_fields_action

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
# Declared endpoint metadata the selected preset cannot faithfully EMIT (e.g.
# database_to_api_sync target request headers, which the archetype defers and
# drops). Blocking, so the tool never emits a draft that silently loses it.
MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT = "MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT"

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

def _selected_recipe_ref(preset: Optional[str]) -> Optional[Dict[str, Any]]:
    """The EXACT executable recipe the draft pins for ``preset``, else ``None``.

    #159 S3: read from the same table the draft is built from
    (``_PRESET_RECIPES``), so the served reference and the invocation can never
    disagree. ``None`` for REST -> REST (drafted as a direct ProcessIR unit; no
    executable recipe exists) and when no preset matched. Additive metadata:
    preset SELECTION and gap analysis are untouched.
    """
    if preset is None or preset not in _PRESET_RECIPES:
        return None
    recipe_id, recipe_version = _PRESET_RECIPES[preset]
    return {
        "recipe_id": recipe_id,
        "recipe_version": recipe_version,
        "entry_kind": "executable_recipe",
    }


# Transform kinds the import can express as archetype transform operations.
_SUPPORTED_TRANSFORM_KINDS = {"field_mapping", "mapping", "direct"}

# REST auth-mode vocabulary (RestCreateSettings enum). Values outside it are
# arbitrary caller content and are never echoed in messages.
_KNOWN_AUTH_MODES = {"basic", "bearer_token", "oauth2_client_credentials"}

# The Boomi component-id language, owned by `connector_replay.ids`. Previously
# spelled out here AND in the connector builder — same language, two spellings,
# each free to drift.
_UUID_RE = BOOMI_COMPONENT_ID_RE

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
                    + ". Unsupported transports need a hand-authored canonical "
                    "authoring request (get_schema_template(schema_name="
                    "'AuthoringRequestV1'))."
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
) -> Tuple[List[Dict[str, Any]], bool]:
    """Validate transform entries.

    Returns ``(mappings, transform_present)`` where ``mappings`` is every
    declared mapping dict (top-level ``mappings`` plus each field-mapping
    transform's ``mappings``) and ``transform_present`` is True when the flow
    declares ANY transform intent — including a singleton-object or an
    unsupported (e.g. XSLT) transform that yields no mappings. The caller uses
    ``transform_present`` to keep the ``map`` stage in the pipeline draft so a
    declared-but-unsupported transform is never hidden from the draft.
    """
    mappings: List[Dict[str, Any]] = []

    raw_mappings = flow.get("mappings")
    if isinstance(raw_mappings, list):
        mappings.extend(m for m in raw_mappings if isinstance(m, dict))

    transforms = flow.get("transforms")
    # A singleton transform object is normalized to a one-element list so it is
    # validated like a list — otherwise an unsupported dict transform (e.g.
    # {"kind": "xslt"}) would slip past the list guard undetected.
    if isinstance(transforms, dict):
        transforms = [transforms]
    # A transform is "present" for pipeline-shape purposes whenever the flow
    # declares mappings or any recognized transform-list entry (the malformed
    # non-list/non-dict case below is gapped and contributes no map stage).
    transform_present = bool(mappings) or (
        isinstance(transforms, list) and bool(transforms)
    )
    if transforms is not None and not isinstance(transforms, list):
        gaps.append(
            _gap(
                MIGRATION_IMPORT_INVALID_INPUT,
                "transforms",
                "transforms must be a transform object or a list of transform "
                "objects",
            )
        )
        transforms = []
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
                            "hand-author the map (a map_script reference is "
                            "available as a recipe transform operation, XSLT is "
                            "not — see issue #42)."
                        ),
                    )
                )
    return mappings, transform_present


def _resolve_mapping_side(
    value: Any,
    leaves: List[str],
    side: str,
    idx: int,
    assumptions: List[Dict[str, str]],
    gaps: List[Dict[str, Any]],
    *,
    strict_index: bool = False,
) -> Optional[str]:
    """Resolve one side of a mapping to a leaf path.

    When ``strict_index`` is True the endpoint schema is a #95-indexed existing
    profile (``schema_status == 'by_reference'``): there is no inline profile
    for archetype validation to check the mapping against, so every path/leaf
    MUST resolve to the supplied index or a PROFILE_INDEX_REQUIRED gap is
    emitted — the import never passes an unindexed path into the draft (never
    invents map keys). For inline/inferred schemas an unresolved full path is
    passed through and the archetype's own transform validator reports it.
    """
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
        if strict_index and token not in leaves:
            gaps.append(
                _gap(
                    MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED,
                    f"mappings[{idx}].{side}",
                    f"mapping {side} path does not resolve to the supplied #95 "
                    "index for the referenced existing profile; the import never "
                    "invents map keys against an indexed profile",
                    hint=(
                        "Use a leaf path present in schema.field_index, or "
                        "re-run index_profile_component to cover it."
                    ),
                )
            )
            return None
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
    if strict_index:
        gaps.append(
            _gap(
                MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED,
                f"mappings[{idx}].{side}",
                f"mapping {side} leaf name does not resolve to the supplied #95 "
                "index for the referenced existing profile; the import never "
                "invents map keys against an indexed profile",
                hint=(
                    "Use a leaf name/path present in schema.field_index, or "
                    "re-run index_profile_component to cover it."
                ),
            )
        )
        return None
    return token  # unknown — archetype validation reports it against the profile


# ---------------------------------------------------------------------------
# Preset parameter derivation
# ---------------------------------------------------------------------------


def _raw_headers(endpoint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the raw declared request-header dict ('request_headers' or
    'headers'), unsanitized — the caller must secret-scan before echoing."""
    headers = endpoint.get("request_headers")
    if not isinstance(headers, dict):
        headers = endpoint.get("headers")
    if isinstance(headers, dict) and headers:
        return headers
    return None


def _sanitized_metadata(
    raw: Any,
    role: str,
    field: str,
    kind: str,
    gaps: List[Dict[str, Any]],
) -> Optional[Dict[str, str]]:
    """Return a ``{str: str}`` copy of declared request metadata, or None.

    Rejects (as a blocking MIGRATION_IMPORT_MISSING_CREDENTIAL gap) any dict
    carrying a secret-shaped KEY at any depth, so a credential smuggled through
    ``request_headers`` / ``query_parameters`` is never copied into the returned
    draft. Neither the key nor the value is echoed in the gap. Non-secret
    metadata is passed through stringified.
    """
    if not isinstance(raw, dict) or not raw:
        return None
    if _scan_for_secret_shaped_keys(raw):
        gaps.append(
            _gap(
                MIGRATION_IMPORT_MISSING_CREDENTIAL,
                field,
                f"{role} declares a secret-shaped {kind} name; the import never "
                "echoes plaintext credentials into a draft",
                hint=(
                    "Route credentials through an opaque auth.credential_ref or "
                    "a reused connection; remove secret-bearing "
                    f"{kind}s from the request metadata."
                ),
            )
        )
        return None
    return {str(k): str(v) for k, v in raw.items()}


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
    gaps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Best-effort semantic mapping onto the selected archetype's contract.

    Slots the import cannot derive are OMITTED — the canonical draft's own
    validation (recipe models, shared authorities, normalization) then reports
    them as per-field gaps, so missing required values always surface as
    blocking gaps naming the draft field. Declared
    request metadata (query params / headers) is secret-scanned before it is
    copied into the draft, and metadata the selected preset cannot faithfully
    emit is gapped rather than silently dropped.
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
        qp = _sanitized_metadata(
            source_ep.get("query_parameters"),
            "source",
            "source.query_parameters",
            "query parameter",
            gaps,
        )
        if qp is not None:
            fetch["query_parameters"] = qp
        headers = _sanitized_metadata(
            _raw_headers(source_ep),
            "source",
            "source.request_headers",
            "request header",
            gaps,
        )
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
        # Declared request metadata is secret-scanned first, then either copied
        # into the archetype's own send-request vocabulary or — when the preset
        # cannot faithfully emit it — surfaced as a blocking gap, never silently
        # dropped or echoed.
        qp = _sanitized_metadata(
            target_ep.get("query_parameters"),
            "target",
            "target.query_parameters",
            "query parameter",
            gaps,
        )
        if qp is not None:
            if preset == "database_to_api_sync":
                # RestSendRequest takes a typed literal list, not a dict; the DB
                # preset emits these literal query params onto the operation.
                send["query_parameters"] = [
                    {
                        "name": k,
                        "value_source": "literal",
                        "literal_value": v,
                    }
                    for k, v in qp.items()
                ]
            else:
                send["query_parameters"] = qp
        if preset == "database_to_api_sync":
            # database_to_api_sync defers connection default_headers (they are
            # NOT emitted onto the connection or operation — see the archetype's
            # metadata-deferred note), so declared target headers cannot be
            # faithfully carried. Secret-scan (so a secret-shaped header is a
            # credential gap), then block on any remaining headers instead of
            # storing them where they would be silently lost.
            safe_headers = _sanitized_metadata(
                _raw_headers(target_ep),
                "target",
                "target.request_headers",
                "request header",
                gaps,
            )
            if safe_headers is not None:
                gaps.append(
                    _gap(
                        MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT,
                        "target.request_headers",
                        "the database_to_api_sync preset cannot emit target "
                        "request headers (its REST target material carries "
                        "none); these headers would be silently lost",
                        hint=(
                            "Reuse an existing REST connection that already "
                            "carries the headers, or hand-author the process "
                            "as a ProcessIR unit (get_schema_template("
                            "schema_name='AuthoringRequestV1'))."
                        ),
                    )
                )
        else:
            headers = _sanitized_metadata(
                _raw_headers(target_ep),
                "target",
                "target.request_headers",
                "request header",
                gaps,
            )
            if headers is not None:
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


# ---------------------------------------------------------------------------
# Canonical authoring-request draft (#159 S3)
# ---------------------------------------------------------------------------
#
# The draft the import serves is a canonical ``AuthoringRequestV1`` payload —
# what ``build_integration(action='plan'|'compile'|'apply',
# config={'authoring_request': ...})`` takes. Four presets are typed recipe
# presets and are drafted as ONE pinned recipe invocation (raw_input plus the
# closed component material); REST -> REST has no executable recipe and is
# drafted as one DIRECT ProcessIR unit whose supporting components come from the
# shared component-material authorities and emitters. Readiness is decided by
# those authorities and by the account-free workflow normalization, never by a
# hand list of required fields: every refusal becomes a value-free gap naming
# the draft field.

#: The exact executable recipe each recipe-backed preset is drafted against.
#: ``api_to_api_sync`` is absent: it is drafted as a direct process unit.
_PRESET_RECIPES: Dict[str, Tuple[str, str]] = {
    "database_to_api_sync": ("boomi.archetype.database_to_api_sync", "1.0.0"),
    "api_to_database_sync": ("boomi.archetype.api_to_database_sync", "1.0.0"),
    "http_listener_to_rest": ("boomi.archetype.http_listener_to_rest", "1.0.0"),
    "http_listener_to_db": ("boomi.archetype.http_listener_to_db", "1.0.0"),
}
_DIRECT_UNIT_PRESETS = frozenset({"api_to_api_sync"})
_DRAFT_FIELD = "authoring_request_draft"
_PROCESS_KEY = "main_process"
_API_TO_API_PROCESS_SUFFIX = "API to API Sync"
_API_TO_API_MAP_LABEL = "Map source response to target payload"


class _DraftBlocked(Exception):
    """The canonical draft cannot be completed; the gaps are already recorded."""


def _pointer_field(pointer: str, prefix: str = _DRAFT_FIELD) -> str:
    parts = [part for part in str(pointer or "").split("/") if part]
    return ".".join([prefix, *parts]) if parts else prefix


def _recipe_error_gaps(exc: Any, prefix: str) -> List[Dict[str, Any]]:
    """Value-free gaps from a ``RecipeError``: its codes, causes and pointers only."""
    gaps = []
    for diagnostic in getattr(exc, "diagnostics", ()) or ():
        causes = tuple(getattr(diagnostic, "cause_codes", ()) or ())
        gaps.append(
            _gap(
                str(diagnostic.code),
                _pointer_field(getattr(diagnostic, "path", "") or "", prefix),
                "the canonical draft refuses this value"
                + (f" ({', '.join(causes)})" if causes else ""),
                hint=(
                    "Correct the migration description at this draft field (see "
                    "get_schema_template for the recipe or component material), "
                    "then re-run import_integration_draft."
                ),
            )
        )
    return gaps


def _validation_error_gaps(exc: ValidationError, prefix: str) -> List[Dict[str, Any]]:
    """Value-free gaps from a pydantic error: location and error TYPE only, never
    the rendered message (it can quote the input)."""
    return [
        _gap(
            "MIGRATION_IMPORT_DRAFT_INVALID",
            ".".join([prefix, *(str(part) for part in error.get("loc", ()))]),
            f"the canonical draft refuses this field ({error.get('type', 'invalid')})",
            hint="Supply or correct this value, then re-run import_integration_draft.",
        )
        for error in exc.errors()
    ]


def _authoring_error_gaps(exc: Any) -> List[Dict[str, Any]]:
    """Value-free gaps from an ``AuthoringWorkflowError``'s ordered diagnostics."""
    gaps = []
    for diagnostic in getattr(exc, "diagnostics", ()) or ():
        if getattr(diagnostic, "severity", "error") != "error":
            continue
        causes = tuple(getattr(diagnostic, "cause_codes", ()) or ())
        gaps.append(
            _gap(
                str(diagnostic.code),
                _pointer_field(getattr(diagnostic, "path", "") or ""),
                str(getattr(diagnostic, "message", "") or "the canonical draft is refused")
                + (f" ({', '.join(causes)})" if causes else ""),
                hint=str(getattr(diagnostic, "remediation", "") or "") or None,
            )
        )
    return gaps or [
        _gap(str(getattr(exc, "code", "AUTHORING_REQUEST_INVALID")), _DRAFT_FIELD,
             "the canonical draft is refused")
    ]


def _unsupported_material(field: str, message: str, hint: str) -> Dict[str, Any]:
    return _gap(MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, field, message, hint=hint)


def _rest_connection_material(
    binding: Optional[Dict[str, Any]], role: str, gaps: List[Dict[str, Any]]
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """``(binding_mode, connection material)`` for a REST endpoint binding."""
    if not isinstance(binding, dict):
        return None, None
    if binding.get("mode") == "reuse":
        selector = {
            name: binding[name] for name in ("component_id", "component_name") if binding.get(name)
        }
        return "reuse", {"mode": "reuse", **selector}
    settings = binding.get("settings") or {}
    if settings.get("auth_mode") not in (None, "none"):
        gaps.append(
            _unsupported_material(
                f"{role}.auth",
                f"the canonical draft creates only an UNAUTHENTICATED {role} REST "
                "connection; an authenticated one must be an existing connection",
                "Reuse an existing REST connection that carries the credentials "
                f"({role}.connection.component_id or component_name), then re-run "
                "import_integration_draft.",
            )
        )
        return "create", None
    return "create", {"mode": "create", "base_url": settings.get("base_url")}


def _rest_request_material(
    request: Optional[Dict[str, Any]], role: str, gaps: List[Dict[str, Any]]
) -> Dict[str, Any]:
    request = dict(request or {})
    if request.get("request_headers"):
        gaps.append(
            _unsupported_material(
                f"{role}.request_headers",
                f"the canonical REST {role} material carries no request headers; "
                "these headers would be silently lost",
                "Reuse an existing REST connection that already carries the "
                "headers, or hand-author the process as a ProcessIR unit "
                "(get_schema_template(schema_name='AuthoringRequestV1')).",
            )
        )
    material: Dict[str, Any] = {}
    if isinstance(request.get("path"), str):
        material["path"] = request["path"]
    query = request.get("query_parameters")
    if isinstance(query, dict) and query:
        material["query_parameters"] = [
            {"name": name, "value_source": "literal", "literal_value": value}
            for name, value in query.items()
        ]
    elif isinstance(query, list) and query:
        material["query_parameters"] = list(query)
    return material


def _transform_material(params: Dict[str, Any], *, by_field_name: bool) -> Optional[Dict[str, Any]]:
    operations = (params.get("transform") or {}).get("operations")
    if not operations:
        return None
    return {
        "operations": [
            {
                "operation_type": "direct",
                "source_field": (
                    op.get("source_field")
                    if by_field_name
                    else op.get("source_path") or op.get("source_field")
                ),
                "target_path": op.get("target_path"),
            }
            for op in operations
        ]
    }


def _recipe_invocation_draft(
    preset: str, params: Dict[str, Any], integration_name: str, gaps: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """One pinned recipe invocation for a recipe-backed preset. Slots the import
    cannot derive are OMITTED, so the recipe's own models name them as gaps."""
    recipe_id, recipe_version = _PRESET_RECIPES[preset]
    naming = params.get("naming") or {}
    raw: Dict[str, Any] = {
        "version": "1",
        "process_key": _PROCESS_KEY,
        "naming": {"component_prefix": naming.get("component_prefix")},
    }
    material: Dict[str, Any] = {"version": "1"}
    source = params.get("source") or {}
    target = params.get("target") or {}
    if preset in ("http_listener_to_rest", "http_listener_to_db"):
        listener = params.get("listener") or {}
        listener_material: Dict[str, Any] = {}
        if listener.get("object_name"):
            listener_material["object_name"] = listener["object_name"]
        if listener.get("payload_profile") is not None:
            listener_material["request_profile"] = listener["payload_profile"]
        material["listener"] = listener_material
    if preset == "database_to_api_sync":
        binding = source.get("binding") or {}
        raw["source"] = {"binding_mode": binding.get("mode") or "create"}
        source_material: Dict[str, Any] = {}
        if binding.get("mode") == "reuse":
            source_material["connection"] = {
                "mode": "reuse",
                **{k: binding[k] for k in ("component_id", "component_name") if binding.get(k)},
            }
        material["source"] = source_material
    if preset == "api_to_database_sync":
        mode, connection = _rest_connection_material(source.get("binding"), "source", gaps)
        raw["source"] = {"binding_mode": mode or "create"}
        source_material = {
            "request": _rest_request_material(source.get("fetch_request"), "source", gaps),
        }
        if connection is not None:
            source_material["connection"] = connection
        if source.get("response_profile") is not None:
            source_material["response_profile"] = source["response_profile"]
        material["source"] = source_material
    if preset in ("database_to_api_sync", "http_listener_to_rest"):
        mode, connection = _rest_connection_material(target.get("binding"), "target", gaps)
        send = target.get("send_request") or {}
        raw["target"] = {"binding_mode": mode or "create", "method": send.get("method", "POST")}
        target_material: Dict[str, Any] = {
            "operation": _rest_request_material(send, "target", gaps),
        }
        if connection is not None:
            target_material["connection"] = connection
        if preset == "database_to_api_sync":
            if target.get("payload_profile") is not None:
                material["payload_profile"] = target["payload_profile"]
        elif target.get("payload_profile") is not None:
            target_material["payload_profile"] = target["payload_profile"]
        material["target"] = target_material
    transform = _transform_material(params, by_field_name=preset == "database_to_api_sync")
    if transform is not None:
        material["transform"] = transform
    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "recipe",
            "integration_name": integration_name,
            "invocations": [
                {
                    "recipe_id": recipe_id,
                    "recipe_version": recipe_version,
                    "invocation_id": preset,
                    "raw_input": raw,
                    "component_material": material,
                }
            ],
        },
    }


def _rest_source_connection_role() -> str:
    from ..patterns.primitives import _helpers

    return _helpers.ROLE_REST_SOURCE_CONNECTION


_API_TO_API_MATERIAL: Dict[str, Any] = {}


def _api_to_api_material_model() -> type:
    """The closed material the REST -> REST draft is validated as: the shared
    REST connection / request / payload-profile blocks and transform leaves, in
    a source / target / transform root (built once, lazily, so importing this
    module does not import the recipe layer)."""
    if "model" in _API_TO_API_MATERIAL:
        return _API_TO_API_MATERIAL["model"]
    from ..recipes.component_material import (
        FanoutRestConnectionMaterialV2,
        FanoutRestOperationMaterialV2,
        FanoutTransformOperationV2,
        _FanoutMaterialModel,
    )
    from pydantic import Field as _Field

    class ImportApiSourceMaterialV1(_FanoutMaterialModel):
        connection: FanoutRestConnectionMaterialV2
        request: FanoutRestOperationMaterialV2
        response_profile: JSONPayloadProfile

    class ImportApiTargetMaterialV1(_FanoutMaterialModel):
        connection: FanoutRestConnectionMaterialV2
        operation: FanoutRestOperationMaterialV2
        payload_profile: JSONPayloadProfile

    class ImportApiTransformMaterialV1(_FanoutMaterialModel):
        operations: Tuple[FanoutTransformOperationV2, ...] = _Field(min_length=1)

    class ImportApiToApiMaterialV1(_FanoutMaterialModel):
        source: ImportApiSourceMaterialV1
        target: ImportApiTargetMaterialV1
        transform: ImportApiTransformMaterialV1

    _API_TO_API_MATERIAL["model"] = ImportApiToApiMaterialV1
    _API_TO_API_MATERIAL["roots"] = {
        (ImportApiToApiMaterialV1, "source"): None,  # structural (model)
        (ImportApiToApiMaterialV1, "target"): None,  # structural (model)
        (ImportApiToApiMaterialV1, "transform"): None,  # structural (model)
        (ImportApiSourceMaterialV1, "connection"): None,  # structural (shared variant)
        (ImportApiSourceMaterialV1, "request"): None,  # structural (shared block)
        (ImportApiSourceMaterialV1, "response_profile"): None,  # structural (shared block)
        (ImportApiTargetMaterialV1, "connection"): None,  # structural (shared variant)
        (ImportApiTargetMaterialV1, "operation"): None,  # structural (shared block)
        (ImportApiTargetMaterialV1, "payload_profile"): None,  # structural (shared block)
        (ImportApiTransformMaterialV1, "operations"): None,  # structural (shared variants)
    }
    return ImportApiToApiMaterialV1


def _api_to_api_material_classes():
    """The credential walker's table: the SHARED table (the one authority for
    every shared block's rules) plus this root's structural rows only."""
    from types import MappingProxyType

    from ..recipes.component_material import _common_material_field_classes

    _api_to_api_material_model()
    return MappingProxyType({**_common_material_field_classes(), **_API_TO_API_MATERIAL["roots"]})


def _api_to_api_unit_draft(
    params: Dict[str, Any], integration_name: str, gaps: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """REST -> REST as ONE direct ProcessIR unit (no executable recipe exists).

    Supporting components come from the shared component-material authorities
    and emitters: the API->DB preset's REST fetch half
    (``assemble_rest_fetch_source_v1``), the shared transform group, and the
    shared REST send group; the root is ``source -> map_ref -> target -> stop``.
    """
    from ..patterns.base import PrimitiveBuildContext
    from ..recipes.builtins.sync import assemble_rest_fetch_source_v1
    from ..recipes.component_material import (
        _DEFAULT_STATUS_CODES_V2,
        _rt,
        authorize_rest_connection,
        check_credential_boundary,
        check_rest_request,
        authorize_rest_request,
        authorize_transform_operations,
        emit_rest_send_components,
        emit_transform_components,
    )
    from ..recipes.errors import RecipeError

    naming = params.get("naming") or {}
    prefix = naming.get("component_prefix")
    source = params.get("source") or {}
    target = params.get("target") or {}
    _, source_connection = _rest_connection_material(source.get("binding"), "source", gaps)
    source_request = _rest_request_material(source.get("fetch_request"), "source", gaps)
    target_mode, target_connection = _rest_connection_material(target.get("binding"), "target", gaps)
    send = target.get("send_request") or {}
    target_request = _rest_request_material(send, "target", gaps)
    transform = _transform_material(params, by_field_name=False)
    missing = [
        field
        for field, value in (
            ("source.connection", source_connection),
            ("source.response_profile", source.get("response_profile")),
            ("target.connection", target_connection),
            ("target.payload_profile", target.get("payload_profile")),
            ("transform.operations", transform),
        )
        if value is None
    ]
    if any(g["severity"] == "blocking" for g in gaps):
        raise _DraftBlocked()
    if missing:
        for field in missing:
            gaps.append(
                _gap(
                    "MIGRATION_IMPORT_DRAFT_INVALID",
                    f"{_DRAFT_FIELD}.material.{field}",
                    "the import could not derive this part of the REST -> REST draft",
                    hint="Describe it in the migration artifact, then re-run "
                    "import_integration_draft.",
                )
            )
        raise _DraftBlocked()
    prefix_field = f"{_DRAFT_FIELD}.material"
    material_model = _api_to_api_material_model()
    try:
        material = material_model.model_validate(
            {
                "source": {
                    "connection": source_connection,
                    "request": source_request,
                    "response_profile": source["response_profile"],
                },
                "target": {
                    "connection": target_connection,
                    "operation": target_request,
                    "payload_profile": target["payload_profile"],
                },
                "transform": transform,
            }
        )
    except ValidationError as exc:
        gaps.extend(_validation_error_gaps(exc, prefix_field))
        raise _DraftBlocked() from None
    source_conn, source_req = material.source.connection, material.source.request
    target_conn, target_req = material.target.connection, material.target.operation
    operations = tuple(material.transform.operations)
    response_profile = material.source.response_profile
    payload_profile = material.target.payload_profile
    rt = _rt()
    source_connection_key = rt.primitive_component_key(
        rt.SOURCE_PREFIX, _rest_source_connection_role()
    )
    try:
        # The shared credential boundary, exactly as a preset's preparer runs it
        # (userinfo, literal query names and values, credential references), then
        # each request's effective target judged on its own.
        check_credential_boundary(material, _api_to_api_material_classes(), pointer="")
        for side, conn, req, req_field in (
            ("source", source_conn, source_req, "request"),
            ("target", target_conn, target_req, "operation"),
        ):
            check_rest_request(
                connection_key=(
                    source_connection_key if side == "source"
                    else rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_REST_CONNECTION)
                ),
                path=req.path,
                path_pointer=f"/{side}/{req_field}/path",
                created_base_url=getattr(conn, "base_url", None) if conn.mode == "create" else None,
                connection_pointer=f"/{side}/connection",
                selected_key=None,
                selected_key_pointer=f"/{side}/connection",
                binding_mode=conn.mode,
                base_catalog={},
                query_parameters=req.query_parameters,
                query_parameters_pointer=f"/{side}/{req_field}/query_parameters",
            )
        fetched = assemble_rest_fetch_source_v1(
            source_conn,
            source_req,
            response_profile,
            component_prefix=prefix,
            pointer="/source",
        )
        authorized_target = authorize_rest_connection(target_conn, pointer="/target/connection")
        authorized_request = authorize_rest_request(
            target_req,
            pointer="/target/operation",
            method=send.get("method", "POST"),
            expected_status_codes=_DEFAULT_STATUS_CODES_V2,
        )
        authorized_operations = authorize_transform_operations(
            operations, base_catalog={}, pointer="/transform/operations"
        )
        context = PrimitiveBuildContext(
            integration_name=prefix, component_prefix=prefix, folder_path=None
        )
        target_profile_key = rt.primitive_component_key(rt.TRANSFORM_PREFIX, rt.ROLE_TARGET_PROFILE)
        transform_components = emit_transform_components(
            authorized_operations,
            context=context,
            key_prefix=rt.TRANSFORM_PREFIX,
            pointer="/transform",
            source={
                "source_profile_id": f"$ref:{fetched.profile_key}",
                "source_profile_type": "profile.json",
                "source_field_index": dict(fetched.field_index),
            },
            target_payload_profile=payload_profile.model_dump(),
        )
        send_components = emit_rest_send_components(
            authorized_target,
            authorized_request,
            context=context,
            key_prefix=rt.TARGET_PREFIX,
            pointer="/target",
            request_profile_key=target_profile_key,
        )
    except RecipeError as exc:
        gaps.extend(_recipe_error_gaps(exc, prefix_field))
        raise _DraftBlocked() from None
    components = [*fetched.components, *transform_components, *send_components]
    by_type = {}
    for component in components:
        by_type.setdefault(component.type, []).append(component.key)
    map_key = by_type["transform.map"][0]
    target_connection_key = rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_REST_CONNECTION)
    target_operation_key = rt.primitive_component_key(rt.TARGET_PREFIX, rt.ROLE_REST_OPERATION)
    depends_on = sorted(
        {fetched.connection_key, fetched.operation_key, map_key, target_connection_key,
         target_operation_key}
    )
    unit = {
        "envelope": {
            "component_key": _PROCESS_KEY,
            "name": f"{prefix} {_API_TO_API_PROCESS_SUFFIX}",
            "action": "create",
            "depends_on": depends_on,
        },
        "process_ir": {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": f"$ref:{fetched.connection_key}",
                        "operation_ref": f"$ref:{fetched.operation_key}",
                    },
                    {"kind": "map_ref", "map_ref": f"$ref:{map_key}", "label": _API_TO_API_MAP_LABEL},
                    {
                        "kind": "target",
                        "connection_ref": f"$ref:{target_connection_key}",
                        "operation_ref": f"$ref:{target_operation_key}",
                    },
                    {"kind": "stop"},
                ],
            },
        },
    }
    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "process_ir",
            "integration_name": integration_name,
            "units": [unit],
            "components": [
                component.model_dump(mode="json", exclude_none=True) for component in components
            ],
        },
    }


def _canonical_draft(
    preset: str, params: Dict[str, Any], gaps: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """The canonical authoring-request draft, or ``None`` with its gaps recorded.

    The draft is accepted only when the account-free workflow normalization
    (``authoring.workflow._normalize_intent``) accepts it: the request parser,
    the recipe input and material models, the trusted preparer and governance.
    """
    from ..authoring.workflow import AuthoringWorkflowError, _normalize_intent
    from ..models.authoring_workflow import parse_authoring_request_v1
    from ..recipes.errors import RecipeError

    integration_name = (params.get("naming") or {}).get("integration_name") or "Imported Integration"
    try:
        if preset in _DIRECT_UNIT_PRESETS:
            draft = _api_to_api_unit_draft(params, integration_name, gaps)
        else:
            draft = _recipe_invocation_draft(preset, params, integration_name, gaps)
    except _DraftBlocked:
        return None
    if any(g["severity"] == "blocking" for g in gaps):
        return None
    try:
        _normalize_intent(parse_authoring_request_v1(json.loads(json.dumps(draft))))
    except AuthoringWorkflowError as exc:
        gaps.extend(_authoring_error_gaps(exc))
        return None
    except RecipeError as exc:
        gaps.extend(_recipe_error_gaps(exc, _DRAFT_FIELD))
        return None
    except ValidationError as exc:
        gaps.extend(_validation_error_gaps(exc, _DRAFT_FIELD))
        return None
    return draft


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
    if trigger_declared and trigger_kind in ("listener", "manual"):
        # A listener trigger selects the listener preset and a manual trigger is
        # the default no-op start — both are realized in the emitted draft, so
        # they are genuine confirmed facts. A scheduled trigger is NOT emitted
        # (handled as an unsupported-construct gap below).
        facts.append(_fact(f"trigger classified as '{trigger_kind}'", "artifact:trigger"))
    elif not trigger_declared:
        assumptions.append(
            _fact(
                "no recognizable trigger declared; assuming manual/on-demand "
                "execution",
                "inferred:default_trigger_manual",
            )
        )

    raw_mappings, transform_present = _analyze_transforms(flow, facts, gaps)

    # Reliability / scheduling constructs the import does not map into any
    # preset's emitted parameters. Declaring them and still returning a
    # build-ready draft would silently drop the caller's reliability/schedule
    # intent, so surface each as a blocking unsupported-construct gap rather
    # than recording it as a confirmed fact that was never honored. Sub-keys are
    # not echoed (only the field name), keeping the secret-safe contract.
    if isinstance(flow.get("retry"), dict) and flow["retry"]:
        gaps.append(
            _gap(
                MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT,
                "retry",
                "the artifact declares retry behavior, but the import does not "
                "emit retry into the selected preset's output; it would be "
                "silently lost",
                hint=(
                    "Remove retry to draft without it, or wire a verified retry "
                    "after build (get_schema_template(schema_name="
                    "'design_pattern:connector_retry_design'))."
                ),
            )
        )
    if isinstance(flow.get("error_handling"), dict) and flow["error_handling"]:
        gaps.append(
            _gap(
                MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT,
                "error_handling",
                "the artifact declares error-handling / dead-letter behavior, "
                "but the import does not emit it into the selected preset's "
                "output; it would be silently lost",
                hint=(
                    "Remove error_handling to draft without it, or wire a "
                    "verified DLQ after build (get_schema_template(schema_name="
                    "'design_pattern:error_routing_and_dlq'))."
                ),
            )
        )
    if trigger_declared and trigger_kind == "scheduled":
        gaps.append(
            _gap(
                MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT,
                "trigger",
                "the artifact declares a scheduled trigger, but the import does "
                "not emit scheduling into the selected preset's output; it would "
                "be silently lost",
                hint=(
                    "Draft without scheduling, then attach a schedule at deploy "
                    "time (manage_schedules / orchestrate_deploy)."
                ),
            )
        )
    if isinstance(flow.get("deployment"), dict) and flow["deployment"]:
        # Deployment / runtime placement is a SEPARATE lifecycle step
        # (orchestrate_deploy), not part of the authoring-request draft this
        # tool emits — recorded as an informational assumption, not a blocking
        # gap that would wrongly hold back an otherwise-valid draft.
        assumptions.append(
            _fact(
                "deployment intent recorded; deployment/runtime activation is a "
                "separate step (orchestrate_deploy) and is not represented in "
                "the emitted draft",
                "inferred:deployment_deferred_to_orchestrate",
            )
        )

    # ---- mapping resolution -------------------------------------------------
    operations: List[Dict[str, Any]] = []
    for idx, mapping in enumerate(raw_mappings):
        from_value = mapping.get("from") or mapping.get("source") or mapping.get("source_path")
        to_value = mapping.get("to") or mapping.get("target") or mapping.get("target_path")
        resolved_from = _resolve_mapping_side(
            from_value,
            source.leaves,
            "from",
            idx,
            assumptions,
            gaps,
            strict_index=source.schema_status == "by_reference",
        )
        resolved_to = _resolve_mapping_side(
            to_value,
            target.leaves,
            "to",
            idx,
            assumptions,
            gaps,
            strict_index=target.schema_status == "by_reference",
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
                        "process as a ProcessIR unit (get_schema_template("
                        "schema_name='AuthoringRequestV1'))."
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
    # ``transform_present`` comes from _analyze_transforms' normalized view, so
    # a singleton-object transform counts here exactly as a list one does.
    has_map = bool(operations) or bool(raw_mappings) or transform_present
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
            selected_preset, flow, source, target, operations, naming, gaps
        )

    # ---- gap ordering + build attempt -------------------------------------------
    gaps.sort(key=lambda g: (g["severity"], g["code"], g["field"]))
    blocking = [g for g in gaps if g["severity"] == "blocking"]

    authoring_request_draft: Optional[Dict[str, Any]] = None
    ready_for_build = False
    if selected_preset is not None and preset_parameters is not None and not blocking:
        # #159 S3: the draft is a canonical authoring request (a pinned recipe
        # invocation, or a direct ProcessIR unit for REST -> REST), accepted only
        # when the shared authorities and the account-free normalization accept it.
        authoring_request_draft = _canonical_draft(selected_preset, preset_parameters, gaps)
        ready_for_build = authoring_request_draft is not None
        gaps.sort(key=lambda g: (g["severity"], g["code"], g["field"]))
        blocking = [g for g in gaps if g["severity"] == "blocking"]

    # ---- next steps -----------------------------------------------------------
    if ready_for_build:
        next_steps.append(
            "Review the drafts, then pass authoring_request_draft to "
            "build_integration(action='plan', config={'authoring_request': "
            "authoring_request_draft}) to preview build steps, then 'compile' "
            "and 'apply'."
        )
    elif selected_preset is not None:
        next_steps.append(
            f"Resolve the {len(blocking)} blocking gap(s) in the migration "
            "description, then re-run import_integration_draft."
        )
    else:
        next_steps.append(
            "No preset matches this source/trigger/target shape. Hand-author a "
            "canonical authoring request with a ProcessIR unit "
            "(get_schema_template(schema_name='AuthoringRequestV1')) and pass it "
            "to build_integration(config={'authoring_request': ...})."
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
        "selected_recipe_ref": _selected_recipe_ref(selected_preset),
        "preset_parameters": preset_parameters,
        "next_steps": next_steps,
    }
    if authoring_request_draft is not None:
        response["authoring_request_draft"] = authoring_request_draft
    return response
