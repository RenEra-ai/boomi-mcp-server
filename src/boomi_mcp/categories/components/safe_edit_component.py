"""Issue #97 (M9.7): safe existing-component edit workflow.

A two-phase, pull -> structured patch -> diff -> push workflow that brings the
Companion edit discipline into MCP without LLM-authored raw XML:

- ``prepare_component_edit_action`` — READ-ONLY. Pull the live component, apply a
  structured patch in memory (metadata smart-merge or builder + #45/#50
  preservation merge), and return a unified diff plus a ``confirmation_token``
  fingerprinting the base version, base XML, and patch. No Boomi mutation.
- ``apply_component_edit_action`` — confirmed write. Requires ``confirm_apply``
  and the token; re-fetches the component, ABORTS if it drifted since preview or
  the patch changed, otherwise pushes the merged XML through the same builder +
  preservation path, then returns a version comparison.

Rollback / version restore is intentionally NOT part of this surface (deferred).

Two patch modes (``patch["config"]`` keys decide which):
- METADATA (partial): only ``name``/``component_name``/``description``/
  ``folder_name``/``folder_id`` — the live root is smart-merged in place, so the
  caller changes just those fields and everything else is preserved verbatim.
- BODY (full config): any other field routes through the typed builder, which
  rebuilds its owned subtree from the config. The body config must therefore be
  the COMPLETE structured config that builder consumes (the same contract as
  ``build_integration``'s update path — e.g. a connector needs ``connector_type``
  plus all connection fields). It is NOT a field-level delta; the #45/#50 merge
  preserves encrypted values + unknown XML *outside* the builder's owned subtree.

#159 S3 adds a third mode, keyed by ``patch["authoring_request"]`` (never mixed
with ``config`` / ``map_context``):
- CANONICAL PROCESS UPDATE: an ``AuthoringRequestV1`` with exactly one direct
  ``process_ir`` unit, ``action="update"``, whose ``component_id`` is the edited
  process; every supporting component is BOUND to an existing component by explicit
  id and none is written. It is compiled (``compile_authoring_request_v1``), every
  dependency id is read and type-checked, the compiled plan is materialized against
  those ids (``materialize_canonical_process_xml``) and merged under
  ``PROCESS_PRESERVATION_POLICY``. The token additionally binds the compile hash and
  the dependencies' live identities; apply recomputes all of it and re-reads the
  target immediately before its single write.

The structured body path reuses ``integration_builder.build_structured_update_xml``
(builder dispatch + ``PRESERVATION_POLICY``) and
``component_update_preservation.merge_for_update`` so the previewed diff matches
exactly what apply later pushes, and encrypted values / unknown XML survive.
"""

import base64
import difflib
import hashlib
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional

from boomi import Boomi

from ._shared import (
    component_get_xml,
    set_description_element,
    ComponentGetDeadlineExceeded,
    component_get_deadline_envelope,
    _extract_api_error_msg,
)
from .component_update_preservation import merge_for_update
from .builders import BuilderValidationError
from .analyze_component import compare_versions
from ..integration_builder import build_structured_update_xml
from ...arrange import (
    DEFAULT_LAYOUT_MODE, ProcessLayoutError, arrange_process_body, layout_refusal,
    layout_report_for, resolve_layout_control,
)
from ...models.integration_models import IntegrationComponentSpec
from ...errors import (
    AUTHORING_APPLY_VALIDATION_REQUIRED,
    COMPONENT_EDIT_DRIFT_DETECTED,
    COMPONENT_EDIT_TOKEN_INVALID,
    COMPONENT_EDIT_TYPE_MISMATCH,
    PROCESS_COMPONENT_SCHEMA_INVALID,
    PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY,
    PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
)


#: #159 S3: the patch key of the canonical process update arm. Its value is an
#: ``AuthoringRequestV1`` carrying exactly one direct ``process_ir`` unit that
#: updates the component being edited.
_CANONICAL_PATCH_KEY = "authoring_request"

#: The ``update_mode`` the canonical arm reports.
_CANONICAL_UPDATE_MODE = "canonical_process_update"


# Patch ``config`` keys that route through the metadata smart-merge path (editing
# the live root in place, mirroring ``manage_component.update_component``). A
# patch whose config keys are all in this set never invokes a structured builder
# (which would fail required-field validation); a patch with any OTHER key is a
# body patch that goes through the builder + preservation merge.
_METADATA_PATCH_KEYS = frozenset({
    "name",
    "component_name",
    "description",
    "folder_name",
    "folder_id",
})


# ---------------------------------------------------------------------------
# Fingerprint / token helpers
# ---------------------------------------------------------------------------

def _canonical_json(value: Any) -> str:
    """Deterministic JSON for hashing (sorted keys, compact separators)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


#: #188: the token now also binds the effective layout mode and the digest of the
#: exact bytes prepare previewed. A token without this policy (issued before #188)
#: is refused with a re-prepare hint rather than applied under the new layout.
TOKEN_POLICY = 2


def _make_confirmation_token(
    component_id: str, version: int, base_xml_sha256: str, patch_sha256: str,
    *, layout_mode: str = DEFAULT_LAYOUT_MODE, proposed_xml_sha256: str = "",
    canonical: Optional[Dict[str, str]] = None,
) -> str:
    # The token is a stateless workflow + integrity FINGERPRINT, not an auth
    # secret or capability grant: the caller of apply already holds the profile
    # credentials and could mutate the component directly via manage_component, so
    # the token is intentionally unsigned (no server-side secret / nonce store
    # exists in this per-credential, potentially multi-instance MCP server — a
    # per-process HMAC key would reject legitimate tokens across Cloud Run
    # instances). The real safety guarantees are re-validated server-side at apply
    # time regardless of the token's provenance: confirm_apply=true is the explicit
    # confirmation gate, the version + base_xml_sha256 drift check re-fetches the
    # live component and aborts on any change, and patch_sha256 binds the applied
    # patch to the previewed one. A forged token can therefore never push a stale
    # or mismatched write.
    fields = {
        "component_id": component_id,
        "version": version,
        "base_xml_sha256": base_xml_sha256,
        "patch_sha256": patch_sha256,
        "token_policy": TOKEN_POLICY,
        "layout_mode": layout_mode,
        "proposed_xml_sha256": proposed_xml_sha256,
    }
    if canonical:
        # #159 S3: a canonical process update ALSO binds the compile it was
        # materialized from and the live identity (id, type, version, XML digest)
        # of every dependency it was materialized against. Only this arm carries
        # them, so every other token keeps its exact pre-S3 bytes.
        fields["canonical_compile_hash"] = canonical["compile_hash"]
        fields["canonical_resolved_sha256"] = canonical["resolved_sha256"]
    payload = _canonical_json(fields)
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


def _decode_confirmation_token(token: str) -> Dict[str, Any]:
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("confirmation_token payload is not an object")
    return data


def _normalize_for_diff(xml_text: str) -> str:
    """Re-serialize XML through ElementTree so a diff against builder/merge output
    (also ET-serialized) shows only logical changes, not namespace-prefix churn."""
    return ET.tostring(ET.fromstring(xml_text), encoding="unicode")


def _unified_diff(current_xml: str, merged_xml: str, max_diff_lines: int):
    """Unified diff of normalized-current vs merged. Returns (lines, truncated)."""
    try:
        from_lines = _normalize_for_diff(current_xml).splitlines()
    except ET.ParseError:
        from_lines = current_xml.splitlines()
    diff_lines = list(difflib.unified_diff(
        from_lines,
        merged_xml.splitlines(),
        fromfile="current",
        tofile="proposed",
        lineterm="",
    ))
    truncated = False
    if max_diff_lines and len(diff_lines) > max_diff_lines:
        diff_lines = diff_lines[:max_diff_lines]
        truncated = True
    return diff_lines, truncated


def _builder_validation_envelope(exc: BuilderValidationError) -> Dict[str, Any]:
    envelope: Dict[str, Any] = {
        "_success": False,
        "error_code": exc.error_code,
        "error": str(exc),
        "field": exc.field,
        "hint": exc.hint,
    }
    if getattr(exc, "details", None):
        envelope["details"] = exc.details
    return envelope


# ---------------------------------------------------------------------------
# Patch validation + merge (shared by prepare and apply so preview == write)
# ---------------------------------------------------------------------------

def _validate_patch_shape(patch: Any) -> Optional[Dict[str, Any]]:
    """Return an error envelope if the patch is structurally invalid, else None."""
    if not isinstance(patch, dict):
        return {
            "_success": False,
            "error": "patch must be a JSON object with optional component_type, config, map_context.",
            "field": "patch",
        }
    config = patch.get("config", {})
    if config is None:
        config = {}
    if not isinstance(config, dict):
        return {
            "_success": False,
            "error": "patch.config must be a JSON object of fields to change.",
            "field": "config",
        }
    if config.get("xml"):
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_RAW_XML_UNSUPPORTED",
            "error": (
                "Raw XML patches are not supported by the safe edit workflow. "
                "Use structured fields, or manage_component(action='update') with "
                "config.xml as an explicit full-replacement escape hatch."
            ),
            "field": "config.xml",
            "hint": (
                "Set structured fields (e.g. host, base_url, query, name, "
                "description) instead of raw XML so encrypted values and unknown "
                "subtrees are preserved through the #45/#50 merge."
            ),
        }
    if _CANONICAL_PATCH_KEY in patch:
        # #159 S3: the canonical process update arm. It carries the WHOLE body as
        # one typed authoring unit, so a structured `config` or a `map_context`
        # beside it would be a second, contradictory description of the same
        # write. Refused rather than merged.
        if not isinstance(patch.get(_CANONICAL_PATCH_KEY), dict):
            return {
                "_success": False,
                "error_code": PROCESS_COMPONENT_SCHEMA_INVALID,
                "error": "patch.authoring_request must be a JSON object (an AuthoringRequestV1).",
                "field": _CANONICAL_PATCH_KEY,
            }
        mixed = sorted(
            key for key in ("config", "map_context")
            if patch.get(key) not in (None, {})
        )
        if mixed:
            return {
                "_success": False,
                "error_code": PROCESS_COMPONENT_SCHEMA_INVALID,
                "error": (
                    "patch.authoring_request cannot be combined with "
                    + " or ".join("patch." + key for key in mixed)
                    + ": the canonical process update arm carries the whole body."
                ),
                "field": mixed[0],
                "hint": (
                    "Send either a patch.config edit, or patch.authoring_request "
                    "alone (rename the process through its unit's envelope name)."
                ),
            }
        return None
    map_context = patch.get("map_context")
    if map_context is not None and not isinstance(map_context, dict):
        return {
            "_success": False,
            "error": "patch.map_context must be a JSON object with source_index/target_index.",
            "field": "map_context",
        }
    return None


def _resolve_component_type(patch: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve the effective component_type, defaulting to the pulled root type.

    Returns {"_success": True, "component_type": str} or an error envelope.
    """
    pulled_type = (current.get("type") or "").strip()
    supplied = patch.get("component_type")
    if supplied is not None:
        if not isinstance(supplied, str) or not supplied.strip():
            return {
                "_success": False,
                "error": "patch.component_type must be a non-empty string when supplied.",
                "field": "component_type",
            }
        if pulled_type and supplied.strip() != pulled_type:
            return {
                "_success": False,
                "error_code": "COMPONENT_EDIT_TYPE_MISMATCH",
                "error": (
                    f"patch.component_type {supplied.strip()!r} does not match the "
                    f"live component type {pulled_type!r}."
                ),
                "field": "component_type",
                "hint": (
                    "Omit component_type to use the live type, or correct it to "
                    "match the component you are editing."
                ),
            }
        component_type = supplied.strip()
    else:
        component_type = pulled_type
    if not component_type:
        return {
            "_success": False,
            "error": "Could not determine component_type from the patch or the live component.",
            "field": "component_type",
            "hint": "Pass patch.component_type explicitly (e.g. 'connector-settings').",
        }
    return {"_success": True, "component_type": component_type}


def _compute_merged_xml(
    boomi_client: Boomi,
    current: Dict[str, Any],
    component_type: str,
    config: Dict[str, Any],
    map_context: Optional[Dict[str, Any]],
    *,
    layout_mode: str = DEFAULT_LAYOUT_MODE,
) -> Dict[str, Any]:
    """Build the merged XML for ``config`` against ``current`` — no push.

    Returns {"_success": True, "merged_xml": str, "update_mode": str, "layout":
    report-or-None} or a structured error envelope. Deterministic for a given
    (current, patch, layout_mode) so the prepare preview and the apply write
    produce byte-identical XML. A body patch on a process is arranged here
    (#188), once, after the merge; a metadata patch keeps the live geometry.
    """
    current_xml = current["xml"]
    config_keys = set(config.keys())

    # Metadata-only patch -> smart-merge the live root in place (mirrors
    # manage_component.update_component). Editing the parsed current tree
    # preserves every other subtree, so encrypted values / unknown XML survive.
    if config_keys and config_keys <= _METADATA_PATCH_KEYS:
        try:
            root = ET.fromstring(current_xml)
        except ET.ParseError as exc:
            return {
                "_success": False,
                "error": f"Live component XML could not be parsed: {exc}",
                "field": "component_id",
            }
        new_name = config.get("name") or config.get("component_name")
        if new_name:
            root.set("name", new_name)
        if config.get("folder_id"):
            root.set("folderId", config["folder_id"])
        if config.get("folder_name"):
            root.set("folderName", config["folder_name"])
        if "description" in config:
            set_description_element(root, config["description"])
        merged_xml = ET.tostring(root, encoding="unicode")
        return {
            "_success": True,
            "merged_xml": merged_xml,
            "update_mode": "metadata_smart_merge",
            "layout": layout_report_for(merged_xml),
        }

    # Body patch -> structured builder + #45/#50 preservation merge. The builder
    # rebuilds its owned subtree from `config`, so `config` must be the COMPLETE
    # structured config that builder requires (incl. the type discriminator such
    # as connector_type / profile_type), not a field-level delta. An incomplete
    # body config surfaces the builder's own structured error (missing field /
    # UPDATE_PRESERVATION_POLICY_UNSUPPORTED with a hint to use metadata-only
    # fields for a partial edit). merge_for_update then preserves everything
    # outside the owned subtree (encrypted values, unknown XML).
    comp = IntegrationComponentSpec(
        key="safe_edit",
        type=component_type,
        action="update",
        name=config.get("name") or config.get("component_name"),
        config=dict(config),
    )
    payload = dict(config)
    payload.setdefault("component_type", component_type)
    if comp.name:
        if component_type == "process":
            payload.setdefault("name", comp.name)
        elif component_type in ("connector-settings", "connector-action"):
            payload.setdefault("component_name", comp.name)
            payload.setdefault("name", comp.name)
        else:
            payload.setdefault("component_name", comp.name)

    map_ctx = map_context or {}
    prep = build_structured_update_xml(
        boomi_client,
        comp,
        payload,
        source_index=map_ctx.get("source_index"),
        target_index=map_ctx.get("target_index"),
    )
    if not prep.get("_success"):
        return prep
    try:
        merged_xml = merge_for_update(current_xml, prep["built_xml"], prep["policy"])
    except BuilderValidationError as exc:
        return _builder_validation_envelope(exc)
    try:
        merged_xml, layout_report = arrange_process_body(merged_xml, layout_mode)
    except ProcessLayoutError as exc:
        return {**layout_refusal(exc), "boomi_mutation": False}
    return {
        "_success": True,
        "merged_xml": merged_xml,
        "update_mode": "read_merge_write",
        "layout": None if layout_report["reason"] == "not_process" else layout_report,
    }


# ---------------------------------------------------------------------------
# #159 S3 — the canonical process update arm (patch.authoring_request)
# ---------------------------------------------------------------------------

def _canonical_refusal(error_code: str, error: str, field: str, hint: str = "") -> Dict[str, Any]:
    envelope = {"_success": False, "error_code": error_code, "error": error, "field": field}
    if hint:
        envelope["hint"] = hint
    return envelope


def _parse_canonical_request(raw: Dict[str, Any], phase: str):
    """``(request, None)`` or ``(None, refusal)``: the SAME parser plan/compile/apply use.

    Malformed requests are served through ``build_integration``'s own value-free
    envelopes, so this route reports a bad unit exactly as that one does.
    """
    from pydantic import ValidationError

    from ...models.authoring_workflow import (
        AuthoringRequestProcessIRValidationError,
        parse_authoring_request_v1,
    )
    from ..integration_builder import (
        _reject_invalid_process_ir,
        _reject_invalid_typed_request,
    )

    try:
        return parse_authoring_request_v1(dict(raw)), None
    except AuthoringRequestProcessIRValidationError as exc:
        return None, _reject_invalid_process_ir(exc, phase)
    except ValidationError as exc:
        return None, _reject_invalid_typed_request(exc, phase)


def _admit_canonical_request(request, component_id: str) -> Optional[Dict[str, Any]]:
    """Refuse anything but ONE direct ``process_ir`` unit updating THIS component,
    supported only by components the request BINDS (never writes) by explicit id.

    "Binds, never writes" is apply's own answer (``apply_writes_component_config``)
    under the request's conflict policy, and "by explicit id" is the declared binding
    apply would act on (``declared_bindings_for_components``) — not a hand list of
    spellings, so a ``reference_only`` UPDATE (a write) or a ``clone`` policy (which
    creates) is refused here exactly because apply would write it.
    """
    from ..integration_builder import (
        apply_writes_component_config,
        canonical_component_id,
        declared_bindings_for_components,
    )

    intent = request.intent
    if getattr(intent, "intent_kind", None) != "process_ir":
        return _canonical_refusal(
            PROCESS_COMPONENT_SCHEMA_INVALID,
            "patch.authoring_request must carry a direct process_ir intent; "
            f"{getattr(intent, 'intent_kind', None)!r} is not one.",
            "authoring_request.intent.intent_kind",
            "Author the process body as one ProcessIR unit "
            "(intent_kind='process_ir') with action='update'.",
        )
    units = tuple(intent.units or ())
    if len(units) != 1:
        return _canonical_refusal(
            PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY,
            f"patch.authoring_request must carry exactly one unit, not {len(units)}: "
            "safe edit updates one existing process and writes no other root.",
            "authoring_request.intent.units",
        )
    envelope = units[0].envelope
    if envelope.action != "update":
        return _canonical_refusal(
            PROCESS_COMPONENT_SCHEMA_INVALID,
            "The unit's envelope must declare action='update': safe edit never creates.",
            "authoring_request.intent.units.0.envelope.action",
        )
    declared_id = canonical_component_id(envelope.component_id)
    if not declared_id or declared_id != canonical_component_id(component_id):
        return _canonical_refusal(
            PROCESS_COMPONENT_SCHEMA_INVALID,
            "The unit's envelope.component_id must name the component being edited "
            f"({component_id!r}).",
            "authoring_request.intent.units.0.envelope.component_id",
            "Set envelope.component_id to the same id passed as component_id.",
        )
    policy = intent.conflict_policy
    components = tuple(intent.components or ())
    bindings = declared_bindings_for_components(components, policy)
    for index, comp in enumerate(components):
        pointer = f"authoring_request.intent.components.{index}"
        if apply_writes_component_config(comp, policy):
            return _canonical_refusal(
                PROCESS_COMPONENT_SCHEMA_INVALID,
                f"Supporting component {comp.key!r} would be WRITTEN by this request; "
                "safe edit writes only the process being edited.",
                pointer,
                "Bind each supporting component to an existing component by explicit id "
                "(component_id with action='create' under conflict_policy='reuse', or "
                "config.reference_only=true with config.component_id).",
            )
        bound = bindings.get(comp.key)
        if not bound:
            return _canonical_refusal(
                PROCESS_COMPONENT_SCHEMA_INVALID,
                f"Supporting component {comp.key!r} does not bind an existing component "
                "by explicit id; safe edit resolves nothing by name.",
                pointer,
            )
        if canonical_component_id(bound) == declared_id:
            return _canonical_refusal(
                PROCESS_COMPONENT_SCHEMA_INVALID,
                f"Supporting component {comp.key!r} names the process being edited.",
                pointer,
            )
    return None


def _verified_dependencies(boomi_client: Boomi, request) -> Dict[str, Any]:
    """Read EVERY supporting id from the account and check its live type.

    A clean compile does not verify a dependency: the compile reads connectors only,
    and an unreadable one contributes nothing there. This route writes a process
    whose emitted ids are these, so each one is read here, a failed read refuses,
    and the reading (id, type, version, XML digest) is what the token binds.
    """
    from ..integration_builder import declared_bindings_for_components

    intent = request.intent
    bindings = declared_bindings_for_components(
        tuple(intent.components or ()), intent.conflict_policy
    )
    readings: Dict[str, Dict[str, Any]] = {}
    for index, comp in enumerate(intent.components or ()):
        bound = bindings[comp.key]
        pointer = f"authoring_request.intent.components.{index}"
        try:
            fetched = component_get_xml(boomi_client, bound)
        except ComponentGetDeadlineExceeded as exc:
            return component_get_deadline_envelope(exc)
        except Exception as exc:
            return _canonical_refusal(
                PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
                f"Supporting component {comp.key!r} ({bound!r}) could not be read from "
                f"the account: {_extract_api_error_msg(exc)}",
                pointer,
                "Bind an id that exists in this account.",
            )
        live_type = (fetched.get("type") or "").strip()
        if live_type != str(comp.type).strip():
            return _canonical_refusal(
                PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
                f"Supporting component {comp.key!r} declares type {comp.type!r}, but "
                f"{bound!r} is a {live_type or 'component of unknown type'!r} in the account.",
                pointer,
            )
        readings[comp.key] = {
            "component_id": bound,
            "type": live_type,
            "version": fetched.get("version"),
            "xml_sha256": _sha256(fetched.get("xml") or ""),
        }
    return {"_success": True, "readings": readings}


def _compute_canonical_xml(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    current: Dict[str, Any],
    raw_request: Dict[str, Any],
    *,
    phase: str,
    layout_mode: str = DEFAULT_LAYOUT_MODE,
) -> Dict[str, Any]:
    """Compile, verify, materialize and merge ONE canonical process update — no push.

    The same chain ``build_integration``'s canonical update runs: the request is
    compiled by ``compile_authoring_request_v1``; the COMPILED materialization plan is
    materialized by ``materialize_canonical_process_xml`` against the verified live
    ids and the compile's connector-resolution snapshot; and the result is merged
    into the live component under ``PROCESS_PRESERVATION_POLICY`` (what
    ``_execute_canonical_process`` passes to ``_apply_structured_update``), then
    arranged once. Deterministic for a given (current, request, account) so prepare
    and apply produce byte-identical XML.

    Returns ``{"_success": True, "merged_xml", "update_mode", "layout",
    "compile_hash", "resolved_sha256"}`` or a refusal envelope.
    """
    from ...authoring.workflow import AuthoringWorkflowError, compile_authoring_request_v1
    from ..integration_builder import _authoring_error_envelope, _client_account_id
    from .builders._process_preservation import PROCESS_PRESERVATION_POLICY
    from .canonical_process_apply import (
        CanonicalProcessApplyError,
        materialize_canonical_process_xml,
    )

    request, refusal = _parse_canonical_request(raw_request, phase)
    if refusal is not None:
        return refusal
    refusal = _admit_canonical_request(request, component_id)
    if refusal is not None:
        return refusal
    root_key = request.intent.units[0].envelope.component_key

    try:
        compile_result, internals = compile_authoring_request_v1(
            request,
            boomi_client=boomi_client,
            profile=profile,
            account_id=_client_account_id(boomi_client),
        )
    except AuthoringWorkflowError as exc:
        return _authoring_error_envelope(exc, phase)
    if internals.component_plan_unjudged:
        return _canonical_refusal(
            AUTHORING_APPLY_VALIDATION_REQUIRED,
            "The component plan could not be built from the account, so the bindings "
            "this update compiles against are unjudged; nothing was written.",
            "authoring_request",
            "Retry once the account's component metadata can be read.",
        )

    verified = _verified_dependencies(boomi_client, request)
    if not verified.get("_success"):
        return verified
    readings = verified["readings"]
    resolved = dict(internals.resolved_component_ids or {})
    from ..integration_builder import canonical_component_id

    mismatched = sorted(
        key for key, reading in readings.items()
        if canonical_component_id(resolved.get(key)) != canonical_component_id(reading["component_id"])
    )
    if canonical_component_id(resolved.get(root_key)) != canonical_component_id(component_id):
        mismatched.append(root_key)
    if mismatched:
        return _canonical_refusal(
            PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
            "The compile resolved these keys to a component other than the one the "
            f"request names: {', '.join(mismatched)}.",
            "authoring_request.intent.components",
        )

    plan = (internals.materialization_plans or {}).get(root_key)
    if plan is None:
        return _canonical_refusal(
            AUTHORING_APPLY_VALIDATION_REQUIRED,
            f"The compile produced no materialization plan for {root_key!r}.",
            "authoring_request.intent.units.0",
        )
    try:
        built_xml = materialize_canonical_process_xml(
            plan=plan,
            id_registry={key: reading["component_id"] for key, reading in readings.items()},
            symbols=internals.symbols,
            snapshot=internals.connector_resolution_snapshot,
        )
    except CanonicalProcessApplyError as exc:
        return _canonical_refusal(
            exc.error_code or PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID,
            str(exc), "authoring_request.intent.units.0",
        )
    try:
        merged_xml = merge_for_update(current["xml"], built_xml, PROCESS_PRESERVATION_POLICY)
    except BuilderValidationError as exc:
        return _builder_validation_envelope(exc)
    try:
        merged_xml, layout_report = arrange_process_body(merged_xml, layout_mode)
    except ProcessLayoutError as exc:
        return {**layout_refusal(exc), "boomi_mutation": False}
    resolved_material = {
        "root": {"key": root_key, "component_id": canonical_component_id(component_id)},
        "dependencies": readings,
    }
    return {
        "_success": True,
        "merged_xml": merged_xml,
        "update_mode": _CANONICAL_UPDATE_MODE,
        "layout": None if layout_report["reason"] == "not_process" else layout_report,
        "compile_hash": compile_result.revision_binding.compile_hash,
        "resolved_sha256": _sha256(_canonical_json(resolved_material)),
    }


def _require_live_process(patch: Dict[str, Any], current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The canonical arm edits a PROCESS only — the live component's own type decides."""
    type_result = _resolve_component_type(patch, current)
    if not type_result.get("_success"):
        return type_result
    if type_result["component_type"] != "process":
        return {
            "_success": False,
            "error_code": COMPONENT_EDIT_TYPE_MISMATCH,
            "error": (
                "patch.authoring_request updates a process, but the live component is a "
                f"{type_result['component_type']!r}."
            ),
            "field": "component_id",
        }
    return None


# ---------------------------------------------------------------------------
# Phase 1 — prepare (read-only)
# ---------------------------------------------------------------------------

def prepare_component_edit_action(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    patch: Dict[str, Any],
    max_diff_lines: int = 200,
    *,
    layout_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Read-only: pull the component, preview a structured patch, return diff + token.

    Performs NO Boomi mutation. The returned ``confirmation_token`` must be
    passed back to ``apply_component_edit_action`` to commit the change.

    #188: ``patch.layout`` ("arrange" default | "preserve") selects the layout of
    a process body patch (an internal caller may pass ``layout_mode``); the diff
    and the token cover the arranged bytes.
    """
    shape_error = _validate_patch_shape(patch)
    if shape_error is not None:
        shape_error.setdefault("boomi_mutation", False)
        return shape_error
    try:
        _, effective_layout = resolve_layout_control(patch, explicit=layout_mode)
    except ProcessLayoutError as exc:
        return {**layout_refusal(exc), "boomi_mutation": False}
    canonical = _CANONICAL_PATCH_KEY in patch
    config = patch.get("config") or {}
    if not config and not canonical:
        return {
            "_success": False,
            "error": "patch.config must contain at least one field to change.",
            "field": "config",
            "boomi_mutation": False,
        }

    try:
        current = component_get_xml(boomi_client, component_id)
    except ComponentGetDeadlineExceeded as exc:
        return component_get_deadline_envelope(exc)
    except Exception as exc:
        return {
            "_success": False,
            "error": f"Failed to fetch component {component_id!r}: {_extract_api_error_msg(exc)}",
            "exception_type": type(exc).__name__,
            "boomi_mutation": False,
        }

    type_result = _resolve_component_type(patch, current)
    if not type_result.get("_success"):
        type_result.setdefault("boomi_mutation", False)
        return type_result
    component_type = type_result["component_type"]

    canonical_binding: Dict[str, Any] = {}
    if canonical:
        refusal = _require_live_process(patch, current)
        if refusal is not None:
            refusal.setdefault("boomi_mutation", False)
            return refusal
        merged = _compute_canonical_xml(
            boomi_client, profile, component_id, current, patch[_CANONICAL_PATCH_KEY],
            phase="prepare", layout_mode=effective_layout,
        )
        if merged.get("_success"):
            canonical_binding = {
                "compile_hash": merged["compile_hash"],
                "resolved_sha256": merged["resolved_sha256"],
            }
    else:
        merged = _compute_merged_xml(
            boomi_client, current, component_type, config, patch.get("map_context"),
            layout_mode=effective_layout,
        )
    if not merged.get("_success"):
        merged.setdefault("boomi_mutation", False)
        return merged
    merged_xml = merged["merged_xml"]

    base_version = current["version"]
    base_xml_sha256 = _sha256(current["xml"])
    patch_sha256 = _sha256(_canonical_json(patch))
    token = _make_confirmation_token(
        component_id, base_version, base_xml_sha256, patch_sha256,
        layout_mode=effective_layout, proposed_xml_sha256=_sha256(merged_xml),
        canonical=canonical_binding or None,
    )
    diff_lines, truncated = _unified_diff(current["xml"], merged_xml, max_diff_lines)

    return {
        "_success": True,
        "read_only": True,
        "boomi_mutation": False,
        "component_id": component_id,
        "component_type": component_type,
        "base_version": base_version,
        "update_mode": merged["update_mode"],
        "preserves_unknown_xml": True,
        "no_change": not diff_lines,
        "diff": diff_lines,
        "diff_truncated": truncated,
        "layout": merged.get("layout"),
        "confirmation_token": token,
        "next_step": (
            "Review the diff, then call apply_component_edit with the same patch, "
            "this confirmation_token, and confirm_apply=true to commit."
        ),
    }


# ---------------------------------------------------------------------------
# Phase 2 — apply (confirmed write)
# ---------------------------------------------------------------------------

def apply_component_edit_action(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    patch: Dict[str, Any],
    confirmation_token: str,
    confirm_apply: bool = False,
    max_diff_lines: int = 200,
    *,
    layout_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Confirmed write: re-fetch, abort on drift, push the merged XML, compare versions.

    #188: the token binds the effective layout mode and the digest of the bytes
    prepare previewed; apply recomputes the same bytes and refuses (no write) if
    they differ, so the write is exactly the previewed diff.

    Requires ``confirm_apply=True`` and the ``confirmation_token`` from prepare.
    Aborts (no mutation) if the patch changed since prepare, the token is
    malformed, or the live component drifted (version or XML hash) after preview.
    """
    if confirm_apply is not True:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_CONFIRMATION_REQUIRED",
            "error": "apply_component_edit requires confirm_apply=true.",
            "field": "confirm_apply",
            "hint": (
                "Run prepare_component_edit first, review the diff, then call "
                "apply_component_edit with the same patch, its confirmation_token, "
                "and confirm_apply=true."
            ),
            "boomi_mutation": False,
        }

    shape_error = _validate_patch_shape(patch)
    if shape_error is not None:
        shape_error.setdefault("boomi_mutation", False)
        return shape_error
    canonical = _CANONICAL_PATCH_KEY in patch
    config = patch.get("config") or {}
    if not config and not canonical:
        return {
            "_success": False,
            "error": "patch.config must contain at least one field to change.",
            "field": "config",
            "boomi_mutation": False,
        }

    if not confirmation_token or not isinstance(confirmation_token, str):
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_TOKEN_INVALID",
            "error": "confirmation_token is required; obtain it from prepare_component_edit.",
            "field": "confirmation_token",
            "boomi_mutation": False,
        }
    try:
        token_data = _decode_confirmation_token(confirmation_token)
    except Exception:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_TOKEN_INVALID",
            "error": "confirmation_token is malformed or not a value from prepare_component_edit.",
            "field": "confirmation_token",
            "hint": "Re-run prepare_component_edit to obtain a fresh token.",
            "boomi_mutation": False,
        }

    if token_data.get("component_id") != component_id:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_TOKEN_INVALID",
            "error": (
                "confirmation_token was issued for a different component "
                f"({token_data.get('component_id')!r}, not {component_id!r})."
            ),
            "field": "confirmation_token",
            "boomi_mutation": False,
        }

    if token_data.get("token_policy") != TOKEN_POLICY:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_TOKEN_INVALID",
            "error": (
                "confirmation_token was issued before server-side canvas Arrange "
                "(#188) and does not bind the layout of the previewed bytes."
            ),
            "field": "confirmation_token",
            "hint": "Re-run prepare_component_edit to obtain a fresh token.",
            "boomi_mutation": False,
        }
    try:
        _, effective_layout = resolve_layout_control(patch, explicit=layout_mode)
    except ProcessLayoutError as exc:
        return {**layout_refusal(exc), "boomi_mutation": False}
    if token_data.get("layout_mode") != effective_layout:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_TOKEN_INVALID",
            "error": (
                f"confirmation_token was issued for layout {token_data.get('layout_mode')!r}, "
                f"not {effective_layout!r}."
            ),
            "field": "confirmation_token",
            "hint": "Re-run prepare_component_edit with the layout you mean to apply.",
            "boomi_mutation": False,
        }

    patch_sha256 = _sha256(_canonical_json(patch))
    if token_data.get("patch_sha256") != patch_sha256:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_PATCH_MISMATCH",
            "error": (
                "The patch differs from the one previewed by prepare_component_edit. "
                "Re-run prepare for the new patch and apply with its token."
            ),
            "field": "patch",
            "boomi_mutation": False,
        }

    try:
        current = component_get_xml(boomi_client, component_id)
    except ComponentGetDeadlineExceeded as exc:
        return component_get_deadline_envelope(exc)
    except Exception as exc:
        return {
            "_success": False,
            "error": f"Failed to fetch component {component_id!r}: {_extract_api_error_msg(exc)}",
            "exception_type": type(exc).__name__,
            "boomi_mutation": False,
        }

    base_version = current["version"]
    base_xml_sha256 = _sha256(current["xml"])
    if token_data.get("version") != base_version or token_data.get("base_xml_sha256") != base_xml_sha256:
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_DRIFT_DETECTED",
            "error": (
                "The component changed since prepare_component_edit previewed it; "
                "the edit was aborted to avoid overwriting that change."
            ),
            "component_id": component_id,
            "expected_version": token_data.get("version"),
            "current_version": base_version,
            "field": "component_id",
            "hint": "Re-run prepare_component_edit to preview against the current version.",
            "boomi_mutation": False,
        }

    type_result = _resolve_component_type(patch, current)
    if not type_result.get("_success"):
        type_result.setdefault("boomi_mutation", False)
        return type_result
    component_type = type_result["component_type"]

    if canonical:
        refusal = _require_live_process(patch, current)
        if refusal is not None:
            refusal.setdefault("boomi_mutation", False)
            return refusal
        # RECOMPUTED, never trusted from the token: the compile, the dependency
        # reads, the materialization and the merge all run again here.
        merged = _compute_canonical_xml(
            boomi_client, profile, component_id, current, patch[_CANONICAL_PATCH_KEY],
            phase="apply", layout_mode=effective_layout,
        )
    else:
        merged = _compute_merged_xml(
            boomi_client, current, component_type, config, patch.get("map_context"),
            layout_mode=effective_layout,
        )
    if not merged.get("_success"):
        merged.setdefault("boomi_mutation", False)
        return merged
    merged_xml = merged["merged_xml"]
    if canonical:
        # A token minted for any other arm carries no canonical binding, and one
        # whose compile or resolved dependencies moved since prepare is stale: the
        # account is not what the previewed bytes were compiled against.
        if (
            not token_data.get("canonical_compile_hash")
            or not token_data.get("canonical_resolved_sha256")
        ):
            return {
                "_success": False,
                "error_code": COMPONENT_EDIT_TOKEN_INVALID,
                "error": (
                    "confirmation_token does not bind a canonical compile; it was not "
                    "issued by prepare_component_edit for this patch.authoring_request."
                ),
                "field": "confirmation_token",
                "hint": "Re-run prepare_component_edit to obtain a fresh token.",
                "boomi_mutation": False,
            }
        moved = [
            name for name, token_field, value in (
                ("compile_hash", "canonical_compile_hash", merged["compile_hash"]),
                ("resolved_dependencies", "canonical_resolved_sha256", merged["resolved_sha256"]),
            )
            if token_data.get(token_field) != value
        ]
        if moved:
            return {
                "_success": False,
                "error_code": COMPONENT_EDIT_DRIFT_DETECTED,
                "error": (
                    "The canonical compile or its live dependencies changed since "
                    "prepare_component_edit previewed this update ("
                    + ", ".join(moved) + "); the edit was aborted."
                ),
                "component_id": component_id,
                "changed": moved,
                "field": "confirmation_token",
                "hint": "Re-run prepare_component_edit and review the new diff.",
                "boomi_mutation": False,
            }
    if token_data.get("proposed_xml_sha256") != _sha256(merged_xml):
        return {
            "_success": False,
            "error_code": "COMPONENT_EDIT_DRIFT_DETECTED",
            "error": (
                "The bytes this apply would write differ from the ones "
                "prepare_component_edit previewed; the edit was aborted."
            ),
            "component_id": component_id,
            "field": "confirmation_token",
            "hint": "Re-run prepare_component_edit and review the new diff.",
            "boomi_mutation": False,
        }

    if canonical:
        # #159 S3: the compile and the dependency reads ran AFTER the drift check
        # above, so the target is read once more, immediately before the single
        # write: a change landing during those reads refuses here, written-nothing.
        try:
            recheck = component_get_xml(boomi_client, component_id)
        except ComponentGetDeadlineExceeded as exc:
            return component_get_deadline_envelope(exc)
        except Exception as exc:
            return {
                "_success": False,
                "error": f"Failed to re-read component {component_id!r} before the write: "
                         f"{_extract_api_error_msg(exc)}",
                "exception_type": type(exc).__name__,
                "boomi_mutation": False,
            }
        if recheck["version"] != base_version or _sha256(recheck["xml"]) != base_xml_sha256:
            return {
                "_success": False,
                "error_code": COMPONENT_EDIT_DRIFT_DETECTED,
                "error": (
                    "The component changed while this apply compiled and read its "
                    "dependencies; the edit was aborted before the write."
                ),
                "component_id": component_id,
                "expected_version": base_version,
                "current_version": recheck["version"],
                "field": "component_id",
                "hint": "Re-run prepare_component_edit to preview against the current version.",
                "boomi_mutation": False,
            }

    try:
        boomi_client.component.update_component_raw(component_id, merged_xml)
    except Exception as exc:
        return {
            "_success": False,
            "error": f"Failed to push merged XML for component {component_id!r}: {_extract_api_error_msg(exc)}",
            "exception_type": type(exc).__name__,
            "boomi_mutation": False,
        }

    result: Dict[str, Any] = {
        "_success": True,
        "component_id": component_id,
        "component_type": component_type,
        "base_version": base_version,
        "update_mode": merged["update_mode"],
        "preserves_unknown_xml": True,
        "boomi_mutation": True,
        "message": f"Updated component {component_id!r} via {merged['update_mode']}.",
        "layout": merged.get("layout"),
    }

    # Expose version comparison after write (acceptance criterion). Best-effort:
    # a comparison failure does not undo the successful write.
    try:
        post = component_get_xml(boomi_client, component_id)
        new_version = post["version"]
        result["new_version"] = new_version
        if new_version != base_version:
            comparison = compare_versions(
                boomi_client,
                profile,
                component_id,
                {"source_version": base_version, "target_version": new_version},
            )
            if comparison.get("_success"):
                result["version_comparison"] = comparison
            else:
                result["version_comparison_error"] = comparison.get("error")
                result["diff"] = _unified_diff(current["xml"], merged_xml, max_diff_lines)[0]
        else:
            result["version_comparison_note"] = (
                "Write produced no new version (no effective change); "
                "no version comparison performed."
            )
    except ComponentGetDeadlineExceeded as exc:
        result["version_comparison_error"] = str(exc)
        result["diff"] = _unified_diff(current["xml"], merged_xml, max_diff_lines)[0]
    except Exception as exc:
        result["version_comparison_error"] = (
            f"Post-write re-fetch failed: {_extract_api_error_msg(exc)}"
        )
        result["diff"] = _unified_diff(current["xml"], merged_xml, max_diff_lines)[0]

    return result


__all__ = [
    "prepare_component_edit_action",
    "apply_component_edit_action",
]
