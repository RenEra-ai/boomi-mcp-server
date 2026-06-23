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
from ...models.integration_models import IntegrationComponentSpec


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


def _make_confirmation_token(
    component_id: str, version: int, base_xml_sha256: str, patch_sha256: str
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
    payload = _canonical_json({
        "component_id": component_id,
        "version": version,
        "base_xml_sha256": base_xml_sha256,
        "patch_sha256": patch_sha256,
    })
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
) -> Dict[str, Any]:
    """Build the merged XML for ``config`` against ``current`` — no push.

    Returns {"_success": True, "merged_xml": str, "update_mode": str} or a
    structured error envelope. Deterministic for a given (current, patch) so the
    prepare preview and the apply write produce byte-identical XML.
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
    return {
        "_success": True,
        "merged_xml": merged_xml,
        "update_mode": "read_merge_write",
    }


# ---------------------------------------------------------------------------
# Phase 1 — prepare (read-only)
# ---------------------------------------------------------------------------

def prepare_component_edit_action(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    patch: Dict[str, Any],
    max_diff_lines: int = 200,
) -> Dict[str, Any]:
    """Read-only: pull the component, preview a structured patch, return diff + token.

    Performs NO Boomi mutation. The returned ``confirmation_token`` must be
    passed back to ``apply_component_edit_action`` to commit the change.
    """
    shape_error = _validate_patch_shape(patch)
    if shape_error is not None:
        shape_error.setdefault("boomi_mutation", False)
        return shape_error
    config = patch.get("config") or {}
    if not config:
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

    merged = _compute_merged_xml(
        boomi_client, current, component_type, config, patch.get("map_context")
    )
    if not merged.get("_success"):
        merged.setdefault("boomi_mutation", False)
        return merged
    merged_xml = merged["merged_xml"]

    base_version = current["version"]
    base_xml_sha256 = _sha256(current["xml"])
    patch_sha256 = _sha256(_canonical_json(patch))
    token = _make_confirmation_token(
        component_id, base_version, base_xml_sha256, patch_sha256
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
) -> Dict[str, Any]:
    """Confirmed write: re-fetch, abort on drift, push the merged XML, compare versions.

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
    config = patch.get("config") or {}
    if not config:
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

    merged = _compute_merged_xml(
        boomi_client, current, component_type, config, patch.get("map_context")
    )
    if not merged.get("_success"):
        merged.setdefault("boomi_mutation", False)
        return merged
    merged_xml = merged["merged_xml"]

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
