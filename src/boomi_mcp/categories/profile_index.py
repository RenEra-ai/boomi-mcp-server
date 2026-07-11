"""index_profile_component — read-only live existing-profile field indexing (M7.5, issue #95).

Fetches a live Boomi profile component (``profile.json`` / ``profile.xml`` /
``profile.db``) and parses its exported XML into the repository's canonical
``field_index_by_path`` contract (platform keys, key paths, name paths,
mappable flags, and the profile component type) — the same shape the
generated-profile builders' ``build_field_index()`` emits. This lets a
transform.map referencing a literal existing-profile UUID be validated against
real fields, pre-mutation (see build_integration).

Read-only: the tool NEVER mutates. The default response is the NORMALIZED field
index, not raw XML; the exported component XML is exposed only when the caller
explicitly opts in with ``include_raw_xml=true``. Every response — success and
every structured error branch — carries ``read_only=True`` /
``boomi_mutation=False`` / ``raw_xml_exposed`` so the advertised safety contract
holds unconditionally, and no error branch ever leaks raw XML.

This is a SEPARATE surface from ``infer_profile_fields`` (issue #47), which stays
artifact-based (DB metadata / sample JSON / XSD / sample XML) and never calls
Boomi.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .components._shared import (
    ComponentGetDeadlineExceeded,
    component_get_xml,
)
from .components.builders.connector_builder import BuilderValidationError
from .components.builders.profile_generation import (
    PROFILE_INDEX_SUPPORTED_TYPES,
    PROFILE_INDEX_UNSUPPORTED_TYPE,
    index_existing_profile_xml,
)

# Every response carries these flags (mirrors the marketplace / inference
# read-only contracts). ``raw_xml_exposed`` is added per-response.
_INDEX_FLAGS = {"read_only": True, "boomi_mutation": False}

INDEX_PROFILE_COMPONENT_INVALID_INPUT = "INDEX_PROFILE_COMPONENT_INVALID_INPUT"
INDEX_PROFILE_COMPONENT_FETCH_FAILED = "INDEX_PROFILE_COMPONENT_FETCH_FAILED"


def _error_envelope(
    *,
    error_code: str,
    error: str,
    field: Optional[str] = None,
    hint: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Structured read-only error envelope — never exposes raw XML."""
    envelope: Dict[str, Any] = {
        "_success": False,
        **_INDEX_FLAGS,
        "raw_xml_exposed": False,
        "error_code": error_code,
        "error": error,
    }
    if field is not None:
        envelope["field"] = field
    if hint is not None:
        envelope["hint"] = hint
    if details:
        envelope["details"] = details
    return envelope


def index_profile_component_action(
    sdk: Any,
    component_id: Any,
    include_raw_xml: bool = False,
) -> Dict[str, Any]:
    """Fetch a live profile component and return its normalized field index.

    Args:
        sdk: an initialized Boomi SDK client (built by the account-scoped wrapper).
        component_id: the profile component UUID to index.
        include_raw_xml: when True, also return the exported component XML under
            ``raw_xml`` (default False — the normalized index only).

    Returns a success dict ``{_success, read_only, boomi_mutation, component_id,
    profile_component_type, field_index_by_path, mappable_paths, raw_xml_exposed
    [, raw_xml]}`` or a structured read-only error envelope.
    """
    if not isinstance(component_id, str) or not component_id.strip():
        return _error_envelope(
            error_code=INDEX_PROFILE_COMPONENT_INVALID_INPUT,
            error="component_id must be a non-empty string",
            field="component_id",
            hint="Pass the UUID of a profile.json / profile.xml / profile.db component.",
        )
    component_id = component_id.strip()

    # Read-only fetch of the component's exported XML.
    try:
        component = component_get_xml(sdk, component_id)
    except ComponentGetDeadlineExceeded as exc:
        return _error_envelope(
            error_code="COMPONENT_GET_DEADLINE_EXCEEDED",
            error=str(exc),
            field="component_id",
            details={"component_id": component_id},
        )
    except Exception as exc:  # SDK ApiError/not-found surface as generic Exception
        return _error_envelope(
            error_code=INDEX_PROFILE_COMPONENT_FETCH_FAILED,
            error=f"Failed to fetch component {component_id!r}: {exc}",
            field="component_id",
            hint=(
                "Confirm the component id exists in this profile and is a "
                "profile.json / profile.xml / profile.db component."
            ),
            details={"component_id": component_id},
        )

    component_type = component.get("type") if isinstance(component, dict) else None
    # ``id`` is the RAW exported componentId (empty when the XML omits it);
    # ``component_id`` would mask a missing one by substituting the request.
    exported_id = component.get("id") if isinstance(component, dict) else None
    exported_id = exported_id.strip() if isinstance(exported_id, str) else ""
    raw_xml = component.get("xml") if isinstance(component, dict) else None
    if not raw_xml:
        return _error_envelope(
            error_code=INDEX_PROFILE_COMPONENT_FETCH_FAILED,
            error=f"Component {component_id!r} returned no XML body",
            field="component_id",
            details={"component_id": component_id, "component_type": component_type},
        )
    # FAIL-CLOSED identity + type verification (plan requirement): the exported
    # componentId must be PRESENT and equal the request, and the declared metadata
    # type must be EXACTLY one of the supported profile types — never fall through
    # to XML-root scanning (which could mis-index a non-profile component that
    # embeds a profile-shaped subtree).
    if not exported_id or exported_id != component_id:
        return _error_envelope(
            error_code=INDEX_PROFILE_COMPONENT_FETCH_FAILED,
            error=(
                f"Fetched component id {exported_id!r} does not match the "
                f"requested id {component_id!r}"
            ),
            field="component_id",
            details={"component_id": component_id, "returned_id": exported_id},
        )
    if component_type not in PROFILE_INDEX_SUPPORTED_TYPES:
        return _error_envelope(
            error_code=PROFILE_INDEX_UNSUPPORTED_TYPE,
            error=(
                f"Component {component_id!r} is a {component_type!r} component, "
                "not a profile.json / profile.xml / profile.db"
            ),
            field="component_id",
            details={"component_id": component_id, "component_type": component_type},
        )

    # Pure parse into the canonical field index. Any structured failure stays a
    # read-only error envelope (never exposing raw XML).
    try:
        indexed = index_existing_profile_xml(raw_xml)
    except BuilderValidationError as exc:
        return _error_envelope(
            error_code=exc.error_code,
            error=str(exc),
            field=exc.field,
            hint=exc.hint,
            details=(getattr(exc, "details", None) or None),
        )

    response: Dict[str, Any] = {
        "_success": True,
        **_INDEX_FLAGS,
        # Provenance marker required by import_integration_draft (issue #48):
        # integration_import._analyze_schema only accepts a live field index whose
        # produced_by == "index_profile_component".
        "produced_by": "index_profile_component",
        "component_id": component_id,
        "profile_component_type": indexed["profile_component_type"],
        "field_index_by_path": indexed["field_index_by_path"],
        "mappable_paths": indexed["mappable_paths"],
        "raw_xml_exposed": bool(include_raw_xml),
    }
    if include_raw_xml:
        response["raw_xml"] = raw_xml
    return response
