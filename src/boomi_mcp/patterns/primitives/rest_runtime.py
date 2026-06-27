"""Issue #96 (M5.4a): typed REST runtime binding model.

The process-step half of the Companion two-part REST runtime binding pattern:
#72 (``rest_fetch``) declares the operation param/header/path **slots**; this
module models the **binding** that fills a declared slot with a runtime value and
validates each binding against those slots.

A :class:`RuntimeBinding` names a ``location`` (``path`` / ``query_parameter`` /
``request_header``), the declared ``slot`` it fills, and a discriminated ``source``
value (``static`` / ``ddp`` / ``dpp`` / ``profile_field``). v1 lowers **path**
bindings into the live-proven ``dynamic_path`` block that the process-flow builder
already emits (Set Properties DDP + connector-step "Path" dynamic operation
property — see ``.codex/plans/issue-96-live-captures.md`` / #100 G2). Query/header
bindings and ``ddp``/``dpp`` path sources are typed and validated but NOT emitted as
process XML until QA captures the exact REST Client dynamic operation property
keys/names — :func:`path_bindings_to_dynamic_path` raises
``PROCESS_RUNTIME_BINDING_UNVERIFIED`` rather than guessing XML.

Like the other primitive-layer modules this emits no XML and calls no Boomi API —
it returns validated JSON (the ``dynamic_path`` dict) consumed by the builder.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field

from ...categories.components.builders.connector_builder import BuilderValidationError
from ._helpers import _key_looks_secret, ref_key, value_looks_secret

# Primitive-layer error code for a malformed/contradictory runtime binding set.
RUNTIME_BINDING_INVALID = "REST_RUNTIME_BINDING_INVALID"
# Raised when a binding is well-formed but its XML emission is not yet live-proven
# (query/header process XML, or a ddp/dpp path source). Same string the builder
# uses so the gate is consistent across the primitive and builder layers.
RUNTIME_BINDING_UNVERIFIED = "PROCESS_RUNTIME_BINDING_UNVERIFIED"

# `{token}` placeholders in an operation path (e.g. /v1/items/{id}/notes/{noteId}).
_PATH_TOKEN_RE = re.compile(r"\{([^{}]+)\}")


# ---------------------------------------------------------------------------
# Declared slot (moved verbatim from rest_fetch so source + target share it).
# ---------------------------------------------------------------------------


class OperationSlot(BaseModel):
    """One declared param/header/path slot on the REST operation (#96 binds it)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    required: bool = True
    description: Optional[str] = None


# ---------------------------------------------------------------------------
# Value sources (discriminated on ``kind``; extra keys rejected so a secret-shaped
# or mis-typed source can never slip past the boundary).
# ---------------------------------------------------------------------------


class RuntimeStaticSource(BaseModel):
    """A constant value baked into the binding."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["static"]
    value: str


class RuntimeDdpSource(BaseModel):
    """A Dynamic Document Property value (per-document, set upstream)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["ddp"]
    property_name: str = Field(
        ..., description="Dynamic Document Property name (e.g. the dynamicdocument.<name> suffix)"
    )


class RuntimeDppSource(BaseModel):
    """A Dynamic Process Property value (per-execution)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["dpp"]
    property_name: str = Field(..., description="Dynamic Process Property name")


class RuntimeProfileFieldSource(BaseModel):
    """A mapped profile-leaf value (the live-proven path source — capture C2)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["profile_field"]
    profile_id: str = Field(
        ..., description="'$ref:KEY' to an in-spec profile or a literal profile UUID"
    )
    profile_type: Literal["profile.json", "profile.xml"]
    element_id: str = Field(..., description="Profile element id (the <profileelement> elementId)")
    element_name: str = Field(..., description="Profile element name (the <profileelement> elementName)")


RuntimeBindingSource = Annotated[
    Union[
        RuntimeStaticSource,
        RuntimeDdpSource,
        RuntimeDppSource,
        RuntimeProfileFieldSource,
    ],
    Field(discriminator="kind"),
]


class RuntimeBinding(BaseModel):
    """One runtime value bound into a declared REST operation slot."""

    model_config = ConfigDict(extra="forbid")

    location: Literal["path", "query_parameter", "request_header"]
    slot: str = Field(..., description="The declared slot name this binding fills")
    source: RuntimeBindingSource


# ---------------------------------------------------------------------------
# Cross-binding validation against the declared slots.
# ---------------------------------------------------------------------------


def _binding_error(message: str, **details: Any) -> BuilderValidationError:
    return BuilderValidationError(
        message,
        error_code=RUNTIME_BINDING_INVALID,
        field="runtime_bindings",
        hint=(
            "Each binding fills a declared slot for its location (#72 declares "
            "path_slots / query_parameter_slots / request_header_slots); a path "
            "binding's slot must also match a '{token}' in operation.path. Slot "
            "and property/static values must be non-blank and not secret-shaped."
        ),
        details=details or None,
    )


def _location_slot_names(slots: Optional[List[OperationSlot]]) -> set:
    return {slot.name for slot in (slots or [])}


def validate_runtime_bindings(
    bindings: Optional[List[RuntimeBinding]],
    *,
    path_slots: Optional[List[OperationSlot]],
    query_parameter_slots: Optional[List[OperationSlot]],
    request_header_slots: Optional[List[OperationSlot]],
    path_tokens: Optional[set] = None,
    path_bound_externally: bool = False,
) -> None:
    """Validate every binding against the declared slots; raise on the first defect.

    Enforces: unique ``(location, slot)``; non-blank, non-secret-shaped slot names;
    non-secret static/ddp/dpp values; each binding's slot is declared for its
    location (missing slot → error) and, for a path binding, also matches a
    ``{token}`` in the operation path (mismatched key → error); profile-field
    sources name a non-blank element and a resolvable ``$ref`` profile key.
    """
    if not bindings:
        return
    declared = {
        "path": _location_slot_names(path_slots),
        "query_parameter": _location_slot_names(query_parameter_slots),
        "request_header": _location_slot_names(request_header_slots),
    }
    seen: set = set()
    for binding in bindings:
        slot = binding.slot
        if not slot or not slot.strip():
            raise _binding_error(f"a {binding.location} runtime binding has a blank slot name")
        key = (binding.location, slot)
        if key in seen:
            raise _binding_error(
                f"duplicate runtime binding for {binding.location} slot {slot!r}",
                offending_location=binding.location,
                offending_slot=slot,
            )
        seen.add(key)
        if _key_looks_secret(slot):
            raise _binding_error(
                f"runtime binding slot name {slot!r} looks secret-shaped",
                offending_slot=slot,
            )
        if slot not in declared[binding.location]:
            raise _binding_error(
                f"runtime binding slot {slot!r} is not a declared "
                f"{binding.location} slot",
                offending_location=binding.location,
                offending_slot=slot,
            )
        if binding.location == "path" and path_tokens is not None and slot not in path_tokens:
            raise _binding_error(
                f"path runtime binding slot {slot!r} does not match a '{{token}}' "
                "in operation.path",
                offending_slot=slot,
            )
        _validate_source(binding)

    # Plan: "missing required slots". When the caller provides runtime_bindings,
    # every declared required slot must be filled — a binding set that omits a
    # required slot fails at plan time. A #72-style caller that forward-declares
    # slots WITHOUT any runtime_bindings is exempt (the slots are recorded as
    # metadata for a later binder); only the act of binding triggers completeness.
    # ``path_bound_externally`` (rest_send with operation.path_replacements) marks
    # the PATH as already satisfied by the #100 surface — declared path slots are
    # then not required to carry a runtime binding (the conflict rule even forbids
    # one), so they are skipped here. Query/header slots have no static alternative
    # (#72 rejects a slot that duplicates a static key), so they are always enforced.
    bound = {(b.location, b.slot) for b in bindings}
    for location, slots in (
        ("path", path_slots),
        ("query_parameter", query_parameter_slots),
        ("request_header", request_header_slots),
    ):
        if location == "path" and path_bound_externally:
            continue
        for slot in slots or []:
            if slot.required and (location, slot.name) not in bound:
                raise _binding_error(
                    f"required {location} slot {slot.name!r} has no runtime binding",
                    offending_location=location,
                    offending_slot=slot.name,
                )


def _validate_source(binding: RuntimeBinding) -> None:
    source = binding.source
    if isinstance(source, RuntimeStaticSource):
        if value_looks_secret(source.value):
            raise _binding_error(
                f"static value for {binding.location} slot {binding.slot!r} looks "
                "like secret material",
                offending_slot=binding.slot,
            )
    elif isinstance(source, (RuntimeDdpSource, RuntimeDppSource)):
        name = source.property_name
        if not name or not name.strip():
            raise _binding_error(
                f"{source.kind} source for {binding.location} slot "
                f"{binding.slot!r} needs a non-blank property_name",
                offending_slot=binding.slot,
            )
        if _key_looks_secret(name) or value_looks_secret(name):
            raise _binding_error(
                f"{source.kind} property_name {name!r} looks secret-shaped",
                offending_slot=binding.slot,
            )
    elif isinstance(source, RuntimeProfileFieldSource):
        for field_name in ("element_id", "element_name", "profile_id"):
            value = getattr(source, field_name)
            if not value or not value.strip():
                raise _binding_error(
                    f"profile_field source for {binding.location} slot "
                    f"{binding.slot!r} needs a non-blank {field_name}",
                    offending_slot=binding.slot,
                )
        stripped = source.profile_id.strip()
        if stripped.startswith("$ref:") and ref_key(stripped) is None:
            raise _binding_error(
                f"profile_field profile_id '$ref:' for slot {binding.slot!r} must "
                "name a non-empty key ('$ref:KEY')",
                offending_slot=binding.slot,
            )


# ---------------------------------------------------------------------------
# Path lowering into the live-proven ``dynamic_path`` block.
# ---------------------------------------------------------------------------


def _unverified_error(message: str, **details: Any) -> BuilderValidationError:
    return BuilderValidationError(
        message,
        error_code=RUNTIME_BINDING_UNVERIFIED,
        field="runtime_bindings",
        hint=(
            "v1 emits process XML only for path slots bound to static literals + "
            "at least one profile_field (the live-proven #100 mechanism). DDP/DPP "
            "path sources and all query/header process XML are pending a live REST "
            "Client fixture (QA live-verify) — they validate but are not emitted."
        ),
        details=details or None,
    )


def lower_path_bindings(
    bindings: Optional[List[RuntimeBinding]],
    *,
    path_template: str,
    ddp_name: str,
) -> Tuple[str, Any]:
    """Lower the PATH runtime bindings, dispatching on what they resolve to.

    Returns ``(mode, value)``:

    - ``("none", None)`` — no path bindings; the operation path is unchanged.
    - ``("static", resolved_path)`` — every path token is bound to a ``static``
      source, so the path is a CONSTANT. It is resolved into the operation path
      directly (no Set Properties / dynamic_path): an all-static path is not a
      "dynamic" path, matching the #100 invariant that a segment list with no
      profile segment is rejected — here it never reaches that validator because it
      is folded into the static operation path instead.
    - ``("dynamic", dynamic_path)`` — at least one ``profile_field`` source; the
      ``{ddp_name, request_profile_id, profile_type, segments}`` block the
      ``_emit_setproperties`` / ``_emit_connectoraction`` emitters consume.

    Raises ``PROCESS_RUNTIME_BINDING_UNVERIFIED`` for a ``ddp``/``dpp`` path source
    (pending a live REST Client fixture) or a profile-id mismatch across profile
    segments, and ``REST_RUNTIME_BINDING_INVALID`` for an ``operation.path`` token
    with no matching binding. All profile_field path segments must share one profile
    id+type (``_emit_setproperties`` emits a single ``profileId``/``profileType``).
    """
    path_bindings = [b for b in (bindings or []) if b.location == "path"]
    if not path_bindings:
        return ("none", None)

    by_slot = {b.slot: b for b in path_bindings}
    request_profile_id: Optional[str] = None
    profile_type: Optional[str] = None
    segments: List[Dict[str, Any]] = []
    resolved_parts: List[str] = []
    profile_segments = 0
    last = 0
    for match in _PATH_TOKEN_RE.finditer(path_template or ""):
        literal = (path_template or "")[last:match.start()]
        if literal:
            segments.append({"type": "static", "value": literal})
            resolved_parts.append(literal)
        token = match.group(1)
        binding = by_slot.get(token)
        if binding is None:
            raise _binding_error(
                f"operation.path token {token!r} has no matching path runtime binding",
                offending_slot=token,
            )
        source = binding.source
        if isinstance(source, RuntimeStaticSource):
            segments.append({"type": "static", "value": source.value})
            resolved_parts.append(source.value)
        elif isinstance(source, RuntimeProfileFieldSource):
            if request_profile_id is None:
                request_profile_id = source.profile_id
                profile_type = source.profile_type
            elif request_profile_id != source.profile_id or profile_type != source.profile_type:
                raise _unverified_error(
                    "all profile_field path segments must share one profile "
                    "(a single Set Properties profile is emitted)",
                    offending_slot=binding.slot,
                )
            segments.append(
                {
                    "type": "profile",
                    "element_id": source.element_id,
                    "element_name": source.element_name,
                }
            )
            resolved_parts.append("")  # placeholder — never used (dynamic path)
            profile_segments += 1
        else:  # ddp / dpp path source — not live-proven in v1
            raise _unverified_error(
                f"{source.kind} path source for slot {binding.slot!r} is not "
                "emitted in v1 (pending a live REST Client fixture)",
                offending_slot=binding.slot,
            )
        last = match.end()
    trailing = (path_template or "")[last:]
    if trailing:
        segments.append({"type": "static", "value": trailing})
        resolved_parts.append(trailing)

    if profile_segments == 0:
        # All path tokens are static → a constant path, folded into the operation
        # path (no dynamic Set Properties). Supports the static source kind without
        # emitting an all-static "dynamic" path (which the #100 builder rejects).
        return ("static", "".join(resolved_parts))
    return (
        "dynamic",
        {
            "ddp_name": ddp_name,
            "request_profile_id": request_profile_id,
            "profile_type": profile_type,
            "segments": segments,
        },
    )


def synth_path_replacements(
    bindings: Optional[List[RuntimeBinding]],
) -> List[Dict[str, str]]:
    """Synthesize the operation-level ``path_replacements`` marker for path bindings.

    The REST Client operation builder only permits a BLANK operation path (the path
    is supplied at the process step) when a usable ``path_replacements`` declaration
    is present (#100 G2). A #96 path runtime binding supplies the path at the process
    step too, so it reuses that exact marker: one ``{name, target_path}`` entry per
    path-bound token. The marker is build-only — it is NOT emitted into the operation
    XML (verified: the live REST operation carries `value=""`, no replacement
    elements) — and the actual per-document value comes from the lowered
    ``dynamic_path`` block, NOT from ``target_path``. Returns ``[]`` when there are no
    path bindings (a static path keeps its literal value).
    """
    return [
        {"name": b.slot, "target_path": b.slot}
        for b in (bindings or [])
        if b.location == "path"
    ]


def pending_runtime_bindings(
    bindings: Optional[List[RuntimeBinding]],
) -> List[Dict[str, Any]]:
    """Dump the query/header (and any deferred) bindings carried as metadata.

    Path bindings are lowered to ``dynamic_path`` and emitted; everything else is
    recorded under ``runtime_bindings_pending`` with an ``emission_status`` so the
    contract is explicit that the value is validated but not yet emitted.
    """
    pending: List[Dict[str, Any]] = []
    for binding in bindings or []:
        if binding.location == "path":
            continue
        pending.append(binding.model_dump(exclude_none=True))
    return pending
