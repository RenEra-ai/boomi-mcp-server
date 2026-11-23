"""#159 S3: raw process authoring is refused at every entry (cutover decision 1).

A flat ``type="process"`` component row may BIND an existing process (it writes
nothing); any row that would write a process body needs a canonical authoring
unit: ``authoring_request.intent`` with ``intent_kind="process_ir"`` units, or a
typed ``integration_spec.processes`` unit.

Step-4 decision 4 tightens the bind: a flat process row is admitted ONLY when it is
a body-less ``action="create"`` naming an EXPLICIT existing process id (top-level
``component_id``, or ``config.component_id`` on a ``reference_only`` row) that apply
REUSES rather than writes. A name-only row is refused even when the account holds a
match, so offline typed intake and live raw planning decide the same request the
same way, and no account state can move a request across the admission boundary.

The validator's only read of a refused row is a PRESENCE check (``process_kind``,
its former alias ``process_type``, ``flow_sequence``, or any other body field in
the process slot), used to choose a static pointer. It never resolves a builder,
lowers a body, echoes a selector or any other value, or consults the account, so
the refusal is value-free and account-independent and runs before any preview,
echo, lookup or write.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Mapping, Optional, Tuple

from ..errors import PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED

#: The only config keys a flat process row may carry: an explicit bind. The
#: ``component_name`` of a ``reference_only`` row is carried, never bound by: the
#: binding authority reads the id first, and a row without an id is refused.
BIND_KEYS = frozenset({"reference_only", "component_id", "component_name"})

#: Selector spellings, in the order the static pointer prefers them.
_SELECTORS = ("process_kind", "process_type", "flow_sequence")

UNIT_REQUIRED_MESSAGE = (
    "Raw process authoring is not accepted: a process body is authored only as a "
    "canonical ProcessIR authoring unit. Nothing was planned or written."
)
UNIT_REQUIRED_HINT = (
    "Author the process as authoring_request.intent with intent_kind='process_ir' "
    "(one unit per process: envelope + ProcessIR root), or as a typed unit in "
    "integration_spec.processes; see get_schema_template(schema_name="
    "'AuthoringRequestV1'). A flat type='process' row may only bind an existing "
    "process by its explicit id (action='create' with config.reference_only=true "
    "and config.component_id, or a top-level component_id under "
    "conflict_policy='reuse'); a name alone does not bind."
)


def _is_process_type(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    from ..categories.integration_builder import _normalize_component_type

    return _normalize_component_type(value) == "process"


def raw_component_lists(config: Mapping[str, Any]) -> Iterator[Tuple[str, Any]]:
    """``(pointer, components)`` for every component list ``_normalize_to_spec``
    reads, in the same envelope precedence."""
    if not isinstance(config, Mapping):
        return
    spec = config.get("integration_spec")
    if spec is not None:
        if isinstance(spec, Mapping):
            yield "integration_spec.components", spec.get("components")
        return
    source = config.get("source_description")
    if isinstance(source, Mapping):
        yield "source_description.components", source.get("components")
        return
    yield "components", config.get("components")


def raw_components_pointer(config: Mapping[str, Any]) -> str:
    """The pointer prefix of the one component list the raw route normalizes."""
    for pointer, _components in raw_component_lists(config):
        return pointer
    return "components"


def _body_field(config: Mapping[str, Any], base: str) -> Optional[str]:
    """The static pointer of a process slot's body, or ``None`` for a bind-shaped slot.

    Key presence only: a selector is preferred, then ``xml``, then the slot itself.
    """
    body = [key for key in config if key not in BIND_KEYS]
    if not body:
        return None
    for selector in _SELECTORS:
        if selector in config:
            return base + ".config." + selector
    if "xml" in config:
        return base + ".config.xml"
    return base + ".config"


def _bind_value_field(config: Mapping[str, Any], base: str) -> Optional[str]:
    """The static pointer of a bind key whose VALUE is not the scalar a bind reads.

    The bind keys are the only keys a flat process row may carry, and the plan
    echoes the row, so their values are held to their scalar shapes:
    ``reference_only`` a bool, ``component_id`` / ``component_name`` a string (or
    null). A container there could carry a secret-shaped key the echo would
    repeat; it is refused value-free instead of scanned.
    """
    reference_only = config.get("reference_only")
    if "reference_only" in config and not isinstance(reference_only, bool):
        return base + ".config.reference_only"
    for key in ("component_id", "component_name"):
        value = config.get(key)
        if key in config and value is not None and not isinstance(value, str):
            return base + ".config." + key
    return None


def row_refusal_field(row: Any, pointer: str) -> Optional[str]:
    """The static field pointer a RAW process row is refused at, or ``None`` when the
    row is bind-shaped. Runs before normalization, so it reads the raw dict; the
    explicit-id rule (:func:`flat_process_refusal_field`) runs on the normalized
    row, still before any account lookup."""
    from ..categories.integration_builder import raw_component_config, raw_component_type

    if not isinstance(row, Mapping):
        return None
    # Normalization's own alias precedence, asked — not copied (CDX-159-S3-r1-03).
    if not _is_process_type(raw_component_type(row)):
        return None
    config = raw_component_config(row)
    if config is None:
        config = {}
    if not isinstance(config, Mapping):
        return pointer + ".config"
    body = _body_field(config, pointer) or _bind_value_field(config, pointer)
    if body is not None:
        return body
    action = row.get("action")
    if isinstance(action, str) and action.strip().lower() == "update":
        # reference_only on an UPDATE does not turn it into a bind.
        return pointer + ".action"
    return None


def raw_process_refusal(config: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """The whole-request refusal for the first raw process row that carries a body,
    or ``None``. Reads only key presence and the row type."""
    for pointer, components in raw_component_lists(config):
        if not isinstance(components, list):
            continue
        for index, row in enumerate(components):
            field = row_refusal_field(row, f"{pointer}.{index}")
            if field is not None:
                return unit_required_envelope(field)
    return None


def unit_required_envelope(field: str) -> Dict[str, Any]:
    return {
        "_success": False,
        "error_code": PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED,
        "error": UNIT_REQUIRED_MESSAGE,
        "field": field,
        "hint": UNIT_REQUIRED_HINT,
        "boomi_mutation": False,
    }


def flat_process_refusal_field(comp: Any, base: str, conflict_policy: str) -> Optional[str]:
    """THE admission rule for one normalized flat component row (step-4 decision 4).

    ``None`` when the row is not a process, or when it is a body-less
    ``action="create"`` naming an explicit existing process id that apply REUSES
    (``apply_writes_component_config`` over the DECLARED binding is False — a
    ``reference_only`` row, or ``conflict_policy="reuse"``). Otherwise the static
    pointer it is refused at:

    * ``<base>.config...`` — the row carries a body (selector, xml, any other key),
      or a bind key whose value is not its scalar shape;
    * ``<base>.action`` — an update, or a create apply would WRITE;
    * ``<base>.component_id`` — no explicit id: a name never binds a process.

    Answered from the request alone (``declared_only``): no account is consulted, so
    the same row is admitted or refused identically on every route.
    """
    if getattr(comp, "type", None) != "process":
        return None
    config = comp.config if isinstance(getattr(comp, "config", None), dict) else {}
    body = _body_field(config, base) or _bind_value_field(config, base)
    if body is not None:
        return body
    if getattr(comp, "action", None) != "create":
        return base + ".action"
    from ..categories.integration_builder import (
        apply_writes_component_config,
        resolve_planner_binding,
    )

    if resolve_planner_binding(None, comp, declared_only=True).existing_id is None:
        return base + ".component_id"
    if apply_writes_component_config(comp, conflict_policy):
        return base + ".action"
    return None


def first_flat_process_refusal(components: Any, pointer: str, conflict_policy: str) -> Optional[str]:
    """The first refused row's pointer across ``components`` (in order), or ``None``."""
    for index, comp in enumerate(components or ()):
        field = flat_process_refusal_field(comp, f"{pointer}.{index}", conflict_policy)
        if field is not None:
            return field
    return None


# ---------------------------------------------------------------------------
# the neutral secret-shaped-key scanner
# ---------------------------------------------------------------------------

#: Substrings that mark a dict key as carrying a secret (case-insensitive). The
#: legacy process-flow builder was the only implementation of this scan; it moves
#: here so the serving path keeps the check without a builder dependency. Pinned
#: in both directions against ``ProcessFlowBuilder.FORBIDDEN_SECRET_FIELDS`` until
#: #160 deletes that class.
SECRET_KEY_SUBSTRINGS: Tuple[str, ...] = (
    "password",
    "passcode",
    "secret",
    "private_key",
    "api_key",
    "apikey",
    "api-key",
    "auth_token",
    "access_token",
    "client_secret",
    "token",
    "authorization",
    "bearer",
    "credentials",
)


def _key_matches_secret(key: Any) -> Optional[str]:
    if not isinstance(key, str):
        return None
    lowered = key.lower()
    for forbidden in SECRET_KEY_SUBSTRINGS:
        if forbidden in lowered:
            return forbidden
    return None


def _secret_rejection(path: str):
    from ..categories.components.builders.connector_builder import BuilderValidationError

    return BuilderValidationError(
        f"Plaintext secret-shaped field {path!r} is not allowed in "
        f"process config; reference connector secrets via a "
        f"connection_id / $ref:KEY token instead.",
        error_code="PLAINTEXT_SECRET_REJECTED",
        field=path,
        hint=(
            "Move credentials onto the connector-settings component "
            "and reference its connection_id from source/target."
        ),
    )


def scan_secret_shaped_fields(config: Any, _path_prefix: str = ""):
    """The first plaintext secret-shaped key at any depth, as a builder error.

    A matching key flags a non-empty string value or any dict / list container;
    scalars (``None`` / bool / int) carry no plaintext and are skipped, as is an
    empty string. Non-matching subtrees are recursed. Reports the key PATH only.
    """
    if isinstance(config, dict):
        for key, value in config.items():
            if _key_matches_secret(key) is not None:
                path = f"{_path_prefix}{key}" if _path_prefix else key
                if isinstance(value, str):
                    if value:
                        return _secret_rejection(path)
                elif isinstance(value, (dict, list)):
                    return _secret_rejection(path)
                continue
            nested = scan_secret_shaped_fields(value, _path_prefix=f"{_path_prefix}{key}.")
            if nested is not None:
                return nested
    elif isinstance(config, list):
        for i, item in enumerate(config):
            nested = scan_secret_shaped_fields(item, _path_prefix=f"{_path_prefix}[{i}].")
            if nested is not None:
                return nested
    return None


__all__ = [
    "BIND_KEYS",
    "SECRET_KEY_SUBSTRINGS",
    "UNIT_REQUIRED_HINT",
    "UNIT_REQUIRED_MESSAGE",
    "first_flat_process_refusal",
    "flat_process_refusal_field",
    "raw_components_pointer",
    "raw_process_refusal",
    "row_refusal_field",
    "scan_secret_shaped_fields",
    "unit_required_envelope",
]
