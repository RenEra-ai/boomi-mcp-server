"""The private materialization catalog and symbol projection (issue #145).

This module holds the one thing a recipe must never see: the real
``IntegrationComponentSpec`` objects, complete with the SQL, hosts, usernames,
credential references and script bodies the legacy archetype inputs legitimately
carry.

The compatibility adapter builds this catalog from its existing public
parameters and keeps it here. The recipe receives only slot NAMES. When the
recipe hands back a ``ComponentContributionV1``, the engine resolves the slot and
verifies that the catalog entry's key, type and mode equal the header the recipe
declared — so a recipe cannot end up attached to a component other than the one
it asked for, and cannot learn anything about the one it got.

``MaterializationCatalog`` is deliberately a plain object, not a pydantic model:
a pydantic model is serializable, and a serializable catalog is one
``model_dump()`` away from appearing in a diagnostic, a log line, or a canonical
contribution snapshot. It refuses pickling for the same reason.
"""

from __future__ import annotations

from typing import Callable, Dict, Mapping, Optional, Sequence, Tuple

from ..errors import RECIPE_CONTRIBUTION_INVALID
from ..models.integration_models import IntegrationComponentSpec
from .errors import RecipeError, recipe_error

_REF_PREFIX = "$ref:"

#: Materialization mode per legacy ``action`` + ``config.reference_only``.
#: Reuse is represented in the existing specs as ``action="create"`` with
#: ``config.reference_only=True`` — not a distinct action — so the mapping has to
#: read both fields rather than the action alone.
_CREATE = "create"
_UPDATE = "update"
_REUSE = "reuse_reference"


def component_materialization_mode(component: IntegrationComponentSpec) -> str:
    """The contribution-facing mode for an existing component spec."""
    config = component.config or {}
    if isinstance(config, dict) and config.get("reference_only") is True:
        return _REUSE
    if component.action == "update":
        return _UPDATE
    return _CREATE


class MaterializationCatalog:
    """Slot name -> real component spec. Never serialized, never handed out."""

    __slots__ = ("_entries",)

    def __init__(self, entries: Mapping[str, IntegrationComponentSpec]) -> None:
        self._entries: Dict[str, IntegrationComponentSpec] = dict(entries)

    def __repr__(self) -> str:  # noqa: D105
        return f"<MaterializationCatalog slots={len(self._entries)}>"

    def __getstate__(self):  # noqa: D105
        raise TypeError("MaterializationCatalog is not serializable")

    def __reduce__(self):  # noqa: D105
        raise TypeError("MaterializationCatalog is not serializable")

    def slots(self) -> Tuple[str, ...]:
        return tuple(sorted(self._entries))

    def resolve(
        self,
        slot: str,
        *,
        component_key: str,
        component_type: str,
        materialization_mode: str,
    ) -> IntegrationComponentSpec:
        """Resolve a slot and VERIFY it matches the contributed header.

        A missing slot and a header mismatch are both
        ``RECIPE_CONTRIBUTION_INVALID`` and both value-free: the diagnostic names
        the failure, never the component. Verification is what makes the opaque
        slot safe — without it the recipe's declared key/type would be a claim
        nobody checked, and the component plan would silently materialize
        something else.
        """
        component = self._entries.get(slot)
        if component is None:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                target="materializer_slot",
            )
        if (
            component.key != component_key
            or component.type != component_type
            or component_materialization_mode(component) != materialization_mode
        ):
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                target="materializer_slot_header",
            )
        return component


def placeholder_component_id(selector: str) -> str:
    """The deterministic placeholder id for an unresolved ``$ref:KEY``.

    Mirrors what the integration-builder plan preflight passes to
    ``emit_legacy_result``: at spec-emission time no Boomi component id exists
    yet, so both the recipe arm and the legacy arm must resolve refs the SAME
    way or a byte differential between them would only be measuring the
    resolver.
    """
    key = selector[len(_REF_PREFIX):] if selector.startswith(_REF_PREFIX) else selector
    return f"id-{key}"


def build_symbol_table(
    components: Sequence[IntegrationComponentSpec],
    *,
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
    resolver: Callable[[str], str] = placeholder_component_id,
):
    """Project components into the compiler's ``SymbolTableV1``.

    Every symbol's ``ref`` is the STABLE ``$ref:KEY`` — never an occurrence-scoped
    alias. That single choice is what lets a recipe's cache fan-out pass the
    strict lineage validator with no legacy exemption: the staging ``cache_put``
    and every consuming ``cache_get`` name one symbol, so the writer is visible
    to the reader. The legacy flow adapter mints a distinct alias per occurrence,
    which is exactly why it needs the ``flow_sequence`` policy.

    ``connector_metadata`` maps a component key to ``(connector_type,
    action_type)``. It rides on the OPERATION symbol only, mirroring the legacy
    adapters and the #136 codec: no connector-action component declares its own
    connection, so the family is a fact of the component plan the compiler
    receives, not one the IR authors.

    The operation->connection edge is the same kind of fact and is read from the
    same place: an operation component's ``config["connection_key"]`` names the
    connection component it binds to, and that becomes the operation symbol's
    ``connection_ref``. ``connector_resolution`` resolves the connection off
    THAT field and nothing else, so leaving it unset made every first-class
    ``connector_call`` fail reference resolution — and, because Try/Catch bodies
    admit ``connector_call``, made no Try/Catch document compilable at all. The
    ``source``/``target`` node kinds were unaffected because they carry their own
    ``connection_ref`` in the IR, which is why the shipped fixtures stayed green.

    Derived here rather than passed in, so both call sites — the authoring
    workflow and the recipe engine — get it from one place.
    """
    from ..compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1

    metadata = connector_metadata or {}
    symbols = []
    for component in components:
        connector_type, action_type = metadata.get(component.key, (None, None))
        ref = f"{_REF_PREFIX}{component.key}"
        connection_key = (component.config or {}).get("connection_key")
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=resolver(ref),
                component_type=component.type,
                connector_type=connector_type,
                action_type=action_type,
                connection_ref=(
                    f"{_REF_PREFIX}{connection_key}" if connection_key else None
                ),
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


__all__ = [
    "MaterializationCatalog",
    "build_symbol_table",
    "component_materialization_mode",
    "placeholder_component_id",
]
