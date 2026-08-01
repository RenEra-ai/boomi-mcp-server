"""The compatibility adapter between legacy patterns and typed recipes (#145).

One place, three jobs:

1. **Materialize legacy input.** The archetype/composition code already ran and
   produced real ``IntegrationComponentSpec`` objects carrying whatever their
   public parameters legitimately contain — SQL, hosts, usernames, credential
   references, script bodies. Those go into a private
   :class:`~boomi_mcp.recipes.materialization.MaterializationCatalog`.

2. **Project a SAFE recipe input.** Component references, closed enumerations,
   optional labels, opaque slot names. Nothing else crosses.

3. **Run the recipe and verify parity.** The engine resolves slots back to the
   catalog entries and compiles the process through the canonical chain. The
   reassembled component list must be the SAME OBJECTS in the SAME ORDER as the
   legacy arm produced — asserted by identity, so an accidental rebuild that
   merely compares equal still fails.

**Connector metadata is derived by the existing legacy lowering, not re-derived
here.** ``adapt_sync_pipeline_config`` / ``adapt_flow_sequence`` already own the
rule that a connector family rides on the OPERATION symbol; re-implementing it
would create the second semantic authority ADR-001 exists to remove, and any
drift between the two would show up as a byte difference nobody could attribute.
"""

from __future__ import annotations

import hashlib

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..models.integration_models import IntegrationComponentSpec
from ..recipes import (
    MaterializationCatalog,
    RecipeError,
    RecipeRequestV1,
    RecipeRunResultV1,
    component_materialization_mode,
    placeholder_component_id,
    run_recipes,
)

_REF_PREFIX = "$ref:"


#: ``RecipeSemanticId``'s bound. A slot longer than this is not a validation
#: finding a caller can act on — it is our projection producing an invalid id.
_MAX_SLOT_LENGTH = 128

#: ``RecipeSemanticId``'s character class, spelled out. Not ``str.isalnum()``,
#: which admits every Unicode letter and digit.
_SLOT_ALPHABET = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._-")


def _slot_for(key: str) -> str:
    """The catalog slot name for a component key. ALWAYS a valid RecipeSemanticId.

    Lower-cased and punctuation-normalized because ``RecipeSemanticId`` is a
    closed lower-case pattern, while component keys are free-form. The slot is
    OPAQUE to the recipe either way — it names a catalog entry and carries no
    meaning — so normalization loses nothing, and the engine re-verifies the real
    key against the contributed header regardless.

    Over-long keys are folded to a digest suffix rather than truncated. Live QA
    found a 101-character part key producing a 129-character slot, which failed
    the input model and surfaced to the caller as ``RECIPE_INPUT_INVALID`` —
    telling them their input carried "credentials, SQL, raw XML" when it carried
    a long name (issue #145). Truncation alone would reintroduce the collision
    ``build_catalog`` now rejects, so the tail carries a hash of the full key:
    distinct keys stay distinct, and the result always fits.
    """
    # ASCII-only. ``str.isalnum()`` is True for 'é' and 'Ω', which are NOT in
    # ``RecipeSemanticId``'s ``[a-z0-9_.-]`` class — so the docstring's "ALWAYS
    # valid" promise failed for any non-ASCII key (issue #145, live QA).
    normalized = "".join(
        ch if (ch in _SLOT_ALPHABET) else "_" for ch in key.lower()
    )
    if not normalized or normalized[0] not in "abcdefghijklmnopqrstuvwxyz":
        normalized = f"s.{normalized}"
    slot = f"slot.{normalized}"
    if len(slot) > _MAX_SLOT_LENGTH:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        head = slot[: _MAX_SLOT_LENGTH - len(digest) - 1]
        slot = f"{head}-{digest}"
    return slot


def build_catalog(
    components: Sequence[IntegrationComponentSpec],
) -> Tuple[MaterializationCatalog, List[Dict[str, Any]]]:
    """The private catalog plus the SAFE slot headers, in component order.

    Slot names are derived by lossy normalization, so two distinct component keys
    CAN collide (``"Order Map"`` and ``"order_map"`` both fold to
    ``slot.order_map``). Live QA found the collision silently overwriting a
    catalog entry, leaving two contributed headers pointing at one component.
    Detected and raised here rather than deduplicated: a collision means the
    projection lost a component, and quietly materializing the survivor is how a
    build succeeds having emitted the wrong thing.
    """
    entries: Dict[str, IntegrationComponentSpec] = {}
    slots: List[Dict[str, Any]] = []
    for index, component in enumerate(components):
        slot = _slot_for(component.key)
        if slot in entries:
            raise RuntimeError(
                "recipe slot projection collided: two component keys normalize "
                f"to the same slot {slot!r}"
            )
        entries[slot] = component
        slots.append(
            {
                "contribution_id": f"c.{index}",
                "component_key": component.key,
                "component_type": component.type,
                "materialization_mode": component_materialization_mode(component),
                "materializer_slot": slot,
            }
        )
    return MaterializationCatalog(entries), slots


def connector_metadata_from_requirements(
    requirements: Sequence[Any],
) -> Mapping[str, Tuple[Optional[str], Optional[str]]]:
    """``component key -> (connector_type, action_type)`` from a legacy adapter.

    Reads the legacy adapter's own symbol requirements rather than the config,
    so the family/action attribution is exactly the one the legacy arm uses.
    """
    metadata: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for requirement in requirements:
        if not (requirement.connector_type or requirement.action_type):
            continue
        selector = requirement.legacy_selector
        key = selector[len(_REF_PREFIX):] if selector.startswith(_REF_PREFIX) else selector
        metadata[key] = (requirement.connector_type, requirement.action_type)
    return metadata


def run_and_verify(
    *,
    recipe_id: str,
    invocation_id: str,
    raw_input: Mapping[str, Any],
    components: Sequence[IntegrationComponentSpec],
    catalog: MaterializationCatalog,
    connector_metadata: Mapping[str, Tuple[Optional[str], Optional[str]]],
    recipe_version: Optional[str] = None,
) -> RecipeRunResultV1:
    """Run the recipe and prove it reassembled exactly the legacy components.

    Identity comparison, not equality: two ``IntegrationComponentSpec`` objects
    that happen to compare equal would let a silent rebuild pass, and a rebuild
    is precisely the failure mode a contribution path can introduce.
    """
    result = run_recipes(
        [
            RecipeRequestV1(
                recipe_id=recipe_id,
                invocation_id=invocation_id,
                raw_input=raw_input,
                # The PIN, carried all the way to resolution. Without this the
                # tool accepted a version and then ran the default — a guarantee
                # in the signature that the execution did not keep (issue #145,
                # Codex review).
                recipe_version=recipe_version,
            )
        ],
        catalog=catalog,
        connector_metadata=connector_metadata,
        resolver=placeholder_component_id,
    )
    if len(result.components) != len(components) or any(
        a is not b for a, b in zip(result.components, components)
    ):
        # A recipe-layer defect, never a caller's: the adapter built both the
        # catalog and the slot list, so a mismatch means our projection lost or
        # reordered something.
        raise RuntimeError(
            "recipe contribution path did not reassemble the legacy component list"
        )
    return result


def declared_target_version(adapter_id: str) -> str:
    """The EXACT version a compatibility adapter declares for its target.

    A surface with no ``recipe_version`` parameter still has one right answer:
    the version its adapter names. Passing ``None`` instead runs whatever is
    default, so the moment the two differ the response's
    ``recipe_provenance.executable`` — which reads the declaration — describes a
    version that did not run (issue #145, Codex review).

    Returns ``str``, never ``None``: ``RecipeReferenceV1.recipe_version`` is a
    non-optional ``SemVerString``, and an entry with no target raises rather than
    returning a value that would silently mean "let the engine decide".
    """
    from ..recipes import production_registry

    adapter = production_registry().resolve(adapter_id)
    if adapter.adapter_target is None:
        # Fail CLOSED. Returning ``None`` would silently reinstate "let the engine
        # pick the default" — the exact split this function exists to remove — and
        # only ``compatibility_adapter`` entries carry a target, which this
        # function never checked (issue #145, live QA note).
        raise RuntimeError(
            f"{adapter_id!r} has entry kind {adapter.entry_kind!r} and declares "
            "no adapter target, so it has no version to run"
        )
    return adapter.adapter_target.recipe_version


_SYNC_ROLE_TO_FIELD = {
    "connectoraction_source.connection": "source_connection_ref",
    "connectoraction_source.operation": "source_operation_ref",
    "map": "map_ref",
    "connectoraction_target.connection": "target_connection_ref",
    "connectoraction_target.operation": "target_operation_ref",
}


def run_sync_preset_recipe(
    *,
    recipe_id: str,
    components: Sequence[IntegrationComponentSpec],
    process: IntegrationComponentSpec,
    recipe_version: Optional[str] = None,
) -> RecipeRunResultV1:
    """Route a ``sync_pipeline`` preset through the typed contribution path.

    The five reference slots are read from the LEGACY ADAPTER's own symbol
    requirements, by role. That is deliberate: ``SyncPipelineBuilder.lower_config``
    is the single source of truth for this dialect's stage semantics, and reading
    the stage list directly here would fork it. A role appearing zero or twice
    means the dialect changed shape under us, which is a build defect rather than
    a caller error.
    """
    from ..compiler.process_ir.legacy_adapters.sync_pipeline import (
        adapt_sync_pipeline_config,
    )

    legacy = adapt_sync_pipeline_config(process.config)
    by_role: Dict[str, List[str]] = {}
    for requirement in legacy.symbol_requirements:
        by_role.setdefault(requirement.role, []).append(requirement.legacy_selector)

    projected: Dict[str, Any] = {}
    for role, field in _SYNC_ROLE_TO_FIELD.items():
        selectors = by_role.get(role, [])
        if len(selectors) != 1:
            raise RuntimeError(
                f"sync_pipeline lowering produced {len(selectors)} symbols for "
                f"role {role!r}; the recipe projection expects exactly one"
            )
        projected[field] = selectors[0]

    catalog, slots = build_catalog(components)
    raw_input = {
        "version": "1",
        "process_key": process.key,
        "component_slots": slots,
        **projected,
    }
    # Labels come from the lowered IR so the native root and the legacy root
    # carry identical label bytes; the sync dialect emits none today, and reading
    # them rather than hard-coding ``None`` means it stays true if it ever does.
    steps = legacy.process_ir.body.steps
    raw_input["source_label"] = getattr(steps[0], "label", None)
    for step in steps:
        if step.kind == "map_ref":
            raw_input["map_label"] = step.label
        elif step.kind == "target":
            raw_input["target_label"] = step.label

    return run_and_verify(
        recipe_id=recipe_id,
        invocation_id=f"preset:{process.key}",
        raw_input=raw_input,
        components=components,
        catalog=catalog,
        connector_metadata=connector_metadata_from_requirements(
            legacy.symbol_requirements
        ),
        recipe_version=recipe_version,
    )


def run_fanout_recipe(
    *,
    recipe_id: str,
    components: Sequence[IntegrationComponentSpec],
    process: IntegrationComponentSpec,
    recipe_version: Optional[str] = None,
) -> RecipeRunResultV1:
    """Route a composed DB -> transform -> REST fan-out through the recipe path.

    Projected from the composed ``flow_sequence`` config rather than by role:
    the fan-out repeats ``connectoraction_target.*`` once per leg, so roles are
    no longer a unique index and leg ORDER — which is semantic — would be lost.
    """
    from ..compiler.process_ir.legacy_adapters.flow_sequence import adapt_flow_sequence

    legacy = adapt_flow_sequence(process.config)
    config = process.config
    source = config["source"]

    map_ref = None
    map_label = None
    branch_label = None
    cache_ref = None
    cache_put_label = None
    cache_get_label = None
    targets: List[Dict[str, Any]] = []

    for node in config.get("flow_sequence", []):
        if node.get("kind") == "map_ref":
            map_ref = node["map_ref"]
            map_label = node.get("label")
        elif node.get("kind") == "branch":
            branch_label = node.get("label")
            for index, leg in enumerate(node.get("legs", [])):
                steps = leg.get("steps", []) or []
                staging = [s for s in steps if s.get("kind") == "cache_put"]
                if staging:
                    # The target-less staging leg. It is re-derived by the recipe
                    # from the per-target handoff modes, so it contributes its
                    # cache reference and label here and nothing else.
                    cache_ref = staging[0]["document_cache_id"]
                    cache_put_label = staging[0].get("label")
                    continue
                reads = [s for s in steps if s.get("kind") == "cache_get"]
                handoff = "document_stream"
                if reads:
                    handoff = "document_cache"
                    cache_ref = reads[0]["document_cache_id"]
                    cache_get_label = reads[0].get("label")
                target = leg["target"]
                targets.append(
                    {
                        "target_id": f"t.{index}",
                        "connection_ref": target["connection_id"],
                        "operation_ref": target["operation_id"],
                        "handoff": handoff,
                        "label": None,
                    }
                )

    catalog, slots = build_catalog(components)
    raw_input: Dict[str, Any] = {
        "version": "1",
        "process_key": process.key,
        "source_connection_ref": source["connection_id"],
        "source_operation_ref": source["operation_id"],
        "map_ref": map_ref,
        "map_label": map_label,
        "branch_label": branch_label,
        "cache_put_label": cache_put_label,
        "cache_get_label": cache_get_label,
        "cache_ref": cache_ref,
        "targets": targets,
        "component_slots": slots,
    }

    return run_and_verify(
        recipe_id=recipe_id,
        invocation_id=f"compose:{process.key}",
        raw_input=raw_input,
        components=components,
        catalog=catalog,
        connector_metadata=connector_metadata_from_requirements(
            legacy.symbol_requirements
        ),
        recipe_version=recipe_version,
    )


__all__ = [
    "build_catalog",
    "connector_metadata_from_requirements",
    "run_and_verify",
    "run_fanout_recipe",
    "run_sync_preset_recipe",
]
