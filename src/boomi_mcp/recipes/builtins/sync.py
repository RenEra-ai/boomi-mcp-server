"""Built-in sync-pipeline recipes: API->API and API->DB (issue #145 M12.10).

Both presets are thin ``sync_pipeline`` presets — fetch, map, send — so their
native ProcessIR root is the same four-step sequence, and only the target's
connector family differs. They are registered as TWO recipes rather than one
parameterized recipe. Their declared capability requirements are IDENTICAL today
— both are source/target/map emitters — so the reason is not a present
difference but a structural one: two registrations give each preset its own
descriptor, its own provenance hash and its own place to diverge when the
Database send eventually needs a requirement the REST send does not. One
registration would have to declare the union from the day they diverged, and an
API->API run would then pass a preflight for an emitter it never touches.

**Nothing in this module can reach a credential.** The input model carries
component references, closed enumerations, optional labels, and opaque slot
names. The connection settings, SQL, headers and profile bodies the archetype
legitimately accepts stay in the compatibility adapter's private catalog and are
never projected here.
"""

from __future__ import annotations

from typing import Optional, Tuple

from pydantic import Field

from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ComponentContributionV1,
    ConstraintRequirementV1,
    ProcessIRPatchV1,
    RecipeComponentKey,
    RecipeComponentType,
    RecipeSemanticId,
    RequireCapabilityV1,
    RequireComponentV1,
    RequireProcessV1,
    SetProcessRootV1,
)
from ..contracts import RecipeInputBase

_VERSION = "1"


class SyncComponentSlotV1(RecipeInputBase):
    """The header of one component the recipe will contribute, plus its slot."""

    contribution_id: RecipeSemanticId
    component_key: RecipeComponentKey
    component_type: RecipeComponentType
    materialization_mode: str = Field(pattern=r"^(create|update|reuse_reference)$")
    materializer_slot: RecipeSemanticId


class SyncRecipeInputV1(RecipeInputBase):
    """The SAFE projection of a sync preset's parameters."""

    version: str = Field(pattern=r"^1$")
    process_key: RecipeComponentKey
    source_connection_ref: str = Field(min_length=1)
    source_operation_ref: str = Field(min_length=1)
    map_ref: str = Field(min_length=1)
    target_connection_ref: str = Field(min_length=1)
    target_operation_ref: str = Field(min_length=1)
    source_label: Optional[str] = None
    map_label: Optional[str] = None
    target_label: Optional[str] = None
    component_slots: Tuple[SyncComponentSlotV1, ...] = Field(min_length=1)


def _root(inp: SyncRecipeInputV1) -> ProcessIRV1:
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": inp.source_connection_ref,
                        "operation_ref": inp.source_operation_ref,
                        "label": inp.source_label,
                    },
                    {
                        "kind": "map_ref",
                        "map_ref": inp.map_ref,
                        "label": inp.map_label,
                    },
                    {
                        "kind": "target",
                        "connection_ref": inp.target_connection_ref,
                        "operation_ref": inp.target_operation_ref,
                        "label": inp.target_label,
                    },
                    {"kind": "stop"},
                ],
            },
        }
    )


def _contributions(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """Ordered components, then the root patch, then the requirements.

    Component order IS the preset's existing ``spec.components`` order — the
    caller projects it that way — and nothing here re-sorts it. That order is
    what the parity fixtures pin.
    """
    contributions: list = [
        ComponentContributionV1(
            contribution_kind="component_contribution",
            version=_VERSION,
            contribution_id=slot.contribution_id,
            component_key=slot.component_key,
            component_type=slot.component_type,
            materialization_mode=slot.materialization_mode,
            materializer_slot=slot.materializer_slot,
        )
        for slot in inp.component_slots
    ]
    contributions.append(
        ProcessIRPatchV1(
            contribution_kind="process_ir_patch",
            version=_VERSION,
            process_key=inp.process_key,
            operations=(
                SetProcessRootV1(
                    operation_id="op.root",
                    op="set_process_root",
                    slot="root",
                    root=_root(inp),
                ),
            ),
        )
    )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        )
    )
    for slot in inp.component_slots:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"req.component.{slot.contribution_id}",
                requirement=RequireComponentV1(
                    kind="component",
                    component_key=slot.component_key,
                    component_type=slot.component_type,
                ),
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.capability.target",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="connectoraction_target",
                required_state="supported",
            ),
        )
    )
    return tuple(contributions)


def emit_api_to_api_sync(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """REST fetch -> map -> REST send."""
    return _contributions(inp)


def emit_api_to_database_sync(inp: SyncRecipeInputV1) -> Tuple[object, ...]:
    """REST fetch -> map -> Database send."""
    return _contributions(inp)


__all__ = [
    "SyncComponentSlotV1",
    "SyncRecipeInputV1",
    "emit_api_to_api_sync",
    "emit_api_to_database_sync",
]
