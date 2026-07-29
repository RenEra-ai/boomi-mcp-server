"""The built-in DB -> transform -> REST fan-out recipe (issue #145 M12.10).

The native form of what ``compose_archetypes`` builds: one database source, one
shared transform, and 2-25 REST targets behind a terminal Branch, with an
optional Document Cache handoff.

**The cache reference is the whole point of this recipe existing natively.** The
legacy ``flow_sequence`` adapter mints an occurrence-scoped alias per reference,
so the staging ``cache_put`` and each consuming ``cache_get`` end up naming
DIFFERENT symbols — the strict lineage validator then cannot see the writer from
the reader, and the dialect needs a registered ``STANDALONE_CACHE_READ``
exemption to compile at all. This recipe uses ONE stable ``$ref:KEY`` across the
put and every get, so the same strict validator sees the sequential writer and
passes with ``validation_policy=None``. That asymmetry is measured, not assumed:
``tests/patterns/test_recipe_preset_parity.py`` asserts the legacy arm still
fails strictly while this one passes.
"""

from __future__ import annotations

from typing import Optional, Tuple

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ComponentContributionV1,
    ConstraintRequirementV1,
    ProcessIRPatchV1,
    RecipeComponentKey,
    RecipeSemanticId,
    RequireCapabilityV1,
    RequireComponentV1,
    RequireProcessV1,
    SetProcessRootV1,
)
from ..contracts import RecipeInputBase
from .sync import SyncComponentSlotV1

_VERSION = "1"

#: Branch cardinality is 2-25 in ProcessIR. A cache handoff consumes one leg for
#: the target-less staging put, so a composition with any cache target can carry
#: at most 24 REST targets. Both bounds are re-enforced by ``BranchNodeV1``; they
#: are stated here too so the failure names the INPUT rather than surfacing as an
#: opaque cardinality error from deep inside the compiler.
_MAX_TARGETS = 25
_MAX_CACHE_TARGETS = 24


class FanoutTargetV1(RecipeInputBase):
    """One REST target leg, in caller order."""

    target_id: RecipeSemanticId
    connection_ref: str = Field(min_length=1)
    operation_ref: str = Field(min_length=1)
    handoff: str = Field(pattern=r"^(document_stream|document_cache)$")
    label: Optional[str] = None


class ComposeDbRestFanoutInputV1(RecipeInputBase):
    """The SAFE projection of a composition's parameters."""

    version: str = Field(pattern=r"^1$")
    process_key: RecipeComponentKey
    source_connection_ref: str = Field(min_length=1)
    source_operation_ref: str = Field(min_length=1)
    map_ref: str = Field(min_length=1)
    map_label: Optional[str] = None
    branch_label: Optional[str] = None
    cache_put_label: Optional[str] = None
    cache_get_label: Optional[str] = None
    cache_ref: Optional[str] = None
    targets: Tuple[FanoutTargetV1, ...] = Field(min_length=2, max_length=_MAX_TARGETS)
    component_slots: Tuple[SyncComponentSlotV1, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _cache_rules(self) -> "ComposeDbRestFanoutInputV1":
        cached = [t for t in self.targets if t.handoff == "document_cache"]
        if cached and self.cache_ref is None:
            raise PydanticCustomError(
                "fanout_cache_ref_required",
                "a document_cache handoff requires cache_ref",
            )
        if not cached and self.cache_ref is not None:
            raise PydanticCustomError(
                "fanout_cache_ref_unused",
                "cache_ref is only valid when a target uses a document_cache handoff",
            )
        if cached and len(self.targets) > _MAX_CACHE_TARGETS:
            raise PydanticCustomError(
                "fanout_cache_target_limit",
                "a cache handoff consumes one Branch leg, so at most 24 targets",
            )
        if len({t.target_id for t in self.targets}) != len(self.targets):
            raise PydanticCustomError(
                "fanout_duplicate_target_id", "target_id must be unique"
            )
        return self


def _root(inp: ComposeDbRestFanoutInputV1) -> ProcessIRV1:
    """source -> map_ref -> terminal Branch, legs in caller target order.

    The staging leg is inserted immediately before the FIRST cache consumer, not
    at position 0: Branch legs run sequentially, so a staging put anywhere before
    the first read satisfies write-before-read, and putting it exactly there is
    what keeps a mixed-mode composition's leg numbering identical to the legacy
    arm's.
    """
    legs: list = []
    staged = False
    for target in inp.targets:
        if target.handoff == "document_cache":
            if not staged:
                legs.append(
                    {
                        "steps": [],
                        "terminal": {
                            "kind": "cache_put",
                            "cache_ref": inp.cache_ref,
                            "label": inp.cache_put_label,
                        },
                    }
                )
                staged = True
            legs.append(
                {
                    "steps": [
                        {
                            "kind": "cache_get",
                            "cache_ref": inp.cache_ref,
                            "label": inp.cache_get_label,
                        }
                    ],
                    "terminal": {
                        "kind": "target",
                        "connection_ref": target.connection_ref,
                        "operation_ref": target.operation_ref,
                        "label": target.label,
                    },
                }
            )
        else:
            legs.append(
                {
                    "steps": [],
                    "terminal": {
                        "kind": "target",
                        "connection_ref": target.connection_ref,
                        "operation_ref": target.operation_ref,
                        "label": target.label,
                    },
                }
            )

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
                        "label": None,
                    },
                    {
                        "kind": "map_ref",
                        "map_ref": inp.map_ref,
                        "label": inp.map_label,
                    },
                    {"kind": "branch", "label": inp.branch_label, "legs": legs},
                ],
            },
        }
    )


def emit_db_rest_fanout(inp: ComposeDbRestFanoutInputV1) -> Tuple[object, ...]:
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
            requirement_id="req.capability.branch",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="branch",
                required_state="supported",
            ),
        )
    )
    if inp.cache_ref is not None:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id="req.capability.cache",
                requirement=RequireCapabilityV1(
                    kind="capability",
                    authority="process_emitter",
                    subject="doccacheload",
                    required_state="supported",
                ),
            )
        )
    return tuple(contributions)


__all__ = [
    "ComposeDbRestFanoutInputV1",
    "FanoutTargetV1",
    "emit_db_rest_fanout",
]
