"""The single immutable production registration tuple (issue #145 M12.10).

A literal tuple, not a package scan. ``patterns.registry`` scans its package at
import, which means the set of executable patterns depends on what happened to be
importable — and on import ORDER when two modules register the same name. For a
layer whose whole job is to make executable code provable, that is the wrong
default: a recipe exists here because a line below says so.

The four entry kinds are all represented deliberately, because the registry's
ability to TELL THEM APART is an acceptance criterion, and a kind with no entry
is a kind nobody tested:

* two executable recipes (the sync presets) and one for the fan-out
* one constraint-only entry
* one advisory entry — the doctrine pointer, with no executor to call
* three compatibility adapters, each naming the exact recipe it adapts to
"""

from __future__ import annotations

from typing import Tuple

from ..contracts import (
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeReferenceV1,
    RecipeRegistrationV1,
)
from .fanout import ComposeDbRestFanoutInputV1, emit_db_rest_fanout
from .sync import (
    SyncRecipeInputV1,
    emit_api_to_api_sync,
    emit_api_to_database_sync,
)

# Recipe ids. Namespaced by owner so a future third-party registration cannot
# collide with a built-in by accident.
RECIPE_DB_REST_FANOUT = "boomi.compose.db_rest_fanout"
RECIPE_API_TO_API_SYNC = "boomi.archetype.api_to_api_sync"
RECIPE_API_TO_DATABASE_SYNC = "boomi.archetype.api_to_database_sync"
RECIPE_CONSTRAINT_INBOUND_VALIDATE = "boomi.constraint.inbound_validate"
RECIPE_ADVISORY_INTEGRATION_DESIGN = "boomi.advisory.integration_design"
ADAPTER_COMPOSE_ARCHETYPES = "boomi.adapter.compose_archetypes"
ADAPTER_API_TO_API_SYNC = "boomi.adapter.api_to_api_sync"
ADAPTER_API_TO_DATABASE_SYNC = "boomi.adapter.api_to_database_sync"

_FANOUT_VERSION = "1.0.0"
_SYNC_VERSION = "0.1.0"
_CONSTRAINT_VERSION = "1.0.0"
_ADVISORY_VERSION = "1.0.0"
_ADAPTER_VERSION = "1.0.0"

# Merge rules must be canonically sorted — the contract enforces it, so the
# registry revision cannot change just because someone reordered a literal.
_SYNC_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("dedupe_identical_constraint",),
)

_FANOUT_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("append_root_terminal_leg", "dedupe_identical_constraint"),
)

_CONSTRAINT_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("dedupe_identical_constraint",),
)

_TARGET_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter",
    subject="connectoraction_target",
    required_state="supported",
)
_SOURCE_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter",
    subject="connectoraction_source",
    required_state="supported",
)
_MAP_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="map", required_state="supported"
)
_BRANCH_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="branch", required_state="supported"
)
_CACHE_LOAD_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="doccacheload", required_state="supported"
)
_CACHE_RETRIEVE_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="doccacheretrieve", required_state="supported"
)


def _constraint_only_executor(inp):  # pragma: no cover - exercised via the registry
    """The constraint-only representative.

    Emits requirements and nothing else — a registry entry whose kind mechanically
    prevents it from contributing a patch or a component. It exists so the
    four-way entry-kind distinction has a tested member for every kind, not only
    for the two that materialize things.
    """
    from ...models.recipe_contributions import (
        ConstraintRequirementV1,
        RequireComponentV1,
    )

    return tuple(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version="1",
            requirement_id=f"req.inbound.{item.contribution_id}",
            requirement=RequireComponentV1(
                kind="component",
                component_key=item.component_key,
                component_type=item.component_type,
            ),
        )
        for item in inp.component_slots
    )


class InboundValidateInputV1(SyncRecipeInputV1):
    """Reuses the sync projection: this entry validates the SAME safe shape."""


PRODUCTION_REGISTRATIONS: Tuple[RecipeRegistrationV1, ...] = (
    RecipeRegistrationV1(
        recipe_id=RECIPE_DB_REST_FANOUT,
        recipe_version=_FANOUT_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=ComposeDbRestFanoutInputV1,
        executor=emit_db_rest_fanout,
        output_types=(
            "component_contribution",
            "constraint_requirement",
            "process_ir_patch",
        ),
        capability_requirements=(
            _SOURCE_EMITTER,
            _TARGET_EMITTER,
            _MAP_EMITTER,
            _BRANCH_EMITTER,
            _CACHE_LOAD_EMITTER,
            _CACHE_RETRIEVE_EMITTER,
        ),
        conflict_policy=_FANOUT_CONFLICT_POLICY,
    ),
    RecipeRegistrationV1(
        recipe_id=RECIPE_API_TO_API_SYNC,
        recipe_version=_SYNC_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=SyncRecipeInputV1,
        executor=emit_api_to_api_sync,
        output_types=(
            "component_contribution",
            "constraint_requirement",
            "process_ir_patch",
        ),
        capability_requirements=(_SOURCE_EMITTER, _TARGET_EMITTER, _MAP_EMITTER),
        conflict_policy=_SYNC_CONFLICT_POLICY,
    ),
    RecipeRegistrationV1(
        recipe_id=RECIPE_API_TO_DATABASE_SYNC,
        recipe_version=_SYNC_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=SyncRecipeInputV1,
        executor=emit_api_to_database_sync,
        output_types=(
            "component_contribution",
            "constraint_requirement",
            "process_ir_patch",
        ),
        capability_requirements=(_SOURCE_EMITTER, _TARGET_EMITTER, _MAP_EMITTER),
        conflict_policy=_SYNC_CONFLICT_POLICY,
    ),
    RecipeRegistrationV1(
        recipe_id=RECIPE_CONSTRAINT_INBOUND_VALIDATE,
        recipe_version=_CONSTRAINT_VERSION,
        entry_kind="constraint_only",
        is_default=True,
        input_model=InboundValidateInputV1,
        executor=_constraint_only_executor,
        output_types=("constraint_requirement",),
        conflict_policy=_CONSTRAINT_CONFLICT_POLICY,
    ),
    # ADVISORY. No executor field is populated, and registry construction rejects
    # one — this is the structural reason doctrine can never become executable.
    RecipeRegistrationV1(
        recipe_id=RECIPE_ADVISORY_INTEGRATION_DESIGN,
        recipe_version=_ADVISORY_VERSION,
        entry_kind="advisory",
        is_default=True,
    ),
    RecipeRegistrationV1(
        recipe_id=ADAPTER_COMPOSE_ARCHETYPES,
        recipe_version=_ADAPTER_VERSION,
        entry_kind="compatibility_adapter",
        is_default=True,
        adapter_target=RecipeReferenceV1(
            recipe_id=RECIPE_DB_REST_FANOUT, recipe_version=_FANOUT_VERSION
        ),
    ),
    RecipeRegistrationV1(
        recipe_id=ADAPTER_API_TO_API_SYNC,
        recipe_version=_ADAPTER_VERSION,
        entry_kind="compatibility_adapter",
        is_default=True,
        adapter_target=RecipeReferenceV1(
            recipe_id=RECIPE_API_TO_API_SYNC, recipe_version=_SYNC_VERSION
        ),
    ),
    RecipeRegistrationV1(
        recipe_id=ADAPTER_API_TO_DATABASE_SYNC,
        recipe_version=_ADAPTER_VERSION,
        entry_kind="compatibility_adapter",
        is_default=True,
        adapter_target=RecipeReferenceV1(
            recipe_id=RECIPE_API_TO_DATABASE_SYNC, recipe_version=_SYNC_VERSION
        ),
    ),
)

__all__ = [
    "ADAPTER_API_TO_API_SYNC",
    "ADAPTER_API_TO_DATABASE_SYNC",
    "ADAPTER_COMPOSE_ARCHETYPES",
    "PRODUCTION_REGISTRATIONS",
    "RECIPE_ADVISORY_INTEGRATION_DESIGN",
    "RECIPE_API_TO_API_SYNC",
    "RECIPE_API_TO_DATABASE_SYNC",
    "RECIPE_CONSTRAINT_INBOUND_VALIDATE",
    "RECIPE_DB_REST_FANOUT",
]
