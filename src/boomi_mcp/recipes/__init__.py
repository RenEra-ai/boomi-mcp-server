"""Typed executable recipe contributions (issue #145 M12.10).

Exports the descriptor, discovery and execution CONTRACTS. It deliberately
exports no registration hook: there is no ``register()`` here, and a test asserts
its absence rather than trusting this sentence. A recipe becomes executable by
being listed in ``builtins.catalog``, which is a code change with a review, a
provenance hash and a registry revision — not a runtime call.

The four contribution MODELS live in ``boomi_mcp.models.recipe_contributions``
and are exported through ``boomi_mcp.models``; this package is deliberately not,
for the same reason the topology planner is not: ``boomi_mcp.models`` is the
authored-contract surface, and an execution engine reachable through it is an
execution engine in an authoring namespace.
"""

from .composer import (
    AttributedContributionV1,
    ComposedContributionsV1,
    RecipeInvocationV1,
    compose,
    order_invocations,
)
from .contracts import (
    ExpectedRecipeEntryV1,
    ExpectedRecipeRegistryV1,
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeDependencyV1,
    RecipeDescriptorV1,
    RecipeInputBase,
    RecipeProvenanceV1,
    RecipeReferenceV1,
    RecipeRegistrationV1,
    RecipeRegistrySkewV1,
    parse_semver,
)
from .engine import RecipeRequestV1, RecipeRunResultV1, run_recipes
from .errors import (
    RecipeDiagnosticV1,
    RecipeError,
    recipe_diagnostic,
    recipe_error,
    recipe_error_envelope,
)
from .materialization import (
    MaterializationCatalog,
    build_symbol_table,
    component_materialization_mode,
    placeholder_component_id,
)
# ``build_test_registry`` is deliberately NOT re-exported here. The contract is
# "no MCP-ACCESSIBLE registrar exists": production registrations are one
# immutable tuple, and the only other way to build a registry is a TEST-ONLY
# factory. Listing that factory in the package surface alongside
# ``production_registry`` made it read as a normal, supported way to assemble a
# registry at runtime. Tests import it from ``.registry`` directly, which is
# where a test-only entry point belongs (issue #145, §6 architect review).
from .registry import RecipeRegistry, production_registry

__all__ = [
    "AttributedContributionV1",
    "ComposedContributionsV1",
    "ExpectedRecipeEntryV1",
    "ExpectedRecipeRegistryV1",
    "MaterializationCatalog",
    "RecipeCapabilityRequirementV1",
    "RecipeConflictPolicyV1",
    "RecipeDependencyV1",
    "RecipeDescriptorV1",
    "RecipeDiagnosticV1",
    "RecipeError",
    "RecipeInputBase",
    "RecipeInvocationV1",
    "RecipeProvenanceV1",
    "RecipeReferenceV1",
    "RecipeRegistrationV1",
    "RecipeRegistry",
    "RecipeRegistrySkewV1",
    "RecipeRequestV1",
    "RecipeRunResultV1",
    "build_symbol_table",
    "component_materialization_mode",
    "compose",
    "order_invocations",
    "parse_semver",
    "placeholder_component_id",
    "production_registry",
    "recipe_diagnostic",
    "recipe_error",
    "recipe_error_envelope",
    "run_recipes",
]
